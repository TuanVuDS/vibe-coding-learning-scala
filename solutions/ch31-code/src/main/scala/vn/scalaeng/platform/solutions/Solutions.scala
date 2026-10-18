package vn.scalaeng.platform.solutions

import vn.scalaeng.platform.*

import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.{Duration, Instant, LocalDate}

object Solutions:
  private def days(from: LocalDate, to: LocalDate): Vector[LocalDate] = Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).toVector

  // Bài 1 — asset KHÔNG phân vùng phụ thuộc NHIỀU partition (many-to-one): tổng theo segment của cả khoảng ngày.
  // Lineage của nó gồm mọi partition daily trong khoảng: một ngày đổi là tổng đổi. Ta lưu nó như "partition" đặc biệt ở ngày `to`.
  final class SegmentTotals(from: LocalDate, to: LocalDate) extends Asset:
    val name = "segment_totals"
    val layer = "serving"
    val codeVersion = "v1"
    val upstream = List(DailyRevenue.name)
    def sources(p: Platform, day: LocalDate): List[Source] =
      days(from, to).toList.map(d => Source("asset", s"${DailyRevenue.name}@$d", p.partitionFile(DailyRevenue.layer, DailyRevenue.name, d)))
    def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int) =
      val rows = inputs.toVector.flatMap(_.lines.drop(1)).map(_.split(",", -1))
      val out = rows.groupMapReduce(_(0))(r => BigInt(r(1)))(_ + _).toVector.sortBy(_._1).map((s, v) => s"$s,$v")
      ("segment,revenue_vnd" +: out, 0)

  // Bài 2 — PROVENANCE (ngược chiều impact): partition này — đúng PHIÊN BẢN đang nằm trên đĩa — được tính từ những file raw nào, bắc cầu qua
  // upstream. Đi theo `materializationId` ghi trong lineage, KHÔNG theo bản mới nhất của upstream: staging có thể đã tính lại (thêm lô muộn,
  // bớt file bị cách ly) mà serving chưa tính lại. Upstream không rõ phiên bản (ID = None) được báo thẳng "?", không đoán.
  def provenance(p: Platform, asset: String, day: LocalDate): Vector[String] =
    val ms = Lineage.all(p)
    val byId = ms.map(m => m.id -> m).toMap
    def go(m: Materialization, seen: Set[String]): (Set[String], Set[String]) = // (nguồn, id đã đi qua)
      m.inputs.foldLeft((Set.empty[String], seen + m.id)) { case ((acc, vis), i) =>
        if i.kind == "raw" then (acc + i.ref, vis)
        else i.materializationId.flatMap(byId.get) match
          case Some(u) if !vis(u.id) => val (more, vis2) = go(u, vis); (acc ++ more, vis2)
          case Some(_)               => (acc, vis)
          case None                  => (acc + s"? ${i.ref} (không rõ phiên bản upstream)", vis)
      }
    Lineage.latestIn(ms, asset, day.toString).fold(Vector.empty[String])(m => go(m, Set.empty)._1.toVector.sorted)

  // Bài 3 — FRESHNESS: partition nào vi phạm SLA (chưa từng tính, cũ do input/code đổi, hoặc tính đã quá `maxAge`).
  def slaViolations(p: Platform, assets: List[Asset], from: LocalDate, to: LocalDate, now: Instant, maxAge: Duration): Vector[String] =
    for
      d <- days(from, to)
      a <- assets.toVector
      // FRESHNESS TẬN NGUỒN, không phải status cục bộ: serving khớp input trực tiếp nhưng staging đã cũ vẫn vi phạm SLA.
      problem <- (Orchestrator.freshness(p, assets, a, d), Lineage.latest(p, a.name, d)) match
        case (Status.Missing, _) => Some("chưa tính")
        case (Status.Stale(rs), _) => Some(rs.mkString("cũ: ", "; ", ""))
        case (_, Some(m)) if Duration.between(Instant.parse(m.at), now).compareTo(maxAge) > 0 => Some(s"tính lúc ${m.at}, quá ${maxAge.toHours} giờ")
        case _ => None
    yield s"$d ${a.name}: $problem"

  // Bài 4 — CỬA SỔ TÍNH LẠI: chỉ backfill N ngày gần nhất; dữ liệu đến muộn hơn cửa sổ được LIỆT KÊ để xử lý riêng, không âm thầm bỏ qua.
  final case class WindowRun(steps: Vector[Orchestrator.Step], lateOutsideWindow: Vector[String])
  def windowedBackfill(p: Platform, assets: List[Asset], today: LocalDate, windowDays: Int, earliest: LocalDate, runId: String): WindowRun =
    val from = today.minusDays((windowDays - 1).toLong)
    val steps = Orchestrator.backfill(p, assets, from, today, runId)
    val late = days(earliest, from.minusDays(1)).flatMap { d =>
      assets.map(a => a -> Orchestrator.status(p, a, d)).collect { case (a, Status.Stale(rs)) => s"$d ${a.name}: ${rs.mkString("; ")}" }
    }
    WindowRun(steps, late)

  // Bài 5 — HỢP ĐỒNG TẠI CỬA NGÕ: file raw sai header bị chuyển sang `raw_quarantine/` TRƯỚC khi pipeline thấy nó (raw vẫn append-only:
  // ta không sửa file, chỉ không nhận nó). Trả danh sách file đã cách ly.
  def quarantineBadHeaders(p: Platform, source: String, expectedHeader: String): Vector[String] =
    p.rawFiles(source).filter(f => Files2.lines(f).headOption.map(_.trim).exists(_ != expectedHeader)).map { f =>
      val target = p.root.resolve("raw_quarantine").resolve(p.root.resolve("raw").relativize(f).toString)
      Files.createDirectories(target.getParent)
      Files.move(f, target, StandardCopyOption.ATOMIC_MOVE)
      p.relative(target)
    }

  // Bài 6 — REPLAY TỪ RAW + ĐỐI CHIẾU (ý tưởng Kappa hướng tới: dùng lại CÙNG logic khi phát lại; lab không có stream processor nên đây chưa
  // phải Kappa): tính lại toàn bộ từ raw trên một root MỚI rồi so từng partition với bản đang phục vụ. Đây là phép kiểm tra nhất quán với
  // MỘT snapshot input (raw + customers.csv HIỆN TẠI) và code hiện tại: bằng nhau là bằng chứng cho lần so này, không chứng minh pipeline tất
  // định. Lệch thì trước hết xem partition có stale không (input/code đã đổi mà chưa backfill), rồi mới nghĩ tới sửa tay hay nondeterminism.
  def replayMatches(p: Platform, assets: List[Asset], from: LocalDate, to: LocalDate, scratch: Path): Vector[String] =
    ArchitectureDemo.copyTree(p.root.resolve("raw"), scratch.resolve("raw"))
    Files.copy(p.root.resolve("customers.csv"), scratch.resolve("customers.csv"))
    val q = Platform(scratch)
    Orchestrator.backfill(q, assets, from, to, "replay", force = true)
    for
      d <- days(from, to)
      a <- assets.toVector
      if Files2.hash(p.partitionFile(a.layer, a.name, d)) != Files2.hash(q.partitionFile(a.layer, a.name, d))
    yield s"$d ${a.name}: serving khác bản tính lại từ raw"

  // Bài 7 — DRY-RUN: kế hoạch KHÔNG thực thi, ba loại:
  //   "chắc chạy"   — missing, hoặc không fresh khi không có upstream nào còn được tính trước nó;
  //   "có thể chạy" — upstream sẽ chạy/có thể chạy, kể cả downstream ĐANG stale: upstream có thể trở về hash downstream đã dùng. Chỉ chạy
  //                   nếu chỉ stale do input đó, runtime chạy khi hash sau tính lại KHÁC hash downstream đã tiêu thụ. Lý do độc lập khác vẫn có thể buộc chạy.
  //                   Không biết trước output upstream được
  //                   (phải tính mới biết hash), nên ước lượng chi phí từ kế hoạch này là CẬN TRÊN;
  //   còn lại skip (không in). `willRun` chỉ theo tên asset trong CÙNG ngày — đủ cho DAG một-một theo ngày của lab; asset many-to-one/khác
  //   ngày (bài 1) cần lan theo partition cụ thể trong InputRef.
  def plan(p: Platform, assets: List[Asset], from: LocalDate, to: LocalDate): Vector[String] =
    days(from, to).flatMap { d =>
      assets.foldLeft((Vector.empty[String], Set.empty[String])) { case ((acc, willRun), a) =>
        val reason = Orchestrator.status(p, a, d) match
          case Status.Missing => Some("chắc chạy — missing")
          case Status.Fresh if a.upstream.exists(willRun) =>
            Some(s"có thể chạy — nếu output của ${a.upstream.filter(willRun).mkString(",")} khác hash partition này đã dùng")
          case Status.Stale(_) if a.upstream.exists(willRun) =>
            Some(s"có thể chạy — nếu output của ${a.upstream.filter(willRun).mkString(",")} khác hash partition này đã dùng")
          case Status.Fresh => None
          case s => Some(s"chắc chạy — ${Orchestrator.describe(s)}")
        reason.fold((acc, willRun))(r => (acc :+ s"$d ${a.name}: $r", willRun + a.name))
      }._1
    }
