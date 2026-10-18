package vn.scalaeng.platform

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import io.circe.syntax.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.util.Try

/** Một dòng lineage: partition `asset@day` được tính bởi `runId`, bằng code `codeVersion`, từ đúng các `inputs` này, ra `outputHash`.
  * `id` định danh BẤT BIẾN của lần tính này — downstream ghi lại ID upstream mà nó đã đọc.
  */
final case class Materialization(asset: String, day: String, codeVersion: String, inputs: List[InputRef], outputHash: String, rows: Int,
    rejected: Int, runId: String, at: String, durationMs: Long, id: String)

object Lineage:
  given Encoder[InputRef] = deriveEncoder
  given Decoder[InputRef] = deriveDecoder
  given Encoder[Materialization] = deriveEncoder
  given Decoder[Materialization] = deriveDecoder

  /** Chỉ THÊM, mỗi dòng ghi kèm DSYNC: lineage là nhật ký, không sửa lịch sử. */
  def append(p: Platform, m: Materialization): Unit =
    Files.createDirectories(p.lineageFile.getParent)
    Files.write(p.lineageFile, (m.asJson.noSpaces + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND,
      StandardOpenOption.DSYNC): Unit

  /** Fail-closed: một dòng hỏng làm MỌI quyết định dừng lại — không đoán, không bỏ qua dòng (cách phục hồi: mục 10 của bài). */
  def all(p: Platform): Vector[Materialization] =
    if !Files.exists(p.lineageFile) then Vector.empty
    else Files2.lines(p.lineageFile).map(l => decode[Materialization](l).fold(e => throw new IllegalStateException(s"lineage hỏng: $e"), identity))

  def latest(p: Platform, asset: String, day: LocalDate): Option[Materialization] = latestIn(all(p), asset, day.toString)

  def latestIn(ms: Vector[Materialization], asset: String, day: String): Option[Materialization] =
    ms.filter(m => m.asset == asset && m.day == day).lastOption

enum Status:
  case Fresh
  case Missing
  case Stale(reasons: List[String])

object Orchestrator:
  /** `asset@day` → (asset, day). */
  private def partitionRef(ref: String): Option[(String, String)] = ref.split("@") match
    case Array(a, d) if a.nonEmpty => Try(LocalDate.parse(d)).toOption.map(day => a -> day.toString)
    case _           => None

  /** Đọc input của partition MỘT lần thành snapshot (xem `InputFile`). */
  def snapshot(p: Platform, a: Asset, day: LocalDate): List[InputFile] = a.relevant(day, a.sources(p, day).map(InputFile.read))

  /** Snapshot → tham chiếu ghi vào lineage. Input là partition upstream: gắn ID lần tính upstream đã sinh ra CHÍNH các byte vừa đọc (lần tính
    * gần nhất của partition đó, nếu hash khớp). Không khớp — output upstream bị sửa tay hoặc job chết giữa "ghi output" và "ghi lineage" —
    * thì ID = None: nguồn gốc KHÔNG RÕ; materialize từ chối publish, yêu cầu phục hồi upstream trước.
    */
  private def refs(ms: Vector[Materialization], files: List[InputFile]): List[InputRef] = files.map { f =>
    val id = if f.kind != "asset" then None
    else partitionRef(f.ref).flatMap((ua, ud) => Lineage.latestIn(ms, ua, ud)).filter(_.outputHash == f.hash).map(_.id)
    InputRef(f.kind, f.ref, f.hash, id)
  }

  def currentInputs(p: Platform, a: Asset, day: LocalDate): List[InputRef] = refs(Lineage.all(p), snapshot(p, a, day))

  private def knownInput(ms: Vector[Materialization], i: InputRef): Boolean =
    i.kind != "asset" || partitionRef(i.ref).exists { (a, d) =>
      i.materializationId.exists(id => ms.exists(m => m.id == id && m.asset == a && m.day == d && m.outputHash == i.hash))
    }

  /** Trạng thái CỤC BỘ: lần tính gần nhất có còn khớp code version, các input TRỰC TIẾP hiện tại và output trên đĩa không. Đây là câu hỏi
    * orchestrator cần trả lời TRƯỚC khi quyết định chạy — không đoán theo lịch ("chạy lại ngày hôm qua"), mà theo dữ liệu. Nó KHÔNG nói upstream
    * còn đúng không: hỏi điều đó dùng `freshness`.
    */
  def status(p: Platform, a: Asset, day: LocalDate): Status =
    val ms = Lineage.all(p)
    Lineage.latestIn(ms, a.name, day.toString) match
      case None => Status.Missing
      case Some(m) =>
        val before = m.inputs.map(i => i.ref -> i.hash).toMap
        val now    = snapshot(p, a, day).map(i => i.ref -> i.hash).toMap
        val reasons =
          m.inputs.filterNot(knownInput(ms, _)).map(i => s"upstream ${i.ref}: tham chiếu không hợp lệ hoặc thiếu phiên bản; phục hồi upstream rồi backfill") ++
          Option.when(m.codeVersion != a.codeVersion)(s"code ${m.codeVersion} → ${a.codeVersion}").toList ++
            (now.keySet -- before.keySet).toList.sorted.map(r => s"input mới $r") ++
            (before.keySet -- now.keySet).toList.sorted.map(r => s"input mất $r") ++
            (now.keySet & before.keySet).toList.sorted.filter(r => now(r) != before(r)).map(r => s"input đổi $r") ++
            Option.when(Files2.hash(p.partitionFile(a.layer, a.name, day)) != m.outputHash)("output trên đĩa khác lần tính gần nhất").toList
        if reasons.isEmpty then Status.Fresh else Status.Stale(reasons)

  /** FRESHNESS TẬN NGUỒN — câu hỏi của giám sát/SLA: partition khớp cục bộ VÀ mọi partition upstream nó đã đọc cũng fresh tận nguồn.
    * Ví dụ: raw ngày 15 có hoàn tiền mới nhưng staging chưa tính lại → `status(daily@15)` vẫn Fresh (input trực tiếp chưa đổi) còn
    * `freshness(daily@15)` là Stale("upstream staging_orders@15 ..."). Đi theo đúng partition ghi trong InputRef; `seen` chống chu trình.
    */
  def freshness(p: Platform, assets: List[Asset], a: Asset, day: LocalDate, seen: Set[String] = Set.empty): Status =
    status(p, a, day) match
      case Status.Fresh =>
        val upstreamReasons = Lineage.latest(p, a.name, day).toList.flatMap(_.inputs).filter(_.kind == "asset").flatMap { i =>
          partitionRef(i.ref) match
            case Some((ua, ud)) if !seen(i.ref) =>
              assets.find(_.name == ua) match
                case None    => List(s"upstream ${i.ref}: không biết asset $ua")
                case Some(u) =>
                  freshness(p, assets, u, LocalDate.parse(ud), seen + s"${a.name}@$day" + i.ref) match
                    case Status.Fresh => Nil
                    case s            => List(s"upstream ${i.ref} ${describe(s)}")
            case Some(_) => List(s"upstream ${i.ref}: chu trình phụ thuộc")
            case None    => List(s"upstream ${i.ref}: tham chiếu không hợp lệ")
        }
        if upstreamReasons.isEmpty then Status.Fresh else Status.Stale(upstreamReasons)
      case s => s

  /** Tính MỘT partition và ghi lineage. Idempotent: cùng input + cùng code → cùng output (output sắp xếp, không chứa thời điểm chạy).
    * Phép tính chạy trên SNAPSHOT: file đến (hoặc đổi) sau khi chụp không lọt vào output lẫn lineage — lần `status` sau sẽ báo "input mới/đổi".
    * Hai bước "thay output" (ATOMIC_MOVE) và "thêm dòng lineage" KHÔNG nằm trong một giao dịch: chết giữa hai bước thì output mới nằm trên đĩa
    * với lineage cũ → `status` báo "output trên đĩa khác lần tính gần nhất" → lần chạy sau tính lại. Khóa độc quyền toàn root từ snapshot đến append
    * từ chối materialize đồng thời; hash được tính từ serialization riêng của run, không đọc lại mutable target.
    */
  def materialize(p: Platform, a: Asset, day: LocalDate, runId: String): Materialization = Files2.withWriterLock(p) {
    val t0 = System.nanoTime()
    val files = snapshot(p, a, day)
    val ms = Lineage.all(p)
    val inputs = refs(ms, files)
    val unknown = inputs.filterNot(knownInput(ms, _))
    require(unknown.isEmpty, s"upstream chưa có phiên bản hợp lệ: ${unknown.map(_.ref).mkString(", ")}; phục hồi upstream rồi backfill")
    val (lines, rejected) = a.compute(day, files)
    val target = p.partitionFile(a.layer, a.name, day)
    val outputHash = Files2.hashBytes(Files2.csvBytes(lines))
    Files2.writeAtomically(target, lines)
    val m = Materialization(a.name, day.toString, a.codeVersion, inputs, outputHash, lines.size - 1, rejected, runId,
      Instant.now().toString, (System.nanoTime() - t0) / 1000000, UUID.randomUUID().toString)
    Lineage.append(p, m)
    m
  }

  final case class Step(asset: String, day: LocalDate, action: String, detail: String)

  /** BACKFILL / chạy hằng ngày là CÙNG một thao tác: với mỗi ngày trong khoảng, theo thứ tự topo, chỉ tính partition Missing/Stale.
    * Dùng trạng thái CỤC BỘ là đủ vì upstream được xử lý TRƯỚC trong cùng vòng: upstream tính lại ra byte khác thì downstream thấy "input đổi";
    * ra byte y hệt thì downstream được skip đúng. Chạy lại khi không có gì đổi → mọi bước "skip" (idempotent). `force` chỉ bỏ qua quyết định skip
    * — nó KHÔNG vượt qua lineage hỏng (lineage vẫn được đọc để ghi tham chiếu upstream).
    */
  def backfill(p: Platform, assets: List[Asset], from: LocalDate, to: LocalDate, runId: String, force: Boolean = false): Vector[Step] =
    Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).toVector.flatMap { day =>
      assets.map { a =>
        status(p, a, day) match
          case Status.Fresh if !force => Step(a.name, day, "skip", "fresh")
          case s =>
            val m = materialize(p, a, day, runId)
            Step(a.name, day, "materialize", s"${describe(s)} → rows=${m.rows} rejected=${m.rejected}")
      }
    }

  def describe(s: Status): String = s match
    case Status.Fresh          => "fresh"
    case Status.Missing        => "missing"
    case Status.Stale(reasons) => reasons.mkString("stale: ", "; ", "")

  /** IMPACT ANALYSIS: một file raw (ví dụ phát hiện bị hỏng) ảnh hưởng tới những partition nào ĐANG PHỤC VỤ — trực tiếp và bắc cầu qua downstream.
    * Đi theo ID lần tính (không theo "bản mới nhất" của upstream): bắt đầu từ MỌI lần tính từng đọc file đó, lan theo các lần tính downstream đã
    * đọc chúng, rồi giữ những partition mà lần tính HIỆN HÀNH nằm trong tập đó. Nhờ vậy staging đã tính lại không còn dùng file, nhưng serving
    * cũ vẫn chứa đóng góp của nó, VẪN bị liệt kê.
    */
  def impact(p: Platform, rawRef: String): Vector[String] =
    val ms = Lineage.all(p)
    val byId = ms.map(m => m.id -> m).toMap
    val latest = ms.groupBy(m => (m.asset, m.day)).values.map(_.last).toVector
    def check(m: Materialization, seen: Set[String]): Unit =
      if !seen(m.id) then
        val unknown = m.inputs.filterNot(knownInput(ms, _))
        require(unknown.isEmpty, s"impact không đầy đủ: ${m.asset}@${m.day}; phục hồi upstream rồi backfill")
        m.inputs.flatMap(_.materializationId).foreach(id => check(byId(id), seen + m.id))
    latest.foreach(m => check(m, Set.empty))
    def closure(frontier: Set[String], seen: Set[String]): Set[String] =
      val next = ms.filter(_.inputs.exists(_.materializationId.exists(frontier))).map(_.id).toSet -- seen
      if next.isEmpty then seen else closure(next, seen ++ next)
    val direct = ms.filter(_.inputs.exists(i => i.kind == "raw" && i.ref == rawRef)).map(_.id).toSet
    val affected = closure(direct, direct)
    latest.filter(m => affected(m.id)).map(m => s"${m.asset}@${m.day}").sorted
