package vn.scalaeng.p5

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import io.circe.syntax.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.util.Try

final case class Materialization(asset: String, day: String, codeVersion: String, inputs: List[InputRef], outputHash: String,
    counts: Map[String, Int], runId: String, at: String, id: String)

object Lineage:
  given Encoder[InputRef] = deriveEncoder
  given Decoder[InputRef] = deriveDecoder
  given Encoder[Materialization] = deriveEncoder
  given Decoder[Materialization] = deriveDecoder

  def append(l: Lake, m: Materialization): Unit =
    Files.createDirectories(l.lineageFile.getParent)
    Files.write(l.lineageFile, (m.asJson.noSpaces + "\n").getBytes(UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND,
      StandardOpenOption.DSYNC): Unit

  def all(l: Lake): Vector[Materialization] =
    if !Files.exists(l.lineageFile) then Vector.empty
    else Io.lines(l.lineageFile).map(x => decode[Materialization](x).fold(e => throw new IllegalStateException(s"lineage hỏng: $e"), identity))

  def latest(l: Lake, asset: String, day: LocalDate): Option[Materialization] = latestIn(all(l), asset, day.toString)

  def latestIn(ms: Vector[Materialization], asset: String, day: String): Option[Materialization] =
    ms.filter(m => m.asset == asset && m.day == day).lastOption

enum Status:
  case Fresh
  case Missing
  case Stale(reasons: List[String])

object Orchestrator:
  def snapshot(l: Lake, a: Asset, day: LocalDate): List[InputFile] = a.relevant(day, a.sources(l, day).map(InputFile.read))

  private def refs(ms: Vector[Materialization], files: List[InputFile]): List[InputRef] = files.map { f =>
    val id = if f.kind != "asset" then None
    else f.ref.split("@") match
      // Upstream có thể đã có bản mới sau snapshot. Tra lịch sử theo đúng hash byte đã đọc,
      // không đánh mất provenance chỉ vì latest khác hash. Nếu nhiều bản cùng byte, chọn bản khớp mới nhất.
      case Array(ua, ud) => Some(ms.reverseIterator.find(m => m.asset == ua && m.day == ud && m.outputHash == f.hash && f.bytes.nonEmpty)
        .getOrElse(throw new IllegalStateException(s"không xác định được provenance cho ${f.ref}, hash=${f.hash}; materialize upstream rồi thử lại")).id)
      case _ => throw new IllegalStateException(s"tham chiếu asset không hợp lệ: ${f.ref}")
    InputRef(f.kind, f.ref, f.hash, id)
  }

  /** Trạng thái CỤC BỘ (Chương 31 mục 3.4): code version, input trực tiếp, output trên đĩa. Backfill theo thứ tự topo dùng câu hỏi này. */
  def status(l: Lake, a: Asset, day: LocalDate): Status =
    Lineage.latest(l, a.name, day) match
      case None => Status.Missing
      case Some(m) =>
        val before = m.inputs.map(i => i.ref -> i.hash).toMap
        val now    = snapshot(l, a, day).map(i => i.ref -> i.hash).toMap
        val reasons =
          Option.when(m.codeVersion != a.codeVersion)(s"code ${m.codeVersion} → ${a.codeVersion}").toList ++
            (now.keySet -- before.keySet).toList.sorted.map(r => s"input mới $r") ++
            (before.keySet -- now.keySet).toList.sorted.map(r => s"input mất $r") ++
            (now.keySet & before.keySet).toList.sorted.filter(r => now(r) != before(r)).map(r => s"input đổi $r") ++
            Option.when(Io.hash(l.partFile(a, day)) != m.outputHash)("output trên đĩa khác lần tính gần nhất").toList
        if reasons.isEmpty then Status.Fresh else Status.Stale(reasons)

  def describe(s: Status): String = s match
    case Status.Fresh          => "fresh"
    case Status.Missing        => "missing"
    case Status.Stale(reasons) => reasons.mkString("stale: ", "; ", "")

  /** Tính một partition TRÊN SNAPSHOT input, ghi nguyên tử, rồi ghi lineage. Idempotent. Bản đầu đọc input hai lần quanh phép tính và ném lỗi
    * khi khác — nhưng SAU khi đã thay output, và không bắt được A → B → A (Chương 31, lỗi 4). Ghi output và ghi lineage không phải một giao dịch:
    * chết giữa hai bước → `status` báo "output trên đĩa khác lần tính gần nhất" → tính lại.
    */
  def materialize(l: Lake, a: Asset, day: LocalDate, runId: String): Materialization =
    val files = snapshot(l, a, day)
    val inputs = refs(Lineage.all(l), files)
    val out = a.compute(day, files)
    Io.writeAtomically(l.partFile(a, day), out.lines.map(_ + "\n").mkString)
    val rejected = l.partitionDir(a.name, day).resolve("rejected.jsonl")
    if out.rejectedLines.nonEmpty then Io.writeAtomically(rejected, out.rejectedLines.mkString("", "\n", "\n")) else Files.deleteIfExists(rejected): Unit
    val m = Materialization(a.name, day.toString, a.codeVersion, inputs, Io.hash(l.partFile(a, day)), out.counts, runId, Instant.now().toString,
      UUID.randomUUID().toString)
    Lineage.append(l, m)
    m

  final case class Step(asset: String, day: LocalDate, action: String, detail: String)

  def days(from: LocalDate, to: LocalDate): Vector[LocalDate] = Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).toVector

  def backfill(l: Lake, assets: List[Asset], from: LocalDate, to: LocalDate, runId: String): Vector[Step] =
    days(from, to).flatMap { day =>
      assets.map { a =>
        status(l, a, day) match
          case Status.Fresh => Step(a.name, day, "skip", "fresh")
          case s =>
            val m = materialize(l, a, day, runId)
            Step(a.name, day, "materialize", s"${describe(s)} → ${m.counts.toList.sorted.map((k, v) => s"$k=$v").mkString(" ")}")
      }
    }

/** QUALITY GATE trước khi nạp serving (write–audit–publish, Chương 24/33). Trả danh sách vi phạm — rỗng mới được publish. Mọi vấn đề của
  * DỮ LIỆU/ARTIFACT (thiếu file, dòng hỏng) trở thành vi phạm có lý do, không thành exception (review P5 N2); lỗi I/O hạ tầng vẫn ném.
  */
object Quality:
  private val Equation = List("rejected", "event_conflicts", "duplicate_events", "superseded", "conflicts", "kept")

  def check(l: Lake, day: LocalDate): List[String] =
    val missing = List(StagingOrders, DailyRevenue).filter(a => Lineage.latest(l, a.name, day).isEmpty || !Files.exists(l.partFile(a, day)))
    if missing.nonEmpty then List(s"thiếu ${missing.map(_.name).mkString("/")} của $day — chưa tính đủ")
    else
      val c = Lineage.latest(l, StagingOrders.name, day).get.counts
      val staging = Try(StagingOrders.parse(Io.lines(l.partFile(StagingOrders, day)))).toEither.left.map(_.getMessage)
      val daily = Io.lines(l.partFile(DailyRevenue, day)).drop(1).zipWithIndex.map { (x, i) =>
        x.split(",", -1) match
          case Array(seg, rev, lines, orders) if Try((BigInt(rev), lines.toInt, orders.toInt)).isSuccess =>
            Right((seg, BigInt(rev), lines.toInt, orders.toInt))
          case _ => Left(s"daily dòng ${i + 2} sai định dạng: '$x'")
      }
      val dailyRows = daily.collect { case Right(r) => r }
      val stale = List(StagingOrders, DailyRevenue).filter(a => Orchestrator.status(l, a, day) != Status.Fresh)
      List(
        Option.when(stale.nonEmpty)("partition cũ — tính lại trước khi publish"),
        Option.when(!Equation.forall(c.contains) || c("rows") != Equation.map(c).sum)(s"đối soát staging hỏng: $c"),
        staging.left.toOption,
        staging.toOption.flatMap(rs => Option.when(rs.map(r => (r.order_id, r.sku)).distinct.size != rs.size)("staging trùng (order_id, sku)")),
        Option.when(daily.exists(_.isLeft))(daily.collect { case Left(e) => e }.mkString("; ")),
        Option.when(dailyRows.map(_._1).distinct.size != dailyRows.size)("daily trùng segment"),
        Option.when(dailyRows.exists((_, rev, ls, os) => rev < 0 || ls < os || os < 1))("daily ngoài miền (doanh thu âm, orders > order_lines)"),
        // Đối soát ĐỘC LẬP với phép join segment: tổng PAID tính thẳng từ staging phải bằng tổng daily (review P5 M1 — gate cũ dùng lại chính
        // phép tính có lỗi nên dòng bị rơi không ai thấy).
        staging.toOption.flatMap { rs =>
          val paid = rs.filter(_.status == "PAID")
          val (rev, ls) = (paid.map(r => BigInt(r.quantity) * BigInt(r.unit_price_vnd)).sum, paid.size)
          Option.when(daily.forall(_.isRight) && (rev != dailyRows.map(_._2).sum || ls != dailyRows.map(_._3).sum))(
            s"daily không khớp staging: PAID $rev VND / $ls dòng, daily ${dailyRows.map(_._2).sum} VND / ${dailyRows.map(_._3).sum} dòng")
        },
        Option.when(staging.isRight && Try(DailyRevenue.compute(day, Orchestrator.snapshot(l, DailyRevenue, day)).lines.drop(1)).toOption
          .forall(_ != Io.lines(l.partFile(DailyRevenue, day)).drop(1)))("daily trên đĩa khác bảng tính lại từ staging")
      ).flatten
