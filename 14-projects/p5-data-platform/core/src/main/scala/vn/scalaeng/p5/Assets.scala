package vn.scalaeng.p5

import io.circe.parser.decode
import io.circe.syntax.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.annotation.unused
import scala.util.Try

/** Input đã dùng: loại, tên, hash TẠI LÚC TÍNH; với input asset, `materializationId` chọn bản lịch sử mới nhất có hash khớp byte snapshot.
  * Nhiều lần tính cùng byte: chọn lần khớp gần nhất trong lịch sử đã đọc, không suy ra danh tính writer vật lý từ hash nội dung.
  * Không có bản khớp: fail trước khi ghi output, không ghi None (Chương 31: provenance/impact đi theo phiên bản).
  */
final case class InputRef(kind: String, ref: String, hash: String, materializationId: Option[String] = None)

/** Một file CÓ THỂ là input của partition. */
final case class Source(kind: String, ref: String, path: Path)

/** Input đọc MỘT lần (snapshot): hash ghi vào lineage và phép tính dùng cùng các byte này (Chương 31 mục 3.3, lỗi 4). */
final case class InputFile(kind: String, ref: String, bytes: Option[Array[Byte]]):
  val hash: String = bytes.fold("missing")(Io.hashBytes)
  def lines: Vector[String] = bytes.fold(Vector.empty)(b => new String(b, UTF_8).linesIterator.filter(_.trim.nonEmpty).toVector)

object InputFile:
  def read(s: Source): InputFile = InputFile(s.kind, s.ref, Option.when(Files.exists(s.path))(Try(Files.readAllBytes(s.path)).toOption).flatten)

/** Kết quả tính một partition: file chính + (tùy) file bản ghi bị loại + bộ đếm đối soát. */
final case class Output(lines: Vector[String], rejectedLines: Vector[String], counts: Map[String, Int])

/** Asset phân vùng theo ngày nghiệp vụ — như Chương 31: KHÔNG tự quyết định khi nào chạy, KHÔNG tự đọc đĩa trong `compute`. */
trait Asset:
  def name: String
  def codeVersion: String
  def upstream: List[String]
  /** Tên file output trong `staging/<asset>/day=D/`. */
  def fileName: String
  def sources(l: Lake, day: LocalDate): List[Source]
  def relevant(@unused day: LocalDate, files: List[InputFile]): List[InputFile] = files
  def compute(day: LocalDate, inputs: List[InputFile]): Output

/** Raw (mọi lô, mọi đường vào) → các đơn của NGÀY NGHIỆP VỤ `day`: validate, bỏ event gửi lại (cùng event_id CÙNG nội dung), cách ly event_id
  * trùng mà khác nội dung, giữ phiên bản mới nhất mỗi dòng hàng, loại xung đột.
  * Đối soát: rows = rejected + event_conflicts + duplicate_events + superseded + conflicts + kept.
  */
object StagingOrders extends Asset:
  val name = "staging_orders"
  val codeVersion = "v3"
  val upstream = Nil
  val fileName = "part.jsonl"

  /** Ngày của một dòng raw theo MỘT quy tắc (`Order.dayOf`); dòng không đọc được hoặc không có ngày hợp lệ → None → đối soát cấp raw. */
  def dayOfLine(line: String): Option[LocalDate] = decode[OrderEvent](line).toOption.flatMap(e => Order.dayOf(e.day))

  def sources(l: Lake, day: LocalDate): List[Source] = l.rawFiles("orders").map(f => Source("raw", l.relative(f), f)).toList
  /** Chỉ mục toàn raw, chuẩn hóa cả timestamp trước khi so nội dung; bản không hợp lệ vẫn tham gia phát hiện collision. */
  private def canonical(e: OrderEvent): OrderEvent = Order.validate(e).fold(_ => e, o =>
    OrderEvent(o.eventId, o.orderId, o.customerId, o.day.toString, o.status, o.sku, o.quantity, o.priceVnd, o.updatedAt.toString))

  private def events(files: List[InputFile]): Vector[OrderEvent] = files.toVector.flatMap(_.lines).flatMap(decode[OrderEvent](_).toOption)

  override def relevant(day: LocalDate, files: List[InputFile]): List[InputFile] =
    val all = events(files)
    val ids = all.filter(e => Order.dayOf(e.day).contains(day)).map(_.event_id.trim).toSet
    // Fingerprint chỉ chứa nội dung của ID chạm ngày này: lô khác ngày có cùng ID làm cũ ngày cũ;
    // lô không liên quan không làm cũ mọi partition. Sắp xếp/khử trùng để không phụ thuộc thứ tự lô.
    val index = all.filter(e => ids(e.event_id.trim)).map(canonical).map(_.asJson.noSpaces).distinct.sorted.mkString("\n")
    files.filter(_.lines.exists(dayOfLine(_).contains(day))) :+
      InputFile("event-index", s"event_ids@$day", Some(index.getBytes(UTF_8)))

  def compute(day: LocalDate, inputs: List[InputFile]): Output =
    val all = events(inputs)
    val collisions = all.groupBy(_.event_id.trim).view.mapValues(_.map(canonical).distinct.size).toMap.filter(_._2 > 1)
    val rows = events(inputs.filter(_.kind == "raw")).filter(e => Order.dayOf(e.day).contains(day))
    val (eventClash, clean) = rows.partition(e => collisions.contains(e.event_id.trim))
    val (bad, good) = clean.partitionMap(e => Order.validate(e).left.map(errs => RejectedRow(e.event_id, errs)))
    // Cùng event_id = CÙNG sự kiện gửi lại — chỉ khi nội dung (đã chuẩn hóa) giống hệt. Khác nội dung: producer hỏng, không chọn `.head` theo thứ
    // tự file (bản đầu làm vậy: đảo hai dòng là đổi doanh thu — review P5 M2). Cả nhóm bị cách ly và đếm riêng.
    val sameEvent = good.groupBy(_.eventId).values.toVector
    val unique = sameEvent.map(_.head)
    val dupEvents = sameEvent.map(_.size - 1).sum
    val clashRejects = eventClash.map(_.event_id.trim).distinct.map(id =>
      RejectedRow(id, List(s"event_id trùng nhưng ${collisions(id)} nội dung khác nhau — cách ly cả nhóm")))
    val perLine = unique.groupBy(o => (o.orderId, o.sku)).values.toVector.map { vs =>
      val top = vs.map(_.updatedAt).max
      val (newest, older) = vs.partition(_.updatedAt == top)
      (newest, older.size)
    }
    val kept = perLine.collect { case (n, _) if n.map(_.payload).distinct.size == 1 => n.head }
    val conflicts = perLine.collect { case (n, _) if n.map(_.payload).distinct.size > 1 => n.size }.sum
    val superseded = perLine.map(_._2).sum + perLine.collect { case (n, _) if n.map(_.payload).distinct.size == 1 => n.size - 1 }.sum
    Output(kept.sortBy(o => (o.orderId, o.sku)).map(o => StagingRow.of(o).asJson.noSpaces),
      (bad ++ clashRejects).sortBy(r => (r.event_id, r.errors.mkString)).map(_.asJson.noSpaces),
      Map("rows" -> rows.size, "rejected" -> bad.size, "event_conflicts" -> eventClash.size, "duplicate_events" -> dupEvents,
        "superseded" -> superseded, "conflicts" -> conflicts, "kept" -> kept.size))

  /** Đọc partition staging. Dòng không giải mã được là LỖI (ném), không âm thầm bỏ qua. */
  def parse(lines: Vector[String]): Vector[StagingRow] =
    lines.zipWithIndex.map((x, i) => decode[StagingRow](x).fold(e => throw new IllegalStateException(s"staging dòng ${i + 1} hỏng: $e"), identity))

/** Doanh thu PAID theo segment của một ngày: staging cùng ngày × dimension khách. Phép join + group này đơn giản — ở hệ thống có warehouse,
  * đây là chỗ viết SQL (P5 README mục 4). Ở đây giữ trong Scala để có lineage và quality gate cùng một chỗ.
  */
object DailyRevenue extends Asset:
  val name = "daily_revenue"
  val codeVersion = "v2"
  val upstream = List(StagingOrders.name)
  val fileName = "part.csv"
  val Header = "segment,revenue_vnd,order_lines,orders"

  def sources(l: Lake, day: LocalDate): List[Source] = List(
    Source("asset", s"${StagingOrders.name}@$day", l.partFile(StagingOrders, day)),
    Source("dimension", l.relative(l.customers), l.customers))

  /** Segment chỉ là chữ/số/khoảng trắng nên daily vẫn là CSV an toàn; segment lạ trong dimension là lỗi dimension, phải ném. */
  def segments(lines: Vector[String]): Map[String, String] =
    lines.drop(1).map(_.split(",", -1)).map {
      case Array(c, s, _*) if s.trim.matches("[\\p{L}0-9 _-]+") => c.trim.toUpperCase(java.util.Locale.ROOT) -> s.trim
      case r => throw new IllegalStateException(s"customers.csv: dòng sai ${r.mkString(",")}")
    }.toMap

  def compute(day: LocalDate, inputs: List[InputFile]): Output =
    def of(kind: String) = inputs.find(_.kind == kind).fold(Vector.empty[String])(_.lines)
    val seg = segments(of("dimension"))
    val paid = StagingOrders.parse(of("asset")).filter(_.status == "PAID").map(r =>
      (r.order_id, seg.getOrElse(r.customer_id, "UNKNOWN"), BigInt(r.quantity) * BigInt(r.unit_price_vnd)))
    val out = paid.groupBy(_._2).toVector.sortBy(_._1).map((s, rs) => s"$s,${rs.map(_._3).sum},${rs.size},${rs.map(_._1).distinct.size}")
    Output(Header +: out, Vector.empty, Map("rows" -> out.size, "paid_lines" -> paid.size))

/** Đối soát CẤP RAW: mỗi dòng raw thuộc ĐÚNG MỘT ngày (theo `Order.dayOf`) hoặc nằm trong `unassigned` kèm lý do. Đối soát theo từng ngày KHÔNG
  * BAO GIỜ thấy dòng ngày sai — phải đếm ở đây, và lệnh `reconcile` công bố nó cho người vận hành (review P5 M4).
  */
object Reconcile:
  final case class Report(rawRows: Int, assigned: Map[LocalDate, Int], unassigned: Vector[String]):
    def balanced: Boolean = rawRows == assigned.values.sum + unassigned.size

  def report(l: Lake): Report =
    val rows = l.rawFiles("orders").flatMap(f => Io.lines(f).map(line => (l.relative(f), line)))
    val (unassigned, assigned) = rows.partitionMap { (f, line) =>
      decode[OrderEvent](line) match
        case Left(_)  => Left(s"$f: dòng không đọc được")
        case Right(e) => Order.dayOf(e.day).toRight(s"$f: ${e.event_id} day='${e.day}'")
    }
    Report(rows.size, assigned.groupMapReduce(identity)(_ => 1)(_ + _), unassigned)

  def unassigned(l: Lake): Vector[String] = report(l).unassigned

object Assets:
  val all: List[Asset] = List(StagingOrders, DailyRevenue) // thứ tự topo
  def byName(n: String): Option[Asset] = all.find(_.name == n)
