package vn.scalaeng.platform

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.annotation.unused

/** Một input mà một partition đã dùng: file raw, file dimension, hoặc partition của asset khác — kèm hash nội dung TẠI LÚC TÍNH.
  * Với input là partition của asset khác, `materializationId` trỏ tới ĐÚNG lần tính upstream đã sinh ra các byte được đọc — impact và
  * provenance đi theo ID này, không theo "bản mới nhất" của upstream (bản mới nhất có thể được tính SAU và từ input khác).
  */
final case class InputRef(kind: String, ref: String, hash: String, materializationId: Option[String] = None)

/** Một file CÓ THỂ là input của partition: loại, tên trong lineage, đường dẫn trên đĩa. */
final case class Source(kind: String, ref: String, path: Path)

/** Một input đã được ĐỌC MỘT LẦN vào bộ nhớ (snapshot). Hash ghi vào lineage và phép tính dùng CÙNG các byte này, nên lineage luôn mô tả đúng
  * dữ liệu đã tính — kể cả khi file trên đĩa đổi trong lúc tính, hay đổi rồi đổi lại (A → B → A). `read` chỉ trả Some khi đọc thành công;
  * mọi source đã khai báo đều bắt buộc có mặt, lỗi đọc được truyền ra trước publish. None chỉ dành cho snapshot tổng hợp do caller tự tạo.
  */
final case class InputFile(kind: String, ref: String, bytes: Option[Array[Byte]]):
  val hash: String = bytes.fold("missing")(Files2.hashBytes)
  def lines: Vector[String] = bytes.fold(Vector.empty)(b => new String(b, UTF_8).linesIterator.filter(_.trim.nonEmpty).toVector)

object InputFile:
  // Mọi source đã khai báo đều bắt buộc đọc được; không đổi lỗi I/O thành dữ liệu rỗng.
  def read(s: Source): InputFile = InputFile(s.kind, s.ref, Some(Files.readAllBytes(s.path)))

/** ASSET = một bảng/partition có định nghĩa: tên, lớp lưu trữ, phiên bản code, phụ thuộc, và cách tính MỘT partition.
  * Asset KHÔNG tự quyết định khi nào chạy — đó là việc của orchestrator. Tách như vậy để cùng một hàm tính chạy được hằng ngày, khi backfill, khi
  * chạy lại sau sửa bug, từ CLI hay từ Dagster.
  *
  * Asset cũng KHÔNG tự đọc đĩa trong `compute`: orchestrator đọc các `sources` thành snapshot, asset chỉ tính trên snapshot đó. Mọi file
  * mà phép tính phụ thuộc PHẢI nằm trong `sources` — file đọc lén ngoài danh sách thì đổi mà partition vẫn báo fresh.
  */
trait Asset:
  def name: String
  def layer: String
  /** Tăng khi đổi logic: mọi partition đã tính bằng phiên bản cũ thành "cũ" (stale) và cần tính lại (reprocessing). */
  def codeVersion: String
  def upstream: List[String]
  /** Các file CÓ THỂ là input của partition `day`. */
  def sources(p: Platform, day: LocalDate): List[Source]
  /** Trong snapshot, những input THỰC SỰ thuộc về partition (lineage chính xác). Mặc định: tất cả. */
  def relevant(@unused day: LocalDate, files: List[InputFile]): List[InputFile] = files
  /** Tính partition CHỈ từ snapshot: các dòng CSV (có header) + số dòng bị loại/xung đột (để quan sát chất lượng). */
  def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int)

/** STAGING: mọi lô raw → các đơn của NGÀY NGHIỆP VỤ `day`, đã validate và dedup (mỗi dòng hàng giữ bản `updated_at` mới nhất).
  * Vì đọc MỌI lô, lô 17/10 chứa hoàn tiền của đơn ngày 16 sẽ làm partition ngày 16 thay đổi — đúng điều ta muốn.
  */
object StagingOrders extends Asset:
  val name = "staging_orders"
  val layer = "staging"
  val codeVersion = "v1"
  val upstream = Nil
  val Header = "order_id,customer_id,status,sku,quantity,unit_price_vnd,updated_at"

  private def ofDay(day: LocalDate)(line: String) = RawOrders.declaredDay(line).contains(day.toString)

  def sources(p: Platform, day: LocalDate): List[Source] = p.rawFiles("orders").map(f => Source("raw", p.relative(f), f)).toList

  /** Lineage CHÍNH XÁC: chỉ những file raw có dòng khai báo ngày `day`. File mới chỉ chứa ngày 17 không làm ngày 15 thành cũ. */
  override def relevant(day: LocalDate, files: List[InputFile]): List[InputFile] = files.filter(_.lines.drop(1).exists(ofDay(day)))

  def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int) =
    val rows = inputs.toVector.flatMap(_.lines.drop(1)).filter(ofDay(day))
    val parsed = rows.flatMap(RawOrders.parse)
    // Dedup: mỗi (order_id, sku) giữ bản mới nhất; nhiều bản mới nhất KHÁC nội dung → xung đột, không chọn bừa (Chương 24).
    val latest = parsed.distinct.groupBy(o => (o.orderId, o.sku)).values.toVector.flatMap { vs =>
      val top = vs.map(_.updatedAt).max
      val newest = vs.filter(_.updatedAt == top)
      if newest.size == 1 then newest else Vector.empty
    }
    val conflicts = parsed.distinct.groupBy(o => (o.orderId, o.sku)).values.count { vs =>
      val top = vs.map(_.updatedAt).max
      vs.count(_.updatedAt == top) > 1
    }
    val out = latest.sortBy(o => (o.orderId, o.sku)).map(o =>
      s"${o.orderId},${o.customerId},${o.status},${o.sku},${o.quantity},${o.priceVnd},${o.updatedAt}")
    (Header +: out, rows.size - parsed.size + conflicts)

/** SERVING: doanh thu PAID theo segment của một ngày — đọc partition staging cùng ngày + bảng khách (dimension không phân vùng). */
object DailyRevenue extends Asset:
  val name = "daily_revenue"
  val layer = "serving"
  val codeVersion = "v1"
  val upstream = List(StagingOrders.name)
  val Header = "segment,revenue_vnd,order_lines,orders"

  def sources(p: Platform, day: LocalDate): List[Source] =
    val customers = p.root.resolve("customers.csv")
    List(Source("asset", s"${StagingOrders.name}@$day", p.partitionFile(StagingOrders.layer, StagingOrders.name, day)),
      Source("raw", p.relative(customers), customers))

  def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int) =
    def linesOf(kind: String) = inputs.find(_.kind == kind).fold(Vector.empty[String])(_.lines).drop(1)
    val segmentOf = linesOf("raw").map(_.split(",", -1)).collect { case Array(c, s, _*) => c.trim.toUpperCase -> s.trim }.toMap
    val paid = linesOf("asset").map(_.split(",", -1)).collect {
      case Array(id, cust, "PAID", _, qty, price, _) => (id, segmentOf.getOrElse(cust, "UNKNOWN"), BigInt(qty) * BigInt(price))
    }
    val out = paid.groupBy(_._2).toVector.sortBy(_._1).map { (seg, rs) =>
      s"$seg,${rs.map(_._3).sum},${rs.size},${rs.map(_._1).distinct.size}"
    }
    (Header +: out, 0)

object Assets:
  /** Thứ tự TOPO: upstream trước downstream. Danh sách nhỏ cố định; hệ thống thật suy ra từ đồ thị phụ thuộc. */
  val all: List[Asset] = List(StagingOrders, DailyRevenue)
  def byName(n: String): Option[Asset] = all.find(_.name == n)
