package vn.scalaeng.quality

import java.time.{Instant, LocalDate}
import scala.util.Try

/** Pipeline đơn hàng của Chương 24, viết bằng Scala thuần trên collection — để chương này tập trung vào CÁCH KIỂM THỬ, không vào Spark.
  * Mọi hàm là hàm thuần: cùng input → cùng output (output được SẮP XẾP, không phụ thuộc thứ tự input).
  */
final case class RawOrder(batch: String, orderId: String, customerId: String, day: String, status: String, sku: String,
    quantity: String, priceVnd: String, updatedAt: String, extra: Option[String] = None)

enum Status:
  case Paid, Refunded, Cancelled

final case class Order(batch: LocalDate, orderId: String, customerId: String, day: LocalDate, status: Status, sku: String,
    quantity: Int, priceVnd: Long, updatedAt: Instant):
  def lineKey: (String, String) = (orderId, sku)
  /** Nội dung nghiệp vụ — lô nhập (`batch`) KHÔNG thuộc nội dung: cùng bản ghi đến ở hai lô vẫn là một. */
  def payload: (String, LocalDate, Status, Int, Long) = (customerId, day, status, quantity, priceVnd)
  /** Khóa sắp xếp TOÀN PHẦN (mọi trường): hai bản ghi khác nhau không bao giờ "hòa". Sắp theo khóa thiếu trường thì các bản hòa giữ thứ tự
    * gốc — thứ tự đó đến từ `groupBy` (bảng băm) nên output phụ thuộc thứ tự input. Property test tìm ra lỗi này (bài, mục 10).
    */
  def sortKey: (String, String, Long, String, Long, Int, Int, Long, Long) =
    (orderId, sku, updatedAt.toEpochMilli, customerId, day.toEpochDay, status.ordinal, quantity, priceVnd, batch.toEpochDay)

final case class Rejected(raw: RawOrder, errors: List[String]):
  /** Thư viện chuẩn chỉ có `Ordering` cho tuple tới 9 phần tử — trường thứ 10 phải lồng vào tuple con (lỗi biên dịch thật, bài mục 10). */
  def sortKey: ((String, String, String, String, String, String, String, String, String), String) =
    ((raw.batch, raw.orderId, raw.sku, raw.customerId, raw.day, raw.status, raw.quantity, raw.priceVnd, raw.updatedAt), raw.extra.getOrElse(""))
final case class Fact(order: Order, segment: String):
  def revenue: BigInt = BigInt(order.quantity) * order.priceVnd
final case class DailyRow(day: LocalDate, segment: String, revenueVnd: BigInt, orderLines: Int, orders: Int)

final case class Output(
    facts: Vector[Fact],
    daily: Vector[DailyRow],
    rejected: Vector[Rejected],
    conflicts: Vector[Order],
    exactDuplicates: Int,
    superseded: Int
)

object Pipeline:
  private def text(s: String): String = Option(s).map(_.trim).getOrElse("")

  def validate(r: RawOrder): Either[Rejected, Order] =
    val batch   = Try(LocalDate.parse(text(r.batch))).toOption
    val day     = Try(LocalDate.parse(text(r.day))).toOption
    val status  = Status.values.find(_.toString.equalsIgnoreCase(text(r.status)))
    val qty     = text(r.quantity).toIntOption.filter(q => q >= 1 && q <= 1000)
    val price   = text(r.priceVnd).toLongOption.filter(_ > 0)
    val updated = Try(Instant.parse(text(r.updatedAt))).toOption
    val errors = List(
      Option.when(batch.isEmpty)("batch: phải là ngày yyyy-MM-dd"),
      Option.when(text(r.orderId).isEmpty)("order_id: bắt buộc"),
      Option.when(text(r.customerId).isEmpty)("customer_id: bắt buộc"),
      Option.when(day.isEmpty)("day: phải là ngày yyyy-MM-dd"),
      Option.when(status.isEmpty)("status: phải là một trong PAID/REFUNDED/CANCELLED"),
      Option.when(text(r.sku).isEmpty)("sku: bắt buộc"),
      Option.when(qty.isEmpty)("quantity: số nguyên 1..1000"),
      Option.when(price.isEmpty)("unit_price_vnd: số nguyên dương"),
      Option.when(updated.isEmpty)("updated_at: phải là timestamp ISO-8601"),
      // Kiểm tra SỰ HIỆN DIỆN của cột thừa, không phải nội dung: dòng kết thúc bằng "," có một cột thừa RỖNG — vẫn là dòng sai cấu trúc
      // (review T17 vòng 2: kiểm nội dung đã trim làm mất dấu hiệu này).
      r.extra.map(x => s"dòng có giá trị thừa ngoài header: '$x'")
    ).flatten
    if errors.nonEmpty then Left(Rejected(r, errors))
    else Right(Order(batch.get, text(r.orderId), text(r.customerId).toUpperCase(java.util.Locale.ROOT), day.get, status.get,
      text(r.sku).toUpperCase(java.util.Locale.ROOT), qty.get, price.get, updated.get))

  final case class Deduped(latest: Vector[Order], conflicts: Vector[Order], exactDuplicates: Int, superseded: Int)

  /** Hai tầng như Chương 24: trùng tuyệt đối (nội dung + updated_at) → một; mỗi dòng hàng giữ updated_at lớn nhất; nhiều bản mới nhất khác
    * nội dung → xung đột. Bản đến TRƯỚC (lô sớm nhất) được giữ khi trùng tuyệt đối — để kết quả không phụ thuộc thứ tự input.
    */
  def dedup(orders: Seq[Order]): Deduped =
    val byRecord = orders.groupBy(o => (o.lineKey, o.payload, o.updatedAt)).values.toVector
    val unique   = byRecord.map(_.minBy(_.batch.toEpochDay))
    val exactDup = orders.size - unique.size
    val perLine  = unique.groupBy(_.lineKey).values.toVector.map { versions =>
      val top = versions.map(_.updatedAt).max
      val (newest, older) = versions.partition(_.updatedAt == top)
      (newest, older.size)
    }
    val latest    = perLine.collect { case (n, _) if n.map(_.payload).distinct.size == 1 => n.head }
    val conflicts = perLine.collect { case (n, _) if n.map(_.payload).distinct.size > 1 => n }.flatten
    Deduped(latest.sortBy(_.sortKey), conflicts.sortBy(_.sortKey), exactDup, perLine.map(_._2).sum)

  def facts(latest: Seq[Order], segments: Map[String, String]): Vector[Fact] =
    latest.map(o => Fact(o, segments.getOrElse(o.customerId, "UNKNOWN"))).toVector

  def daily(facts: Seq[Fact]): Vector[DailyRow] =
    facts.filter(_.order.status == Status.Paid).groupBy(f => (f.order.day, f.segment)).toVector.map { case ((d, s), fs) =>
      DailyRow(d, s, fs.map(_.revenue).sum, fs.size, fs.map(_.order.orderId).distinct.size)
    }.sortBy(r => (r.day, r.segment))

  /** Khóa dimension phải duy nhất — trùng thì join nhân doanh thu (Chương 23). */
  def segments(customers: Seq[(String, String)]): Either[List[String], Map[String, String]] =
    val dups = customers.groupBy(_._1).collect { case (k, v) if v.size > 1 => k }.toList.sorted
    if dups.nonEmpty then Left(dups) else Right(customers.toMap)

  def run(raw: Seq[RawOrder], segmentOf: Map[String, String]): Output =
    val (rejected, valid) = raw.partitionMap(validate)
    val d = dedup(valid)
    val f = facts(d.latest, segmentOf)
    Output(f, daily(f), rejected.toVector.sortBy(_.sortKey), d.conflicts, d.exactDuplicates, d.superseded)

/** Trạng thái GỌN cho chạy tăng dần (incremental): mỗi dòng hàng chỉ nhớ updated_at lớn nhất và các nội dung ở thời điểm đó.
  * Dùng khi không muốn đọc lại toàn bộ lịch sử mỗi lần — nhưng phải CHỨNG MINH tương đương với tính lại toàn bộ (property test).
  */
object Incremental:
  type State = Map[(String, String), (Instant, Set[Order])]

  def step(s: State, batch: Seq[Order]): State =
    batch.foldLeft(s) { (acc, o) =>
      acc.get(o.lineKey) match
        case None                                  => acc.updated(o.lineKey, (o.updatedAt, Set(o)))
        case Some((t, _)) if o.updatedAt.isAfter(t) => acc.updated(o.lineKey, (o.updatedAt, Set(o)))
        case Some((t, os)) if o.updatedAt == t      =>
          // Cùng nội dung ở lô khác là trùng tuyệt đối: giữ bản lô sớm nhất, như Pipeline.dedup.
          val same = os.find(_.payload == o.payload)
          val kept = same match
            case Some(x) if x.batch.isAfter(o.batch) => os - x + o
            case Some(_)                             => os
            case None                                => os + o
          acc.updated(o.lineKey, (t, kept))
        case Some(_) => acc // bản cũ hơn đến muộn: bỏ qua
    }

  def latest(s: State): Vector[Order] = s.values.collect { case (_, os) if os.size == 1 => os.head }.toVector.sortBy(_.sortKey)

  /** Các dòng hàng đang XUNG ĐỘT (nhiều nội dung ở cùng updated_at lớn nhất) — phải khớp `Pipeline.dedup(...).conflicts`, không chỉ `latest`. */
  def conflicts(s: State): Vector[Order] = s.values.collect { case (_, os) if os.size > 1 => os }.flatten.toVector.sortBy(_.sortKey)
