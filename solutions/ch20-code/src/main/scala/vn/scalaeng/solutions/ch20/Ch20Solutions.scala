package vn.scalaeng.solutions.ch20

import cats.Monoid
import cats.syntax.all.*
import java.time.LocalDate
import vn.scalaeng.transform.*

/** Bài 1 — quy tắc chất lượng trên TỔNG HỢP: tỷ lệ ĐƠN hoàn tiền mỗi ngày không vượt ngưỡng.
  * Một dòng không sai; NGÀY có tỷ lệ bất thường mới sai — kiểm tra trên nhóm, không trên dòng.
  *
  * GRAIN (đơn vị đếm) là ĐƠN, không phải dòng hàng: `Order` của lab là một dòng hàng (order_id, sku). Bản đầu chia theo số DÒNG:
  * O1 Paid 10 sku + O2 Refunded 1 sku → 1/11 = 9% (qua), trong khi theo đơn là 1/2 = 50% (review chéo, M5).
  * Một đơn tính là hoàn tiền nếu CÓ dòng Refunded (quy ước — ghi rõ, vì dữ liệu có thể có trạng thái lẫn lộn giữa các dòng).
  */
object RefundRateRule:
  def apply(maxRate: Double): Rule[Order] =
    QualityReport.requireRate(maxRate)
    Rule("tỷ lệ hoàn tiền theo ngày", Severity.Error, lines =>
      val byDay = lines.groupBy(_.day).view.mapValues { ls =>
        val orders = ls.groupBy(_.orderId).values
        orders.count(_.exists(_.status == Status.Refunded)).toDouble / orders.size
      }.toMap
      val bad   = byDay.filter(_._2 > maxRate).toList.sortBy(_._1.toEpochDay)
      RuleResult("tỷ lệ hoàn tiền theo ngày", Severity.Error, byDay.size, bad.size, bad.map((d, r) => f"$d:${r * 100}%.0f%%")))

/** Bài 2 — trung bình ĐÚNG là một Monoid trên (tổng, số lượng); chỉ chia ở bước cuối. */
final case class Mean(sum: BigDecimal, count: Long):
  def value: Option[BigDecimal] = Option.when(count > 0)(sum / count)

object Mean:
  def of(x: BigDecimal): Mean = Mean(x, 1)
  given Monoid[Mean] with
    def empty: Mean                    = Mean(0, 0)
    def combine(a: Mean, b: Mean): Mean = Mean(a.sum + b.sum, a.count + b.count)

/** Bài 3 — dedup TỔNG QUÁT bằng type class: "khóa nghiệp vụ" và "phiên bản" của một kiểu bản ghi.
  * Giữ bản có phiên bản LỚN NHẤT. Hai bản CÙNG phiên bản: giống nhau → một bản; KHÁC nhau → XUNG ĐỘT, không chọn bản nào
  * (bản đầu dùng `maxBy` chọn bản gặp trước → phụ thuộc thứ tự, review chéo M6). Nhờ vậy kết quả không phụ thuộc thứ tự đến.
  */
trait Versioned[A]:
  type Key
  def key(a: A): Key
  def version(a: A): Long

object Versioned:
  final case class Result[A, K](latest: List[A], conflicts: List[K])

  def dedupLatest[A](xs: List[A])(using v: Versioned[A]): Result[A, v.Key] =
    val decided = xs.groupBy(v.key).toList.map { (k, versions) =>
      val top  = versions.map(v.version).max
      val best = versions.filter(a => v.version(a) == top).distinct
      if best.size == 1 then Right(best.head) else Left(k)
    }
    Result(decided.collect { case Right(a) => a }, decided.collect { case Left(k) => k })

final case class CustomerUpdate(customerId: String, city: String, updatedAtEpochMs: Long)

object CustomerUpdate:
  given Versioned[CustomerUpdate] with
    type Key = String
    def key(a: CustomerUpdate): String     = a.customerId
    def version(a: CustomerUpdate): Long   = a.updatedAtEpochMs

/** Bài 4 — Top-K sản phẩm theo doanh thu, tính được theo partition.
  *
  * SAI: lấy top-K TRONG từng partition rồi gộp — một sku có doanh thu chia đều ở nhiều partition có thể không lọt top-K
  * ở partition nào, dù tổng của nó cao nhất. ĐÚNG: gộp tổng THEO KHÓA trước (Monoid trên Map), chỉ cắt top-K ở cuối.
  */
object TopK:
  // Tổng bằng BigInt: cộng Long thường thì Long.MaxValue + 1 thành số âm và "đứng đầu" ngược (review chéo, M1).
  def wrong(partitions: List[List[Order]], k: Int): List[(String, BigInt)] =
    partitions.flatMap(p => p.groupMapReduce(_.sku)(o => BigInt(o.revenueVnd))(_ + _).toList.sortBy(-_._2).take(k))
      .groupMapReduce(_._1)(_._2)(_ + _).toList.sortBy((s, r) => (-r, s)).take(k)

  def right(partitions: List[List[Order]], k: Int): List[(String, BigInt)] =
    partitions.map(p => p.groupMapReduce(_.sku)(o => BigInt(o.revenueVnd))(_ + _)).combineAll // Monoid[Map[String, BigInt]]: cộng theo khóa
      .toList.sortBy((s, r) => (-r, s)).take(k)

/** Bài 5 — số user KHÁC NHAU mỗi ngày: Set là Monoid (hợp tập) nên tính theo partition được, nhưng bộ nhớ ~ số user.
  * Khi quá lớn, dùng cấu trúc xấp xỉ (HyperLogLog) — sai số nhỏ, bộ nhớ cố định; không cài ở đây.
  */
object DistinctUsers:
  def perDay(partitions: List[List[Order]]): Map[LocalDate, Int] =
    partitions.map(p => Aggregate.by(p)(_.day)(o => Set(o.customerId))).foldLeft(Map.empty[LocalDate, Set[String]])(Aggregate.merge)
      .view.mapValues(_.size).toMap

/** Bài 6 — pipeline có CỔNG CHẤT LƯỢNG: validation → quality gate → báo cáo. Gate chặn thì KHÔNG tính báo cáo.
  *
  * PHẠM VI gate (review chéo, m2): quy tắc chỉ chạy trên dòng ĐÃ qua validation; dòng bị loại ở bước parse được đếm riêng
  * (`rejected`) và KHÔNG làm gate chặn. Nếu MỌI dòng bị loại, `orders` rỗng → mọi failRate = 0 → gate "qua" với báo cáo rỗng.
  * Muốn chặn cả trường hợp đó, thêm điều kiện trên `rejected / raw.size` — ở đây cố ý để lộ ra như một bẫy cần nhớ.
  * Ngưỡng được kiểm tra trong `QualityReport.blocking` (NaN hay ngoài [0, 1] → IllegalArgumentException).
  */
object GatedPipeline:
  enum Outcome:
    case Blocked(report: QualityReport, rejected: Int)
    case Published(revenue: Map[(Segment, LocalDate), Stats], report: QualityReport, rejected: Int)

  def run(raw: List[RawOrder], customers: Map[String, Customer], today: LocalDate, maxErrorRate: Double): Outcome =
    val (orders, bad) = Validate.partition(raw)(Validate.order)
    val report        = OrderRules.check(orders, customers.keySet, today)
    if report.blocking(maxErrorRate).nonEmpty then Outcome.Blocked(report, bad.size)
    else Outcome.Published(Reports.revenueBySegmentDay(Clean.accept(orders, customers.keySet, today), customers), report, bad.size)
