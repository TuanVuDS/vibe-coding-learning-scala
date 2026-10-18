package vn.scalaeng.transform

import cats.data.{NonEmptyChain, ValidatedNec}
import cats.syntax.all.*
import java.time.LocalDate
import scala.util.Try

/** Dòng THÔ đọc từ nguồn (mọi trường là chuỗi). Chương 19 đã bàn cách đọc; chương này tập trung vào BIẾN ĐỔI. */
final case class RawOrder(orderId: String, customerId: String, day: String, status: String, sku: String, quantity: String, unitPriceVnd: String)
final case class RawCustomer(customerId: String, segment: String, city: String)

enum Status:
  case Paid, Refunded, Cancelled

enum Segment:
  case Retail, Wholesale

final case class Order(orderId: String, customerId: String, day: LocalDate, status: Status, sku: String, quantity: Int, unitPriceVnd: Long):
  def revenueVnd: Long = quantity.toLong * unitPriceVnd

final case class Customer(customerId: String, segment: Segment, city: String)

/** Lỗi dữ liệu là DỮ LIỆU: trường nào, giá trị gì, sai quy tắc nào — đủ để người sửa nguồn hành động. */
final case class DataError(field: String, value: String, rule: String):
  override def toString: String = s"$field='$value': $rule"

/** Validation TÍCH LŨY bằng `ValidatedNec` của Cats.
  *
  * Chương 19 gom lỗi bằng tuple `Either` + `collect`. `ValidatedNec[E, A]` làm việc đó như một kiểu dữ liệu: `mapN` chạy MỌI
  * kiểm tra độc lập và gom tất cả lỗi vào một `NonEmptyChain` (danh sách không rỗng, nối O(1)). Khác `Either` — `Either` dừng ở
  * lỗi đầu tiên vì nó là Monad (bước sau có thể phụ thuộc kết quả bước trước), còn `Validated` chỉ là Applicative.
  */
object Validate:
  type V[A] = ValidatedNec[DataError, A]

  /** Nguồn ngoài (Java, JDBC) có thể đưa `null` vào chuỗi: mọi validator đọc qua `text` để `null` thành lỗi, không thành NullPointerException. */
  private def text(s: String): String = Option(s).map(_.trim).getOrElse("")

  /** Giới hạn NGHIỆP VỤ cho đơn giá: 1.000 tỷ VND. Cùng giới hạn số lượng 1..1000, doanh thu một dòng ≤ 10^15 luôn vừa Long —
    * bản đầu nhận đơn giá Long.MaxValue, `quantity * price` tràn thành -2 trước khi vào Stats (review chéo, M1).
    */
  val MaxUnitPriceVnd: Long = 1_000_000_000_000L

  private def nonEmpty(field: String, s: String): V[String] =
    Option(text(s)).filter(_.nonEmpty).toValidNec(DataError(field, String.valueOf(s), "bắt buộc"))

  private def enumOf[E](field: String, s: String, values: Array[E]): V[E] =
    values.find(_.toString.equalsIgnoreCase(text(s))).toValidNec(DataError(field, String.valueOf(s), s"phải là một trong ${values.mkString("/")}"))

  private def intIn(field: String, s: String, min: Int, max: Int): V[Int] =
    text(s).toIntOption.filter(n => n >= min && n <= max).toValidNec(DataError(field, String.valueOf(s), s"số nguyên trong $min..$max"))

  private def positiveLong(field: String, s: String): V[Long] =
    text(s).toLongOption.filter(p => p > 0 && p <= MaxUnitPriceVnd).toValidNec(DataError(field, String.valueOf(s), s"số nguyên trong 1..$MaxUnitPriceVnd"))

  private def date(field: String, s: String): V[LocalDate] =
    Try(LocalDate.parse(text(s))).toOption.toValidNec(DataError(field, String.valueOf(s), "ngày dạng yyyy-MM-dd"))

  def order(r: RawOrder): V[Order] =
    (
      nonEmpty("order_id", r.orderId),
      nonEmpty("customer_id", r.customerId).map(_.toUpperCase(java.util.Locale.ROOT)),
      date("day", r.day),
      enumOf("status", r.status, Status.values),
      nonEmpty("sku", r.sku).map(_.toUpperCase(java.util.Locale.ROOT)),
      intIn("quantity", r.quantity, 1, 1000),
      positiveLong("unit_price_vnd", r.unitPriceVnd)
    ).mapN(Order.apply)

  def customer(r: RawCustomer): V[Customer] =
    (nonEmpty("customer_id", r.customerId).map(_.toUpperCase(java.util.Locale.ROOT)), enumOf("segment", r.segment, Segment.values), nonEmpty("city", r.city))
      .mapN(Customer.apply)

  /** Tách hợp lệ / không hợp lệ trong MỘT lượt; giữ dòng gốc cạnh lỗi để truy vết. */
  def partition[R, A](rows: List[R])(f: R => V[A]): (List[A], List[(R, NonEmptyChain[DataError])]) =
    val (bad, good) = rows.partitionMap(r => f(r).toEither.leftMap(r -> _))
    (good, bad)
