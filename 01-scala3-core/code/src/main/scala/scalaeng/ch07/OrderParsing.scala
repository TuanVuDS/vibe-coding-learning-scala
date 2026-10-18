package scalaeng.ch07

import scala.util.Try
import java.time.LocalDate

/** Chương 7 — Functional Programming.
  * Viết lại bài toán parse đơn hàng (Chương 3) theo phong cách FP:
  * lỗi là DỮ LIỆU (ADT), không phải exception.
  */

/** ADT lỗi: tập hữu hạn các loại lỗi, mỗi loại mang ngữ cảnh riêng. */
enum ParseError:
  case WrongColumnCount(lineNo: Int, found: Int)
  case InvalidNumber(lineNo: Int, field: String, raw: String)
  case InvalidDate(lineNo: Int, raw: String)
  case NonPositive(lineNo: Int, field: String)
  case EmptyField(lineNo: Int, field: String)

  def message: String = this match
    case WrongColumnCount(n, found)   => s"dòng $n: cần 5 cột, nhận $found"
    case InvalidNumber(n, field, raw) => s"dòng $n: $field không phải số: '$raw'"
    case InvalidDate(n, raw)          => s"dòng $n: ngày không hợp lệ: '$raw'"
    case NonPositive(n, field)        => s"dòng $n: $field phải > 0"
    case EmptyField(n, field)         => s"dòng $n: $field rỗng"

final case class OrderRecord(orderId: String, sku: String, unitPrice: BigDecimal, quantity: Int, orderDate: LocalDate):
  def total: BigDecimal = unitPrice * quantity

object OrderParsing:
  import ParseError.*

  // Các hàm nhỏ, thuần, mỗi hàm kiểm tra một điều
  def nonEmpty(lineNo: Int, field: String, raw: String): Either[ParseError, String] =
    if raw.isEmpty then Left(EmptyField(lineNo, field)) else Right(raw)

  def parseDecimal(lineNo: Int, field: String, raw: String): Either[ParseError, BigDecimal] =
    Try(BigDecimal(raw)).toOption.toRight(InvalidNumber(lineNo, field, raw))

  def parseInt(lineNo: Int, field: String, raw: String): Either[ParseError, Int] =
    raw.toIntOption.toRight(InvalidNumber(lineNo, field, raw))

  /** Try bọc API Java ném exception, rồi chuyển thành Either với lỗi của domain. */
  def parseDate(lineNo: Int, raw: String): Either[ParseError, LocalDate] =
    Try(LocalDate.parse(raw)).toEither.left.map(_ => InvalidDate(lineNo, raw))

  def positive[A](lineNo: Int, field: String, value: A)(using num: Numeric[A]): Either[ParseError, A] =
    if num.gt(value, num.zero) then Right(value) else Left(NonPositive(lineNo, field))

  /** for-comprehension trên Either: dừng ở lỗi ĐẦU TIÊN (fail-fast). */
  def parseLine(lineNo: Int, line: String): Either[ParseError, OrderRecord] =
    val cols = line.split(",", -1).map(_.trim).toList
    for
      _       <- Either.cond(cols.size == 5, (), WrongColumnCount(lineNo, cols.size))
      orderId <- nonEmpty(lineNo, "order_id", cols(0))
      sku     <- nonEmpty(lineNo, "sku", cols(1))
      price   <- parseDecimal(lineNo, "unit_price", cols(2)).flatMap(positive(lineNo, "unit_price", _))
      qty     <- parseInt(lineNo, "quantity", cols(3)).flatMap(positive(lineNo, "quantity", _))
      date    <- parseDate(lineNo, cols(4))
    yield OrderRecord(orderId, sku, price, qty, date)

  /** partitionMap: tách lỗi và bản ghi hợp lệ, KHÔNG dừng ở dòng lỗi. */
  def parseAll(lines: List[String]): (List[ParseError], List[OrderRecord]) =
    lines.zipWithIndex.partitionMap((line, idx) => parseLine(idx + 1, line))

/** Kết quả xử lý: dữ liệu thuần, có thể test, in, ghi file. */
final case class Report(
    validCount: Int,
    rejected: List[ParseError],
    duplicateIds: List[String],
    revenueByDate: List[(LocalDate, BigDecimal)]
)

/** FUNCTIONAL CORE: không đọc file, không in, không gọi đồng hồ. */
object OrderPipeline:
  /** Dedup theo orderId: giữ bản ghi xuất hiện ĐẦU TIÊN (quy tắc nghiệp vụ, ghi rõ). */
  def dedup(records: List[OrderRecord]): (List[OrderRecord], List[String]) =
    val (kept, dups, _) = records.foldLeft((List.empty[OrderRecord], List.empty[String], Set.empty[String])):
      case ((kept, dups, seen), r) =>
        if seen.contains(r.orderId) then (kept, r.orderId :: dups, seen)
        else (r :: kept, dups, seen + r.orderId)
    (kept.reverse, dups.reverse)

  def revenueByDate(records: List[OrderRecord]): List[(LocalDate, BigDecimal)] =
    records.groupMapReduce(_.orderDate)(_.total)(_ + _).toList.sortBy(_._1)

  def run(lines: List[String]): Report =
    val (errors, records) = OrderParsing.parseAll(lines)
    val (unique, dups)    = dedup(records)
    Report(unique.size, errors, dups, revenueByDate(unique))

/** Option: giá trị có thể không tồn tại — thay cho null. */
final case class Customer(id: String, name: String, email: Option[String], referrerId: Option[String])

object CustomerLookup:
  def find(db: Map[String, Customer], id: String): Option[Customer] = db.get(id)

  def emailDomain(db: Map[String, Customer], id: String): Option[String] =
    for
      customer <- find(db, id)
      email    <- customer.email
      at        = email.indexOf('@')
      if at > 0
    yield email.substring(at + 1).toLowerCase

  /** Chuỗi Option: tên người giới thiệu của khách hàng. */
  def referrerName(db: Map[String, Customer], id: String): String =
    find(db, id).flatMap(_.referrerId).flatMap(db.get).map(_.name).getOrElse("(không có)")

val sampleCsv: List[String] = List(
  "ORD-1, AO-01, 120000, 2, 2026-10-01",
  "ORD-2, TAT-02, 85000, 3, 2026-10-01",
  "ORD-3, NON-03, abc, 1, 2026-10-02",
  "ORD-4, , 50000, 1, 2026-10-02",
  "ORD-5, GIAY-05, 450000, 0, 2026-10-02",
  "ORD-6, MU-06, 99000, 1, 2026-13-40",
  "ORD-7, AO-01, 120000",
  "ORD-1, AO-01, 120000, 2, 2026-10-01",
  "ORD-8, BALO-08, 350000, 1, 2026-10-02"
)

/** IMPERATIVE SHELL: chỗ duy nhất có side effect (in ra màn hình). */
@main def ch07Demo(): Unit =
  val report = OrderPipeline.run(sampleCsv)
  println(s"Hợp lệ (sau dedup): ${report.validCount}")
  println(s"Bị loại: ${report.rejected.size}")
  report.rejected.foreach(e => println(s"  - ${e.message}"))
  println(s"Trùng order_id: ${report.duplicateIds}")
  report.revenueByDate.foreach((d, rev) => println(s"  $d: $rev"))

  val db = Map(
    "C1" -> Customer("C1", "An", Some("an@Example.com"), None),
    "C2" -> Customer("C2", "Bình", None, Some("C1")),
    "C3" -> Customer("C3", "Chi", Some("chi-at-example"), Some("C9"))
  )
  println(s"Domain C1: ${CustomerLookup.emailDomain(db, "C1")}")
  println(s"Domain C2: ${CustomerLookup.emailDomain(db, "C2")}")
  println(s"Domain C3: ${CustomerLookup.emailDomain(db, "C3")}")
  println(s"Domain C404: ${CustomerLookup.emailDomain(db, "C404")}")
  println(s"Người giới thiệu của C2: ${CustomerLookup.referrerName(db, "C2")}")
  println(s"Người giới thiệu của C3: ${CustomerLookup.referrerName(db, "C3")}")
