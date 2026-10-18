package scalaeng.ch08b

import java.time.LocalDate

/** Chương 8B — Type class: hành vi "mã hóa thành dòng CSV" cho các kiểu KHÔNG sửa được mã nguồn
  * (Int, String, LocalDate, Option...) và cho kiểu của chính ta, mà không cần kế thừa.
  */
trait CsvEncoder[A]:
  def header: List[String]
  def encode(a: A): List[String]

object CsvEncoder:
  /** summon-style accessor: CsvEncoder[A] thay vì summon[CsvEncoder[A]]. */
  def apply[A](using enc: CsvEncoder[A]): CsvEncoder[A] = enc

  /** Helper tạo instance cho một cột. */
  def column[A](name: String)(f: A => String): CsvEncoder[A] = new CsvEncoder[A]:
    def header: List[String] = List(name)
    def encode(a: A): List[String] = List(f(a))

  /** Escape theo RFC 4180: bọc trong nháy kép khi có dấu phẩy, nháy kép hoặc xuống dòng. */
  def escape(s: String): String =
    if s.exists(c => c == ',' || c == '"' || c == '\n') then "\"" + s.replace("\"", "\"\"") + "\""
    else s

/** Instances được đặt trong một object riêng; người dùng import bằng `import CsvInstances.given`. */
object CsvInstances:
  given CsvEncoder[String]    = CsvEncoder.column("value")(identity)
  given CsvEncoder[Int]       = CsvEncoder.column("value")(_.toString)
  given CsvEncoder[Long]      = CsvEncoder.column("value")(_.toString)
  given CsvEncoder[BigDecimal] = CsvEncoder.column("value")(_.bigDecimal.toPlainString)
  given CsvEncoder[LocalDate] = CsvEncoder.column("value")(_.toString)

  /** Conditional given: có CsvEncoder[A] thì suy ra CsvEncoder[Option[A]]. None -> ô rỗng. */
  given [A](using inner: CsvEncoder[A]): CsvEncoder[Option[A]] with
    def header: List[String] = inner.header
    def encode(a: Option[A]): List[String] = a match
      case Some(v) => inner.encode(v)
      case None    => inner.header.map(_ => "")

/** Extension method: cú pháp `value.toCsvRow` cho mọi A có CsvEncoder. */
object CsvSyntax:
  extension [A](a: A)(using enc: CsvEncoder[A])
    def toCsvRow: String = enc.encode(a).map(CsvEncoder.escape).mkString(",")

/** Context bound [A: CsvEncoder] là cách viết gọn của (using CsvEncoder[A]). */
object CsvWriter:
  def write[A: CsvEncoder](rows: List[A]): String =
    val enc = CsvEncoder[A]
    val lines = enc.header.mkString(",") :: rows.map(r => enc.encode(r).map(CsvEncoder.escape).mkString(","))
    lines.mkString("\n")

final case class OrderReport(orderId: String, customer: String, total: BigDecimal, paidOn: Option[LocalDate])

object OrderReport:
  import CsvInstances.given

  /** Instance cho kiểu của ta đặt trong COMPANION OBJECT -> compiler tự tìm thấy, không cần import. */
  given CsvEncoder[OrderReport] with
    def header: List[String] = List("order_id", "customer", "total", "paid_on")
    def encode(r: OrderReport): List[String] =
      List(r.orderId, r.customer) ++
        summon[CsvEncoder[BigDecimal]].encode(r.total) ++
        summon[CsvEncoder[Option[LocalDate]]].encode(r.paidOn)
