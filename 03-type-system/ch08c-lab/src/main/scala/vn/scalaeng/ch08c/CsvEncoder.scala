package vn.scalaeng.ch08c

import java.time.LocalDate
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror

/** Một giá trị thành đúng một ô, chưa escape. Tách khỏi encoder của cả dòng. */
trait CellEncoder[A]:
  def encode(value: A): String

object CellEncoder:
  given CellEncoder[String] with
    def encode(value: String): String = value
  given CellEncoder[Int] with
    def encode(value: Int): String = value.toString
  given CellEncoder[Long] with
    def encode(value: Long): String = value.toString
  given CellEncoder[Boolean] with
    def encode(value: Boolean): String = value.toString
  given CellEncoder[BigDecimal] with
    def encode(value: BigDecimal): String = value.bigDecimal.toPlainString
  given CellEncoder[LocalDate] with
    def encode(value: LocalDate): String = value.toString
  given [A](using inner: CellEncoder[A]): CellEncoder[Option[A]] with
    def encode(value: Option[A]): String = value.fold("")(inner.encode)

trait CsvEncoder[A]:
  def header: List[String]
  def encode(value: A): List[String]

object CsvEncoder:
  def apply[A](using encoder: CsvEncoder[A]): CsvEncoder[A] = encoder

  inline def derived[A <: Product](using mirror: Mirror.ProductOf[A]): CsvEncoder[A] =
    productEncoder[A](
      TypeTools.labels[mirror.MirroredElemLabels],
      cellWriters[mirror.MirroredElemTypes]
    )

  private inline def cellWriters[Ts <: Tuple]: List[Any => String] =
    inline erasedValue[Ts] match
      case _: EmptyTuple => Nil
      case _: (head *: tail) =>
        writer(summonInline[CellEncoder[head]]) :: cellWriters[tail]

  // Chỉ gọi theo thứ tự MirroredElemTypes của đúng product A.
  // Đây là ranh giới erasure nội bộ, không phải API nhận dữ liệu Any từ bên ngoài.
  private def writer[A](encoder: CellEncoder[A]): Any => String =
    value => encoder.encode(value.asInstanceOf[A])

  // Tạo anonymous class trong def thường: không nhân bản class ở mỗi chỗ inline.
  private def productEncoder[A <: Product](names: List[String], writers: List[Any => String]): CsvEncoder[A] =
    new CsvEncoder[A]:
      def header: List[String] = names
      def encode(value: A): List[String] =
        value.productIterator.zip(writers.iterator).map { (field, encodeCell) =>
          encodeCell(field)
        }.toList

object CsvWriter:
  def escape(value: String): String =
    if value.exists(c => c == ',' || c == '"' || c == '\r' || c == '\n') then
      "\"" + value.replace("\"", "\"\"") + "\""
    else value

  def write[A](rows: List[A])(using encoder: CsvEncoder[A]): String =
    val lines = encoder.header :: rows.map(encoder.encode)
    lines.map(_.map(escape).mkString(",")).mkString("\r\n")

final case class OrderReport(
    orderId: String,
    customer: String,
    total: BigDecimal,
    paidOn: Option[LocalDate]
) derives CsvEncoder
