package vn.scalaeng.p1.parse

import vn.scalaeng.p1.csv.Csv
import vn.scalaeng.p1.model.*
import java.time.{Instant, LocalDate, ZoneId}
import scala.util.Try

/** Thu thập MỌI Issue từ một tuple các Either (tích lũy lỗi không cần thư viện — Chương 7). */
object Issues:
  def collect(fields: Product): List[Issue] =
    fields.productIterator.collect { case Left(i: Issue) => i }.toList

object OrderParser:
  val Header: Vector[String] =
    Vector("order_id", "user_id", "sku", "unit_price", "quantity", "status", "order_date", "updated_at")

  /** Ngày nghiệp vụ tính theo giờ Việt Nam: 2026-10-01T18:00:00Z là ngày 02/10 ở VN. */
  val BusinessZone: ZoneId = ZoneId.of("Asia/Ho_Chi_Minh")

  private def date(s: String): Either[String, LocalDate] =
    Try(LocalDate.parse(s.trim)).toEither.left.map(_ => s"'$s' không phải ngày yyyy-MM-dd")

  private def instant(s: String): Either[String, Instant] =
    Try(Instant.parse(s.trim)).toEither.left.map(_ => s"'$s' không phải thời điểm ISO-8601 UTC (ví dụ 2026-10-01T08:30:00Z)")

  def parseRow(lineNo: Int, raw: String): Either[Rejected, Order] =
    def reject(issues: List[Issue]) = Rejected(Source.Orders, lineNo, raw, issues)
    Csv.parseLine(raw) match
      case Left(err) => Left(reject(List(Issue("_line", err))))
      case Right(cols) if cols.size != Header.size =>
        Left(reject(List(Issue("_line", s"cần ${Header.size} cột, nhận ${cols.size}"))))
      case Right(cols) =>
        def field[A](idx: Int)(p: String => Either[String, A]): Either[Issue, A] = p(cols(idx)).left.map(Issue(Header(idx), _))
        val all = (
          field(0)(OrderId.parse), field(1)(UserId.parse), field(2)(Sku.parse), field(3)(Vnd.parse),
          field(4)(Quantity.parse), field(5)(OrderStatus.parse), field(6)(date), field(7)(instant)
        )
        all match
          case (Right(id), Right(user), Right(sku), Right(price), Right(qty), Right(status), Right(d), Right(upd)) =>
            // Quy tắc liên trường: chỉ kiểm tra khi từng trường đã hợp lệ
            val updDate = upd.atZone(BusinessZone).toLocalDate
            if updDate.isBefore(d) then
              Left(reject(List(Issue("updated_at", s"$upd (ngày VN $updDate) sớm hơn order_date $d"))))
            else Right(Order(id, user, sku, price, qty, status, d, upd, lineNo))
          case other => Left(reject(Issues.collect(other)))
