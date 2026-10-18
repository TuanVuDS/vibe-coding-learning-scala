package vn.scalaeng.order.core

import vn.scalaeng.order.domain.*
import java.time.LocalDate
import scala.util.Try

final case class DailyRevenue(date: LocalDate, orders: Int, revenue: Long)

final case class PipelineResult(
    inputRows: Int,
    valid: List[OrderLine],
    rejected: List[RowError],
    duplicates: List[RowError],
    daily: List[DailyRevenue]
):
  def rejectRatio: Double = if inputRows == 0 then 0.0 else rejected.size.toDouble / inputRows
  /** Kiểm tra cân bằng: không bản ghi nào biến mất. */
  def isBalanced: Boolean = inputRows == valid.size + rejected.size + duplicates.size

object OrderPipeline:
  val ExpectedHeader: List[String] = List("order_id", "sku", "unit_price", "quantity", "order_date")

  def parseRow(lineNo: Int, raw: String): Either[RowError, OrderLine] =
    def fail(reason: String) = RowError(lineNo, reason, raw)
    for
      cols  <- CsvLine.split(raw).left.map(fail)
      _     <- Either.cond(cols.size == 5, (), fail(s"cần 5 cột, nhận ${cols.size}"))
      id    <- OrderId.parse(cols(0)).left.map(fail)
      sku   <- Sku.parse(cols(1)).left.map(fail)
      price <- Vnd.parse(cols(2)).left.map(fail)
      qty   <- Quantity.parse(cols(3)).left.map(fail)
      date  <- Try(LocalDate.parse(cols(4).trim)).toEither.left.map(_ => fail(s"ngày không hợp lệ: '${cols(4)}'"))
    yield OrderLine(id, sku, price, qty, date)

  /** Dữ liệu vào: toàn bộ dòng của file, KỂ CẢ header. */
  def run(lines: List[String]): Either[String, PipelineResult] =
    lines match
      case Nil => Left("file rỗng (không có header)")
      case header :: body =>
        // Regression (Chương 10): file CSV xuất từ Excel có BOM UTF-8 ở đầu -> header không khớp.
        val headerCols = CsvLine.split(header.stripPrefix("\uFEFF")).map(_.map(_.trim.toLowerCase))
        if headerCols != Right(ExpectedHeader) then Left(s"header không đúng: '$header'")
        else
          val rows = body.zipWithIndex.filter((l, _) => l.trim.nonEmpty).map((l, i) => (l, i + 2))
          val (rejected, parsed) = rows.partitionMap((l, n) => parseRow(n, l).map(o => (o, n, l)))
          val (valid, dups, _) = parsed.foldLeft((Vector.empty[OrderLine], Vector.empty[RowError], Set.empty[String])):
            case ((acc, d, seen), (o, n, l)) =>
              if seen(o.orderId.value) then (acc, d :+ RowError(n, s"trùng order_id ${o.orderId.value}", l), seen)
              else (acc :+ o, d, seen + o.orderId.value)
          val daily = valid.toList
            .groupBy(_.orderDate).toList.sortBy(_._1)
            .map((d, os) => DailyRevenue(d, os.size, os.map(_.total.dong).sum))
          Right(PipelineResult(rows.size, valid.toList, rejected, dups.toList, daily))

object Reports:
  def dailyCsv(daily: List[DailyRevenue]): String =
    ("date,orders,revenue_vnd" :: daily.map(d => s"${d.date},${d.orders},${d.revenue}")).mkString("", "\n", "\n")

  def rejectedCsv(errors: List[RowError]): String =
    ("line_no,reason,raw" :: errors.sortBy(_.lineNo).map(e => s"${e.lineNo},${CsvLine.escape(e.reason)},${CsvLine.escape(e.raw)}"))
      .mkString("", "\n", "\n")
