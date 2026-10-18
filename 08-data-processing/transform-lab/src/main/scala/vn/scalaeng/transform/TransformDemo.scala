package vn.scalaeng.transform

import cats.syntax.all.*
import java.time.LocalDate
import scala.io.Source
import scala.util.Using

/** Lọc dữ liệu để tính báo cáo: bỏ bản trùng (giữ bản ĐẦU), bỏ tham chiếu không tồn tại, bỏ ngày ở tương lai. */
object Clean:
  def accept(orders: List[Order], customers: Set[String], today: LocalDate): List[Order] =
    orders.distinctBy(o => (o.orderId, o.sku))
      .filter(o => customers.contains(o.customerId))
      .filterNot(_.day.isAfter(today))

/** Doanh thu theo (phân khúc khách hàng, ngày) — join bằng Map trong bộ nhớ, tổng hợp bằng Monoid[Stats]. */
object Reports:
  def revenueBySegmentDay(orders: List[Order], customers: Map[String, Customer]): Map[(Segment, LocalDate), Stats] =
    Aggregate.by(orders.filter(_.status == Status.Paid))(o => (customers(o.customerId).segment, o.day))(o => Stats.of(o.revenueVnd))

object TransformDemo:
  /** CSV đơn giản (không có dấu phẩy trong giá trị) — đọc file không phải trọng tâm của chương này (xem Chương 19). */
  private def rows(path: String): List[Array[String]] =
    Using.resource(Source.fromFile(path, "UTF-8"))(_.getLines().drop(1).filter(_.nonEmpty).map(_.split(",", -1)).toList)

  def main(args: Array[String]): Unit =
    val today = LocalDate.parse("2026-10-17")
    val (customers, badCustomers) =
      Validate.partition(rows("data/customers.csv").map(c => RawCustomer(c(0), c(1), c(2))))(Validate.customer)
    val (orders, badOrders) =
      Validate.partition(rows("data/orders.csv").map(c => RawOrder(c(0), c(1), c(2), c(3), c(4), c(5), c(6))))(Validate.order)

    println(s"== Validation: ${customers.size} customer hợp lệ, ${orders.size} dòng đơn hàng hợp lệ")
    (badCustomers ++ badOrders).foreach((raw, errors) => println(s"  loại $raw -> ${errors.toList.mkString("; ")}"))

    val known  = customers.map(_.customerId).toSet
    val report = OrderRules.check(orders, known, today)
    println("== Data quality")
    println(report.render)
    val blocking = report.blocking(maxErrorRate = 0.10)
    println(s"== Quality gate (Error > 10%): ${if blocking.isEmpty then "QUA" else s"CHẶN — ${blocking.map(_.rule).mkString(", ")}"}")

    val accepted = Clean.accept(orders, known, today)
    println(s"== Doanh thu PAID theo (segment, ngày) trên ${accepted.size} dòng được chấp nhận")
    Reports.revenueBySegmentDay(accepted, customers.map(c => c.customerId -> c).toMap).toList.sortBy(_._1.toString).foreach {
      case ((seg, day), s) => println(f"  $seg%-9s $day  dòng hàng=${s.count}%d  tổng=${s.sum}%,d  min=${s.min}%,d  max=${s.max}%,d")
    }

    println("== Trung bình của các trung bình (SAI) vs trung bình đúng")
    val parts = List(List(100L, 100L, 100L, 100L), List(1000L))
    println(s"  dữ liệu chia 2 phần: $parts")
    println(s"  trung bình của trung bình = ${Aggregate.averageOfAverages(parts)}")
    println(s"  trung bình đúng (sum/count) = ${parts.flatten.map(Stats.of).reduce(cats.Monoid[Stats].combine).mean.get}")
