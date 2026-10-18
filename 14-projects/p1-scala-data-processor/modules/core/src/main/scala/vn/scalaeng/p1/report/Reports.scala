package vn.scalaeng.p1.report

import vn.scalaeng.p1.csv.Csv
import vn.scalaeng.p1.json.Json
import vn.scalaeng.p1.json.Json.*
import vn.scalaeng.p1.pipeline.{ReportRow, Stats}
import vn.scalaeng.p1.model.Rejected

/** Output KHÔNG chứa thời điểm chạy -> chạy lại cho file giống hệt từng byte (idempotent). */
object Reports:
  def reportCsv(rows: List[ReportRow]): String =
    (Csv.renderRow(Seq("date", "city", "paid_orders", "items", "revenue_vnd")) ::
      rows.map(r => Csv.renderRow(Seq(r.date.toString, r.city.value, r.paidOrders.toString, r.items.toString, r.revenue.dong.toString))))
      .mkString("", "\n", "\n")

  def rejectedCsv(rs: List[Rejected]): String =
    (Csv.renderRow(Seq("source", "position", "issues", "raw")) ::
      rs.map(r => Csv.renderRow(Seq(r.source.toString.toLowerCase, r.position.toString, r.issues.map(_.render).mkString(" | "), r.raw))))
      .mkString("", "\n", "\n")

  def summaryJson(s: Stats): String =
    def n(i: Long): Json = JNum(BigDecimal(i))
    JObj(Vector(
      "order_rows" -> n(s.orderRows), "orders_valid" -> n(s.ordersValid),
      "orders_rejected" -> n(s.ordersRejected), "orders_superseded" -> n(s.ordersSuperseded),
      "user_items" -> n(s.userItems), "users_valid" -> n(s.usersValid), "users_rejected" -> n(s.usersRejected),
      "orphan_orders" -> n(s.orphanOrders), "paid" -> n(s.paid), "cancelled" -> n(s.cancelled), "refunded" -> n(s.refunded),
      "paid_revenue_vnd" -> n(s.paidRevenue), "balanced" -> JBool(s.balanced)
    )).render + "\n"
