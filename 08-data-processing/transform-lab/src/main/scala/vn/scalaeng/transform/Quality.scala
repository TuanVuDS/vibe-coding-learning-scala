package vn.scalaeng.transform

import java.time.LocalDate

/** Data quality rule là DỮ LIỆU, không phải `if` rải rác: có tên, mức độ, và hàm kiểm tra trên CẢ tập dữ liệu
  * (nhiều quy tắc như "duy nhất" hay "tham chiếu tồn tại" không thể kiểm tra trên từng dòng riêng lẻ).
  */
enum Severity:
  case Warn, Error

final case class RuleResult(rule: String, severity: Severity, checked: Int, failed: Int, examples: List[String]):
  def failRate: Double = if checked == 0 then 0.0 else failed.toDouble / checked

final case class Rule[A](name: String, severity: Severity, run: List[A] => RuleResult)

object Rule:
  /** Quy tắc trên từng dòng: đếm số dòng vi phạm, giữ tối đa 3 ví dụ. */
  def row[A](name: String, severity: Severity)(ok: A => Boolean)(show: A => String): Rule[A] =
    Rule(name, severity, xs =>
      val bad = xs.filterNot(ok)
      RuleResult(name, severity, xs.size, bad.size, bad.take(3).map(show)))

  /** Duy nhất theo khóa: mỗi khóa xuất hiện quá một lần → mọi bản ghi của khóa đó đều tính là vi phạm. */
  def unique[A, K](name: String, severity: Severity)(key: A => K): Rule[A] =
    Rule(name, severity, xs =>
      val dups = xs.groupBy(key).filter(_._2.size > 1)
      RuleResult(name, severity, xs.size, dups.values.map(_.size).sum, dups.keys.take(3).map(_.toString).toList))

  /** Toàn vẹn tham chiếu: khóa ngoại phải có trong tập khóa của bảng kia. */
  def references[A, K](name: String, severity: Severity, known: Set[K])(fk: A => K): Rule[A] =
    row(name, severity)((a: A) => known.contains(fk(a)))(a => fk(a).toString)

final case class QualityReport(results: List[RuleResult]):
  /** Cổng chất lượng (quality gate): dừng pipeline nếu một quy tắc mức Error vượt ngưỡng tỷ lệ lỗi. */
  def blocking(maxErrorRate: Double): List[RuleResult] =
    QualityReport.requireRate(maxErrorRate)
    results.filter(r => r.severity == Severity.Error && r.failRate > maxErrorRate)

  def render: String =
    results.map(r => f"${r.rule}%-34s ${r.severity}%-5s ${r.failed}%4d/${r.checked}%-4d ${r.failRate * 100}%6.2f%%  ${r.examples.mkString(", ")}")
      .mkString("\n")

object QualityReport:
  /** Ngưỡng phải là số hữu hạn trong [0, 1]. `failRate > NaN` luôn false: ngưỡng NaN (đọc cấu hình lỗi) làm gate KHÔNG BAO GIỜ chặn
    * (review chéo, M4). Cấu hình sai là lỗi khởi động, không phải dữ liệu xấu → ném ngoại lệ ngay.
    */
  def requireRate(rate: Double): Unit =
    require(!rate.isNaN && rate >= 0.0 && rate <= 1.0, s"ngưỡng tỷ lệ lỗi phải trong [0, 1], nhận $rate")

object OrderRules:
  def all(customers: Set[String], today: LocalDate): List[Rule[Order]] = List(
    Rule.unique[Order, (String, String)]("(order_id, sku) duy nhất", Severity.Error)(o => (o.orderId, o.sku)),
    Rule.references[Order, String]("customer_id tồn tại", Severity.Error, customers)(_.customerId),
    Rule.row[Order]("ngày không ở tương lai", Severity.Error)(o => !o.day.isAfter(today))(o => s"${o.orderId}@${o.day}"),
    Rule.row[Order]("đơn giá <= 50 triệu", Severity.Warn)(_.unitPriceVnd <= 50_000_000L)(o => s"${o.orderId}:${o.unitPriceVnd}")
  )

  def check(orders: List[Order], customers: Set[String], today: LocalDate): QualityReport =
    QualityReport(all(customers, today).map(_.run(orders)))
