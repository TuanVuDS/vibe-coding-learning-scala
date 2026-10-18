package vn.scalaeng.quality

import java.time.LocalDate

enum Severity:
  case Warn, Error

final case class CheckResult(name: String, severity: Severity, checked: Int, failed: Int, samples: List[String]):
  def passed: Boolean = failed == 0

/** Một kiểm tra chất lượng là DỮ LIỆU (Chương 20): tên, mức độ, hàm trên CẢ tập — vì uniqueness, toàn vẹn tham chiếu hay số dòng không kiểm
  * tra được trên từng dòng. Thư viện thật (Deequ, Great Expectations, dbt tests, Soda) làm đúng việc này ở quy mô lớn.
  */
final case class Check[A](name: String, severity: Severity, run: Seq[A] => CheckResult)

object Check:
  def row[A](name: String, sev: Severity)(bad: A => Boolean)(show: A => String): Check[A] =
    Check(name, sev, xs => { val b = xs.filter(bad); CheckResult(name, sev, xs.size, b.size, b.take(3).map(show).toList) })

  def notNull[A](name: String, sev: Severity)(field: A => Option[Any])(show: A => String): Check[A] = row[A](name, sev)(a => field(a).isEmpty)(show)

  def unique[A, K](name: String, sev: Severity)(key: A => K): Check[A] =
    Check(name, sev, xs => {
      val dups = xs.groupBy(key).filter(_._2.size > 1)
      CheckResult(name, sev, xs.size, dups.values.map(_.size).sum, dups.keys.take(3).map(_.toString).toList)
    })

  def references[A, K](name: String, sev: Severity, known: Set[K])(fk: A => K): Check[A] = row[A](name, sev)((a: A) => !known(fk(a)))(a => fk(a).toString)

  def inRange[A](name: String, sev: Severity)(value: A => BigInt, min: BigInt, max: BigInt)(show: A => String): Check[A] =
    row[A](name, sev)((a: A) => value(a) < min || value(a) > max)(show)

  /** Số dòng trong khoảng — bắt được việc MẤT dữ liệu mà không quy tắc từng dòng nào bắt được. */
  def rowCount[A](name: String, sev: Severity, min: Int, max: Int): Check[A] =
    Check(name, sev, xs => CheckResult(name, sev, xs.size, if xs.size < min || xs.size > max then 1 else 0,
      if xs.size < min || xs.size > max then List(s"${xs.size} dòng, kỳ vọng $min..$max") else Nil))

  /** Độ mới: ngày lớn nhất trong dữ liệu không được cũ hơn `asOf - maxLagDays` (dữ liệu "đứng" là lỗi im lặng). */
  def freshness[A](name: String, sev: Severity, asOf: LocalDate, maxLagDays: Int)(day: A => LocalDate): Check[A] =
    Check(name, sev, xs => {
      val newest = xs.map(day).maxOption
      val stale  = newest.forall(_.isBefore(asOf.minusDays(maxLagDays.toLong)))
      CheckResult(name, sev, xs.size, if stale then 1 else 0, if stale then List(s"mới nhất ${newest.getOrElse("không có")}, asOf $asOf") else Nil)
    })

object OutputChecks:
  def facts(customers: Set[String], asOf: LocalDate): List[Check[Fact]] = List(
    Check.unique[Fact, (String, String)]("fact: (order_id, sku) duy nhất", Severity.Error)(_.order.lineKey),
    Check.references[Fact, String]("fact: customer_id tồn tại", Severity.Warn, customers)(_.order.customerId),
    Check.inRange[Fact]("fact: doanh thu dòng 1..10 tỷ", Severity.Error)(_.revenue, 1, BigInt(10_000_000_000L))(f => f.order.orderId),
    Check.freshness[Fact]("fact: có dữ liệu trong 2 ngày gần nhất", Severity.Error, asOf, 2)(_.order.day)
  )

  def daily: List[Check[DailyRow]] = List(
    Check.unique[DailyRow, (LocalDate, String)]("daily: (day, segment) duy nhất", Severity.Error)(r => (r.day, r.segment)),
    Check.rowCount[DailyRow]("daily: 1..1000 dòng", Severity.Error, 1, 1000),
    Check.row("daily: orders <= order_lines", Severity.Error)((r: DailyRow) => r.orders > r.orderLines)(r => s"${r.day}/${r.segment}")
  )
