package vn.scalaeng.solutions.ch20

import cats.Monoid
import cats.syntax.all.*
import java.time.LocalDate
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import vn.scalaeng.transform.*

class Ch20SolutionsSuite extends ScalaCheckSuite:
  private val d15 = LocalDate.parse("2026-10-15")
  private val d16 = LocalDate.parse("2026-10-16")
  private def o(id: String, sku: String, rev: Long, day: LocalDate = d15, st: Status = Status.Paid, cust: String = "C1") =
    Order(id, cust, day, st, sku, 1, rev)

  test("bài 1 (review M5): đếm theo ĐƠN — một đơn 10 sku Paid + một đơn Refunded = 50%, không phải 9%"):
    val lines = (1 to 10).toList.map(i => o("O1", s"S$i", 1)) :+ o("O2", "S1", 1, st = Status.Refunded)
    assertEquals(RefundRateRule(0.20).run(lines).failed, 1)
    List(Double.NaN, 2.0).foreach(x => intercept[IllegalArgumentException](RefundRateRule(x)))

  test("bài 1: ngày có tỷ lệ hoàn tiền > 20% bị đánh dấu"):
    val orders = List(o("1", "A", 1), o("2", "A", 1), o("3", "A", 1, st = Status.Refunded), o("4", "A", 1, day = d16))
    val r      = RefundRateRule(0.20).run(orders)
    assertEquals((r.checked, r.failed, r.examples), (2, 1, List("2026-10-15:33%")))

  property("bài 2: Monoid[Mean] kết hợp; trung bình không phụ thuộc cách chia"):
    forAll(Gen.listOf(Gen.choose(-1000, 1000)), Gen.choose(0, 50)) { (xs, cut) =>
      val all   = xs.map(x => Mean.of(BigDecimal(x))).combineAll
      val parts = List(xs.take(cut), xs.drop(cut)).map(_.map(x => Mean.of(BigDecimal(x))).combineAll).combineAll
      all == parts && (Monoid[Mean].empty |+| all) == all
    }

  test("bài 3: dedup tổng quát giữ bản cập nhật MỚI NHẤT, không phụ thuộc thứ tự"):
    val updates = List(CustomerUpdate("C1", "Huế", 2), CustomerUpdate("C1", "Hà Nội", 5), CustomerUpdate("C2", "TP.HCM", 1),
      CustomerUpdate("C1", "Đà Nẵng", 3))
    val expected = Set(CustomerUpdate("C1", "Hà Nội", 5), CustomerUpdate("C2", "TP.HCM", 1))
    assertEquals(Versioned.dedupLatest(updates).latest.toSet, expected)
    assertEquals(Versioned.dedupLatest(updates.reverse).latest.toSet, expected)

  test("bài 3 (review M6): cùng phiên bản, khác nội dung = xung đột ở MỌI hoán vị; giống nhau = một bản"):
    val tie = List(CustomerUpdate("C1", "HN", 1), CustomerUpdate("C1", "Hue", 1), CustomerUpdate("C2", "Vinh", 1), CustomerUpdate("C2", "Vinh", 1))
    tie.permutations.foreach { p =>
      val r = Versioned.dedupLatest(p)
      assertEquals((r.latest, r.conflicts), (List(CustomerUpdate("C2", "Vinh", 1)), List("C1")))
    }

  test("bài 4: top-K theo partition rồi gộp cho SAI; gộp theo khóa trước cho ĐÚNG"):
    // "C" có tổng cao nhất (3 × 40 = 120) nhưng không đứng đầu ở MỌI partition chứa nó (p1, p2) → bị cắt mất một phần.
    val p1 = List(o("1", "A", 100), o("2", "C", 40))
    val p2 = List(o("3", "B", 90), o("4", "C", 40))
    val p3 = List(o("5", "A", 10), o("6", "C", 40))
    assertEquals(TopK.right(List(p1, p2, p3), 1), List("C" -> BigInt(120)))
    // Bản sai: top-1 từng partition là A=100, B=90, C=40 → "A" thắng với 100 (10 của A ở p3 cũng bị cắt mất).
    assertEquals(TopK.wrong(List(p1, p2, p3), 1), List("A" -> BigInt(100)))

  test("bài 4 (review M1): tổng vượt Long không tràn thành số âm"):
    val big = o("1", "A", 9_000_000_000_000_000_000L - 1)
    assertEquals(TopK.right(List(List(big), List(big)), 1), List("A" -> BigInt(2) * (9_000_000_000_000_000_000L - 1)))

  test("bài 5: số user khác nhau — một user ở nhiều partition chỉ đếm một lần"):
    val p1 = List(o("1", "A", 1, cust = "C1"), o("2", "A", 1, cust = "C2"))
    val p2 = List(o("3", "A", 1, cust = "C1"), o("4", "A", 1, cust = "C3", day = d16))
    assertEquals(DistinctUsers.perDay(List(p1, p2)), Map(d15 -> 2, d16 -> 1))

  test("bài 6: gate chặn thì không có báo cáo; nới ngưỡng thì báo cáo được xuất bản"):
    val customers = Map("C1" -> Customer("C1", Segment.Retail, "Hà Nội"))
    val raw = List(
      RawOrder("O1", "C1", "2026-10-15", "PAID", "KB-01", "2", "450000"),
      RawOrder("O2", "C9", "2026-10-15", "PAID", "KB-01", "1", "450000"), // khách không tồn tại: 1/3 = 33%
      RawOrder("O3", "C1", "2026-10-15", "PAID", "MS-01", "1", "250000"),
      RawOrder("O4", "C1", "bad-date", "PAID", "MS-01", "1", "250000")    // không qua validation
    )
    GatedPipeline.run(raw, customers, LocalDate.parse("2026-10-17"), maxErrorRate = 0.10) match
      case GatedPipeline.Outcome.Blocked(report, rejected) =>
        assertEquals(rejected, 1)
        assertEquals(report.blocking(0.10).map(_.rule), List("customer_id tồn tại"))
      case other => fail(s"phải bị chặn: $other")
    GatedPipeline.run(raw, customers, LocalDate.parse("2026-10-17"), maxErrorRate = 0.50) match
      case GatedPipeline.Outcome.Published(revenue, _, _) =>
        assertEquals(revenue.view.mapValues(_.sum).toMap, Map((Segment.Retail, LocalDate.parse("2026-10-15")) -> BigInt(1150000)))
      case other => fail(s"phải được xuất bản: $other")
