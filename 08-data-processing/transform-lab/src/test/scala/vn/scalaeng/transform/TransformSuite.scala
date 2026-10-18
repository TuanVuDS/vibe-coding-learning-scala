package vn.scalaeng.transform

import cats.data.Validated
import cats.syntax.all.*
import java.time.LocalDate
import munit.FunSuite

class TransformSuite extends FunSuite:
  private val today = LocalDate.parse("2026-10-17")

  test("Validated tích lũy MỌI lỗi độc lập của một dòng (5 lỗi)"):
    val r = Validate.order(RawOrder("O6", "", "2026-13-01", "SHIPPED", "", "1", "-5"))
    assertEquals(r.leftMap(_.toList.map(_.field)), Validated.invalid(List("customer_id", "day", "status", "sku", "unit_price_vnd")))

  test("dòng hợp lệ được chuẩn hóa: mã chữ hoa, enum không phân biệt hoa thường"):
    val r = Validate.order(RawOrder("O1", "c1", "2026-10-15", "paid", "kb-01", "2", "450000"))
    assertEquals(r, Validated.valid(Order("O1", "C1", LocalDate.parse("2026-10-15"), Status.Paid, "KB-01", 2, 450000L)))

  test("Either dừng ở lỗi ĐẦU, Validated gom TẤT CẢ — cùng hai kiểm tra"):
    val asEither    = (Left("lỗi 1"): Either[String, Int]).flatMap(_ => Left("lỗi 2"))
    val asValidated = ("lỗi 1".invalidNec[Int], "lỗi 2".invalidNec[Int]).mapN(_ + _)
    assertEquals(asEither, Left("lỗi 1"))
    assertEquals(asValidated.leftMap(_.toList), Validated.invalid(List("lỗi 1", "lỗi 2")))

  private val orders = List(
    Order("O1", "C1", LocalDate.parse("2026-10-15"), Status.Paid, "KB-01", 2, 450000),
    Order("O1", "C1", LocalDate.parse("2026-10-15"), Status.Paid, "MS-01", 1, 250000),
    Order("O2", "C2", LocalDate.parse("2026-10-15"), Status.Paid, "KB-01", 50, 400000),
    Order("O3", "C3", LocalDate.parse("2026-10-16"), Status.Refunded, "HP-01", 1, 990000),
    Order("O4", "C9", LocalDate.parse("2026-10-16"), Status.Paid, "KB-01", 1, 450000),
    Order("O7", "C1", LocalDate.parse("2026-10-16"), Status.Paid, "KB-01", 1, 60000000),
    Order("O2", "C2", LocalDate.parse("2026-10-15"), Status.Paid, "KB-01", 50, 400000),
    Order("O8", "C1", LocalDate.parse("2026-10-20"), Status.Paid, "MS-01", 1, 250000)
  )
  private val customers = Map(
    "C1" -> Customer("C1", Segment.Retail, "Hà Nội"),
    "C2" -> Customer("C2", Segment.Wholesale, "TP.HCM"),
    "C3" -> Customer("C3", Segment.Retail, "Đà Nẵng")
  )

  test("quality rule: số vi phạm khớp tính tay; quy tắc trên CẢ tập (duy nhất, tham chiếu)"):
    val report = OrderRules.check(orders, customers.keySet, today)
    assertEquals(report.results.map(r => (r.rule, r.failed, r.checked)), List(
      ("(order_id, sku) duy nhất", 2, 8),
      ("customer_id tồn tại", 1, 8),
      ("ngày không ở tương lai", 1, 8),
      ("đơn giá <= 50 triệu", 1, 8)
    ))

  test("quality gate: ngưỡng 10% chặn 3 quy tắc Error; ngưỡng 30% cho qua; Warn không bao giờ chặn"):
    val report = OrderRules.check(orders, customers.keySet, today)
    assertEquals(report.blocking(0.10).map(_.rule), List("(order_id, sku) duy nhất", "customer_id tồn tại", "ngày không ở tương lai"))
    assertEquals(report.blocking(0.30), Nil)
    assert(!report.blocking(0.0).exists(_.severity == Severity.Warn))

  test("GOLDEN: doanh thu PAID theo (segment, ngày) khớp tính tay"):
    val accepted = Clean.accept(orders, customers.keySet, today)
    // So CẢ bốn trường. Bản đầu chỉ so (count, sum) — mutation test (min sai) đi qua golden mà không bị phát hiện.
    val revenue  = Reports.revenueBySegmentDay(accepted, customers)
    assertEquals(accepted.map(o => (o.orderId, o.sku)), List(("O1", "KB-01"), ("O1", "MS-01"), ("O2", "KB-01"), ("O3", "HP-01"), ("O7", "KB-01")))
    assertEquals(revenue, Map(
      (Segment.Retail, LocalDate.parse("2026-10-15"))    -> Stats(2, BigInt(1150000), 250000L, 900000L),
      (Segment.Retail, LocalDate.parse("2026-10-16"))    -> Stats(1, BigInt(60000000), 60000000L, 60000000L),
      (Segment.Wholesale, LocalDate.parse("2026-10-15")) -> Stats(1, BigInt(20000000), 20000000L, 20000000L)
    ))

  test("trung bình của các trung bình phụ thuộc cách chia dữ liệu; (sum, count) thì không"):
    val data   = List(100L, 100L, 100L, 100L, 1000L)
    val split1 = List(List(100L, 100L, 100L, 100L), List(1000L))
    val split2 = List(List(100L, 100L), List(100L, 100L, 1000L))
    assertEquals(Aggregate.averageOfAverages(split1), BigDecimal(550))
    assertEquals(Aggregate.averageOfAverages(split2), BigDecimal(250))
    assertEquals(data.map(Stats.of).combineAll.mean, Some(BigDecimal(280)))

  // ---- Regression từ review chéo (Codex) ----
  test("M3: tổng BigInt — đặt ngoặc thế nào cũng cùng kết quả, kể cả quanh Long.MaxValue"):
    val (a, b, c) = (Stats.of(Long.MaxValue), Stats.of(1L), Stats.of(-1L))
    assertEquals((a |+| b) |+| c, a |+| (b |+| c))
    assertEquals(((a |+| b) |+| c).sum, BigInt(Long.MaxValue))
    assertEquals((a |+| b).sum, BigInt(Long.MaxValue) + 1) // không tràn

  test("M1: đơn giá vượt giới hạn nghiệp vụ bị loại ở Validate; tổng doanh thu không thể âm vì tràn số"):
    val huge = Validate.order(RawOrder("O9", "C1", "2026-10-15", "PAID", "KB-01", "2", Long.MaxValue.toString))
    assertEquals(huge.leftMap(_.toList.map(_.field)), Validated.invalid(List("unit_price_vnd")))
    val max = Validate.order(RawOrder("O9", "C1", "2026-10-15", "PAID", "KB-01", "1000", Validate.MaxUnitPriceVnd.toString)).toOption.get
    val rev = Reports.revenueBySegmentDay(List(max, max.copy(sku = "MS-01")), customers)
    assertEquals(rev.values.map(_.sum).toList, List(BigInt(2) * 1000 * Validate.MaxUnitPriceVnd))

  test("m1: trường null từ nguồn ngoài thành lỗi tích lũy, không NullPointerException"):
    val r = Validate.order(RawOrder(null, null, null, null, null, null, null))
    assertEquals(r.leftMap(_.length), Validated.invalid(7L))

  test("M4: ngưỡng gate không hợp lệ (NaN, âm, > 1) bị từ chối thay vì vô hiệu hóa gate"):
    val report = OrderRules.check(orders, customers.keySet, today)
    List(Double.NaN, -0.1, 1.5, Double.PositiveInfinity).foreach(x => intercept[IllegalArgumentException](report.blocking(x)))
    assertEquals(report.blocking(1.0), Nil)
