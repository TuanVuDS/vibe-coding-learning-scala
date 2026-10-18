package scalaeng.ch07

import munit.FunSuite
import java.time.LocalDate

class OrderParsingSuite extends FunSuite:
  import OrderParsing.*, ParseError.*

  test("parseLine hợp lệ"):
    assertEquals(
      parseLine(1, "O1, S1, 1000, 2, 2026-01-31"),
      Right(OrderRecord("O1", "S1", BigDecimal(1000), 2, LocalDate.of(2026, 1, 31)))
    )

  test("parseLine trả lỗi cụ thể cho từng trường hợp"):
    assertEquals(parseLine(2, "O1, S1, 1000"), Left(WrongColumnCount(2, 3)))
    assertEquals(parseLine(3, "O1, S1, x, 2, 2026-01-01"), Left(InvalidNumber(3, "unit_price", "x")))
    assertEquals(parseLine(4, "O1, , 10, 2, 2026-01-01"), Left(EmptyField(4, "sku")))
    assertEquals(parseLine(5, "O1, S1, 10, 0, 2026-01-01"), Left(NonPositive(5, "quantity")))
    assertEquals(parseLine(6, "O1, S1, 10, 1, 2026-02-30"), Left(InvalidDate(6, "2026-02-30")))
    assertEquals(parseLine(7, "O1, S1, -5, 1, 2026-01-01"), Left(NonPositive(7, "unit_price")))

  test("fail-fast: chỉ báo lỗi đầu tiên"):
    assertEquals(parseLine(1, ", S1, x, 0, bad"), Left(EmptyField(1, "order_id")))

  test("parseAll không dừng ở dòng lỗi"):
    val (errors, records) = parseAll(sampleCsv)
    assertEquals(errors.size, 5)
    assertEquals(records.size, 4)

  test("dedup giữ bản ghi đầu tiên và báo id trùng"):
    val r = OrderRecord("A", "S", BigDecimal(1), 1, LocalDate.of(2026, 1, 1))
    val (kept, dups) = OrderPipeline.dedup(List(r, r.copy(sku = "X"), r.copy(orderId = "B"), r))
    assertEquals(kept.map(r => (r.orderId, r.sku)), List(("A", "S"), ("B", "S")))
    assertEquals(dups, List("A", "A"))

  test("pipeline end-to-end trên dữ liệu mẫu"):
    val report = OrderPipeline.run(sampleCsv)
    assertEquals(report.validCount, 3)
    assertEquals(report.duplicateIds, List("ORD-1"))
    assertEquals(
      report.revenueByDate,
      List(LocalDate.of(2026, 10, 1) -> BigDecimal(495_000), LocalDate.of(2026, 10, 2) -> BigDecimal(350_000))
    )

  test("pipeline chạy lại cho cùng kết quả (deterministic)"):
    assertEquals(OrderPipeline.run(sampleCsv), OrderPipeline.run(sampleCsv))

class CustomerLookupSuite extends FunSuite:
  import CustomerLookup.*
  private val db = Map(
    "C1" -> Customer("C1", "An", Some("an@Example.com"), None),
    "C2" -> Customer("C2", "Bình", None, Some("C1")),
    "C3" -> Customer("C3", "Chi", Some("bad"), Some("C9"))
  )

  test("emailDomain"):
    assertEquals(emailDomain(db, "C1"), Some("example.com"))
    assertEquals(emailDomain(db, "C2"), None)
    assertEquals(emailDomain(db, "C3"), None)
    assertEquals(emailDomain(db, "X"), None)

  test("referrerName"):
    assertEquals(referrerName(db, "C2"), "An")
    assertEquals(referrerName(db, "C3"), "(không có)")
    assertEquals(referrerName(db, "C1"), "(không có)")

class PuritySuite extends FunSuite:
  import Purity.*, Purity.WalletEvent.*, Purity.WalletError.*

  test("replay thành công"):
    assertEquals(replay(BigDecimal(0), List(Deposit(500), Withdraw(200))), Right(BigDecimal(300)))

  test("replay dừng ở lỗi đầu tiên"):
    assertEquals(
      replay(BigDecimal(0), List(Deposit(100), Withdraw(300), Deposit(-1))),
      Left(InsufficientFunds(BigDecimal(100), BigDecimal(300)))
    )

  test("số tiền không hợp lệ"):
    assertEquals(applyEvent(BigDecimal(10), Withdraw(0)), Left(InvalidAmount(BigDecimal(0))))

  test("danh sách rỗng giữ nguyên số dư"):
    assertEquals(replay(BigDecimal(7), Nil), Right(BigDecimal(7)))
