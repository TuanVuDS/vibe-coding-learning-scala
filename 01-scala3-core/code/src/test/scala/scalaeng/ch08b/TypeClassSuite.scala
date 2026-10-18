package scalaeng.ch08b

import munit.FunSuite
import java.time.LocalDate
import CsvSyntax.*

class TypeClassSuite extends FunSuite:

  test("escape theo RFC 4180"):
    assertEquals(CsvEncoder.escape("abc"), "abc")
    assertEquals(CsvEncoder.escape("a,b"), "\"a,b\"")
    assertEquals(CsvEncoder.escape("say \"hi\""), "\"say \"\"hi\"\"\"")

  test("instance trong companion được tìm tự động"):
    val r = OrderReport("O1", "An", BigDecimal("10.50"), None)
    assertEquals(r.toCsvRow, "O1,An,10.50,")
    assertEquals(CsvWriter.write(List(r)), "order_id,customer,total,paid_on\nO1,An,10.50,")

  test("BigDecimal không dùng ký hiệu khoa học"):
    import CsvInstances.given
    assertEquals(BigDecimal("1E+3").toCsvRow, "1000")

  test("conditional given cho Option"):
    import CsvInstances.given
    assertEquals(Option(LocalDate.of(2026, 1, 2)).toCsvRow, "2026-01-02")
    assertEquals(Option.empty[Int].toCsvRow, "")

  test("Monoid: gộp từng phần = gộp một lần (associativity)"):
    val xs = List(5L, 1L, 9L, 3L).map(Stats.of)
    val parts = xs.grouped(2).toList.map(_.combineAll)
    assertEquals(parts.combineAll, xs.combineAll)
    assertEquals(xs.combineAll, Stats(4, 18, 9))

  test("Monoid Map gộp theo key"):
    assertEquals(List(Map("a" -> 1), Map("a" -> 2, "b" -> 3)).combineAll, Map("a" -> 3, "b" -> 3))
    assertEquals(List.empty[Map[String, Int]].combineAll, Map.empty[String, Int])

  test("using: context được truyền qua nhiều tầng"):
    given RequestContext = RequestContext("r-1", None)
    given Clock = Clock.fixed(5L)
    assertEquals(OrderService.placeOrder("O"), List("5 [r-1] user=anonymous validate O", "5 [r-1] user=anonymous place O"))

  test("Ordering tự định nghĩa"):
    val ps = List(Product("A", 300, 4.5), Product("B", 100, 4.8), Product("C", 50, 4.8))
    assertEquals(ps.sorted(using ProductOrderings.byRatingDescThenPrice).map(_.sku), List("C", "B", "A"))
