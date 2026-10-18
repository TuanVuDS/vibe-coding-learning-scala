package scalaeng.ch08b

import java.time.LocalDate
import CsvSyntax.*

@main def ch08bDemo(): Unit =
  val reports = List(
    OrderReport("ORD-1", "Nguyễn Văn An", BigDecimal("495000"), Some(LocalDate.of(2026, 10, 1))),
    OrderReport("ORD-2", "Công ty \"ABC\", chi nhánh HN", BigDecimal("1250000.50"), None)
  )
  println(CsvWriter.write(reports))
  println(reports.head.toCsvRow)

  import CsvInstances.given
  println(CsvWriter.write(List(Option(1), None, Some(3))))

  val batchA = List(Stats.of(35), Stats.of(120))
  val batchB = List(Stats.of(900), Stats.of(48))
  val partial = List(batchA.combineAll, batchB.combineAll)
  val total = partial.combineAll
  println(s"Gộp từng lô rồi gộp tiếp: $total, avg=${total.avg}")
  println(s"Gộp một lần:            ${(batchA ++ batchB).combineAll}")
  println(List(Map("/a" -> 2, "/b" -> 1), Map("/a" -> 3, "/c" -> 7)).combineAll)
  println(List.empty[Int].combineAll)

  given RequestContext = RequestContext("req-123", Some("U-42"))
  given Clock = Clock.fixed(1_700_000_000_000L)
  OrderService.placeOrder("ORD-9").foreach(println)

  val products = List(Product("A", 300, 4.5), Product("B", 100, 4.8), Product("C", 200, 4.8))
  println(products.sorted(using ProductOrderings.byPriceAsc).map(_.sku))
  println(products.sorted(using ProductOrderings.byRatingDescThenPrice).map(_.sku))
  println(products.maxBy(_.rating).sku)
