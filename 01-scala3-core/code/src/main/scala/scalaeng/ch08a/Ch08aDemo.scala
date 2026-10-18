package scalaeng.ch08a

import Identifiers.*
import Money.*
import StringSyntax.*

@main def ch08aDemo(): Unit =
  println(UserId.parse(" U-42 "))
  println(UserId.parse("42"))
  val uid = UserId.unsafe("U-7")
  println(s"value = ${uid.value}")

  val price = Vnd.unsafe(120_000)
  val total = price * 3 + Vnd.unsafe(30_000)
  println(s"Tổng: ${total.format}, Long bên dưới: ${total.dong}")
  println(Vnd(-5))

  println("  An.Nguyen@Example.COM ".normalizeEmail)

  val products = (1 to 7).toList.map(i => Product(s"P$i", s"Sản phẩm $i", i * 10_000L))
  val p2 = Page.paginate(products, page = 2, pageSize = 3)
  println(s"Trang ${p2.page}/${p2.totalPages}: ${p2.items.map(_.id)}, hasNext=${p2.hasNext}")
  val names: Page[String] = p2.map(_.name)
  println(names.items)
  println(Repo.duplicateIds(List(Customer("C1", "A"), Customer("C2", "B"), Customer("C1", "A2"))))

  println(Variance.dispatch(Variance.anySource, Variance.auditHandler))
  println(Variance.dispatch(Variance.orderSource, Variance.orderHandler))

  val ne: NonEmpty[OrderPlaced] = NonEmpty(OrderPlaced("ORD-9", 1L), Nil)
  val widened: NonEmpty[Event] = ne.prepend(UserSignedUp("U-1", 0L))
  println(widened.toList.map(_.getClass.getSimpleName))

  println(List[Unions.ImportError](ParseFailure("price", "abc"), RuleViolation("qty > 0")).map(Unions.describe))
  println(List[Int | String](45, "30s", "5m", "1h", "xs").map(Unions.timeoutSeconds))

  val rows = List(CustomerRow("C1", "An", 1L), CustomerRow("C1", "An Nguyễn", 5L), CustomerRow("C2", "Bình", 3L))
  println(Intersections.latestById(rows).map(r => (r.id, r.name)))
  println(Intersections.latestById(List(ProductRow("P1", "A", 2L), ProductRow("P1", "B", 1L))).map(_.sku))

  val check = Aliases.validateAll(List(Aliases.requireEmail, Aliases.requireName))
  println(check(Map("email" -> "a@b.vn", "name" -> "An")))
  println(check(Map("email" -> "ab.vn")))
