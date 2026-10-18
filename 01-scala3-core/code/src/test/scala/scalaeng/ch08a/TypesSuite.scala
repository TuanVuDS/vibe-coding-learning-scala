package scalaeng.ch08a

import munit.FunSuite
import Identifiers.*
import Money.*
import StringSyntax.*

class TypesSuite extends FunSuite:

  test("opaque UserId: smart constructor"):
    assertEquals(UserId.parse(" U-42 ").map(_.value), Right("U-42"))
    assert(UserId.parse("U-").isLeft)
    assert(UserId.parse("u-1").isLeft)
    assert(OrderId.parse("ORD-1").isRight)
    assert(OrderId.parse("ORD-").isLeft)

  test("opaque Vnd: phép toán và định dạng"):
    val t = Vnd.unsafe(120_000) * 3 + Vnd.unsafe(30_000)
    assertEquals(t.dong, 390_000L)
    assertEquals(t.format, "390,000 VND")
    assert(Vnd(-1).isLeft)
    assert(t >= Vnd.Zero)

  test("extension method trên String"):
    assertEquals(" A@B.VN ".normalizeEmail, "a@b.vn")
    assert("   ".isBlankOrEmpty)

  test("Page: phân trang và map"):
    val p = Page.paginate((1 to 7).toList, page = 3, pageSize = 3)
    assertEquals(p.items, List(7))
    assertEquals(p.totalPages, 3L)
    assert(!p.hasNext)
    assertEquals(p.map(_ * 10).items, List(70))
    assertEquals(Page.paginate(List.empty[Int], 1, 10).totalPages, 0L)
    assertEquals(Page.paginate((1 to 3).toList, 5, 10).items, Nil)

  test("upper bound: hàm generic cho mọi Entity"):
    val cs = List(Customer("C1", "A"), Customer("C2", "B"), Customer("C1", "C"))
    assertEquals(Repo.duplicateIds(cs), Set("C1"))
    assertEquals(Repo.indexById(List(Product("P1", "x", 1))).keySet, Set("P1"))

  test("variance: dispatch"):
    assertEquals(Variance.dispatch(Variance.orderSource, Variance.orderHandler), List("audit@100", "audit@200"))

  test("lower bound: prepend mở rộng kiểu"):
    val ne = NonEmpty(OrderPlaced("O", 1L), Nil).prepend(UserSignedUp("U", 0L))
    assertEquals(ne.toList.map(_.occurredAt), List(0L, 1L))

  test("union types"):
    assertEquals(Unions.timeoutSeconds(10), Right(10))
    assertEquals(Unions.timeoutSeconds("2m"), Right(120))
    assertEquals(Unions.timeoutSeconds("xs"), Left("sai: xs"))
    assertEquals(Unions.timeoutSeconds("1h"), Left("không hiểu đơn vị: 1h"))
    assertEquals(Unions.describe(RuleViolation("r")), "vi phạm: r")

  test("intersection types: latestById cho hai loại bảng"):
    val rows = List(CustomerRow("C1", "old", 1L), CustomerRow("C1", "new", 9L), CustomerRow("C0", "x", 1L))
    assertEquals(Intersections.latestById(rows).map(_.name), List("x", "new"))
    assertEquals(Intersections.latestById(List.empty[ProductRow]), Nil)

  test("type alias Validator"):
    val v = Aliases.validateAll(List(Aliases.requireEmail, Aliases.requireName))
    assertEquals(v(Map.empty), List("email thiếu hoặc sai", "name rỗng"))
