package scalaeng.solutions

import munit.FunSuite
import Ch08aSolutions.*

class Ch08aSolutionsSuite extends FunSuite:

  test("bài 1"):
    val r = Result(2, Nil).addWarning("w1").map(_ * 10)
    assertEquals(r, Result(20, List("w1")))

  test("bài 2"):
    import EmailType.*
    assertEquals(Email.parse(" An@Shop.VN ").map(_.domain), Right("shop.vn"))
    assert(Email.parse("a@b").isLeft)
    assert(Email.parse("a@b@c.vn").isLeft)
    assert(Email.parse("@b.vn").isLeft)
    assert(Email.parse("a@.vn").isLeft)

  test("bài 3"):
    import PercentType.*
    assertEquals(Percent.parse(15).map(_.applyTo(99_999)), Right(14_999L))
    assert(Percent.parse(101).isLeft)
    assert(Percent.parse(-1).isLeft)
    intercept[ArithmeticException](Percent.parse(100).map(_.applyTo(Long.MaxValue)))

  test("bài 4: giữ kiểu cụ thể"):
    final case class Row(id: String, updatedAt: Long) extends HasUpdatedAt
    val r: Option[Row] = maxByField(List(Row("a", 1), Row("b", 5)))
    assertEquals(r.map(_.id), Some("b"))
    assertEquals(maxByField(List.empty[Row]), None)

  test("bài 5: Pipeline[OrderPlaced] nối với Sink[Event]"):
    val p: Pipeline[OrderPlaced] = () => List(OrderPlaced("1"), OrderPlaced("2"))
    val s: Sink[Event] = e => s"saved $e"
    assertEquals(connect(p, s), List("saved OrderPlaced(1)", "saved OrderPlaced(2)"))
