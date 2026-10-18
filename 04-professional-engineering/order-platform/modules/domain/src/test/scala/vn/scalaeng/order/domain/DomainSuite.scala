package vn.scalaeng.order.domain

import munit.FunSuite

class DomainSuite extends FunSuite:
  test("OrderId"):
    assert(OrderId.parse(" ORD-12 ").isRight)
    assert(OrderId.parse("ORD-").isLeft)
    assert(OrderId.parse("ord-1").isLeft)

  test("Vnd và Quantity"):
    val total = for
      p <- Vnd.parse("120000")
      q <- Quantity.parse("3")
    yield (p * q).dong
    assertEquals(total, Right(360000L))
    assert(Vnd.parse("0").isLeft)
    assert(Quantity.parse("-1").isLeft)
    assert(Quantity.parse("1.5").isLeft)

  test("Vnd tràn số báo lỗi thay vì ra số âm"):
    val big = Vnd.unsafe(Long.MaxValue)
    intercept[ArithmeticException](big + Vnd.unsafe(1))
