package scalaeng.solutions

import munit.FunSuite

class Ch05SolutionsSuite extends FunSuite:
  import Ch05Solutions.*

  test("bài 1"):
    assertEquals(highest(List(Priority.Low, Priority.High, Priority.Medium)), Priority.High)
    intercept[IllegalArgumentException](highest(Nil))

  test("bài 2: copy không đổi đối tượng gốc"):
    val a = Address("12 Lê Lợi", "Bến Nghé", "Quận 1", "TP.HCM")
    val b = a.copy(ward = "Bến Thành")
    assertEquals(a.ward, "Bến Nghé")
    assertEquals(b.full, "12 Lê Lợi, Bến Thành, Quận 1, TP.HCM")

  test("bài 3"):
    val policies: List[ShippingPolicy] = List(FlatRate(30_000), FreeOver(500_000, 30_000))
    assertEquals(policies.map(_.fee(BigDecimal(500_000))), List(BigDecimal(30_000), BigDecimal(0)))
    assertEquals(FreeOver(500_000, 30_000).fee(BigDecimal(499_999)), BigDecimal(30_000))

  test("bài 4"):
    val inv = Inventory(Map("A" -> 5))
    assert(inv.reserve("A", 3))
    assert(!inv.reserve("A", 3))
    assert(!inv.reserve("A", 0))
    assert(!inv.reserve("B", 1))
    assertEquals(inv.available("A"), 2)

  test("bài 5: toàn bộ bảng chuyển trạng thái"):
    import Status.*
    assertEquals(allowedNext(Pending), Set(Paid, Cancelled))
    assertEquals(allowedNext(Paid), Set(Shipped, Cancelled))
    assertEquals(allowedNext(Shipped), Set(Refunded))
    assertEquals(allowedNext(Cancelled), Set.empty[Status])
    assertEquals(allowedNext(Refunded), Set.empty[Status])
