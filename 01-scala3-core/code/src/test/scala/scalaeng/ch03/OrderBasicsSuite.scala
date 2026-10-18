package scalaeng.ch03

import munit.FunSuite

class OrderBasicsSuite extends FunSuite:
  import OrderBasics.*

  test("shippingFee: dưới ngưỡng thì tính phí chuẩn"):
    assertEquals(shippingFee(BigDecimal(499_999)), BigDecimal(30_000))

  test("shippingFee: đúng ngưỡng thì miễn phí (boundary case)"):
    assertEquals(shippingFee(BigDecimal(500_000)), BigDecimal(0))

  test("quantityCategory phân loại theo khoảng"):
    assertEquals(quantityCategory(0), "invalid")
    assertEquals(quantityCategory(-5), "invalid")
    assertEquals(quantityCategory(1), "single")
    assertEquals(quantityCategory(9), "small")
    assertEquals(quantityCategory(10), "bulk")

  test("parseQuantity xử lý khoảng trắng và input lỗi"):
    assertEquals(parseQuantity(" 7 "), 7)
    assertEquals(parseQuantity("abc"), 0)
    assertEquals(parseQuantity(""), 0)

  test("subtotal functional và imperative cho cùng kết quả"):
    val items = List((BigDecimal(120_000), 2), (BigDecimal(85_000), 3))
    assertEquals(subtotal(items), BigDecimal(495_000))
    assertEquals(subtotalImperative(items), subtotal(items))

  test("giỏ hàng rỗng có tạm tính 0 và vẫn tính phí ship"):
    assertEquals(subtotal(Nil), BigDecimal(0))
    assertEquals(orderTotal(Nil), BigDecimal(30_000))

  test("formatVnd dùng dấu phân cách hàng nghìn"):
    assertEquals(formatVnd(BigDecimal(1_234_567)), "1,234,567 VND")

  test("invalidInput ném IllegalArgumentException"):
    intercept[IllegalArgumentException](invalidInput("bad"))
