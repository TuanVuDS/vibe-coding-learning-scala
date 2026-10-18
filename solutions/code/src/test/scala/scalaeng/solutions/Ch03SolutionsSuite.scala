package scalaeng.solutions

import munit.FunSuite

class Ch03SolutionsSuite extends FunSuite:
  import Ch03Solutions.*

  test("bài 1"):
    assertEquals(describeProduct("Bút bi", BigDecimal(5000), 3), "Bút bi x3 = 15000")

  test("bài 2"):
    assert(isValidPhone("0912345678"))
    assert(isValidPhone(" 0912345678 "))
    assert(!isValidPhone("912345678"))
    assert(!isValidPhone("09123456789"))
    assert(!isValidPhone("09123x5678"))

  test("bài 3: làm tròn HALF_UP"):
    assertEquals(vatAmount(BigDecimal(12_345), BigDecimal("0.1")), BigDecimal(1_235))
    assertEquals(vatAmount(BigDecimal(12_344), BigDecimal("0.1")), BigDecimal(1_234))

  test("bài 4: boundary của status code"):
    assertEquals(classifyStatus(199), "informational")
    assertEquals(classifyStatus(200), "success")
    assertEquals(classifyStatus(404), "client-error")
    assertEquals(classifyStatus(503), "server-error")
    assertEquals(classifyStatus(600), "unknown")

  test("bài 5: dòng hợp lệ"):
    assertEquals(parseOrderLine(" SKU-001 , 120000 , 2 "), ("SKU-001", BigDecimal(120_000), 2))

  test("bài 5: dòng lỗi"):
    intercept[IllegalArgumentException](parseOrderLine("SKU-001,120000"))
    intercept[IllegalArgumentException](parseOrderLine("SKU-001,abc,2"))
    intercept[IllegalArgumentException](parseOrderLine("SKU-001,1000,x"))
    intercept[IllegalArgumentException](parseOrderLine("SKU-001,1000,0"))
    intercept[IllegalArgumentException](parseOrderLine(",1000,1"))
