package vn.scalaeng.p1

import munit.FunSuite
import vn.scalaeng.p1.model.*

class ValuesSuite extends FunSuite:
  test("Vnd: các định dạng được chấp nhận"):
    for (raw, expected) <- List("120000" -> 120000L, "1.250.000" -> 1250000L, "1,250,000" -> 1250000L,
                                "99,000 VND" -> 99000L, "50.000đ" -> 50000L, " 7 " -> 7L) do
      assertEquals(Vnd.parse(raw).map(_.dong), Right(expected), raw)

  test("Vnd: các định dạng bị từ chối"):
    for raw <- List("abc", "0", "-5", "1.000,000", "12.5", "1.00", "0120", "", "1,2345", "99999999999") do
      assert(Vnd.parse(raw).isLeft, raw)

  test("Quantity: 1..1000, chỉ chữ số"):
    assertEquals(Quantity.parse(" 3 ").map(_.count), Right(3))
    for raw <- List("0", "1001", "+3", "-1", "1.5", "", "x") do assert(Quantity.parse(raw).isLeft, raw)

  test("Email chuẩn hóa và kiểm tra"):
    assertEquals(Email.parse(" An.Nguyen@Example.COM ").map(_.value), Right("an.nguyen@example.com"))
    for raw <- List("a@@b.vn", "@b.vn", "a@b", "a@.vn", "a@b.", "a b@c.vn", "a@b..vn") do assert(Email.parse(raw).isLeft, raw)

  test("Phone: chuẩn hóa về 10 chữ số"):
    for raw <- List("+84 912 345 678", "84912345678", "0912.345.678", "(091) 234-5678") do
      assertEquals(Phone.parse(raw).map(_.value).map(_.length), Right(10), raw)
    assertEquals(Phone.parse("+84 912 345 678").map(_.value), Right("0912345678"))
    for raw <- List("12345", "0912 345 67", "(028) 3822-1234", "091234567x") do assert(Phone.parse(raw).isLeft, raw)

  test("City: alias, khoảng trắng và Unicode NFC"):
    assertEquals(City.parse("TP HCM").map(_.value), Right("Hồ Chí Minh"))
    assertEquals(City.parse("  ha   noi ").map(_.value), Right("Hà Nội"))
    val nfd = java.text.Normalizer.normalize("Hà Nội", java.text.Normalizer.Form.NFD)
    assert(nfd != "Hà Nội")
    assertEquals(City.parse(nfd).map(_.value), Right("Hà Nội"))
    assertEquals(City.parse("  Cần   Thơ ").map(_.value), Right("Cần Thơ"))
    assert(City.parse("   ").isLeft)

  test("Id và status chuẩn hóa chữ hoa"):
    assertEquals(OrderId.parse("ord-20").map(_.value), Right("ORD-20"))
    assertEquals(OrderStatus.parse("paid"), Right(OrderStatus.Paid))
    assert(OrderStatus.parse("SHIPPED").isLeft)
    assert(Sku.parse("GIAY,DEN").isLeft)
