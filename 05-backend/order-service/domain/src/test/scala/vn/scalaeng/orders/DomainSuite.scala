package vn.scalaeng.orders

import munit.FunSuite

class DomainSuite extends FunSuite:
  test("id và Money từ chối 0/âm, giữ chính xác Long.MaxValue"):
    assert(UserId.from(0).isLeft)
    assert(OrderId.from(-1).isLeft)
    assert(Money.fromMinor(0).isLeft)
    assertEquals(Money.fromMinor(Long.MaxValue).map(_.minor), Right(Long.MaxValue))

  test("validation tích lũy lỗi và chuẩn hóa email bằng Locale.ROOT"):
    assertEquals(Validation.user(" ", "bad"), Left(DomainError.Invalid(List("name phải có 1..100 ký tự", "email không hợp lệ"))))
    assertEquals(Validation.user(" An ", " AN@EXAMPLE.COM "), Right(UserDraft("An", "an@example.com")))
    assertEquals(Validation.order(0, -1).left.toOption.collect { case DomainError.Invalid(es) => es.size }, Some(2))

  test("whitelist sort và ba lỗi query được giữ cùng lúc"):
    val result = Validation.query(None, "password", -1, 101)
    assertEquals(result.left.toOption.collect { case DomainError.Invalid(es) => es.size }, Some(3))
    assertEquals(Validation.query(Some(" AN "), "-name", 0, 10).map(q => (q.q, q.sort, q.descending)),
      Right((Some("an"), UserSort.Name, true)))

  test("mọi cặp transition: chỉ cạnh hợp lệ và retry cùng trạng thái"):
    val allowed = Set((OrderStatus.Pending, OrderStatus.Paid), (OrderStatus.Pending, OrderStatus.Cancelled),
      (OrderStatus.Paid, OrderStatus.Shipped))
    for from <- OrderStatus.values; to <- OrderStatus.values do
      assertEquals(from.canMoveTo(to), from == to || allowed.contains((from, to)))

  test("page chặn giới hạn và offset lớn để phép cộng không tràn Int"):
    assert(PageRequest.from(1_000_001, 20).isLeft)
    assert(PageRequest.from(0, 0).isLeft)
    assert(PageRequest.from(1_000_000, 100).isRight)
