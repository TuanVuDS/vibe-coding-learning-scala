package vn.scalaeng.p2.domain

class CoreSuite extends munit.FunSuite:
  test("merge chặn tràn Int và dòng trùng vượt quota") {
    assert(Checkout.merge(List(Line(1,Int.MaxValue),Line(1,Int.MaxValue),Line(1,3))).isLeft)
    assert(Checkout.merge(List(Line(1,600),Line(1,500))).isLeft)
    assertEquals(Checkout.merge(List(Line(1,2),Line(1,3))),Right(Map(1L -> 5)))
  }
  test("giá server và tràn Long") {
    val p = Product(1,"P","Bút",Long.MaxValue,3,true)
    assert(Checkout.price(Map(1L -> 2),List(p)).isLeft)
    assertEquals(Checkout.price(Map(1L -> 2),List(p.copy(price=10))).map(_._1.minor),Right(20L))
  }
  test("không đủ kho, sản phẩm xóa mềm và đơn rỗng") {
    assert(Checkout.merge(Nil).isLeft)
    val p = Product(1,"P","Bút",10,1,true)
    assertEquals(Checkout.price(Map(1L -> 2),List(p)),Left(DomainError.OutOfStock))
    assertEquals(Checkout.price(Map(1L -> 1),List(p.copy(active=false))),Left(DomainError.MissingProduct))
  }
  test("validation tích lũy, sort whitelist và giới hạn phân trang") {
    assert(Validation.user("", "sai").isLeft)
    assert(Validation.query(None,"id;drop table users",0,10).isLeft)
    assert(PageRequest.from(-1,101).isLeft)
    assert(Checkout.product(ProductDraft("bad sku","",0,-1)).isLeft)
  }
  test("transition idempotent, không giao lại đơn hủy") {
    assert(OrderStatus.Pending.canMoveTo(OrderStatus.Pending))
    assert(OrderStatus.Pending.canMoveTo(OrderStatus.Cancelled))
    assert(!OrderStatus.Cancelled.canMoveTo(OrderStatus.Paid))
    assert(!OrderStatus.Paid.canMoveTo(OrderStatus.Cancelled))
  }
