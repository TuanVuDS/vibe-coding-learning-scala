package scalaeng.ch05

import munit.FunSuite

class OrderDomainSuite extends FunSuite:
  private val order = Order.create(
    "ORD-1", "C-1",
    List(OrderLine("A", Money.vnd(100_000), 2), OrderLine("B", Money.vnd(50_000), 1)),
    PaymentMethod.Cod
  )

  test("create luôn ở trạng thái Pending và tính subtotal"):
    assertEquals(order.status, OrderStatus.Pending)
    assertEquals(order.subtotal, Money.vnd(250_000))

  test("transitionTo trả về đối tượng mới, đối tượng cũ không đổi"):
    val paid = order.transitionTo(OrderStatus.Paid)
    assertEquals(paid.status, OrderStatus.Paid)
    assertEquals(order.status, OrderStatus.Pending)

  test("chuyển trạng thái không hợp lệ ném exception"):
    intercept[IllegalArgumentException](order.transitionTo(OrderStatus.Shipped))

  test("máy trạng thái đầy đủ"):
    import OrderStatus.*
    val allowed = for
      from <- OrderStatus.values.toList
      to   <- OrderStatus.values.toList
      if from.canTransitionTo(to)
    yield (from, to)
    assertEquals(allowed, List((Pending, Paid), (Pending, Cancelled), (Paid, Shipped), (Paid, Cancelled)))

  test("invariant: không tạo được dòng hàng số lượng 0, đơn rỗng, tiền âm"):
    intercept[IllegalArgumentException](OrderLine("A", Money.vnd(1), 0))
    intercept[IllegalArgumentException](Order.create("O", "C", Nil, PaymentMethod.Cod))
    intercept[IllegalArgumentException](Money(BigDecimal(-1)))

  test("Money - không âm"):
    assertEquals(Money.vnd(10) - Money.vnd(30), Money.Zero)

  test("case class so sánh theo giá trị"):
    assertEquals(OrderLine("A", Money.vnd(1), 1), OrderLine("A", Money.vnd(1), 1))

  test("PaymentMethod.label"):
    assertEquals(PaymentMethod.Card("1234").label, "Thẻ ****1234")
    assertEquals(PaymentMethod.EWallet("MoMo").label, "Ví MoMo")

  test("các DiscountPolicy"):
    assertEquals(NoDiscount.discount(order), Money.Zero)
    assertEquals(PercentageDiscount(10).discount(order), Money.vnd(25_000))
    assertEquals(MinSpendDiscount(Money.vnd(250_000), Money.vnd(30_000)).discount(order), Money.vnd(30_000))
    assertEquals(MinSpendDiscount(Money.vnd(250_001), Money.vnd(30_000)).discount(order), Money.Zero)
    intercept[IllegalArgumentException](PercentageDiscount(101))

  test("CheckoutService ghép policy và notifier"):
    val (payable, receipt) = CheckoutService(PercentageDiscount(10), EmailNotifier()).checkout(order, "a@b.c")
    assertEquals(payable, Money.vnd(225_000))
    assert(receipt.startsWith("[email] To: a@b.c"))

  test("SmsNotifier cắt độ dài nội dung"):
    assertEquals(SmsNotifier(maxLength = 3).send("09", "abcdef"), "[sms] 09: abc")

  test("RequestCounter đóng gói trạng thái"):
    val c = RequestCounter()
    c.increment(); c.increment()
    assertEquals(c.current, 2)
