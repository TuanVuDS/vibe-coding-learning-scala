package scalaeng.ch05

/** Trait như interface: hợp đồng hành vi. */
trait DiscountPolicy:
  def name: String
  def discount(order: Order): Money

object NoDiscount extends DiscountPolicy:
  val name = "Không giảm giá"
  def discount(order: Order): Money = Money.Zero

final class PercentageDiscount(percent: Int) extends DiscountPolicy:
  require(percent >= 0 && percent <= 100, s"percent ngoài [0, 100]: $percent")
  val name = s"Giảm $percent%"
  def discount(order: Order): Money =
    Money((order.subtotal.amount * percent / 100).setScale(0, BigDecimal.RoundingMode.DOWN))

final class MinSpendDiscount(minSpend: Money, off: Money) extends DiscountPolicy:
  val name = s"Giảm ${off.amount} cho đơn từ ${minSpend.amount}"
  def discount(order: Order): Money =
    if order.subtotal >= minSpend then off else Money.Zero

/** Abstract class: có constructor parameter và trạng thái chung cho lớp con. */
abstract class Notifier(val channel: String):
  protected def format(to: String, message: String): String
  /** Trả về nội dung đã gửi để dễ test (gửi thật là side effect — Chương 7). */
  final def send(to: String, message: String): String = s"[$channel] ${format(to, message)}"

final class EmailNotifier extends Notifier("email"):
  protected def format(to: String, message: String): String = s"To: $to | $message"

final class SmsNotifier(maxLength: Int = 160) extends Notifier("sms"):
  protected def format(to: String, message: String): String =
    s"$to: ${message.take(maxLength)}"

/** Trait có method cụ thể, dùng làm mixin. */
trait Auditable:
  def createdBy: String
  def auditLabel: String = s"tạo bởi $createdBy"

/** Class thường với trạng thái mutable được ĐÓNG GÓI (encapsulation). */
final class RequestCounter:
  private var count = 0
  def increment(): Unit = count += 1
  def current: Int = count

/** Composition: CheckoutService KHÔNG kế thừa policy/notifier mà NHẬN chúng qua constructor. */
final class CheckoutService(policy: DiscountPolicy, notifier: Notifier) extends Auditable:
  val createdBy = "checkout-service"

  def checkout(order: Order, contact: String): (Money, String) =
    val payable = order.subtotal - policy.discount(order)
    val receipt = notifier.send(
      contact,
      s"Đơn ${order.id}: ${payable.amount} VND (${policy.name}, ${order.payment.label})"
    )
    (payable, receipt)

@main def ch05Demo(): Unit =
  val order = Order.create(
    id = "ORD-001",
    customerId = "C-42",
    lines = List(
      OrderLine("AO-01", Money.vnd(120_000), 2),
      OrderLine("TAT-02", Money.vnd(85_000), 3)
    ),
    payment = PaymentMethod.Card("4242")
  )
  println(order.status)
  println(s"Tạm tính: ${order.subtotal.amount}")

  val paid = order.transitionTo(OrderStatus.Paid)
  println(s"Sau thanh toán: ${paid.status}; đơn gốc vẫn là: ${order.status}")
  println(s"Pending -> Shipped hợp lệ? ${OrderStatus.Pending.canTransitionTo(OrderStatus.Shipped)}")
  println(s"Các trạng thái: ${OrderStatus.values.mkString(", ")}")
  println(s"Parse từ chuỗi: ${OrderStatus.valueOf("Paid")}")

  val policies = List(NoDiscount, PercentageDiscount(10), MinSpendDiscount(Money.vnd(400_000), Money.vnd(50_000)))
  for p <- policies do
    val (payable, receipt) = CheckoutService(p, EmailNotifier()).checkout(order, "an@example.com")
    println(s"${p.name}: ${payable.amount}")
    println(s"  $receipt")

  val sms = CheckoutService(NoDiscount, SmsNotifier(maxLength = 20))
  println(sms.checkout(order, "0912345678")._2)
  println(sms.auditLabel)

  val counter = RequestCounter()
  (1 to 3).foreach(_ => counter.increment())
  println(s"Counter: ${counter.current}")

  println(order.lines.head)
  println(order.lines.head == OrderLine("AO-01", Money.vnd(120_000), 2))
  try OrderLine("X", Money.vnd(1), 0)
  catch case e: IllegalArgumentException => println(s"Lỗi: ${e.getMessage}")
