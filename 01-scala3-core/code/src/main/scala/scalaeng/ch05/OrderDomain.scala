package scalaeng.ch05

/** Chương 5 — OOP trong Scala: domain model đơn hàng.
  *
  * Ở chương này lỗi nghiệp vụ được báo bằng exception (`require`).
  * Chương 7 sẽ thay bằng `Either` để lỗi trở thành dữ liệu.
  */

/** Enum đơn giản: tập giá trị hữu hạn, có method. */
enum OrderStatus:
  case Pending, Paid, Shipped, Cancelled

  /** Máy trạng thái: chuyển trạng thái nào là hợp lệ. */
  def canTransitionTo(next: OrderStatus): Boolean = (this, next) match
    case (Pending, Paid) | (Pending, Cancelled) => true
    case (Paid, Shipped) | (Paid, Cancelled)    => true
    case _                                      => false

/** Enum có tham số: mỗi case mang dữ liệu riêng (ADT, học sâu ở Chương 7). */
enum PaymentMethod:
  case Cod
  case Card(last4: String)
  case EWallet(provider: String)

  def label: String = this match
    case Cod               => "Thanh toán khi nhận hàng"
    case Card(last4)       => s"Thẻ ****$last4"
    case EWallet(provider) => s"Ví $provider"

/** Case class: dữ liệu bất biến, có equals/hashCode/toString/copy tự động.
  * `require` kiểm tra bất biến (invariant) ngay khi tạo.
  */
final case class Money(amount: BigDecimal):
  require(amount >= 0, s"Số tiền không được âm: $amount")
  def +(other: Money): Money = Money(amount + other.amount)
  def -(other: Money): Money = Money((amount - other.amount).max(0))
  def *(quantity: Int): Money = Money(amount * quantity)
  def >=(other: Money): Boolean = amount >= other.amount

/** Companion object: cùng tên, cùng file với class; chứa factory và hằng số. */
object Money:
  val Zero: Money = Money(BigDecimal(0))
  def vnd(value: Long): Money = Money(BigDecimal(value))

final case class OrderLine(sku: String, unitPrice: Money, quantity: Int):
  require(sku.nonEmpty, "SKU rỗng")
  require(quantity > 0, s"Số lượng phải > 0: $quantity")
  def total: Money = unitPrice * quantity

final case class Order(
    id: String,
    customerId: String,
    lines: List[OrderLine],
    status: OrderStatus,
    payment: PaymentMethod
):
  def subtotal: Money = Money(lines.map(_.total.amount).sum)

  /** Trả về Order MỚI — không sửa đối tượng cũ. */
  def transitionTo(next: OrderStatus): Order =
    require(status.canTransitionTo(next), s"Không thể chuyển $status -> $next")
    copy(status = next)

object Order:
  /** Smart constructor: chỗ duy nhất tạo đơn hàng mới, luôn ở trạng thái Pending. */
  def create(id: String, customerId: String, lines: List[OrderLine], payment: PaymentMethod): Order =
    require(lines.nonEmpty, "Đơn hàng phải có ít nhất 1 dòng")
    Order(id, customerId, lines, OrderStatus.Pending, payment)
