package scalaeng.solutions

/** Lời giải bài tập Chương 5. */
object Ch05Solutions:

  // Bài 1: enum có tham số
  enum Priority(val weight: Int):
    case Low    extends Priority(1)
    case Medium extends Priority(5)
    case High   extends Priority(10)

  def highest(ps: List[Priority]): Priority =
    require(ps.nonEmpty, "danh sách rỗng")
    ps.maxBy(_.weight)

  // Bài 2: case class + copy
  final case class Address(street: String, ward: String, district: String, city: String):
    def full: String = List(street, ward, district, city).filter(_.nonEmpty).mkString(", ")

  // Bài 3: trait + hai cài đặt
  trait ShippingPolicy:
    def fee(subtotal: BigDecimal): BigDecimal

  final case class FlatRate(amount: BigDecimal) extends ShippingPolicy:
    def fee(subtotal: BigDecimal): BigDecimal = amount

  final case class FreeOver(threshold: BigDecimal, amount: BigDecimal) extends ShippingPolicy:
    def fee(subtotal: BigDecimal): BigDecimal = if subtotal >= threshold then BigDecimal(0) else amount

  // Bài 4: đóng gói trạng thái mutable
  final class Inventory(initial: Map[String, Int]):
    private val stock = scala.collection.mutable.Map.from(initial)
    def available(sku: String): Int = stock.getOrElse(sku, 0)
    def reserve(sku: String, qty: Int): Boolean =
      if qty <= 0 || available(sku) < qty then false
      else
        stock.update(sku, available(sku) - qty)
        true

  // Bài 5: máy trạng thái mở rộng, allowedNext suy ra từ canTransitionTo
  enum Status:
    case Pending, Paid, Shipped, Cancelled, Refunded
    def canTransitionTo(next: Status): Boolean = (this, next) match
      case (Pending, Paid) | (Pending, Cancelled) => true
      case (Paid, Shipped) | (Paid, Cancelled)    => true
      case (Shipped, Refunded)                    => true
      case _                                      => false

  def allowedNext(s: Status): Set[Status] = Status.values.filter(s.canTransitionTo).toSet
