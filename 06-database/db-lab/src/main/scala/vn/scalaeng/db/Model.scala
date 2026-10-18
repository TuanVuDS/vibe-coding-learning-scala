package vn.scalaeng.db

import java.time.Instant

// Id là opaque type (Chương 8A): cùng biểu diễn Long trong DB, nhưng compiler không cho nhầm UserId với ProductId.
opaque type UserId = Long
object UserId:
  def apply(v: Long): UserId = v
  extension (id: UserId) def value: Long = id

opaque type ProductId = Long
object ProductId:
  def apply(v: Long): ProductId = v
  extension (id: ProductId) def value: Long = id

opaque type OrderId = Long
object OrderId:
  def apply(v: Long): OrderId = v
  extension (id: OrderId) def value: Long = id

final case class User(id: UserId, email: String, name: String, createdAt: Instant)
final case class Product(id: ProductId, sku: String, name: String, priceVnd: Long, stock: Int)

enum OrderStatus(val dbName: String):
  case Pending   extends OrderStatus("PENDING")
  case Paid      extends OrderStatus("PAID")
  case Shipped   extends OrderStatus("SHIPPED")
  case Cancelled extends OrderStatus("CANCELLED")

object OrderStatus:
  def fromDb(s: String): Either[String, OrderStatus] = values.find(_.dbName == s).toRight(s"trạng thái không hợp lệ trong DB: $s")

final case class Order(id: OrderId, userId: UserId, status: OrderStatus, totalVnd: Long, createdAt: Instant)
final case class OrderLine(productId: ProductId, quantity: Int)

enum CreateUserError:
  case DuplicateEmail(email: String)

enum PlaceOrderError:
  case EmptyOrder
  case UnknownUser(id: UserId)
  case UnknownProducts(ids: List[ProductId])
  case OutOfStock(id: ProductId, requested: Int, available: Int)
  case InvalidQuantity(id: ProductId, quantity: Long) // mỗi dòng 1..MaxQuantity; tổng một sản phẩm cũng vậy
  case TotalTooLarge                                    // tổng tiền vượt miền Long

/** Sắp xếp được phép — whitelist. Tên cột KHÔNG BAO GIỜ lấy trực tiếp từ input của client (mục 3.9). */
enum UserSort(val column: String):
  case ByName      extends UserSort("name")
  case ByCreatedAt extends UserSort("created_at")
