package scalaeng.ch08a

/** Generics: một kiểu Page[A] dùng cho mọi loại tài nguyên của REST API. */
final case class Page[A](items: List[A], page: Int, pageSize: Int, total: Long):
  def totalPages: Long = if total == 0 then 0 else (total + pageSize - 1) / pageSize
  def hasNext: Boolean = page < totalPages
  /** map giữ nguyên thông tin phân trang, đổi kiểu phần tử: Page[A] => Page[B]. */
  def map[B](f: A => B): Page[B] = Page(items.map(f), page, pageSize, total)

object Page:
  def paginate[A](all: List[A], page: Int, pageSize: Int): Page[A] =
    require(page >= 1 && pageSize >= 1, "page và pageSize phải >= 1")
    Page(all.slice((page - 1) * pageSize, page * pageSize), page, pageSize, all.size.toLong)

/** Upper bound: A phải là con của Entity để có `id`. */
trait Entity:
  def id: String

final case class Product(id: String, name: String, price: Long) extends Entity
final case class Customer(id: String, name: String) extends Entity

object Repo:
  def indexById[A <: Entity](items: List[A]): Map[String, A] =
    items.map(a => a.id -> a).toMap

  /** Hàm generic tổng quát: tìm phần tử trùng id — dùng cho mọi Entity. */
  def duplicateIds[A <: Entity](items: List[A]): Set[String] =
    items.groupMapReduce(_.id)(_ => 1)(_ + _).collect { case (id, n) if n > 1 => id }.toSet

/** Variance với hệ thống event. */
sealed trait Event:
  def occurredAt: Long
final case class OrderPlaced(orderId: String, occurredAt: Long) extends Event
final case class UserSignedUp(userId: String, occurredAt: Long) extends Event

/** Covariant (+A): một nguồn OrderPlaced CŨNG LÀ một nguồn Event. */
trait Source[+A]:
  def poll(): List[A]

/** Contravariant (-A): một handler xử lý được mọi Event thì CŨNG xử lý được OrderPlaced. */
trait Handler[-A]:
  def handle(a: A): String

object Variance:
  val orderSource: Source[OrderPlaced] = () => List(OrderPlaced("ORD-1", 100L), OrderPlaced("ORD-2", 200L))

  // Hợp lệ nhờ covariance: Source[OrderPlaced] <: Source[Event]
  val anySource: Source[Event] = orderSource

  val auditHandler: Handler[Event] = e => s"audit@${e.occurredAt}"

  // Hợp lệ nhờ contravariance: Handler[Event] <: Handler[OrderPlaced]
  val orderHandler: Handler[OrderPlaced] = auditHandler

  def dispatch[A](source: Source[A], handler: Handler[A]): List[String] =
    source.poll().map(handler.handle)

/** Lower bound: thêm phần tử kiểu "rộng hơn" vào danh sách bất biến covariant. */
final case class NonEmpty[+A](head: A, tail: List[A]):
  def prepend[B >: A](b: B): NonEmpty[B] = NonEmpty(b, head :: tail)
  def toList: List[A] = head :: tail
