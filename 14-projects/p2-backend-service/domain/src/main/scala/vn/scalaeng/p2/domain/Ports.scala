package vn.scalaeng.p2.domain

/** Cổng lưu trữ: không import HTTP, JSON hay DB. Lỗi hạ tầng vẫn là failed IO.
  * createUser: email chuẩn hóa duy nhất. createOrder: kiểm tra user và insert nguyên tử.
  * updateStatus: đọc trạng thái, kiểm tra transition và ghi nguyên tử (kể cả idempotent retry).
  * getOrder: đọc snapshot, không lấy row lock; lockOrder: chỉ dành cho transaction chuyển trạng thái.
  * list: total và items thuộc cùng snapshot; sort luôn có id làm khóa phụ.
  */
trait OrderRepository[F[_]]:
  def createUser(draft: UserDraft): F[Either[DomainError, User]]
  def getUser(id: UserId): F[Option[User]]
  def listUsers(query: UserQuery): F[Page[User]]
  def createOrder(userId: UserId, total: Money): F[Either[DomainError, Order]]
  def getOrder(id: OrderId): F[Option[Order]]
  def listOrders(userId: UserId, page: PageRequest): F[Either[DomainError, Page[Order]]]
  def updateStatus(id: OrderId, next: OrderStatus): F[Either[DomainError, Order]]


trait Store[F[_]] extends OrderRepository[F]:
  def lockOrder(id: OrderId): F[Option[Order]]
  def account(email: String): F[Option[Account]]
  def accountById(id: UserId): F[Option[Account]]
  def register(draft: UserDraft, hash: String): F[Either[DomainError, User]]
  def revoked(jti: String): F[Boolean]
  def revoke(jti: String, expiresEpoch: Long): F[Unit]
  def product(id: Long): F[Option[Product]]
  def products(q: Option[String], descending: Boolean, page: PageRequest): F[Page[Product]]
  def saveProduct(id: Option[Long], draft: ProductDraft): F[Either[DomainError, Product]]
  def deleteProduct(id: Long): F[Boolean]
  def lockProducts(ids: List[Long]): F[List[Product]]
  def insertItems(id: OrderId, lines: List[PricedLine]): F[Unit]
  def decrement(lines: List[PricedLine]): F[Unit]
  def restore(id: OrderId): F[Unit]
