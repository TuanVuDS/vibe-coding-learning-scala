package vn.scalaeng.orders.solutions

import cats.effect.{IO, Ref}
import vn.scalaeng.orders.*

object LineTotal:
  // Bài 1: BigInt chỉ dùng khi tính trung gian; kết quả lưu trữ vẫn là Money/Long.
  def calculate(lines: List[(Long, Int)]): Either[String, Money] =
    if lines.isEmpty then Left("đơn hàng phải có ít nhất một dòng")
    else if lines.exists((price, quantity) => price <= 0 || quantity <= 0) then Left("giá và số lượng phải dương")
    else
      val total = lines.foldLeft(BigInt(0)) { case (sum, (price, quantity)) => sum + BigInt(price) * quantity }
      if !total.isValidLong then Left("tổng tiền vượt Long.MaxValue") else Money.fromMinor(total.toLong)

object OrderSearch:
  // Bài 2: lọc TRƯỚC phân trang; total đếm trên tập đã lọc.
  def page(all: List[Order], status: Option[OrderStatus], descending: Boolean, p: PageRequest): Page[Order] =
    val filtered = all.filter(o => status.forall(_ == o.status))
    val sorted = filtered.sortBy(o => (o.total.minor, o.id.value))
    val ordered = if descending then sorted.reverse else sorted
    Page(ordered.slice(p.offset, p.offset + p.limit), ordered.size, p.offset, p.limit)

final case class CursorPage(items: List[User], nextAfter: Option[Long])
object CursorPagination:
  // Bài 3: cursor = id tăng dần. Lấy dư một dòng để biết còn trang tiếp không.
  def page(all: List[User], after: Long, limit: Int): Either[String, CursorPage] =
    if after < 0 || limit < 1 || limit > 100 then Left("after >= 0 và limit trong 1..100")
    else
      val batch = all.filter(_.id.value > after).sortBy(_.id.value).take(limit + 1)
      val items = batch.take(limit)
      Right(CursorPage(items, Option.when(batch.size > limit)(items.last.id.value)))

final case class VersionedOrder(order: Order, version: Long)
enum UpdateError:
  case MissingOrder, PreconditionFailed, InvalidTransition

final class ConditionalOrders private (state: Ref[IO, Map[OrderId, VersionedOrder]]):
  // Bài 4: version check + transition + increment trong MỘT modify.
  def update(id: OrderId, expectedVersion: Long, next: OrderStatus): IO[Either[UpdateError, VersionedOrder]] =
    state.modify { all =>
      all.get(id) match
        case None => (all, Left(UpdateError.MissingOrder))
        case Some(v) if v.version != expectedVersion => (all, Left(UpdateError.PreconditionFailed))
        case Some(v) if !v.order.status.canMoveTo(next) => (all, Left(UpdateError.InvalidTransition))
        case Some(v) =>
          val updated = VersionedOrder(v.order.copy(status = next), Math.addExact(v.version, 1L))
          (all.updated(id, updated), Right(updated))
    }
object ConditionalOrders:
  def of(orders: List[Order]): IO[ConditionalOrders] =
    Ref.of[IO, Map[OrderId, VersionedOrder]](orders.map(o => o.id -> VersionedOrder(o, 0L)).toMap)
      .map(new ConditionalOrders(_))

/** Bài 5: fault injection ở cổng repository để kiểm tra HTTP thật của lab, không tạo route /boom. */
final class FailingReadRepository(delegate: OrderRepository) extends OrderRepository:
  def createUser(draft: UserDraft): IO[Either[DomainError, User]] = delegate.createUser(draft)
  def getUser(id: UserId): IO[Option[User]] = IO.raiseError(new IllegalStateException(s"db password=secret; id=${id.value}"))
  def listUsers(query: UserQuery): IO[Page[User]] = delegate.listUsers(query)
  def createOrder(userId: UserId, total: Money): IO[Either[DomainError, Order]] = delegate.createOrder(userId, total)
  def getOrder(id: OrderId): IO[Option[Order]] = delegate.getOrder(id)
  def listOrders(userId: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]] = delegate.listOrders(userId, page)
  def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]] = delegate.updateStatus(id, next)
