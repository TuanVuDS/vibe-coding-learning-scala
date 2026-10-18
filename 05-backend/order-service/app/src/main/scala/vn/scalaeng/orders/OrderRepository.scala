package vn.scalaeng.orders

import cats.effect.{IO, Ref}
import java.util.Locale

/** Cổng lưu trữ: không import HTTP, JSON hay DB. Lỗi hạ tầng vẫn là failed IO.
  * createUser: email chuẩn hóa duy nhất. createOrder: kiểm tra user và insert nguyên tử.
  * updateStatus: đọc trạng thái, kiểm tra transition và ghi nguyên tử (kể cả idempotent retry).
  * list: total và items thuộc cùng snapshot; sort luôn có id làm khóa phụ.
  */
trait OrderRepository:
  def createUser(draft: UserDraft): IO[Either[DomainError, User]]
  def getUser(id: UserId): IO[Option[User]]
  def listUsers(query: UserQuery): IO[Page[User]]
  def createOrder(userId: UserId, total: Money): IO[Either[DomainError, Order]]
  def getOrder(id: OrderId): IO[Option[Order]]
  def listOrders(userId: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]]
  def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]]

object InMemoryOrderRepository:
  private final case class State(nextUser: Long, nextOrder: Long, users: Map[UserId, User], orders: Map[OrderId, Order])
  def empty: IO[OrderRepository] =
    Ref.of[IO, State](State(1L, 1L, Map.empty, Map.empty)).map(new Memory(_))

  private def pageOf[A](all: List[A], p: PageRequest): Page[A] =
    Page(all.slice(p.offset, p.offset + p.limit), all.size, p.offset, p.limit)

  private final class Memory(state: Ref[IO, State]) extends OrderRepository:
    def createUser(draft: UserDraft): IO[Either[DomainError, User]] = state.modify { s =>
      if s.users.values.exists(_.email == draft.email) then (s, Left(DomainError.DuplicateEmail(draft.email)))
      else
        val id = UserId.assigned(s.nextUser)
        val user = User(id, draft.name, draft.email)
        (s.copy(nextUser = Math.addExact(s.nextUser, 1L), users = s.users.updated(id, user)), Right(user))
    }

    def getUser(id: UserId): IO[Option[User]] = state.get.map(_.users.get(id))

    def listUsers(query: UserQuery): IO[Page[User]] = state.get.map { s =>
      val filtered = s.users.values.toList.filter(u => query.q.forall(q =>
        u.name.toLowerCase(Locale.ROOT).contains(q) || u.email.contains(q)))
      val sorted = query.sort match
        case UserSort.Id => filtered.sortBy(_.id.value)
        case UserSort.Name => filtered.sortBy(u => (u.name.toLowerCase(Locale.ROOT), u.id.value))
        case UserSort.Email => filtered.sortBy(u => (u.email, u.id.value))
      pageOf(if query.descending then sorted.reverse else sorted, query.page)
    }

    def createOrder(userId: UserId, total: Money): IO[Either[DomainError, Order]] = state.modify { s =>
      if !s.users.contains(userId) then (s, Left(DomainError.UnknownUser(userId)))
      else
        val id = OrderId.assigned(s.nextOrder)
        val order = Order(id, userId, total, OrderStatus.Pending)
        (s.copy(nextOrder = Math.addExact(s.nextOrder, 1L), orders = s.orders.updated(id, order)), Right(order))
    }

    def getOrder(id: OrderId): IO[Option[Order]] = state.get.map(_.orders.get(id))

    def listOrders(userId: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]] = state.get.map { s =>
      if !s.users.contains(userId) then Left(DomainError.UnknownUser(userId))
      else Right(pageOf(s.orders.values.toList.filter(_.userId == userId).sortBy(_.id.value), page))
    }

    def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]] = state.modify { s =>
      s.orders.get(id) match
        case None => (s, Left(DomainError.UnknownOrder(id)))
        case Some(o) if !o.status.canMoveTo(next) => (s, Left(DomainError.InvalidTransition(o.status, next)))
        case Some(o) =>
          val updated = o.copy(status = next)
          (s.copy(orders = s.orders.updated(id, updated)), Right(updated))
    }
