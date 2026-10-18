package vn.scalaeng.p2.http

import cats.effect.{IO,Ref}
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.service.TransactionRunner

/** Test adapter, not a durable implementation or evidence of SQL correctness. */
final class MemoryStore private (accounts: Ref[IO,Map[Long,Account]], orders: Ref[IO,Map[Long,Order]],
    catalog: Ref[IO,Map[Long,Product]], revokedIds: Ref[IO,Set[String]]) extends Store[IO]:
  def promote(id: UserId): IO[Unit] = accounts.update(m => m.updated(id.value,m(id.value).copy(role="Admin")))
  def account(email: String): IO[Option[Account]] = accounts.get.map(_.values.find(_.user.email == email))
  def accountById(id: UserId): IO[Option[Account]] = accounts.get.map(_.get(id.value))
  def createUser(d: UserDraft): IO[Either[DomainError,User]] = register(d,"disabled")
  def register(d: UserDraft,h: String): IO[Either[DomainError,User]] = accounts.modify { a =>
    if a.values.exists(_.user.email==d.email) then (a,Left(DomainError.DuplicateEmail(d.email)))
    else
      val id = a.size.toLong+1
      val u = User(UserId.assigned(id),d.name,d.email)
      (a.updated(id,Account(u,h,"Customer")),Right(u))
  }
  def getUser(id: UserId): IO[Option[User]] = accountById(id).map(_.map(_.user))
  def listUsers(q: UserQuery): IO[Page[User]] = accounts.get.map(a => Page(a.values.toList.map(_.user),a.size,q.page.offset,q.page.limit))
  def createOrder(id: UserId,total: Money): IO[Either[DomainError,Order]] = orders.modify { all =>
    val o = Order(OrderId.assigned(all.size.toLong+1),id,total,OrderStatus.Pending)
    (all.updated(o.id.value,o),Right(o))
  }
  def getOrder(id: OrderId): IO[Option[Order]] = orders.get.map(_.get(id.value))
  def listOrders(id: UserId,p: PageRequest): IO[Either[DomainError,Page[Order]]] = orders.get.map { all =>
    val found = all.values.filter(_.userId==id).toList.sortBy(_.id.value)
    Right(Page(found.slice(p.offset,p.offset+p.limit),found.size,p.offset,p.limit))
  }
  def updateStatus(id: OrderId,s: OrderStatus): IO[Either[DomainError,Order]] = orders.modify { all =>
    all.get(id.value) match
      case Some(o) if o.status.canMoveTo(s) => val updated=o.copy(status=s);(all.updated(id.value,updated),Right(updated))
      case Some(o) => (all,Left(DomainError.InvalidTransition(o.status,s)))
      case None => (all,Left(DomainError.UnknownOrder(id)))
  }
  def revoked(jti: String): IO[Boolean] = revokedIds.get.map(_.contains(jti))
  def revoke(jti: String,expiresEpoch: Long): IO[Unit] = revokedIds.update(_+jti)
  def product(id: Long): IO[Option[Product]] = catalog.get.map(_.get(id).filter(_.active))
  def products(q: Option[String],descending: Boolean,p: PageRequest): IO[Page[Product]] = catalog.get.map { m =>
    val ps=m.values.filter(_.active).toList.sortBy(_.id)
    Page(ps,ps.size,p.offset,p.limit)
  }
  def saveProduct(id: Option[Long],d: ProductDraft): IO[Either[DomainError,Product]] = catalog.modify { all =>
    val i=id.getOrElse(all.size.toLong+1)
    val p=Product(i,d.sku,d.name,d.price,d.stock,true)
    (all.updated(i,p),Right(p))
  }
  def deleteProduct(id: Long): IO[Boolean] = catalog.modify(m => (m-id,m.contains(id)))
  def lockProducts(ids: List[Long]): IO[List[Product]] = catalog.get.map(m => ids.flatMap(m.get))
  def insertItems(id: OrderId,lines: List[PricedLine]): IO[Unit] = IO.unit
  def decrement(lines: List[PricedLine]): IO[Unit] = catalog.update(m => lines.foldLeft(m)((all,l) => all.updated(l.productId,all(l.productId).copy(stock=all(l.productId).stock-l.quantity))))
  def lockOrder(id: OrderId): IO[Option[Order]] = getOrder(id)
  def restore(id: OrderId): IO[Unit] = IO.unit
object MemoryStore:
  val runner: TransactionRunner[IO] = new TransactionRunner[IO]:
    def run[A](program: IO[A]): IO[A] = program
  def make: IO[MemoryStore] = for
    a <- Ref.of[IO,Map[Long,Account]](Map.empty)
    o <- Ref.of[IO,Map[Long,Order]](Map.empty)
    p <- Ref.of[IO,Map[Long,Product]](Map.empty)
    r <- Ref.of[IO,Set[String]](Set.empty)
  yield new MemoryStore(a,o,p,r)
