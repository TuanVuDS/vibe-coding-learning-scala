package vn.scalaeng.p2.service

import cats.Monad
import cats.effect.IO
import cats.syntax.all.*
import vn.scalaeng.p2.domain.*

/** A single interpreter call is a single use-case transaction. */
trait TransactionRunner[F[_]]:
  def run[A](program: F[A]): IO[A]

final class Shop[F[_]: Monad](store: Store[F], tx: TransactionRunner[F]):
  private def pure[A](e: Either[DomainError, A]): F[Either[DomainError, A]] = e.pure[F]
  private def owns(p: Principal, id: UserId): Boolean = p.admin || p.id == id

  def checkout(p: Principal, user: UserId, lines: List[Line]): IO[Either[DomainError, Order]] =
    if !owns(p, user) then IO.pure(Left(DomainError.Forbidden))
    else Checkout.merge(lines) match
      case Left(e) => IO.pure(Left(e))
      case Right(wanted) => tx.run {
        store.lockProducts(wanted.keys.toList.sorted).flatMap { products =>
          Checkout.price(wanted, products) match
            case Left(e) => pure(Left(e))
            case Right((money, items)) =>
              store.createOrder(user, money).flatMap {
                case Left(e) => pure(Left(e))
                case Right(o) => (store.insertItems(o.id, items) *> store.decrement(items)).as(Right(o))
              }
        }
      }

  def order(p: Principal, id: OrderId): IO[Either[DomainError, Order]] = tx.run {
    store.getOrder(id).map {
      case None => Left(DomainError.UnknownOrder(id))
      case Some(o) => Either.cond(owns(p, o.userId), o, DomainError.Forbidden)
    }
  }

  def orders(p: Principal, user: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]] =
    if !owns(p, user) then IO.pure(Left(DomainError.Forbidden)) else tx.run(store.listOrders(user, page))

  def user(p: Principal, id: UserId): IO[Either[DomainError, User]] =
    if !owns(p, id) then IO.pure(Left(DomainError.Forbidden))
    else tx.run(store.getUser(id).map(_.toRight(DomainError.UnknownUser(id))))

  def users(p: Principal, query: UserQuery): IO[Either[DomainError, Page[User]]] =
    if !p.admin then IO.pure(Left(DomainError.Forbidden)) else tx.run(store.listUsers(query)).map(Right(_))

  def status(p: Principal, id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]] = tx.run {
    // Only the transition path locks the order: restore is executed at most once.
    store.lockOrder(id).flatMap {
      case None => pure(Left(DomainError.UnknownOrder(id)))
      case Some(o) if !owns(p, o.userId) || (!p.admin && next != OrderStatus.Cancelled) => pure(Left(DomainError.Forbidden))
      case Some(o) => store.updateStatus(id, next).flatMap {
        case Right(updated) if o.status != next && next == OrderStatus.Cancelled => store.restore(id).as(Right(updated))
        case result => pure(result)
      }
    }
  }

  def product(id: Long): IO[Option[Product]] =
    Resilience.retryRead(tx.run(store.product(id)),2,scala.concurrent.duration.DurationInt(20).millis)(_.isInstanceOf[java.sql.SQLTransientException])
  def products(q: Option[String], descending: Boolean, page: PageRequest): IO[Page[Product]] =
    tx.run(store.products(q, descending, page))
  def saveProduct(p: Principal, id: Option[Long], draft: ProductDraft): IO[Either[DomainError, Product]] =
    if !p.admin then IO.pure(Left(DomainError.Forbidden))
    else Checkout.product(draft).fold(e => IO.pure(Left(e)), d => tx.run(store.saveProduct(id, d)))
  def deleteProduct(p: Principal, id: Long): IO[Either[DomainError, Unit]] =
    if !p.admin then IO.pure(Left(DomainError.Forbidden))
    else tx.run(store.deleteProduct(id)).map(ok => Either.cond(ok, (), DomainError.MissingProduct))
