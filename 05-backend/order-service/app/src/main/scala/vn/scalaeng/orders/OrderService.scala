package vn.scalaeng.orders

import cats.effect.IO

/** Validation/nghiệp vụ không biết HTTP status. Repository bảo vệ điều kiện cần tính nguyên tử. */
final class OrderService(repository: OrderRepository):
  private def run[A, B](input: Either[DomainError, A])(f: A => IO[Either[DomainError, B]]): IO[Either[DomainError, B]] =
    input.fold(e => IO.pure(Left(e)), f)
  private def userId(raw: Long): Either[DomainError, UserId] =
    UserId.from(raw).left.map(e => DomainError.Invalid(List(e)))
  private def orderId(raw: Long): Either[DomainError, OrderId] =
    OrderId.from(raw).left.map(e => DomainError.Invalid(List(e)))

  def createUser(name: String, email: String): IO[Either[DomainError, User]] =
    run(Validation.user(name, email))(repository.createUser)
  def getUser(raw: Long): IO[Either[DomainError, User]] =
    run(userId(raw))(id => repository.getUser(id).map(_.toRight(DomainError.UnknownUser(id))))
  def listUsers(q: Option[String], sort: String, offset: Int, limit: Int): IO[Either[DomainError, Page[User]]] =
    run(Validation.query(q, sort, offset, limit))(query => repository.listUsers(query).map(Right(_)))
  def createOrder(rawUserId: Long, totalMinor: Long): IO[Either[DomainError, Order]] =
    run(Validation.order(rawUserId, totalMinor))((id, total) => repository.createOrder(id, total))
  def getOrder(raw: Long): IO[Either[DomainError, Order]] =
    run(orderId(raw))(id => repository.getOrder(id).map(_.toRight(DomainError.UnknownOrder(id))))
  def listOrders(rawUserId: Long, offset: Int, limit: Int): IO[Either[DomainError, Page[Order]]] =
    val valid = for
      id <- userId(rawUserId)
      p <- PageRequest.from(offset, limit).left.map(DomainError.Invalid(_))
    yield (id, p)
    run(valid)((id, p) => repository.listOrders(id, p))
  def updateStatus(raw: Long, next: OrderStatus): IO[Either[DomainError, Order]] =
    run(orderId(raw))(id => repository.updateStatus(id, next))
