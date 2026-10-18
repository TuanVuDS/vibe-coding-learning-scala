package vn.scalaeng.orders

import cats.effect.{IO, Ref}
import cats.effect.std.CountDownLatch
import cats.syntax.all.*
import munit.CatsEffectSuite
import Fixtures.right

/** Phản ví dụ có chủ ý cho mục 3.5; không được nối vào repository của server. */
class RefRaceExampleSuite extends CatsEffectSuite:
  private def unsafeUpdateStatus(state: Ref[IO, Map[OrderId, Order]], id: OrderId,
      next: OrderStatus, afterRead: IO[Unit]): IO[Either[DomainError, Order]] =
    state.get.flatMap { snapshot =>
      afterRead *> (snapshot.get(id) match
        case None => IO.pure(Left(DomainError.UnknownOrder(id)))
        case Some(o) if !o.status.canMoveTo(next) => IO.pure(Left(DomainError.InvalidTransition(o.status, next)))
        case Some(o) =>
          val updated = o.copy(status = next)
          state.set(snapshot.updated(id, updated)).as(Right(updated)))
    }

  test("phản ví dụ get rồi set: hai request cùng đọc Pending đều báo thành công"):
    for
      service <- Fixtures.service
      user <- service.createUser("An", "an@shop.vn").flatMap(right)
      order <- service.createOrder(user.id.value, 100).flatMap(right)
      state <- Ref.of[IO, Map[OrderId, Order]](Map(order.id -> order))
      bothRead <- CountDownLatch[IO](2)
      results <- List(OrderStatus.Paid, OrderStatus.Cancelled).parTraverse(next =>
        unsafeUpdateStatus(state, order.id, next, bothRead.release *> bothRead.await))
      stored <- state.get
    yield
      assertEquals(results.count(_.isRight), 2)
      assertEquals(results.collect { case Right(o) => o.status }.toSet,
        Set(OrderStatus.Paid, OrderStatus.Cancelled))
      assert(Set(OrderStatus.Paid, OrderStatus.Cancelled).contains(stored(order.id).status))
      assertEquals(stored.size, 1)
