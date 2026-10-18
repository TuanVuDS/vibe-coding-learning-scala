package vn.scalaeng.orders.solutions

import cats.effect.IO
import cats.syntax.all.*
import io.circe.parser
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status}
import org.http4s.implicits.*
import org.typelevel.ci.CIString
import vn.scalaeng.orders.*
import vn.scalaeng.orders.TransportCodecs.given

class SolutionsSuite extends CatsEffectSuite:
  private def user(id: Long): User = User(UserId.from(id).toOption.get, s"User $id", s"u$id@shop.vn")
  private def order(id: Long, total: Long, status: OrderStatus = OrderStatus.Pending): Order =
    Order(OrderId.from(id).toOption.get, user(1).id, Money.fromMinor(total).toOption.get, status)

  test("bài 1: tổng chính xác, đầu vào âm/rỗng, tràn phép nhân và phép cộng"):
    assertEquals(LineTotal.calculate(List(100L -> 3, 50L -> 2)).map(_.minor), Right(400L))
    assert(LineTotal.calculate(Nil).isLeft)
    assert(LineTotal.calculate(List(100L -> 0)).isLeft)
    assert(LineTotal.calculate(List(-1L -> 1)).isLeft)
    assert(LineTotal.calculate(List(Long.MaxValue -> 2)).isLeft)
    assert(LineTotal.calculate(List(Long.MaxValue -> 1, 1L -> 1)).isLeft)

  test("bài 2: status filter trước page, sort total có id phụ và total đúng"):
    val all = List(order(3, 100, OrderStatus.Paid), order(1, 100, OrderStatus.Paid), order(2, 50))
    val p = OrderSearch.page(all, Some(OrderStatus.Paid), false, PageRequest.from(1, 1).toOption.get)
    assertEquals(p.items.map(_.id.value), List(3L))
    assertEquals(p.total, 2)
    val desc = OrderSearch.page(all, None, true, PageRequest.from(0, 2).toOption.get)
    assertEquals(desc.items.map(_.id.value), List(3L, 1L))

  test("bài 3: cursor không lặp/bỏ dòng khi có gap, trang cuối không phát cursor"):
    val users = List(user(8), user(1), user(4))
    val first = CursorPagination.page(users, 0, 2).toOption.get
    val second = CursorPagination.page(users :+ user(9), first.nextAfter.get, 2).toOption.get
    assertEquals(first.items.map(_.id.value), List(1L, 4L))
    assertEquals(first.nextAfter, Some(4L))
    assertEquals(second.items.map(_.id.value), List(8L, 9L))
    assertEquals(second.nextAfter, None)
    assertEquals(CursorPagination.page(Nil, 9, 10).map(_.items), Right(Nil))
    assert(CursorPagination.page(users, -1, 101).isLeft)

  test("bài 4: hai client cùng version -> một 412; transition sai không tiêu hao version"):
    val o = order(1, 100)
    for
      store <- ConditionalOrders.of(List(o))
      invalid <- store.update(o.id, 0, OrderStatus.Shipped)
      results <- List(OrderStatus.Paid, OrderStatus.Cancelled).parTraverse(store.update(o.id, 0, _))
    yield
      assertEquals(invalid, Left(UpdateError.InvalidTransition))
      assertEquals(results.count(_.isRight), 1)
      assertEquals(results.count(_ == Left(UpdateError.PreconditionFailed)), 1)
      assertEquals(results.flatMap(_.toOption).map(_.version), List(1L))

  test("bài 5: failed IO -> 500 Problem, không lộ exception/message nội bộ"):
    for
      repo <- InMemoryOrderRepository.empty
      a = Api.httpApp(new OrderService(new FailingReadRepository(repo)))
      r <- a.run(Request[IO](Method.GET, uri"/api/v1/users/1"))
      body <- r.bodyText.compile.string
      p <- IO.fromEither(parser.decode[Problem](body))
    yield
      assertEquals(r.status, Status.InternalServerError)
      assertEquals(p.status, 500)
      assertEquals(p.detail, "Internal server error")
      assert(!body.contains("password") && !body.contains("IllegalStateException"), body)
      assertEquals(r.contentType.map(ct => s"${ct.mediaType.mainType}/${ct.mediaType.subType}"), Some("application/problem+json"))

  test("bài 6: thiếu bearer -> 401 trong HTTP/body, giữ WWW-Authenticate"):
    for
      r <- DecodeExample.app.run(Request[IO](Method.GET, uri"/protected"))
      body <- r.bodyText.compile.string
      p <- IO.fromEither(parser.decode[Problem](body))
    yield
      assertEquals((r.status.code, p.status), (401, 401))
      assertEquals(p.title, "Unauthorized")
      assertEquals(r.headers.get(CIString("WWW-Authenticate")).map(_.head.value), Some("Bearer"))
