package vn.scalaeng.orders

import cats.syntax.all.*
import munit.CatsEffectSuite
import Fixtures.*

class RepositorySuite extends CatsEffectSuite:
  test("20 đăng ký đồng thời: email duy nhất được kiểm tra cùng lần ghi"):
    for
      s <- service
      results <- (1 to 20).toList.parTraverse(_ => s.createUser("An", " AN@EXAMPLE.COM "))
      page <- s.listUsers(None, "id", 0, 20).flatMap(right)
    yield
      assertEquals(results.count(_.isRight), 1)
      assertEquals(results.count(_ == Left(DomainError.DuplicateEmail("an@example.com"))), 19)
      assertEquals(page.total, 1)

  test("filter -> sort với id phụ -> page; total tính trước page"):
    for
      s <- service
      _ <- s.createUser("An", "a@shop.vn")
      _ <- s.createUser("Bình", "b@shop.vn")
      _ <- s.createUser("AN", "c@shop.vn")
      p <- s.listUsers(Some("AN"), "name", 1, 1).flatMap(right)
      desc <- s.listUsers(None, "-id", 0, 2).flatMap(right)
      empty <- s.listUsers(None, "id", 99, 20).flatMap(right)
    yield
      assertEquals(p.items.map(_.id.value), List(3L))
      assertEquals((p.total, p.offset, p.limit), (2, 1, 1))
      assertEquals(desc.items.map(_.id.value), List(3L, 2L))
      assertEquals((empty.items, empty.total), (Nil, 3))

  test("tạo order cho user thiếu: không ghi và không tiêu hao id"):
    for
      s <- service
      bad <- s.createOrder(99, 500)
      u <- s.createUser("An", "an@shop.vn").flatMap(right)
      o <- s.createOrder(u.id.value, 500).flatMap(right)
      p <- s.listOrders(u.id.value, 0, 20).flatMap(right)
    yield
      assert(bad.isLeft)
      assertEquals(o.id.value, 1L)
      assertEquals(p.total, 1)

  test("list orders cô lập theo user và phân biệt user thiếu với danh sách rỗng"):
    for
      s <- service
      a <- s.createUser("An", "a@shop.vn").flatMap(right)
      b <- s.createUser("Bình", "b@shop.vn").flatMap(right)
      _ <- s.createOrder(a.id.value, 100)
      _ <- s.createOrder(a.id.value, 200)
      p <- s.listOrders(a.id.value, 1, 1).flatMap(right)
      empty <- s.listOrders(b.id.value, 0, 20).flatMap(right)
      missing <- s.listOrders(99, 0, 20)
    yield
      assertEquals((p.items.map(_.total.minor), p.total), (List(200L), 2))
      assertEquals(empty.total, 0)
      assert(missing.isLeft)

  test("Paid và Cancelled cạnh tranh: một thành công, một conflict, không lost update"):
    for
      s <- service
      _ <- s.createUser("An", "a@shop.vn")
      _ <- s.createOrder(1, 100)
      results <- List(OrderStatus.Paid, OrderStatus.Cancelled).parTraverse(s.updateStatus(1, _))
      finalOrder <- s.getOrder(1).flatMap(right)
    yield
      assertEquals(results.count(_.isRight), 1)
      assertEquals(results.count(_.isLeft), 1)
      assert(Set(OrderStatus.Paid, OrderStatus.Cancelled).contains(finalOrder.status))

  test("transition hợp lệ, retry idempotent, conflict không đổi trạng thái"):
    for
      s <- service
      _ <- s.createUser("An", "a@shop.vn")
      _ <- s.createOrder(1, 100)
      paid <- s.updateStatus(1, OrderStatus.Paid)
      retry <- s.updateStatus(1, OrderStatus.Paid)
      _ <- s.updateStatus(1, OrderStatus.Shipped)
      bad <- s.updateStatus(1, OrderStatus.Cancelled)
      got <- s.getOrder(1).flatMap(right)
    yield
      assertEquals(retry, paid)
      assert(bad.isLeft)
      assertEquals(got.status, OrderStatus.Shipped)
