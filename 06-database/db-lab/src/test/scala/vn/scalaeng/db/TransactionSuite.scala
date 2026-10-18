package vn.scalaeng.db

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*

class TransactionSuite extends PostgresSuite:
  private def seed(stock: Int): IO[(UserId, ProductId, ProductId)] =
    (for
      u  <- UserRepo.create("an@x.vn", "An").map(_.toOption.get)
      kb <- ProductRepo.create("KB-01", "Bàn phím", 450_000, stock)
      ms <- ProductRepo.create("MS-01", "Chuột", 250_000, stock)
    yield (u.id, kb, ms)).transact(xa)

  private def stockOf(id: ProductId): IO[Int] = ProductRepo.find(id).transact(xa).map(_.get.stock)

  test("đặt hàng thành công: ghi đơn + chi tiết + trừ kho trong MỘT transaction"):
    for
      (u, kb, ms) <- seed(stock = 5)
      order       <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 2), OrderLine(ms, 1), OrderLine(kb, 1)))
      items       <- sql"select product_id, quantity, unit_price_vnd from order_items order by product_id"
                       .query[(Long, Int, Long)].to[List].transact(xa)
      left        <- (stockOf(kb), stockOf(ms)).tupled
    yield
      assertEquals(order.map(o => (o.status, o.totalVnd)), Right((OrderStatus.Pending, 3 * 450_000L + 250_000L)))
      assertEquals(items, List((kb.value, 3, 450_000L), (ms.value, 1, 250_000L))) // dòng trùng sản phẩm đã được gộp
      assertEquals(left, (2, 4))

  test("thiếu hàng: không ghi gì, không trừ kho"):
    for
      (u, kb, ms) <- seed(stock = 1)
      result      <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 1), OrderLine(ms, 2)))
      orders      <- OrderRepo.countFor(u).transact(xa)
      left        <- (stockOf(kb), stockOf(ms)).tupled
    yield
      assertEquals(result, Left(PlaceOrderError.OutOfStock(ms, requested = 2, available = 1)))
      assertEquals((orders, left), (0, (1, 1)))

  test("lỗi giữa chừng -> ROLLBACK toàn bộ (atomicity)"):
    val boom = new RuntimeException("mất kết nối giả lập")
    for
      (u, kb, _) <- seed(stock = 5)
      tx          = OrderService(xa).placeOrderTx(u, List(OrderLine(kb, 2))) *> FC.raiseError[Unit](boom)
      attempt    <- tx.transact(xa).attempt
      orders     <- OrderRepo.countFor(u).transact(xa)
      left       <- stockOf(kb)
    yield
      assertEquals(attempt.left.map(_.getMessage), Left("mất kết nối giả lập"))
      assertEquals((orders, left), (0, 5)) // đơn đã INSERT và kho đã UPDATE đều bị hủy

  test("sản phẩm và user không tồn tại được báo rõ"):
    for
      (u, kb, _) <- seed(stock = 5)
      noProduct  <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 1), OrderLine(ProductId(999), 1)))
      noUser     <- OrderService(xa).placeOrder(UserId(999), List(OrderLine(kb, 1)))
      empty      <- OrderService(xa).placeOrder(u, Nil)
    yield
      assertEquals(noProduct, Left(PlaceOrderError.UnknownProducts(List(ProductId(999)))))
      assertEquals(noUser, Left(PlaceOrderError.UnknownUser(UserId(999))))
      assertEquals(empty, Left(PlaceOrderError.EmptyOrder))

  test("số lượng không hợp lệ hoặc tràn số bị từ chối TRƯỚC khi chạm database"):
    for
      (u, kb, _) <- seed(stock = 10)
      overflow   <- OrderService(xa).placeOrder(u, List(OrderLine(kb, Int.MaxValue), OrderLine(kb, Int.MaxValue), OrderLine(kb, 3)))
      zero       <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 0)))
      tooMany    <- OrderService(xa).placeOrder(u, List.fill(3)(OrderLine(kb, 400)))
      left       <- stockOf(kb)
    yield
      assertEquals(overflow, Left(PlaceOrderError.InvalidQuantity(kb, Int.MaxValue.toLong)))
      assertEquals(zero, Left(PlaceOrderError.InvalidQuantity(kb, 0)))
      assertEquals(tooMany, Left(PlaceOrderError.InvalidQuantity(kb, 1200))) // từng dòng hợp lệ, TỔNG vượt giới hạn
      assertEquals(left, 10)

  test("tổng tiền vượt miền Long -> TotalTooLarge (không quay vòng về 0)"):
    assertEquals(OrderService.total(List((ProductId(1), 4, 4611686018427387904L))), Left(PlaceOrderError.TotalTooLarge))
    assertEquals(OrderService.total(List((ProductId(1), 3, 450_000L), (ProductId(2), 1, 250_000L))), Right(1_600_000L))

  // ----- Đồng thời: 2 khách cùng mua món hàng CUỐI CÙNG -----
  /** Rào chắn (barrier): mỗi transaction đếm xuống SAU khi đọc tồn kho rồi chờ bên kia cũng đọc xong.
    * Nhờ vậy CHẮC CHẮN cả hai đã đọc giá trị cũ trước khi ai ghi — không dựa vào `Thread.sleep` (máy chậm có thể làm test sai).
    * Chỉ dùng cho bản đọc-không-khóa: dùng với `FOR UPDATE` sẽ treo, vì bên kia không đọc được tới rào chắn.
    */
  private def bothReadFirst: ConnectionIO[Unit] =
    val latch = java.util.concurrent.CountDownLatch(2)
    FC.delay { latch.countDown(); assert(latch.await(10, java.util.concurrent.TimeUnit.SECONDS), "rào chắn hết giờ") }

  test("lost update: đọc-rồi-ghi không khóa -> bán 2 món khi kho chỉ có 1"):
    for
      (_, kb, _) <- seed(stock = 1)
      naive       = NaiveStock(xa, bothReadFirst)
      results    <- (naive.buyOne(kb), naive.buyOne(kb)).parTupled
      left       <- stockOf(kb)
    yield
      assertEquals(results, (true, true)) // cả hai đều "mua được"
      assertEquals(left, 0)               // nhưng kho chỉ giảm 1: một lần ghi bị ghi đè (lost update)

  test("REPEATABLE READ: cùng code sai, PostgreSQL hủy transaction thứ hai (SQLSTATE 40001)"):
    for
      (_, kb, _) <- seed(stock = 1)
      naive       = NaiveStock(xa, bothReadFirst)
      results    <- (naive.buyOne(kb, Some("repeatable read")).attempt, naive.buyOne(kb, Some("repeatable read")).attempt).parTupled
      left       <- stockOf(kb)
    yield
      val states = List(results._1, results._2).map(_.left.map {
        case e: java.sql.SQLException => e.getSQLState
        case e                        => e.toString
      })
      assertEquals(states.sortBy(_.isRight), List(Left("40001"), Right(true)))
      assertEquals(left, 0)

  test("SELECT ... FOR UPDATE: đúng một khách mua được, khách kia nhận OutOfStock"):
    for
      (u, kb, _) <- seed(stock = 1)
      service     = OrderService(xa)
      results    <- (service.placeOrder(u, List(OrderLine(kb, 1))), service.placeOrder(u, List(OrderLine(kb, 1)))).parTupled
      left       <- stockOf(kb)
      orders     <- OrderRepo.countFor(u).transact(xa)
    yield
      val outcomes = List(results._1, results._2).map(_.isRight).sorted
      assertEquals(outcomes, List(false, true))
      assertEquals((left, orders), (0, 1))
      assert(List(results._1, results._2).contains(Left(PlaceOrderError.OutOfStock(kb, 1, 0))))
