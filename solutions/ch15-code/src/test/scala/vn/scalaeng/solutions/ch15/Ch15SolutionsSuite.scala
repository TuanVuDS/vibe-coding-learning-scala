package vn.scalaeng.solutions.ch15

import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import vn.scalaeng.db.*

class Ch15SolutionsSuite extends PostgresSuite:
  // PostgresSuite đã chạy V1, V2. Test nào cần V3 thì gọi migrateV3: Flyway bỏ qua version đã chạy,
  // nên gọi nhiều lần vẫn an toàn (idempotent). `truncate` trước mỗi test không xóa schema.
  private def migrateV3: IO[Int] = Migrations.migrateWithSolutions(config)

  private def seed: IO[(UserId, ProductId, ProductId, ProductId)] =
    (for
      u  <- UserRepo.create("an@x.vn", "An").map(_.toOption.get)
      kb <- ProductRepo.create("KB-01", "Bàn phím", 450_000, 10)
      ms <- ProductRepo.create("MS-01", "Chuột", 250_000, 10)
      hp <- ProductRepo.create("HP-01", "Tai nghe", 990_000, 10)
    yield (u.id, kb, ms, hp)).transact(xa)

  test("bài 2: V3 áp dụng sau V1, V2; ràng buộc cancelled_at hoạt động"):
    for
      _        <- migrateV3
      versions <- sql"select version from flyway_schema_history order by installed_rank".query[String].to[List].transact(xa)
      _        <- UserRepo.create("b@x.vn", "B").transact(xa)
      bad      <- sql"insert into orders (user_id, total_vnd, status) values (1, 0, 'CANCELLED')".update.run.transact(xa).attempt
    yield
      assertEquals(versions, List("1", "2", "3"))
      assert(bad.left.exists(_.getMessage.contains("orders_cancelled_consistent")), bad.toString)

  test("bài 1: top sản phẩm theo số lượng, bỏ đơn đã hủy"):
    for
      _              <- migrateV3
      (u, kb, ms, hp) <- seed
      svc             = OrderService(xa)
      _              <- svc.placeOrder(u, List(OrderLine(kb, 3), OrderLine(ms, 1)))
      _              <- svc.placeOrder(u, List(OrderLine(ms, 1), OrderLine(hp, 1)))
      cancelled      <- svc.placeOrder(u, List(OrderLine(hp, 5)))
      _              <- CancelOrder.cancel(cancelled.toOption.get.id).transact(xa)
      top            <- TopSellers.top(2).transact(xa)
    yield assertEquals(top, List(
      TopSellers.Row("KB-01", "Bàn phím", 3, 1_350_000),
      TopSellers.Row("MS-01", "Chuột", 2, 500_000)
    ))

  test("bài 3: hủy hoàn kho; hủy lần hai / đơn không tồn tại được báo đúng"):
    for
      _              <- migrateV3
      (u, kb, _, _)  <- seed
      order          <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 4))).map(_.toOption.get)
      first          <- CancelOrder.cancel(order.id).transact(xa)
      second         <- CancelOrder.cancel(order.id).transact(xa)
      missing        <- CancelOrder.cancel(OrderId(999)).transact(xa)
      stock          <- ProductRepo.find(kb).transact(xa).map(_.get.stock)
    yield
      assertEquals((first, second, missing), (CancelOrder.Result.Cancelled, CancelOrder.Result.NotCancellable, CancelOrder.Result.NotFound))
      assertEquals(stock, 10)

  test("bài 3: hai yêu cầu hủy ĐỒNG THỜI chỉ hoàn kho một lần"):
    for
      _             <- migrateV3
      (u, kb, _, _) <- seed
      order         <- OrderService(xa).placeOrder(u, List(OrderLine(kb, 4))).map(_.toOption.get)
      results       <- (CancelOrder.cancel(order.id).transact(xa), CancelOrder.cancel(order.id).transact(xa)).parTupled
      stock         <- ProductRepo.find(kb).transact(xa).map(_.get.stock)
    yield
      assertEquals(List(results._1, results._2).count(_ == CancelOrder.Result.Cancelled), 1)
      assertEquals(stock, 10)

  test("bài 4: keyset đi hết 5 user qua 3 trang, không trùng không sót — kể cả khi created_at TRÙNG nhau"):
    def all(cursor: Option[KeysetPage.Cursor], acc: List[String]): IO[List[String]] =
      KeysetPage.page(cursor, 2).transact(xa).flatMap {
        case Left(e) => IO.raiseError(RuntimeException(e))
        case Right((users, next)) =>
          val names = acc ++ users.map(_.name)
          next.fold(IO.pure(names))(c => all(Some(c), names))
      }
    for
      // Cả 5 dòng chèn trong MỘT câu lệnh có cùng now() → created_at trùng nhau; chỉ id phân biệt thứ tự.
      _     <- sql"insert into users (email, name) select 'u' || g || '@x.vn', 'U' || g from generate_series(1, 5) g".update.run.transact(xa)
      times <- sql"select count(distinct created_at) from users".query[Long].unique.transact(xa)
      names <- all(None, Nil)
      bad   <- List(0, 101).traverse(n => KeysetPage.page(None, n).transact(xa))
    yield
      assertEquals(times, 1L)
      assertEquals(names, List("U1", "U2", "U3", "U4", "U5"))
      assertEquals(bad, List(Left("size phải trong 1..100"), Left("size phải trong 1..100")))

  test("bài 5: UPDATE có điều kiện -> đúng một người mua được món cuối"):
    for
      pid     <- ProductRepo.create("LAST-1", "Món cuối", 100_000, 1).transact(xa)
      results <- (AtomicStock.buyOne(pid).transact(xa), AtomicStock.buyOne(pid).transact(xa)).parTupled
      stock   <- ProductRepo.find(pid).transact(xa).map(_.get.stock)
    yield
      assertEquals(List(results._1, results._2).sorted, List(false, true))
      assertEquals(stock, 0)

  test("bài 6: index (user_id, created_at desc, id desc) -> Index Scan, không còn Sort"):
    val seedOrders =
      sql"""insert into users (email, name) select 'u' || g || '@x.vn', 'U' || g from generate_series(1, 500) g""".update.run *>
        sql"""insert into orders (user_id, total_vnd, created_at)
              select (g % 500) + 1, 1000, timestamptz '2026-01-01' + g * interval '1 minute'
              from generate_series(1, 50000) g""".update.run *>
        sql"analyze orders".update.run
    for
      _    <- migrateV3
      _    <- seedOrders.transact(xa)
      plan <- Database.explain(LatestOrders.query(UserId(42))).transact(xa)
    yield
      assert(plan.exists(_.contains("orders_user_created_idx")), plan.mkString("\n"))
      assert(!plan.exists(_.contains("Sort")), plan.mkString("\n"))
