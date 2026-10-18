package vn.scalaeng.p2.db

import cats.effect.{IO,Resource}
import cats.syntax.all.*
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import scala.concurrent.duration.*
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.service.*

class PostgresSuite extends munit.CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes
  private val realDb: Resource[IO,(DbConfig,Transactor[IO])] = for
    pg <- Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(p => IO.blocking(p.close()))
    cfg = new DbConfig(pg.getJdbcUrl("postgres","postgres"),"postgres","postgres",4)
    _ <- Resource.eval(Database.migrate(cfg))
    xa <- Database.transactor(cfg)
  yield (cfg,xa)
  // Do not start a subprocess when DB tests are disabled. Enabling them never silently hides startup errors.
  private val fixture = ResourceSuiteLocalFixture("postgres", if sys.env.get("RUN_DB_TESTS").contains("1") then realDb.map(Some(_)) else Resource.pure[IO,Option[(DbConfig,Transactor[IO])]](None))
  override def munitFixtures = List(fixture)
  private def database: (DbConfig,Transactor[IO]) =
    assume(fixture().isDefined,"đặt RUN_DB_TESTS=1 ngoài sandbox để chạy PostgreSQL thật")
    fixture().get
  private def run[A](program: ConnectionIO[A]): IO[A] = IO.defer(program.transact(database._2))
  private val store = new PgStore
  private def shop: Shop[ConnectionIO] = new Shop(store,Database.runner(database._2))
  private def seed(stock: Int=3): IO[(User,Product)] = run(for
    _ <- sql"truncate users,products,orders,order_items,revoked_tokens restart identity cascade".update.run
    u <- store.register(UserDraft("An","an@example.vn"),"hash").map(_.toOption.get)
    p <- store.saveProduct(None,ProductDraft("P","Bút",10,stock)).map(_.toOption.get)
  yield (u,p))
  test("migration chạy lại bằng không; readiness select 1") {
    IO.defer { val (cfg,xa)=database
      (Database.migrate(cfg),Database.ready(xa)).tupled.map { (n,up) => assertEquals(n,0);assert(up) }
    }
  }
  test("duplicate ON CONFLICT không abort transaction; unique không phân biệt hoa thường") {
    seed() *> run(for
      duplicate <- store.register(UserDraft("Khác","AN@example.vn"),"hash")
      n <- sql"select count(*) from users".query[Int].unique
    yield {assert(duplicate.isLeft);assertEquals(n,1)})
  }
  test("checkout tổng tiền, chi tiết, kho cùng transaction") {
    seed().flatMap { (u,p) =>
      shop.checkout(Principal(u.id,false),u.id,List(Line(p.id,2))).flatMap { result =>
        run((store.product(p.id),sql"select count(*) from order_items".query[Int].unique).tupled).map { (product,items) =>
          assertEquals(result.map(_.total.minor),Right(20L));assertEquals(product.get.stock,1);assertEquals(items,1)
        }
      }
    }
  }
  test("hai checkout đồng thời không oversell món cuối") {
    seed(1).flatMap { (u,p) =>
      List.fill(2)(shop.checkout(Principal(u.id,false),u.id,List(Line(p.id,1)))).parSequence.flatMap { results =>
        run(store.product(p.id)).map { product => assertEquals(results.count(_.isRight),1);assertEquals(product.get.stock,0) }
      }
    }
  }
  test("lỗi sau insert order rollback cả order, items và kho") {
    seed().flatMap { (u,p) =>
      val install = Fragment.const("""create function fail_items() returns trigger language plpgsql as $$
        begin raise exception 'forced item failure'; end; $$""").update.run *>
        Fragment.const("create trigger fail_items before insert on order_items for each row execute function fail_items()").update.run
      val cleanup = Fragment.const("drop trigger if exists fail_items on order_items").update.run *>
        Fragment.const("drop function if exists fail_items()").update.run
      (run(install) *> shop.checkout(Principal(u.id,false),u.id,List(Line(p.id,2))).attempt.flatMap { result =>
        run((sql"select count(*) from orders".query[Int].unique,store.product(p.id)).tupled)
          .map { (n,product) => assert(result.isLeft);assertEquals(n,0);assertEquals(product.get.stock,3) }
      }).guarantee(run(cleanup).void)
    }
  }
  test("hủy đồng thời chỉ hoàn kho một lần; transition sai không ghi") {
    seed().flatMap { (u,p) =>
      val principal=Principal(u.id,false)
      shop.checkout(principal,u.id,List(Line(p.id,2))).flatMap { result =>
        val id=result.toOption.get.id
        List.fill(2)(shop.status(principal,id,OrderStatus.Cancelled)).parSequence *>
          (run(store.product(p.id)),shop.status(Principal(u.id,true),id,OrderStatus.Paid)).tupled.map { (product,bad) =>
            assertEquals(product.get.stock,3);assert(bad.isLeft)
          }
      }
    }
  }
  test("GET owner/stranger không chờ row lock; updateStatus vẫn phải lấy lock") {
    seed().flatMap { (u,p) =>
      val owner = Principal(u.id,false)
      shop.checkout(owner,u.id,List(Line(p.id,1))).flatMap { result =>
        val order = result.toOption.get
        val cfg = database._1
        // A separate physical connection holds an uncommitted row lock throughout both reads.
        val locked = Resource.make(IO.blocking {
          val c = java.sql.DriverManager.getConnection(cfg.url,cfg.user,cfg.password)
          c.setAutoCommit(false)
          c
        })(c => IO.blocking { try c.rollback() finally c.close() })
        val reads = locked.use { c =>
          IO.blocking {
            val s = c.prepareStatement("select id from orders where id=? for update")
            try
              s.setLong(1,order.id.value)
              val rows = s.executeQuery()
              try assert(rows.next()) finally rows.close()
            finally s.close()
          } *> (for
            own <- shop.order(owner,order.id).timeout(2.seconds)
            denied <- shop.order(Principal(UserId.assigned(999),false),order.id).timeout(2.seconds)
            write <- run(sql"set local lock_timeout = '200ms'".update.run *>
              store.updateStatus(order.id,OrderStatus.Paid)).attempt
          yield {
            assertEquals(own,Right(order));assertEquals(denied,Left(DomainError.Forbidden))
            assertEquals(write.swap.toOption.get.asInstanceOf[java.sql.SQLException].getSQLState,"55P03")
          })
        }
        reads *> shop.status(owner,order.id,OrderStatus.Cancelled).flatMap { changed =>
          run(store.product(p.id)).map { product =>
            assertEquals(changed.map(_.status),Right(OrderStatus.Cancelled));assertEquals(product.get.stock,3)
          }
        }
      }
    }
  }
  test("overflow và sản phẩm thiếu không để lại ghi dở") {
    seed().flatMap { (u,p) =>
      run(store.saveProduct(Some(p.id),ProductDraft(p.sku,p.name,Long.MaxValue,3))) *>
        shop.checkout(Principal(u.id,false),u.id,List(Line(p.id,2))).flatMap { bad =>
          run(sql"select count(*) from orders".query[Int].unique).map(n => {assert(bad.isLeft);assertEquals(n,0)})
        }
    }
  }
  test("lọc/sort/phân trang ổn định, trang rỗng vẫn có total; injection là dữ liệu") {
    seed() *> run(for
      _ <- store.register(UserDraft("Bình","binh@example.vn"),"hash")
      q = Validation.query(None,"-name",0,1).toOption.get
      first <- store.listUsers(q)
      empty <- store.listUsers(q.copy(page=PageRequest.from(100,1).toOption.get))
      injected <- store.listUsers(q.copy(q=Some("' or 1=1 --")))
      ps <- store.products(None,false,PageRequest.from(100,1).toOption.get)
    yield {assertEquals(first.total,2);assertEquals(first.items.head.name,"Bình");assertEquals(empty.total,2);assert(empty.items.isEmpty);assert(injected.items.isEmpty);assertEquals(ps.total,1);assert(ps.items.isEmpty)})
  }
  test("xóa mềm không mất lịch sử giá trong order_items") {
    seed().flatMap { (u,p) =>
      shop.checkout(Principal(u.id,false),u.id,List(Line(p.id,1))) *>
        run(for
          _ <- store.saveProduct(Some(p.id),ProductDraft(p.sku,p.name,99,2))
          _ <- store.deleteProduct(p.id)
          price <- sql"select price from order_items".query[Long].unique
          product <- store.product(p.id)
        yield {assertEquals(price,10L);assertEquals(product,None)})
    }
  }
  test("revocation theo jti lưu bền; token hết hạn được dọn") {
    seed() *> run(for
      _ <- store.revoke("future",4102444800L)
      _ <- store.revoke("past",1L)
      ok <- store.revoked("future")
      old <- store.revoked("past")
    yield {assert(ok);assert(!old)})
  }
  test("authorization từ service chặn đọc và checkout chéo user") {
    seed().flatMap { (u,p) =>
      val stranger=Principal(UserId.assigned(999),false)
      (shop.user(stranger,u.id),shop.checkout(stranger,u.id,List(Line(p.id,1)))).tupled.map { (a,b) =>
        assertEquals(a,Left(DomainError.Forbidden));assertEquals(b,Left(DomainError.Forbidden))
      }
    }
  }
  test("CHECK constraint là tuyến phòng thủ cuối") {
    seed().flatMap { (_,p) =>
      run(sql"update products set stock=-1 where id=${p.id}".update.run).attempt.map { result =>
        assertEquals(result.swap.toOption.get.asInstanceOf[java.sql.SQLException].getSQLState,"23514")
      }
    }
  }
