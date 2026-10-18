package vn.scalaeng.p2.app

import cats.effect.{IO,Resource}
import cats.syntax.all.*
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import scala.concurrent.duration.*
import vn.scalaeng.p2.db.*
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.security.Passwords

class BootstrapAdminSuite extends munit.CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes
  private val realDb = for
    pg <- Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(p => IO.blocking(p.close()))
    cfg = new DbConfig(pg.getJdbcUrl("postgres","postgres"),"postgres","postgres",4)
    _ <- Resource.eval(Database.migrate(cfg))
    xa <- Database.transactor(cfg)
  yield xa: Transactor[IO]
  private val fixture = ResourceSuiteLocalFixture("bootstrap-postgres",
    if sys.env.get("RUN_DB_TESTS").contains("1") then realDb.map(Some(_))
    else Resource.pure[IO,Option[Transactor[IO]]](None))
  override def munitFixtures = List(fixture)
  private def xa: Transactor[IO] =
    assume(fixture().isDefined,"đặt RUN_DB_TESTS=1 ngoài sandbox để chạy PostgreSQL thật")
    fixture().get
  private def run[A](program: ConnectionIO[A]): IO[A] = IO.defer(program.transact(xa))
  private val store = new PgStore
  private val email = "admin@example.vn"
  private val password = "một câu bí mật dài năm 2026"
  private def reset: IO[Unit] = run(sql"truncate users restart identity cascade".update.run).void
  private def create: IO[Either[DomainError,User]] = IO.defer(BootstrapAdmin.create(xa,email,password))

  test("bootstrap lưu Admin và Argon2id; chạy lại không sửa account/hash") {
    reset *> (for
      created <- create
      account <- run(store.account(email)).map(_.get)
      valid <- Passwords.verify(password,account.hash)
      duplicate <- create
      after <- run(store.account(email))
    yield {
      assertEquals(created,Right(account.user));assertEquals(account.role,"Admin")
      assert(account.hash.startsWith("$argon2id$"));assert(valid)
      assertEquals(duplicate,Left(DomainError.DuplicateEmail(email)));assertEquals(after,Some(account))
    })
  }
  test("email Customer có sẵn không bao giờ được tự nâng quyền") {
    reset *> (for
      _ <- run(store.register(UserDraft("An",email),"original-hash"))
      before <- run(store.account(email))
      result <- create
      after <- run(store.account(email))
    yield {
      assertEquals(result,Left(DomainError.DuplicateEmail(email)));assertEquals(after,before)
      assertEquals(after.get.role,"Customer");assertEquals(after.get.hash,"original-hash")
    })
  }
  test("promotion lỗi rollback cả registration; bỏ lỗi rồi chạy lại tạo được Admin") {
    val install = Fragment.const("""create function fail_admin() returns trigger language plpgsql as $$
      begin if new.role = 'Admin' then raise exception 'forced promotion failure'; end if; return new; end; $$""").update.run *>
      Fragment.const("create trigger fail_admin before update on users for each row execute function fail_admin()").update.run
    val cleanup = Fragment.const("drop trigger if exists fail_admin on users").update.run *>
      Fragment.const("drop function if exists fail_admin()").update.run
    reset *> (run(install) *> (for
      failed <- create.attempt
      account <- run(store.account(email))
    yield {
      assertEquals(failed.swap.toOption.get.asInstanceOf[java.sql.SQLException].getSQLState,"P0001")
      assertEquals(account,None)
    })).guarantee(run(cleanup).void) *> (for
      retried <- create
      account <- run(store.account(email))
    yield {assert(retried.isRight);assertEquals(account.get.role,"Admin")})
  }
  test("input không hợp lệ không ghi user") {
    reset *> (for
      invalid <- IO.defer(BootstrapAdmin.create(xa,email,"ngắn"))
      account <- run(store.account(email))
    yield {assert(invalid.isLeft);assertEquals(account,None)})
  }
