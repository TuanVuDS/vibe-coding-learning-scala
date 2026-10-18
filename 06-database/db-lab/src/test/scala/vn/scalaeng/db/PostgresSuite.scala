package vn.scalaeng.db

import cats.effect.{IO, Resource}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import munit.CatsEffectSuite
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import scala.concurrent.duration.*

/** Khung test chung: MỘT PostgreSQL thật (nhúng, chạy tiến trình `postgres` thật) cho cả suite,
  * migrate bằng Flyway như production, và dọn dữ liệu TRƯỚC mỗi test để các test độc lập.
  */
abstract class PostgresSuite extends CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes

  private val db: Resource[IO, (DbConfig, Transactor[IO])] =
    for
      pg  <- Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(pg => IO.blocking(pg.close()))
      cfg  = DbConfig(pg.getJdbcUrl("postgres", "postgres"), "postgres", "postgres", poolSize = 4)
      _   <- Resource.eval(Database.migrate(cfg))
      xa  <- Database.transactor(cfg)
    yield (cfg, xa)

  val database = ResourceSuiteLocalFixture("postgres", db)
  override def munitFixtures = List(database)

  def xa: Transactor[IO] = database()._2
  def config: DbConfig   = database()._1

  override def beforeEach(context: BeforeEach): Unit =
    import cats.effect.unsafe.implicits.global
    sql"truncate users, products, orders, order_items, page_views restart identity cascade".update.run
      .transact(xa).void.unsafeRunSync()
