package vn.scalaeng.p2.db

import cats.syntax.all.*
import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariConfig
import org.flywaydb.core.Flyway
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.hikari.HikariTransactor
import scala.concurrent.duration.*
import vn.scalaeng.p2.service.TransactionRunner

final class DbConfig(val url: String, val user: String, val password: String, val poolSize: Int):
  override def toString: String = "DbConfig(****)"

object Database:
  def migrate(c: DbConfig): IO[Int] = IO.blocking {
    Flyway.configure().dataSource(c.url,c.user,c.password).load().migrate().migrationsExecuted
  }
  def transactor(c: DbConfig): Resource[IO, HikariTransactor[IO]] =
    val h = new HikariConfig()
    h.setJdbcUrl(c.url)
    h.setUsername(c.user)
    h.setPassword(c.password)
    h.setMaximumPoolSize(c.poolSize)
    h.setConnectionTimeout(2000L)
    h.setPoolName("p2-db")
    // JDBC bounds complement the effect timeout; cancellation of blocking JDBC is not instantaneous.
    h.addDataSourceProperty("connectTimeout", "2")
    h.addDataSourceProperty("socketTimeout", "5")
    HikariTransactor.fromHikariConfig[IO](h)
  def runner(xa: Transactor[IO]): TransactionRunner[ConnectionIO] = new TransactionRunner[ConnectionIO]:
    def run[A](program: ConnectionIO[A]): IO[A] = program.transact(xa)
  def ready(xa: Transactor[IO]): IO[Boolean] =
    (sql"set local statement_timeout = '800ms'".update.run *>
      sql"select 1".query[Int].unique).transact(xa).map(_ == 1).timeout(1.second).handleError(_ => false)
