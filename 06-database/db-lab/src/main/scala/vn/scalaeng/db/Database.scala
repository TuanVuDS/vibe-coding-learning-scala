package vn.scalaeng.db

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariConfig
import org.flywaydb.core.Flyway
import org.typelevel.doobie.*
import org.typelevel.doobie.hikari.HikariTransactor

final case class DbConfig(url: String, user: String, password: String, poolSize: Int)

object DbConfig:
  /** Đọc từ biến môi trường; mặc định khớp `docker-compose.yml`. Mật khẩu production KHÔNG có mặc định (Chương 17). */
  def fromEnv(env: Map[String, String]): DbConfig =
    DbConfig(
      url = env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/scalaeng"),
      user = env.getOrElse("DB_USER", "scalaeng"),
      password = env.getOrElse("DB_PASSWORD", "scalaeng"),
      poolSize = env.get("DB_POOL_SIZE").flatMap(_.toIntOption).getOrElse(10)
    )

object Database:
  /** Chạy mọi migration chưa chạy. Trả số migration vừa áp dụng (0 nếu schema đã mới nhất). */
  def migrate(cfg: DbConfig): IO[Int] = IO.blocking {
    Flyway.configure().dataSource(cfg.url, cfg.user, cfg.password).load().migrate().migrationsExecuted
  }

  /** Connection pool HikariCP bọc trong `Resource`: pool được đóng khi ứng dụng dừng, kể cả khi lỗi hoặc bị hủy. */
  def transactor(cfg: DbConfig): Resource[IO, HikariTransactor[IO]] =
    val hc = new HikariConfig()
    hc.setDriverClassName("org.postgresql.Driver")
    hc.setJdbcUrl(cfg.url)
    hc.setUsername(cfg.user)
    hc.setPassword(cfg.password)
    hc.setMaximumPoolSize(cfg.poolSize)
    hc.setPoolName("scalaeng-db")
    HikariTransactor.fromHikariConfig[IO](hc)

  /** Kế hoạch thực thi của một truy vấn (EXPLAIN). `analyze = true` CHẠY THẬT truy vấn để đo thời gian. */
  def explain(query: Fragment, analyze: Boolean = false): ConnectionIO[List[String]] =
    (Fragment.const(if analyze then "explain (analyze, costs off, timing off, summary off)" else "explain (costs off)") ++ query)
      .query[String].to[List]
