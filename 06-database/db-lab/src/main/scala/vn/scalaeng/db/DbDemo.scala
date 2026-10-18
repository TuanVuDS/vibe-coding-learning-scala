package vn.scalaeng.db

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import org.typelevel.doobie.implicits.*

/** Chạy tay với PostgreSQL thật: `docker compose up -d` rồi
  * `scala-cli run . --main-class vn.scalaeng.db.DbDemo`. Chạy lại nhiều lần được (idempotent theo email/sku).
  */
object DbDemo extends IOApp.Simple:
  def run: IO[Unit] =
    val cfg = DbConfig.fromEnv(sys.env)
    Database.migrate(cfg).flatMap(n => IO.println(s"migration mới áp dụng: $n")) *>
      Database.transactor(cfg).use { xa =>
        val service = OrderService(xa)
        for
          user <- UserRepo.findByEmail("an@example.vn").flatMap {
                    case Some(u) => u.pure[org.typelevel.doobie.ConnectionIO]
                    case None    => UserRepo.create("an@example.vn", "Nguyễn An").map(_.fold(e => sys.error(e.toString), identity))
                  }.transact(xa)
          sku  <- IO.realTime.map(t => s"DEMO-${t.toMillis}")
          pid  <- ProductRepo.create(sku, "Bàn phím", 450_000, 3).transact(xa)
          ok   <- service.placeOrder(user.id, List(OrderLine(pid, 2)))
          bad  <- service.placeOrder(user.id, List(OrderLine(pid, 2)))
          left <- ProductRepo.find(pid).transact(xa)
          _    <- IO.println(s"user: ${user.email} (id=${user.id.value})")
          _    <- IO.println(s"đơn 1: ${ok.map(o => (o.status, o.totalVnd))}")
          _    <- IO.println(s"đơn 2: $bad")
          _    <- IO.println(s"tồn kho còn: ${left.map(_.stock)}")
        yield ()
      }
