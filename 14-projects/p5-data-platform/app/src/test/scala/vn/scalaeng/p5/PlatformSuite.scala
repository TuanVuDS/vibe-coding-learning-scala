package vn.scalaeng.p5

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import io.circe.Json
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.{Status, Uri}
import org.typelevel.doobie.{Fragment, Transactor}
import org.typelevel.doobie.implicits.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.time.{Instant, LocalDate}
import scala.concurrent.duration.*

/** PostgreSQL THẬT (nhúng, zonky) — opt-in RUN_DB_TESTS=1 như Project 2: không bật thì test tự bỏ qua, bật mà lỗi thì FAIL (không giấu lỗi). */
class PlatformSuite extends munit.CatsEffectSuite:
  import Fixture.*
  override def munitIOTimeout: Duration = 3.minutes
  private val now = Instant.parse("2026-10-17T10:00:00Z")

  private val pg = ResourceSuiteLocalFixture("postgres",
    if sys.env.get("RUN_DB_TESTS").contains("1") then Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(p => IO.blocking(p.close())).map(Some(_))
    else Resource.pure[IO, Option[EmbeddedPostgres]](None))
  override def munitFixtures = List(pg)

  /** Mỗi test một DATABASE riêng trong cùng server — test không giẫm lên nhau. */
  private def freshDb(name: String): IO[ServingDb] =
    assume(pg().isDefined, "đặt RUN_DB_TESTS=1 để chạy PostgreSQL thật (nhúng)")
    val server = pg().get
    val admin = Transactor.fromDriverManager[IO]("org.postgresql.Driver", server.getJdbcUrl("postgres", "postgres"), "postgres", "postgres", None)
    // CREATE DATABASE không chạy được trong transaction — dùng autocommit. Tên database là định danh, không phải tham số `?` được:
    // ghép bằng Fragment.const (chỉ với tên do test tự đặt — KHÔNG BAO GIỜ với input người dùng). `#$name` không được nội suy ở bản
    // Doobie này: SQL gửi đi còn nguyên dấu `#` (lỗi thật: syntax error at or near "#").
    val create = Fragment.const(s"DROP DATABASE IF EXISTS $name").update.run *> Fragment.const(s"CREATE DATABASE $name").update.run
    org.typelevel.doobie.FC.setAutoCommit(true).flatMap(_ => create).transact(Transactor.strategy.set(admin, org.typelevel.doobie.util.transactor.Strategy.void)) *>
      IO {
        ServingDb(Transactor.fromDriverManager[IO]("org.postgresql.Driver", server.getJdbcUrl("postgres", name), "postgres", "postgres", None))
      }.flatTap(_.migrate)

  private def publishRange(l: Lake, db: ServingDb, from: LocalDate, to: LocalDate, runId: String): IO[List[String]] =
    Orchestrator.days(from, to).toList.traverse(d => Main.publish(l, db, d, runId, now).map(r => s"$d ${r.fold(_.mkString(";"), identity)}"))

  test("serving: nạp một ngày trong một transaction; nạp lại cùng hash = unchanged; daily đổi → thay đúng ngày đó") {
    for
      db <- freshDb("t_load")
      l = newLake()
      _ = Seq(d15, d16).foreach(landDay(l, _))
      _ = Orchestrator.backfill(l, Assets.all, d15, d16, "r1")
      first <- publishRange(l, db, d15, d16, "r1")
      again <- publishRange(l, db, d15, d16, "r2")
      _ = landDay(l, d17)
      _ = Orchestrator.backfill(l, Assets.all, d15, d17, "r3")
      third <- publishRange(l, db, d15, d17, "r3")
      rows <- db.daily(d15, d17)
    yield
      assertEquals(first, List("2026-10-15 loaded", "2026-10-16 loaded"))
      assertEquals(again, List("2026-10-15 unchanged", "2026-10-16 unchanged"))
      assertEquals(third, List("2026-10-15 unchanged", "2026-10-16 loaded", "2026-10-17 loaded"))
      assertEquals(rows.map(r => s"${r.day} ${r.segment} ${r.revenueVnd}"), List(
        "2026-10-15 Retail 700000", "2026-10-15 UNKNOWN 450000", "2026-10-15 Wholesale 20000000", "2026-10-17 Retail 1890000"))
  }

  test("TIÊU CHÍ P5: backfill 3 ngày một lần cho serving GIỐNG HỆT chạy hằng ngày khi từng lô đến") {
    for
      dbA <- freshDb("t_daily")
      dbB <- freshDb("t_backfill")
      a = newLake()
      // A: mỗi ngày một lô đến, orchestrator chạy (chỉ partition cũ/thiếu) rồi publish các ngày đã có.
      dailyRuns <- List(d15, d16, d17).traverse { d =>
        IO(landDay(a, d)) *> IO(Orchestrator.backfill(a, Assets.all, d15, d, s"daily-$d")) *> publishRange(a, dbA, d15, d, s"daily-$d")
      }
      b = newLake()
      // B: cả ba lô đến cùng lúc (ví dụ sau sự cố), một lần backfill 15..17.
      _ <- IO(Seq(d15, d16, d17).foreach(landDay(b, _))) *> IO(Orchestrator.backfill(b, Assets.all, d15, d17, "backfill"))
      backfill <- publishRange(b, dbB, d15, d17, "backfill")
      ra <- dbA.daily(d15, d17); rb <- dbB.daily(d15, d17)
      ma <- dbA.monthly; mb <- dbB.monthly
      la <- dbA.loads; lb <- dbB.loads
    yield
      assertEquals(ra, rb)
      assertEquals(ma, mb)
      assertEquals(la.map(x => (x.day, x.outputHash)), lb.map(x => (x.day, x.outputHash))) // cùng output byte-for-byte
      // Lô 16 mang hoàn tiền O1 và đơn trễ O8 của NGÀY 15 → lần chạy ngày 16 nạp lại ngày 15; lô 17 chạm ngày 16 → nạp lại ngày 16.
      assertEquals(dailyRuns.flatten, List("2026-10-15 loaded", "2026-10-15 loaded", "2026-10-16 loaded",
        "2026-10-15 unchanged", "2026-10-16 loaded", "2026-10-17 loaded"))
      assertEquals(backfill, List("2026-10-15 loaded", "2026-10-16 loaded", "2026-10-17 loaded"))
      assertEquals(ma, List(("2026-10", "Retail", BigDecimal(2590000), 4L), ("2026-10", "UNKNOWN", BigDecimal(450000), 1L),
        ("2026-10", "Wholesale", BigDecimal(20000000), 1L)))
  }

  test("quality gate chặn: daily bị sửa tay → không nạp, serving giữ bản cũ") {
    for
      db <- freshDb("t_gate")
      l = newLake()
      _ = landDay(l, d15)
      _ = Orchestrator.backfill(l, Assets.all, d15, d15, "r")
      _ <- Main.publish(l, db, d15, "r", now)
      _ = Files.write(l.partFile(DailyRevenue, d15), "segment,revenue_vnd,order_lines,orders\nRetail,1,1,1\n".getBytes(UTF_8))
      r <- Main.publish(l, db, d15, "r2", now)
      rows <- db.daily(d15, d15)
    yield
      assertEquals(r, Left(List("partition cũ — tính lại trước khi publish",
        "daily không khớp staging: PAID 21600000 VND / 4 dòng, daily 1 VND / 1 dòng", "daily trên đĩa khác bảng tính lại từ staging")))
      assertEquals(rows.map(_.revenueVnd).sum, BigDecimal(21600000))
  }

  test("API + monitoring qua HTTP thật: daily, monthly (view SQL), 400 khi khoảng sai, readiness, metrics") {
    val client = EmberClientBuilder.default[IO].build
    for
      db <- freshDb("t_api")
      l = newLake()
      _ = Seq(d15, d16, d17).foreach(landDay(l, _))
      _ = Orchestrator.backfill(l, Assets.all, d15, d17, "r")
      _ <- publishRange(l, db, d15, d17, "r")
      res <- EmberServerBuilder.default[IO].withHost(Host.fromString("127.0.0.1").get).withPort(Port.fromInt(0).get)
        .withHttpApp(Api.routes(db, () => now).orNotFound).build.both(client).use { (server, c) =>
          val base = Uri.unsafeFromString(s"http://127.0.0.1:${server.address.getPort}")
          (c.expect[Json](base / "api" / "v1" / "revenue" / "daily" +? ("from", "2026-10-17") +? ("to", "2026-10-17")),
            c.expect[Json](base / "api" / "v1" / "revenue" / "monthly"),
            c.status(org.http4s.Request[IO](uri = base / "api" / "v1" / "revenue" / "daily" +? ("from", "2026-10-17") +? ("to", "2026-10-01"))),
            c.status(org.http4s.Request[IO](uri = base / "health" / "ready")),
            c.expect[String](base / "metrics")).tupled
        }
    yield
      val (daily, monthly, bad, ready, metrics) = res
      assertEquals(daily.noSpaces, """[{"day":"2026-10-17","segment":"Retail","revenue_vnd":1890000,"order_lines":2,"orders":2}]""")
      assertEquals(monthly.asArray.map(_.size), Some(3))
      assertEquals((bad, ready), (Status.BadRequest, Status.Ok))
      assert(metrics.contains("p5_serving_days_loaded 3") && metrics.contains("p5_serving_last_day_epoch_day 20743"), metrics)
  }

  // Hai test dưới KHÔNG cần PostgreSQL: chạy cả khi RUN_DB_TESTS chưa bật.
  test("review P5 N1: API báo 400 (không 404) khi thiếu/lặp tham số; 366 ngày tính cả hai đầu") {
    val noDb = ServingDb(Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:1/none", "x", "x", None))
    def status(q: String) = Api.routes(noDb, () => now).orNotFound.run(org.http4s.Request[IO](uri = Uri.unsafeFromString(s"/api/v1/revenue/daily$q"))).map(_.status)
    List("", "?from=2026-10-15", "?from=bad&to=2026-10-15", "?from=2026-10-15&to=2026-10-15&to=bad", "?from=2026-01-01&to=2027-01-02")
      .traverse(status).map(ss => assertEquals(ss.distinct, List(Status.BadRequest)))
      .map { _ =>
        assertEquals(Api.range(Map("from" -> Seq("2026-01-01"), "to" -> Seq("2027-01-01"))).map(_ => "ok"), Right("ok")) // 366 ngày
        assertEquals(Api.range(Map("from" -> Seq("2026-01-01"), "to" -> Seq("2027-01-02"))), Left("tối đa 366 ngày (tính cả hai đầu)"))
        assertEquals(Api.range(Map("from" -> Seq("2026-10-15"))), Left("thiếu to"))
      }
  }

  test("R2-N1: API từ chối năm mở rộng đã URL-encode và ngày không tồn tại theo Order.dayOf") {
    val noDb = ServingDb(Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:1/none", "x", "x", None))
    List("%2B02026-10-15", "2026-02-30", "2026-1-01").traverse { day =>
      val uri = Uri.unsafeFromString(s"/api/v1/revenue/daily?from=$day&to=2026-10-15")
      Api.routes(noDb, () => now).orNotFound.run(org.http4s.Request[IO](uri = uri)).flatMap { response =>
        response.as[Json].map { body =>
          assertEquals(response.status, Status.BadRequest)
          assertEquals(body.hcursor.get[String]("error"), Right("from phải là ngày yyyy-MM-dd"))
        }
      }
    }.void
  }

  test("review P5 M4: lệnh `reconcile` (entry point thật) công bố dòng raw không thuộc ngày nào") {
    val l = newLake()
    Seq(d15, d16, d17).foreach(landDay(l, _))
    Main.run(List("reconcile", s"--root=${l.root}")).map { code =>
      val report = new String(Files.readAllBytes(l.root.resolve("_quality/raw_reconcile.json")), UTF_8)
      assertEquals(code, cats.effect.ExitCode.Success)
      assert(report.contains("\"unassigned\":1") && report.contains("E1506 day='2026-13-01'") && report.contains("\"balanced\":true"), report)
    }
  }
