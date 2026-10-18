package vn.scalaeng.sec

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.comcast.ip4s.{host, port}
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.{AuthScheme, Credentials, HttpRoutes, Method, Request, Status, Uri}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import sttp.tapir.server.http4s.Http4sServerInterpreter
import org.http4s.headers.{Authorization, Origin}
import org.http4s.implicits.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

class ProductionSuite extends CatsEffectSuite:
  private val boom: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "boom" => IO.raiseError(IllegalStateException("bí mật nội bộ: password=hunter2"))
    case GET -> Root / "slow" => IO.sleep(1.second) *> Ok("xong")
  }

  private def built(logs: Option[Ref[IO, Vector[String]]] = None) =
    AppWiring.build(Fixtures.config, Fixtures.clock(), logs.map(r => (s: String) => r.update(_ :+ s)), boom)

  private def loginReq(pw: String) =
    Request[IO](Method.POST, uri"/api/v1/auth/login").withEntity(LoginRequest("an@x.vn", pw).asJson)

  test("rate limit đăng nhập: quá 3 lần/phút -> 429 + Retry-After; endpoint khác không bị ảnh hưởng"):
    built().use { b =>
      for
        statuses <- (1 to 4).toList.traverse(_ => b.app.run(loginReq("sai-mat-khau-2026")).map(r => r.status -> r.headers.get(CIString("Retry-After"))))
        health   <- b.app.run(Request[IO](Method.GET, uri"/health/live"))
      yield
        assertEquals(statuses.map(_._1.code), List(401, 401, 401, 429))
        assert(statuses.last._2.isDefined)
        assertEquals(health.status, Status.Ok)
    }

  test("biến thể path của login (/login/, %6Cogin) DÙNG CHUNG quota và metric (lỗi bản đầu, mục 10.7)"):
    built().use { b =>
      val paths = List(uri"/api/v1/auth/login", uri"/api/v1/auth/login/", uri"/api/v1/auth/%6Cogin", uri"/api/v1/auth/login/")
      for
        statuses <- paths.traverse(u =>
                      b.app.run(Request[IO](Method.POST, u).withEntity(LoginRequest("an@x.vn", "sai-mat-khau-2026").asJson)).map(_.status.code))
        text     <- b.app.run(Request[IO](Method.GET, uri"/metrics")).flatMap(_.bodyText.compile.string)
      yield
        assertEquals(statuses, List(401, 401, 401, 429))
        assert(text.contains("login_failures_total 3.0"), text.linesIterator.filter(_.contains("login")).mkString("\n"))
    }

  test("exception trong serverLogic CỦA TAPIR cũng thành Problem 500 có requestId (lỗi bản đầu, mục 10.10)"):
    import sttp.tapir.*
    val failing = Http4sServerInterpreter[IO](Api.serverOptions).toRoutes(
      endpoint.get.in("tapir-boom").out(stringBody).serverLogic[IO](_ => IO.raiseError(IllegalStateException("bí mật nội bộ trong Tapir")))
    )
    AppWiring.build(Fixtures.config, Fixtures.clock(), None, failing).use { b =>
      for
        resp <- b.app.run(Request[IO](Method.GET, uri"/tapir-boom"))
        body <- resp.as[Json]
      yield
        assertEquals(resp.status, Status.InternalServerError)
        assert(!body.noSpaces.contains("bí mật nội bộ"), body.noSpaces)
        assert(body.hcursor.get[String]("requestId").exists(_.length >= 32), body.noSpaces)
    }

  test("token bucket hồi token theo thời gian (thời gian ảo của TestControl)"):
    val program =
      for
        limiter <- RateLimiter.make(capacity = 3, per = 1.minute)
        burst   <- (1 to 4).toList.traverse(_ => limiter.acquire("ip-1"))
        _       <- IO.sleep(20.seconds) // hồi 1 token (3 token / 60 giây)
        later   <- limiter.acquire("ip-1")
        again   <- limiter.acquire("ip-1")
      yield (burst.map(_.isRight), later.isRight, again.left.map(_.toSeconds))
    TestControl.executeEmbed(program).map { case (burst, later, again) =>
      assertEquals(burst, List(true, true, true, false))
      assertEquals(later, true)
      assertEquals(again, Left(20L))
    }

  test("CORS: origin được phép nhận Access-Control-Allow-Origin; origin lạ thì không"):
    def preflight(origin: String) =
      Request[IO](Method.OPTIONS, uri"/api/v1/me")
        .putHeaders(Origin.parse(origin).toOption.get, "Access-Control-Request-Method" -> "GET")
    built().use { b =>
      for
        ok  <- b.app.run(preflight("https://shop.scalaeng.vn"))
        bad <- b.app.run(preflight("https://evil.example"))
      yield
        assertEquals(ok.headers.get(CIString("Access-Control-Allow-Origin")).map(_.head.value), Some("https://shop.scalaeng.vn"))
        assertEquals(bad.headers.get(CIString("Access-Control-Allow-Origin")), None)
    }

  test("log request che header Authorization"):
    Ref.of[IO, Vector[String]](Vector.empty).flatMap { logs =>
      built(Some(logs)).use { b =>
        val req = Request[IO](Method.GET, uri"/api/v1/me")
          .putHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, "eyJ-token-that-must-not-leak")))
        b.app.run(req) *> logs.get.map { lines =>
          val all = lines.mkString("\n")
          assert(all.contains("Authorization"), all)
          assert(!all.contains("eyJ-token-that-must-not-leak"), all)
        }
      }
    }

  test("exception chưa xử lý -> 500 Problem có requestId, KHÔNG lộ thông điệp nội bộ"):
    built().use { b =>
      for
        resp <- b.app.run(Request[IO](Method.GET, uri"/boom"))
        body <- resp.as[Json]
      yield
        assertEquals(resp.status, Status.InternalServerError)
        assert(!body.noSpaces.contains("hunter2"), body.noSpaces)
        assert(body.hcursor.get[String]("requestId").exists(_.length >= 32), body.noSpaces)
    }

  test("readiness: database DOWN -> 503; liveness vẫn 200"):
    built().use { b =>
      for
        _     <- b.dbUp.set(false)
        ready <- b.app.run(Request[IO](Method.GET, uri"/health/ready"))
        body  <- ready.as[Json]
        live  <- b.app.run(Request[IO](Method.GET, uri"/health/live"))
      yield
        assertEquals((ready.status, live.status), (Status.ServiceUnavailable, Status.Ok))
        assertEquals(body.noSpaces, """{"database":"DOWN"}""")
    }

  test("/metrics xuất metric HTTP và metric nghiệp vụ login_failures_total"):
    built().use { b =>
      for
        _    <- b.app.run(loginReq("sai-mat-khau-2026"))
        _    <- b.app.run(Request[IO](Method.GET, uri"/api/v1/me")) // 401 nhưng KHÔNG phải đăng nhập thất bại
        // KHÔNG dùng `.as[String]`: CirceEntityCodec.* đã import biến decoder String thành "parse JSON rồi lấy chuỗi".
        text <- b.app.run(Request[IO](Method.GET, uri"/metrics")).flatMap(_.bodyText.compile.string)
      yield
        assert(text.contains("login_failures_total 1.0"), text.linesIterator.filter(_.contains("login")).mkString("\n"))
        assert(text.contains("http_response_duration_seconds"), text.take(500))
    }

  test("graceful shutdown: request đang chạy được trả lời; sau khi dừng không nhận kết nối mới"):
    val server = built().flatMap(b =>
      EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0")
        .withHttpApp(b.app).withShutdownTimeout(5.seconds).build
    )
    EmberClientBuilder.default[IO].build.use { client =>
      server.allocated.flatMap { (srv, release) =>
        val base = Uri.unsafeFromString(s"http://127.0.0.1:${srv.address.getPort}")
        for
          inFlight <- client.expect[String](base / "slow").start
          _        <- IO.sleep(300.millis)          // request /slow đã vào server, đang ngủ 1 giây
          _        <- release                       // bắt đầu shutdown TRONG LÚC request đang chạy
          answer   <- inFlight.joinWithNever
          after    <- client.expect[String](base / "health" / "live").attempt
        yield
          assertEquals(answer, "xong")
          assert(after.isLeft, after.toString)
      }
    }
