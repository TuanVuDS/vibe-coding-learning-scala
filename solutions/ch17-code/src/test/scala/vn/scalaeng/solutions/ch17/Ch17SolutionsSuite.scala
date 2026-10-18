package vn.scalaeng.solutions.ch17

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.comcast.ip4s.{host, port}
import io.circe.syntax.*
import java.nio.file.Files
import munit.CatsEffectSuite
import org.http4s.{Header, HttpApp, HttpRoutes, Method, Request, Status, Uri}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*
import scala.sys.process.*
import vn.scalaeng.sec.*

class Ch17SolutionsSuite extends CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes

  private val config = SecurityConfig.load(Map("JWT_SECRET" -> "loi-giai-chuong-17-secret-0123456789")).toOption.get
  private val clock  = java.time.Clock.systemUTC()

  private def withAuth(scheme: String, token: String)(uri: Uri) =
    Request[IO](Method.GET, uri).putHeaders(Header.Raw(CIString("Authorization"), s"$scheme $token"))

  private def loginToken(app: HttpApp[IO]): IO[String] =
    app.run(Request[IO](Method.POST, uri"/api/v1/auth/login").withEntity(LoginRequest("an@scalaeng.vn", "mat-khau-cua-an-2026").asJson))
      .flatMap(_.as[TokenResponse]).map(_.accessToken)

  private def wired(revocation: Option[Revocation] = None) =
    AppWiring.build(config, clock, isRevoked = revocation.fold((_: VerifiedToken) => IO.pure(false))(r => r.isRevoked))
      .evalTap(_.users.register("an@scalaeng.vn", "An", "mat-khau-cua-an-2026"))

  test("bài 1: token sai -> WWW-Authenticate có error=\"invalid_token\"; không token -> giữ nguyên `Bearer`"):
    wired().use { b =>
      val app = InvalidTokenChallenge(HttpRoutes(req => cats.data.OptionT.liftF(b.app.run(req)))).orNotFound
      for
        bad  <- app.run(withAuth("Bearer", "abc.def.ghi")(uri"/api/v1/me"))
        none <- app.run(Request[IO](Method.GET, uri"/api/v1/me"))
      yield
        assertEquals(bad.headers.get(CIString("WWW-Authenticate")).map(_.head.value), Some("Bearer error=\"invalid_token\""))
        assertEquals(none.headers.get(CIString("WWW-Authenticate")).map(_.head.value), Some("Bearer"))
    }

  test("bài 2: header bảo mật; HSTS chỉ khi bật HTTPS"):
    val app = HttpRoutes.of[IO] { case GET -> Root / "x" => Ok("ok") }.orNotFound
    for
      plain <- SecurityHeaders(httpsOnly = false)(app).run(Request[IO](Method.GET, uri"/x"))
      tls   <- SecurityHeaders(httpsOnly = true)(app).run(Request[IO](Method.GET, uri"/x"))
    yield
      assertEquals(plain.headers.get(CIString("X-Content-Type-Options")).map(_.head.value), Some("nosniff"))
      assertEquals(plain.headers.get(CIString("Strict-Transport-Security")), None)
      assert(tls.headers.get(CIString("Strict-Transport-Security")).isDefined)

  /** Đổi ký tự base64url CUỐI của chữ ký HS256 (32 byte → 43 ký tự, 2 bit cuối thừa) sang ký tự tương đương. */
  private def equivalentSignature(token: String): String =
    val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    val last     = alphabet.indexOf(token.last)
    token.dropRight(1) + alphabet((last & ~3) | ((last + 1) & 3))

  test("bài 3: thu hồi theo jti — chặn được cả `bearer` viết thường và chữ ký viết khác (hai lỗi review tìm ra)"):
    Revocation.make.flatMap { revocation =>
      wired(Some(revocation)).use { b =>
        val tokens = Tokens(config, clock)
        for
          token    <- loginToken(b.app)
          variant   = equivalentSignature(token)
          _        <- IO(assert(variant != token && tokens.verify(variant).isRight, "biến thể phải vẫn là chữ ký hợp lệ"))
          before   <- b.app.run(withAuth("Bearer", token)(uri"/api/v1/me"))
          _        <- revocation.revoke(tokens.verifyClaims(token).toOption.get)
          attempts <- List(withAuth("Bearer", token), withAuth("bearer", token), withAuth("bEaReR", token), withAuth("Bearer", variant))
                        .traverse(mk => b.app.run(mk(uri"/api/v1/me")).map(_.status))
        yield
          assertEquals(before.status, Status.Ok)
          assertEquals(attempts, List.fill(4)(Status.Unauthorized))
      }
    }

  private def authService: IO[AuthService] =
    for
      users <- UserStore.empty
      _     <- users.register("an@scalaeng.vn", "An", "mat-khau-cua-an-2026")
      auth  <- AuthService.make(users, Tokens(config, clock))
    yield auth

  test("bài 4: CHỈ lần sai bị đếm; đăng nhập đúng nhiều lần không bị khóa; đúng thì xóa bộ đếm"):
    for
      auth    <- authService
      limiter <- FailureLimiter.make(max = 3, window = 15.minutes)
      ok      = limiter.guard("an@scalaeng.vn")(auth.login("an@scalaeng.vn", "mat-khau-cua-an-2026"))
      bad     = (email: String) => limiter.guard(email)(auth.login(email, "sai-mat-khau-2026"))
      correct <- List.fill(5)(ok).sequence
      twoBad  <- List.fill(2)(bad("AN@scalaeng.vn ")).sequence
      _       <- ok                                   // đúng → xóa 2 lần sai trước đó
      threeBad <- List.fill(3)(bad("an@scalaeng.vn")).sequence
      locked  <- ok                                   // đã 3 lần sai: khóa cả khi đúng mật khẩu
      other   <- bad("ai-do@scalaeng.vn")
    yield
      assert(correct.forall(_.isInstanceOf[GuardedResult.Token]), correct.toString)
      assertEquals(twoBad, List(GuardedResult.Rejected, GuardedResult.Rejected))
      assertEquals(threeBad, List.fill(3)(GuardedResult.Rejected))
      assert(locked.isInstanceOf[GuardedResult.TooManyAttempts], locked.toString)
      assertEquals(other, GuardedResult.Rejected) // email khác độc lập

  test("bài 4: 10 lần sai ĐỒNG THỜI không vượt quota 3 (giữ chỗ nguyên tử)"):
    for
      auth    <- authService
      limiter <- FailureLimiter.make(max = 3, window = 15.minutes)
      results <- List.fill(10)(limiter.guard("an@scalaeng.vn")(auth.login("an@scalaeng.vn", "sai-mat-khau-2026"))).parSequence
    yield
      assertEquals(results.count(_ == GuardedResult.Rejected), 3)
      assertEquals(results.count(_.isInstanceOf[GuardedResult.TooManyAttempts]), 7)

  test("bài 4: hết cửa sổ 15 phút thì được thử lại (thời gian ảo)"):
    val program =
      for
        limiter <- FailureLimiter.make(max = 1, window = 15.minutes)
        first   <- limiter.guard("x@y.vn")(IO.pure(None))
        blocked <- limiter.guard("x@y.vn")(IO.pure(None))
        _       <- IO.sleep(15.minutes + 1.second)
        again   <- limiter.guard("x@y.vn")(IO.pure(Some("t")))
      yield (first, blocked, again)
    TestControl.executeEmbed(program).map { (first, blocked, again) =>
      assertEquals(first, GuardedResult.Rejected)
      assertEquals(blocked, GuardedResult.TooManyAttempts(15.minutes))
      assertEquals(again, GuardedResult.Token("t"))
    }

  test("bài 5: HTTPS thật với keystore tự ký sinh bằng keytool; client tin đúng cert đó"):
    val dir      = Files.createTempDirectory("ch17-tls")
    val keystore = dir.resolve("server.p12")
    val password = "changeit-hoc-tap"
    val cmd = Seq("keytool", "-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
      "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
      "-storetype", "PKCS12", "-keystore", keystore.toString, "-storepass", password, "-keypass", password)
    for
      exit <- IO.blocking(cmd.!(ProcessLogger(_ => ())))
      _    <- IO(assertEquals(exit, 0))
      body <- Tls.context(keystore, password).flatMap { tls =>
                val app = HttpRoutes.of[IO] { case GET -> Root / "hello" => Ok("xin chào qua TLS".asJson) }.orNotFound
                for
                  server <- EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0").withTLS(tls).withHttpApp(app).build
                  client <- EmberClientBuilder.default[IO].withTLSContext(tls).build
                yield (server.address.getPort, client)
              }.use { (p, client) =>
                client.expect[String](Uri.unsafeFromString(s"https://localhost:$p/hello"))
              }
    yield assertEquals(body, "xin chào qua TLS")
