package vn.scalaeng.sec

import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.{AuthScheme, Credentials, HttpApp, Method, Request, Status, Uri}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString
import org.http4s.implicits.*

/** Kịch bản xác thực/phân quyền chạy trên HttpApp hoàn chỉnh (đủ middleware), không mở cổng mạng. */
class ApiSuite extends CatsEffectSuite:
  private val app: Resource[IO, (HttpApp[IO], UserStore)] =
    AppWiring.build(Fixtures.config, Fixtures.clock()).evalTap { b =>
      b.users.register("admin@scalaeng.vn", "Quản trị", "mat-khau-quan-tri-2026", Role.Admin) *>
        b.users.register("an@scalaeng.vn", "An", "mat-khau-cua-an-2026")
    }.map(b => (b.app, b.users))

  private def json(method: Method, uri: Uri, body: Json) = Request[IO](method, uri).withEntity(body)
  private def bearer(r: Request[IO], token: String)      = r.putHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, token)))

  private def login(a: HttpApp[IO], email: String, password: String): IO[String] =
    a.run(json(Method.POST, uri"/api/v1/auth/login", Json.obj("email" -> email.asJson, "password" -> password.asJson)))
      .flatMap(_.as[TokenResponse]).map(_.accessToken)

  test("đăng ký: mật khẩu yếu -> 422 liệt kê lý do; email trùng (khác hoa thường) -> 409"):
    app.use { (a, _) =>
      for
        weak <- a.run(json(Method.POST, uri"/api/v1/auth/register",
                  RegisterRequest("binh@x.vn", "Bình", "ngan").asJson))
        p    <- weak.as[Problem]
        dup  <- a.run(json(Method.POST, uri"/api/v1/auth/register",
                  RegisterRequest("AN@scalaeng.vn", "An 2", "mot-mat-khau-khac-2026").asJson))
      yield
        assertEquals((weak.status.code, p.errors), (422, List("mật khẩu phải có ít nhất 15 ký tự")))
        assertEquals(dup.status, Status.Conflict)
    }

  test("đăng nhập sai: CÙNG thông báo cho email không tồn tại và mật khẩu sai"):
    app.use { (a, _) =>
      def attempt(email: String, pw: String) =
        a.run(json(Method.POST, uri"/api/v1/auth/login", LoginRequest(email, pw).asJson)).flatMap(r => r.as[Problem].map(r.status -> _))
      for
        wrongPw   <- attempt("an@scalaeng.vn", "sai-mat-khau-roi-2026")
        noAccount <- attempt("khong-ton-tai@scalaeng.vn", "sai-mat-khau-roi-2026")
      yield
        assertEquals(wrongPw._1, Status.Unauthorized)
        assertEquals(wrongPw, noAccount)
    }

  test("không có token -> 401 kèm WWW-Authenticate; token rác -> 401"):
    app.use { (a, _) =>
      for
        none    <- a.run(Request[IO](Method.GET, uri"/api/v1/me"))
        problem <- none.as[Problem]
        garbage <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/me"), "abc.def.ghi"))
      yield
        assertEquals(none.status, Status.Unauthorized)
        // Tra theo TÊN header: `headers.get[`WWW-Authenticate`]` (typed) trả None vì "Bearer" không kèm realm không parse được.
        assertEquals(none.headers.get(CIString("WWW-Authenticate")).map(_.head.value), Some("Bearer"))
        assertEquals(garbage.status, Status.Unauthorized)
        // status trong body khớp status HTTP (lỗi thật của bản đầu: HTTP 401 nhưng body ghi 400)
        assertEquals(problem.status, 401)
        assertEquals(problem.detail, "Invalid value for: header Authorization (missing)")
    }

  test("token hợp lệ -> /me trả đúng người, không có passwordHash trong response"):
    app.use { (a, _) =>
      for
        t    <- login(a, "an@scalaeng.vn", "mat-khau-cua-an-2026")
        resp <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/me"), t))
        body <- resp.as[Json]
      yield
        assertEquals(resp.status, Status.Ok)
        assertEquals(body.hcursor.get[String]("email"), Right("an@scalaeng.vn"))
        assertEquals(body.hcursor.get[String]("role"), Right("Customer")) // chuỗi, không phải {"Customer":{}}
        assert(!body.noSpaces.contains("argon2"), body.noSpaces)
    }

  test("phân quyền theo vai trò: Customer -> 403, Admin -> 200"):
    app.use { (a, _) =>
      for
        an    <- login(a, "an@scalaeng.vn", "mat-khau-cua-an-2026")
        admin <- login(a, "admin@scalaeng.vn", "mat-khau-quan-tri-2026")
        r1    <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/admin/users"), an))
        r2    <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/admin/users"), admin))
      yield assertEquals((r1.status, r2.status), (Status.Forbidden, Status.Ok))
    }

  test("chống IDOR: An (id=2) không xem được hồ sơ Admin (id=1), nhưng xem được của mình"):
    app.use { (a, _) =>
      for
        an    <- login(a, "an@scalaeng.vn", "mat-khau-cua-an-2026")
        other <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/users/1"), an))
        own   <- a.run(bearer(Request[IO](Method.GET, uri"/api/v1/users/2"), an))
      yield assertEquals((other.status, own.status), (Status.Forbidden, Status.Ok))
    }

  test("email được chuẩn hóa NHẤT QUÁN: ' AN@ScalaEng.vn ' đăng nhập được tài khoản an@scalaeng.vn"):
    app.use { (a, _) =>
      a.run(json(Method.POST, uri"/api/v1/auth/login", LoginRequest(" AN@ScalaEng.vn ", "mat-khau-cua-an-2026").asJson))
        .map(r => assertEquals(r.status, Status.Ok))
    }

