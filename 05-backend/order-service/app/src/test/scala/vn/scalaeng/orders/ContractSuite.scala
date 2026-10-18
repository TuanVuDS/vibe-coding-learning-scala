package vn.scalaeng.orders

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
import org.http4s.{HttpApp, Method, Response, Status}
import Fixtures.*
import TransportCodecs.given

/** Không mở cổng: gọi đúng HttpApp mà Main dùng. Có thể tái sử dụng khi thay repository. */
class ContractSuite extends CatsEffectSuite:
  private def problem(response: Response[IO], code: Int): IO[Problem] = read[Problem](response).map { p =>
    assertEquals(response.status.code, code)
    assertEquals(p.status, code)
    assertEquals(p.`type`, "about:blank")
    assertEquals(response.contentType.map(ct => s"${ct.mediaType.mainType}/${ct.mediaType.subType}"), Some("application/problem+json"))
    p
  }
  private def seeded: IO[HttpApp[IO]] = for
    a <- app
    _ <- a.run(request(Method.POST, "/api/v1/users", """{"name":"Nguyễn An","email":"an@shop.vn"}"""))
  yield a

  test("POST user -> 201 Location -> GET, UTF-8 round trip"):
    for
      a <- app
      r <- a.run(request(Method.POST, "/api/v1/users", """{"name":"Nguyễn An","email":" AN@SHOP.VN "}"""))
      u <- read[UserView](r)
      g <- a.run(request(Method.GET, "/api/v1/users/1"))
      got <- read[UserView](g)
    yield
      assertEquals(r.status, Status.Created)
      assertEquals(r.headers.get(org.typelevel.ci.CIString("Location")).map(_.head.value), Some("/api/v1/users/1"))
      assertEquals(got, u)
      assertEquals((u.name, u.email), ("Nguyễn An", "an@shop.vn"))

  test("validation 422 tích lũy; request bị từ chối không ghi"):
    for
      a <- app
      r <- a.run(request(Method.POST, "/api/v1/users", """{"name":" ","email":"bad"}"""))
      p <- problem(r, 422)
      all <- a.run(request(Method.GET, "/api/v1/users")).flatMap(read[UsersPage])
    yield
      assertEquals(p.errors.size, 2)
      assertEquals(all.total, 0)

  test("duplicate email -> 409 Problem"):
    for
      a <- seeded
      r <- a.run(request(Method.POST, "/api/v1/users", """{"name":"An 2","email":"AN@SHOP.VN"}"""))
      p <- problem(r, 409)
    yield assertEquals(p.detail, "email đã tồn tại")

  test("JSON hỏng, sai kiểu, thiếu field -> 400 Problem"):
    app.flatMap(a => List("{", """{"name":3,"email":"a@b.vn"}""", """{"name":"An"}""").traverse_ { body =>
      a.run(request(Method.POST, "/api/v1/users", body)).flatMap(problem(_, 400)).map(p =>
        assert(p.detail.startsWith("Invalid value for: body"), p.detail))
    })

  test("decode query/path -> 400, semantics query -> 422"):
    for
      a <- app
      _ <- a.run(request(Method.GET, "/api/v1/users?limit=abc")).flatMap(problem(_, 400))
      _ <- a.run(request(Method.GET, "/api/v1/users/nope")).flatMap(problem(_, 400))
      r <- a.run(request(Method.GET, "/api/v1/users?sort=password&offset=-1&limit=101"))
      p <- problem(r, 422)
    yield assertEquals(p.errors.size, 3)

  test("Content-Type sai -> 415 Problem, giữ status do Tapir quyết định"):
    for
      a <- app
      r <- a.run(request(Method.POST, "/api/v1/users", "{}").putHeaders(
        org.http4s.Header.Raw(org.typelevel.ci.CIString("Content-Type"), "text/plain")))
      _ <- problem(r, 415)
    yield ()

  test("unknown user/order/route -> 404 Problem; id âm -> 422"):
    app.flatMap(a => List("/api/v1/users/99", "/api/v1/orders/99", "/missing", "/api/v2/users").traverse_ { path =>
      a.run(request(Method.GET, path)).flatMap(problem(_, 404)).void
    } *> a.run(request(Method.GET, "/api/v1/users/-1")).flatMap(problem(_, 422)).void)

  test("order CRUD flow, list user, status chuỗi, retry và conflict"):
    for
      a <- seeded
      created <- a.run(request(Method.POST, "/api/v1/users/1/orders", """{"totalMinor":450000}"""))
      o <- read[OrderView](created)
      get <- a.run(request(Method.GET, "/api/v1/orders/1")).flatMap(read[OrderView])
      list <- a.run(request(Method.GET, "/api/v1/users/1/orders?limit=1")).flatMap(read[OrdersPage])
      paid <- a.run(request(Method.PATCH, "/api/v1/orders/1/status", """{"status":"Paid"}"""))
      json <- paid.bodyText.compile.string
      retry <- a.run(request(Method.PATCH, "/api/v1/orders/1/status", """{"status":"Paid"}""")).flatMap(read[OrderView])
      r <- a.run(request(Method.PATCH, "/api/v1/orders/1/status", """{"status":"Cancelled"}"""))
      _ <- problem(r, 409)
    yield
      assertEquals(created.status, Status.Created)
      assertEquals(created.headers.get(org.typelevel.ci.CIString("Location")).map(_.head.value), Some("/api/v1/orders/1"))
      assertEquals(get, o)
      assertEquals((list.items, list.total), (List(o), 1))
      assert(json.contains("\"status\":\"Paid\""), json)
      assertEquals(retry.status, OrderStatus.Paid)

  test("order unknown user, invalid amount, unknown enum"):
    for
      a <- seeded
      _ <- a.run(request(Method.POST, "/api/v1/users/99/orders", """{"totalMinor":100}""")).flatMap(problem(_, 404))
      _ <- a.run(request(Method.GET, "/api/v1/users/99/orders")).flatMap(problem(_, 404))
      _ <- a.run(request(Method.POST, "/api/v1/users/0/orders", """{"totalMinor":0}""")).flatMap(problem(_, 422)).map(p => assertEquals(p.errors.size, 2))
      _ <- a.run(request(Method.PATCH, "/api/v1/orders/1/status", """{"status":"Refunded"}""")).flatMap(problem(_, 400))
    yield ()

  test("GET users: filter, sort, offset, limit, total và default"):
    for
      a <- seeded
      _ <- a.run(request(Method.POST, "/api/v1/users", """{"name":"Bình","email":"b@shop.vn"}"""))
      r <- a.run(request(Method.GET, "/api/v1/users?q=SHOP&sort=-id&offset=1&limit=1"))
      p <- read[UsersPage](r)
      all <- a.run(request(Method.GET, "/api/v1/users")).flatMap(read[UsersPage])
    yield
      assertEquals((p.items.map(_.id), p.total, p.offset, p.limit), (List(1L), 2, 1, 1))
      assertEquals((all.offset, all.limit), (0, 20))

  test("method mismatch -> 405 Problem; docs được phục vụ từ cùng endpoint"):
    for
      a <- app
      rejected <- a.run(request(Method.DELETE, "/api/v1/users"))
      _ <- problem(rejected, 405)
      docs <- a.run(request(Method.GET, "/docs/openapi.yaml"))
      yaml <- docs.bodyText.compile.string
    yield
      assertEquals(docs.status, Status.Ok)
      assertEquals(rejected.headers.get(org.typelevel.ci.CIString("Allow")).map(_.head.value), Some("GET, POST"))
      assertEquals(yaml, Api.openApiYaml)
      assertEquals(docs.contentType.map(ct => s"${ct.mediaType.mainType}/${ct.mediaType.subType}"), Some("application/yaml"))
