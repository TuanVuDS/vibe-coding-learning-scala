package vn.scalaeng.fw

import cats.effect.IO
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.{HttpApp, HttpRoutes, MediaType, Method, Request, Status}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.headers.{`Content-Type`, Location}
import org.http4s.implicits.*

/** MỘT bộ kịch bản chạy trên HAI cách cài đặt: cùng hợp đồng HTTP thì phải cùng hành vi.
  * Không cần mở cổng mạng: `HttpApp[IO]` chỉ là hàm `Request => IO[Response]`.
  */
abstract class ContractSuite(name: String, mkRoutes: ProductStore => HttpRoutes[IO]) extends CatsEffectSuite:
  private def app: IO[HttpApp[IO]] = ProductStore.empty.map(s => mkRoutes(s).orNotFound)

  private def post(body: Json) = Request[IO](Method.POST, uri"/api/v1/products").withEntity(body)

  test(s"$name: POST hợp lệ -> 201 + Location, GET lại -> 200"):
    for
      a        <- app
      created  <- a.run(post(NewProduct("SKU-1", "Bàn phím", 450_000).asJson))
      body     <- created.as[Product]
      fetched  <- a.run(Request[IO](Method.GET, uri"/api/v1/products/1"))
      fetchedP <- fetched.as[Product]
    yield
      assertEquals(created.status, Status.Created)
      assertEquals(created.headers.get[Location].map(_.uri.renderString), Some("/api/v1/products/1"))
      assertEquals(body, Product(1, "SKU-1", "Bàn phím", 450_000))
      assertEquals((fetched.status, fetchedP), (Status.Ok, body))

  test(s"$name: id không tồn tại -> 404 dạng Problem"):
    app.flatMap(_.run(Request[IO](Method.GET, uri"/api/v1/products/99"))).flatMap { r =>
      r.as[Problem].map(p => assertEquals((r.status, p.status), (Status.NotFound, 404)))
    }

  test(s"$name: tích lũy mọi lỗi validation -> 422"):
    app.flatMap(_.run(post(NewProduct("x", " ", 0).asJson))).flatMap { r =>
      r.as[Problem].map { p =>
        assertEquals(r.status, Status.UnprocessableContent)
        assertEquals(p.errors.map(_.field), List("sku", "name", "price"))
      }
    }

  test(s"$name: sku trùng -> 409"):
    for
      a  <- app
      _  <- a.run(post(NewProduct("SKU-1", "A", 1).asJson))
      r2 <- a.run(post(NewProduct("SKU-1", "B", 2).asJson))
    yield assertEquals(r2.status, Status.Conflict)

  test(s"$name: JSON hỏng -> 400"):
    val bad = Request[IO](Method.POST, uri"/api/v1/products")
      .withEntity("{\"sku\": ")
      .withContentType(`Content-Type`(MediaType.application.json))
    app.flatMap(_.run(bad)).map(r => assertEquals(r.status, Status.BadRequest))

class Http4sContractSuite extends ContractSuite("http4s", Http4sApi.routes)
class TapirContractSuite  extends ContractSuite("tapir", TapirApi.routes)
