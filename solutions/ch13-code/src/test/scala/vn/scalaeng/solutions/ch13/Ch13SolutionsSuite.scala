package vn.scalaeng.solutions.ch13

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{host, port}
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.{HttpRoutes, MediaType, Method, Request, Status, Uri}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.`Content-Type`
import org.http4s.implicits.*
import vn.scalaeng.fw.{NewProduct, Problem, Product, ProductStore, TapirApi}

class Ch13SolutionsSuite extends CatsEffectSuite:
  private def seeded: IO[ProductStore] =
    for
      s <- ProductStore.empty
      _ <- s.create(NewProduct("KB-01", "Bàn phím cơ", 450_000))
      _ <- s.create(NewProduct("MS-01", "Chuột không dây", 250_000))
      _ <- s.create(NewProduct("KB-02", "Bàn PHÍM văn phòng", 150_000))
    yield s

  private def get(routes: HttpRoutes[IO], uri: Uri) = routes.orNotFound.run(Request[IO](Method.GET, uri))

  List("http4s" -> ListProducts.http4s, "tapir" -> ListProducts.tapir).foreach { (name, mk) =>
    test(s"bài 2 ($name): lọc ?q= không phân biệt hoa thường, giữ thứ tự id"):
      for
        s   <- seeded
        all <- get(mk(s), uri"/api/v1/products").flatMap(_.as[List[Product]])
        kb  <- get(mk(s), uri"/api/v1/products".withQueryParam("q", "phím")).flatMap(_.as[List[Product]])
      yield
        assertEquals(all.map(_.id), List(1L, 2L, 3L))
        assertEquals(kb.map(_.sku), List("KB-01", "KB-02"))
  }

  test("bài 3: id không phải số -> 400 Problem; id số vẫn đi route chính"):
    for
      s   <- seeded
      bad <- get(BadIdAs400.routes(s), uri"/api/v1/products/abc")
      p   <- bad.as[Problem]
      ok  <- get(BadIdAs400.routes(s), uri"/api/v1/products/2")
    yield
      assertEquals((bad.status, p.detail), (Status.BadRequest, "id phải là số nguyên, nhận được 'abc'"))
      assertEquals(ok.status, Status.Ok)

  test("bài 4: Tapir trả Problem JSON khi decode thất bại"):
    val broken = Request[IO](Method.POST, uri"/api/v1/products")
      .withEntity("{\"sku\": ")
      .withContentType(`Content-Type`(MediaType.application.json))
    for
      s <- ProductStore.empty
      r <- TapirProblemOnDecodeFailure.routes(s).orNotFound.run(broken)
      p <- r.as[Problem]
    yield
      assertEquals(r.status, Status.BadRequest)
      assertEquals(r.contentType.map(_.mediaType), Some(MediaType.application.json))
      assert(p.detail.startsWith("Invalid value for: body"), p.detail)

  test("bài 4: lỗi KHÔNG phải decode (sai method -> 405) không bị gắn nhãn 400"):
    for
      s    <- ProductStore.empty
      r    <- TapirProblemOnDecodeFailure.routes(s).orNotFound.run(Request[IO](Method.PUT, uri"/api/v1/products"))
      body <- r.bodyText.compile.string
    yield
      assertEquals(r.status, Status.MethodNotAllowed)
      assert(!body.contains("\"status\":400"), body)

  // Bài 5 — integration test qua socket thật: server cổng 0 (OS tự chọn cổng trống) + Ember client.
  private val serverAndClient: Resource[IO, (Uri, Client[IO])] =
    for
      store  <- Resource.eval(ProductStore.empty)
      server <- EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0")
                  .withHttpApp(TapirApi.routes(store).orNotFound).build
      client <- EmberClientBuilder.default[IO].build
    yield (Uri.unsafeFromString(s"http://127.0.0.1:${server.address.getPort}"), client)

  test("bài 5: POST rồi GET qua HTTP thật"):
    serverAndClient.use { (base, client) =>
      val create = Request[IO](Method.POST, base / "api" / "v1" / "products")
        .withEntity(NewProduct("SKU-9", "Tai nghe", 990_000).asJson)
      for
        status <- client.run(create).use(r => IO.pure(r.status))
        got    <- client.expect[Product](base / "api" / "v1" / "products" / "1")
      yield
        assertEquals(status, Status.Created)
        assertEquals(got, Product(1, "SKU-9", "Tai nghe", 990_000))
    }
