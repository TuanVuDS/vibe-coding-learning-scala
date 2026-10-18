package vn.scalaeng.http

import munit.FunSuite
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpRequest.BodyPublishers
import java.util.concurrent.{Callable, Executors}
import scala.jdk.CollectionConverters.*

/** Integration: HttpServer thật trên cổng ngẫu nhiên (port 0) + HttpClient thật. */
class IntegrationSuite extends FunSuite:
  private val Admin = "admin-token-0123456789"
  private val server = FunFixture[HttpAdapter.Running](
    setup = _ => HttpAdapter.start(App.handler(ProductStore(), Map(Admin -> Role.Admin), _ => ()), "127.0.0.1", 0, threads = 8),
    teardown = _.stop(0)
  )
  private val client = HttpClient.newHttpClient()

  private def send(port: Int, method: String, path: String, body: String = "", headers: Map[String, String] = Map.empty): HttpResponse[String] =
    val b = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
      .method(method, if body.isEmpty then BodyPublishers.noBody() else BodyPublishers.ofString(body))
    headers.foreach((k, v) => b.header(k, v))
    client.send(b.build(), HttpResponse.BodyHandlers.ofString())

  private val json = Map("Content-Type" -> "application/json", "Authorization" -> s"Bearer $Admin")

  server.test("vòng đời qua HTTP thật: POST 201 -> GET 200 -> DELETE 204 (không body)"):
    srv =>
      val created = send(srv.port, "POST", "/api/v1/products", """{"sku":"AO-01","name":"Áo thun","price":120000}""", json)
      assertEquals(created.statusCode, 201)
      assertEquals(created.headers.firstValue("Location").orElse(""), "/api/v1/products/1")
      assert(created.headers.firstValue("Content-Type").orElse("").startsWith("application/json"))
      assertEquals(send(srv.port, "GET", "/api/v1/products/1").body, """{"id":1,"sku":"AO-01","name":"Áo thun","price":120000}""")
      val del = send(srv.port, "DELETE", "/api/v1/products/1", headers = Map("Authorization" -> s"Bearer $Admin"))
      assertEquals((del.statusCode, del.body), (204, ""))

  server.test("405 kèm Allow, request id trả về, method lạ -> 501, body quá lớn -> 413"):
    srv =>
      val r = send(srv.port, "PUT", "/api/v1/products")
      assertEquals((r.statusCode, r.headers.firstValue("Allow").orElse("")), (405, "GET, POST"))
      assert(r.headers.firstValue("X-Request-Id").isPresent)
      assertEquals(send(srv.port, "TRACE", "/health").statusCode, 501)
      assertEquals(send(srv.port, "POST", "/api/v1/products", "x" * 1_000_001, json).statusCode, 413)

  server.test("ĐỒNG THỜI: 20 request tạo cùng SKU -> đúng 1 lần 201, 19 lần 409"):
    srv =>
      val pool = Executors.newFixedThreadPool(20)
      try
        val tasks = (1 to 20).map(i => (() => send(srv.port, "POST", "/api/v1/products", s"""{"sku":"SAME","name":"n$i","price":1}""", json).statusCode): Callable[Int])
        val codes = pool.invokeAll(tasks.asJava).asScala.map(_.get).toList
        assertEquals(codes.count(_ == 201), 1)
        assertEquals(codes.count(_ == 409), 19)
      finally pool.shutdown()

  server.test("ĐỒNG THỜI: 100 SKU khác nhau -> 100 id duy nhất, liên tục 1..100"):
    srv =>
      val pool = Executors.newFixedThreadPool(16)
      try
        val tasks = (1 to 100).map(i => (() => send(srv.port, "POST", "/api/v1/products", s"""{"sku":"S$i","name":"n","price":1}""", json)
          .headers.firstValue("Location").orElse("")): Callable[String])
        val ids = pool.invokeAll(tasks.asJava).asScala.map(_.get.split("/").last.toInt).toList
        assertEquals(ids.sorted, (1 to 100).toList)
      finally pool.shutdown()
