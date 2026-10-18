package vn.scalaeng.http

import munit.FunSuite
import scala.collection.mutable.ListBuffer

/** Test toàn bộ API như một HÀM: không mở cổng mạng, không thread, chạy trong mili-giây. */
class ApiSuite extends FunSuite:
  private val Admin = "admin-token-0123456789"
  private val Reader = "reader-token-0123456789"

  private def fresh() =
    val logs = ListBuffer.empty[String]
    val ids = Iterator.from(1).map(i => s"req-$i")
    val h = App.handler(ProductStore(), Map(Admin -> Role.Admin, Reader -> Role.Reader), logs += _, () => 1000L, () => ids.next())
    (h, logs)

  private def post(body: String, token: String = Admin, ct: String = "application/json") =
    Request(Method.POST, "/api/v1/products", headers = Map("authorization" -> s"Bearer $token", "content-type" -> ct), body = body)

  test("health, 404, 405 kèm Allow"):
    val (h, _) = fresh()
    assertEquals(h(Request(Method.GET, "/health")).body, """{"status":"ok"}""")
    assertEquals(h(Request(Method.GET, "/nope")).status, 404)
    val r = h(Request(Method.PUT, "/api/v1/products"))
    assertEquals((r.status, r.headers.get("Allow")), (405, Some("GET, POST")))

  test("tạo sản phẩm: 201 + Location, rồi đọc lại"):
    val (h, _) = fresh()
    val r = h(post("""{"sku":"AO-01","name":" Áo thun ","price":120000}"""))
    assertEquals(r.status, 201)
    assertEquals(r.headers.get("Location"), Some("/api/v1/products/1"))
    assertEquals(h(Request(Method.GET, "/api/v1/products/1")).body, """{"id":1,"sku":"AO-01","name":"Áo thun","price":120000}""")

  test("401 khi thiếu/sai token, 403 khi sai vai trò"):
    val (h, _) = fresh()
    val noToken = h(Request(Method.POST, "/api/v1/products", headers = Map("content-type" -> "application/json"), body = "{}"))
    assertEquals((noToken.status, noToken.headers.get("WWW-Authenticate")), (401, Some("Bearer")))
    assertEquals(h(post("{}", token = "wrong")).status, 401)
    assertEquals(h(post("{}", token = Reader)).status, 403)

  test("415, 400, 422 (tích lũy mọi lỗi), 409"):
    val (h, _) = fresh()
    assertEquals(h(post("{}", ct = "text/plain")).status, 415)
    assertEquals(h(post("{not json")).status, 400)
    val v = h(post("""{"sku":"bad sku","price":1.5}"""))
    assertEquals(v.status, 422)
    assert(v.body.contains("\"field\":\"sku\"") && v.body.contains("\"field\":\"name\"") && v.body.contains("\"field\":\"price\""), v.body)
    assertEquals(h(post("""{"sku":"A","name":"x","price":1}""")).status, 201)
    val dup = h(post("""{"sku":"A","name":"y","price":2}"""))
    assertEquals(dup.status, 409)
    assert(dup.body.contains("\"existing_id\":1"), dup.body)

  test("phân trang, lọc và validate query"):
    val (h, _) = fresh()
    (1 to 5).foreach(i => h(post(s"""{"sku":"S$i","name":"Áo $i","price":$i}""")))
    val p = h(Request(Method.GET, "/api/v1/products", query = Map("page" -> List("2"), "size" -> List("2"))))
    assert(p.body.contains("\"page\":2,\"size\":2,\"total\":5") && p.body.contains("\"sku\":\"S3\""), p.body)
    assertEquals(h(Request(Method.GET, "/api/v1/products", query = Map("size" -> List("1000")))).status, 400)
    assertEquals(h(Request(Method.GET, "/api/v1/products/abc")).status, 400)

  test("DELETE: 204 rồi 404"):
    val (h, _) = fresh()
    h(post("""{"sku":"A","name":"x","price":1}"""))
    val del = Request(Method.DELETE, "/api/v1/products/1", headers = Map("authorization" -> s"Bearer $Admin"))
    assertEquals(h(del).status, 204)
    assertEquals(h(del).status, 404)

  test("request id: dùng id của client nếu hợp lệ, sinh mới nếu không; access log có cấu trúc"):
    val (h, logs) = fresh()
    assertEquals(h(Request(Method.GET, "/health", headers = Map("x-request-id" -> "abc-123"))).headers.get("X-Request-Id"), Some("abc-123"))
    assertEquals(h(Request(Method.GET, "/health", headers = Map("x-request-id" -> "bad id!"))).headers.get("X-Request-Id"), Some("req-1"))
    assertEquals(logs.last, """{"request_id":"req-1","method":"GET","path":"/health","status":200,"duration_ms":0}""")

  test("exception trong handler -> 500 không lộ chi tiết"):
    val logs = ListBuffer.empty[String]
    val boom: Handler = _ => throw IllegalStateException("db password=secret")
    val h = Middlewares.compose(Middlewares.requestId(() => "r1"), Middlewares.errorHandling(logs += _))(boom)
    val r = h(Request(Method.GET, "/x"))
    assertEquals(r.status, 500)
    assert(!r.body.contains("secret"), r.body)
    assert(logs.head.contains("secret"))   // chi tiết chỉ nằm trong log nội bộ

  test("Query.parse giải mã URL"):
    assertEquals(Query.parse("q=%C3%A1o%20thun&a=1&a=2&flag"), Map("q" -> List("áo thun"), "a" -> List("1", "2"), "flag" -> List("")))
