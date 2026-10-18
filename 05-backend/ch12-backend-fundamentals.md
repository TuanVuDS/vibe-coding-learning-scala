# Chương 12 — Backend Fundamentals

> **Phiên bản:** Scala 3.9.0 · JDK 21 (`jdk.httpserver`, `java.net.http`) · munit 1.2.4 · **không framework, không thư viện ngoài**
> **Code:** [`http-lab/`](http-lab/) — REST API `products` hoàn chỉnh: router, middleware, xác thực, validation, phân trang, graceful shutdown
> **Trạng thái kiểm chứng:** Code **đã biên dịch** với `-Werror` và **đã chạy thật**: server nhận request qua `curl`, mọi trao đổi HTTP trích trong bài là output thật.
> 13/13 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21), gồm integration test với `HttpServer` thật và test đồng thời 20 request.

---

## Vì sao chương này không dùng framework?

Từ Chương 13 trở đi ta dùng http4s + Tapir. Nhưng framework che giấu đúng những thứ người mới cần hiểu:
một request HTTP trông như thế nào, router làm gì, middleware là gì, vì sao 401 khác 403. Chương này tự xây một API nhỏ trên `HttpServer` có sẵn trong JDK
(~400 dòng) để mọi khái niệm đều **nhìn thấy được**. Khi gặp http4s, bạn sẽ nhận ra: `HttpRoutes` chính là `Handler` của ta, `Middleware` cũng vậy — chỉ được bọc trong effect `IO`.

## 1. Mục tiêu học tập

- Đọc và viết một HTTP request/response thô; hiểu method, status code, header, body.
- Thiết kế REST API theo tài nguyên: URL, method, status code, versioning, phân trang, lọc.
- Trả lỗi nhất quán theo chuẩn Problem Details (RFC 9457); phân biệt 400 / 401 / 403 / 404 / 405 / 409 / 415 / 422 / 500.
- Hiểu router và **middleware như function composition**; biết thứ tự middleware quan trọng thế nào.
- Phân biệt **authentication** và **authorization**.
- Logging có request id, cấu hình secrets qua môi trường, graceful shutdown.
- Nhận ra rằng server xử lý request **đồng thời** → trạng thái chia sẻ phải an toàn đa luồng.

## 2. Kiến thức tiên quyết

- Chương 4 (function composition), 7 (`Either`, Functional Core / Imperative Shell), 8A (type alias), 9 (cấu hình, log có cấu trúc), 10 (fixture, integration test).

## 3. Lý thuyết

### 3.1. Client–server và HTTP

```
Client (app mobile, frontend, service khác)          Server (backend của ta)
   │  HTTP request: method + đường dẫn + header + body     │
   ├──────────────────────────────────────────────────────►│ router -> handler -> store
   │◄──────────────────────────────────────────────────────┤
   │  HTTP response: status + header + body                 │
```

HTTP là giao thức **không trạng thái** (stateless): mỗi request mang đủ thông tin (kể cả token xác thực). Server không "nhớ" client giữa các request — nhờ vậy có thể chạy nhiều bản server sau load balancer.

Một trao đổi thật (output của `curl -i` với server của chương):

```
POST /api/v1/products HTTP/1.1                         <- request line: method, đường dẫn, phiên bản
Authorization: Bearer admin-token-0123456789           <- header
Content-Type: application/json
X-Request-Id: demo-1

{"sku":"AO-01","name":"Áo thun","price":120000}        <- body

HTTP/1.1 201 Created                                   <- status line
Content-type: application/json; charset=utf-8
X-request-id: demo-1
Content-length: 55
Location: /api/v1/products/1                           <- tài nguyên mới nằm ở đâu

{"id":1,"sku":"AO-01","name":"Áo thun","price":120000}
```

Chú ý: JDK `HttpServer` viết `Content-type`, `X-request-id` — **tên header không phân biệt hoa thường** (RFC 9110). Code của ta lưu tên header chữ thường khi đọc.

### 3.2. Method: an toàn và idempotent

| Method | Dùng cho | Safe (không đổi trạng thái) | Idempotent (gọi N lần = 1 lần) |
|---|---|---|---|
| GET | Đọc | ✅ | ✅ |
| POST | Tạo mới, hành động | ❌ | ❌ |
| PUT | Thay toàn bộ tài nguyên | ❌ | ✅ |
| PATCH | Sửa một phần | ❌ | không đảm bảo |
| DELETE | Xóa | ❌ | ✅ (lần 2 trả 404 nhưng trạng thái server như nhau) |

**Vì sao quan trọng:** client, proxy, thư viện HTTP có thể **tự retry** request idempotent khi mạng lỗi. Retry một POST tạo đơn hàng có thể tạo hai đơn — Chương 17 dùng *idempotency key* để giải quyết.

### 3.3. Status code dùng trong API

| Code | Khi nào | Ví dụ trong http-lab |
|---|---|---|
| 200 OK | Đọc thành công | `GET /products/1` |
| 201 Created | Tạo thành công, kèm `Location` | `POST /products` |
| 204 No Content | Thành công, không có body | `DELETE /products/1` |
| 400 Bad Request | Request sai **cú pháp**: JSON hỏng, tham số không parse được | `?size=1000`, body `{not json` |
| 401 Unauthorized | **Chưa xác thực** hoặc token sai — kèm `WWW-Authenticate` | thiếu `Authorization` |
| 403 Forbidden | Đã xác thực nhưng **không có quyền** | token Reader gọi POST |
| 404 Not Found | Tài nguyên không tồn tại | `GET /products/999` |
| 405 Method Not Allowed | Đường dẫn đúng, method sai — kèm `Allow` | `PATCH /products/1` |
| 409 Conflict | Xung đột trạng thái | SKU đã tồn tại |
| 413 Content Too Large | Body quá lớn | body > 1 MB |
| 415 Unsupported Media Type | Sai `Content-Type` | `text/plain` |
| 422 Unprocessable Content | Cú pháp đúng nhưng **dữ liệu vi phạm quy tắc** | `price: -1` |
| 500 Internal Server Error | Lỗi phía server — **không lộ chi tiết** | exception trong handler |
| 501 Not Implemented | Server không hỗ trợ method | `TRACE` |

**400 hay 422?** Quy ước dùng trong giáo trình: không đọc được (JSON hỏng, sai kiểu tham số query) → 400; đọc được nhưng giá trị sai quy tắc nghiệp vụ → 422 kèm danh sách lỗi theo trường. Nhiều API chỉ dùng 400 cho cả hai — điều quan trọng là **nhất quán** và **ghi trong tài liệu API**.

### 3.4. Thiết kế REST theo tài nguyên

- URL là **danh từ số nhiều**: `/api/v1/products`, `/api/v1/products/{id}`. Hành động thể hiện bằng **method**, không bằng URL (`/createProduct` là sai phong cách).
- **Versioning trong đường dẫn** (`/v1/`): thay đổi không tương thích → `/v2/` chạy song song. Thêm trường mới vào response là thay đổi **tương thích**; đổi tên/xóa trường thì không.
- **Phân trang:** `?page=2&size=20`, response có `page`, `size`, `total`. Luôn có **giới hạn trên** cho `size` (ở đây 100) — nếu không, một request có thể đòi cả triệu bản ghi.
  Phân trang offset đơn giản nhưng chậm với trang sâu và lệch khi dữ liệu thay đổi; Chương 15 giới thiệu phân trang theo con trỏ (keyset).
- **Lọc/sắp xếp:** query parameter (`?q=áo`). Mọi tham số phải được validate — giá trị lạ trả 400 với thông báo rõ.
- **Sắp xếp ổn định** (theo `id`) để phân trang không trả trùng/thiếu bản ghi giữa các trang.

### 3.5. JSON và Problem Details

Mọi lỗi trả về cùng một hình dạng (RFC 9457, `Content-Type: application/problem+json`):

```json
{"type":"about:blank","title":"Unprocessable Content","status":422,"detail":"dữ liệu không hợp lệ",
 "errors":[{"field":"sku","message":"'bad sku' không hợp lệ (A-Z, 0-9, '-', tối đa 32)"},
           {"field":"name","message":"bắt buộc, kiểu chuỗi"},
           {"field":"price","message":"số nguyên trong khoảng 1..10.000.000.000"}]}
```

Client chỉ cần **một** đoạn code xử lý lỗi. Validation **tích lũy mọi lỗi** (Chương 7) để người dùng sửa một lần.

### 3.6. Handler, router, middleware — tất cả là hàm

```scala
type Handler    = Request => Response
type Middleware = Handler => Handler
```

- **Handler:** hàm thuần nhận request, trả response. Test được mà **không mở cổng mạng** (`ApiSuite`).
- **Router:** một `Handler` được dựng từ danh sách `Route(method, pattern, handler)`. Đường dẫn khớp nhưng sai method → 405 + `Allow`; không khớp → 404.
- **Middleware:** nhận handler, trả handler mới có thêm hành vi **xuyên suốt** (cross-cutting): request id, log, xử lý lỗi, xác thực, CORS, nén, rate limit...

```scala
Middlewares.compose(
  Middlewares.requestId(newId),       // ngoài cùng: mọi response (kể cả 500) đều có request id
  Middlewares.accessLog(log, clock),  // đo thời gian toàn bộ phần bên trong
  Middlewares.errorHandling(log),     // bắt exception từ mọi lớp bên trong
  Middlewares.authenticate(tokens)    // trong cùng: gần router
)(Router(ProductApi.routes(store)))
```

**Thứ tự quan trọng.** Nếu `errorHandling` nằm **ngoài** `requestId`, response 500 sẽ không có request id; nếu `accessLog` nằm **trong** `errorHandling`, request lỗi sẽ không được log với status 500.
Hãy vẽ "củ hành": request đi từ ngoài vào trong, response đi từ trong ra ngoài.

### 3.7. Input validation

Ba tầng, từ ngoài vào trong:
1. **Giao thức:** method hỗ trợ (501), kích thước body (413), `Content-Type` (415).
2. **Cú pháp:** JSON hợp lệ, là object (400); tham số query đúng kiểu và trong khoảng (400).
3. **Nghiệp vụ:** định dạng SKU, độ dài tên, khoảng giá (422); trùng SKU (409 — cần trạng thái hiện tại).

Không bao giờ tin dữ liệu từ client — kể cả khi client là app của chính công ty.

### 3.8. Authentication và authorization

| | Authentication (xác thực) | Authorization (phân quyền) |
|---|---|---|
| Câu hỏi | **Bạn là ai?** | **Bạn được làm gì?** |
| Thất bại | 401 + `WWW-Authenticate` | 403 |
| Ở http-lab | Middleware `authenticate`: token → vai trò | `requireRole(Role.Admin)` bọc từng route |

Token xác thực **không có giá trị mặc định** trong code: `Main` từ chối khởi động nếu thiếu `ADMIN_TOKEN`/`READER_TOKEN` hoặc token ngắn hơn 16 ký tự (exit code 1).
Token tĩnh chỉ phù hợp cho service nội bộ đơn giản; Chương 17 dùng password hashing, JWT/session, và lưu secrets đúng cách.

### 3.9. Logging, request id, cấu hình, observability

- **Request id:** lấy từ header `X-Request-Id` của client nếu hợp lệ (chống chèn ký tự lạ vào log), nếu không thì sinh UUID; trả lại trong response.
  Khi người dùng báo lỗi, họ đưa request id → tìm ngay đúng dòng log. Lỗi 500 đưa request id vào `detail` thay vì chi tiết exception.
- **Access log có cấu trúc** (Chương 9): `{"request_id":"demo-1","method":"POST","path":"/api/v1/products","status":201,"duration_ms":88}`.
- **Không log** body chứa dữ liệu nhạy cảm, không log header `Authorization`.
- **Observability** gồm ba trụ: logs (đã có), **metrics** (số request, độ trễ theo percentile, tỷ lệ lỗi), **traces** (theo dõi một request qua nhiều service). Chương 17 bổ sung metrics và health check đầy đủ.

### 3.10. Server xử lý đồng thời — trạng thái chia sẻ

`HttpServer` dùng pool 8 thread: nhiều request chạy **cùng lúc**. `ProductStore` là trạng thái chia sẻ, nên:

```scala
private val state = AtomicReference(State(1L, Map.empty))       // trạng thái BẤT BIẾN

@tailrec private def modify[A](f: State => (State, A)): A =
  val current = state.get()
  val (next, result) = f(current)
  if state.compareAndSet(current, next) then result else modify(f)   // bị thread khác chen -> làm lại
```

Test `IntegrationSuite` bắn **20 request cùng SKU đồng thời** → đúng 1 lần 201 và 19 lần 409; **100 SKU khác nhau** → 100 id liên tục 1..100, không trùng.
Nếu viết bằng `var` + `Map` mutable, kết quả có thể sai — và có thể **không lộ ra trên máy dev** (Chương 16A chứng minh điều này).

### 3.11. Graceful shutdown

Khi Kubernetes/systemd dừng service, nó gửi SIGTERM. `sys.addShutdownHook` gọi `running.stop(graceSeconds = 5)`: ngừng nhận kết nối mới, chờ request đang xử lý hoàn tất, rồi đóng pool.
Không có bước này, request đang chạy bị cắt ngang giữa chừng. Output thật khi gửi SIGTERM: dòng log `{"msg":"shutting down"}` xuất hiện sau request cuối.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Stateless | Server không giữ trạng thái phiên giữa các request |
| Safe / idempotent method | Không đổi trạng thái / gọi nhiều lần như một lần |
| Status code, reason phrase | Mã trạng thái, cụm mô tả đi kèm |
| Resource, endpoint | Tài nguyên, điểm truy cập API |
| Problem Details (RFC 9457) | Định dạng chuẩn cho lỗi HTTP API |
| Router, route | Bộ định tuyến, một tuyến |
| Middleware | Lớp bọc handler thêm hành vi xuyên suốt |
| Cross-cutting concern | Mối quan tâm xuyên suốt (log, auth, lỗi) |
| Authentication / authorization | Xác thực / phân quyền |
| Bearer token | Token đặt trong header `Authorization: Bearer ...` |
| Request id / correlation id | Mã định danh request để truy vết |
| Compare-and-set (CAS) | Thao tác nguyên tử "đặt giá trị mới nếu giá trị hiện tại vẫn là X" |
| Graceful shutdown | Dừng có trật tự, hoàn tất việc đang làm |

## 5. Ví dụ đơn giản

```scala
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress

@main def hello(): Unit =
  val server = HttpServer.create(InetSocketAddress(8080), 0)
  server.createContext("/hello", ex =>
    val body = """{"message":"xin chào"}""".getBytes("UTF-8")
    ex.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    ex.sendResponseHeaders(200, body.length.toLong)
    ex.getResponseBody.write(body)
    ex.close()
  )
  server.start()
```

`curl -i localhost:8080/hello`. So với http-lab, phiên bản này trộn I/O với logic — không test được nếu không mở cổng mạng.

## 6. Ví dụ thực tế — API `products`

| Endpoint | Quyền | Thành công | Lỗi có thể có |
|---|---|---|---|
| `GET /health` | công khai | 200 | — |
| `GET /api/v1/products?page&size&q` | công khai | 200 + trang | 400 |
| `GET /api/v1/products/{id}` | công khai | 200 | 400, 404 |
| `POST /api/v1/products` | Admin | 201 + `Location` | 400, 401, 403, 409, 413, 415, 422 |
| `DELETE /api/v1/products/{id}` | Admin | 204 | 400, 401, 403, 404 |

## 7. Code đầy đủ

| File | Vai trò |
|---|---|
| [`Http.scala`](http-lab/src/main/scala/vn/scalaeng/http/Http.scala) | `Method`, `Request`, `Response`, `Handler`, `Middleware`, Problem Details, parse query |
| [`Router.scala`](http-lab/src/main/scala/vn/scalaeng/http/Router.scala) | Khớp đường dẫn có tham số, 404/405 |
| [`Middleware.scala`](http-lab/src/main/scala/vn/scalaeng/http/Middleware.scala) | request id, access log, xử lý lỗi, xác thực, phân quyền, `compose` |
| [`Products.scala`](http-lab/src/main/scala/vn/scalaeng/http/Products.scala) | Store CAS, validation, route |
| [`Server.scala`](http-lab/src/main/scala/vn/scalaeng/http/Server.scala) | Adapter `HttpServer`, ghép ứng dụng, `Main` |
| [`json/Json.scala`](http-lab/src/main/scala/vn/scalaeng/http/json/Json.scala) | Bản sao JSON parser của Project 1 |

Router:

```scala
def apply(routes: List[Route]): Handler = req =>
  val byPath = routes.flatMap(r => matchPath(r.pattern, req.segments).map(r -> _))
  byPath.find(_._1.method == req.method) match
    case Some((route, params)) => route.handler(req, params)
    case None if byPath.nonEmpty =>
      val allow = byPath.map(_._1.method).distinct.sortBy(_.ordinal).mkString(", ")
      Response.problem(405, "Method Not Allowed", s"${req.method} không được hỗ trợ cho ${req.path}").withHeader("Allow", allow)
    case None => Response.problem(404, "Not Found", s"không có tài nguyên ${req.path}")
```

Xác thực:

```scala
def authenticate(tokens: Map[String, Role]): Middleware = next => req =>
  req.header("Authorization") match
    case None => next(req)
    case Some(h) if h.startsWith("Bearer ") =>
      tokens.get(h.drop(7).trim) match
        case Some(role) => next(req.withAttr("role", role.toString))
        case None       => unauthorized("token không hợp lệ")
    case Some(_) => unauthorized("chỉ hỗ trợ Authorization: Bearer <token>")
```

## 8. Giải thích code

- `Request.attributes`: middleware **gắn thông tin** (request id, vai trò) vào request bất biến bằng `withAttr` — tạo request mới, không sửa request cũ.
- `ProductApi.routes` dùng `for` trên `Either[Response, _]` (Chương 7): mọi bước validation trả `Left(response lỗi)`; `merge` gộp `Either[Response, Response]` thành `Response`.
- `decodeNewProduct` tích lũy lỗi bằng tuple + `productIterator` như Project 1.
- `HttpAdapter.toRequest` đọc tối đa `MaxBodyBytes + 1` byte: nếu đọc được nhiều hơn giới hạn → 413, không bao giờ nạp body vô hạn vào bộ nhớ.
- `sendResponseHeaders(status, -1)` khi không có body — bắt buộc với 204; ngược lại khai báo đúng độ dài để client biết khi nào body kết thúc.
- `App.handler` nhận `clock`, `newId`, `log` làm tham số (seam, Chương 10) → `ApiSuite` kiểm tra chính xác dòng access log `"duration_ms":0`.
- `HttpAdapter.start(..., port = 0)` trong test: hệ điều hành chọn cổng trống → test chạy song song không xung đột cổng.

## 9. Output mong đợi (output thật)

Khởi động thiếu token:

```
$ java ... vn.scalaeng.http.Main
cần ADMIN_TOKEN và READER_TOKEN khác nhau, mỗi token >= 16 ký tự (KHÔNG có giá trị mặc định)
exit=1
```

Lọc có tiếng Việt (`q=%C3%A1o` là "áo" đã mã hóa URL):

```
$ curl -i "localhost:18080/api/v1/products?page=1&size=10&q=%C3%A1o"
HTTP/1.1 200 OK
Content-type: application/json; charset=utf-8
X-request-id: 8e0c7307-040d-420d-8de5-8077f8705a4d
Content-length: 96

{"items":[{"id":1,"sku":"AO-01","name":"Áo thun","price":120000}],"page":1,"size":10,"total":1}
```

Sai vai trò, xóa, sai method:

```
HTTP/1.1 403 Forbidden
Content-type: application/problem+json; charset=utf-8
{"type":"about:blank","title":"Forbidden","status":403,"detail":"vai trò Reader không được phép, cần Admin"}

HTTP/1.1 204 No Content
X-request-id: 0dac3c7a-3287-4c5b-b8f3-3936a1a7a5e7

HTTP/1.1 405 Method Not Allowed
Allow: GET, DELETE
{"type":"about:blank","title":"Method Not Allowed","status":405,"detail":"PATCH không được hỗ trợ cho /api/v1/products/1"}
```

Đáng chú ý: response 422 có status line `HTTP/1.1 422 ` với **reason phrase rỗng** — JDK không có sẵn tên cho mã 422. Client tốt chỉ dựa vào **mã số**, không dựa vào reason phrase.

Log server (stderr), gồm cả lúc nhận SIGTERM:

```
{"msg":"listening","port":18080}
{"request_id":"demo-1","method":"POST","path":"/api/v1/products","status":201,"duration_ms":88}
...
{"request_id":"06fadb6c-...","method":"PATCH","path":"/api/v1/products/1","status":405,"duration_ms":13}
{"msg":"shutting down"}
```

## 10. Lỗi thường gặp

| Lỗi | Hậu quả | Cách đúng |
|---|---|---|
| Trả 200 kèm `{"error": ...}` | Client, proxy, monitoring coi là thành công | Dùng đúng status code |
| Nhầm 401 và 403 | Client không biết nên đăng nhập lại hay báo thiếu quyền | 401 = chưa xác thực, 403 = không có quyền |
| Trả stack trace/thông điệp exception cho client | Lộ cấu trúc nội bộ, có khi lộ mật khẩu (test "exception trong handler" chứng minh) | 500 chung chung + request id; chi tiết vào log |
| Không giới hạn `size` phân trang, kích thước body | Một request làm cạn bộ nhớ (DoS) | Giới hạn trên + 400/413 |
| Trạng thái chia sẻ bằng `var` + collection mutable | Mất cập nhật khi đồng thời | `AtomicReference` + CAS, hoặc database (Ch15) |
| Middleware sai thứ tự | Response lỗi thiếu request id, request lỗi không được log | Vẽ "củ hành", viết test cho response 500 |
| `sendResponseHeaders(204, 0)` với JDK HttpServer | Client có thể chờ body | Dùng `-1` khi không có body |
| Token/mật khẩu có giá trị mặc định trong code | Lên production với secret công khai | Bắt buộc từ môi trường, từ chối khởi động nếu thiếu |
| Dùng reason phrase để quyết định logic | Hỏng với 422 trên JDK HttpServer | Chỉ dùng mã số |

## 11. Cách debug

- **`curl -i`** (hiện header) hoặc **`curl -v`** (hiện cả request gửi đi) — công cụ số 1 khi làm backend.
- Tìm theo request id: `grep demo-1 server.log` hoặc `jq 'select(.request_id=="demo-1")'`.
- Test handler như hàm trong `sbt console`: `App.handler(ProductStore(), Map(...), println)(Request(Method.GET, "/health"))`.
- Lỗi chỉ xuất hiện khi tải cao → nghi ngờ trạng thái chia sẻ; viết test đồng thời như `IntegrationSuite`.
- Cổng bị chiếm (`BindException: Address already in use`): `ss -ltnp | grep 8080` (Linux) hoặc đổi `PORT`.

## 12. Bài tập

1. **(Cơ bản)** Thêm `PUT /api/v1/products/{id}` (Admin) thay toàn bộ `name` và `price` (không đổi `sku`). Trả 200 + sản phẩm mới, 404 nếu không có. Chứng minh bằng test rằng gọi 2 lần cho cùng kết quả (idempotent).
2. **(Cơ bản)** Thêm tham số `sort` cho danh sách: `id` (mặc định), `price_asc`, `price_desc`; giá trị khác → 400. Khi bằng giá, sắp theo `id` để phân trang ổn định.
3. **(Trung bình)** HTTP caching cho `GET /products/{id}`: trả header `ETag` (ví dụ hash nội dung); nếu request có `If-None-Match` trùng ETag → **304 Not Modified** không body.
4. **(Trung bình)** Tách health check: `GET /health/live` (tiến trình còn sống) và `GET /health/ready` (sẵn sàng nhận traffic). Khi bắt đầu shutdown, `ready` trả 503 trước khi server dừng. Giải thích vì sao load balancer cần sự khác biệt này.
5. **(Nâng cao)** Optimistic concurrency: `PUT` yêu cầu header `If-Match: <etag>`; nếu ETag hiện tại khác → **412 Precondition Failed**. Viết test: hai client cùng đọc, cùng sửa — client thứ hai nhận 412 thay vì ghi đè âm thầm (lost update).

## 13. Đáp án và lời giải

Xem [`solutions/ch12-ch16a-solutions.md`](../solutions/ch12-ch16a-solutions.md).

## 14. Câu hỏi kiểm tra

1. HTTP "stateless" nghĩa là gì và vì sao nó giúp mở rộng (scale) server?
2. Method nào idempotent? Vì sao client được phép tự retry chúng?
3. Khi nào trả 400, khi nào 422, khi nào 409?
4. 401 và 403 khác nhau thế nào? Header nào đi kèm 401?
5. Vì sao `Handler = Request => Response` giúp test dễ?
6. Đổi chỗ `errorHandling` và `requestId` trong `compose` thì điều gì thay đổi?
7. Vì sao `ProductStore` dùng `AtomicReference` thay vì `var` + `Map` mutable?
8. Graceful shutdown giải quyết vấn đề gì?

## 15. Mini project

Thêm tài nguyên **`orders`** vào http-lab: `POST /api/v1/orders` (Reader hoặc Admin) với body `{"items":[{"product_id":1,"quantity":2}]}`.
Validate: product tồn tại (422 nếu không), số lượng 1..1000; tính tổng tiền từ giá hiện tại. `GET /api/v1/orders/{id}` chỉ cho người tạo hoặc Admin xem (403 nếu không).
Viết `ApiSuite` cho mọi status code và một test đồng thời. Đây là bản nháp của Project 2.

## 16. Nguồn tài liệu chính thức

- RFC 9110 — HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- RFC 9457 — Problem Details for HTTP APIs: https://www.rfc-editor.org/rfc/rfc9457
- RFC 6750 — Bearer Token Usage: https://www.rfc-editor.org/rfc/rfc6750
- JDK `com.sun.net.httpserver`: https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/com/sun/net/httpserver/package-summary.html
- JDK `java.net.http.HttpClient`: https://docs.oracle.com/en/java/javase/21/docs/api/java.net.http/java/net/http/HttpClient.html
- MDN — HTTP status codes: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status

## 17. Checklist tự đánh giá

- [ ] Tôi đọc được một request/response thô và chỉ ra từng phần.
- [ ] Tôi chọn đúng method và status code cho mọi thao tác của một tài nguyên.
- [ ] API của tôi trả lỗi theo một định dạng duy nhất, không lộ chi tiết nội bộ.
- [ ] Tôi viết được middleware và giải thích được thứ tự của chúng.
- [ ] Tôi phân biệt authentication/authorization và 401/403.
- [ ] Mọi đầu vào (body, query, đường dẫn) đều được validate với giới hạn trên.
- [ ] Trạng thái chia sẻ trong server của tôi an toàn khi có nhiều request đồng thời, và tôi có test chứng minh.
