# Chương 13 — Chọn Scala Backend Framework

> **Phiên bản:** Scala 3.9.0 · JDK 21 · http4s 0.23.38 (Ember) · Tapir 1.13.33 · circe 0.14.17 · Cats Effect 3.7.x (kéo theo http4s) ·
> sttp-apispec 0.11.10 · ZIO 2.1.27 · ZIO HTTP 3.11.6 · munit 1.2.4 · munit-cats-effect 2.2.0 (bản 2.2.1 kéo theo munit 1.3.6 nên không dùng) · zio-test 2.1.27
> **Code:** [`framework-lab/`](framework-lab/) — **cùng một API `products`** viết 3 lần: http4s, Tapir (chạy trên http4s), ZIO HTTP
> **Trạng thái kiểm chứng:** Phiên bản tra trên Maven Central ngày **16/10/2026**. Code **đã biên dịch** với
> `-deprecation -feature -Wunused:all -Wvalue-discard`, **0 cảnh báo**. **24/24 test pass với thư viện thật** (13 Typelevel + 5 ZIO + 6 lời giải,
> trong đó 1 test chạy qua socket HTTP thật). Server đã chạy thật; mọi trao đổi `curl` trích nguyên văn.
> Review chéo của Codex: sửa lời giải bài 4 (405 bị ghi status 400) và diễn đạt quá mức về bảo đảm của Tapir/OpenAPI (mục 3.3).
> Play Framework: phiên bản tra trên Maven Central, **không có lab** (lý do ở mục 3.2).

---

## Vị trí trong lộ trình

Chương 12 xây API `products` **không dùng framework** để thấy rõ HTTP, router, middleware thực chất là gì.
Chương 16A cho thấy giới hạn của `Future`, Chương 16B giới thiệu Cats Effect `IO`, `Resource`, `Ref`.
Chương này trả lời câu hỏi: **"Trong production, viết backend Scala 3 bằng gì — và vì sao?"**
Kết quả của chương là stack dùng xuyên suốt Chương 14, 15, 17, 18 và Project 2.

> Chương này **không** dạy hết framework nào. Mục tiêu là biết cách **đánh giá** và hiểu **lựa chọn đã chốt**.
> Chi tiết Tapir/http4s (pagination, versioning, middleware, auth) nằm ở Chương 14.

## 1. Mục tiêu học tập

Sau chương này bạn có thể:

1. Giải thích framework backend giải quyết những việc gì mà http-lab ở Chương 12 phải tự viết.
2. Mô tả bốn lựa chọn chính trong Scala 3: **http4s**, **Tapir**, **Play Framework**, **ZIO HTTP** — mô hình lập trình, runtime, hệ sinh thái.
3. Đánh giá một framework theo 8 tiêu chí (hỗ trợ Scala 3, ổn định, tài liệu, độ thân thiện, khả năng test,
   production, tích hợp database, độ phức tạp hệ sinh thái) **dựa trên bằng chứng có thể kiểm tra**.
4. Phân biệt hai cách mô tả API: **route là hàm** (http4s, ZIO HTTP) và **endpoint là dữ liệu** (Tapir).
5. Viết **contract test** chạy cùng một bộ kịch bản trên nhiều cài đặt, không cần mở cổng mạng.
6. Tự kiểm chứng phiên bản thư viện trên Maven Central thay vì tin bài blog.

## 2. Kiến thức tiên quyết

- Chương 12 (HTTP, status code, REST, Problem Details, validation, contract của API `products`).
- Chương 16A (thread, `Future`, vì sao cần effect system) và Chương 16B (`IO`, `Resource`, `Ref`, `IOApp`).
- Chương 8B (`given`/`using`, type class) — để hiểu vì sao "thiếu import" gây lỗi biên dịch ở mục 10.

## 3. Lý thuyết

### 3.1. Framework thay ta làm những gì?

**Vấn đề.** http-lab ở Chương 12 chạy được, nhưng ta đã phải tự viết: parser JSON, router bằng pattern matching trên chuỗi,
middleware, tích lũy lỗi validation, bộ đếm request an toàn đa luồng, graceful shutdown. Mỗi team viết lại những thứ này
thì mỗi team có một bộ bug riêng.

**Framework cung cấp:**

| Việc | Chương 12 tự viết | Framework |
|---|---|---|
| Server HTTP, keep-alive, HTTP/2, TLS | `com.sun.net.httpserver` (giới hạn) | Ember (http4s), Netty (Play/Pekko HTTP, ZIO HTTP) |
| Router | `match` trên `(method, path segments)` | DSL route hoặc mô tả endpoint |
| JSON | `Json.scala` tự viết | circe, zio-json/zio-schema, play-json — derive codec từ case class |
| Middleware | hàm `Handler => Handler` | Có sẵn: CORS, logging, timeout, gzip... |
| Tài nguyên, shutdown | tự quản lý | `Resource` (CE) / `Scope` (ZIO) / lifecycle (Play) |
| Tài liệu API | không có | OpenAPI sinh tự động (Tapir, ZIO HTTP endpoint API) |
| Test không cần mạng | tự dựng | route là hàm → gọi trực tiếp |

**Điều framework KHÔNG làm thay ta:** thiết kế tài nguyên REST, chọn status code đúng, validation nghiệp vụ, mô hình domain,
transaction boundary. Đó là lý do Chương 12 vẫn cần thiết.

### 3.2. Bốn lựa chọn — mô tả và phiên bản đã kiểm chứng

Tra trực tiếp `maven-metadata.xml` trên `repo1.maven.org` ngày 16/10/2026 (artifact có hậu tố `_3` nghĩa là có bản build cho Scala 3):

| Framework | Bản ổn định mới nhất `_3` | Bản đang phát triển | Runtime / hệ sinh thái |
|---|---|---|---|
| **http4s** | `0.23.38` (29/09/2026) | `1.0.0-M49` (milestone) | Cats Effect 3 + fs2; server Ember |
| **Tapir** | `1.13.33` | — | Không phải server: thư viện **mô tả endpoint**, chạy trên http4s, Netty, Pekko HTTP, ZIO HTTP, Play... |
| **Play Framework** | `3.0.12` | `3.1.0-M9` | Apache Pekko (fork của Akka 2.6) |
| **ZIO HTTP** | `3.11.6` | — | ZIO 2 (`2.1.27`) |

Nguồn: `https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/maven-metadata.xml` và tương tự cho
`com/softwaremill/sttp/tapir/tapir-core_3`, `org/playframework/play_3`, `dev/zio/zio-http_3`.

> **Bài học kiểm chứng.** Lần tra đầu tiên dùng giao diện tìm kiếm `search.maven.org` cho kết quả **cũ hơn hơn một năm**
> (http4s 0.23.30, Tapir 1.11.25). Chỉ `maven-metadata.xml` trên `repo1.maven.org` phản ánh đúng bản đã publish.
> Luôn đối chiếu nguồn gốc, đừng dừng ở chỉ mục tìm kiếm.

**http4s.** Thư viện HTTP thuần functional của hệ Typelevel. `HttpRoutes[IO]` về bản chất là hàm
`Request[IO] => OptionT[IO, Response[IO]]` — "có thể có response". Route ghép bằng `<+>` (thử route trái, không khớp thì sang phải).
Series **0.23 là series ổn định** dùng cho production; 1.0 vẫn đang milestone từ nhiều năm nay, đừng chọn milestone cho giáo trình.

**Tapir.** Endpoint được mô tả **như một giá trị**: đầu vào (path, query, header, body), đầu ra, các lỗi có thể có.
Từ cùng giá trị đó, *interpreter* sinh ra route server, tài liệu OpenAPI, hoặc client. Logic nghiệp vụ gắn bằng `serverLogic`.
Tapir không tự chạy server — trong giáo trình nó chạy **trên http4s**.

**Play Framework.** Framework "đầy đủ" kiểu MVC: router file `conf/routes`, controller, cấu hình HOCON, hot reload, template.
Play 3.0 thay Akka bằng Apache Pekko; tài liệu Play 3.0 ghi rõ: *"It switches from Akka and Akka HTTP to Apache Pekko and Apache
Pekko HTTP"* và *"continues to offer support for the latest Java LTS versions and Scala 3"*
(https://www.playframework.com/documentation/3.0.x/Highlights30).
**Không có lab Play trong chương này** vì Play cần sbt plugin và cấu trúc project riêng (`app/`, `conf/`); một lab tối giản
không thể hiện đúng cách Play được dùng, còn một lab đầy đủ thì lạc đề. Ta so sánh Play ở mức kiến trúc.

**ZIO HTTP.** Thư viện HTTP của hệ ZIO, chạy trên Netty. `Routes` gồm các cặp `Method / path -> handler`.
Có API endpoint khai báo (tương tự ý tưởng Tapir) để sinh OpenAPI. Trong giáo trình ZIO chỉ dùng **để so sánh**.

### 3.3. Hai cách mô tả API: route là hàm, endpoint là dữ liệu

**Route là hàm** (http4s, ZIO HTTP):

```scala
case GET -> Root / "api" / "v1" / "products" / LongVar(id) =>
  store.get(id).flatMap {
    case Some(p) => Ok(p)
    case None    => NotFound(Problem(404, "Not Found", s"không có sản phẩm id=$id"))
  }
```

- Ưu: gần với HTTP, linh hoạt tuyệt đối, dễ đọc với người đã hiểu Chương 12, lỗi biên dịch dễ hiểu.
- Nhược: "hợp đồng" API nằm rải rác trong code — status nào có thể trả về, body có dạng gì chỉ biết khi đọc hết handler.
  Tài liệu OpenAPI phải viết tay và **sẽ lệch** khỏi code.

**Endpoint là dữ liệu** (Tapir):

```scala
val getProduct: PublicEndpoint[Long, Problem, Product, Any] =
  endpoint.get
    .in("api" / "v1" / "products" / path[Long]("id"))
    .errorOut(problems(StatusCode.NotFound))
    .out(jsonBody[Product])
```

- Kiểu `PublicEndpoint[Long, Problem, Product, Any]` mô tả **kiểu** của hợp đồng: vào `Long`, lỗi `Problem`, ra `Product`.
  Compiler buộc `serverLogic` trả đúng `IO[Either[Problem, Product]]` — sai **kiểu** là lỗi biên dịch.
- **Nhưng compiler không kiểm tra giá trị.** `Problem.status` là một `Int`; việc chọn status nào nằm trong `oneOfVariantValueMatcher`, được kiểm tra
  lúc **chạy**. Review chéo đã thử: cho logic của `getProduct` (chỉ khai báo lỗi 404) trả `Left(Problem(409, ...))` → **biên dịch được**, khi gọi nhận
  `HTTP=500; BODY=Internal server error` vì không variant nào khớp. Status, quy tắc validation và cấu hình handler vẫn cần **test**.
- OpenAPI sinh từ cùng giá trị nên phần **khai báo** (path, kiểu input/output, status đã khai báo) không lệch khỏi code. OpenAPI không tự biết
  quy tắc nghiệp vụ trong `Validation.validate` (định dạng sku, giá > 0) hay hành vi của interceptor — những thứ đó phải mô tả và test riêng.
- Nhược: thêm một tầng trừu tượng; thông báo lỗi biên dịch dài hơn (mục 10); việc "lạ" (streaming, websocket) cần học thêm API.

**Ở lab, cả hai cách có cùng hành vi trên 5 kịch bản** — điều này được chứng minh bằng **một** `ContractSuite` chạy cho cả hai (mục 7).

### 3.4. Tiêu chí đánh giá — bằng chứng, không phải cảm tính

Thang điểm 1–5 dưới đây là **đánh giá của giáo trình** cho bối cảnh *người mới học Scala 3, làm backend + data, ưu tiên ổn định*.
Mỗi ô có lý do; bạn có thể chấm khác nếu bối cảnh khác (bài tập 1).

| Tiêu chí | http4s 0.23 | Tapir 1.13 (+http4s) | Play 3.0 | ZIO HTTP 3.x |
|---|---|---|---|---|
| **Hỗ trợ Scala 3** | 5 — artifact `_3` từ lâu; lab biên dịch 0 cảnh báo | 5 — `_3`, derive `Schema` bằng macro Scala 3 | 4 — `play_3` có, nhưng nhiều module/plugin cộng đồng đi sau | 5 — `_3`; lab 0 cảnh báo |
| **Ổn định** | 5 — series 0.23 duy trì nhiều năm, đổi API có deprecation trước (ví dụ `UnprocessableEntity` → `UnprocessableContent` ở 0.23.31, mục 10) | 4 — 1.x ổn định nhưng phát hành rất dày (1.13.33) | 4 — 3.0.x ổn định; chuyển Akka→Pekko là thay đổi lớn gần đây | 3 — major 3.x, tốc độ đổi API cao hơn |
| **Tài liệu** | 3 — đủ nhưng giả định người đọc biết Cats Effect | 4 — tài liệu theo chủ đề, nhiều ví dụ | 4 — tài liệu lớn, nhưng nhiều ví dụ cộng đồng còn Scala 2/Akka | 3 — đang hoàn thiện theo tốc độ đổi API |
| **Thân thiện người mới** | 3 — phải hiểu `IO`, `OptionT`, type class | 3 — thêm khái niệm endpoint/interpreter | 4 — mô hình MVC quen thuộc, hot reload | 3 — phải hiểu `ZIO[R, E, A]`, layer |
| **Khả năng test** | 5 — route là hàm, test không cần mạng (lab) | 5 — như http4s + có thể test riêng mô tả endpoint/OpenAPI | 4 — có helper test, cần dựng application | 5 — `routes.runZIO(request)` không cần mạng (lab) |
| **Production** | 5 — Ember, Resource, metrics/tracing qua Typelevel | 5 — chạy trên server production đã chọn | 5 — dùng rộng rãi, nhiều năm | 4 — Netty, hệ ZIO đầy đủ |
| **Tích hợp database** | 5 — Doobie/Skunk cùng hệ Cats Effect | 5 — như http4s | 4 — Slick/Anorm/JPA, mô hình khác Cats Effect | 4 — ZIO JDBC/Quill/Doobie qua interop |
| **Độ phức tạp hệ sinh thái** | 3 — Cats, Cats Effect, fs2 | 3 — thêm Tapir | 3 — Pekko, Guice DI, sbt plugin | 3 — ZIO, layer, zio-schema |

Ghi chú về **bằng chứng có thể kiểm tra** trong bảng: hỗ trợ Scala 3 và phiên bản (Maven Central), deprecation (cảnh báo thật của
compiler ở mục 10), khả năng test không cần mạng (lab chạy được). Các ô "tài liệu", "thân thiện" là nhận định — được ghi lý do để bạn
tự phản biện.

### 3.5. Lựa chọn của giáo trình: **Tapir + http4s (Ember) + circe + Cats Effect 3**

**Lý do:**

1. **Một runtime cho cả BE và DE.** Cats Effect + fs2 dùng tiếp ở Chương 15 (Doobie), Chương 28 (fs2-kafka). Học một mô hình
   effect, dùng cho cả hai nhánh nghề.
2. **Hợp đồng API là dữ liệu có kiểu.** Tapir biến input/lỗi/output thành kiểu — sai kiểu là lỗi biên dịch; giá trị (status nào, quy tắc nào)
   vẫn cần test, nhưng phạm vi lỗi thu hẹp đáng kể so với route viết tay.
   OpenAPI sinh tự động đáp ứng mục "OpenAPI" của Chương 14.
3. **Không khoá chặt server.** Logic viết với Tapir có thể đổi interpreter (Netty, Pekko HTTP) nếu cần.
4. **http4s vẫn hiện diện**: middleware, server, client, test đều là http4s — người học vẫn hiểu tầng dưới.
5. **Test không cần mạng** (contract test của lab) + integration test qua socket thật khi cần (lời giải bài 5).

**Khi nào nên chọn khác:**

- Team đã có hệ thống **Play** lớn, cần hot reload và mô hình MVC quen thuộc → Play 3.0.
- Tổ chức đã chuẩn hoá trên **ZIO** (ZIO Kafka, ZIO JDBC) → ZIO HTTP, hoặc Tapir chạy trên ZIO HTTP.
- API rất nhỏ, nội bộ, không cần OpenAPI → http4s thuần đủ dùng, ít khái niệm hơn.

**Lỗi thiết kế thường gặp khi chọn framework:**

- Chọn theo số sao GitHub hoặc bài blog cũ (nhiều bài vẫn dùng Akka HTTP, `org.tpolecat` cho Doobie, http4s Blaze server).
- Trộn hai hệ effect (Cats Effect và ZIO) trong một service "cho tiện" → hai thread pool, hai mô hình hủy (cancellation), interop khó debug.
  Lab của chương **cố ý tách** ZIO thành project riêng.
- Chọn bản milestone/RC cho production (http4s `1.0.0-M49`, Play `3.1.0-M9`).
- Nghĩ framework thay được thiết kế API: status code sai vẫn sai dù viết bằng framework nào.

### 3.6. Ngôn ngữ vs thư viện

Không có gì trong chương này là **một phần của ngôn ngữ Scala**. `IO`, `HttpRoutes`, `endpoint`, `Routes`, `derives Codec.AsObject`
đều đến từ thư viện ngoài. Thứ ngôn ngữ cung cấp là cơ chế để thư viện hoạt động: `given`/`using` (codec được tìm thấy qua given),
`derives` (gọi macro derive của thư viện), string interpolator (`uri"..."`, `host"..."` được kiểm tra **lúc biên dịch** bởi macro của thư viện),
extension method, pattern matching với extractor (`LongVar`).

### 3.7. Những khác biệt hành vi đã đo trong lab

Cùng "đặc tả", mỗi cách cài đặt vẫn có hành vi mặc định khác nhau ở các trường hợp biên — thứ chỉ phát hiện được khi **chạy thật**:

| Tình huống | http4s thuần | Tapir trên http4s | Ghi chú |
|---|---|---|---|
| `GET /api/v1/products/abc` | `404 Not Found`, body `Not found` (text/plain) | `400 Bad Request`, body `Invalid value for: path parameter id` | http4s: `LongVar` không khớp → không route nào khớp. Tapir: path khớp, decode thất bại |
| `POST` body JSON hỏng | `400` + Problem JSON (do ta tự viết nhánh `Left`) | `400` + `Invalid value for: body (exhausted input)` dạng **text/plain** | Tapir dùng *decode failure handler* mặc định — bài 4 đổi sang Problem JSON |
| `POST` thiếu header `Content-Type` | `201 Created` — decoder circe của http4s chấp nhận | (không đo trong lab) | Chương 14 bàn có nên bắt buộc `Content-Type` |
| OpenAPI | không có | sinh tự động, gồm cả các lỗi `400` do Tapir tự thêm | Xem mục 9 |

Bài học: **contract test phải bao gồm trường hợp biên**, và chọn framework rồi thì cần ghi rõ hành vi mặc định nào ta giữ, hành vi nào ta đổi.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Framework vs library | Framework gọi code của bạn theo khuôn của nó (Play); library được code của bạn gọi (http4s, Tapir) |
| Route / routing | Ánh xạ (method, path) → handler |
| `HttpRoutes[F]` | Route http4s: hàm `Request => OptionT[F, Response]` — có thể "không khớp" |
| `HttpApp[F]` | Route đã "đóng": luôn có response (`orNotFound` thêm 404 cho trường hợp không khớp) |
| Endpoint description | Mô tả endpoint dưới dạng giá trị (Tapir): input, output, error output |
| Interpreter | Thành phần biến mô tả endpoint thành route server / tài liệu / client |
| Server logic | Hàm nghiệp vụ gắn vào endpoint: `Input => F[Either[Error, Output]]` |
| Entity encoder/decoder | Type class của http4s chuyển giữa body HTTP và kiểu Scala |
| Codec | Cặp encoder + decoder (circe `Codec.AsObject`) |
| Schema (Tapir) | Mô tả cấu trúc kiểu dữ liệu để sinh OpenAPI và validate |
| OpenAPI | Chuẩn mô tả REST API dạng YAML/JSON (bản 3.1 trong lab) |
| Decode failure | Request không chuyển được thành input của endpoint (path sai kiểu, body hỏng) |
| Contract test | Test hợp đồng HTTP độc lập với cài đặt; chạy được trên nhiều cài đặt |
| Ember | Server/client HTTP của http4s, viết trên fs2 |
| Milestone (M), Release Candidate (RC) | Bản chưa ổn định; không dùng cho production |

## 5. Ví dụ đơn giản

Một route http4s và cách gọi nó **không cần server**:

```scala
import cats.effect.IO
import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.implicits.*

val hello: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "hello" / name => Ok(s"xin chào $name") }

// HttpApp = route + 404 cho trường hợp không khớp; gọi như một hàm bình thường
val response: IO[Response[IO]] = hello.orNotFound.run(Request[IO](Method.GET, uri"/hello/An"))
```

Đây là ý tưởng quan trọng nhất của chương: **route là hàm**, nên test route là gọi hàm. Không cổng mạng, không `Thread.sleep`.

## 6. Ví dụ thực tế — API `products` viết ba lần

Đặc tả lấy từ Chương 12 (rút gọn còn 2 endpoint để so sánh):

| Request | Kết quả |
|---|---|
| `POST /api/v1/products` hợp lệ | `201 Created` + header `Location` + body `Product` |
| `POST` sku trùng | `409 Conflict` + Problem |
| `POST` vi phạm validation | `422 Unprocessable Content` + Problem liệt kê **mọi** lỗi (sku, name, price) |
| `POST` body không phải JSON | `400 Bad Request` |
| `GET /api/v1/products/{id}` | `200` + `Product`, hoặc `404` + Problem |

Ba cài đặt dùng chung domain và quy tắc validation, khác nhau ở lớp HTTP:

- `typelevel/Http4sApi.scala` — http4s DSL.
- `typelevel/TapirApi.scala` — Tapir endpoint + `serverLogic`, interpret thành `HttpRoutes[IO]`, sinh OpenAPI.
- `zio-http/ZioApi.scala` — ZIO HTTP `Routes`, codec từ zio-schema.

## 7. Code đầy đủ

```
05-backend/framework-lab/
├── typelevel/                         ← stack chính (Cats Effect)
│   ├── project.scala                  ← Scala CLI directives, mọi version
│   └── src/
│       ├── main/scala/vn/scalaeng/fw/
│       │   ├── Domain.scala           ← Product, NewProduct, Problem, Validation, ProductStore (Ref)
│       │   ├── Http4sApi.scala        ← cách 1: http4s DSL
│       │   ├── TapirApi.scala         ← cách 2: Tapir + OpenAPI
│       │   └── Main.scala             ← Http4sMain (8081), TapirMain (8082), PrintOpenApi
│       └── test/scala/vn/scalaeng/fw/
│           ├── ContractSuite.scala    ← MỘT bộ kịch bản, chạy cho CẢ HAI cài đặt
│           └── OpenApiSuite.scala     ← kiểm tra tài liệu sinh ra
└── zio-http/                          ← chỉ để so sánh (project riêng, không trộn hệ effect)
    ├── project.scala
    └── src/{main,test}/scala/vn/scalaeng/fwzio/{ZioApi.scala, ZioApiSpec.scala}
```

Chạy:

```bash
cd 05-backend/framework-lab/typelevel
scala-cli test .
scala-cli run . --main-class vn.scalaeng.fw.TapirMain        # http://127.0.0.1:8082, tài liệu: /docs/openapi.yaml
scala-cli run . --main-class vn.scalaeng.fw.PrintOpenApi

cd ../zio-http
scala-cli test .
scala-cli run . --main-class vn.scalaeng.fwzio.ZioMain       # http://127.0.0.1:8083
```

Trên Windows, thêm `--java-opt -Dstdout.encoding=UTF-8` để chữ tiếng Việt in ra console không thành `?` (xem Chương 2).

## 8. Giải thích code

**`Domain.scala`** — `derives Codec.AsObject` yêu cầu macro của circe sinh encoder/decoder JSON lúc biên dịch.
`ProductStore` dùng `Ref[IO, State]` thay cho `AtomicReference` + vòng lặp CAS của Chương 12: `state.modify` nhận hàm thuần
`State => (State, Result)` và tự lo việc thử lại khi có tranh chấp. Constructor `private` + `ProductStore.empty: IO[ProductStore]`
vì **tạo** trạng thái có thể thay đổi là một effect.

**`Http4sApi.scala`**
- `HttpRoutes.of[IO] { case ... }` — partial function; request không khớp case nào → route "không khớp" (không phải lỗi).
- `LongVar(id)` là *extractor*: chỉ khớp khi đoạn path parse được thành `Long`. Đó là lý do `/products/abc` cho 404 (mục 3.7).
- `req.attemptAs[NewProduct].value` — decode body thành `Either[DecodeFailure, NewProduct]` thay vì ném lỗi, để ta tự chọn response 400.
- `import org.http4s.circe.CirceEntityCodec.*` — cầu nối: biến `Encoder`/`Decoder` của circe thành `EntityEncoder`/`EntityDecoder` của http4s.
  Thiếu dòng này là lỗi số 1 của người mới (mục 10).

**`TapirApi.scala`**
- `oneOfVariantValueMatcher(code, jsonBody[Problem]) { case p => p.status == code.code }` — một kiểu lỗi `Problem`, nhiều status;
  matcher chọn status dựa trên giá trị. Mỗi endpoint chỉ khai báo lỗi **của chính nó** (`problems(StatusCode.NotFound)` cho GET,
  `Conflict` + `UnprocessableEntity` cho POST) — phiên bản đầu của lab dùng chung một danh sách lỗi và OpenAPI đã liệt kê 409 cho GET,
  tức là **tài liệu nói sai**. `OpenApiSuite` có test chặn lỗi này.
- `.out(statusCode(StatusCode.Created)).out(jsonBody[Product]).out(header[String]("Location"))` — output là tuple `(Product, String)`.
- `serverLogic` buộc trả `IO[Either[Problem, Output]]`: lỗi nghiệp vụ là **giá trị**, không phải exception.
- `import sttp.tapir.generic.auto.*` — derive `Schema` tự động cho case class.

**`Main.scala`** — `EmberServerBuilder...build` trả `Resource[IO, Server]`; `useForever` giữ server chạy và **đảm bảo đóng socket** khi
dừng. `<+>` (từ `cats.syntax.semigroupk`) ghép route API với route tài liệu.

**`ContractSuite.scala`** — lớp `abstract` nhận hàm `ProductStore => HttpRoutes[IO]`; hai lớp con một dòng chạy **cùng 5 test** cho
http4s và Tapir. Kỹ thuật này dùng lại ở Chương 14 khi refactor: đổi cài đặt, giữ nguyên test.

**`ZioApi.scala`** — `Method.GET / "api" / "v1" / "products" / long("id") -> handler { (id: Long, _: Request) => ... }`:
cùng ý tưởng route là hàm. `ZIO` có kiểu `ZIO[R, E, A]` (môi trường, lỗi, kết quả); ở đây handler không thể lỗi (`Nothing`).

## 9. Output mong đợi (output thật)

**Test — stack chính** (`scala-cli test .` trong `typelevel/`):

```
Test run vn.scalaeng.fw.Http4sContractSuite finished: 0 failed, 0 ignored, 5 total 0.78s
Test run vn.scalaeng.fw.OpenApiSuite finished: 0 failed, 0 ignored, 3 total 0.01s
Test run vn.scalaeng.fw.TapirContractSuite finished: 0 failed, 0 ignored, 5 total 0.294s
```

**Test — ZIO HTTP** (`zio-http/`):

```
+ ZIO HTTP products
  + id không tồn tại -> 404
  + JSON hỏng -> 400
  + POST hợp lệ -> 201 + Location, GET lại -> 200
  + sku trùng -> 409
  + tích lũy mọi lỗi validation -> 422
5 tests passed. 0 tests failed. 0 tests ignored.
```

**Server Tapir thật + `curl -i`** (dấu `$` là lệnh; header `Date` lược bớt). Trình tự trên server **vừa khởi động** (kho rỗng):
(1) tạo `KB-01` → id 1; (2) tạo `MS-01` từ file `p.json` → id 2; (3) tạo lại `KB-01` → 409; tiếp theo là các request lỗi. `...` là rút gọn
`http://127.0.0.1:8082/api/v1/products -H Content-Type:application/json`. Nội dung `p.json` (lưu UTF-8):
`{"sku":"MS-01","name":"Chuột không dây","price":250000}`.

```
$ curl -i -X POST ... --data-binary @kb.json      # {"sku":"KB-01","name":"Bàn phím","price":450000}
HTTP/1.1 201 Created
Location: /api/v1/products/1

$ curl -i -X POST http://127.0.0.1:8082/api/v1/products -H Content-Type:application/json --data-binary @p.json
HTTP/1.1 201 Created
Connection: keep-alive
Location: /api/v1/products/2
Content-Type: application/json
Content-Length: 66

{"id":2,"sku":"MS-01","name":"Chuột không dây","price":250000}

$ curl -i -X POST ... -d '{"sku":"KB-01","name":"Khác","price":1}'
HTTP/1.1 409 Conflict
Content-Type: application/json

{"status":409,"title":"Conflict","detail":"sku đã tồn tại (id=1)","errors":[]}

$ curl -i -X POST ... -d '{"sku":"x","name":" ","price":0}'
HTTP/1.1 422 Unprocessable Content
Content-Type: application/json

{"status":422,"title":"Unprocessable Content","detail":"dữ liệu không hợp lệ","errors":[{"field":"sku","message":"3-20 ký tự A-Z, 0-9 hoặc dấu gạch ngang"},{"field":"name","message":"không được rỗng"},{"field":"price","message":"phải > 0"}]}

$ curl -i -X POST ... -d '{"sku": '
HTTP/1.1 400 Bad Request
Content-Type: text/plain; charset=UTF-8
Content-Length: 41

Invalid value for: body (exhausted input)

$ curl -i http://127.0.0.1:8082/api/v1/products/abc
HTTP/1.1 400 Bad Request
Content-Type: text/plain; charset=UTF-8

Invalid value for: path parameter id
```

**Server http4s thuần** (cổng 8081) — cùng hai request biên:

```
$ curl -i http://127.0.0.1:8081/api/v1/products/abc
HTTP/1.1 404 Not Found
Content-Type: text/plain; charset=UTF-8
Content-Length: 9

Not found

$ curl -i -X POST http://127.0.0.1:8081/api/v1/products -H Content-Type:application/json -d '{"sku": '
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"status":400,"title":"Bad Request","detail":"body phải là JSON có đủ sku, name, price","errors":[]}
```

**OpenAPI sinh ra** (`PrintOpenApi`, trích phần GET):

```yaml
openapi: 3.1.0
info:
  title: Products API
  version: 1.0.0
paths:
  /api/v1/products/{id}:
    get:
      description: Lấy một sản phẩm theo id
      operationId: getApiV1ProductsId
      parameters:
      - name: id
        in: path
        required: true
        schema:
          type: integer
          format: int64
      responses:
        '200':
          description: ''
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Product'
        '400':
          description: 'Invalid value for: path parameter id'
          content:
            text/plain:
              schema:
                type: string
        '404':
          description: ''
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Problem'
```

Để ý: Tapir **tự thêm** response `400 text/plain` vì nó biết path `id` có thể decode thất bại. Trong `components/schemas/Problem`,
trường `errors` **không** nằm trong `required` vì có giá trị mặc định `Nil`.

## 10. Lỗi thường gặp (thông báo thật)

**1. Quên import cầu nối circe ↔ http4s.** Xóa `import org.http4s.circe.CirceEntityCodec.*` khỏi `Http4sApi.scala`:

```
[error] Http4sApi.scala:13:30
[error] Cannot convert from vn.scalaeng.fw.Product to an Entity, because no EntityEncoder[cats.effect.IO, vn.scalaeng.fw.Product] instance could be found.
[error]         case Some(p) => Ok(p)
[error]                              ^
[error] Http4sApi.scala:18:32
[error] Cannot decode into a value of type vn.scalaeng.fw.NewProduct, because no EntityDecoder[cats.effect.IO, vn.scalaeng.fw.NewProduct] instance could be found.
[error]
[error] The following import might make progress towards fixing the problem:
[error]
[error]   import org.http4s.FormDataDecoder.formEntityDecoder
```

Cẩn thận: compiler **gợi ý sai** (`FormDataDecoder` dành cho form HTML, không phải JSON). Gợi ý import của Scala 3 chỉ là tìm kiếm
những given "có vẻ khớp" trong classpath. Sửa đúng: thêm lại `import org.http4s.circe.CirceEntityCodec.*`.

**2. Quên derive Schema cho Tapir.** Xóa `import sttp.tapir.generic.auto.*`:

```
[error] TapirApi.scala:18:53
[error] Could not find Schema for type vn.scalaeng.fw.Problem.
[error] Schemas can be derived automatically by adding: `import sttp.tapir.generic.auto._`, or manually using `Schema.derived[T]`.
[error] The latter is also useful for debugging derivation errors.
```

Lần này thông báo **đúng và rõ**. Lựa chọn thay thế có kiểm soát hơn: `given Schema[Problem] = Schema.derived` cho từng kiểu.

**3. Dùng API đã deprecated từ ví dụ cũ.** Bản đầu của lab dùng `UnprocessableEntity(...)` như nhiều tài liệu cũ:

```
[warn] .\src/main/scala/vn/scalaeng/fw/Http4sApi.scala:25:15
[warn] method http4sUnprocessableEntitySyntax in trait Responses is deprecated since 0.23.31: use http4sUnprocessableContentSyntax
[warn] .\src/test/scala/vn/scalaeng/fw/ContractSuite.scala:41:32
[warn] value UnprocessableEntity in object Status is deprecated since 0.23.31: now called UnprocessableContent
```

HTTP đổi tên mã 422 thành "Unprocessable Content" (RFC 9110); http4s đổi theo. Đây là lý do bật `-deprecation`.
(Trong Tapir, `StatusCode.UnprocessableEntity` và ZIO HTTP `Status.UnprocessableEntity` **không** báo deprecated ở phiên bản lab.)

**4. Ký tự không phải ASCII trong `uri"..."`.** Viết `uri"/api/v1/products?q=phím"` trong test:

```
[error] .\src/test/scala/vn/scalaeng/solutions/ch13/Ch13SolutionsSuite.scala:32:27
[error] invalid URI: /api/v1/products?q=ph�m
[error] expectation:
[error] * must end the string
```

Đây là lỗi **lúc biên dịch**: interpolator `uri` là macro kiểm tra literal. URI chỉ cho phép ASCII; ký tự khác phải percent-encode.
Sửa: `uri"/api/v1/products".withQueryParam("q", "phím")` — http4s tự encode.

**5. Cảnh báo SLF4J khi chạy test/server:**

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
```

Không phải lỗi: một thư viện trong cây dependency gọi SLF4J nhưng project chưa chọn backend log, nên log bị bỏ đi.
Chương 17 thêm backend log có cấu hình. Đừng "sửa" bằng cách thêm bừa nhiều backend — SLF4J sẽ cảnh báo nhiều binding.

**6. Chữ tiếng Việt bị hỏng khi gửi bằng `curl -d` trên Windows (Git Bash).** `-d '{"name":"Bàn phím"}'` cho response
`"name":"B�n ph�m"`. Server không sai: shell chuyển tham số dòng lệnh sang code page ANSI trước khi `curl` gửi đi. Sửa: lưu body vào
file UTF-8 và gửi `--data-binary @p.json` (mục 9).

## 11. Cách debug

- **Route không khớp (404 bất ngờ):** gọi `routes.orNotFound.run(request)` trong test với đúng method/path; in `request.uri.path.segments`.
  Nhớ thứ tự `<+>`: route bên trái thắng.
- **Lỗi decode body:** dùng `req.attemptAs[A].value` và in `DecodeFailure.getMessage` khi debug (không trả chi tiết nội bộ cho client).
- **Tapir trả 400 text/plain:** đó là decode failure handler mặc định — xem bài 4 để thay bằng Problem JSON.
- **Kiểm tra hợp đồng:** sinh OpenAPI (`PrintOpenApi`) và đọc như người dùng API; so với contract test.
- **Lỗi derive codec/schema dài:** derive tường minh cho từng kiểu (`Schema.derived[Problem]`, `Codec.AsObject.derived`) để compiler chỉ đúng
  trường gây lỗi.
- **Xung đột phiên bản:** `scala-cli dependency-update .` hoặc xem cây phụ thuộc (`sbt dependencyTree` ở Chương 14).

## 12. Bài tập

1. **(Cơ bản — phân tích)** Chấm lại bảng mục 3.4 cho bối cảnh: *team 3 người đã viết Java Spring 5 năm, cần một admin API nội bộ,
   không làm data pipeline.* Chọn framework nào? Viết lý do theo ít nhất 5 tiêu chí.
2. **(Cơ bản — code)** Thêm `GET /api/v1/products?q=...` (lọc theo tên, không phân biệt hoa thường) bằng **cả** http4s và Tapir.
   Gợi ý: `OptionalQueryParamDecoderMatcher` / `query[Option[String]]`. Dùng `ProductStore.list` đã có.
3. **(Trung bình)** Sửa bản http4s để `GET /api/v1/products/abc` trả `400` + Problem thay vì `404 Not found`, mà **không** sửa `Http4sApi.scala`.
4. **(Trung bình)** Làm cho bản Tapir trả Problem JSON (`application/json`) khi decode thất bại thay vì text/plain.
   Gợi ý: `Http4sServerOptions.customiseInterceptors[IO].decodeFailureHandler(...)` với `DefaultDecodeFailureHandler[IO].copy[IO](response = ...)`.
   Kiểm tra cả trường hợp **không phải** decode failure (ví dụ `PUT` → 405): body không được ghi status khác với status HTTP.
5. **(Nâng cao)** Viết integration test chạy server Ember **thật** trên cổng do OS chọn (cổng 0) và gọi bằng Ember client.
   Vì sao không nên hard-code cổng 8082 trong test?
6. **(Nâng cao — nghiên cứu)** Tra Maven Central để trả lời: Doobie đã có bản 1.0.0 final chưa? fs2-kafka 4.0 đã GA chưa?
   Ghi URL `maven-metadata.xml` và ngày tra. (Kết quả ảnh hưởng Chương 15 và 28.)

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch13-solutions.md`](../solutions/ch13-solutions.md)
- Code + test (bài 2–5, 5 test pass với thư viện thật): [`solutions/ch13-code/`](../solutions/ch13-code/)

## 14. Câu hỏi kiểm tra

1. `HttpRoutes[IO]` và `HttpApp[IO]` khác nhau thế nào? `orNotFound` làm gì?
2. Vì sao `GET /products/abc` cho 404 ở http4s nhưng 400 ở Tapir, dù cùng "đặc tả"?
3. Tapir có phải là HTTP server không? Trong giáo trình, request đi qua những thành phần nào?
4. Kiểu `PublicEndpoint[Long, Problem, Product, Any]` cho bạn biết những gì về endpoint?
5. Vì sao lab tách ZIO HTTP thành project riêng thay vì thêm dependency vào project Typelevel?
6. Kể hai thông tin trong bảng so sánh có thể **kiểm chứng**, và hai thông tin là **nhận định**.
7. `derives Codec.AsObject` là tính năng ngôn ngữ hay thư viện? Phần nào thuộc về mỗi bên?
8. Vì sao không chọn http4s `1.0.0-M49` dù nó có số phiên bản "lớn hơn"?

<details><summary>Gợi ý đáp án</summary>

1. `HttpRoutes` có thể không khớp (`OptionT`); `HttpApp` luôn trả response; `orNotFound` biến "không khớp" thành `404`.
2. http4s: extractor `LongVar` không khớp → không route nào khớp → 404. Tapir: path khớp mẫu, nhưng decode `Long` thất bại → decode failure → 400.
3. Không. Request: Ember server → `HttpApp` → route sinh bởi Tapir interpreter → `serverLogic` → `ProductStore`.
4. Input là `Long` (id), lỗi nghiệp vụ kiểu `Problem`, output `Product`, không cần capability đặc biệt (`Any`), không có security input (Public).
5. Tránh hai runtime effect trên một classpath (thread pool, cancellation, interop) và để mỗi project tối giản dependency.
6. Kiểm chứng: phiên bản `_3` trên Maven Central; cảnh báo deprecation; test chạy không cần mạng. Nhận định: "thân thiện người mới", "chất lượng tài liệu".
7. `derives` là cú pháp ngôn ngữ (Scala 3); `Codec.AsObject.derived` (macro sinh code) là của thư viện circe.
8. Milestone không cam kết ổn định API/binary; series 0.23 là bản được duy trì cho production.
</details>

## 15. Mini project

**"Hội đồng chọn framework"**: viết một ADR (*Architecture Decision Record*) 1–2 trang cho một hệ thống giả định
(ví dụ: dịch vụ đặt vé, 20 endpoint, PostgreSQL, Kafka consumer). ADR gồm: bối cảnh, các lựa chọn, bảng tiêu chí có **bằng chứng**
(link Maven Central, kết quả chạy thử), quyết định, hệ quả, điều kiện xem xét lại.
Kèm một prototype 1 endpoint bằng framework bạn chọn + 1 contract test.

## 16. Nguồn tài liệu chính thức

- http4s: https://http4s.org/ · releases: https://github.com/http4s/http4s/releases
- Tapir: https://tapir.softwaremill.com/ (endpoint, schemas, server interpreters, OpenAPI)
- Play Framework 3.0 highlights: https://www.playframework.com/documentation/3.0.x/Highlights30
- ZIO HTTP: https://zio.dev/zio-http/
- Cats Effect: https://typelevel.org/cats-effect/
- circe: https://circe.github.io/circe/
- munit-cats-effect: https://github.com/typelevel/munit-cats-effect
- OpenAPI 3.1: https://spec.openapis.org/oas/v3.1.0
- RFC 9110 (HTTP Semantics, mã 422 "Unprocessable Content"): https://www.rfc-editor.org/rfc/rfc9110
- Maven Central (tra phiên bản): https://repo1.maven.org/maven2/

## 17. Checklist tự đánh giá

- [ ] Kể được 5 việc framework làm thay http-lab của Chương 12, và 3 việc framework không làm thay.
- [ ] Giải thích được "route là hàm" và "endpoint là dữ liệu", ưu/nhược mỗi cách.
- [ ] Tự tra được phiên bản `_3` mới nhất của một thư viện trên `repo1.maven.org`.
- [ ] Chạy được `scala-cli test .` trong `framework-lab/typelevel` và `framework-lab/zio-http`.
- [ ] Viết được một contract test chạy cho hai cài đặt.
- [ ] Đọc được OpenAPI sinh ra và chỉ ra response nào do Tapir tự thêm.
- [ ] Sửa được lỗi thiếu `EntityEncoder` / thiếu `Schema` mà không cần tra mạng.
- [ ] Giải thích được vì sao giáo trình chọn Tapir + http4s + Cats Effect, và khi nào nên chọn khác.
