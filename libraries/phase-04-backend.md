# Thư viện — Phase 4: Backend, database và concurrency

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| Cats Effect / testkit | 3.7.1 | `_3` | Effect runtime, thời gian ảo | Ch16B, Ch13–18 |
| munit-cats-effect | 2.2.0 | `_3` | Test IO | Ch16B, backend |
| http4s Ember / DSL / Circe / client | 0.23.38 | `_3` | HTTP server và kiểm thử | Ch13–18 |
| Circe core / parser | 0.14.17 | `_3` | JSON codec/parse | Ch13–18 |
| Tapir | 1.13.33 | `_3` | Contract endpoint | Ch13–18 |
| sttp-apispec YAML | 0.11.10 | `_3` | Xuất OpenAPI | Ch13–14, P2 |
| ZIO / Test / adapter sbt | 2.1.27 | `_3` | Stack so sánh | Ch13 |
| ZIO HTTP | 3.11.6 | `_3` | HTTP so sánh | Ch13 |
| Doobie | 1.0.0-RC13 | `_3`, RC | SQL có kiểu, pool | Ch15, P2 |
| Flyway core / PostgreSQL | 13.10.3 | Java | Migration | Ch15, P2 |
| PostgreSQL JDBC | 42.7.14 | Java | Driver | Ch15, P2 |
| HikariCP | 7.0.2 | Java | Connection pool | P2 (Ch15 bắc cầu Doobie) |
| PostgreSQL server | 18.6 | Ngoài JVM | Database | Ch15, P2 |
| zonky + binaries | 2.2.2 / 18.6.0 | Java + binary OS | PostgreSQL thật trong test | Ch15, P2 |
| Password4j | 1.8.4 | Java | Băm mật khẩu | Ch17, P2 |
| jwt-circe | 11.0.4 | `_3` | Ký/kiểm JWT | Ch17, P2 |
| log4cats / Logback | 2.8.0 / 1.6.5 | `_3` / Java | Logging | Ch17, P2 |
| http4s Prometheus metrics | 0.25.0 | `_3` | HTTP metrics | Ch17 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

Phạm vi build gồm `05-backend`, `06-database`, `07-concurrency` và P2. P2 được xếp vào phase 4 cho dependency lần đầu (ví dụ HikariCP khai báo trực tiếp); [phase 8](phase-08-projects.md) giải thích cách ghép project. munit dùng lại: [1.2.4](tooling.md#munit); plugin P2 dùng lại: [sbt-assembly 2.3.1](tooling.md#assembly). Play, Slick, Pekko/Akka chỉ là lựa chọn so sánh trong chương, không có dependency build trực tiếp nên không lập mục thư viện sử dụng.

Bằng chứng test theo họ: [T01](../.team/handoffs/T01-ch16b-cats-effect.md), [T02](../.team/handoffs/T02-ch13-framework.md), [T03](../.team/handoffs/T03-ch14-rest-apis.md), [T04](../.team/handoffs/T04-ch15-database.md), [T05](../.team/handoffs/T05-ch17-security.md), [T07/P2](../.team/handoffs/T07-ch18-p2-backend.md). T27 không thực hiện lại các kiểm chứng đó.

<a id="cats-effect"></a>
## Cats Effect

### 1. Là gì / giải quyết vấn đề gì

Cats Effect mô tả tác dụng phụ bằng `IO` rồi dùng runtime để thực thi có kiểm soát. Backend cần tài nguyên, hủy tác vụ và timeout; biểu diễn chúng rõ ràng giúp tránh connection bị đóng quá sớm hoặc task tiếp tục ghi sau khi request hết hạn.

### 2. Phiên bản trong giáo trình

- `org.typelevel:cats-effect_3:3.7.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect_3/3.7.1/cats-effect_3-3.7.1.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`IO` là mô tả tính toán; `IOApp` là điểm vào; fiber là tác vụ nhẹ có thể join/cancel. `Resource` ghép acquire/use/release, `Ref.modify` cập nhật nguyên tử, `Semaphore` giới hạn đồng thời. `IO.blocking` dành cho lời gọi chặn; timeout không tự làm JDBC ngừng tức thì.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala:50](../07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala#L50). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala:50-60 -->
```scala
  def quote(services: Services, pool: FakePool, requests: Ref[IO, Long], budget: FiniteDuration): IO[Quote] =
    def borrowed[A](action: IO[A]): IO[A] = pool.connection.use(_ => action)
    requests.update(_ + 1) *>
      (
        borrowed(services.price).timeout(budget),
        borrowed(services.stock).timeoutTo(budget, IO.pure(0)),
        borrowed(services.promotion).timeoutTo(budget, IO.pure(0L))
      ).parMapN(Quote.apply)

  val demoServices: Services = Services(
    IO.sleep(20.millis).as(120_000L),
```

### 5. Lựa chọn thay thế

`Future` phù hợp tích hợp API eager hiện có nhưng không có mô hình cancellation như IO. ZIO có typed error/environment và hệ sinh thái riêng; chọn một runtime cho app thay vì bắc cầu IO/ZIO ở mọi tầng.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Could not find an implicit IORuntime.” — [07-concurrency/ch16b-cats-effect.md:478](../07-concurrency/ch16b-cats-effect.md#L478). Dùng IOApp ở biên chạy; không rải unsafeRunSync trong service.

### 7. Dùng ở đâu trong giáo trình

[07-concurrency/ch16b-cats-effect.md](../07-concurrency/ch16b-cats-effect.md); [05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [05-backend/ch18-backend-architecture.md](../05-backend/ch18-backend-architecture.md)

### 8. Tài liệu chính thức

[Cats Effect](https://typelevel.org/cats-effect/), [Resource](https://typelevel.org/cats-effect/docs/std/resource).

<a id="effect-testing"></a>
## munit-cats-effect và Cats Effect testkit

### 1. Là gì / giải quyết vấn đề gì

`munit-cats-effect` cho test trả về `IO`, để framework thực thi và chờ kết quả assertion. `cats-effect-testkit` điều khiển lịch chạy/thời gian ảo, giúp test timeout/retry có thể tái hiện mà không phải chờ đồng hồ thật.

### 2. Phiên bản trong giáo trình

- `org.typelevel:cats-effect-testkit_3:3.7.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect-testkit_3/3.7.1/cats-effect-testkit_3-3.7.1.pom); HTTP 200, 18/10/2026.
- `org.typelevel:munit-cats-effect_3:2.2.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`CatsEffectSuite`, `ResourceSuiteLocalFixture` và `TestControl.executeEmbed` phục vụ test effect/tài nguyên. TestControl giả lập lịch của effect được kiểm soát, không giả lập JDBC, socket hay thread ngoài runtime. Giữ adapter **2.2.0** vì 2.2.1 kéo munit 1.3.6, lệch chuẩn 1.2.4.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [07-concurrency/effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala:63](../07-concurrency/effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala#L63). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 07-concurrency/effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala:63-74 -->
```scala
  test("TestControl: retry lỗi tạm thời, thời gian ảo chính xác 100+200ms") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      begin <- IO.monotonic
      result <- Retry(count.updateAndGet(_ + 1).flatMap { n =>
        if n < 3 then IO.raiseError[Int](java.io.IOException("retry")) else IO.pure(n)
      }, Retry.backoff(100.millis, 1.second, 4))(_.isInstanceOf[java.io.IOException])
      end <- IO.monotonic
    yield
      assertEquals(result, 3)
      assertEquals(end - begin, 300.millis)
    )
```

### 5. Lựa chọn thay thế

Test đồng hồ thật phù hợp integration/network nhưng dễ chậm và flaky; TestControl phù hợp logic sleep/retry thuần IO. Fake clock tự truyền được cho core nhưng không điều khiển mọi timer runtime.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “bỏ assertion IO” — [07-concurrency/ch16b-cats-effect.md:433](../07-concurrency/ch16b-cats-effect.md#L433). Assertion trả IO cần được ghép/trả về test; tạo IO rồi bỏ kết quả không thực thi assertion.

### 7. Dùng ở đâu trong giáo trình

[07-concurrency/ch16b-cats-effect.md](../07-concurrency/ch16b-cats-effect.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md)

### 8. Tài liệu chính thức

[munit-cats-effect](https://typelevel.org/munit-cats-effect/), [TestControl](https://typelevel.org/cats-effect/docs/core/test-runtime).

<a id="http4s"></a>
## http4s: Ember, DSL, Circe và client

### 1. Là gì / giải quyết vấn đề gì

http4s biểu diễn HTTP bằng các hàm trên request/response có effect. Ember cung cấp server/client thật, DSL làm route dễ đọc, còn module Circe nối JSON codec vào body HTTP; tách các vai trò này giúp phân biệt lỗi contract với lỗi transport.

### 2. Phiên bản trong giáo trình

- `org.http4s:http4s-circe_3:0.23.38` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/http4s/http4s-circe_3/0.23.38/http4s-circe_3-0.23.38.pom); HTTP 200, 18/10/2026.
- `org.http4s:http4s-dsl_3:0.23.38` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/http4s/http4s-dsl_3/0.23.38/http4s-dsl_3-0.23.38.pom); HTTP 200, 18/10/2026.
- `org.http4s:http4s-ember-client_3:0.23.38` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/http4s/http4s-ember-client_3/0.23.38/http4s-ember-client_3-0.23.38.pom); HTTP 200, 18/10/2026.
- `org.http4s:http4s-ember-server_3:0.23.38` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/http4s/http4s-ember-server_3/0.23.38/http4s-ember-server_3-0.23.38.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`HttpRoutes[IO]` có thể không khớp route; `.orNotFound` tạo HttpApp đầy đủ. `EntityEncoder`/`EntityDecoder` chuyển body, builder Ember dùng Resource để đóng cổng/kết nối. `http4s-ember-client` được khai báo ở Test trong nhiều build để kiểm socket thật.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/Http4sApi.scala:11](../05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/Http4sApi.scala#L11). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/Http4sApi.scala:11-20 -->
```scala
  def routes(store: ProductStore): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "api" / "v1" / "products" / LongVar(id) =>
      store.get(id).flatMap {
        case Some(p) => Ok(p)
        case None    => NotFound(Problem(404, "Not Found", s"không có sản phẩm id=$id"))
      }

    case req @ POST -> Root / "api" / "v1" / "products" =>
      req.attemptAs[NewProduct].value.flatMap {
        case Left(_) =>
```

### 5. Lựa chọn thay thế

JDK HttpServer ở Ch12 phù hợp học giao thức, ít abstraction nhưng phải tự xử lý nhiều thứ. Tapir đặt contract lên trên http4s; ZIO HTTP phù hợp app đã chọn ZIO. Chọn http4s thuần khi route đơn giản và cần kiểm soát rõ request/response.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “method http4sUnprocessableEntitySyntax in trait Responses is deprecated since 0.23.31: use http4sUnprocessableContentSyntax” — [05-backend/ch13-choosing-a-framework.md:490](../05-backend/ch13-choosing-a-framework.md#L490). Dùng tên UnprocessableContent ở http4s; không tự đổi tên enum của Tapir/ZIO theo http4s.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[http4s docs 0.23](https://http4s.org/v0.23/), [Ember](https://http4s.org/v0.23/docs/client.html).

<a id="circe"></a>
## Circe core và parser

### 1. Là gì / giải quyết vấn đề gì

Circe chuyển dữ liệu Scala thành JSON và đọc JSON thành kiểu nghiệp vụ. Core chứa mô hình JSON/codec; parser đọc văn bản JSON, nên dùng parser không thay cho validation quy tắc miền hay kiểm Content-Type.

### 2. Phiên bản trong giáo trình

- `io.circe:circe-core_3:0.14.17` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/circe/circe-core_3/0.14.17/circe-core_3-0.14.17.pom); HTTP 200, 18/10/2026.
- `io.circe:circe-parser_3:0.14.17` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`Json`, `Encoder`, `Decoder`, `Codec.AsObject`, `asJson` và `parser.decode` là các điểm chính. Codec quy định hình dạng wire format; code derive cho enum có thể không ra chuỗi như client mong đợi. `circe-generic` được khai báo trực tiếp lần đầu ở streaming, xem phase 6.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala:13](../05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala#L13). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala:13-20 -->
```scala
object Role:
  /** JSON là chuỗi "Customer"/"Admin". `derives Codec.AsObject` cho enum sẽ ra `{"Customer":{}}` — hình dạng tệ cho API. */
  given Codec[Role] = Codec.from(
    Decoder[String].emap(s => Role.values.find(_.toString == s).toRight(s"vai trò không hợp lệ: $s")),
    Encoder[String].contramap(_.toString)
  )

final case class AuthUser(id: Long, email: String, role: Role)
```

### 5. Lựa chọn thay thế

ZIO Schema thuận khi app đã dùng ZIO; Jackson phổ biến ở hệ Java nhưng cách cấu hình khác. Circe phù hợp kiểu bất biến và codec tường minh; không derive mọi domain công khai nếu wire contract cần ổn định độc lập.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Enum ra JSON dạng `{"Customer":{}}`” — [05-backend/ch17-security-production.md:442](../05-backend/ch17-security-production.md#L442). Kiểm JSON thật và viết codec chuỗi khi contract yêu cầu.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [08-data-processing/ch19-scala-data-processing.md](../08-data-processing/ch19-scala-data-processing.md) dùng lại.

### 8. Tài liệu chính thức

[Circe](https://circe.github.io/circe/), [encoding/decoding](https://circe.github.io/circe/codecs/).

<a id="tapir"></a>
## Tapir

### 1. Là gì / giải quyết vấn đề gì

Tapir mô tả endpoint như dữ liệu gồm input, output và lỗi. Một contract có thể được diễn giải thành route server và tài liệu OpenAPI, giảm việc route thực tế và đặc tả bị viết hai lần rồi lệch nhau.

### 2. Phiên bản trong giáo trình

- `com.softwaremill.sttp.tapir:tapir-http4s-server_3:1.13.33` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/softwaremill/sttp/tapir/tapir-http4s-server_3/1.13.33/tapir-http4s-server_3-1.13.33.pom); HTTP 200, 18/10/2026.
- `com.softwaremill.sttp.tapir:tapir-json-circe_3:1.13.33` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/softwaremill/sttp/tapir/tapir-json-circe_3/1.13.33/tapir-json-circe_3-1.13.33.pom); HTTP 200, 18/10/2026.
- `com.softwaremill.sttp.tapir:tapir-openapi-docs_3:1.13.33` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/softwaremill/sttp/tapir/tapir-openapi-docs_3/1.13.33/tapir-openapi-docs_3-1.13.33.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`PublicEndpoint`, `jsonBody`, `Schema`, `oneOf`, `serverLogic`, `Http4sServerInterpreter` và `OpenAPIDocsInterpreter` nối contract với implementation. Module http4s-server chạy endpoint, json-circe cung cấp JSON body, openapi-docs sinh mô hình OpenAPI; lỗi decode và lỗi nghiệp vụ là hai đường khác nhau.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala:27](../05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala#L27). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala:27-33 -->
```scala
  val getProduct: PublicEndpoint[Long, Problem, Product, Any] =
    products.get
      .in(path[Long]("id"))
      .errorOut(problems(StatusCode.NotFound))
      .out(jsonBody[Product])
      .description("Lấy một sản phẩm theo id")

```

### 5. Lựa chọn thay thế

Route http4s thuần ít lớp hơn cho API nhỏ; Tapir có lợi khi muốn schema/contract test và tài liệu cùng nguồn. Viết OpenAPI thủ công thích hợp contract-first nhưng phải kiểm đồng bộ implementation.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Could not find Schema for type vn.scalaeng.fw.Problem.” — [05-backend/ch13-choosing-a-framework.md:479](../05-backend/ch13-choosing-a-framework.md#L479). Import generic.auto hoặc khai báo Schema.derived tường minh; codec JSON và Schema Tapir là hai type class khác nhau.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[Tapir](https://tapir.softwaremill.com/en/latest/), [http4s interpreter](https://tapir.softwaremill.com/en/latest/server/http4s.html).

<a id="apispec"></a>
## sttp-apispec OpenAPI Circe YAML

### 1. Là gì / giải quyết vấn đề gì

sttp-apispec biểu diễn đặc tả API, còn module openapi-circe-yaml xuất mô hình OpenAPI thành YAML. Tapir sinh mô hình từ endpoint; bước xuất YAML tạo artifact cho người đọc hoặc công cụ kiểm contract.

### 2. Phiên bản trong giáo trình

- `com.softwaremill.sttp.apispec:openapi-circe-yaml_3:0.11.10` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/softwaremill/sttp/apispec/openapi-circe-yaml_3/0.11.10/openapi-circe-yaml_3-0.11.10.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Import `sttp.apispec.openapi.circe.yaml.*` thêm `.toYaml`. Tên import `sttp.apispec` khác groupId Maven `com.softwaremill.sttp.apispec`. Lab dùng OpenAPI 3.1 theo bằng chứng Ch13, không suy từ version 0.11.10 thành số phiên bản OpenAPI.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala:56](../05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala#L56). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/framework-lab/typelevel/src/main/scala/vn/scalaeng/fw/TapirApi.scala:56-60 -->
```scala
  def routes(store: ProductStore): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(serverEndpoints(store))

  val openApiYaml: String =
    OpenAPIDocsInterpreter().toOpenAPI(List(getProduct, createProduct), "Products API", "1.0.0").toYaml
```

### 5. Lựa chọn thay thế

Có thể giữ mô hình OpenAPI để kiểm trong test thay vì serialize; YAML thuận cho artifact đọc bằng mắt. Spec thủ công cần thêm kiểm drift; sinh tự động vẫn cần test đủ error variants.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Body HTTP 401 vẫn chứa status 400.” — [05-backend/ch14-rest-apis.md:844](../05-backend/ch14-rest-apis.md#L844). YAML sinh đúng kiểu chưa chứng minh HTTP/body thực tế nhất quán; contract test phải so cả hai.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/ch14-rest-apis.md](../05-backend/ch14-rest-apis.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[sttp-apispec](https://github.com/softwaremill/sttp-apispec), [Tapir OpenAPI](https://tapir.softwaremill.com/en/latest/docs/openapi.html).

<a id="zio"></a>
## ZIO, ZIO Test và ZIO HTTP (stack so sánh)

### 1. Là gì / giải quyết vấn đề gì

ZIO là effect runtime khác với Cats Effect, biểu diễn cả dependency môi trường và lỗi có kiểu. ZIO HTTP cung cấp server/routing trên runtime này; giáo trình giữ một lab so sánh cùng domain sản phẩm để người học thấy mô hình thay đổi mà nghiệp vụ không đổi.

### 2. Phiên bản trong giáo trình

- `dev.zio:zio-http_3:3.11.6` — [POM Maven Central](https://repo.maven.apache.org/maven2/dev/zio/zio-http_3/3.11.6/zio-http_3-3.11.6.pom); HTTP 200, 18/10/2026.
- `dev.zio:zio-test-sbt_3:2.1.27` — [POM Maven Central](https://repo.maven.apache.org/maven2/dev/zio/zio-test-sbt_3/2.1.27/zio-test-sbt_3-2.1.27.pom); HTTP 200, 18/10/2026.
- `dev.zio:zio-test_3:2.1.27` — [POM Maven Central](https://repo.maven.apache.org/maven2/dev/zio/zio-test_3/2.1.27/zio-test_3-2.1.27.pom); HTTP 200, 18/10/2026.
- `dev.zio:zio_3:2.1.27` — [POM Maven Central](https://repo.maven.apache.org/maven2/dev/zio/zio_3/2.1.27/zio_3-2.1.27.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`ZIO[R,E,A]`, `Ref`, `ZIOAppDefault`, `Routes`, `handler` và `Server.serve` là các điểm vào. `zio-test` chứa đặc tả/assertion, `zio-test-sbt` là cầu nối framework runner; lab chọn framework ZIO Test chứ không ghép munit vào runtime này. Schema JSON trong ví dụ đến từ zio-schema bắc cầu, không phải Circe.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/framework-lab/zio-http/src/main/scala/vn/scalaeng/fwzio/ZioApi.scala:39](../05-backend/framework-lab/zio-http/src/main/scala/vn/scalaeng/fwzio/ZioApi.scala#L39). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/framework-lab/zio-http/src/main/scala/vn/scalaeng/fwzio/ZioApi.scala:39-46 -->
```scala
  def routes(state: Ref[State]): Routes[Any, Nothing] = Routes(
    Method.GET / "api" / "v1" / "products" / long("id") -> handler { (id: Long, _: Request) =>
      state.get.map(_.byId.get(id) match
        case Some(p) => json(Status.Ok, p)
        case None    => json(Status.NotFound, Problem(404, "Not Found", s"không có sản phẩm id=$id")))
    },
    Method.POST / "api" / "v1" / "products" -> handler { (req: Request) =>
      req.body.to[NewProduct].foldZIO(
```

### 5. Lựa chọn thay thế

Cats Effect/http4s là đường chính của giáo trình; ZIO phù hợp team chọn typed error/environment xuyên suốt. Học cả hai runtime cùng lúc làm tăng tải khái niệm; so sánh contract và tài nguyên trước khi chọn.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Trong Tapir, `StatusCode.UnprocessableEntity` và ZIO HTTP `Status.UnprocessableEntity`” — [05-backend/ch13-choosing-a-framework.md:496](../05-backend/ch13-choosing-a-framework.md#L496). Các tên này không báo deprecated ở bản lab; tránh áp API http4s sang ZIO chỉ vì cùng HTTP 422.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch13-choosing-a-framework.md](../05-backend/ch13-choosing-a-framework.md); [05-backend/framework-lab/zio-http/src/test/scala/vn/scalaeng/fwzio/ZioApiSpec.scala](../05-backend/framework-lab/zio-http/src/test/scala/vn/scalaeng/fwzio/ZioApiSpec.scala); 5 test ZIO có bằng chứng T02/audit.

### 8. Tài liệu chính thức

[ZIO](https://zio.dev/), [ZIO HTTP](https://ziohttp.com/), [ZIO Test](https://zio.dev/reference/test/).

<a id="doobie"></a>
## Doobie

### 1. Là gì / giải quyết vấn đề gì

Doobie mô tả chương trình SQL có kiểu rồi chạy qua transactor trong effect. Backend cần ranh giới transaction rõ để nhiều câu lệnh cùng commit/rollback; SQL vẫn hiện ra để người học phân tích query plan và constraint.

### 2. Phiên bản trong giáo trình

- `org.typelevel:doobie-core_3:1.0.0-RC13` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/doobie-core_3/1.0.0-RC13/doobie-core_3-1.0.0-RC13.pom); HTTP 200, 18/10/2026.
- `org.typelevel:doobie-hikari_3:1.0.0-RC13` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/doobie-hikari_3/1.0.0-RC13/doobie-hikari_3-1.0.0-RC13.pom); HTTP 200, 18/10/2026.
- `org.typelevel:doobie-postgres_3:1.0.0-RC13` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/doobie-postgres_3/1.0.0-RC13/doobie-postgres_3-1.0.0-RC13.pom); HTTP 200, 18/10/2026.

Có `_3`, dùng Scala 3.9.0/JDK 21; **Release Candidate**, không gọi là 1.0 final. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`ConnectionIO[A]`, `sql`, `Fragment`, `query`, `update` và `.transact(xa)` ghép chương trình JDBC. Core chứa chương trình SQL, postgres thêm kiểu/cú pháp PostgreSQL, hikari tích hợp pool. **RC13 đổi cả groupId và package sang org.typelevel**; không copy import doobie từ bản cũ.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:28](../06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala#L28). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:28-37 -->
```scala
  def transactor(cfg: DbConfig): Resource[IO, HikariTransactor[IO]] =
    val hc = new HikariConfig()
    hc.setDriverClassName("org.postgresql.Driver")
    hc.setJdbcUrl(cfg.url)
    hc.setUsername(cfg.user)
    hc.setPassword(cfg.password)
    hc.setMaximumPoolSize(cfg.poolSize)
    hc.setPoolName("scalaeng-db")
    HikariTransactor.fromHikariConfig[IO](hc)

```

### 5. Lựa chọn thay thế

JDBC trực tiếp ít lớp nhưng phải tự quản connection/rollback; Slick dùng mô hình query khác. Chọn Doobie khi muốn SQL tường minh và Resource/IO của stack Typelevel; SQL compile được không chứng minh query hợp schema DB hiện tại.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Not found: doobie - did you mean Double?” — [06-database/ch15-database-engineering.md:520](../06-database/ch15-database-engineering.md#L520). Dùng import org.typelevel.doobie và Cats syntax riêng theo Ch15.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[Doobie](https://typelevel.org/doobie/), [release](https://github.com/typelevel/doobie/releases).

<a id="flyway"></a>
## Flyway core và module PostgreSQL

### 1. Là gì / giải quyết vấn đề gì

Flyway đưa schema database từ trạng thái này sang trạng thái khác bằng migration có phiên bản. Nó giúp service biết schema nào đã áp dụng trước khi nhận request và giảm việc mỗi máy dev tự sửa DB khác nhau.

### 2. Phiên bản trong giáo trình

- `org.flywaydb:flyway-core:13.10.3` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/flywaydb/flyway-core/13.10.3/flyway-core-13.10.3.pom); HTTP 200, 18/10/2026.
- `org.flywaydb:flyway-database-postgresql:13.10.3` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/flywaydb/flyway-database-postgresql/13.10.3/flyway-database-postgresql-13.10.3.pom); HTTP 200, 18/10/2026.

Thư viện Java, không có hậu tố Scala; đã dùng từ Scala 3 qua JDBC. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Core quản lý lịch sử migration/checksum; module PostgreSQL nhận diện/tương tác database tương ứng. `configure().dataSource(...).load().migrate()` chạy ở IO.blocking trong lab. Migration đã áp dụng là lịch sử; sửa nội dung file cũ làm lệch checksum thay vì tạo bước nâng cấp mới.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:21](../06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala#L21). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:21-26 -->
```scala
object Database:
  /** Chạy mọi migration chưa chạy. Trả số migration vừa áp dụng (0 nếu schema đã mới nhất). */
  def migrate(cfg: DbConfig): IO[Int] = IO.blocking {
    Flyway.configure().dataSource(cfg.url, cfg.user, cfg.password).load().migrate().migrationsExecuted
  }

```

### 5. Lựa chọn thay thế

Liquibase có mô hình changelog đa dạng; SQL thủ công đơn giản nhưng khó giữ lịch sử nhất quán. Flyway phù hợp bộ migration SQL tuyến tính của project; vẫn cần kiểm rollback/recovery và tránh nhiều replica tranh migration không có kế hoạch.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “ERROR: current transaction is aborted, commands ignored until end of transaction block” — [06-database/ch15-database-engineering.md:575](../06-database/ch15-database-engineering.md#L575). Lỗi này thuộc PostgreSQL/transaction, không phải Flyway tự sửa được; kiểm migration và transaction độc lập.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[Flyway](https://documentation.red-gate.com/flyway), [PostgreSQL driver support](https://documentation.red-gate.com/flyway/reference/database-driver-reference/postgresql-database).

<a id="jdbc"></a>
## PostgreSQL JDBC driver

### 1. Là gì / giải quyết vấn đề gì

JDBC driver dịch lời gọi JDBC thành giao thức PostgreSQL để ứng dụng JVM trao đổi với server. Nó không chứa database server và không tự tạo transaction nghiệp vụ; Doobie/Hikari dùng nó như lớp kết nối bên dưới.

### 2. Phiên bản trong giáo trình

- `org.postgresql:postgresql:42.7.14` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.pom); HTTP 200, 18/10/2026.

Java/JDBC, dùng được từ Scala 3; không suy hỗ trợ một wrapper Scala từ việc driver Java chạy được. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

JDBC URL, `org.postgresql.Driver`, connection, statement và SQLSTATE là các thành phần chính. Query parameter được bind qua prepared statement; timeout connect/socket khác deadline effect. Version driver 42.7.14 khác version server 18.6.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:28](../06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala#L28). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:28-37 -->
```scala
  def transactor(cfg: DbConfig): Resource[IO, HikariTransactor[IO]] =
    val hc = new HikariConfig()
    hc.setDriverClassName("org.postgresql.Driver")
    hc.setJdbcUrl(cfg.url)
    hc.setUsername(cfg.user)
    hc.setPassword(cfg.password)
    hc.setMaximumPoolSize(cfg.poolSize)
    hc.setPoolName("scalaeng-db")
    HikariTransactor.fromHikariConfig[IO](hc)

```

### 5. Lựa chọn thay thế

Gọi JDBC trực tiếp phù hợp adapter nhỏ; Doobie thêm composability trên JDBC nhưng không bỏ đặc tính blocking. Thay driver không biến PostgreSQL thành DB khác; test SQL với đúng engine.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “SQLState=23505” — [06-database/ch15-database-engineering.md:549](../06-database/ch15-database-engineering.md#L549). Phân loại theo mã SQLSTATE ổn định; đừng parse văn bản lỗi hoặc trả chi tiết email cho client.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[pgJDBC](https://jdbc.postgresql.org/documentation/).

<a id="hikari"></a>
## HikariCP

### 1. Là gì / giải quyết vấn đề gì

HikariCP tái sử dụng một nhóm connection JDBC để request không phải mở TCP/authentication lại mỗi lần. Pool là tài nguyên hữu hạn: tăng kích thước không đảm bảo DB xử lý nhanh hơn và có thể làm database quá tải.

### 2. Phiên bản trong giáo trình

- `com.zaxxer:HikariCP:7.0.2` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.pom); HTTP 200, 18/10/2026.

Java; artifact không gắn binary version Scala. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`HikariConfig`, maximumPoolSize, connectionTimeout, poolName và Resource đóng pool là các điểm lab dùng. P2 khai báo trực tiếp **7.0.2**, còn Ch15 lấy qua doobie-hikari; pool timeout chờ connection khác socket timeout của driver.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala:20](../14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala#L20). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala:20-31 -->
```scala
  def transactor(c: DbConfig): Resource[IO, HikariTransactor[IO]] =
    val h = new HikariConfig()
    h.setJdbcUrl(c.url)
    h.setUsername(c.user)
    h.setPassword(c.password)
    h.setMaximumPoolSize(c.poolSize)
    h.setConnectionTimeout(2000L)
    h.setPoolName("p2-db")
    // JDBC bounds complement the effect timeout; cancellation of blocking JDBC is not instantaneous.
    h.addDataSourceProperty("connectTimeout", "2")
    h.addDataSourceProperty("socketTimeout", "5")
    HikariTransactor.fromHikariConfig[IO](h)
```

### 5. Lựa chọn thay thế

Mở connection từng query đơn giản nhưng đắt; dùng pool khác cần so hành vi timeout/metrics và shutdown. Chọn Hikari theo adapter doobie-hikari của giáo trình, đo số connection thực tế trước khi tăng.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “java.lang.IllegalArgumentException: pool size must be positive” — [07-concurrency/ch16b-cats-effect.md:430](../07-concurrency/ch16b-cats-effect.md#L430). Đây là lỗi **FakePool của Ch16B**, không phải thông báo Hikari; dùng để nhắc validate giới hạn trước acquire. Chưa có thông báo Hikari riêng ở mục 10 để trích.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[HikariCP](https://github.com/brettwooldridge/HikariCP).

<a id="postgres-server"></a>
## PostgreSQL server

### 1. Là gì / giải quyết vấn đề gì

PostgreSQL là database quan hệ chạy ngoài JVM, chịu trách nhiệm lưu dữ liệu và thực thi constraint/transaction. Driver hay ORM không thay thế server; engine thật cần có mặt để kiểm isolation, index và SQLSTATE của bài đơn hàng.

### 2. Phiên bản trong giáo trình

**18.6**, tag Docker `postgres:18.6`; binary Maven **18.6.0** ở mục zonky bên dưới. Nguồn: [release 18.6](https://www.postgresql.org/docs/release/18.6/), ngày đối chiếu 18/10/2026. Server ngoài JVM, không có khái niệm artifact Scala 3.

### 3. Khái niệm cốt lõi

Table/constraint/index, MVCC, isolation và WAL quyết định tính đúng/bền của transaction. Docker `postgres:18.6` và binary nhúng 18.6.0 là hai cách chạy cùng họ server; không nhầm version binary bundle với version JDBC.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:21](../06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala#L21). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 06-database/db-lab/src/main/scala/vn/scalaeng/db/Database.scala:21-26 -->
```scala
object Database:
  /** Chạy mọi migration chưa chạy. Trả số migration vừa áp dụng (0 nếu schema đã mới nhất). */
  def migrate(cfg: DbConfig): IO[Int] = IO.blocking {
    Flyway.configure().dataSource(cfg.url, cfg.user, cfg.password).load().migrate().migrationsExecuted
  }

```

### 5. Lựa chọn thay thế

H2/mock phù hợp một số seam test nhưng không xác nhận semantics PostgreSQL. PostgreSQL nhúng và Docker đều chạy server thật; chọn theo quyền tạo process/daemon và mức giống production cần kiểm.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “ERROR: could not serialize access due to concurrent update” — [06-database/ch15-database-engineering.md:565](../06-database/ch15-database-engineering.md#L565). Retry toàn bộ transaction khi chính sách cho phép, không retry riêng câu lệnh cuối.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[PostgreSQL 18 docs](https://www.postgresql.org/docs/18/), [release notes 18.6](https://www.postgresql.org/docs/release/18.6/).

<a id="embedded-postgres"></a>
## zonky embedded-postgres và binary theo OS

### 1. Là gì / giải quyết vấn đề gì

zonky khởi động tiến trình PostgreSQL thật từ binary được đóng gói để integration test không cần Docker. Test dùng schema/migration thật nên bắt được lỗi constraint và transaction mà repository fake không thấy.

### 2. Phiên bản trong giáo trình

- `io.zonky.test:embedded-postgres:2.2.2` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/zonky/test/embedded-postgres/2.2.2/embedded-postgres-2.2.2.pom); HTTP 200, 18/10/2026.
- `io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-darwin-arm64v8/18.6.0/embedded-postgres-binaries-darwin-arm64v8-18.6.0.pom); HTTP 200, 18/10/2026.
- `io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-amd64/18.6.0/embedded-postgres-binaries-linux-amd64-18.6.0.pom); HTTP 200, 18/10/2026.
- `io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/18.6.0/embedded-postgres-binaries-windows-amd64-18.6.0.pom); HTTP 200, 18/10/2026.

Wrapper Java + binary native; dùng qua Scala 3, không có hậu tố Scala. Ch15 có bằng chứng lead chạy ngoài sandbox; không khẳng định T27 đã khởi PostgreSQL. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Wrapper **2.2.2** quản lifecycle; ba gói binary **18.6.0** tương ứng Windows amd64, Linux amd64, macOS arm64v8. `EmbeddedPostgres.builder().start()` cấp JDBC URL, fixture mở/migrate và đóng sau suite. Đây là binary native theo OS/CPU, không phải jar thuần Java chạy mọi máy.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala:16](../06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala#L16). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala:16-25 -->
```scala
  private val db: Resource[IO, (DbConfig, Transactor[IO])] =
    for
      pg  <- Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(pg => IO.blocking(pg.close()))
      cfg  = DbConfig(pg.getJdbcUrl("postgres", "postgres"), "postgres", "postgres", poolSize = 4)
      _   <- Resource.eval(Database.migrate(cfg))
      xa  <- Database.transactor(cfg)
    yield (cfg, xa)

  val database = ResourceSuiteLocalFixture("postgres", db)
  override def munitFixtures = List(database)
```

### 5. Lựa chọn thay thế

Docker PostgreSQL phù hợp kiểm triển khai và image production; zonky nhẹ về yêu cầu daemon nhưng vẫn cần quyền tạo subprocess. Dùng repository fake cho unit test core, không nhận fake là PostgreSQL thật.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “SQLState=25P02” — [06-database/ch15-database-engineering.md:574](../06-database/ch15-database-engineering.md#L574). Engine nhúng vẫn có transaction abort như server ngoài. Sandbox chặn subprocess thì phải ghi chưa chạy/skip theo lab, không tính là DB pass.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service); P5 dùng lại gói Windows.

### 8. Tài liệu chính thức

[embedded-postgres](https://github.com/zonkyio/embedded-postgres), [binaries](https://github.com/zonkyio/embedded-postgres-binaries).

<a id="password4j"></a>
## Password4j

### 1. Là gì / giải quyết vấn đề gì

Password4j cung cấp hàm băm mật khẩu và kiểm tra hash lưu trong DB. Khác mã hóa có thể giải ngược, băm mật khẩu có salt và chi phí tính toán để làm đoán mật khẩu tốn tài nguyên; lớp này không thay thế policy, rate limit hoặc kiểm quyền.

### 2. Phiên bản trong giáo trình

- `com.password4j:password4j:1.8.4` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/password4j/password4j/1.8.4/password4j-1.8.4.pom); HTTP 200, 18/10/2026.

Java; dùng từ Scala 3.9.0/JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Lab dùng `Argon2Function`, `Password.hash`, salt ngẫu nhiên và `Password.check`. Hash tự mô tả tham số, giúp kiểm hash cũ khi đổi cấu hình; chạy trên blocking pool vì có chủ ý tốn CPU/RAM. Các tham số trích là lựa chọn lab đã test, không phải khuyến nghị bảo mật mới được T27 đánh giá lại.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Passwords.scala:11](../05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Passwords.scala#L11). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Passwords.scala:11-19 -->
```scala
object Passwords:
  private val argon2id = Argon2Function.getInstance(19456, 2, 1, 32, Argon2.ID)

  /** Chậm CÓ CHỦ Ý (~vài chục ms) và tốn RAM để đoán thử hàng loạt rất đắt → chạy trên blocking pool (Chương 16B). */
  def hash(plain: String): IO[String] =
    IO.blocking(Password.hash(plain).addRandomSalt(16).`with`(argon2id).getResult)

  def verify(plain: String, stored: String): IO[Boolean] =
    IO.blocking(Password.check(plain, stored).`with`(Argon2Function.getInstanceFromHash(stored)))
```

### 5. Lựa chọn thay thế

Có thể dùng thư viện Argon2 JVM khác nếu đã kiểm thuật toán/format/migration; BCrypt phù hợp hệ cũ có hash sẵn nhưng cần kế hoạch đổi hash. Không thay bằng hash nhanh tổng quát cho mật khẩu.

### 6. Lỗi/bẫy thường gặp

Chưa có thông báo compiler/runtime riêng của Password4j được ghi trong [Ch17](../05-backend/ch17-security-production.md), mục 10; không gán lỗi JWT của cùng lab thành lỗi băm mật khẩu. Đối chiếu [CryptoSuite](../05-backend/security-lab/src/test/scala/vn/scalaeng/sec/CryptoSuite.scala) để xem các ca hash/verify thật. Băm đúng là một lớp riêng với policy và rate limit; chưa có lỗi thư viện được tái hiện thì không đặt thêm thông báo chẩn đoán.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[Password4j](https://github.com/Password4j/password4j), [wiki](https://github.com/Password4j/password4j/wiki).

<a id="jwt"></a>
## jwt-circe

### 1. Là gì / giải quyết vấn đề gì

jwt-circe tích hợp JWT với JSON Circe để phát hành và xác minh token. Chữ ký bảo vệ tính nguyên vẹn payload; bearer token vẫn cần kiểm claim, quyền sở hữu và cơ chế thu hồi theo chính sách app.

### 2. Phiên bản trong giáo trình

- `com.github.jwt-scala:jwt-circe_3:11.0.4` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/github/jwt-scala/jwt-circe_3/11.0.4/jwt-circe_3-11.0.4.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`JwtCirce`, `JwtClaim`, `JwtAlgorithm.HS256`, issuer/subject/exp/jti là các thành phần lab dùng. Thư viện kiểm claim tùy có mặt/cấu hình; ứng dụng đòi exp/jti và cố định thuật toán. JWT ký không mã hóa dữ liệu trong payload.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala:39](../05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala#L39). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala:39-47 -->
```scala
  def issue(user: AuthUser, tokenId: String = UUID.randomUUID().toString): String =
    val claim = JwtClaim(content = Claims(user.email, user.role).asJson.noSpaces)
      .by(config.jwtIssuer)
      .about(user.id.toString)
      .withId(tokenId)
      .issuedNow
      .expiresIn(config.tokenTtl.toSeconds)
    jwt.encode(claim, config.jwtSecret.reveal, Algorithm)

```

### 5. Lựa chọn thay thế

Session ID phía server thuận tiện thu hồi tập trung; JWT thuận trao đổi token giữa service nhưng trạng thái thu hồi vẫn phải thiết kế. Chọn theo trust boundary và lifecycle thay vì xem JWT là mặc định cho mọi API.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Token không có `exp` vẫn được chấp nhận.” — [05-backend/ch17-security-production.md:512](../05-backend/ch17-security-production.md#L512). Lab sửa verifyClaims để bắt buộc claim; decode thành công không đủ xác thực chính sách.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[jwt-scala](https://jwt-scala.github.io/jwt-scala/), [GitHub](https://github.com/jwt-scala/jwt-scala).

<a id="logging"></a>
## log4cats-slf4j và Logback

### 1. Là gì / giải quyết vấn đề gì

log4cats biểu diễn lời gọi log trong effect để ghép với IO; adapter SLF4J chuyển sang logging facade JVM. Logback là backend xuất log và áp cấu hình; chỉ thêm facade không đảm bảo log được ghi ở production.

### 2. Phiên bản trong giáo trình

- `ch.qos.logback:logback-classic:1.6.5` — [POM Maven Central](https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/1.6.5/logback-classic-1.6.5.pom); HTTP 200, 18/10/2026.
- `org.typelevel:log4cats-slf4j_3:2.8.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/log4cats-slf4j_3/2.8.0/log4cats-slf4j_3-2.8.0.pom); HTTP 200, 18/10/2026.

log4cats-slf4j có `_3`; Logback là Java. Version 2.8.0 và 1.6.5 thuộc hai dự án khác nhau. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`Slf4jLogger`, logger name, level và `logback.xml` quyết định nội dung/đích log. Runtime exception cần được log ở server kèm request id rồi trả lỗi đã che cho client. Log là side effect; phải ghép logger.error vào IO, tránh log token/password.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:34](../05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala#L34). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:34-45 -->
```scala
  def errorHandler(app: HttpApp[IO]): HttpApp[IO] = Kleisli { (req: Request[IO]) =>
    app(req).handleErrorWith { e =>
      val requestId = req.headers.get(CIString("X-Request-ID")).map(_.head.value).getOrElse("-")
      logger.error(e)(s"lỗi không mong đợi request_id=$requestId ${req.method} ${req.uri.path}") *>
        InternalServerError(Json.obj(
          "status"    -> Json.fromInt(500),
          "title"     -> Json.fromString("Internal Server Error"),
          "detail"    -> Json.fromString("đã có lỗi xảy ra; liên hệ hỗ trợ kèm mã request"),
          "requestId" -> Json.fromString(requestId)
        ))
    }
  }
```

### 5. Lựa chọn thay thế

`println` tiện demo nhưng thiếu level/cấu hình; backend SLF4J khác phù hợp nếu nền tảng đã chuẩn hóa. Chọn một backend nhất quán; `slf4j-nop` ở phase 5 cố ý bỏ log, không phải cấu hình vận hành backend.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “SLF4J: Defaulting to no-operation (NOP) logger implementation” — [05-backend/ch13-choosing-a-framework.md:514](../05-backend/ch13-choosing-a-framework.md#L514). Đây là cảnh báo runtime thật của stack cũ trong lab; không phải compiler warning. Kiểm version facade/backend trước khi thêm binding.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service)

### 8. Tài liệu chính thức

[log4cats](https://typelevel.org/log4cats/), [Logback](https://logback.qos.ch/manual/), [SLF4J](https://www.slf4j.org/manual.html).

<a id="metrics"></a>
## http4s Prometheus metrics

### 1. Là gì / giải quyết vấn đề gì

Module metrics của http4s đo request/response rồi xuất số liệu theo định dạng Prometheus. Nó giúp quan sát latency/lỗi/tải độc lập với log từng request; đếm chỉ có ý nghĩa khi ranh giới và nhãn đo phản ánh hành vi thật.

### 2. Phiên bản trong giáo trình

- `org.http4s:http4s-prometheus-metrics_3:0.25.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/http4s/http4s-prometheus-metrics_3/0.25.0/http4s-prometheus-metrics_3-0.25.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`PrometheusExportService`, `metricsOps`, middleware `Metrics` và registry được quản lý trong Resource. HTTP metrics khác counter nghiệp vụ login thất bại; thư viện Prometheus Java trong import là dependency bắc cầu, không có khai báo trực tiếp riêng trong build.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:76](../05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala#L76). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:76-86 -->
```scala
      metricsSvc <- PrometheusExportService.build[IO]
      ops        <- Prometheus.metricsOps[IO](metricsSvc.collectorRegistry, "http")
      // Metric NGHIỆP VỤ, bổ sung cho metric HTTP chung. Chỉ endpoint login gọi `inc()` (mục 10.6).
      failures    = Counter.build("login_failures_total", "Số lần đăng nhập thất bại").register(metricsSvc.collectorRegistry)
      users      <- Resource.eval(UserStore.empty)
      tokens      = Tokens(config, clock)
      auth       <- Resource.eval(AuthService.make(users, tokens))
      dbUp       <- Resource.eval(Ref.of[IO, Boolean](true))
      limiter    <- Resource.eval(RateLimiter.make(config.loginAttemptsPerMinute, 1.minute))
      api         = Api(ApiDeps(users, auth, tokens, config, limiter, IO(failures.inc()), isRevoked))
      // Readiness ở lab là một CỜ mô phỏng database (`dbUp`). Project 2 thay bằng truy vấn `select 1` thật có timeout.
```

### 5. Lựa chọn thay thế

Counter tự viết phù hợp demo nhỏ nhưng phải tự xử lý thread safety và format. OpenTelemetry thuận cho chuẩn quan sát nhiều tín hiệu; cần pipeline/exporter riêng. Chọn module này cho lab http4s và kiểm `/metrics` thật.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “expected json value got '# HELP...' (line 1, column 1)” — [05-backend/ch17-security-production.md:470](../05-backend/ch17-security-production.md#L470). Đang dùng JSON decoder để đọc body metrics văn bản; chọn bodyText thay vì as[String] khi import codec JSON tổng quát.

### 7. Dùng ở đâu trong giáo trình

[05-backend/ch17-security-production.md](../05-backend/ch17-security-production.md)

### 8. Tài liệu chính thức

[http4s Prometheus metrics](https://github.com/http4s/http4s-prometheus-metrics), [Prometheus instrumentation](https://prometheus.io/docs/practices/instrumentation/).

