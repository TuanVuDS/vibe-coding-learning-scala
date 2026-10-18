# Lời giải — Chương 13: Chọn Scala Backend Framework

> Code: [`ch13-code/`](ch13-code/) — dùng lại mã nguồn `05-backend/framework-lab/typelevel/src/main` qua directive `//> using file`.
> Chạy: `cd solutions/ch13-code && scala-cli test .` → **6/6 test pass** (munit 1.2.4 + munit-cats-effect 2.2.0, thư viện thật; chạy lại 17/10/2026 sau review chéo).

## Bài 1 — Chấm lại cho team Java Spring, admin API nội bộ

Không có đáp án duy nhất; đây là một lời giải mẫu có lập luận.

| Tiêu chí | Nhận xét cho bối cảnh này | Hệ quả |
|---|---|---|
| Thân thiện người mới | Team quen MVC + DI (Spring). Play có controller, router, DI (Guice) gần với kinh nghiệm cũ | Play +1 |
| Độ phức tạp hệ sinh thái | Không làm data pipeline → lợi thế "một runtime cho BE + DE" của Cats Effect không còn | Typelevel mất lợi thế chính |
| Tài liệu | Play có tài liệu lớn, nhưng phải lọc ví dụ Scala 2/Akka | Trung tính |
| OpenAPI | Admin API nội bộ, 1 client → OpenAPI ít quan trọng | Tapir mất lợi thế |
| Production | Cả hai đều đủ | Trung tính |
| Tuyển người / bảo trì | Người Java dễ đọc Play hơn code `IO`/`OptionT` | Play +1 |

**Kết luận mẫu:** Play 3.0 (hoặc http4s thuần nếu team muốn học functional dần). Điều kiện xem xét lại: khi hệ thống bắt đầu có
consumer Kafka/stream bằng Scala → cân nhắc chuẩn hoá về Cats Effect để dùng một runtime.

Bài học: **bảng tiêu chí phụ thuộc bối cảnh**; lựa chọn của giáo trình (Tapir + http4s) tối ưu cho *người học hai nhánh BE + DE*.

## Bài 2 — `GET /api/v1/products?q=...`

```scala
object ListProducts:
  private object Q extends OptionalQueryParamDecoderMatcher[String]("q")

  def http4s(store: ProductStore): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "api" / "v1" / "products" :? Q(q) => store.list(q).flatMap(Ok(_))
  }

  val endpointDef: PublicEndpoint[Option[String], Unit, List[Product], Any] =
    endpoint.get
      .in("api" / "v1" / "products")
      .in(query[Option[String]]("q").description("lọc theo tên, không phân biệt hoa thường"))
      .out(jsonBody[List[Product]])

  def tapir(store: ProductStore): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(endpointDef.serverLogicSuccess(store.list))
```

Giải thích:
- http4s: `:?` tách query string; `OptionalQueryParamDecoderMatcher` cho `Option[String]` (không có `q` → `None`).
- Tapir: `query[Option[String]]("q")` — kiểu `Option` nghĩa là tham số không bắt buộc (thiếu `q` → `None`).
- `serverLogicSuccess` dùng khi endpoint không có lỗi nghiệp vụ (`Unit` error).
- Logic lọc ở `ProductStore.list` — hàm thuần trên `State`; hai lớp HTTP chỉ chuyển đổi.

Test chạy **cùng kịch bản cho cả hai** (vòng `foreach` trong `Ch13SolutionsSuite`); có tên chứa chữ hoa "PHÍM" để kiểm tra
không phân biệt hoa thường. Lỗi thật gặp khi viết test: `uri"...?q=phím"` không biên dịch (Chương 13, mục 10.4).

## Bài 3 — id không phải số → 400 mà không sửa `Http4sApi.scala`

```scala
object BadIdAs400:
  private val fallback: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "api" / "v1" / "products" / raw =>
    BadRequest(Problem(400, "Bad Request", s"id phải là số nguyên, nhận được '$raw'"))
  }
  def routes(store: ProductStore): HttpRoutes[IO] = Http4sApi.routes(store) <+> fallback
```

- `HttpRoutes` là `OptionT`: route chính không khớp (`LongVar` thất bại) → `<+>` thử `fallback`.
- **Thứ tự quan trọng**: nếu viết `fallback <+> Http4sApi.routes(store)`, mọi `GET /products/{x}` kể cả `x = 2` đều trả 400.
  Test "id số vẫn đi route chính" chặn lỗi này.
- Đây là ví dụ *mở rộng mà không sửa* (open/closed) nhờ route là giá trị ghép được.

## Bài 4 — Tapir trả Problem JSON khi decode thất bại

```scala
private val decodeFailures: DefaultDecodeFailureHandler[IO] =
  DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
    ValuedEndpointOutput(statusCode.and(headers).and(jsonBody[Problem]), (code, hdrs, Problem(code.code, "Bad Request", msg)))
  )

private val options: Http4sServerOptions[IO] =
  Http4sServerOptions.customiseInterceptors[IO].decodeFailureHandler(decodeFailures).options
```

- Chỉ thay cách hiển thị của **decode failure**; hàm `response` nhận **status và header** mà Tapir đã chọn, nên `status` trong body luôn khớp HTTP.
- `msg` vẫn là thông điệp của Tapir (`Invalid value for: body (exhausted input)`).
- Test kiểm tra: (a) JSON hỏng → 400, `Content-Type: application/json`, `detail` bắt đầu bằng `Invalid value for: body`; (b) `PUT` → **405** và body
  không ghi `"status":400`.

**Bản đầu của lời giải sai** (review chéo của Codex, M1): dùng `defaultHandlers(msg => Problem(400, ...))`. `defaultHandlers` thay renderer của
**cả** decode failure, reject (405...) và exception, nhưng hàm chỉ nhận thông điệp, nên mọi lỗi đều bị ghi `status: 400`. Probe thật:

```
PROBE reject: HTTP=405; BODY={"status":400,"title":"Bad Request","detail":"Method Not Allowed","errors":[]}
```

Test cũ chỉ gửi JSON hỏng nên không phát hiện. Chương 17 (mục 10.2) gặp lại đúng lỗi này với 401.
- Cân nhắc bảo mật: thông điệp decode của Tapir chỉ nói *phần nào* sai, không lộ stack trace — chấp nhận được cho API công khai.
  Chương 14 sẽ dùng cách này cho toàn bộ API.

## Bài 5 — Integration test qua socket thật

```scala
private val serverAndClient: Resource[IO, (Uri, Client[IO])] =
  for
    store  <- Resource.eval(ProductStore.empty)
    server <- EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0")
                .withHttpApp(TapirApi.routes(store).orNotFound).build
    client <- EmberClientBuilder.default[IO].build
  yield (Uri.unsafeFromString(s"http://127.0.0.1:${server.address.getPort}"), client)
```

- **Cổng 0**: OS cấp một cổng trống; đọc lại cổng thật qua `server.address.getPort`. Hard-code 8082 sẽ fail khi
  (a) bạn đang chạy `TapirMain` cùng lúc, (b) CI chạy song song nhiều job, (c) cổng bị chương trình khác chiếm.
- `Resource` ghép server + client: kết thúc `use` → client đóng trước, server đóng sau (ngược thứ tự tạo) — không rò socket giữa các test.
- Test này chậm hơn contract test (~2 giây lần đầu vì khởi động Ember); vì vậy contract test (không mạng) là lớp chính,
  integration test chỉ vài kịch bản "đường ống có thông không".

## Bài 6 — Tra phiên bản (16/10/2026)

| Câu hỏi | Kết quả | Nguồn |
|---|---|---|
| Doobie 1.0.0 final? | **Chưa.** Bản mới nhất `1.0.0-RC13` dưới groupId `org.typelevel`; groupId cũ `org.tpolecat` dừng ở `1.0.0-RC12` | `https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/maven-metadata.xml`, `.../org/tpolecat/doobie-core_3/maven-metadata.xml` |
| fs2-kafka 4.0 GA? | **Rồi.** Series 4.x dưới `org.typelevel` đã tới `4.1.3`; series 3.x dưới `com.github.fd4s` tới `3.9.1` | `https://repo1.maven.org/maven2/org/typelevel/fs2-kafka_3/maven-metadata.xml`, `.../com/github/fd4s/fs2-kafka_3/maven-metadata.xml` |

Hệ quả: compatibility matrix (ghi fs2-kafka `4.0.0-RC1`) **đã lỗi thời** — được cập nhật cùng chương này.
Đây chính là lý do giáo trình yêu cầu kiểm chứng lại phiên bản trước mỗi giai đoạn.
