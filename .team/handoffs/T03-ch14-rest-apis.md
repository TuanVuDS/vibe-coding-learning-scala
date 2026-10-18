# Handoff T03 — Chương 14: Building REST APIs

Tác giả: **Codex** · Reviewer: **Claude Code** · Ngày kiểm chứng: **17/10/2026 (UTC+7)**.

**Cập nhật:** mục 1–7 bên dưới giữ lịch sử bàn giao vòng 1. Kết quả review và bản sửa hiện tại được ghi tại **mục 8 — Vòng 2**; các ghi chú “chưa review/chưa chạy sbt” của vòng 1 đã được cập nhật tại đó.

Đã hoàn thành phần tác giả của brief: bài đủ 17 mục, service user/order chạy thật, sbt hai module, Scala CLI kiểm chứng với `--server=false`, sáu bài tập có code/test, nguồn dependency và output thật. **Chưa đánh dấu hoàn tất review chéo; sbt phải do lead chạy theo ngoại lệ trong brief.** Không sửa file dùng chung, không commit.

## 1. File đã tạo

Tất cả là file mới trong phạm vi T03:

- `05-backend/ch14-rest-apis.md` — 17 mục, giải thích đầy đủ các lựa chọn, liên kết Ch13/15/16B/17, transcript thật.
- `05-backend/order-service/.gitignore`, `build.sbt`, `project.scala`, `project/build.properties`.
- `05-backend/order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala`.
- `05-backend/order-service/domain/src/test/scala/vn/scalaeng/orders/DomainSuite.scala`.
- `05-backend/order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala`, `OrderService.scala`, `Transport.scala`, `Api.scala`, `Main.scala` (gồm PrintOpenApi).
- `05-backend/order-service/app/src/test/scala/vn/scalaeng/orders/Fixtures.scala`, `RepositorySuite.scala`, `ContractSuite.scala`, `OpenApiSuite.scala`, `SocketSuite.scala`.
- `05-backend/order-service/requests/user.json`, `invalid-user.json`, `order.json`, `paid.json`, `cancelled.json`, `broken.json` (cố ý JSON hỏng cho test/curl).
- `05-backend/order-service/evidence/versions.json`, `tests.txt`, `compile-error.txt`, `curl-transcript.txt`, `openapi.yaml`, `capture_http.py`.
- `solutions/ch14-solutions.md`.
- `solutions/ch14-code/.gitignore`, `project.scala`, `test-output.txt`.
- `solutions/ch14-code/src/main/scala/vn/scalaeng/orders/solutions/Extensions.scala`, `DecodeExample.scala`.
- `solutions/ch14-code/src/test/scala/vn/scalaeng/orders/solutions/SolutionsSuite.scala`.
- `.team/handoffs/T03-ch14-rest-apis.md` — file này.

`.cache/`, `.scala-build/`, `.bsp/` là cache/build sinh tự động đã ignore; không bàn giao artifact nhị phân. Cache Coursier dùng `05-backend/order-service/.cache/coursier` cho cả lab và lời giải. Có các thay đổi đồng thời ngoài phạm vi T03 của tác giả khác; Codex không sửa hoặc hoàn tác chúng.

## 2. Kiểm chứng thật

### Lab

Tại `05-backend/order-service`:

```powershell
$env:COURSIER_CACHE = Join-Path (Get-Location) '.cache/coursier'
scala-cli.bat compile . --server=false --scalac-option -Werror
scala-cli.bat test . --server=false --scalac-option -Werror
```

Compile exit 0. Lần test cuối trên source bàn giao: **25 pass, 0 fail, 0 ignored**, exit 0, **0 compiler warning** với `-deprecation -feature -Wunused:all -Wvalue-discard -Werror`. Trích nguyên văn từ [tests.txt](../../05-backend/order-service/evidence/tests.txt):

```text
Test run vn.scalaeng.orders.ContractSuite finished: 0 failed, 0 ignored, 11 total 1.414s
Test run vn.scalaeng.orders.DomainSuite finished: 0 failed, 0 ignored, 5 total 0.009s
Test run vn.scalaeng.orders.OpenApiSuite finished: 0 failed, 0 ignored, 2 total 0.006s
Test run vn.scalaeng.orders.RepositorySuite finished: 0 failed, 0 ignored, 6 total 0.051s
Test run vn.scalaeng.orders.SocketSuite finished: 0 failed, 0 ignored, 1 total 0.718s
```

- Contract test gọi đúng `HttpApp` Main dùng, không mở socket; đủ bảy endpoint, lỗi decode/body/query/path, media type, enum, validation tích lũy, Location, Problem body/status, pagination, docs, 405/Allow, fallback 404.
- Repository test gồm 20 tạo user đồng thời cùng email (1 Right/19 DuplicateEmail), Paid/Cancelled cạnh tranh (1 Right/1 Left), retry cùng status, filter/sort/offset/limit/total và user thiếu.
- SocketSuite chạy Ember server **cổng 0** + Ember client thật, tạo user → tạo order → Paid → GET → decode lỗi query; Resource đóng server/client. Không cần dịch vụ ngoài/Docker.

Kiểm tra domain độc lập (không dependency HTTP/JSON/effect), exit 0, không output/cảnh báo:

```powershell
scala-cli.bat compile domain/src/main/scala --server=false --scala 3.9.0 --jvm 21 --scalac-option -deprecation --scalac-option -feature --scalac-option -Wunused:all --scalac-option -Wvalue-discard --scalac-option -Werror
```

Lệnh này xác nhận source domain tự biên dịch; **không thay thế** việc sbt kiểm tra dependency graph đa module.

### Lời giải

Tại `solutions/ch14-code`:

```powershell
$env:COURSIER_CACHE = (Resolve-Path ../../05-backend/order-service).Path + '/.cache/coursier'
scala-cli.bat test . --server=false --scalac-option -Werror
```

**6 pass, 0 fail, 0 ignored**, exit 0, 0 compiler warning. Trích nguyên văn từ [test-output.txt](../../solutions/ch14-code/test-output.txt):

```text
Test run vn.scalaeng.orders.solutions.SolutionsSuite finished: 0 failed, 0 ignored, 6 total 1.233s
```

Sáu bài: tính tổng chống tràn số; filter/status + sort total trước page; cursor ID; version check/update nguyên tử; failed IO → 500 không lộ bí mật; decode thiếu bearer → HTTP/body 401 và giữ WWW-Authenticate. Bài version chỉ kiểm chứng primitive lưu trữ; mapping If-Match/ETag được giải thích, **không tuyên bố đã có endpoint ETag**.

### Server và curl

```powershell
scala-cli.bat run . --server=false --main-class vn.scalaeng.orders.Main
# Terminal khác, cùng thư mục order-service, Main vừa khởi động với RAM rỗng:
python evidence/capture_http.py
```

Server bind `127.0.0.1:8084`. **22 trao đổi curl.exe -i thật** được capture; script kiểm tra tất cả HTTP status và body.status trên nhánh lỗi. Trích output script:

```text
create user: 201 OK
get user: 200 OK
duplicate email: 409 OK
validation: 422 OK
bad JSON: 400 OK
wrong media type: 415 OK
JSON with UTF-8 charset: 409 OK
bad query type: 400 OK
bad path type: 400 OK
query validation: 422 OK
list users: 200 OK
missing user: 404 OK
create order: 201 OK
missing order owner: 404 OK
get order: 200 OK
pay: 200 OK
retry pay: 200 OK
transition conflict: 409 OK
list orders: 200 OK
method mismatch: 405 OK
unknown route: 404 OK
OpenAPI: 200 OK
22 HTTP exchanges checked
```

Stdout curl đầy đủ ở [curl-transcript.txt](../../05-backend/order-service/evidence/curl-transcript.txt); body request UTF-8 gửi bằng `--data-binary @file`. Lệnh trong capture script chạy trực tiếp bằng argv, không qua shell; khi copy URL có `&` sang PowerShell phải đặt trong nháy như bài học. YAML lấy trực tiếp từ `/docs/openapi.yaml`, lưu [openapi.yaml](../../05-backend/order-service/evidence/openapi.yaml): OpenAPI 3.1.0, 5 path/7 operationId, Problem media type và enum chuỗi. Dừng Main sau kiểm chứng, không để server nền.

### Rà soát tài liệu

Script read-only đã xác nhận: đủ 17 mục đánh số; mọi link file cục bộ ở bài/lời giải tồn tại; không có file tác giả rỗng; 10 dòng output JSON trích trong bài khớp nguyên văn transcript; 5 dòng tóm tắt test khớp log. Không dùng output giả hoặc bản mô phỏng munit.

## 3. Phiên bản và nguồn đã kiểm chứng

Ngày **17/10/2026**: đọc `maven-metadata.xml` bằng Python urllib trực tiếp từ repo1.maven.org, tìm version đã pin; **11/11 present=true**, đầy đủ URL trong [versions.json](../../05-backend/order-service/evidence/versions.json). PowerShell Invoke-WebRequest/curl TLS đến Maven bị lỗi credentials ở sandbox; Python urllib và Coursier tải thành công. Không dùng search.maven.org.

| Thư viện/artifact Scala 3 | Bản pin | Nguồn |
|---|---|---|
| http4s-ember-server | 0.23.38 | [metadata](https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/maven-metadata.xml) |
| http4s-ember-client (test) | 0.23.38 | [metadata](https://repo1.maven.org/maven2/org/http4s/http4s-ember-client_3/maven-metadata.xml) |
| http4s-dsl | 0.23.38 | [metadata](https://repo1.maven.org/maven2/org/http4s/http4s-dsl_3/maven-metadata.xml) |
| tapir-http4s-server | 1.13.33 | [metadata](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-http4s-server_3/maven-metadata.xml) |
| tapir-json-circe | 1.13.33 | [metadata](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-json-circe_3/maven-metadata.xml) |
| tapir-openapi-docs | 1.13.33 | [metadata](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-openapi-docs_3/maven-metadata.xml) |
| openapi-circe-yaml | 0.11.10 | [metadata](https://repo1.maven.org/maven2/com/softwaremill/sttp/apispec/openapi-circe-yaml_3/maven-metadata.xml) |
| circe-core | 0.14.17 | [metadata](https://repo1.maven.org/maven2/io/circe/circe-core_3/maven-metadata.xml) |
| cats-effect | 3.7.1 | [metadata](https://repo1.maven.org/maven2/org/typelevel/cats-effect_3/maven-metadata.xml) |
| munit | 1.2.4 | [metadata](https://repo1.maven.org/maven2/org/scalameta/munit_3/maven-metadata.xml) |
| munit-cats-effect | 2.2.0 | [metadata](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/maven-metadata.xml) |

Scala 3.9.0/JDK 21 được dùng thực tế qua directives; sbt pin 1.12.11 trong build.properties theo brief. circe-core pin 0.14.17; các module circe transitive có thể mang bản khác từ Tapir/apispec, không tuyên bố toàn bộ graph circe đều 0.14.17. Không thêm Swagger UI artifact. Các dependency đều cùng stack Ch13, thêm pin Cats Effect 3.7.1 như Ch16B và Ember client cho socket test.

Đã đọc tài liệu chính thức Tapir [error handling](https://tapir.softwaremill.com/en/latest/server/errors.html), [OpenAPI](https://tapir.softwaremill.com/en/latest/docs/openapi.html), [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html), cùng source tag v1.13.33 của CodecFormat/Tapir/DecodeFailureHandler/RejectHandler/ExceptionHandler. Nguồn đã dẫn trong bài.

## 4. Trait repository bàn giao cho Ch15/P2

Package **`vn.scalaeng.orders`**, file `app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala`. Domain model ở module domain; trait/cài đặt IO ở app để domain không kéo Cats Effect.

```scala
trait OrderRepository:
  def createUser(draft: UserDraft): IO[Either[DomainError, User]]
  def getUser(id: UserId): IO[Option[User]]
  def listUsers(query: UserQuery): IO[Page[User]]
  def createOrder(userId: UserId, total: Money): IO[Either[DomainError, Order]]
  def getOrder(id: OrderId): IO[Option[Order]]
  def listOrders(userId: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]]
  def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]]
```

Kiểu liên quan:

- `User(id: UserId, name: String, email: String)`, `UserDraft(name,email)` đã trim, email lowercase Locale.ROOT theo policy service.
- `Order(id: OrderId, userId: UserId, total: Money, status: OrderStatus)`.
- `UserId`/`OrderId` opaque Long dương; public `from`, extension `.value`; factory assigned giới hạn package cho repository.
- `Money` opaque Long dương, **VND**, `.minor`; DTO thêm `currency: "VND"`. Chưa có order lines/catalog/tồn kho; số tiền gửi trực tiếp chỉ phục vụ lab caller đáng tin, chưa phải checkout production.
- `OrderStatus`: Pending/Paid/Shipped/Cancelled. Cho phép Pending→Paid, Pending→Cancelled, Paid→Shipped, hoặc cùng trạng thái (retry). Cạnh còn lại InvalidTransition.
- `DomainError`: Invalid(List[String]), UnknownUser(UserId), UnknownOrder(OrderId), DuplicateEmail(String), InvalidTransition(from,to).
- `UserQuery(q: Option[String], sort: UserSort, descending: Boolean, page: PageRequest)`; UserSort Id/Name/Email; q đã trim/lowercase, empty thành None. Tìm substring name/email, không bỏ dấu.
- `PageRequest` offset 0..1_000_000, limit 1..100 (factory from); `Page[A](items,total,offset,limit)`. Total sau filter, trước page; sort name/email có ID phụ, cả tuple đảo khi descending. Orders sort ID tăng.

**Hợp đồng nguyên tử cần giữ khi thay Ref bằng DB:**

1. createUser kiểm tra email unique và ghi trong một thao tác; DB dùng constraint, không chỉ SELECT trước INSERT.
2. createOrder kiểm tra user tồn tại và insert nguyên tử; DB dùng FK/transaction.
3. updateStatus đọc trạng thái hiện tại, kiểm tra cạnh, cập nhật nguyên tử; DB dùng row lock hoặc conditional UPDATE. Không service.getOrder rồi repository.setStatus vô điều kiện.
4. total và items của một page thuộc cùng snapshot. Adapter DB muốn giữ đúng hợp đồng phải dùng một câu query phù hợp hoặc transaction/isolation bảo đảm hai query count/select nhìn cùng snapshot; hai SELECT ở READ COMMITTED không tự bảo đảm điều đó.
5. Lỗi nghiệp vụ dự kiến là Left; lỗi hạ tầng vẫn failed IO, được biên HTTP chuyển thành 500 đã che chi tiết.

Đã đọc lưu ý trong handoff T04: repository Ch15 trả **ConnectionIO**, service quyết định `.transact`. Để cài trait này, giữ chương trình ConnectionIO **bên trong adapter** rồi transact tại từng method IO. Với use case nhiều bước cần rollback chung, thêm method nguyên tử cấp use case hoặc transaction algebra; không ghép các method IO đã transact riêng và gọi đó là một transaction. Ch15 dùng package/model `vn.scalaeng.db`, có createdAt/product/order lines và mã trạng thái DB uppercase; cần mapping tường minh ID/Money/enum, **không phải adapter drop-in đã có sẵn**. Không sửa Ch15 trong task này.

## 5. Những phát hiện khi chạy thật

- `jsonBody` đơn lẻ không bắt buộc Content-Type application/json ở Tapir 1.13.33: text/plain với JSON hợp lệ vẫn nhận 201. Đổi đầu vào ba body types sang `oneOfBody(jsonBody[...])`; test 415 và curl application/json có charset UTF-8 đều pass.
- Problem media type phải thay codec format để OpenAPI và runtime cùng application/problem+json; không chỉ đặt header ở ngoài.
- Decode failure dùng `DefaultDecodeFailureHandler.copy[IO](response = ...)` theo Ch17, giữ code/hdrs. Lời giải có test 401/raw WWW-Authenticate.
- RejectHandler thu các FixedMethod mismatch sau khi path đã khớp, trả 405 + Allow, ví dụ `/users` là GET, POST. Fallback http4s cũng trả Problem 404.
- MediaType.toString là chuỗi debug; thử renderString nhận compiler error thật, lưu compile-error.txt. Bản cuối so mainType/subType.
- Có SLF4J runtime NOP binding warnings và Scala CLI hints nâng dependency; **không có compiler warning**. Giữ munit-cats-effect 2.2.0 theo chuẩn. Logging production để nối Ch17, không trả exception message cho client.

## 6. Đề xuất cập nhật file dùng chung — lead áp dụng

- **PROGRESS.md:** ghi T03 phần tác giả có bài 17 mục + sbt domain/app + 25 test lab/6 test lời giải pass bằng Scala CLI với -Werror, 22 trao đổi curl thật, OpenAPI được phục vụ. **Chờ Claude review chéo + sbt test**, chưa đánh dấu ✅.
- **Glossary:** DTO, repository port, decode failure, keyset pagination/cursor, tie-breaker, optimistic concurrency, conditional update/If-Match, snapshot; Problem Details liên hệ thuật ngữ đã có ở Ch12.
- **Compatibility matrix:** cùng stack Ch13; bổ sung evidence tổ hợp Scala 3.9.0/JDK21 + http4s 0.23.38 + Tapir 1.13.33 + apispec 0.11.10 + circe-core 0.14.17 + Cats Effect 3.7.1; munit 1.2.4/munit-cats-effect 2.2.0; module domain thuần không dependency runtime; sbt build cần lead xác nhận.
- Có thể cập nhật liên kết bài tập ở `exercises/README.md` sau review; Codex không sửa các file này.

## 7. Chưa kiểm chứng / phần lead còn thực hiện

- **sbt test và app/run:** chưa chạy trong phiên Codex theo brief/AGENTS mục 5 (launcher sandbox đã biết lỗi BootServerSocket ở T01). Viết build.sbt đa module đầy đủ; Claude cần chạy `sbt.bat test` tại order-service bằng JDK21/sbt1.12.11, kiểm tra không warning và app/run. Không ghi “sbt pass” dựa trên Scala CLI.
- **Review chéo:** chưa có review của Claude cho T03; không sửa PROGRESS/checklist hoàn tất chung.
- **Adapter PostgreSQL/transaction nhiều bước:** chưa cài hoặc chạy ở task này; hướng nối Ch15 đã nêu, không cần Docker/DB cho 31 test hiện tại.
- **Authentication, authorization, logger backend, request size policy, vận hành nhiều instance:** ngoài lab HTTP của Ch14; nối Ch17/production project khi cần. Main chỉ bind loopback.
- **ETag endpoint và pagination DB/cursor trên dữ liệu mutable:** bài tập/mini project mô tả giới hạn; lời giải chỉ có các primitive cụ thể đã test, không tuyên bố đã có endpoint production tương ứng.
- Chưa đo load/performance hay kiểm tra platform khác; số test tranh chấp hữu hạn không chứng minh mọi interleaving.

## 8. Vòng 2 — sửa toàn bộ MAJOR/MINOR theo review Claude

Ngày sửa và chạy lại: **17/10/2026 (UTC+7)**. Nguồn yêu cầu: [review T03](../reviews/T03-ch14-rest-apis-review.md). Không có BLOCKER; đã xử lý **2 MAJOR và 3 MINOR**. Chỉ ghi trong phạm vi brief T03, gồm handoff này. Không sửa file dùng chung, T06/formats-lab hoặc Ch13/15/17/19; không commit.

### 8.1. Đối chiếu từng mục review

1. **MAJOR 1 — bổ sung code trong bài:** hiện có **25 khối Scala**, gồm đoạn cũ ở mục 5. Mỗi khối có link source và dòng bắt đầu; nội dung giữ nguyên source, chỉ bỏ thụt lề chung khi trình bày. Các nhóm yêu cầu đều đã có:
   - **3.1:** path dùng chung, helper `errors` tạo `oneOf`, endpoint `createUser` với `oneOfBody`, 201/Location; `result` và `serverLogic`.
   - **3.2:** toàn bộ `Validation.user` tích lũy lỗi, khai báo query offset/limit, endpoint list ghép query; giải thích description không thực thi validation.
   - **3.3:** opaque UserId/Money và factory; Encoder/Decoder chuỗi cùng Schema enum ở `TransportCodecs`.
   - **3.4:** toàn bộ trait OrderRepository và helper `OrderService.run`; giải thích nhánh Left không gọi repository.
   - **3.5:** phiên bản sai get/set đặt cạnh `updateStatus` dùng một modify. Phiên bản sai nằm riêng trong **RefRaceExampleSuite**, không nối vào server. CountDownLatch buộc cả hai request đọc Pending trước khi ghi; test khẳng định hai Right để chứng minh vi phạm hợp đồng, trong khi RepositorySuite hiện có khẳng định chỉ một Right với repository đúng.
   - **3.6:** problemBody/codec format, decodeFailures, exceptions, reject handler 405/Allow và cấu hình gắn handler vào interpreter.
   - **3.7:** UserSort, toàn bộ Validation.query, filter/sort trước page và pageOf.
   - **3.9:** một contract test gọi HttpApp.run và khung Resource của SocketSuite, dùng cổng 0 và cổng thực từ server.address.
2. **MAJOR 2 — văn phong:** tách phần giải thích 3.1–3.9 thành các đoạn ngắn; bổ sung lý do trước code, rồi giải thích biến/nhánh sau code. Tách các đoạn về opaque/factory, codec/Schema, lỗi decode/media type, whitelist/tie-break/cursor và transaction adapter. Viết lại mục 8 theo đường đi request → DTO → validation → repository → response, kèm vòng đời Main/test. Giữ các giới hạn kỹ thuật đã nêu ở vòng 1.
3. **MINOR 3 — tiêu đề/trạng thái:** đưa `# Chương 14` lên dòng đầu. Ghi rõ đã review chéo; sbt **25/25 do Claude chạy trước vòng 2**, domain 5/app 20, theo review. Bản mới có thêm một test app nên ghi Scala CLI **26/26**, chờ lead kiểm tra lại; không suy diễn test mới đã chạy bằng sbt.
4. **MINOR 4 — vị trí trong lộ trình:** thêm mục riêng sau phần mở đầu: học sau Ch13/16B, trước Ch15/17/18; Project 2 ghép Ch14 + Ch15 + Ch17.
5. **MINOR 5 — cách chạy:** mục 7 ưu tiên `cd 05-backend/order-service`, `sbt.bat test`, `sbt.bat "app/run"`; có lựa chọn `scala-cli.bat test .`/run. Tách COURSIER_CACHE, `--server=false` và lệnh kiểm chứng `-Werror` vào ghi chú môi trường hạn chế. Đồng bộ cách trình bày ở lời giải; lệnh debug mặc định cũng bỏ cờ sandbox.

### 8.2. File tạo/sửa trong vòng 2

- **Sửa:** `05-backend/ch14-rest-apis.md`, `solutions/ch14-solutions.md`, `.team/handoffs/T03-ch14-rest-apis.md`.
- **Tạo:** `05-backend/order-service/app/src/test/scala/vn/scalaeng/orders/RefRaceExampleSuite.scala` — một test minh họa race có kiểm soát.
- **Tạo:** `05-backend/order-service/evidence/verify_snippets.py` — đối chiếu từng khối Scala với đoạn source liên tiếp, caption file/dòng, giới hạn 20 dòng/khối; kiểm tra 17 mục, link nội bộ và summary test khớp log.
- **Tạo bằng chạy công cụ thật:** `05-backend/order-service/evidence/tests-round2.txt`, `05-backend/order-service/evidence/snippets-round2.txt`, `solutions/ch14-code/test-output-round2.txt`.

Không sửa production Scala, build.sbt, project.scala hay code lời giải. Các log vòng 1 được giữ nguyên. Log vòng 2 chuyển encoding từ UTF-16 của PowerShell Tee-Object sang UTF-8 để đọc nhất quán; không thay nội dung output.

### 8.3. Kiểm chứng chạy thật trên bản sửa

Tại `05-backend/order-service`:

```powershell
$env:COURSIER_CACHE = Join-Path (Get-Location) '.cache/coursier'
scala-cli.bat test . --server=false --scalac-option -Werror
```

**Exit 0; 26 pass, 0 fail, 0 ignored; 0 compiler warning.** Output thật từ [tests-round2.txt](../../05-backend/order-service/evidence/tests-round2.txt):

```text
Test run vn.scalaeng.orders.ContractSuite finished: 0 failed, 0 ignored, 11 total 1.77s
Test run vn.scalaeng.orders.DomainSuite finished: 0 failed, 0 ignored, 5 total 0.016s
Test run vn.scalaeng.orders.OpenApiSuite finished: 0 failed, 0 ignored, 2 total 0.008s
Test run vn.scalaeng.orders.RefRaceExampleSuite finished: 0 failed, 0 ignored, 1 total 0.024s
Test run vn.scalaeng.orders.RepositorySuite finished: 0 failed, 0 ignored, 6 total 0.049s
Test run vn.scalaeng.orders.SocketSuite finished: 0 failed, 0 ignored, 1 total 0.683s
```

Tại `solutions/ch14-code`:

```powershell
$env:COURSIER_CACHE = (Resolve-Path ../../05-backend/order-service).Path + '/.cache/coursier'
scala-cli.bat test . --server=false --scalac-option -Werror
```

**Exit 0; 6 pass, 0 fail, 0 ignored; 0 compiler warning.** Output thật từ [test-output-round2.txt](../../solutions/ch14-code/test-output-round2.txt):

```text
Test run vn.scalaeng.orders.solutions.SolutionsSuite finished: 0 failed, 0 ignored, 6 total 2.19s
```

Từ root repo:

```powershell
python 05-backend/order-service/evidence/verify_snippets.py
```

**Exit 0.** Trích output thật từ [snippets-round2.txt](../../05-backend/order-service/evidence/snippets-round2.txt):

```text
25/25 Scala excerpts match source; 17 sections; local links OK
05-backend/order-service/evidence/tests-round2.txt: 26 passed, 0 failed, 0 ignored; OK
solutions/ch14-code/test-output-round2.txt: 6 passed, 0 failed, 0 ignored; OK
```

Mọi source chứa đoạn trích đã được Scala CLI biên dịch trong lần test trên. Test mới đã chạy, không phải ví dụ code chỉ kiểm tra bằng mắt. Script Python chỉ xác nhận tài liệu khớp source/log, không thay thế compiler. Scala CLI vẫn in hint nâng dependency và runtime vẫn báo SLF4J NOP như vòng 1; không có compiler warning với `-Werror`.

### 8.4. Phiên bản, đề xuất cho lead và phần chưa kiểm chứng

- **Phiên bản giữ nguyên toàn bộ:** Scala 3.9.0/JDK21, munit 1.2.4, munit-cats-effect 2.2.0 và stack ở mục 3. Test mới dùng CountDownLatch có sẵn trong Cats Effect **3.7.1**, không thêm dependency. Link nguồn/ngày kiểm chứng **17/10/2026** giữ tại mục 3 và `evidence/versions.json`; vòng 2 không thực hiện lượt tra phiên bản mới.
- **PROGRESS đề xuất:** đã sửa đủ 2 MAJOR/3 MINOR của review; 26 test lab + 6 test lời giải pass Scala CLI với `-Werror`; 25 đoạn trích khớp source; chờ lead kiểm tra bản sửa và chạy lại sbt. Codex không cập nhật file dùng chung hoặc tự đánh dấu ✅.
- **Glossary/compatibility matrix:** không thêm dependency hay thay trait/domain; các đề xuất mục 6 vẫn áp dụng. Bổ sung trạng thái sbt 25/25 đã do reviewer kiểm chứng ở vòng trước, phân biệt với bản mới thêm test.
- **Chưa kiểm chứng lại:** sbt trên bản có test mới do giới hạn sandbox trong brief; lead cần chạy `sbt.bat -batch test` (kỳ vọng domain 5/app 21). Không tuyên bố đã chạy lệnh này trong vòng 2.
- **HTTP curl/Main:** không chạy lại 22 trao đổi curl trong vòng 2 vì không sửa production source; giữ bằng chứng thật vòng 1. Contract test, socket cổng 0 và lời giải fault injection/decode đã chạy lại trên source hiện tại.
- Các giới hạn PostgreSQL adapter, transaction nhiều bước, ETag endpoint, auth và performance tại mục 7 vẫn còn nguyên; vòng 2 không mở rộng phạm vi đó.
