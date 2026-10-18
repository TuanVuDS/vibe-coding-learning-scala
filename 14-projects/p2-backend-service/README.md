> Scala 3.9.0 · JDK 21 · sbt 1.12.11 · Cats Effect 3.7.1 · http4s 0.23.38 · Tapir 1.13.33 · Doobie 1.0.0-RC13 · PostgreSQL 18.6 · Flyway 13.10.3 · JDBC 42.7.14 · HikariCP 7.0.2 · circe 0.14.17 · sttp-apispec 0.11.10 · password4j 1.8.4 · jwt-circe 11.0.4 · log4cats 2.8.0 · logback 1.6.5 · munit 1.2.4 · munit-cats-effect 2.2.0 · zonky embedded-postgres 2.2.2/binary 18.6.0 · sbt-assembly 2.3.1.

# Project 2 — Backend Service

Tác giả Codex, reviewer Claude Code. Đã review vòng 1, bản sửa vòng 2 chờ kiểm tra lại. Các POM release chính xác được tải ngày 17/10/2026:
[evidence/versions.json](evidence/versions.json), kèm URL và SHA-256. Đây là project minh họa
[Ch18](../../05-backend/ch18-backend-architecture.md), nối tiếp Ch14, Ch15, Ch17.

**Trạng thái kiểm chứng:** code/test được kiểm tra bằng Scala CLI với `--server=false`, `-Werror`.
Claude đã chạy sbt + PostgreSQL thật ngày 17/10/2026: **37/37** ở vòng 1, **47/47** ở vòng 2 (sau review chéo); `sbt app/assembly` thành công.
Vòng 2 sửa/thêm test DB, cần lead chạy lại; trong sandbox chúng tự skip khi không bật `RUN_DB_TESTS=1`.
Docker build/up, API qua container, SIGTERM và restart giữ dữ liệu **đã kiểm chứng 18/10/2026** (Docker Desktop, engine 28.5.1 — mục 6); không xem test skip là pass.
Kết quả và số đo thực tế nằm ở mục 6–7 và [evidence](evidence/).

## 1. Mục tiêu

Xây backend đơn hàng có hợp đồng HTTP, PostgreSQL bền vững, đăng ký/đăng nhập/thu hồi JWT, quyền sở hữu và admin,
transaction checkout, kiểm tra readiness thật, metrics, quản lý tài nguyên và cấu hình môi trường.
Một deployment JVM với sáu module; không có frontend hay thư viện resilience ngoài Cats Effect.

## 2. Tiên quyết

Học Ch14 (REST/DTO/port), Ch15 (Doobie/migration/locking), Ch16B (`IO`, `Resource`, cancellation), Ch17 (security).
Cài JDK 21, Scala CLI hoặc sbt; lần đầu cần Internet để tải dependency/binary.
Unit/HTTP test không cần DB, Docker hay mạng bên ngoài sau khi đã tải cache. Socket test dùng loopback cổng ngẫu nhiên.
Integration test dùng **PostgreSQL thật nhúng zonky**, không H2/mock SQL; cần quyền tạo subprocess.
Sandbox Codex chặn sbt và PostgreSQL nhúng; lead phải kiểm tra bên ngoài.

## 3. Đặc tả

### 3.1. Hợp đồng tài nguyên

Base path `/api/v1`; body là JSON UTF-8, tối đa **65536 byte (64 KiB)** trước decode JSON; quá giới hạn trả 413,
kể cả stream không có Content-Length ([http4s EntityLimiter](https://http4s.org/v0.23/docs/server-middleware.html#entitylimiter)
đếm byte thực; đối chiếu 17/10/2026). Endpoint ghi trả 201/200/204 như bảng. Lỗi dùng
`application/problem+json` với `type`, `title`, `status`, `detail`, `errors`, `requestId` tùy trường hợp.
`GET /openapi.yaml` sinh từ cùng 14 endpoint Tapir đang phục vụ, không giữ một spec viết tay khác.

| Method/path | Quyền | Chức năng/thành công |
|---|---|---|
| POST `/auth/register` | Public | `{name,email,password}` → 201 user; chỉ Customer |
| POST `/auth/login` | Public, 5 lần/phút/IP | `{email,password}` → 200 access token, TTL 900 giây |
| POST `/auth/logout` | Bearer | Thu hồi jti trong PostgreSQL → 204 |
| GET `/users/{userId}` | Chính mình/admin | 200 user, không có hash/role bí mật trong response |
| GET `/users?q=&sort=id&offset=0&limit=20` | Admin | Page user; sort `id/name/email`, thêm `-` để giảm |
| POST `/users/{userId}/orders` | Chủ tài khoản/admin | `{lines:[{productId,quantity}]}` → 201 order |
| GET `/users/{userId}/orders?offset=0&limit=20` | Chủ tài khoản/admin | Page order, ID tăng |
| GET `/orders/{orderId}` | Chủ đơn/admin | 200 order |
| PATCH `/orders/{orderId}/status` | Chủ đơn: Cancelled; admin: transition hợp lệ | `{status}` → 200 order |
| GET `/products/{productId}` | Public | 200 product đang hoạt động, cache TTL 2 giây |
| GET `/products?q=&sort=price&offset=0&limit=20` | Public | Page catalog; sort `price/-price`, lọc tên |
| POST `/products` | Admin | `{sku,name,price,stock}` → 201 product |
| PUT `/products/{productId}` | Admin | Cùng body; SKU bất biến; 200 product |
| DELETE `/products/{productId}` | Admin | Soft delete → 204, giữ lịch sử |

Không có endpoint đổi role public. `BootstrapAdmin` tạo tài khoản mới bằng biến môi trường riêng; email có sẵn không tự được nâng quyền.
Validation/băm mật khẩu chạy trước khi lấy connection; insert Customer và update Admin nằm trong cùng một `transact`.
Nếu promotion lỗi, cả registration rollback, nên chạy lại không bị kẹt bởi account Customer tạo dở.
User API giới hạn đăng ký/đọc, không có tự sửa quyền hoặc xóa user. Order API tạo/đọc/chuyển trạng thái, không xóa lịch sử.
Lọc user q là substring tên/email đã chuẩn hóa; catalog lọc tên không phân biệt hoa thường.
Offset 0..1.000.000, limit 1..100; sort có ID phụ. Items và total thuộc một statement snapshot, kể cả trang rỗng.
P2 dùng offset cho lab; catalog lớn nên cân nhắc keyset pagination.

### 3.2. Quy tắc nghiệp vụ và lỗi

- Name 1..100 ký tự; email chuẩn hóa lower/trim, tối đa 254; duplicate case-insensitive → 409.
- Password theo policy Ch17: 15..128 ký tự, tránh một số mật khẩu phổ biến/tên email; băm Argon2id m=19456 KiB, t=2, p=1, salt 16 byte.
  Danh sách password yếu chỉ là minh họa, chưa tích hợp kho mật khẩu lộ quy mô lớn.
- JWT HS256 đúng issuer/audience, subject dương, có exp còn hạn và jti UUID. Revocation kiểm tra sau xác minh; role đọc mới từ DB.
- SKU 1..32 `[A-Z0-9-]`, name 1..100, price VND `Long` >0, stock `Int` >=0. SKU giữ nguyên khi PUT; SKU khác/ID không tồn tại →404.
- Đơn có 1..100 dòng; mỗi dòng ID dương, quantity 1..1000; gộp trùng rồi kiểm tra tổng mỗi món <=1000.
- Không nhận giá thanh toán từ client. Giá lấy từ DB đã khóa; số học overflow →422, hết kho→409, thiếu/inactive product→404.
- Pending→Paid/Cancelled; Paid→Shipped; cùng trạng thái idempotent. Chỉ khách sở hữu mới hủy Pending; admin chuyển theo cùng state machine.
  Hủy hoàn kho đúng một lần, trong transaction có khóa order và sản phẩm. Paid không được hủy trong mô hình này.
- JSON lỗi/query không decode được→400, media type không hỗ trợ→415; thiếu/sai/thu hồi token→401, đủ token thiếu quyền→403;
  route thiếu→404, method sai→405 có Allow, validation→422, lỗi bất ngờ→500 an toàn, quá tải/timeout→503.

### 3.3. Endpoint vận hành

`/health` và `/health/live`: liveness độc lập DB. `/health/ready`: `SELECT 1` qua pool thật, statement timeout 800 ms,
effect timeout 1 giây; failure→503. Lệnh JDBC có thể cần chờ cleanup sau cancellation.
`/metrics`: Prometheus text, `http_requests_total{status}`, `login_failures_total` và summary
`# TYPE http_request_duration_seconds summary` với `_sum{status}`/`_count{status}` cùng label.
Chia duration sum cho duration count để lấy trung bình; chưa có histogram/p95. Không gắn ID người dùng/path tự do lên label.

## 4. Thiết kế và trade-off

```text
domain       Model, Checkout thuần; OrderRepository[F], Store[F]
service      Shop[F], TransactionRunner[F]; cache/retry/bulkhead
security     Argon2id, JWT, AuthService[F], rate limiter
http         DTO/codecs, Tapir endpoints, OpenAPI, Problem handler, metrics/health
 db          PgStore[ConnectionIO], Flyway, Hikari, SELECT 1 thật
app          Config + Resource composition, Ember, lệnh bootstrap admin
```

Đồ thị `.dependsOn`: service→domain; db→service; security→service; http→security; app→db/http.
Dependency transitively nhìn thấy domain/service nhưng HTTP không được import DB.
`ArchitectureSuite` kiểm tra import của domain/service, compiler kiểm tra domain riêng;
sbt là phép kiểm tra đầy đủ ranh giới sáu module; Claude đã chạy vòng 1, cần chạy lại sau sửa vòng 2.
`Store` gom nhiều port cho lab; khi lớn lên hãy tách AccountStore/CatalogStore thay vì ép adapter không liên quan cài mọi thao tác.

**Ranh giới transaction:** `PgStore` giữ tám thao tác Ch14 với `F=ConnectionIO`, thêm account/catalog.
`Shop` ghép toàn bộ checkout rồi `TransactionRunner.run` một lần; interpreter ở db gọi `transact`.
Mọi lỗi domain phải được quyết định trước ghi. Failed SQL sau ghi gây rollback; `Left` không tự rollback.
Khóa product theo ID tăng; `getOrder` chỉ đọc snapshot, `lockOrder` dùng `FOR UPDATE` trong status/updateStatus.
GET owner hoặc GET trả 403 không lấy row lock. Khóa order khi transition bảo vệ hoàn kho một lần;
`ON CONFLICT` xử lý duplicate mà không abort transaction.
Tổng tiền `Long` có kiểm tra; `order_items.price` là giá mua, soft delete giữ FK/lịch sử.

**Cache:** chỉ đọc catalog đơn lẻ; checkout đọc DB mới. Cache tối đa 1000 key, TTL 2 giây, invalidate sau thao tác ghi thành công.
Generation chặn fill cũ quay lại map sau invalidation, không sửa response đã đọc trước thay đổi.
Một instance, không single-flight; nhiều instance/ghi ngoài app có thể thấy stock/giá cũ tới TTL.
Crash sau commit trước invalidate cũng có cửa sổ cũ tối đa TTL. Catalog stock chỉ tham khảo, DB quyết định lúc checkout.
Số đo cache dùng upstream sleep 10 ms, không được coi là benchmark PostgreSQL.

**Resilience:** bulkhead 32 tác vụ API, fail-fast 503; timeout API 5 giây, Hikari acquisition 2 giây, JDBC connect 2/socket 5 giây.
Product read retry tối đa hai lần cho SQLTransientException (20/40 ms). Không tự retry checkout: chưa có idempotency key.
Sau timeout client có thể không biết commit đã xảy ra; cần điều tra, không bấm mua lại mặc định.
Retry/bulkhead/cancellation được test bằng Cats Effect TestControl và Deferred.

**Bảo mật:** JWT không có role; request có bảo vệ đọc role/revocation DB. Quota RAM theo IP nằm trong endpoint login;
path slash cuối/encoded không tạo quota khác. Sau proxy có thể chung remote IP; production cần cấu hình trusted proxy/quota chung.
Rate limiter dùng map RAM tối đa 10000 bucket; khi gặp key mới, loại bucket nhàn rỗi đã hồi đầy token.
Khi đủ bucket đang hoạt động, IP mới nhận 429, không đẩy bucket đang hoạt động ra để né quota.
Việc dọn quét map có chi phí O(n) với key mới; giới hạn map bảo vệ RAM nhưng có thể từ chối IP hợp lệ khi tải lớn.
Quota chưa chia sẻ giữa instance; production nên đặt quota tại gateway/kho chung.
Log không body, header Authorization được che; cấu hình secret không có mặc định và toString che.
HTTP error handler chung với request id; Tapir exceptionHandler(None). Không bật CORS do không có client khác origin trong project.
HTTPS do TLS terminator của môi trường triển khai; Docker chỉ bind localhost.

**Tài nguyên:** Main mở pool rồi server bằng Resource. Server đóng trước pool, chờ 10 giây; Compose cho 20 giây và truyền SIGTERM tới Java.
Socket test kiểm tra Resource đóng cổng; SIGTERM dưới Docker đã kiểm chứng: pool Hikari đóng êm, `docker compose stop` mất ~1 giây (mục 6).
Observability nội bộ chưa có distributed tracing hoặc dashboard; metrics/health nên đặt trong mạng vận hành.

## 5. Cách chạy

### 5.1. Test trong sandbox (không PostgreSQL)

Từ root repo, PowerShell:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
scala-cli.bat test 14-projects/p2-backend-service --server=false
scala-cli.bat test solutions/ch18-code --server=false
python 14-projects/p2-backend-service/evidence/run_checks.py
```

Script cuối chạy test P2, lời giải, compile domain riêng, demo cache và probe thiếu cấu hình, lưu log UTF-8.
Nó chủ động bỏ `RUN_DB_TESTS` trong môi trường con để phù hợp sandbox; chạy test DB bằng lệnh mục 5.2.
Có `-Werror` trong directives. Hints về bản munit mới không phải cảnh báo compiler; giữ munit-cats-effect 2.2.0 để khớp munit 1.2.4.

### 5.2. Kiểm tra đầy đủ ngoài sandbox — dành cho lead

```powershell
$env:RUN_DB_TESTS = '1'
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
scala-cli.bat test 14-projects/p2-backend-service --server=false
Push-Location 14-projects/p2-backend-service
sbt.bat test
sbt.bat 'app/assembly'
Pop-Location
Remove-Item Env:RUN_DB_TESTS
```

Test DB chỉ bật khi `RUN_DB_TESTS=1`; khi đã bật, lỗi khởi động PostgreSQL là failure, không tự skip để che lỗi.
Zonky tải/chạy binary PostgreSQL 18.6 thật, Flyway dùng migration production. Không cần Docker.
Integration DB mặc định tự assume skip vì không thể tạo restricted token trong sandbox.
Không đặt RUN_DB_TESTS=1 trong build image; Docker build tạo artifact, verification chạy trước deployment.

### 5.3. Chạy app với PostgreSQL

Trong thư mục P2, tạo secret trong bộ nhớ shell (không có secret mặc định trong code/compose):

```powershell
$env:DB_PASSWORD = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:JWT_SECRET = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
docker compose up -d postgres
$env:DB_URL = 'jdbc:postgresql://127.0.0.1:5432/shop'
$env:DB_USER = 'shop'
$env:PORT = '8080'
scala-cli.bat run . --server=false --main-class vn.scalaeng.p2.app.Main
```

Giữ nguyên DB_PASSWORD đã dùng tạo volume. Tạo password mới trong shell sau khi volume tồn tại không thay password user trong PostgreSQL;
dùng secret manager/lưu biến môi trường an toàn của bạn để giữ nó qua các phiên.
Nếu không có Docker, dùng PostgreSQL đã có và đặt DB_URL/DB_USER/DB_PASSWORD tương ứng.
Main chạy Flyway trước khi mở server; không phục vụ request nếu migration thất bại.
Env bắt buộc: DB_URL, DB_USER, DB_PASSWORD, JWT_SECRET (>=32 byte UTF-8). PORT mặc định 8080; DB_POOL_SIZE mặc định 8, cho phép 1..32.
`DbConfig.toString` che cả JDBC URL vì URL có thể chứa thông tin nhạy cảm.

Tạo admin một lần trong shell có các biến DB/JWT trên:

```powershell
$env:ADMIN_EMAIL = 'admin@example.vn'
# Đặt ADMIN_PASSWORD riêng, ít nhất 15 ký tự, không chứa tên email; không có giá trị mặc định.
scala-cli.bat run . --server=false --main-class vn.scalaeng.p2.app.BootstrapAdmin
Remove-Item Env:ADMIN_PASSWORD
```

Password do người vận hành cung cấp. Nếu account tồn tại, lệnh từ chối nâng quyền; cần quy trình quản trị DB riêng.
Đăng ký user bằng POST `/api/v1/auth/register`, login lấy accessToken, gửi `Authorization: Bearer <token>`.
Tài liệu request/response đầy đủ sinh tại `http://127.0.0.1:8080/openapi.yaml`.

### 5.4. Docker hai service

```powershell
# Cùng DB_PASSWORD/JWT_SECRET đã thiết lập; không đưa chúng vào git.
docker compose config --quiet
docker compose up --build -d
curl.exe http://127.0.0.1:8080/health/ready
curl.exe http://127.0.0.1:8080/metrics
docker compose run --rm -e ADMIN_EMAIL -e ADMIN_PASSWORD --entrypoint java app -cp /app/p2.jar vn.scalaeng.p2.app.BootstrapAdmin
docker compose stop app
```

Dockerfile multi-stage dùng Temurin JDK 21 để `app/assembly`, runtime JRE 21 chạy UID 10001;
`META-INF/services` được concat để giữ Flyway/JDBC service providers. Image app không chứa compiler/source/secret.
Compose dùng `postgres:18.6`, healthcheck `pg_isready`, volume parent `/var/lib/postgresql` theo official image PostgreSQL 18.
App vẫn tự migrate/SELECT 1, không chỉ dựa healthcheck của container. Cổng app/DB chỉ publish vào loopback.
Không xóa volume khi muốn kiểm tra dữ liệu còn sau restart. Build/start, API, SIGTERM và restart đã chạy thật (mục 6).
Lưu ý khi chạy lệnh `docker compose run ... -cp /app/p2.jar` từ **Git Bash**: MSYS đổi `/app/p2.jar` thành đường dẫn Windows → `ClassNotFoundException:
vn.scalaeng.p2.app.BootstrapAdmin` (gặp thật). Chạy từ PowerShell, hoặc đặt `MSYS_NO_PATHCONV=1` trong Git Bash.

## 6. Output thực tế

**Vòng 1, 17/10/2026: `RUN_DB_TESTS=1 sbt.bat -batch test`, exit 0, 37/37 pass với PostgreSQL thật**. **Vòng 2: 47/47 pass**, `app/assembly` exit 0.
Nguồn: [review Claude](../../.team/reviews/T07-ch18-p2-review.md), trích nguyên output lưu trong
[sbt-postgres-review-round1.txt](evidence/sbt-postgres-review-round1.txt):

```text
vn.scalaeng.p2.db.PostgresSuite: finished 21.928s
Test run vn.scalaeng.p2.db.PostgresSuite finished: 0 failed, 0 ignored, 12 total 21.951s
vn.scalaeng.p2.app.PostgresHttpSuite: finished 15.967s
Test run vn.scalaeng.p2.app.PostgresHttpSuite finished: 0 failed, 0 ignored, 1 total 15.968s
```

Kết quả Scala CLI **vòng 2: 29 test P2 pass + 6 test lời giải pass, 0 fail, 0 cảnh báo compiler; 18 test DB assume-skip**.
Chi tiết: [summary.json](evidence/summary.json), [tests.txt](evidence/tests.txt),
[solutions.txt](evidence/solutions.txt). Test DB sửa/thêm vòng 2 chờ Claude chạy lại.
Munit tính assume-skip vào `total` dù dòng tổng ghi `0 ignored`; số pass trong summary đã trừ các dòng `==> s ... skipped`.
Main/test biên dịch với `-Werror`; domain có kiểm tra compile riêng. Demo/probe thật từ vòng 1:
[demo.txt](evidence/demo.txt), [config-missing.txt](evidence/config-missing.txt).

Output demo thật:

```text
20 reads, upstream=10ms: uncached calls=20 elapsed_ms=318
20 reads, upstream=10ms: cached calls=1 elapsed_ms=17
after invalidation: upstream calls=2
java.lang.IllegalArgumentException: permits phải dương
```

Nguồn mô phỏng bằng sleep, không phải PostgreSQL. Thời gian scheduler Windows có thể vượt 10 ms mỗi lượt;
so sánh số lần gọi và kiểm tra invalidation trước khi suy luận hiệu năng thực tế.

Probe thiếu cấu hình thật:

```text
cấu hình không hợp lệ:
- thiếu DB_URL
- thiếu DB_USER
- thiếu DB_PASSWORD
- thiếu JWT_SECRET
```

Main chọn ExitCode(2); Scala CLI báo shell exit 1 cho app exit khác 0. Script verification kiểm tra đúng shell exit 1.
`docker compose config --quiet` đã chạy exit 0; CLI cảnh báo không đọc được Docker config cá nhân trong sandbox.
**Docker (18/10/2026, Docker Desktop, engine 28.5.1, Windows 11):** `docker compose config --quiet` exit 0; `docker compose up --build -d` exit 0
(build lần đầu 159 giây gồm sbt `app/assembly` trong stage build; image `p2-backend-service-app` 512 MB). Chuỗi kiểm chứng thật qua container:

```text
GET /health/ready                → database=UP HTTP 200
GET /health/live                 → UP HTTP 200
docker compose exec app id       → uid=10001(app) gid=999(app) groups=999(app)
BootstrapAdmin (docker compose run) → đã tạo admin
POST /products (admin)           → 201 {"id":1,"sku":"KB-01",...,"price":450000,"stock":5,"active":true}
POST /auth/register              → 201 {"id":2,"name":"An","email":"an@example.vn"}
POST /users/2/orders (2 × KB-01) → 201 {"id":1,"userId":2,"totalMinor":900000,"status":"Pending"}
GET /products/1                  → 200 ... "stock":3 ...      (tồn kho 5 → 3)
GET /users/2 không token         → 401
docker compose stop app          → 1028 ms; log: "p2-db - Shutdown initiated..." → "Shutdown completed."; exit code container 143
docker compose start app         → sản phẩm vẫn còn (volume pgdata giữ dữ liệu)
```

Tiếng Việt qua API (lượt riêng, body gửi bằng file UTF-8): `POST /products` → 201 `{"id":1,"sku":"KB-02","name":"Bàn phím cơ",...}`. Gửi `-d '...cơ...'` từ
Git Bash làm tên thành `B�n ph�m co` — lỗi mã hóa phía **client** (console Windows), không phải của API; dùng `--data-binary @file.json` với file UTF-8.

Exit code **143** = 128 + 15 (SIGTERM): JVM kết thúc vì tín hiệu sau khi chạy shutdown hook — pool đã đóng êm trước đó (log trên), nên đây là dừng có kiểm soát,
không phải crash. Muốn orchestrator thấy 0 thì app phải tự bắt tín hiệu và trả `ExitCode.Success`; P2 giữ hành vi mặc định của JVM.
Không có transcript curl chạy với PostgreSQL trong sandbox; bảng mục 3 là đặc tả, không phải output được bịa.

## 7. Kiểm thử

| Suite | Thuộc tính kiểm tra |
|---|---|
| CoreSuite | merge overflow, tổng tiền, kho, inactive, validation, state machine |
| ArchitectureSuite | domain/service không import adapter; thêm compile domain độc lập |
| ResilienceSuite | TTL/invalidation, race in-flight, retry/backoff, bulkhead/cancel/timeout |
| CryptoSuite | Argon2id, salt, policy không phụ thuộc locale, secret, JWT sai secret/hết hạn/thiếu claim |
| RateLimiterSuite | refill/thời gian chờ; chặn IP mới khi map đầy, giữ quota đang hoạt động, dọn bucket hồi đầy |
| ApiSuite | body quá 64 KiB→413 có/không Content-Length; register/login/duplicate/decode, owner/admin, giá server, invalidation, revocation/signature alias, quota path alias, OpenAPI, socket |
| OperationsSuite | summary TYPE/sum/count cùng label; Tapir exception→Problem 500 có request id; readiness timeout |
| ConfigSuite | không secret mặc định, giá trị không lộ qua lỗi/toString |
| PostgresSuite | GET owner/403 không chờ row lock nhưng updateStatus phải khóa; migration/readiness, ON CONFLICT không abort, checkout, oversell, rollback, hủy đồng thời, overflow, pagination/injection, soft delete, revocation, authorization, CHECK |
| PostgresHttpSuite | HTTP→DB: register/login, admin catalog, checkout/hết kho, IDOR, readiness/logout |
| BootstrapAdminSuite | Admin/hash thật; duplicate không sửa account; Customer không bị nâng quyền; trigger ép promotion lỗi→rollback rồi retry thành công; invalid input không ghi |
| Lời giải | Sáu bài về policy, cache key, SQLSTATE retry, deadline, gauge, commit/invalidate |

Adapter bộ nhớ trong http test chỉ kiểm tra hợp đồng và dây nối, không giả vờ có transaction SQL.
PostgresSuite vòng 1 đã chạy thật ngoài sandbox; hồi quy vòng 2 và BootstrapAdminSuite cần lead chạy lại.
Code đã biên dịch không chứng minh query mới hợp lệ với server.
Mọi module có `src/test/resources/logback-test.xml` đặt root WARN; Scala CLI nạp bản ở module http qua `test.resourceDir`.
Test cố ý gây lỗi vẫn có thể in ERROR; log DEBUG/INFO của Tapir/http4s không tràn output.
Test socket đóng client trước server rồi thử socket mới để tránh keep-alive cũ làm sai phép kiểm tra shutdown.

## 8. Lỗi thường gặp

- **Thiếu cấu hình:** Main chọn exit 2 (Scala CLI wrapper trả 1), liệt kê tên biến còn thiếu, không in giá trị; output thật ở config-missing.txt.
- **Compiler API Tapir:** `.contentType` không tồn tại trên Body; dùng CodecFormat như Ch18 mục 10. Không tắt Werror để lướt qua import thừa.
- **Sbt/pg_ctl trong sandbox:** giới hạn đã nêu trong brief; không thể lấy kết quả DB pass bằng unit test.
- **Docker daemon (lúc biên soạn, đã khắc phục 18/10/2026):** probe khi daemon chưa chạy báo `open //./pipe/docker_engine: The system cannot find the file specified.` — bật Docker Desktop rồi chạy lại.
  Bật Docker Desktop ở môi trường phù hợp rồi kiểm tra; artifact Docker chưa được chạy tại đây.
- **Mua lại sau timeout:** có thể nhân đôi order; P2 chưa có idempotency key. Thiết kế thêm key + fingerprint + unique trong DB trước khi retry POST tự động.
- **Catalog cũ:** TTL trong một instance không đồng bộ các replica; checkout vẫn đọc giá/kho DB mới.

## 9. Bài tập mở rộng

Làm [sáu bài có code/test](../../solutions/ch18-solutions.md), rồi mở rộng:

1. Idempotency key cho checkout: key theo user, request fingerprint, unique, cùng transaction; concurrent duplicate chỉ tạo một order.
2. Outbox transactional cho OrderCreated; consumer dedup theo event id; kiểm tra crash sau gửi trước đánh dấu.
3. Tách StockView khỏi catalog cache, đo hit/miss/staleness khi chạy hai instance, chọn version hoặc invalidation phân tán.
4. Thêm histogram và dashboard RED; thử SIGTERM trong khi có request dài, chứng minh pool đóng sau server.

Các phần mở rộng này chưa được triển khai; không nhận là tính năng có sẵn.

## 10. Checklist bàn giao

- [x] Chạy unit/HTTP/solution với đúng Scala 3.9.0/JDK 21, không cảnh báo.
- [ ] Chạy PostgreSQL thật với RUN_DB_TESTS=1; không tính skip là pass.
- [ ] Lead kiểm tra sbt test, app/assembly và đồ thị module.
- [ ] Chạy Docker build/compose, migration, readiness và restart còn dữ liệu.
- [ ] Kiểm tra SIGTERM, request đang chạy, không lộ secret trong log.
- [ ] Claude review chéo và chạy lại sau sửa; chỉ lead cập nhật file dùng chung/commit.
