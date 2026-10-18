# Chương 17 — Backend Security và Production

> **Phiên bản:** Scala 3.9.0 · JDK 21 · http4s 0.23.38 · Tapir 1.13.33 · password4j 1.8.4 (Argon2id) · jwt-scala (`jwt-circe`) 11.0.4 ·
> http4s-prometheus-metrics 0.25.0 · log4cats-slf4j 2.8.0 · logback 1.6.5 · munit 1.2.4 · munit-cats-effect 2.2.0 · cats-effect-testkit 3.7.1
> **Code:** [`security-lab/`](security-lab/) — đăng ký/đăng nhập, JWT, phân quyền vai trò + sở hữu, rate limit, CORS, secret, log che header,
> lỗi 500 an toàn, health check, metrics Prometheus, graceful shutdown
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **28/28 test pass** (`scala-cli test .` và `sbt test`), lời giải 7/7
> (gồm HTTPS thật với keystore sinh bằng `keytool`). Server đã chạy thật; mọi trao đổi `curl` và dòng log là output thật.
> Lần chạy thật đầu tiên phát hiện **3 lỗi thiết kế** (mục 10.1, 10.2, 10.6); **review chéo của Codex** phát hiện thêm **2 lỗi nghiêm trọng và 4 lỗi lớn**
> — các cách vượt qua bảo vệ có thật, đã tái hiện bằng code (mục 10.7–10.11). Tất cả đã sửa, mỗi lỗi có một regression test.

---

## Vị trí trong lộ trình

Chương 12 giới thiệu xác thực bằng token tĩnh để hiểu khái niệm; Chương 13–15 dựng API và database. Chương này biến API thành thứ
**dám đưa lên Internet**: lưu mật khẩu đúng cách, phát hành và kiểm tra token, phân quyền, chống dò mật khẩu, không lộ bí mật qua log,
và vận hành được (health check, metrics, dừng an toàn). Project 2 dùng lại toàn bộ.

> **Phạm vi.** Bảo mật là lĩnh vực rộng. Chương này dạy những điều **mọi backend engineer phải làm đúng**; không thay thế cho
> đánh giá bảo mật chuyên nghiệp, threat modeling hay pentest. Nguồn tham chiếu chính: OWASP (mục 16).

## 1. Mục tiêu học tập

1. Lưu mật khẩu bằng **Argon2id** với tham số theo OWASP và chính sách độ dài theo NIST SP 800-63B-4; giải thích vì sao không dùng SHA-256.
2. Phát hành và xác minh **JWT**; liệt kê các kiểm tra bắt buộc (thuật toán, chữ ký, hạn dùng, issuer) và giới hạn của JWT.
3. Phân biệt **authentication (401)** và **authorization (403)**; cài phân quyền theo vai trò và theo **quyền sở hữu** (chống IDOR).
4. Thiết kế đăng nhập không lộ thông tin (cùng thông báo lỗi, thời gian tương đương) và **rate limit** chống dò mật khẩu.
5. Cấu hình **CORS** đúng; hiểu HTTPS/TLS và nơi kết thúc TLS.
6. Quản lý **secret**: không giá trị mặc định, không lộ qua `toString`/log.
7. Vận hành: log có request id và che header nhạy cảm, lỗi 500 không lộ chi tiết nội bộ, **health check** live/ready, **metrics**, **graceful shutdown**.

## 2. Kiến thức tiên quyết

- Chương 12 (HTTP, header, status code, middleware là hàm), Chương 13 (Tapir + http4s, decode failure handler).
- Chương 16B (`IO.blocking`, `Ref`, `Resource`, `TestControl`).
- Chương 9 (cấu hình theo môi trường, log).

## 3. Lý thuyết

### 3.1. Lưu mật khẩu: băm chậm có chủ ý

**Vấn đề.** Database sẽ có ngày bị lộ (backup bị đánh cắp, SQL injection, nhân viên). Nếu lưu mật khẩu gốc — hoặc mã hóa có thể giải ngược —
mọi tài khoản bị lộ ngay, kể cả ở **trang web khác** vì người dùng dùng lại mật khẩu.

**Vì sao không dùng SHA-256?** SHA-256 được thiết kế để **nhanh**. Đo trên máy biên soạn (12 CPU logic):

| Hàm | Thời gian một lần (trung vị 10 lần) | Kẻ tấn công thử được mỗi giây (1 lõi, ước lượng) |
|---|---|---|
| SHA-256 | **0.023 ms** | ~40.000 |
| Argon2id m=19 MiB, t=2, p=1 | **44.4 ms** | ~22 — và mỗi lần thử tốn 19 MiB RAM |

Argon2id chậm hơn khoảng 2.000 lần **và** tốn bộ nhớ, nên đoán thử hàng loạt bằng GPU/ASIC đắt hơn nhiều. Với người dùng thật, 44 ms
một lần đăng nhập là không đáng kể.

**Cách hoạt động** (`Passwords.scala`):

```scala
private val argon2id = Argon2Function.getInstance(19456, 2, 1, 32, Argon2.ID)   // 19 MiB, 2 vòng, song song 1, 32 byte

def hash(plain: String): IO[String] =
  IO.blocking(Password.hash(plain).addRandomSalt(16).`with`(argon2id).getResult)

def verify(plain: String, stored: String): IO[Boolean] =
  IO.blocking(Password.check(plain, stored).`with`(Argon2Function.getInstanceFromHash(stored)))
```

- **Salt ngẫu nhiên 16 byte** mỗi lần băm: cùng mật khẩu → hash khác nhau (test `CryptoSuite`), nên không dùng được bảng tra sẵn (rainbow table)
  và không nhận ra hai người dùng cùng mật khẩu.
- Kết quả **tự mô tả**: `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`. `verify` đọc tham số từ chính chuỗi hash, nên tăng tham số trong tương lai
  không làm hỏng hash cũ (băm lại khi người dùng đăng nhập thành công).
- `IO.blocking`: băm tốn CPU và chặn thread khoảng 44 ms → không chạy trên compute pool (Chương 16B mục 3.4).
- ``with`` có backtick vì `with` là từ khóa Scala; password4j là thư viện Java.

**Lựa chọn thay thế:** bcrypt (cũ hơn, giới hạn 72 byte đầu vào), scrypt, PBKDF2 (có sẵn trong JDK — dùng khi bắt buộc chuẩn FIPS, cần số vòng rất lớn).
**Chính sách mật khẩu** (`Passwords.policy`): tối thiểu **15 ký tự**, tối đa 128 (chặn DoS bằng chuỗi khổng lồ), chặn mật khẩu phổ biến;
**không** ép "phải có ký tự đặc biệt". NIST SP 800-63B-4 (bản hiện hành, thay thế SP 800-63B): mật khẩu là yếu tố xác thực **duy nhất** phải dài
ít nhất 15 ký tự; khi là một phần của MFA được phép ngắn hơn nhưng tối thiểu 8. Bản đầu của lab dùng 12 theo nguồn cũ — review chéo đã chỉ ra.

**Lỗi thiết kế thường gặp:** băm bằng MD5/SHA không salt; tự "mã hóa" mật khẩu để còn giải ngược; log request đăng nhập kèm body (lộ mật khẩu
vào log — lab dùng `logBody = false`); so sánh hash bằng `==` tự viết thay vì hàm `check` của thư viện.

### 3.2. Token: JWT và những gì phải kiểm tra

**Vấn đề.** Sau khi đăng nhập, mỗi request tiếp theo cần chứng minh "tôi là An" mà không gửi lại mật khẩu.

**JWT** (JSON Web Token) gồm ba phần base64url nối bằng dấu chấm: `header.payload.signature`. Payload thật của lab (giải mã phần thứ hai):

```json
{"iss":"scalaeng-orders","sub":"2","exp":1792172103,"iat":1792171203,"email":"an@scalaeng.vn","role":"Customer"}
```

**Điều quan trọng nhất:** payload **không được mã hóa** — ai có token cũng đọc được. Chữ ký HMAC-SHA256 chỉ chứng minh token do server
(người giữ secret) phát hành và **chưa bị sửa**. Không đặt bí mật (số CMND, mật khẩu) vào payload.

`Tokens.verify` kiểm tra:

| Kiểm tra | Tấn công bị chặn | Test |
|---|---|---|
| Chỉ chấp nhận `HS256` (`Seq(Algorithm)`) | Đổi header thành `"alg":"none"` và bỏ chữ ký | `JWT: token alg=none ... bị từ chối` |
| Chữ ký | Sửa payload `Customer` → `Admin` | `JWT: sửa payload (nâng quyền thành Admin) -> chữ ký sai` |
| `exp` (hết hạn) | Dùng lại token bị lộ mãi mãi | `JWT: hết hạn sau TTL` — đồng hồ giả, không phải chờ 15 phút |
| `iss` (issuer) | Token của hệ thống khác dùng chung secret | `JWT: token do issuer khác ... bị từ chối` |
| `exp` **bắt buộc có** | Token không hạn dùng (phát hành nhầm, công cụ nội bộ) dùng được mãi | `JWT: token ký đúng secret nhưng KHÔNG có exp bị từ chối` |
| `jti` bắt buộc có | Không thu hồi được một token cụ thể | `JWT: mỗi token có jti riêng` |

> **Bẫy của thư viện:** `jwt.decode` chỉ kiểm tra `exp` **nếu claim có mặt**. Theo RFC 7519, `exp` là tùy chọn, nên token không có `exp` vẫn
> "hợp lệ" với thư viện. Chính sách "access token ngắn hạn" là của **ứng dụng**, nên `verifyClaims` phải tự đòi `exp` (mục 10.11).

**Secret HS256 cần ≥ 256 bit entropy.** `SecurityConfig.load` từ chối secret < 32 ký tự — nhưng đó chỉ là kiểm tra **độ dài**: 32 chữ `a` vẫn qua.
Secret yếu bị dò offline: kẻ tấn công chỉ cần **một** token hợp lệ. Sinh secret bằng nguồn ngẫu nhiên mật mã, ví dụ `openssl rand -base64 32`
(32 byte ngẫu nhiên = 256 bit); không tự nghĩ ra.

**Giới hạn của JWT (stateless token):** server không lưu gì → **không thu hồi được** trước khi hết hạn. Hệ quả thiết kế:
access token **ngắn hạn** (lab: 15 phút) + refresh token lưu ở server, hoặc danh sách thu hồi theo `jti` (lời giải bài 3).

**Thu hồi phải so theo `jti` sau khi đã xác minh chữ ký, không so theo chuỗi token.** Chữ ký HS256 dài 32 byte = 256 bit, viết bằng 43 ký tự
base64url = 258 bit: 2 bit cuối là bit thừa, nên một chữ ký có **nhiều cách viết** đều được thư viện chấp nhận. Review chéo chứng minh token
"đã thu hồi" vẫn dùng được nếu chỉ đổi ký tự cuối (mục 10.9).

**Lựa chọn thay thế: session phía server.** Server lưu session (Redis/PostgreSQL), client giữ cookie `HttpOnly; Secure; SameSite` chứa id ngẫu nhiên.
Thu hồi tức thì, payload không lộ; đổi lại cần kho session dùng chung và phải chống CSRF khi dùng cookie. **Chọn:** web app cùng domain → session cookie
thường đơn giản và an toàn hơn; API cho nhiều client (mobile, service khác) → access token ngắn hạn. Bài tập 6 yêu cầu bạn lập luận.

**Lỗi thiết kế:** token sống nhiều ngày; lưu token trong `localStorage` của trình duyệt (XSS đọc được) — vấn đề phía frontend, nhưng backend
quyết định TTL và cơ chế thu hồi; chấp nhận mọi thuật toán trong header; dùng chung secret giữa các hệ thống.

### 3.3. Authentication vs authorization; IDOR

| | Câu hỏi | Thất bại | Ở đâu trong lab |
|---|---|---|---|
| Authentication | *Bạn là ai?* | **401 Unauthorized** + `WWW-Authenticate` | `securityIn(auth.bearer[String]())` + `serverSecurityLogicPure` |
| Authorization | *Bạn được làm gì?* | **403 Forbidden** | `requireRole`, kiểm tra sở hữu trong `userById` |

Tapir tách hai phần: `securityIn` + `serverSecurityLogic` chạy **trước**, biến token thành `AuthUser`; logic nghiệp vụ nhận `AuthUser` đã xác thực.
Không thể "quên" kiểm tra token cho endpoint dùng `secured` — compiler buộc logic nhận `AuthUser`.

**Phân quyền theo vai trò** (`listUsers`): chỉ `Admin`. **Phân quyền theo sở hữu** (`userById`): user chỉ xem hồ sơ của chính mình.
Thiếu kiểm tra thứ hai là lỗi **IDOR** (Insecure Direct Object Reference) — đổi `/users/2` thành `/users/1` là đọc được dữ liệu người khác.
IDOR thuộc nhóm *Broken Access Control*, đứng đầu danh sách OWASP Top 10 (bản 2021).

**403 hay 404?** Trả 404 cho tài nguyên không thuộc về người hỏi giấu được việc tài nguyên tồn tại; 403 dễ debug hơn. Lab chọn 403 cho rõ ràng; với
dữ liệu nhạy cảm (đơn hàng của người khác), nhiều hệ thống chọn 404.

### 3.4. Đăng nhập không lộ thông tin + rate limit

**Không cho dò email.** Sai email và sai mật khẩu trả **cùng** `401` + cùng thông báo (`đăng nhập sai: CÙNG thông báo...` so sánh nguyên response).
Khi email không tồn tại, `AuthService` vẫn chạy `verify` trên một hash giả để thời gian phản hồi tương đương (nếu không, "email không tồn tại" trả về
nhanh hơn 44 ms — đủ để đo).

> Lưu ý: endpoint **đăng ký** vẫn trả `409 email đã được đăng ký` — tức là vẫn dò được email qua đăng ký. Hệ thống cần chống dò triệt để sẽ
> trả cùng một thông báo ("kiểm tra email để hoàn tất") và gửi email xác nhận. Đây là đánh đổi giữa trải nghiệm và bảo mật — hãy quyết định có chủ ý.

**Rate limit — token bucket** (`RateLimiter.scala`). Mỗi khóa (IP) có tối đa `capacity` token, hồi đều `capacity` token mỗi phút; mỗi lần đăng nhập
tiêu 1 token; hết → `429 Too Many Requests` + `Retry-After` (giây) + Problem JSON.

**Đặt rate limit ở đâu?** Bản đầu đặt ở một middleware http4s so khớp chuỗi `req.uri.path == "/api/v1/auth/login"`. Router của Tapir lại chấp nhận
cả `/api/v1/auth/login/` và `/api/v1/auth/%6Cogin` (percent-encoding của chữ `l`) cho **cùng** endpoint → hai biến thể đó không bị giới hạn,
không bị đếm (mục 10.7). Sửa: gọi `loginLimiter.acquire(ip)` **bên trong logic của endpoint login**, sau khi router đã nhận diện đúng endpoint;
IP lấy bằng `extractFromRequest(_.connectionInfo.remote...)`. Nguyên tắc: **kiểm tra an ninh đặt ở nơi đã biết chắc request là gì**, không đặt
ở chỗ phải đoán lại bằng một quy tắc so khớp riêng. Trạng thái là `Map[String, Bucket]` trong một `Ref` (Chương 16B), thời gian lấy từ
`Clock[IO].monotonic` (không lùi khi chỉnh đồng hồ hệ thống) — nhờ đó test được bằng **thời gian ảo** của `TestControl`
(`token bucket hồi token theo thời gian`: đợi 20 giây ảo hồi đúng 1 token).

**Giới hạn của cách làm trong lab:**
- Trạng thái trong bộ nhớ → mỗi instance đếm riêng; nhiều instance cần kho chung (Redis) hoặc rate limit ở API gateway.
- Khóa theo IP: kẻ tấn công xoay vòng nhiều IP (*credential stuffing*) → thêm giới hạn theo **email bị nhắm tới** (lời giải bài 4).
- Sau proxy, `req.remote` là IP của proxy. Chỉ tin `X-Forwarded-For` khi biết chắc request đi qua proxy của bạn.
- `Map` không bao giờ dọn khóa cũ → production cần hết hạn khóa (bài tập mở rộng).
- Email phải được chuẩn hóa **bằng một hàm duy nhất** (`Email.canonical`: trim + lowercase `Locale.ROOT`) ở đăng ký, tra cứu, đăng nhập và mọi bộ
  giới hạn. Nếu mỗi nơi chuẩn hóa một kiểu, `" AN@x.vn"` có thể vượt giới hạn theo email nhưng vẫn đăng nhập được, hoặc ngược lại.

### 3.5. CORS

**Vấn đề.** Trình duyệt chặn JavaScript ở `https://shop.scalaeng.vn` đọc response từ `https://api.scalaeng.vn` (khác *origin*) — **same-origin policy**.
CORS là cách server **nới lỏng** chặn đó cho origin tin cậy.

**Quan trọng:** CORS là cơ chế **của trình duyệt**. Nó không bảo vệ API khỏi `curl`, script hay server khác. Đừng coi CORS là xác thực.

Lab chỉ cho phép origin trong `CORS_ORIGINS`, method GET/POST, header `Authorization`, `Content-Type`. Preflight thật (mục 9) trả
`Access-Control-Allow-Origin: https://shop.scalaeng.vn`; origin lạ không nhận header đó → trình duyệt chặn (test `CORS:`).
**Lỗi thường gặp:** `Access-Control-Allow-Origin: *` cùng `Allow-Credentials: true` (trình duyệt từ chối tổ hợp này); phản chiếu (echo) mọi `Origin` nhận được.

### 3.6. HTTPS và nơi kết thúc TLS

HTTPS mã hóa kết nối và xác thực server bằng chứng chỉ. **Không có HTTPS thì mọi token, mật khẩu đi trên mạng dạng rõ.**

| Mô hình | Mô tả | Khi nào |
|---|---|---|
| TLS ở load balancer / ingress | LB giữ chứng chỉ, chuyển tiếp HTTP vào mạng nội bộ | Phổ biến nhất; ứng dụng đơn giản |
| TLS đến tận service | Ứng dụng tự bật TLS (Ember `.withTLS`) | Yêu cầu mã hóa nội bộ (zero-trust), không có proxy |

Lời giải bài 5 chạy **HTTPS thật**: sinh keystore PKCS12 tự ký bằng `keytool` của JDK, bật `withTLS` cho Ember server, client tin đúng chứng chỉ đó.
Header `Strict-Transport-Security` (HSTS) bảo trình duyệt chỉ dùng HTTPS cho domain — chỉ gửi khi đã phục vụ qua HTTPS (lời giải bài 2).

### 3.7. Secret và cấu hình

`SecurityConfig.load` **không có giá trị mặc định cho `JWT_SECRET`**: thiếu → ứng dụng in lỗi và thoát mã 2 (mục 9). Một secret mặc định trong code
sớm muộn sẽ chạy trên production.

`Secret` là một lớp bọc với `toString = "Secret(****)"` — `println(config)` ở mục 9 in `SecurityConfig(Secret(****),...)`. Muốn lấy giá trị phải gọi
`reveal`, nên tìm được mọi chỗ dùng secret bằng một lệnh `grep`.

**Ở production:** secret đến từ secret manager (Vault, AWS/GCP Secret Manager, Kubernetes Secret) qua biến môi trường hoặc file; xoay vòng (rotate) định kỳ;
không commit vào git (kể cả file `.env`); mỗi môi trường một secret khác.

### 3.8. Log, request id và lỗi 500 an toàn

- `RequestId.httpApp` gắn `X-Request-ID` (sinh UUID nếu client không gửi) vào request **và** response. Người dùng báo lỗi kèm mã này → tìm đúng dòng log.
- `Logger.httpApp(logHeaders = true, logBody = false)` của http4s **mặc định che** `Authorization`, `Cookie`, `Set-Cookie`. Dòng log thật:
  `... GET /api/v1/me Headers(..., Authorization: <REDACTED>, X-Request-ID: 375be43b-...)`. Test `log request che header Authorization` khóa hành vi này.
- `Middleware.errorHandler`: exception chưa xử lý → log **đầy đủ** ở server (kèm stack trace và request id), trả client Problem 500 **không** có
  thông điệp exception. Test ném `IllegalStateException("bí mật nội bộ: password=hunter2")` và kiểm tra response không chứa `hunter2`.
- **Exception trong `serverLogic` của Tapir** mặc định bị Tapir tự bắt và trả `500 Internal server error` dạng text — **không** đi qua
  `errorHandler` (không Problem JSON, không request id, log theo đường khác). Lab đặt `exceptionHandler(None)` trong `Api.serverOptions` để exception
  đi tiếp tới `errorHandler`: **một** chỗ xử lý cho mọi lỗi. Test `exception trong serverLogic CỦA TAPIR...` khóa hành vi này (mục 10.10).

**Error tracking** (Sentry, Rollbar...) là dịch vụ gom exception từ log/SDK, nhóm lại, cảnh báo. Trong lab, điểm tích hợp chính là `errorHandler`:
mọi lỗi chưa xử lý đi qua một chỗ duy nhất.

### 3.9. Health check, metrics

| Endpoint | Ý nghĩa | Hệ thống dùng nó làm gì |
|---|---|---|
| `/health/live` | Tiến trình còn sống, không kẹt | Kubernetes **restart** container nếu fail |
| `/health/ready` | Sẵn sàng phục vụ (database kết nối được...) | Load balancer **ngừng gửi traffic** nếu fail (không restart) |

Database chết → `ready` = 503 `{"database":"DOWN"}` nhưng `live` vẫn 200 (test `readiness:`). Trong lab, "database" là một **cờ mô phỏng** (`dbUp`)
vì lab chưa có database; Project 2 thay bằng truy vấn `select 1` thật có timeout. Nếu `live` cũng kiểm tra database, database chậm một lúc
sẽ làm **mọi** instance bị restart cùng lúc — sự cố lan rộng.

**Metrics** (`/metrics`, định dạng Prometheus): `Metrics[IO](ops)` của http4s đo mọi request (`http_request_count_total`,
`http_response_duration_seconds_*`, `http_active_request_count`) và JVM (`jvm_memory_bytes_used`, GC...). Metric **nghiệp vụ**
`login_failures_total` đếm đăng nhập thất bại — tăng đột biến là dấu hiệu bị dò mật khẩu.

### 3.10. Graceful shutdown

Khi deploy phiên bản mới, nền tảng gửi SIGTERM. Dừng đột ngột → request đang xử lý bị cắt, client nhận lỗi.

`SecureMain`: `IOApp` bắt SIGTERM/SIGINT và **hủy** fiber chính → `Resource` của server được giải phóng: Ember ngừng nhận kết nối mới, chờ request đang chạy
tối đa `withShutdownTimeout(10.seconds)`, rồi đóng metrics registry (thứ tự ngược với lúc tạo).

Test `graceful shutdown` chứng minh bằng server thật trên cổng 0: gửi `/slow` (ngủ 1 giây), sau 300 ms bắt đầu `release` server → request vẫn nhận `"xong"`,
request mới sau đó **thất bại** (không còn ai lắng nghe).

**Phối hợp với readiness:** trước khi tắt, chuyển `ready` sang DOWN vài giây để load balancer ngừng gửi request mới (bài tập mở rộng).

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Hashing / salt / pepper | Băm một chiều / chuỗi ngẫu nhiên riêng mỗi mật khẩu / bí mật chung giữ ngoài DB |
| Argon2id | Hàm băm mật khẩu tốn bộ nhớ, chống GPU; khuyến nghị đầu tiên của OWASP |
| Credential stuffing | Thử hàng loạt cặp email/mật khẩu bị lộ từ trang khác |
| User enumeration | Dò xem email nào đã đăng ký qua khác biệt trong response |
| JWT, claim, `iss`/`sub`/`exp`/`iat` | Token JSON có chữ ký; trường trong payload: người phát hành / chủ thể / hết hạn / thời điểm phát hành |
| HMAC / HS256 | Chữ ký bằng khóa bí mật chung; HMAC với SHA-256 |
| Bearer token | Token "ai cầm cũng dùng được", gửi trong `Authorization: Bearer ...` |
| Authentication / Authorization | Xác thực (bạn là ai) / phân quyền (được làm gì) |
| IDOR, Broken Access Control | Truy cập trực tiếp đối tượng không kiểm tra quyền |
| Rate limiting, token bucket | Giới hạn tần suất; thuật toán xô token |
| CORS, origin, preflight | Chia sẻ tài nguyên khác origin; scheme+host+port; request `OPTIONS` thăm dò |
| TLS, HSTS | Giao thức mã hóa của HTTPS; header buộc trình duyệt dùng HTTPS |
| Secret manager | Dịch vụ lưu và cấp phát bí mật |
| Liveness / readiness probe | Kiểm tra còn sống / sẵn sàng nhận traffic |
| Graceful shutdown | Dừng có trật tự, không cắt request đang chạy |
| Redaction | Che dữ liệu nhạy cảm trong log |

## 5. Ví dụ đơn giản

Băm và kiểm tra một mật khẩu:

```scala
import cats.effect.{IO, IOApp}
import vn.scalaeng.sec.Passwords

object HashDemo extends IOApp.Simple:
  def run: IO[Unit] =
    for
      h  <- Passwords.hash("mot cau dai de nho 2026")
      ok <- Passwords.verify("mot cau dai de nho 2026", h)
      no <- Passwords.verify("mot cau dai de nho 2027", h)
      _  <- IO.println(s"$h\nđúng=$ok sai=$no")
    yield ()
```

Chạy hai lần sẽ in hai chuỗi hash khác nhau (salt mới), nhưng `đúng=true sai=false` cả hai lần.

## 6. Ví dụ thực tế — luồng đăng nhập hoàn chỉnh

```
client ──POST /auth/register──▶ policy mật khẩu ─▶ Argon2id (blocking pool) ─▶ lưu HASH
client ──POST /auth/login────▶ rate limit theo IP ─▶ tìm user ─▶ verify (hoặc verify hash giả) ─▶ JWT 15 phút
client ──GET /me + Bearer────▶ CORS ─▶ securityIn: verify chữ ký/alg/exp/iss ─▶ AuthUser ─▶ logic
                                └── mọi request: RequestId ─▶ Logger (che Authorization) ─▶ errorHandler ─▶ Metrics
```

`AppWiring.build` ghép tất cả thành một `Resource[IO, Built]` — test dùng **đúng** bản ghép đó (`ApiSuite`, `ProductionSuite`), nên test kiểm tra cả thứ tự middleware.

## 7. Code đầy đủ

```
05-backend/security-lab/
├── project.scala / build.sbt / project/build.properties
├── src/main/resources/logback.xml
├── src/main/scala/vn/scalaeng/sec/
│   ├── Config.scala       ← Secret (che toString), SecurityConfig.load (không mặc định cho secret)
│   ├── Passwords.scala    ← Argon2id + chính sách mật khẩu
│   ├── Tokens.scala       ← Role (codec chuỗi), JWT phát hành/xác minh
│   ├── Users.scala        ← UserStore (chỉ lưu hash), AuthService (chống dò email)
│   ├── RateLimiter.scala  ← token bucket (dùng TRONG endpoint login)
│   ├── Api.scala          ← endpoint Tapir: register, login (rate limit + metric), me, admin/users, users/{id};
│   │                        securityIn: xác minh JWT + kiểm tra thu hồi theo jti; decode failure/exception → Problem
│   ├── Production.scala   ← Health, errorHandler, CORS, logging, AppWiring (Resource)
│   └── SecureMain.scala   ← IOApp, graceful shutdown
└── src/test/scala/vn/scalaeng/sec/
    ├── Fixtures.scala, CryptoSuite.scala (11), ApiSuite.scala (7), ProductionSuite.scala (10)
```

```bash
cd 05-backend/security-lab
scala-cli test .                                   # hoặc: sbt test
JWT_SECRET=day-la-secret-hoc-tap-dai-hon-32-ky-tu CORS_ORIGINS=https://shop.scalaeng.vn \
  scala-cli run . --main-class vn.scalaeng.sec.SecureMain
```

PowerShell: `$env:JWT_SECRET = "..."; scala-cli run . --main-class vn.scalaeng.sec.SecureMain`.
Khi chạy, `SecureMain` tạo sẵn tài khoản `admin@scalaeng.vn` với mật khẩu học tập in trong code — **chỉ để thử nghiệm**.

## 8. Giải thích code

- **`UserStore.register`** băm mật khẩu **trước** `state.modify`: hàm trong `modify` có thể chạy lại khi tranh chấp (Chương 16B), không được chứa việc 44 ms.
- **`Api.secured`** — `serverSecurityLogicPure[AuthUser, IO](token => tokens.verify(token).left.map(...))`: "Pure" vì xác minh JWT không cần I/O.
  Endpoint dựng từ `secured` nhận logic dạng `user => input => IO[Either[Problem, Output]]`.
- **`Api.decodeFailures`** — `DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) => ...)`: hàm `response` nhận **status và header**
  Tapir đã chọn (401 + `WWW-Authenticate: Bearer` khi thiếu token) nên body Problem luôn khớp status. Bài 4 Chương 13 dùng `defaultHandlers`, chỉ nhận `msg`
  — đủ cho lỗi 400 nhưng sai với 401 (mục 10.2).
- **`given Schema[Role] = Schema.derivedEnumeration[Role].defaultStringBased`** — OpenAPI mô tả `role` là enum chuỗi, khớp codec JSON trong `object Role`.
- **`RateLimiter.acquire`** — toàn bộ "đọc bucket → tính token → ghi" nằm trong **một** `modify`: không có lost update giữa hai request đồng thời (bài học Chương 15 mục 3.6, ở mức bộ nhớ).
- **`ApiDeps`** gom những gì `Api` cần: kho user, dịch vụ đăng nhập, `Tokens`, cấu hình, `loginLimiter`, `onLoginFailure` (tăng metric) và
  `isRevoked` (mặc định không thu hồi gì; lời giải bài 3 cắm danh sách thu hồi vào đây). Truyền phụ thuộc tường minh giúp test thay từng phần.
- **`login`** — errorOut là `oneOf[LoginError]`: `Unauthorized(Problem)` → 401, `TooManyAttempts(Problem, retryAfterSeconds)` → 429 với header
  `Retry-After` lấy từ chính giá trị lỗi (`jsonBody[Problem].and(header[Long]("Retry-After")).mapTo[LoginError.TooManyAttempts]`).
- **`secured`** dùng `serverSecurityLogic` (có effect) vì kiểm tra thu hồi là một `IO`.
- **`AppWiring.build`** — thứ tự middleware có ý nghĩa: `RequestId` ngoài cùng (để log và lỗi đều có id) → `Logger` → `errorHandler` → route.

## 9. Output mong đợi (output thật)

**Thiếu secret** — ứng dụng từ chối khởi động:

```
$ scala-cli run . --main-class vn.scalaeng.sec.SecureMain
cấu hình không hợp lệ:
  - thiếu JWT_SECRET
```

**Có secret:**

```
secure-api lắng nghe tại /127.0.0.1:8090 (config: SecurityConfig(Secret(****),scalaeng-orders,15 minutes,List(https://shop.scalaeng.vn),5))
```

**Đăng ký, đăng nhập, phân quyền** (`curl -i`; bỏ `Date`, `Connection`, `Vary`, `X-Request-ID`; body gửi bằng `--data-binary @file.json`):

```
$ POST /api/v1/auth/register  {"email":"an@scalaeng.vn","name":"An","password":"ngan"}
HTTP/1.1 422 Unprocessable Content
{"status":422,"title":"Unprocessable Content","detail":"mật khẩu không đạt yêu cầu","errors":["mật khẩu phải có ít nhất 15 ký tự"]}

$ POST /api/v1/auth/register  {"email":"an@scalaeng.vn","name":"Nguyễn An","password":"mot cau dai de nho 2026"}
HTTP/1.1 201 Created
{"id":2,"email":"an@scalaeng.vn","name":"Nguyễn An","role":"Customer"}

$ POST /api/v1/auth/login → {"accessToken":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzY2FsYWVuZy1vcmRlcnMi...","tokenType":"Bearer","expiresIn":900}
$ payload (giải mã base64url phần 2) → {"iss":"scalaeng-orders","sub":"2","exp":1792174394,"iat":1792173494,"jti":"d30b9ba2-7a02-47e1-a3f4-59d2a49c3fd7","email":"an@scalaeng.vn","role":"Customer"}
$ GET /api/v1/me -H 'Authorization: bearer <token>'   (scheme viết thường) → 200

$ GET /api/v1/me  (không token)
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
{"status":401,"title":"Unauthorized","detail":"Invalid value for: header Authorization (missing)","errors":[]}

$ GET /api/v1/me  -H 'Authorization: Bearer abc.def.ghi'
HTTP/1.1 401 Unauthorized
{"status":401,"title":"Unauthorized","detail":"token không hợp lệ hoặc đã hết hạn","errors":[]}

$ GET /api/v1/admin/users  (token của An, vai trò Customer)
HTTP/1.1 403 Forbidden
{"status":403,"title":"Forbidden","detail":"bạn không có quyền thực hiện thao tác này","errors":[]}

$ GET /api/v1/users/1  (token của An — hồ sơ của Admin)
403
```

**Đăng nhập sai và rate limit** (giới hạn mặc định 5 lần/phút/IP):

```
$ login sai mật khẩu        → {"status":401,"title":"Unauthorized","detail":"email hoặc mật khẩu không đúng","errors":[]}
$ login email không tồn tại → {"status":401,"title":"Unauthorized","detail":"email hoặc mật khẩu không đúng","errors":[]}
$ login sai lần 2, 3, 4, 5  → 401 401 401 401
$ login sai lần 6
HTTP/1.1 429 Too Many Requests
Retry-After: 12
Content-Type: application/json

{"status":429,"title":"Too Many Requests","detail":"thử lại sau ít phút","errors":[]}

$ login lần 7 qua biến thể path /api/v1/auth/login/  → 429   (dùng chung quota — xem 10.7)
```

**Vận hành:**

```
$ curl /health/ready   → {"database":"UP"}
$ curl /metrics | grep login_failures_total      # 4 lần sai được đếm; lần bị 429 không kiểm tra mật khẩu nên không tính
login_failures_total 4.0
$ curl -i -X OPTIONS /api/v1/me -H 'Origin: https://shop.scalaeng.vn' -H 'Access-Control-Request-Method: GET'
HTTP/1.1 200 OK
Access-Control-Allow-Origin: https://shop.scalaeng.vn
Access-Control-Allow-Methods: GET, POST
Access-Control-Allow-Headers: Authorization, Content-Type
Access-Control-Max-Age: 600
```

**Log server** (một dòng; `Authorization` bị che):

```
00:20:04.267 INFO  org.http4s.server.middleware.Logger - HTTP/1.1 GET /api/v1/me Headers(Host: 127.0.0.1:8090, User-Agent: curl/8.22.0, Accept: */*, Authorization: <REDACTED>, X-Request-ID: 375be43b-4f8a-4319-a914-4085ec1a5968)
```

**Lỗi 500** — log server có đủ chi tiết, client thì không:

```
00:18:24.469 ERROR vn.scalaeng.sec.errors - lỗi không mong đợi request_id=a16d69bf-f8f6-4029-ba37-97bd18985018 GET /boom
java.lang.IllegalStateException: bí mật nội bộ: password=hunter2
```

**Test:** `CryptoSuite 11/11`, `ApiSuite 7/7`, `ProductionSuite 10/10`; `sbt test`: `Passed: Total 28, Failed 0`.

## 10. Lỗi thường gặp (thật)

**10.1. Enum ra JSON dạng `{"Customer":{}}`.** Bản đầu của lab khai báo `enum Role derives Codec.AsObject`. Response thật:
`{"id":2,"email":"an@scalaeng.vn","name":"Nguyễn An","role":{"Customer":{}}}` — client phải viết `role.Customer != null` thay vì `role == "Customer"`.
Sửa: codec chuỗi tự viết trong `object Role` + `Schema.derivedEnumeration[Role].defaultStringBased` cho OpenAPI. Test kiểm tra `role == "Customer"`.

**10.2. Status trong body không khớp status HTTP.** Bản đầu dùng `defaultHandlers(msg => ...Problem(400, ...))` (như bài 4 Chương 13). Thiếu token cho:

```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
{"status":400,"title":"Bad Request","detail":"Invalid value for: header Authorization (missing)","errors":[]}
```

`defaultHandlers` chỉ nhận thông điệp, không biết Tapir đã chọn 401. Sửa: `DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) => ...)` (mục 8).

**10.3. Trùng tên với DSL của thư viện.** Đặt tham số constructor là `auth`:

```
[error] .\src/main/scala/vn/scalaeng/sec/Api.scala:34:19
[error] value bearer is not a member of vn.scalaeng.sec.AuthService
[error]       .securityIn(auth.bearer[String]())
[error]                   ^^^^^^^^^^^
```

Tham số `auth` che mất `sttp.tapir.auth` đã import bằng `sttp.tapir.*`. Sửa: đổi tên tham số (`authService`).

**10.4. `.as[String]` không đọc được body thuần văn bản.** Đọc `/metrics` bằng `response.as[String]` trong file có `import org.http4s.circe.CirceEntityCodec.*`:

```
Caused by: org.typelevel.jawn.ParseException: expected json value got '# HELP...' (line 1, column 1)
```

Import đó cung cấp decoder cho **mọi** kiểu có `io.circe.Decoder` — kể cả `String` — nên body bị parse như JSON. Sửa: `response.bodyText.compile.string`.

**10.5. Header có mặt nhưng `headers.get[T]` trả `None`.** Response có `WWW-Authenticate: Bearer`, nhưng `headers.get[`WWW-Authenticate`]` (bản typed) là `None`
vì parser của http4s không chấp nhận challenge `Bearer` không có tham số. Khi kiểm tra header trong test, tra theo **tên**: `headers.get(CIString("WWW-Authenticate"))`.

**10.6. Metric đếm sai thứ cần đếm.** Bản đầu tăng `login_failures_total` cho **mọi** response 401. Sau phiên curl ở mục 9 (4 lần đăng nhập sai
và 1 lần gọi `/me` thiếu token), `/metrics` báo `login_failures_total 5.0` — thừa 1. Không có exception nào báo lỗi này; chỉ đối chiếu số liệu với
hành động thật mới thấy. Sửa: chỉ đếm 401 của `POST /api/v1/auth/login`; test gọi thêm `/me` thiếu token và vẫn đòi `1.0`.

**Các lỗi 10.7–10.11 do review chéo của Codex phát hiện.** Test của bài đều pass, nhưng không test nào thử **tấn công** đúng chỗ đó.
Mỗi lỗi được tái hiện bằng một chương trình thăm dò (probe) trên chính code của lab, rồi mới sửa và thêm regression test.

**10.7. Biến thể path vượt qua rate limit.** Middleware so khớp chuỗi path; router của Tapir khớp rộng hơn. Probe (capacity = 2, ba request mỗi path):

```
PROBE login /api/v1/auth/login statuses=List(401, 401, 429)
PROBE login /api/v1/auth/login/ statuses=List(401, 401, 401)
PROBE login /api/v1/auth/%6Cogin statuses=List(401, 401, 401)
```

Sửa: rate limit và metric nằm trong logic endpoint (mục 3.4). Test `biến thể path của login ... DÙNG CHUNG quota và metric`.

**10.8. `bearer` viết thường vượt qua danh sách thu hồi.** Bản đầu của lời giải bài 3 tự parse header bằng `startsWith("Bearer ")`, còn Tapir chấp nhận
scheme không phân biệt hoa thường (đúng RFC 9110). Token đã thu hồi gửi với `bearer` hay `bEaReR` vẫn vào được (`HTTP=200`). Sửa: chỉ dùng **một**
parser (của Tapir) và kiểm tra thu hồi ngay trong `serverSecurityLogic`.

**10.9. Chữ ký viết khác, cùng giá trị.** Bản đầu băm **chuỗi** token để thu hồi. Đổi ký tự base64url cuối của chữ ký sang ký tự tương đương
(chỉ khác 2 bit thừa) → chữ ký vẫn đúng nhưng chuỗi đã khác → không có trong danh sách thu hồi:

```
PROBE revoked equivalent signature HTTP=200
PROBE equivalent signature accepted=true
```

Sửa: mỗi token có `jti` (UUID) nằm trong payload **được ký**; thu hồi theo `jti` sau khi đã xác minh chữ ký. Lời giải bài 3 thử cả 4 biến thể.

**10.10. Exception trong Tapir không đi qua `errorHandler`.** Probe: endpoint Tapir ném exception → `HTTP=500; BODY=Internal server error`
(text, không request id). Sửa: `exceptionHandler(None)` (mục 3.8).

**10.11. Token không có `exp` vẫn được chấp nhận.** Probe: JWT ký đúng secret, đủ issuer/subject nhưng không có `exp` →
`Right(AuthUser(1,an@x.vn,Customer))`. Sửa: `verifyClaims` đòi `exp` và `jti` (mục 3.2).

Bài học chung: **test chứng minh code làm điều ta nghĩ tới; review chéo tìm điều ta không nghĩ tới.** Với code bảo mật, hãy viết test từ góc nhìn
kẻ tấn công: biến thể chữ hoa/thường, encoding, path tương đương, claim bị thiếu.

**10.12. API deprecated của fs2.** Lời giải bài 5 dùng `fromKeyStoreFile(java.nio.file.Path, ...)`:

```
[warn] method fromKeyStoreFile in trait BuilderPlatform is deprecated since 3.13.0: Use overload that takes an fs2.io.file.Path instead
```

Sửa: truyền `fs2.io.file.Path.fromNioPath(keystore)`.

## 11. Cách debug

- **401 không rõ lý do:** giải mã payload JWT (phần 2, base64url) để xem `exp`, `iss`; so `exp` với `date +%s`. Kiểm tra secret giữa nơi phát hành và nơi kiểm tra.
  Trả lý do cụ thể (`JwtExpirationException`) vào **log** server, không trả cho client.
- **403 bất ngờ:** log `AuthUser` (id, role — không log token) ở logic phân quyền.
- **CORS lỗi trên trình duyệt:** xem tab Network, request `OPTIONS` preflight: có `Access-Control-Allow-Origin` đúng origin không? Origin gồm cả scheme và cổng.
- **429 quá sớm:** nhiều client sau cùng một NAT/proxy chung một IP — xem khóa rate limit đang dùng.
- **Không thấy log request:** kiểm tra `logback.xml` (logger `org.http4s.server.middleware.Logger` mức INFO).
- **Graceful shutdown không hoạt động:** tiến trình bị kill bằng SIGKILL (`kill -9`, `Stop-Process -Force`) thì không có gì chạy được — chỉ SIGTERM/SIGINT mới cho
  `Resource` cơ hội đóng.

## 12. Bài tập

1. **(Cơ bản)** Theo RFC 6750, token sai/hết hạn nên kèm `WWW-Authenticate: Bearer error="invalid_token"`. Lab hiện chỉ có header này khi **thiếu** token.
   Viết middleware bổ sung, không sửa `Api.scala`.
2. **(Cơ bản)** Middleware thêm header bảo mật cho API JSON: `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`, `Referrer-Policy: no-referrer`,
   và HSTS **chỉ khi** chạy HTTPS.
3. **(Trung bình)** Đăng xuất: thu hồi một access token trước khi hết hạn. Token đã thu hồi phải bị từ chối với **mọi** cách gửi mà tầng xác thực
   chấp nhận (scheme hoa/thường, cách viết base64url khác của cùng chữ ký).
4. **(Trung bình)** Chống credential stuffing: giới hạn số lần đăng nhập **sai** theo email (dù đến từ nhiều IP). Đăng nhập đúng không bị đếm;
   email khác không bị ảnh hưởng; nhiều lần sai **đồng thời** không được vượt quota.
5. **(Nâng cao)** Chạy API qua **HTTPS** thật: sinh keystore bằng `keytool`, bật TLS cho Ember, viết test gọi bằng `https://`.
6. **(Nâng cao — thiết kế)** Một web app (frontend cùng domain) và một app mobile dùng chung API. Chọn session cookie hay JWT (hoặc cả hai) cho từng client.
   Trình bày: thu hồi, CSRF, XSS, lưu trữ phía client, mở rộng nhiều instance.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch17-solutions.md`](../solutions/ch17-solutions.md)
- Code + test (bài 1–5, 7 test, gồm HTTPS thật và thử 4 biến thể token đã thu hồi): [`solutions/ch17-code/`](../solutions/ch17-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao "băm chậm" lại là ưu điểm? Salt giải quyết tấn công nào?
2. Payload JWT có bí mật không? Chữ ký chứng minh điều gì, không chứng minh điều gì?
3. Kể 5 kiểm tra bắt buộc khi xác minh JWT và tấn công tương ứng. Vì sao phải tự đòi `exp`?
4. 401 và 403 khác nhau thế nào? IDOR là gì, chặn ở đâu?
5. Vì sao đăng nhập sai email và sai mật khẩu phải cho cùng response? Vì sao vẫn chạy `verify` khi email không tồn tại?
6. CORS có bảo vệ API khỏi `curl` không?
7. Vì sao liveness không nên kiểm tra database?
8. Graceful shutdown cần điều kiện gì từ phía nền tảng (tín hiệu nào)?

<details><summary>Gợi ý đáp án</summary>

1. Đoán thử hàng loạt trở nên đắt (thời gian + RAM). Salt chống bảng tra sẵn và chống nhận ra mật khẩu trùng giữa các tài khoản.
2. Không — chỉ base64url. Chữ ký chứng minh token do người giữ secret phát hành và nguyên vẹn; không chứng minh người gửi là chủ token (bearer), không chứng minh token chưa bị thu hồi.
3. Thuật toán cố định (chống `alg: none`/đổi thuật toán), chữ ký (chống sửa payload), `exp` có mặt và còn hạn (chống token sống mãi), `iss`/`aud` (chống token của hệ thống khác), `jti` (để thu hồi được). Thư viện chỉ kiểm tra `exp` nếu có — RFC 7519 cho phép thiếu, nên chính sách ngắn hạn phải do ứng dụng áp đặt.
4. 401: chưa xác thực được; 403: đã biết bạn là ai nhưng không được phép. IDOR: truy cập đối tượng theo id mà không kiểm tra quyền sở hữu — chặn ở logic từng endpoint (hoặc tầng service) với `AuthUser`.
5. Chống dò email (user enumeration). Không verify thì nhánh "email không tồn tại" nhanh hơn ~44 ms — đo được.
6. Không. CORS chỉ là chính sách của trình duyệt.
7. Database chậm/chết sẽ khiến nền tảng restart mọi instance cùng lúc; restart không sửa được database.
8. SIGTERM (hoặc SIGINT) và đủ thời gian chờ trước SIGKILL (`terminationGracePeriodSeconds` của Kubernetes lớn hơn `shutdownTimeout`).
</details>

## 15. Mini project

**"Khóa tài khoản và cảnh báo"**: thêm vào lab
- Khóa tài khoản 15 phút sau 10 lần sai liên tiếp (lưu theo user, reset khi đăng nhập đúng); response vẫn không lộ việc tài khoản bị khóa với người không biết mật khẩu.
- Metric `account_lockouts_total` và log cảnh báo kèm request id (không log email đầy đủ — che `a***@scalaeng.vn`).
- Endpoint `POST /api/v1/auth/password` đổi mật khẩu: yêu cầu mật khẩu cũ, kiểm tra policy, thu hồi mọi token cũ của user.
- Test cho từng quy tắc, dùng `TestControl` cho thời gian khóa.

## 16. Nguồn tài liệu chính thức

- OWASP Password Storage Cheat Sheet (tham số Argon2id): https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP JSON Web Token Cheat Sheet (Java): https://cheatsheetseries.owasp.org/cheatsheets/JSON_Web_Token_for_Java_Cheat_Sheet.html
- OWASP Top 10 (2021) — A01 Broken Access Control: https://owasp.org/Top10/A01_2021-Broken_Access_Control/
- NIST SP 800-63B-4 (Digital Identity Guidelines — mật khẩu, bản hiện hành): https://pages.nist.gov/800-63-4/sp800-63b.html
- RFC 7519 (JWT), RFC 8725 (JWT Best Current Practices), RFC 6750 (Bearer Token Usage): https://www.rfc-editor.org/
- MDN — Cross-Origin Resource Sharing: https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
- http4s middleware (CORS, Logger, RequestId, Metrics): https://http4s.org/
- Tapir security inputs: https://tapir.softwaremill.com/en/latest/endpoint/security.html
- password4j: https://github.com/Password4j/password4j · jwt-scala: https://jwt-scala.github.io/jwt-scala/
- Prometheus exposition format: https://prometheus.io/docs/instrumenting/exposition_formats/
- Kubernetes probes: https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/

## 17. Checklist tự đánh giá

- [ ] Giải thích được vì sao Argon2id thay vì SHA-256, có số đo cụ thể.
- [ ] Phát hành/xác minh JWT; nêu đủ 4 kiểm tra và giới hạn "không thu hồi được".
- [ ] Phân biệt 401/403; tìm được lỗ hổng IDOR trong code review.
- [ ] Viết được đăng nhập không lộ email và có rate limit; biết giới hạn của rate limit trong bộ nhớ.
- [ ] Cấu hình CORS đúng và giải thích CORS không phải xác thực.
- [ ] Không để secret có giá trị mặc định hay lộ qua log.
- [ ] Đọc được log có request id; lỗi 500 không lộ chi tiết nội bộ.
- [ ] Phân biệt liveness/readiness; đọc `/metrics`.
- [ ] Giải thích và tự kiểm chứng graceful shutdown.
- [ ] Chạy được 28 test của lab và 7 test lời giải (gồm HTTPS).
- [ ] Viết được ít nhất một test từ góc nhìn kẻ tấn công cho mỗi cơ chế bảo vệ (mục 10.7–10.11).
