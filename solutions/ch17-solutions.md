# Lời giải — Chương 17: Backend Security và Production

> Code: [`ch17-code/`](ch17-code/) — dùng lại mã nguồn `05-backend/security-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch17-code && scala-cli test .` → **7/7 test pass** (17/10/2026). Bài 5 cần `keytool` (có sẵn trong JDK) trong PATH.

## Bài 1 — `WWW-Authenticate: Bearer error="invalid_token"`

```scala
object InvalidTokenChallenge:
  def apply(routes: HttpRoutes[IO]): HttpRoutes[IO] = Kleisli { (req: Request[IO]) =>
    routes(req).map { resp =>
      val hadToken = req.headers.get(CIString("Authorization")).isDefined
      if resp.status == Status.Unauthorized && hadToken then
        resp.putHeaders(Header.Raw(CIString("WWW-Authenticate"), "Bearer error=\"invalid_token\""))
      else resp
    }
  }
```

- RFC 6750: thiếu token → `WWW-Authenticate: Bearer` (không có mã lỗi); token sai/hết hạn → `error="invalid_token"`. Client (mobile app) dựa vào đó để
  **làm mới token** thay vì hỏi lại mật khẩu.
- Middleware đứng **ngoài** route Tapir, không sửa `Api.scala`: route là giá trị ghép được (Chương 13).
- `putHeaders` **thay** header cùng tên đã có (Tapir đã đặt `Bearer` cho trường hợp thiếu token, nhưng nhánh đó không có `Authorization` nên không bị đổi).
- Test kiểm tra cả hai nhánh.

## Bài 2 — Header bảo mật

```scala
resp.putHeaders(
  Header.Raw(CIString("X-Content-Type-Options"), "nosniff"),   // trình duyệt không "đoán" kiểu nội dung
  Header.Raw(CIString("Cache-Control"), "no-store"),           // response chứa dữ liệu cá nhân không được cache
  Header.Raw(CIString("Referrer-Policy"), "no-referrer")
)
// HSTS chỉ khi httpsOnly = true
```

- HSTS qua HTTP thuần không có tác dụng (trình duyệt bỏ qua) và nếu đặt sai có thể khóa người dùng khỏi domain đến hết `max-age` khi HTTPS hỏng —
  vì vậy bật có điều kiện.
- Header như `Content-Security-Policy` chủ yếu cho trang HTML; API JSON thuần không cần CSP phức tạp.

## Bài 3 — Đăng xuất: thu hồi access token theo `jti`

```scala
final class Revocation private (revoked: Ref[IO, Map[String, Instant]]):
  def revoke(token: VerifiedToken): IO[Unit] =
    IO.realTimeInstant.flatMap(now => revoked.update(m => m.filter((_, exp) => exp.isAfter(now)).updated(token.tokenId, token.expiresAt)))
  def isRevoked(token: VerifiedToken): IO[Boolean] = revoked.get.map(_.contains(token.tokenId))

// Cắm vào tầng xác thực của lab — KHÔNG viết middleware riêng:
AppWiring.build(config, clock, isRevoked = revocation.isRevoked)
```

- Lab phát hành mỗi token với `jti` (UUID) trong payload **được ký**; `Tokens.verifyClaims` trả `VerifiedToken(user, tokenId, expiresAt)`.
- `isRevoked` chạy **trong** `serverSecurityLogic`, sau khi chữ ký đã được xác minh. Tapir đã parse header `Authorization` (scheme không phân biệt hoa
  thường) — chỉ có **một** parser.
- Bản ghi chỉ cần sống đến `expiresAt` của token; `revoke` dọn bản ghi đã quá hạn mỗi lần ghi.
- Test thu hồi rồi thử **4 cách gửi**: `Bearer`, `bearer`, `bEaReR`, và chữ ký viết khác (đổi ký tự base64url cuối sang ký tự tương đương — test
  kiểm tra trước rằng biến thể đó **vẫn** được `Tokens.verify` chấp nhận). Cả 4 đều phải nhận 401.

**Vì sao bản đầu sai** (review chéo của Codex tìm ra, xem bài học mục 10.8, 10.9): bản đầu băm SHA-256 của **chuỗi** token và chặn bằng middleware tự
parse `startsWith("Bearer ")`. (1) `bearer` viết thường không bị middleware nhận ra nhưng Tapir vẫn chấp nhận → vào được. (2) Một chữ ký có nhiều cách
viết base64url → chuỗi khác → băm khác → không có trong danh sách. Bài học: **so sánh theo giá trị đã được xác minh (`jti`), không theo cách biểu diễn**,
và đừng parse lại cái mà tầng xác thực đã parse.

**Giới hạn:** danh sách trong bộ nhớ chỉ đúng với MỘT instance. Nhiều instance → lưu `jti` ở Redis/PostgreSQL (có index, có hạn dùng), hoặc
"token version" theo user (tăng version khi đổi mật khẩu → mọi token cũ vô hiệu).

## Bài 4 — Giới hạn số lần đăng nhập SAI theo email

```scala
def guard(email: String)(attempt: IO[Option[String]]): IO[GuardedResult] =
  // 1. Giữ chỗ NGUYÊN TỬ: chỉ cho thử nếu (số lần sai trong cửa sổ + số đang thử) < max
  // 2. Chạy kiểm tra mật khẩu
  // 3. Đúng → xóa bộ đếm của email; Sai → đổi suất giữ chỗ thành một lần sai
  // onError / onCancel → trả suất giữ chỗ
```

- **Chỉ lần sai bị đếm.** Bản đầu tiêu token của `RateLimiter` **trước** khi kiểm tra mật khẩu, nên 3 lần đăng nhập ĐÚNG liên tiếp cũng bị khóa —
  không đúng đề bài (review chéo, M3). Test mới: 5 lần đúng liên tiếp đều nhận token.
- **Đúng thì xóa bộ đếm**: 2 lần sai → 1 lần đúng → bộ đếm về 0. Đây là một **lựa chọn chính sách** (dễ chịu cho người dùng gõ nhầm); hệ thống nhạy cảm
  có thể chỉ giảm dần.
- **Đồng thời:** nếu chỉ "kiểm tra số lần sai rồi tăng", 10 request sai cùng lúc đều thấy 0 và đều được thử. Giữ chỗ trong **một** `Ref.modify`
  (giống `SELECT ... FOR UPDATE` ở Chương 15) chặn điều đó. Test 10 request sai song song: đúng **3** được thử, **7** nhận `TooManyAttempts`.
- **Cửa sổ thời gian:** lần sai cũ hơn 15 phút bị bỏ khỏi bộ đếm. Test dùng `TestControl` đợi 15 phút ảo.
- **Email chuẩn hóa một chỗ:** `Email.canonical` (trim + lowercase `Locale.ROOT`) — cùng hàm với đăng ký/đăng nhập của lab, nên `"AN@scalaeng.vn "`
  vừa bị đếm chung quota vừa đăng nhập được bằng đúng mật khẩu (review chéo, m1).
- Đặt ở **tầng service** vì đã có email; kết hợp với giới hạn theo IP của lab: IP chặn một máy dò nhiều tài khoản, email chặn nhiều máy dò một tài khoản.
- Đánh đổi: kẻ xấu có thể cố ý đăng nhập sai để khóa tạm tài khoản nạn nhân (DoS). Giảm thiểu: khóa mềm (CAPTCHA, tăng dần thời gian chờ),
  thông báo cho chủ tài khoản.

## Bài 5 — HTTPS thật

```scala
// Test sinh keystore tự ký (EC P-256, SAN localhost + 127.0.0.1, hạn 2 ngày):
keytool -genkeypair -alias server -keyalg EC -groupname secp256r1 -dname CN=localhost \
  -ext SAN=dns:localhost,ip:127.0.0.1 -validity 2 -storetype PKCS12 -keystore server.p12 -storepass ... -keypass ...

Network[IO].tlsContext.fromKeyStoreFile(Path.fromNioPath(keystore), password, password)   // TLSContext
EmberServerBuilder...withTLS(tls)          // server phục vụ HTTPS
EmberClientBuilder...withTLSContext(tls)   // client tin chứng chỉ trong cùng keystore
```

- Client **phải** xác minh chứng chỉ. Test dùng keystore của server làm nguồn tin cậy cho client — thay cho việc tắt kiểm tra (`curl -k` hay "trust all" là lỗ hổng
  nếu lọt vào code production).
- `SAN=dns:localhost` cần thiết: client kiểm tra tên máy (`localhost`) khớp chứng chỉ.
- Production: chứng chỉ từ CA (Let's Encrypt hoặc CA nội bộ), tự động gia hạn; thường kết thúc TLS ở load balancer.
- Lỗi thật gặp khi viết: `fromKeyStoreFile(java.nio.file.Path, ...)` deprecated từ fs2 3.13.0 → dùng `fs2.io.file.Path` (bài học mục 10.7).

## Bài 6 — Session cookie hay JWT? (lời giải mẫu)

| Tiêu chí | Web app cùng domain: **session cookie** | Mobile app: **access token ngắn + refresh token** |
|---|---|---|
| Lưu ở client | Cookie `HttpOnly; Secure; SameSite=Lax` — JavaScript không đọc được (giảm thiệt hại XSS) | Keychain/Keystore của hệ điều hành |
| Thu hồi | Xóa session ở server — tức thì | Access token sống 5–15 phút; refresh token lưu ở server, thu hồi được |
| CSRF | Có rủi ro vì trình duyệt tự gửi cookie → `SameSite` + CSRF token cho thao tác ghi | Không áp dụng (token gửi tường minh trong header) |
| Nhiều instance | Kho session dùng chung (Redis/PostgreSQL) | Access token stateless; refresh token cần kho chung |

**Kết luận mẫu:** dùng cả hai — cùng tầng xác thực (một `AuthUser` cho logic phân quyền), hai cơ chế đưa danh tính vào request. Không dùng một JWT sống dài
cho mọi client chỉ vì "stateless tiện hơn": mất khả năng thu hồi là cái giá lớn khi tài khoản bị chiếm.
