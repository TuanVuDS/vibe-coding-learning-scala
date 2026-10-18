# Review chéo T05 — Chương 17 (Codex)

Ngày review: **17/10/2026**. Đã đọc handoff, toàn bộ bài học, nguồn/build/logback, ba suite lab, lời giải Markdown, nguồn/test lời giải. Chỉ ghi review này và dữ liệu kiểm chứng trong `.cache/`.

**Kết luận: cần sửa 2 BLOCKER, 4 MAJOR, 4 MINOR. Bộ test hiện có pass 28/28, nhưng chưa bao phủ các đường vượt qua bảo vệ bên dưới.** BLOCKER được dùng cho hai cách tái sử dụng token đã thu hồi, không cần biết secret.

## BLOCKER

### B1 — Đổi hoa/thường của Bearer bỏ qua thu hồi token

- Vị trí: `solutions/ch17-code/src/main/scala/vn/scalaeng/solutions/ch17/Ch17Solutions.scala:58` và dòng 59.
- Middleware chỉ nhận chuỗi bắt đầu đúng `Bearer `; Tapir chấp nhận auth scheme không phân biệt hoa/thường theo HTTP. Vì vậy token đã thu hồi vẫn truy cập `/me` khi client gửi `bearer` hoặc `bEaReR`. Đã chạy trên chính `Revocation.middleware(api.routes)`:

  ```text
  PROBE revoked token scheme=Bearer HTTP=401
  PROBE revoked token scheme=bearer HTTP=200
  PROBE revoked token scheme=bEaReR HTTP=200
  ```

- Đề xuất: parse Authorization bằng cùng quy tắc với tầng xác thực, so scheme không phân biệt hoa/thường, xử lý whitespace/duplicate header nhất quán. Tốt hơn, kiểm tra thu hồi ngay trong security logic sau khi xác minh token, dùng một parser duy nhất. Thêm regression test các dạng scheme và khoảng trắng; giữ token nguyên vẹn, không lowercase token.
- Nguồn: [RFC 9110 §11.1 — Authentication Scheme](https://www.rfc-editor.org/rfc/rfc9110.html#section-11.1), kiểm tra 17/10/2026.

### B2 — Hash chuỗi token thô không nhận ra chữ ký JWT có cách biểu diễn tương đương

- Vị trí: `solutions/ch17-code/src/main/scala/vn/scalaeng/solutions/ch17/Ch17Solutions.scala:47`, các lần dùng digest tại dòng 51 và 54.
- jwt-circe 11.0.4 chấp nhận những chuỗi base64url chữ ký khác nhau nhưng giải mã thành cùng byte chữ ký (khác các bit dư ở ký tự cuối). Đổi ký tự cuối tương ứng không đổi header/payload hay chữ ký thực, nên token vẫn xác minh đúng; SHA-256 của **chuỗi** lại khác và không nằm trong denylist. Lỗi này vẫn tồn tại sau khi sửa scheme của B1.
- Đã sinh token bằng `Tokens.issue`, revoke token gốc, thử biến thể ký tự cuối được `Tokens.verify` chấp nhận rồi gọi `/me` với **Bearer đúng hoa/thường**:

  ```text
  PROBE revoked equivalent signature HTTP=200
  PROBE equivalent signature accepted=true
  ```

- Đề xuất ưu tiên: phát hành `jti` ngẫu nhiên duy nhất, xác minh chữ ký/claim rồi tra denylist theo `(issuer, jti)` đến exp. Nếu giữ digest token, phải có quy tắc canonical encoding được enforce trước khi chấp nhận/revoke, hoặc digest một biểu diễn chuẩn được xác định chặt chẽ. Thêm test revoke gốc rồi thử mọi biến thể chữ ký tương đương mà verifier có thể nhận; tất cả phải bị từ chối. Không khắc phục bằng cách hash mật khẩu chậm hơn hay đổi SHA-256 sang thuật toán hash khác.
- Nguồn nền tảng: [RFC 4648 §3.5 — Canonical Encoding](https://www.rfc-editor.org/rfc/rfc4648.html#section-3.5), [RFC 7515 §2 — base64url/JWS](https://www.rfc-editor.org/rfc/rfc7515.html#section-2). Hành vi thư viện được xác nhận bằng runtime đúng phiên bản lab, 17/10/2026.

## MAJOR

### M1 — Path tương đương vượt qua rate limit đăng nhập và bộ đếm thất bại

- Vị trí: `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:83`, dòng 89 và 92.
- `isLogin` so `req.uri.path.renderString` với đúng một chuỗi. Tapir vẫn phục vụ endpoint login khi có slash cuối hoặc percent-encoding của ký tự path. Những request đó không bị limit và cũng không tăng `login_failures_total`.
- Probe đặt capacity=2, gửi lần lượt ba request cho mỗi path, cùng app/cùng khóa IP, thu được:

  ```text
  PROBE login /api/v1/auth/login statuses=List(401, 401, 429)
  PROBE login /api/v1/auth/login/ statuses=List(401, 401, 401)
  PROBE login /api/v1/auth/%6Cogin statuses=List(401, 401, 401)
  ```

- Đề xuất: gắn limit và metric với **endpoint login đã được router nhận diện**, chẳng hạn interceptor/logic endpoint với client IP được trích rõ; hoặc dùng chung chuẩn hóa/matching path với router, hay từ chối các biến thể một cách nhất quán. Thêm test những URL tương đương và xác nhận quota dùng chung, metric vẫn đếm. Đây là bỏ qua bảo vệ thực tế, khác với giới hạn nhiều instance/rotating IP mà bài đã khai báo.

### M2 — Verify kiểm tra exp nếu có, chưa bắt buộc access token phải có exp

- Vị trí: `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Tokens.scala:41` và dòng 45; khẳng định trong bài `05-backend/ch17-security-production.md:100`.
- `jwt.decode` mặc định chấp nhận claim không có expiration; code chỉ bổ sung issuer/subject/private claims. Đã ký một JWT thử bằng secret của fixture reviewer, có issuer/subject/email/role hợp lệ, **không có exp**, nhận:

  ```text
  PROBE signed JWT without exp = Right(AuthUser(1,an@x.vn,Customer))
  ```

- Không có bằng chứng attacker ký được token mới khi chưa có secret. Vấn đề là verifier không enforce chính sách access token ngắn hạn mà bài hứa, dễ nhận nhầm token từ đường phát hành khác hoặc token cũ. RFC chung cho phép exp tùy chọn, nên ứng dụng phải tự yêu cầu theo profile của mình.
- Đề xuất: yêu cầu `claim.expiration` tồn tại, kiểm tra chưa hết hạn bằng clock đã inject; nếu policy cần giới hạn tuổi thọ, kiểm tra thêm iat/max TTL. Thêm test JWT hợp lệ về chữ ký nhưng thiếu exp; giải thích rõ sự khác nhau giữa kiểm tra claim và yêu cầu claim bắt buộc.
- Nguồn: [RFC 7519 §4.1.4](https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.4), [jwt-scala — Circe](https://jwt-scala.github.io/jwt-scala/jwt-circe.html), kiểm tra 17/10/2026.

### M3 — Lời giải bài 4 đếm cả đăng nhập thành công, chưa đáp ứng đề bài

- Vị trí: `solutions/ch17-code/src/main/scala/vn/scalaeng/solutions/ch17/Ch17Solutions.scala:82`; đề `05-backend/ch17-security-production.md:470`; lời giải thừa nhận giới hạn ở phần cuối bài 4.
- Đề yêu cầu giới hạn **số lần đăng nhập sai theo email**, nhưng `acquire` tiêu token trước khi kiểm tra mật khẩu và không hoàn lại/reset khi thành công. Probe capacity=2, ba lần mật khẩu đúng:

  ```text
  PROBE guarded repeated correct password=List(Token, Token, TooManyAttempts)
  ```

- Việc ghi “bản tốt hơn ... bài tập mở rộng” không hoàn thành yêu cầu ban đầu. Test hiện tại chỉ dùng mật khẩu sai nên không phát hiện khác biệt.
- Đề xuất: cài bộ đếm/quota thất bại theo email với cập nhật an toàn khi đồng thời, tách khỏi quota request theo IP. Nếu chọn quy tắc reset khi thành công, ghi rõ và test. Bổ sung test nhiều lần đúng không làm cạn quota thất bại, email khác độc lập, quota hồi theo thời gian. Tránh cách check rồi increment rời rạc cho phép nhiều request sai đồng thời vượt quota.

### M4 — Exception trong Tapir không đi qua errorHandler chung như bài mô tả

- Vị trí: `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Api.scala:106`, `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:95`; khẳng định `05-backend/ch17-security-production.md:191` và dòng 194–195; test `05-backend/security-lab/src/test/scala/vn/scalaeng/sec/ProductionSuite.scala:84`.
- Tapir có exception handler mặc định, chuyển failed effect thành response 500 trước khi lỗi tới `Middleware.errorHandler`. Api chỉ thay decode failure handler, không thay exception handler. Test hiện tại ném lỗi ở `/boom` là route http4s riêng nên không kiểm tra đường Tapir đang dùng cho nghiệp vụ.
- Reviewer đặt một endpoint Tapir ném exception dưới cùng AppWiring; interpreter giữ default exception handler như Api, nhận:

  ```text
  PROBE exception inside Tapir HTTP=500; BODY=Internal server error
  ```

- Response vẫn giấu exception, nhưng không có Problem JSON/requestId trong body như bài hứa; log đi qua `Http4sDefaultServerLog`, không qua logger/error-tracking hook chung có request id được mô tả. Đây là khác biệt handler, không phải lỗi sandbox.
- Đề xuất: cấu hình Tapir exception handler/server log dùng chung chính sách Problem 500, request id và error tracking; hoặc cấu hình để exception được xử lý ở tầng ngoài đúng chủ ý. Thêm endpoint/test thất bại **trong serverLogic Tapir** và assert body, Content-Type, request id, log an toàn. Nếu giữ hai đường xử lý, sửa bài để mô tả chính xác từng đường.
- Nguồn: [Tapir — Failed effects: unhandled exceptions](https://tapir.softwaremill.com/en/latest/server/errors.html#failed-effects-unhandled-exceptions), kiểm tra 17/10/2026.

## MINOR

### m1 — Chuẩn hóa email cho quota nhưng chưa chuẩn hóa giá trị đem đăng nhập

- Vị trí: `solutions/ch17-code/src/main/scala/vn/scalaeng/solutions/ch17/Ch17Solutions.scala:82` và dòng 85; test `solutions/ch17-code/src/test/scala/vn/scalaeng/solutions/ch17/Ch17SolutionsSuite.scala:73`.
- Quota dùng `email.trim.toLowerCase`, còn `auth.login(email, ...)` nhận nguyên email có khoảng trắng. UserStore chỉ `equalsIgnoreCase`, nên đúng mật khẩu với `"AN@x.vn "` vẫn bị từ chối. Probe xác nhận `Rejected`. Test lời giải dùng mật khẩu sai và email có khoảng trắng nên thực tế đi vào nhánh không có tài khoản, chưa chứng minh hành vi đăng nhập của tài khoản chuẩn hóa.
- Đề xuất: dùng một hàm canonical email thống nhất khi đăng ký, tra user và tính quota; hoặc ghi rõ whitespace bị từ chối và validate nhất quán. Thêm test đúng mật khẩu với các biến thể được tuyên bố tương đương; dùng Locale.ROOT nếu chuyển hoa/thường theo quy tắc cố định.

### m2 — Độ dài chuỗi secret không chứng minh 256 bit entropy

- Vị trí: `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Config.scala:26`; `05-backend/ch17-security-production.md:103`.
- Kiểm tra 32 ký tự chỉ là kiểm tra độ dài, vẫn nhận 32 ký tự lặp hoặc câu dễ đoán. Nó không xác nhận khóa có entropy phù hợp để chống dò offline.
- Đề xuất: hướng dẫn sinh ít nhất 32 byte bằng CSPRNG và định nghĩa rõ env lưu raw/base64/hex, cách decode thành key; diễn đạt giới hạn kiểm tra độ dài. Các secret học tập trong fixture/lệnh minh họa đã được ghi rõ học tập, không coi việc chúng xuất hiện trong repo là lộ secret thực.
- Nguồn: [RFC 8725 §3.5 — Ensure Cryptographic Keys Have Sufficient Entropy](https://www.rfc-editor.org/rfc/rfc8725.html#section-3.5).

### m3 — Readiness trong wiring là mô phỏng database, cần ghi rõ trong bài

- Vị trí: `05-backend/security-lab/src/main/scala/vn/scalaeng/sec/Production.scala:79` và dòng 82; `05-backend/ch17-security-production.md:204`.
- `dbUp` khởi tạo true và chỉ test đổi nó thành false; SecureMain chưa probe database thật. Câu “Database chết → ready=503” hiện mô tả khả năng của Health/check được inject, chưa phải hành vi khi chạy SecureMain với một DB bên ngoài.
- Đề xuất: ghi rõ cờ mô phỏng trong lab in-memory và hướng dẫn Project 2 inject probe DB thật có timeout; không khẳng định đã kiểm chứng mất kết nối database. Không yêu cầu chương security phải tự thêm integration PostgreSQL.

### m4 — Nguồn NIST cũ và ngưỡng mật khẩu của lab cần được phân biệt

- Vị trí: `05-backend/ch17-security-production.md:75`, dòng 76 và 517.
- Link đang trỏ SP 800-63B của bộ 800-63-3. Bản hiện hành SP 800-63B-4 yêu cầu tối thiểu **15 ký tự** khi mật khẩu là yếu tố xác thực duy nhất; lab đang dùng 12, không có MFA. Lời khuyên bỏ composition rules vẫn phù hợp, nhưng người mới dễ hiểu toàn bộ policy lab là policy NIST hiện hành.
- Đề xuất: cập nhật [nguồn SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html#passwordver), tách rõ quy tắc không ép thành phần với ngưỡng tối thiểu; nâng policy/test hoặc ghi đây là mức lab đã giản lược. Đối chiếu 17/10/2026.

## Kiểm chứng thực chạy

Môi trường Scala CLI 1.9.1, directives Scala **3.9.0**, JDK **21**, munit **1.2.4**, munit-cats-effect **2.2.0**. Đặt cache/temp dưới project:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:SCALA_CLI_HOME = "$PWD/.cache/scala-cli"
$env:TMP = "$PWD/.cache/tmp"
$env:TEMP = $env:TMP
scala-cli.bat test 05-backend/security-lab --server=false --workspace .cache/review-t05-lab --java-opt "-Djava.io.tmpdir=$PWD/.cache/tmp"
scala-cli.bat test solutions/ch17-code --server=false --workspace .cache/review-t05-solutions --java-opt "-Djava.io.tmpdir=$PWD/.cache/tmp"
```

| Suite | Kết quả thật |
|---|---|
| ApiSuite | 6 total, 0 failed, 0 ignored |
| CryptoSuite | 9 total, 0 failed, 0 ignored |
| ProductionSuite | 8 total, 0 failed, 0 ignored |
| Ch17SolutionsSuite | 5 total, 0 failed, 0 ignored |

**Tổng 28 pass, 0 fail, 0 skip**, exit code 0 của cả hai lệnh. Có chạy HTTPS với keytool và graceful release qua socket thật; sandbox không chặn những test này. Không thấy compiler warning của lab gốc; các hint cập nhật dependency không phải cảnh báo compiler.

Probe bổ sung dùng code gốc trong `.cache/review-t05-probe.scala`, chạy `scala-cli.bat run ... --server=false --main-class ReviewT05Probe`, exit code 0. Log `.cache/review-t05-probe.log` chứa toàn bộ output trích ở trên; không in token hay secret. Probe ném exception ở một endpoint thử nghiệm, không sửa Api nguồn. Cảnh báo chained directives/multiple logback resources của probe do cách reviewer tái sử dụng source, không tính cho project gốc.

Log test gốc: `.cache/review-t05-lab.log`, `.cache/review-t05-solutions.log`. Không chạy sbt do giới hạn AGENTS.md. Không gửi SIGTERM thật trên Windows; kết quả graceful shutdown chỉ xác nhận release Resource giống giới hạn handoff đã ghi.

## Template, sư phạm, output và lời giải

- Đủ **17/17 mục**, không rỗng. Tiếng Việt có dấu, giải thích salt/claim/IDOR/rate limit/CORS/health theo vấn đề và đánh đổi; nối kiến thức IO/Resource/Ref tốt. Phần lỗi thực tế 10.1/10.2/10.6 hữu ích và được test gốc bảo vệ.
- Sáu bài tập có lời giải; bài 1–5 có năm test thật; bài 6 là so sánh thiết kế có tiêu chí. Bài 3 cần sửa B1/B2, bài 4 cần sửa M3 trước khi coi lời giải đúng đủ. Test HTTPS dùng trust store cụ thể, không tắt xác minh chứng chỉ; đã đọc nguồn EmberClientBuilder 0.23.38 xác nhận endpoint identification bật mặc định.
- Các response chính (422/409/401/403/200), role dạng chuỗi, che Authorization, preflight, metric 1.0 sau một login sai và body lỗi `/boom` khớp test/code và output thật đã chạy. Thời gian, UUID, chữ ký JWT, salt, timestamp thay đổi nên không so byte nguyên transcript. Retry-After=11 trong phiên curl cũ phụ thuộc thời gian, không phải hằng số mà code bảo đảm.
- **Chưa đo lại benchmark** SHA-256/Argon2id hay khởi động SecureMain để thu lại toàn bộ transcript curl. Handoff chỉ có số đo, chưa có script benchmark để tái lập; nên lưu script/lệnh, warm-up và cách lấy trung vị nếu dùng bảng số đo làm bằng chứng hiệu năng. Không coi con số tác giả đã đo là sai chỉ vì reviewer chưa tái đo.
- Tài khoản admin hard-code ở SecureMain đã được bài dòng 302 khai báo là **chỉ để thử nghiệm**. Đây là giới hạn ví dụ; khi ghép Project 2 cần dùng bootstrap riêng, không mang credential học tập sang deployment.

## Đối chiếu nguồn ngoài

Đã tải trực tiếp Maven Central ngày **17/10/2026**:

- [password4j metadata](https://repo1.maven.org/maven2/com/password4j/password4j/maven-metadata.xml): 1.8.4.
- [jwt-circe metadata](https://repo1.maven.org/maven2/com/github/jwt-scala/jwt-circe_3/maven-metadata.xml): 11.0.4.
- [http4s-prometheus-metrics metadata](https://repo1.maven.org/maven2/org/http4s/http4s-prometheus-metrics_3/maven-metadata.xml): stable 0.25.0; latest overall có milestone 1.0.0-M38, không nhầm milestone với stable.
- [log4cats-slf4j metadata](https://repo1.maven.org/maven2/org/typelevel/log4cats-slf4j_3/maven-metadata.xml): 2.8.0.
- [logback-classic metadata](https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/maven-metadata.xml): 1.6.5.

Đã đọc [OWASP Password Storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html): cấu hình Argon2id 19 MiB, t=2, p=1 phù hợp khuyến nghị tối thiểu. Đã đối chiếu RFC JWT/JWS/HTTP và tài liệu Tapir ở từng finding; các vấn đề runtime được tái hiện bằng đúng dependency của lab/lời giải.
