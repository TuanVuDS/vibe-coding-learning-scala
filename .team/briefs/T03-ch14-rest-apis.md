# Brief T03 — Chương 14: Building REST APIs (Tapir + http4s)

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 14), template, `05-backend/ch12-backend-fundamentals.md`,
  `05-backend/ch13-choosing-a-framework.md` + `05-backend/framework-lab/typelevel/` (stack đã chốt, contract test, bài 4 về
  decode failure → Problem JSON), `07-concurrency/ch16b-cats-effect.md`.

## Phạm vi được phép ghi
- `05-backend/ch14-rest-apis.md`
- `05-backend/order-service/` — project **sbt nhiều module** (+ `project.scala` không bắt buộc), ví dụ: `domain`, `app` (service + repository
  interface + HTTP), có thể thêm `testkit`. Package `vn.scalaeng.orders`.
- `solutions/ch14-solutions.md`, `solutions/ch14-code/`
- `.team/handoffs/T03-ch14-rest-apis.md`

## Nội dung bắt buộc
Dự án: **service quản lý người dùng và đơn hàng** (yêu cầu đề bài). Repository ở chương này là **in-memory** (cài trên `Ref`) đứng sau
một trait — Chương 15 (Claude viết song song) sẽ thay bằng PostgreSQL + Doobie, nên trait repository phải:
- dùng `IO` (hoặc `F[_]` nếu bạn giải thích được vì sao), không lộ kiểu của thư viện DB;
- có các thao tác: users (create, get, list có filter/sort/pagination), orders (create cho user, get, list theo user, cập nhật trạng thái).
Ghi rõ trait trong handoff để Claude dùng ở Chương 15.

Chủ đề: routing; request parsing; response serialization; JSON codecs (circe, **tách codec khỏi domain** như đã hứa ở Ch13);
domain model (opaque type cho id, enum trạng thái đơn hàng, Money theo đơn vị nhỏ nhất); service layer; repository layer; validation
tích lũy lỗi; error handling thống nhất bằng Problem JSON (kể cả decode failure); pagination (offset/limit và so sánh với cursor);
filtering; sorting (whitelist trường được sort); API versioning (path `/api/v1`, bàn về header/media type); OpenAPI sinh tự động + phục vụ
tại `/docs/openapi.yaml` (Swagger UI là tùy chọn — kiểm chứng artifact nếu dùng); API testing: contract test không mạng + integration test
qua socket thật cổng 0.

Phiên bản: dùng đúng các phiên bản đã kiểm chứng ở Ch13 (http4s 0.23.38, Tapir 1.13.33, sttp-apispec 0.11.10, circe 0.14.17,
munit 1.2.4, **munit-cats-effect 2.2.0** — KHÔNG dùng 2.2.1 vì nó kéo munit 1.3.6, như Codex phát hiện ở T01). Thêm thư viện mới → kiểm chứng trên `https://repo1.maven.org/maven2/.../maven-metadata.xml`
(KHÔNG dùng search.maven.org — dữ liệu cũ).

## Tiêu chí xong
- sbt bị sandbox Codex chặn (T01: `AccessDeniedException ... BootServerSocket`). Viết `build.sbt` đầy đủ nhưng kiểm chứng bằng Scala CLI (`project.scala` ở gốc project gom mọi module, hoặc chạy từng module); Claude sẽ chạy `sbt test` khi review. Đặt `COURSIER_CACHE` trong thư mục project (`.cache/coursier`, đã ignore) như T01.
- (Ghi chú cũ: nếu sandbox chặn sbt — ghi `~/.sbt`, `~/.ivy2` — hãy thử `sbt.bat -Dsbt.global.base=.sbt-global -Dsbt.ivy.home=.ivy2 -Dsbt.boot.directory=.sbt-boot test`
  bên trong thư mục project, các thư mục này đã nằm trong `.gitignore` của project; nếu vẫn không được, kiểm chứng bằng Scala CLI và ghi rõ
  trong handoff để Claude chạy `sbt test`.)
- `sbt.bat test` trong `05-backend/order-service` pass, 0 cảnh báo (`-deprecation -feature -Wunused:all -Wvalue-discard`).
- Server chạy thật; trao đổi `curl -i` trong bài là thật (Windows: gửi body UTF-8 bằng `--data-binary @file.json`).
- Lỗi trong mục 10 là thông báo thật. Bài tập ≥6 có lời giải + test.
- Handoff theo AGENTS.md mục 2.3 (kèm định nghĩa trait repository).
