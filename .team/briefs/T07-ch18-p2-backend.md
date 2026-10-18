# Brief T07 — Chương 18 Backend Architecture + Project 2 Backend Service

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 18, Project 2), template, `00-roadmap/06-projects-overview.md` (đặc tả P2 nếu có),
  `05-backend/ch14-rest-apis.md` + `order-service/` (của bạn), `06-database/ch15-database-engineering.md` + `db-lab/` (Doobie RC13 package
  `org.typelevel.doobie`, `ON CONFLICT` thay `attemptSomeSqlState`, `FOR UPDATE`, Flyway, HikariCP, test PostgreSQL nhúng zonky),
  `05-backend/ch17-security-production.md` + `security-lab/` (Argon2id, JWT có `jti`/`exp` bắt buộc, revocation trong `serverSecurityLogic`,
  rate limit TRONG endpoint, `exceptionHandler(None)` + errorHandler chung, health/metrics, graceful shutdown). Đọc mục 10 của Ch15 và Ch17:
  các lỗi review chéo đã tìm ra KHÔNG được lặp lại trong P2.

## Phạm vi được phép ghi
- `05-backend/ch18-backend-architecture.md`
- `14-projects/p2-backend-service/` (sbt nhiều module + `README.md` theo khuôn P1: mục tiêu, tiên quyết, đặc tả, thiết kế & trade-off, cách chạy,
  output, kiểm thử, lỗi thường gặp, bài tập mở rộng, checklist)
- `solutions/ch18-solutions.md`, `solutions/ch18-code/`, `.team/handoffs/T07-ch18-p2-backend.md`

## Project 2 — yêu cầu
REST API users + orders (+ products/tồn kho nếu hợp lý) bằng Scala 3.9.0, Tapir + http4s, **PostgreSQL** qua Doobie (adapter cài trait repository
của Ch14 bằng `ConnectionIO` + `transact` ở đúng ranh giới use case), Flyway migration, validation, Problem JSON, **authentication + authorization**
(đăng ký/đăng nhập/JWT, chủ sở hữu đơn hàng, admin), integration test với PostgreSQL thật (zonky, như Ch15), health/readiness **kiểm tra DB thật**
(`select 1` có timeout), metrics, graceful shutdown, **Docker**: `Dockerfile` (multi-stage) + `docker-compose.yml` (app + postgres:18.6).
Cấu hình bằng biến môi trường, không secret mặc định. Module gợi ý: `domain` (thuần), `app`/`http`, `db`, có thể `security`.

## Chương 18 — yêu cầu
Layered architecture, modular monolith, dependency inversion, domain modeling, Functional Core / Imperative Shell, separation of concerns, Clean
Architecture ở mức phù hợp, caching (có số đo + vấn đề invalidation), transaction boundaries, resilience (timeout, retry có điều kiện, circuit breaker
hoặc bulkhead — kiểm chứng thư viện trước, hoặc tự viết bằng Cats Effect), observability. **Dùng chính P2 để minh họa** việc cải tiến kiến trúc
(ví dụ: quy tắc phụ thuộc giữa module được build kiểm tra; một test kiến trúc).

## Giới hạn môi trường (QUAN TRỌNG)
- Sandbox Codex **chặn sbt** và **chặn PostgreSQL nhúng** (`pg_ctl: could not create restricted token: error code 87`). Hãy: viết đầy đủ test DB và
  build.sbt; kiểm chứng phần không cần DB bằng Scala CLI (`--server=false`, `COURSIER_CACHE` trong `.cache/`); ghi rõ trong handoff phần nào CHƯA chạy
  được để Claude chạy `sbt test` + test PostgreSQL. Không đánh dấu output của test DB là "đã chạy" nếu chưa chạy.
- Docker daemon có thể chưa chạy: viết Dockerfile/compose nhưng ghi "chưa kiểm chứng" nếu không chạy được.

## Tiêu chí xong
- Mọi phần chạy được trong sandbox đã chạy, 0 cảnh báo; output trong bài là thật; bài 17 mục; ≥6 bài tập có lời giải + test; handoff đầy đủ.
