# Handoff T07 — Chương 18 + Project 2 Backend Service

- Tác giả: Codex; reviewer/lead: Claude Code.
- Ngày cập nhật: 17/10/2026 (Asia/Bangkok).
- Trạng thái: đã sửa toàn bộ 2 MAJOR + 6 MINOR của [review vòng 1](../reviews/T07-ch18-p2-review.md).
  Bản vòng 1 đã được Claude kiểm chứng **37/37 pass bằng sbt + PostgreSQL thật**; bản vòng 2 cần lead chạy lại DB/sbt và kiểm tra review.
- Chỉ ghi trong phạm vi brief T07. Không sửa file dùng chung, 09-spark/Ch25/Ch23, Ch22; không commit git.

## 1. Nội dung bàn giao

Ch18 đủ 17 mục, dùng P2 minh họa modular monolith, dependency inversion, functional core,
transaction boundary, cache/invalidation, resilience và observability. Sáu bài tập có lời giải code và test.
P2 gồm domain/service/db/security/http/app; Scala CLI + sbt nhiều module; PostgreSQL/Doobie/Hikari/Flyway,
Tapir/http4s, JWT/Argon2id, checkout/hoàn kho nguyên tử, OpenAPI, readiness DB, metrics, Resource shutdown,
Dockerfile multi-stage và Compose.

Các quyết định giữ nguyên: checked arithmetic; ON CONFLICT; khóa product theo ID tăng; revocation theo jti
trong serverSecurityLogic; role đọc mới từ DB; rate limit trong endpoint login; exceptionHandler(None).
Giới hạn còn có chủ đích: cache/limiter một instance, TTL stale, chưa có idempotency key/outbox/histogram/tracing.

## 2. Bằng chứng vòng 1 từ lead

Nguồn: [.team/reviews/T07-ch18-p2-review.md](../reviews/T07-ch18-p2-review.md).
Claude chạy ngoài sandbox: `RUN_DB_TESTS=1 sbt.bat -batch test`, exit 0, **37 pass, 0 fail**,
gồm 12 PostgresSuite và 1 PostgresHttpSuite với PostgreSQL nhúng zonky 18.6.0 thật.
Đây là kết quả bản trước sửa vòng 2, không dùng để nhận các test mới đã pass.

Đoạn output THẬT do Claude cung cấp:

```text
vn.scalaeng.p2.db.PostgresSuite: finished 21.928s
Test run vn.scalaeng.p2.db.PostgresSuite finished: 0 failed, 0 ignored, 12 total 21.951s
vn.scalaeng.p2.app.PostgresHttpSuite: finished 15.967s
Test run vn.scalaeng.p2.app.PostgresHttpSuite finished: 0 failed, 0 ignored, 1 total 15.968s
```

Bản trích nguyên code block đầu của review: `14-projects/p2-backend-service/evidence/sbt-postgres-review-round1.txt`;
JSON đi kèm ghi nguồn, SHA-256 review, command và phạm vi vòng 1.
Log Scala CLI `tests.txt`/`solutions.txt`/`summary.json` được cập nhật theo lượt vòng 2 mới nhất;
không còn dùng số 13 assume-skip ban đầu làm trạng thái DB chung.

## 3. Vòng 2 — sửa review và hồi quy

| Mục | Sửa và kiểm tra |
|---|---|
| M1 | Ch18, README, handoff và evidence ghi 37/37 pass sbt + PostgreSQL thật của vòng 1; trích output có nguồn; phân biệt rõ test vòng 2 và Docker/assembly chưa chạy. |
| M2 | `PgStore.getOrder` SELECT thường; thêm port `Store.lockOrder` với FOR UPDATE; chỉ status/updateStatus dùng lock. Ports docs và Ch18 mục 3.3 giải thích GET không lấy row lock. |
| m1 | `BootstrapAdmin.create` validate/hash trước khi lấy connection, ghép register + update Admin trong một ConnectionIO và một transact. Email có sẵn không nâng quyền. |
| m2 | Passwords.policy dùng Locale.ROOT ở mọi chỗ chuẩn hóa; regression đặt locale tr-TR rồi phục hồi trong finally. |
| m3 | Limiter tối đa 10000 bucket; key mới dọn bucket đã hồi đầy, map đầy active bucket thì key mới nhận 429. Giữ active quota, không eviction để né limit. TestControl kiểm tra bound/refill/cleanup và thời gian chờ. README ghi O(n)/key mới và giới hạn một instance. |
| m4 | http4s EntityLimiter 65536 byte, kiểm Content-Length sớm và đếm stream thực; handler trả Problem 413. ApiSuite kiểm cả body có/không Content-Length và không ghi user. |
| m5 | Metrics có TYPE summary, duration sum/count cùng label status; các mẫu cùng metric family được nhóm liên tục. OperationsSuite kiểm TYPE/sum/count. |
| m6 | Sáu module có src/test/resources/logback-test.xml root WARN; db thêm logback Test; CLI nạp test.resourceDir ở http. Stack trace cố ý chỉ in ngắn; không tràn DEBUG/INFO. |

**Test DB hồi quy mới (đã compile, chưa chạy runtime trong sandbox):**

- PostgresSuite thêm 1 test M2: JDBC connection riêng giữ row lock chưa commit; Shop.order owner trả Right,
  stranger trả Forbidden trong timeout 2 giây; updateStatus vẫn bị lock timeout SQLSTATE 55P03;
  sau nhả khóa hủy thành công và kho phục hồi. Test hủy đồng thời cũ tiếp tục bảo vệ hoàn kho một lần.
- BootstrapAdminSuite thêm 4 test m1: tạo Admin với hash Argon2id verify thật và duplicate không đổi hash;
  email Customer có sẵn không được nâng quyền; trigger ép UPDATE role thất bại SQLSTATE P0001 →
  không còn account tạo dở, bỏ trigger rồi chạy lại thành Admin; input sai không ghi user.
- Tổng test DB hiện tại: **18** = PostgresSuite 13 + PostgresHttpSuite 1 + BootstrapAdminSuite 4.
  Giữ RUN_DB_TESTS opt-in; đã bật mà PostgreSQL không khởi động được thì fail, không che bằng skip.

**Kiểm chứng vòng 2 trong sandbox:** **29 test P2 pass + 6 test lời giải pass = 35 pass, 0 fail, 0 cảnh báo compiler**.
**18 test DB assume-skip**, chỉ compile, chưa nhận là pass. Directives vẫn bật
`-deprecation -feature -Wunused:all -Wvalue-discard -Werror`.

Lệnh đã chạy từ root (script đặt COURSIER_CACHE=`<repo>/.cache/coursier`, bỏ RUN_DB_TESTS trong process con,
đặt Java UTF-8, mọi Scala CLI dùng `--server=false`):

```powershell
python 14-projects/p2-backend-service/evidence/run_checks.py tests solutions domain-isolated
```

Tương đương các command trong `evidence/checks.json`:

```powershell
scala-cli.bat test 14-projects/p2-backend-service --server=false --suppress-outdated-dependency-warning
scala-cli.bat test solutions/ch18-code --server=false --suppress-outdated-dependency-warning
scala-cli.bat compile 14-projects/p2-backend-service/domain/src/main/scala --server=false --scala 3.9.0 --jvm 21 --scalac-option -Wunused:all --scalac-option -Werror
```

Output thật từng suite xem `evidence/tests.txt`, `evidence/solutions.txt`; số pass đã trừ các dòng skipped trong `summary.json`.

Đoạn output THẬT của các suite không cần DB trong lượt vòng 2 cuối:

```text
Test run vn.scalaeng.p2.app.ConfigSuite finished: 0 failed, 0 ignored, 1 total 0.008s
Test run vn.scalaeng.p2.domain.CoreSuite finished: 0 failed, 0 ignored, 5 total 0.036s
Test run vn.scalaeng.p2.http.ApiSuite finished: 0 failed, 0 ignored, 8 total 6.034s
Test run vn.scalaeng.p2.http.OperationsSuite finished: 0 failed, 0 ignored, 3 total 1.034s
Test run vn.scalaeng.p2.security.CryptoSuite finished: 0 failed, 0 ignored, 4 total 0.28s
Test run vn.scalaeng.p2.security.RateLimiterSuite finished: 0 failed, 0 ignored, 2 total 0.032s
Test run vn.scalaeng.p2.service.ArchitectureSuite finished: 0 failed, 0 ignored, 1 total 0.053s
Test run vn.scalaeng.p2.service.ResilienceSuite finished: 0 failed, 0 ignored, 5 total 0.038s
Test run vn.scalaeng.solutions.ch18.ExercisesSuite finished: 0 failed, 0 ignored, 6 total 0.132s
```
`verify_content.py`: 17 lesson sections; local links resolve; no placeholder headings.
`git -c core.whitespace=cr-at-eol diff --check` chỉ phạm vi T07 được kiểm tra trước bàn giao (giữ CRLF của file gốc).

Không chạy lại demo/Compose vì không đổi các phần đó. Giữ output demo/probe vòng 1 có nguồn trong evidence,
không thay bằng số đo dự đoán. Không thử lách giới hạn sandbox sbt/pg_ctl đã ghi trong brief.

## 4. Phiên bản và nguồn kiểm tra 17/10/2026

Vòng 2 không đổi dependency version. Đã đối chiếu thêm [http4s 0.23 EntityLimiter](https://http4s.org/v0.23/docs/server-middleware.html#entitylimiter)
ngày 17/10/2026: middleware kiểm độ dài body thực, không chỉ dựa Content-Length. Test 413 chạy trên http4s 0.23.38 đã pin.

`evidence/verify_versions.py` đã tải **30 POM exact release**, tất cả HTTP 200, lưu URL/timestamp UTC/SHA-256 trong versions.json.
Artifact Scala dùng `_3`, không suy diễn từ Scala 2. HikariCP được pin 7.0.2 như dependency doobie-hikari RC13.
munit-cats-effect giữ 2.2.0 để không kéo munit 1.3.6. Tồn tại POM không có nghĩa sbt/Docker đã chạy.

- `org.typelevel:cats-effect_3:3.7.1`: [POM release](https://repo1.maven.org/maven2/org/typelevel/cats-effect_3/3.7.1/cats-effect_3-3.7.1.pom), HTTP 200.
- `org.http4s:http4s-ember-server_3:0.23.38`: [POM release](https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/0.23.38/http4s-ember-server_3-0.23.38.pom), HTTP 200.
- `org.http4s:http4s-dsl_3:0.23.38`: [POM release](https://repo1.maven.org/maven2/org/http4s/http4s-dsl_3/0.23.38/http4s-dsl_3-0.23.38.pom), HTTP 200.
- `io.circe:circe-core_3:0.14.17`: [POM release](https://repo1.maven.org/maven2/io/circe/circe-core_3/0.14.17/circe-core_3-0.14.17.pom), HTTP 200.
- `com.softwaremill.sttp.tapir:tapir-http4s-server_3:1.13.33`: [POM release](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-http4s-server_3/1.13.33/tapir-http4s-server_3-1.13.33.pom), HTTP 200.
- `com.softwaremill.sttp.tapir:tapir-json-circe_3:1.13.33`: [POM release](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-json-circe_3/1.13.33/tapir-json-circe_3-1.13.33.pom), HTTP 200.
- `com.softwaremill.sttp.tapir:tapir-openapi-docs_3:1.13.33`: [POM release](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-openapi-docs_3/1.13.33/tapir-openapi-docs_3-1.13.33.pom), HTTP 200.
- `com.softwaremill.sttp.apispec:openapi-circe-yaml_3:0.11.10`: [POM release](https://repo1.maven.org/maven2/com/softwaremill/sttp/apispec/openapi-circe-yaml_3/0.11.10/openapi-circe-yaml_3-0.11.10.pom), HTTP 200.
- `org.typelevel:doobie-core_3:1.0.0-RC13`: [POM release](https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/1.0.0-RC13/doobie-core_3-1.0.0-RC13.pom), HTTP 200.
- `org.typelevel:doobie-postgres_3:1.0.0-RC13`: [POM release](https://repo1.maven.org/maven2/org/typelevel/doobie-postgres_3/1.0.0-RC13/doobie-postgres_3-1.0.0-RC13.pom), HTTP 200.
- `org.typelevel:doobie-hikari_3:1.0.0-RC13`: [POM release](https://repo1.maven.org/maven2/org/typelevel/doobie-hikari_3/1.0.0-RC13/doobie-hikari_3-1.0.0-RC13.pom), HTTP 200.
- `org.flywaydb:flyway-core:13.10.3`: [POM release](https://repo1.maven.org/maven2/org/flywaydb/flyway-core/13.10.3/flyway-core-13.10.3.pom), HTTP 200.
- `org.flywaydb:flyway-database-postgresql:13.10.3`: [POM release](https://repo1.maven.org/maven2/org/flywaydb/flyway-database-postgresql/13.10.3/flyway-database-postgresql-13.10.3.pom), HTTP 200.
- `org.postgresql:postgresql:42.7.14`: [POM release](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.pom), HTTP 200.
- `com.zaxxer:HikariCP:7.0.2`: [POM release](https://repo1.maven.org/maven2/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.pom), HTTP 200.
- `com.password4j:password4j:1.8.4`: [POM release](https://repo1.maven.org/maven2/com/password4j/password4j/1.8.4/password4j-1.8.4.pom), HTTP 200.
- `com.github.jwt-scala:jwt-circe_3:11.0.4`: [POM release](https://repo1.maven.org/maven2/com/github/jwt-scala/jwt-circe_3/11.0.4/jwt-circe_3-11.0.4.pom), HTTP 200.
- `org.typelevel:log4cats-slf4j_3:2.8.0`: [POM release](https://repo1.maven.org/maven2/org/typelevel/log4cats-slf4j_3/2.8.0/log4cats-slf4j_3-2.8.0.pom), HTTP 200.
- `ch.qos.logback:logback-classic:1.6.5`: [POM release](https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/1.6.5/logback-classic-1.6.5.pom), HTTP 200.
- `org.scalameta:munit_3:1.2.4`: [POM release](https://repo1.maven.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom), HTTP 200.
- `org.typelevel:munit-cats-effect_3:2.2.0`: [POM release](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom), HTTP 200.
- `org.typelevel:cats-effect-testkit_3:3.7.1`: [POM release](https://repo1.maven.org/maven2/org/typelevel/cats-effect-testkit_3/3.7.1/cats-effect-testkit_3-3.7.1.pom), HTTP 200.
- `org.http4s:http4s-ember-client_3:0.23.38`: [POM release](https://repo1.maven.org/maven2/org/http4s/http4s-ember-client_3/0.23.38/http4s-ember-client_3-0.23.38.pom), HTTP 200.
- `io.zonky.test:embedded-postgres:2.2.2`: [POM release](https://repo1.maven.org/maven2/io/zonky/test/embedded-postgres/2.2.2/embedded-postgres-2.2.2.pom), HTTP 200.
- `io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0`: [POM release](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/18.6.0/embedded-postgres-binaries-windows-amd64-18.6.0.pom), HTTP 200.
- `io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0`: [POM release](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-amd64/18.6.0/embedded-postgres-binaries-linux-amd64-18.6.0.pom), HTTP 200.
- `io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0`: [POM release](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-darwin-arm64v8/18.6.0/embedded-postgres-binaries-darwin-arm64v8-18.6.0.pom), HTTP 200.
- `org.scala-lang:scala3-compiler_3:3.9.0`: [POM release](https://repo1.maven.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom), HTTP 200.
- `org.scala-sbt:sbt:1.12.11`: [POM release](https://repo1.maven.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom), HTTP 200.
- `com.eed3si9n:sbt-assembly_2.12_1.0:2.3.1`: [POM release](https://repo1.maven.org/maven2/com/eed3si9n/sbt-assembly_2.12_1.0/2.3.1/sbt-assembly_2.12_1.0-2.3.1.pom), HTTP 200.

Tài liệu chính thức đã đối chiếu: [Doobie connections](https://typelevel.org/doobie/docs/14-Managing-Connections.html),
[PostgreSQL 18 locking](https://www.postgresql.org/docs/18/explicit-locking.html),
[Tapir errors](https://tapir.softwaremill.com/en/latest/server/errors.html),
[Tapir security logic](https://github.com/softwaremill/tapir/blob/master/doc/server/logic.md),
[sbt-assembly 2.3.1](https://github.com/sbt/sbt-assembly/releases/tag/v2.3.1),
[Postgres official image](https://hub.docker.com/_/postgres), [Temurin official image](https://hub.docker.com/_/eclipse-temurin).
Docker base image tags trong Dockerfile/Compose chưa được pull/run; không khẳng định image đã kiểm chứng.


## 5. Việc lead cần làm và đề xuất file dùng chung

Từ root repo:

```powershell
$env:RUN_DB_TESTS = '1'
Push-Location 14-projects/p2-backend-service
sbt.bat -batch test
sbt.bat 'app/assembly'
Pop-Location
Remove-Item Env:RUN_DB_TESTS
```

- Chạy lại toàn bộ **47 test P2**, gồm **18 test PostgreSQL** và **5 test DB mới**, sau đó kiểm tra lại review vòng 2.
  Con số 47 là số test được định nghĩa, chưa phải số pass được xác nhận với DB của vòng 2.
- Chạy `app/assembly`; Docker build/up, persistence qua restart và SIGTERM khi request đang chạy.
- PROGRESS: ghi review vòng 1 0 BLOCKER/2 MAJOR/6 MINOR đã sửa; vòng 1 sbt/DB 37 pass;
  vòng 2 CLI 29 P2 + 6 lời giải pass, 18 DB skip, chờ sbt/DB chạy lại. Chưa tự đánh dấu hoàn tất.
- Compatibility matrix: không đổi version; db thêm logback 1.6.5 ở scope Test.
- Glossary: có thể bổ sung modular monolith, composition root, dependency inversion, transaction boundary,
  bulkhead, cache generation, idempotency key/outbox (hai mục cuối là mở rộng, chưa là tính năng P2).

## 6. Những gì chưa kiểm chứng và giới hạn

1. Bản vòng 2 chưa chạy sbt/PostgreSQL runtime; 5 test DB mới và các thay đổi SQL/transaction chờ lead chạy.
   Bản vòng 1 đã được lead chạy thật 37/37 pass, không còn trạng thái “DB chưa từng chạy”.
2. app/assembly, image pull/build/up, persistence và SIGTERM container chưa kiểm chứng.
   Lượt trước Compose config exit 0; docker info exit 1 vì không có daemon.
3. Cache đo bằng sleep, không phải benchmark PostgreSQL/production. Limiter có bound RAM nhưng quét O(n) khi key mới;
   map đầy có thể chặn IP hợp lệ. Cache/limiter chưa đồng bộ đa instance.
4. Chưa idempotency key/outbox, cleanup revocation độc lập, histogram/distributed tracing.
   Timeout không chứng minh write chưa commit; không retry checkout tự động.
5. Lead cần kiểm tra lại sửa review rồi cập nhật file dùng chung/commit. Codex không commit.

## 7. File sửa/tạo trong vòng 2

- `05-backend/ch18-backend-architecture.md`; `14-projects/p2-backend-service/README.md`.
- `domain/src/main/scala/vn/scalaeng/p2/domain/Ports.scala`; `service/src/main/scala/vn/scalaeng/p2/service/Shop.scala`.
- `db/src/main/scala/vn/scalaeng/p2/db/PgStore.scala`; `db/src/test/scala/vn/scalaeng/p2/db/PostgresSuite.scala`.
- `app/src/main/scala/vn/scalaeng/p2/app/Main.scala`; mới `app/src/test/scala/vn/scalaeng/p2/app/BootstrapAdminSuite.scala`.
- `security/src/main/scala/vn/scalaeng/p2/security/Passwords.scala`, `RateLimiter.scala`;
  `security/src/test/scala/vn/scalaeng/p2/security/CryptoSuite.scala`; mới `RateLimiterSuite.scala`.
- `http/src/main/scala/vn/scalaeng/p2/http/Operations.scala`;
  `http/src/test/scala/vn/scalaeng/p2/http/ApiSuite.scala`, `OperationsSuite.scala`, `MemoryStore.scala`.
- Mới `{domain,service,db,security,http,app}/src/test/resources/logback-test.xml`.
- `build.sbt`, `project.scala`.
- `evidence/run_checks.py`, `checks.json`, `summary.json`, `tests.txt`, `solutions.txt`, `domain-isolated.txt`,
  `source-sha256.json`; mới `sbt-postgres-review-round1.txt` và `sbt-postgres-review-round1.json`.
- `solutions/ch18-solutions.md` (cập nhật trạng thái kiểm chứng); code lời giải không cần sửa, đã chạy lại 6/6.
- `.team/handoffs/T07-ch18-p2-backend.md` (file này).

Các đường dẫn module/evidence ở danh sách trên tính từ `14-projects/p2-backend-service/`.
