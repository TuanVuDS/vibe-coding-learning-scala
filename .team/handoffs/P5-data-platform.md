# Handoff Project 5 — Production-Oriented Data Platform (tác giả: Claude Code, reviewer: Codex)

## File
`14-projects/p5-data-platform/`: README (khuôn project + runbook 3 sự cố), `build.sbt` (core, ingest, serving, app), `project/{build.properties,plugins.sbt}`,
`project.scala` (Scala CLI — chạy `scala-cli test project.scala core ingest serving app`), `data/drop/*.jsonl` (3 lô + 1 file sai contract), `data/dimension/customers.csv`,
`core/` (Model, Lake, Landing, Assets, Orchestrator/Lineage/Quality + CoreSuite), `ingest/` (KafkaLanding + KafkaLandingSuite), `serving/` (ServingDb, Api),
`app/` (Main CLI + PlatformSuite), `orchestration/` (defs.py Dagster, LocalPostgres.scala công cụ dev, run-platform.sh).

## Kiểm chứng (17/10/2026, Windows 11, JDK 21)
- `RUN_DB_TESTS=1 RUN_KAFKA_IT=1 scala-cli test project.scala core ingest serving app` → 5 + 2 + 4 = 11/11; `sbt test` cùng biến → 5/2/4 pass; mặc định 5 chạy, 6 tự bỏ qua.
- `sbt app/assembly` → p5.jar. `orchestration/run-platform.sh`: PostgreSQL 18.6 nhúng + Dagster 1.13.26 (4 asset × 3 ngày exit 0) + `stale` + API — output ở README mục 6.
- Golden tính tay trước khi chạy (cùng dữ liệu Ch31) — khớp.

## Phiên bản
Như P2/P4 (đã tra repo1.maven.org 17/10/2026); thêm `http4s-circe_3 0.23.38` (POM HTTP 200 ngày 17/10/2026).

## Chưa kiểm chứng
Docker/MinIO/Parquet/Spark trong P5 (README 4.5), Dagster UI/sensor, PostgreSQL ngoài (chỉ bản nhúng), nhiều consumer Kafka/rebalance.

## Điểm reviewer nên soi
- Đối soát staging (`superseded` với nhiều bản cùng payload), `Reconcile.unassigned`.
- KafkaLanding: thứ tự ghi/commit, quarantine record null, lag khi chưa commit.
- `ServingDb.load`: advisory lock + DELETE/INSERT + output_hash; ngày rỗng.
- Quality gate có bỏ sót (dimension đổi sau khi tính daily?), API validate tham số, `/metrics`.
- Khẳng định trong bảng 4.1 (khi nào Scala/SQL/Dagster/PostgreSQL).

## Vòng 2 — phản hồi review Codex (`.team/reviews/P5-review.md`, 6 MAJOR + 4 MINOR), 17/10/2026

| ID | Sửa | Bằng chứng |
|---|---|---|
| M1 | Staging lưu **JSON Lines** qua codec circe (`StagingRow`, file `part.jsonl`), bỏ CSV tự ghép chuỗi; `StagingOrders.parse` ném khi dòng hỏng (không `collect` bỏ qua); gate thêm đối soát **độc lập**: tổng PAID (VND, số dòng) của staging = tổng daily. Segment trong dimension bị kiểm ký tự (daily vẫn CSV an toàn). | CoreSuite `review P5 M1` (dấu phẩy, ngoặc kép, xuống dòng → `Retail,300,3,3`; xóa dòng daily → `daily không khớp staging: PAID 300 VND / 3 dòng, daily 0 VND / 0 dòng`). |
| M2 | Dedup chỉ khi cùng `event_id` **và** cùng nội dung chuẩn hóa; khác nội dung → cả nhóm vào `rejected.jsonl` + đếm `event_conflicts`; phương trình đối soát thêm số hạng này. | CoreSuite `review P5 M2`: hai thứ tự cho cùng kết quả, khác key/khác timestamp → xung đột, cùng instant viết khác múi giờ → gửi lại hợp lệ. |
| M3 | Một hàm `Order.dayOf` cho chọn partition, validate, `Reconcile`. | CoreSuite `review P5 M3`: `+02026-10-15`, `2026-02-30`, rỗng → unassigned; ` 2026-10-15 ` → ngày 15; `raw_rows = assigned + unassigned`. |
| M4 | `Reconcile.report` + lệnh CLI `reconcile` (ghi `_quality/raw_reconcile.json`, mã 1 nếu không cân) + asset Dagster `raw_reconcile` (metadata + cảnh báo). Chính sách: không chặn publish ngày hợp lệ, nhưng phải nhìn thấy. | PlatformSuite `review P5 M4` qua `Main.run`; `run-platform.sh` in file báo cáo (`unassigned: 1`, E1506). |
| M5 | `run-platform.sh` không xóa gì: `WORK` phải tồn tại, `mktemp -d` thư mục con, giữ log. | Chạy thật. |
| M6 | Mỗi bước Dagster/API được kiểm mã; `curl -f`; `trap` giữ mã thoát; tổng kết. | Probe Dagster giả trả 23 → script **exit 1**; chạy thật → exit 0. |
| N1 | Route bắt path trước, validate `from`/`to` (thiếu, lặp, sai, > 366 ngày tính cả hai đầu) → 400 có lý do. | PlatformSuite `review P5 N1` (không cần DB). |
| N2 | `Quality.check` kiểm tồn tại artifact trước, parse daily an toàn → vi phạm có dòng/lý do thay vì exception. | CoreSuite `review P5 N2`. |
| N3 | Quarantine Kafka thành phong bì JSON `{topic, partition, offset, reason, value}`; runbook tách 3a (file) / 3b (Kafka). | KafkaLandingSuite assert phong bì. |
| N4 | Bảng phiên bản + link dưới đây; đề xuất file dùng chung đã áp dụng (lead). | — |

**Thêm (Claude tự phát hiện khi chạy probe M6):** `kill` vào `scala-cli.bat` trên Windows không chạy shutdown hook → `postgres.exe` của `LocalPostgres` sống sót
giữ cổng 55432; lần chạy sau âm thầm dùng database CŨ (API trả dữ liệu của lần trước). Sửa: `LocalPostgres` dừng êm khi stdin đóng; script giữ stdin qua FIFO,
đóng trong `cleanup`, và từ chối chạy nếu cổng đang bận. Đồng bộ với Ch31: Asset tính trên **snapshot** input, `Materialization.id` + `InputRef.materializationId`.

**Phiên bản** (POM HTTP 200 trên repo1.maven.org, kiểm 17/10/2026): kafka-clients 4.4.0, cats-effect_3 3.7.1, doobie-core_3/doobie-postgres_3 1.0.0-RC13,
postgresql 42.7.14, http4s-dsl_3/ember-server_3/circe_3/ember-client_3 0.23.38, circe-parser_3/circe-generic_3 0.14.17, slf4j-nop 2.0.20, munit_3 1.2.4,
munit-cats-effect_3 2.2.0, embedded-kafka_3 4.4.0, embedded-postgres 2.2.2, embedded-postgres-binaries-windows-amd64 18.6.0, sbt-assembly 2.3.1. Dagster 1.13.26
(PyPI). Ví dụ link: https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.pom — các link khác cùng mẫu `group/artifact/version`.

**Kiểm chứng (lead):** `RUN_DB_TESTS=1 RUN_KAFKA_IT=1 scala-cli test project.scala core ingest serving app --server=false` → Core 9, Kafka 2, Platform 6 — 0 failed;
cùng biến môi trường `sbt test` → `Passed: Total 9 / 2 / 6`; `sbt app/assembly` → p5.jar.

## Vòng 3

**Codex sửa thay theo giao việc T24, 17/10/2026.** Phạm vi chỉ cơ chế vòng đời PostgreSQL nhúng trong script demo và README mục 8; không sửa file dùng chung, không commit, không chạy sbt. Các kết quả sbt/runtime Vòng 2 phía trên là của lead, không phải kiểm chứng bản stop-file mới.

**File đã sửa:**

- `14-projects/p5-data-platform/orchestration/LocalPostgres.scala`: bỏ vòng đọc stdin; nhận thêm run-dir do script tạo, kiểm marker `p5-run.owner`, ghi PID/thời điểm khởi động JVM và server (`jvm.pid/started`, `pg.pid/started`), `pg.ready`; poll file `stop` mỗi 200 ms; `pg.close()` trong finally. Chỉ ghi `pg.stopped` khi cổng đã đóng và `postmaster.pid` đã mất (zonky có thể nuốt lỗi pg_ctl khi close). Stop có sẵn lúc launcher còn compile thì JVM khởi động trễ không start server. Thêm flags `-deprecation -feature -Wunused:all`.
- `14-projects/p5-data-platform/orchestration/run-platform.sh`: bỏ FIFO/fd 3. Cleanup tạo stop, chờ launcher tối đa 30 giây, rồi fallback `pg_ctl -D <run>/pg-data -m immediate -t 10 -w stop` giới hạn ngoài 15 giây + kill-after 2 giây. Nếu cần, Windows chỉ taskkill PID có start-time/commandline trùng run-dir, PostgreSQL phải trỏ đúng pg-data; hỗ trợ đọc postmaster.pid khi startup hỏng trước marker. Unix kiểm commandline trước signal. Không kill theo tên postgres.exe hoặc theo cổng. Chờ API/launcher/cổng đều có hạn, curl và connect probe có timeout. Kiểm tra stale cuối và mã thoát rõ ràng: 0 khi mọi bước và cleanup ok, 1 nếu bất kỳ lỗi nào, kể cả timeout/mất marker dừng. Đường dẫn sang JVM/Python chuẩn hóa bằng cygpath trên Windows.
- `14-projects/p5-data-platform/README.md` mục 8, lỗi 14: mô tả nguyên nhân FIFO/EOF, stop-file/marker/deadline/fallback, phạm vi sở hữu, trạng thái **Chưa kiểm chứng runtime** và yêu cầu lead chạy thật.

Script không xóa dữ liệu. Mỗi lần tạo thư mục `p5-run.*` mới trong WORK; pg-data và cả thư mục parent/binaries mà zonky dùng cũng nằm trong run-dir; `setCleanDataDirectory(false)` giữ dữ liệu/log. Server có sẵn giữ 55432 trước startup thì trả 1, không đụng tiến trình đó. Cleanup giữ `stop` để JVM khởi động trễ thấy yêu cầu hủy. Fallback dừng cưỡng bức là lỗi của lần chạy và luôn giữ exit 1 dù sau đó đã giải phóng cổng.

**Lệnh/kiểm tra đã chạy thật** (Windows, JDK 21; `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`, TEMP/TMP trong `.cache/t24/tmp`):

| Lệnh/kiểm tra | Kết quả thật | Bằng chứng |
|---|---|---|
| `scala-cli.bat compile 14-projects/p5-data-platform/orchestration/LocalPostgres.scala --server=false` (root; chạy lại sau thêm PID server) | exit **0**, **0 cảnh báo compiler**; đây là compile, **0 test runtime được chạy** | `.cache/t24/p5-compile.log` (trống vì compile thành công) |
| `bash -n 14-projects/p5-data-platform/orchestration/run-platform.sh` (Git Bash) | exit **0**, gồm bản cuối có timeout connect/curl | `.team/progress/T24.md` |
| Tách heredoc PowerShell, `[System.Management.Automation.Language.Parser]::ParseInput(...)` | **0 lỗi cú pháp** | Tiến độ T24; kiểm cú pháp, không thực thi taskkill |
| `python .cache/t24/run_p5.py` — gọi Git Bash thật, SCALA_CLI=scala-cli.bat, DAGSTER trỏ venv thật; WORK tương đối trong cache, dùng jar có sẵn của lead | **script exit 1**, cleanup kết thúc và sentinel caller giữ nguyên; lỗi trước startup server, chưa tới Dagster/API | `.cache/t24/p5-runtime.{stdout,stderr,exit}.txt`, `.cache/t24/p5-work/p5-run.ZYiLoO/postgres.log` |
| Socket probe `connect_ex(('127.0.0.1', 55432))` sau lần thử lỗi | **10061** (connection refused), cổng không có listener | Output công cụ; không có server PostgreSQL mới nào đã start trong lượt bị chặn |

Lỗi thật của lượt thử: `java.nio.file.AccessDeniedException: C:\Users\VuTT\zero_to_hero_scala\.cache\t24\p5-work\p5-run.ZYiLoO`, tại `WindowsLinkSupport.getRealPath` → `LocalPostgres.scala:19` (`toRealPath`). Cleanup in `HỎNG: thiếu xác nhận PostgreSQL đã dừng`, `HỎNG: LocalPostgres/launcher trả lỗi`, rồi `== CLEANUP: exit=1; giữ dữ liệu/log tại ...`; không có wait treo. Lượt thử trước phần bổ sung marker PID server cuối, nhưng cùng điểm toRealPath; bản source cuối được compile và kiểm cú pháp lại.

**Chưa kiểm chứng runtime:** sandbox chặn resolve run-dir trước khi PostgreSQL start, nên chưa chạy được stop-file với PostgreSQL thật, nhánh pg_ctl/taskkill dự phòng, trường hợp thành công exit 0, lỗi giữa các partition Dagster exit 1, hoặc API trên bản script mới. Không có test PostgreSQL/Dagster thành công được tính cho Vòng 3. Lead cần chạy thật ngoài sandbox: (1) toàn luồng thành công, (2) wrapper Dagster lỗi ở partition giữa, partition cuối thành công, (3) yêu cầu cleanup lúc launcher còn startup, (4) giả lập JVM không poll stop để đi nhánh fallback; sau mỗi lượt kiểm không còn postgres.exe của run giữ 55432, chạy lại từ WORK có sentinel để kiểm dữ liệu caller nguyên vẹn. Không khẳng định kill cưỡng bức không thể thất bại khi OS từ chối quyền; trường hợp đó script báo lỗi và giữ log.

**Dependency/nguồn (17/10/2026):** giữ nguyên Scala 3.9.0, zonky embedded-postgres 2.2.2, binaries Windows/Linux 18.6.0, slf4j-nop 2.0.20. Compile dùng các dependency thật. Đối chiếu [source tag v2.2.2](https://github.com/zonkyio/embedded-postgres/blob/v2.2.2/src/main/java/io/zonky/test/db/postgres/embedded/EmbeddedPostgres.java) cho setDataDirectory, setOverrideWorkingDirectory, thư mục parent/cleanup và close nuốt lỗi stop; [PostgreSQL 18 pg_ctl](https://www.postgresql.org/docs/18/app-pg-ctl.html) cho `-D`, `-m immediate`, `-t`, `-w`. Không thay pin hay suy diễn compatibility mới.

**Đề xuất lead:** review cơ chế và chạy các runtime case trên trước khi cập nhật trạng thái chung. Không có đề xuất đổi glossary/compatibility matrix; PROGRESS chỉ nên ghi compile/cú pháp đạt, **Chưa kiểm chứng runtime Vòng 3** cho đến khi lead có kết quả thật.


## Vòng 4 (Codex sửa thay)

Ngày **17/10/2026**, theo giao việc T26 của lead. Sửa **R2-M1, R2-M2, R2-N1, R2-N2** trong review Vòng 2. Không chạy sbt, không commit, không sửa file dùng chung.

**File tạo/sửa trong P5:**

- `core/src/main/scala/vn/scalaeng/p5/Assets.scala`: `staging_orders` v3 phát hiện collision trên toàn snapshot raw trước chọn ngày; dependency tổng hợp `event-index / event_ids@day` gồm mọi nội dung chuẩn hóa của các ID chạm ngày, sắp xếp/khử trùng. Lô đến sau khác ngày làm stale ngày cũ; lô không liên quan không làm cũ ngày đó. Bản sai nghiệp vụ/ngày cũng tham gia chỉ mục (giữ nội dung đã decode khi không chuẩn hóa được); chỉ các dòng thuộc ngày đang tính mới được đếm vào `event_conflicts`. Mỗi ID xung đột có lý do trong `rejected.jsonl` của từng ngày; `unassigned` vẫn được đối soát riêng.
- `core/src/main/scala/vn/scalaeng/p5/Orchestrator.scala`: tra lịch sử theo asset/ngày/hash byte snapshot, chọn bản khớp gần nhất. Không chỉ xét latest rồi ghi None; asset thiếu byte/thiếu lịch sử khớp hash hoặc ref sai → `IllegalStateException` rõ trước compute/ghi downstream.
- `core/src/test/scala/vn/scalaeng/p5/CoreSuite.scala`: thêm **6 test**: khác ngày cùng file và đảo dòng; hai lô đến lần lượt; đảo hai lô; collision với bản sai nghiệp vụ/ngày; rematerialize xen giữa snapshot và refs, có hai bản cũ cùng byte; không có provenance khớp hoặc mất file thì fail trước output/lineage downstream.
- `serving/src/main/scala/vn/scalaeng/p5/Serving.scala`: tham số API dùng `Order.dayOf`, cùng quy tắc yyyy-MM-dd với staging/reconcile.
- `app/src/test/scala/vn/scalaeng/p5/PlatformSuite.scala`: thêm **1 test không cần DB**, gọi route với `%2B02026-10-15`, ngày không tồn tại, sai số chữ số → 400 có JSON error trước truy vấn DB.
- `README.md`: metadata đầu tài liệu; quy tắc toàn raw/fingerprint, provenance theo hash và chính sách nhiều bản cùng byte; số test mục 7, lỗi R2 mục 8; phân biệt bằng chứng runtime cũ của lead với kiểm chứng bản hiện tại.
- `.team/handoffs/P5-data-platform.md`: phần bàn giao Vòng 4 này, kèm từng URL POM chính xác bên dưới.

**Lệnh và output thật:** Scala **3.9.0**, JDK **21**, munit **1.2.4**, munit-cats-effect **2.2.0**. Cache Coursier dưới `.cache/coursier`, TEMP/TMP/java.io.tmpdir dưới `.cache/t26/`; log cuối thu trực tiếp byte UTF-8 từ JVM (`file.encoding/stdout.encoding/stderr.encoding=UTF-8`).

| Lệnh tại P5 | Kết quả thật | Log |
|---|---|---|
| `scala-cli.bat test project.scala core ingest serving app --server=false`, không bật DB/Kafka | **exit 0: 18 pass, 0 failed, 6 skipped**, 0 compiler warning | `.cache/t26/p5-utf8.log`, metadata lệnh/exit `.cache/t26/p5-utf8.json` |
| `RUN_KAFKA_IT=1`, không bật `RUN_DB_TESTS`; cùng lệnh | **exit 0: 20 pass, 0 failed, 4 skipped**, 0 compiler warning; Kafka KRaft thật | `.cache/t26/p5-kafka-utf8.log`, metadata lệnh/exit `.cache/t26/p5-kafka-utf8.json` |

Output summary của lượt Kafka thật:

```text
Test run vn.scalaeng.p5.CoreSuite finished: 0 failed, 0 ignored, 15 total 10.132s
Test run vn.scalaeng.p5.KafkaLandingSuite finished: 0 failed, 0 ignored, 2 total 5.573s
Test run vn.scalaeng.p5.PlatformSuite finished: 0 failed, 0 ignored, 7 total 0.616s
```

Platform có **3 pass + 4 skipped**, không tính cả 7 thành pass. Hai hint nâng munit không phải compiler warning; giữ chuẩn version repo.

**Giới hạn:** Không bật DB ở T26 vì review Vòng 2 đã tái hiện fixture PostgreSQL nhúng không start trong sandbox: timeout 10000 ms → SQLException connect failed → `ConnectException: Connection refused: getsockopt` (log `.cache/t25/p5-db-round2.log`). Không tuyên bố đã chạy lại DB ở T26, không suy đoán cơ chế chặn. Bốn test load/backfill/API có DB phải để lead chạy ngoài sandbox. Chưa chạy sbt/assembly, Dagster thật, Docker, stop-file/fallback runtime Vòng 3, nhiều writer/kill JVM/rebalance trong T26.

**Chính sách provenance:** Khi nhiều materialization cùng byte, chọn bản khớp hash mới nhất trong lịch sử đã đọc. Đây là định danh theo nội dung đã snapshot, không chứng minh writer vật lý nào đã tạo byte giống hệt. Không thêm khóa root hay tuyên bố giao dịch nhiều writer. Regression xen kẽ buộc latest đổi hash, vẫn giữ đúng ID lịch sử của byte giá 100 và status downstream stale so với upstream giá 900. Không tìm thấy bản khớp thì fail trước output, không ghi None cho asset. `daily_revenue` tăng codeVersion v1 → v2 để các partition cũ dựng lại refs ngay cả khi byte doanh thu không đổi; `staging_orders` v2 → v3 tương ứng quy tắc collision toàn raw.

**Dependency giữ nguyên pin; nguồn chính xác:** Lead đã kiểm HTTP 200 ngày 17/10/2026 theo brief T26; Codex kiểm lại **20/20 POM HTTP 200 ngày 17/10/2026** bằng Python urllib, parse XML kiểm artifact/version (version có thể kế thừa parent), ghi URL/status/SHA-256 tại `.cache/t26/pom-checks.json`. Bảng gồm cả binary Linux/macOS khai báo trong build.sbt; kiểm POM không phải xác nhận runtime trên các OS đó. sbt-assembly là bản plugin cross-publish cho Scala 2.12/sbt 1.0; không phải dependency `_3` của ứng dụng.

| Dependency (group:artifact) | Version | URL chính xác |
|---|---|---|
| `org.apache.kafka:kafka-clients` | 4.4.0 | [POM](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.pom) |
| `org.typelevel:cats-effect_3` | 3.7.1 | [POM](https://repo1.maven.org/maven2/org/typelevel/cats-effect_3/3.7.1/cats-effect_3-3.7.1.pom) |
| `org.typelevel:doobie-core_3` | 1.0.0-RC13 | [POM](https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/1.0.0-RC13/doobie-core_3-1.0.0-RC13.pom) |
| `org.typelevel:doobie-postgres_3` | 1.0.0-RC13 | [POM](https://repo1.maven.org/maven2/org/typelevel/doobie-postgres_3/1.0.0-RC13/doobie-postgres_3-1.0.0-RC13.pom) |
| `org.postgresql:postgresql` | 42.7.14 | [POM](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.pom) |
| `org.http4s:http4s-dsl_3` | 0.23.38 | [POM](https://repo1.maven.org/maven2/org/http4s/http4s-dsl_3/0.23.38/http4s-dsl_3-0.23.38.pom) |
| `org.http4s:http4s-ember-server_3` | 0.23.38 | [POM](https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/0.23.38/http4s-ember-server_3-0.23.38.pom) |
| `org.http4s:http4s-circe_3` | 0.23.38 | [POM](https://repo1.maven.org/maven2/org/http4s/http4s-circe_3/0.23.38/http4s-circe_3-0.23.38.pom) |
| `org.http4s:http4s-ember-client_3` | 0.23.38 | [POM](https://repo1.maven.org/maven2/org/http4s/http4s-ember-client_3/0.23.38/http4s-ember-client_3-0.23.38.pom) |
| `io.circe:circe-parser_3` | 0.14.17 | [POM](https://repo1.maven.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom) |
| `io.circe:circe-generic_3` | 0.14.17 | [POM](https://repo1.maven.org/maven2/io/circe/circe-generic_3/0.14.17/circe-generic_3-0.14.17.pom) |
| `org.slf4j:slf4j-nop` | 2.0.20 | [POM](https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/2.0.20/slf4j-nop-2.0.20.pom) |
| `org.scalameta:munit_3` | 1.2.4 | [POM](https://repo1.maven.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom) |
| `org.typelevel:munit-cats-effect_3` | 2.2.0 | [POM](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom) |
| `io.github.embeddedkafka:embedded-kafka_3` | 4.4.0 | [POM](https://repo1.maven.org/maven2/io/github/embeddedkafka/embedded-kafka_3/4.4.0/embedded-kafka_3-4.4.0.pom) |
| `io.zonky.test:embedded-postgres` | 2.2.2 | [POM](https://repo1.maven.org/maven2/io/zonky/test/embedded-postgres/2.2.2/embedded-postgres-2.2.2.pom) |
| `io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64` | 18.6.0 | [POM](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/18.6.0/embedded-postgres-binaries-windows-amd64-18.6.0.pom) |
| `io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64` | 18.6.0 | [POM](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-amd64/18.6.0/embedded-postgres-binaries-linux-amd64-18.6.0.pom) |
| `io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8` | 18.6.0 | [POM](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-darwin-arm64v8/18.6.0/embedded-postgres-binaries-darwin-arm64v8-18.6.0.pom) |
| `com.eed3si9n:sbt-assembly_2.12_1.0` | 2.3.1 | [POM](https://repo1.maven.org/maven2/com/eed3si9n/sbt-assembly_2.12_1.0/2.3.1/sbt-assembly_2.12_1.0-2.3.1.pom) |

Scala 3.9.0/JDK 21/sbt 1.12.11 và Dagster 1.13.26 giữ nguyên; nguồn Dagster: https://pypi.org/pypi/dagster/1.13.26/json (không cài/chạy lại ở T26). Không suy diễn hỗ trợ Scala 3 cho Spark; P5 không thêm Spark.

**Ngoài P5 theo cùng giao việc:** đổi pattern `a` → `adaptive` trong `solutions/ch22-code/src/main/scala/vn/scalaeng/solutions/ch22/Ch22Solutions.scala`; `scala-cli.bat test . --server=false` tại `solutions/ch22-code` → **exit 0, 8/8 pass, 0 compiler warning** (Scala 2.13.18, Spark 4.2.0, JDK 21). Log UTF-8 `.cache/t26/ch22-utf8.log`; Hadoop runtime WARN không phải compiler warning. Đưa nguyên khối metadata lên đầu **36** tài liệu audit T23a (33 chương + README P1/P3/P5; P2/P4 vốn đã ở đầu), giữ heading/nội dung; Ch27 cập nhật trạng thái theo review T13 của lead. Danh sách file đầy đủ: `.cache/t26/metadata-files.json` và progress T26.

**Đề xuất file dùng chung:** chỉ liệt kê trong `.team/progress/T26.md` theo giao việc; không sửa PROGRESS/glossary/compatibility-matrix/template hay README gốc. Lead cần review thay đổi và kiểm chứng DB/sbt/Dagster trước khi chốt P5; Codex không đánh dấu ✅ thay lead.
