# Tiến độ biên soạn

Cập nhật lần cuối: 17/10/2026 (chiều). Kế hoạch các phase còn lại: [.team/plan-phase6-9.md](.team/plan-phase6-9.md).

## Cách làm việc từ 16/10/2026: team Claude Code + Codex

Giáo trình được biên soạn bởi hai agent theo quy ước [AGENTS.md](AGENTS.md): **mỗi chương có một tác giả và một reviewer là agent còn lại**.
Không chương nào được đánh dấu ✅ khi chưa qua **review chéo** VÀ chưa chạy **test thật** (munit thật, `sbt test` do lead chạy ngoài sandbox).
Brief, handoff và review lưu trong `.team/` (briefs, handoffs, reviews) — đọc để biết vì sao một quyết định được đưa ra.

Review chéo đã tìm ra các lỗi thật mà test của tác giả bỏ sót, ví dụ: hai cách vượt qua thu hồi token và một cách vượt qua rate limit (Ch17),
bắt lỗi SQL làm transaction bị abort — SQLSTATE 25P02 (Ch15), tràn số ở tổng tiền (Ch15), handler trả HTTP 405 với body ghi 400 (Ch13).
Mỗi lỗi có regression test và được đưa vào mục "Lỗi thường gặp" của chương.

## Tổng quan theo phase

| Phase | Nội dung | Trạng thái |
|---|---|---|
| 1 | Nghiên cứu tài liệu chính thức, xác định chủ đề, kiểm chứng compatibility | ✅ Hoàn thành |
| 2 | Curriculum, prerequisite graph, stack, lịch học, project | ✅ Hoàn thành |
| 3 | Scala 3 Core (Ch1–8) + Professional (Ch9–11) | ✅ Ch1–11 + Ch8C chuyên sâu (Codex; review đạt, 4 MINOR đã sửa; 25 test + 7 ca biên dịch âm) |
| 4 | Backend Engineering (Ch12–18) | ✅ Ch12–18 xong (review chéo + test thật); P2 47/47 test với PostgreSQL thật |
| 5 | Data Processing (Ch19–21) và Spark (Ch22–26) | ✅ Ch19–26 (Ch24, Ch26: review chéo 2 vòng + vòng 2b; sbt 18/18, 13/13; spark-submit WSL2 chạy lại với jar mới) |
| 6 | Kafka và Streaming (Ch27–30) | ✅ Ch27–30 (review chéo 2 vòng; P4 `RUN_KAFKA_IT=1 sbt test` 14/14, KRaft thật, kill JVM thật) |
| 7 | Data Platform và Performance (Ch31–34) | ✅ Ch31–34 (Ch31: review 2 vòng, vòng 2 do Codex sửa thay; sbt 17/17, lời giải 10/10, Dagster chạy lại) |
| 8 | Project 1–5 | ✅ P1–P5 (P2: Docker build/up/API/SIGTERM chạy thật 18/10/2026). P5: review 2 vòng (6+2 MAJOR đã sửa), sbt 24/24 với PostgreSQL + Kafka thật, toàn luồng Dagster exit 0 |
| 9 | Kiểm tra chất lượng toàn bộ | ✅ Codex rà soát tự động (T23a): 56/56 lab build, 698 test pass; 4 lỗi chỉ do sandbox chặn PostgreSQL nhúng (lead chạy lại ngoài sandbox: pass); 1 cảnh báo Ch22 đã sửa; không chương nào thiếu mục, không liên kết hỏng |

## Phase 1–2 — đã hoàn thành

- [x] Kiểm chứng phiên bản Scala (3.9.0 LTS, 3.3.8 LTS, 3.10.0 Next, 2.13.18)
- [x] Kiểm chứng Spark ↔ Scala (Spark 4.1.x chỉ Scala 2.13; SPARK-54150 còn mở)
- [x] Kiểm chứng Flink (Scala API đã bị xóa ở 2.0)
- [x] Kiểm chứng stack backend (Cats Effect 3.7, http4s 0.23, Tapir 1.13, Doobie 1.0.0-RC13)
- [x] Kiểm chứng Kafka (4.2.2), fs2-kafka (3.x / 4.0.0-RC1 — **16/10/2026: 4.x đã GA, 4.1.3**), Pekko (Scala 3), sbt (1.12.11 / 2.0.0), munit (1.2.4)
- [x] `00-roadmap/` — 8 tài liệu

## Phase 3 — Scala 3 Core

| Chương | Bài học | Code | Test | Lời giải | Kiểm chứng |
|---|---|---|---|---|---|
| Ch1 Introduction | ✅ | ✅ (dùng chung Ch2) | — | ✅ | Đã chạy |
| Ch2 Environment | ✅ | ✅ | — | ✅ | Code đã chạy; lệnh cài đặt chưa chạy |
| Ch3 Variables/Types/Expressions | ✅ | ✅ | ✅ 8 test | ✅ code + 6 test | Đã chạy; test qua bản mô phỏng munit |
| Ch4 Functions & Methods | ✅ | ✅ | ✅ 12 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch5 OOP | ✅ | ✅ | ✅ 12 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch6 Collections | ✅ | ✅ | ✅ 12 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch7 Functional Programming | ✅ | ✅ | ✅ 13 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch8A Types cốt lõi | ✅ | ✅ | ✅ 10 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch8B Contextual Abstractions | ✅ | ✅ | ✅ 8 test | ✅ code + 5 test | Đã chạy; test qua bản mô phỏng munit |
| Ch9 Project Structure | ✅ | ✅ order-platform | ✅ (dùng chung Ch10) | ✅ md | Code đã chạy với -Werror; build.sbt/CI chưa chạy |
| Ch10 Testing | ✅ | ✅ testkit + 6 suite | ✅ 25 test | ✅ md + code | Đã chạy; munit qua bản mô phỏng |
| Ch11 JVM | ✅ | ✅ jvm-lab (8 thí nghiệm) | — | ✅ md + số liệu thật | Đã chạy lại toàn bộ thí nghiệm |

## Kiểm tra chính xác đã thực hiện ở phase này

- Mọi output trích trong bài là output thật từ `scalac 3.9.0` + OpenJDK 21.0.10.
- Các thông báo lỗi trong mục "Lỗi thường gặp" là output thật của compiler.
- Sửa hai điểm sai tiềm ẩn phát hiện nhờ chạy thật:
  1. `scala.math.BigDecimal(0.1)` cho `0.1` (khác `new java.math.BigDecimal(0.1)`) — đã viết lại phần giải thích.
  2. Định dạng `"%,d"` phụ thuộc Locale — `formatVnd` chỉ định `Locale.US`, có test.
- Xác nhận `List(1, "a", true)` được Scala 3.9 suy ra kiểu `List[Int | String | Boolean]`.
- Xác nhận đệ quy không tail-recursive gây `StackOverflowError` với 200.000 phần tử (stack mặc định).
- Ch5–7: thông báo lỗi thật cho abstract class, thiếu `override`, truy cập `private`, match không đầy đủ, `mapValues` deprecated,
  trộn `Option`/`Either` trong for-comprehension, `foldLeft` thiếu tham số kiểu với `Either`; exception runtime thật của `Map.apply`, `None.get`, `head`, `maxBy`.
- Ch8: lỗi thật cho opaque type, variance, invariance, upper bound, thiếu given, `import .*` không import given, given mơ hồ, strict equality.
- Ch8 — sửa một khẳng định sai trong bản nháp: Scala 3.9 **giữ** union type khi suy luận (`val`, `def`, `List(1, "a")` đều là `Int | String`).
- Ch8 — chứng minh type erasure: `Page(List("a"))` khớp `case p: Page[Int]` (cảnh báo E092).
- Ch9–11: phát hiện và XÓA các file tồn đọng từ một lần biên soạn trước không còn trong ngữ cảnh (module `pipeline`, `testkit` cũ);
  Ch9, Ch10 được viết lại theo code mới đã kiểm chứng; Ch11 + jvm-lab được GIỮ sau khi chạy lại toàn bộ thí nghiệm và đối chiếu số liệu.
- Ch9: lỗi thật khi extension method chưa được import giữa hai file (`Quantity.value`).
- Ch10: property sai tìm ra phản ví dụ thật ở lần thử đầu (seed 42); shrinking thu nhỏ từ 2 trường xuống 1.
- Ch11: độ sâu đệ quy không tỷ lệ thuận với -Xss (1m ≈ 2m) do JIT đổi kích thước frame — đưa vào đáp án.
- Xác nhận `List(1).sliding(3)` trả về một cửa sổ chứa 1 phần tử (cơ sở cho kiểm tra biên ở bài tập Ch6).

## Hạn chế đã biết

- [ ] Chạy test với munit thật (`scala-cli test .` / `sbt test`) trên máy có Internet.
- [ ] Chạy `build.sbt` bằng sbt 1.12.11.
- [ ] Thử lệnh cài đặt trên Windows, macOS, Linux.

## Project 1 — đã hoàn thành

- [x] Đặc tả input/quy tắc/output/exit code (README mục 3)
- [x] Core thuần: không `var`, không `while`, không I/O (đã grep kiểm tra)
- [x] CSV parser máy trạng thái + JSON parser recursive descent, tự viết, có property round-trip
- [x] Golden dataset: `report.csv`, `summary.json` tính tay TRƯỚC khi chạy — khớp 100%; `rejected.csv` rà soát từng dòng
- [x] Phát hiện thật trong lúc làm: cảnh báo infix (Scala 3.4+) làm fail `-Werror`; số điện thoại cố định bị loại (đổi dữ liệu mẫu, đưa vào bài mở rộng);
      user trùng thiếu JSON gốc trong `rejected.csv` (đã sửa); đường dẫn golden khi sbt fork (đã xử lý)
- [ ] Chạy bằng sbt thật (chờ người học xác nhận)

## Phase 4 — Backend Engineering

| Chương | Tác giả / Reviewer | Code | Test (thật) | Lời giải | Trạng thái |
|---|---|---|---|---|---|
| Ch12 Backend Fundamentals | (trước team) | http-lab (chỉ JDK) | 13 | ✅ code + md | ✅ |
| Ch16A Concurrency | (trước team) | concurrency-lab | 9 | ✅ | ✅ |
| Ch16B Cats Effect | Codex / Claude | effects-lab | 22 (sbt + CLI) | ✅ 12 test | ✅ review 2 vòng |
| Ch13 Chọn framework | Claude / Codex | framework-lab (http4s, Tapir, ZIO HTTP) | 18 | ✅ 6 test | ✅ review: 2 MAJOR đã sửa |
| Ch14 REST APIs | Codex / Claude | order-service (sbt 2 module) | 26 (sbt) | ✅ 6 test | ✅ review 2 vòng |
| Ch15 Database | Claude / Codex | db-lab (PostgreSQL 18.6 thật, nhúng) | 18 (sbt + CLI) | ✅ 7 test | ✅ review: 4 MAJOR đã sửa |
| Ch17 Security & Production | Claude / Codex | security-lab | 28 (sbt + CLI) | ✅ 7 test (gồm HTTPS thật) | ✅ review: 2 BLOCKER + 4 MAJOR đã sửa |
| Ch18 Architecture + **Project 2** | Codex / Claude | p2-backend-service (6 module sbt, Tapir + Doobie + Flyway + JWT) | 47 (sbt, gồm 18 test PostgreSQL thật) | ✅ 6 test | ✅ review: 2 MAJOR + 6 MINOR đã sửa; Docker chưa kiểm chứng |

Phát hiện thật trong Phase 4 (đã đưa vào bài):
- Race condition **không lộ ra** trên máy 1 CPU (`var` vẫn đúng 8.000.000); phải mở rộng cửa sổ race mới thấy mất ~75% cập nhật.
- `Future.sequence` các lời gọi chặn trên `global` vẫn tuần tự (~900 ms) trên máy 1 CPU; `blocking {}` hoặc pool riêng -> ~300 ms.
- Song song cho việc CPU không tăng tốc trên 1 CPU (x1.06).
- JDK HttpServer: tên header bị đổi hoa thường (`Content-type`), mã 422 có reason phrase rỗng.
- `NoSuchElementException` không có trong phạm vi mặc định của Scala 3.9.0.

## Phase 5 — Data Processing

| Chương | Tác giả / Reviewer | Code | Test (thật) | Lời giải | Trạng thái |
|---|---|---|---|---|---|
| Ch19 Scala cho Data Processing | Claude / Codex | ingest-lab (JSONL lồng nhau + CSV, fs2) | 20 (sbt + CLI) + golden tính tay | ✅ 8 test | ✅ review: đã sửa |
| Ch20 Functional Data Transformation | Claude / Codex | transform-lab (ValidatedNec, quality gate, CommutativeMonoid, 3 mô hình thực thi) | 16 (sbt, 5 property) | ✅ 9 test | ✅ review: 6 MAJOR + 4 MINOR đã sửa |
| Ch21 Data Formats | Codex / Claude | formats-lab (Avro, Parquet, schema evolution, benchmark 1 triệu dòng) | 27 (sbt) | ✅ 12 test | ✅ review 2 vòng |
| Ch22 Spark Fundamentals (Scala 2.13) | Claude / Codex | spark-fundamentals-lab (Spark 4.2.0 local) | 12 (sbt + CLI) | ✅ 8 test | ✅ review: 5 MAJOR + 3 MINOR đã sửa |
| Ch23 Spark SQL & DataFrames | Claude / Codex | spark-sql-lab | 18 (sbt + CLI) | ✅ 7 test | ✅ review: 7 MAJOR + 6 MINOR đã sửa |
| Ch24 Batch Data Pipeline | Claude / Codex | batch-pipeline-lab (10 bước, snapshot + dọn partition mồ côi, verify so nội dung, `TIMESTAMP_LTZ` tường minh) | 18 (sbt + CLI) | ✅ 6 test | ✅ review 2 vòng (4 MAJOR + M4 vòng 2 đã sửa) |
| **Project 3** Batch Data Pipeline | Claude / Codex | p3-batch-pipeline (2 nguồn, write-audit-publish có nhật ký INTENT/DONE + rollback, generator) | 19 (sbt + CLI) + spark-submit WSL2 | — | ✅ review 2 vòng (6 MAJOR + 2 MAJOR vòng 2 đã sửa) |
| Ch25 Spark Performance | Codex / Claude | spark-performance-lab (join strategy, skew/salting, AQE, spill, retry) | 13 (sbt) | ✅ 6 test | ✅ review: đạt, 3 MINOR |
| Ch26 Spark Production Engineering | Claude / Codex | spark-production-lab (config, log JSON, metric, retry, manifest ghi cuối với mã thoát thật, fat jar, spark-submit) + scala3-spark-probe | 13 (sbt + CLI) + spark-submit WSL2 | ✅ 6 test | ✅ review 2 vòng (7 MAJOR + 2 MAJOR vòng 2 đã sửa) |
| Ch27 Streaming Fundamentals | Codex / Claude | streaming-lab | 10 (sbt) | ✅ 7 test | ✅ review: đạt, 3 MINOR |
| Ch28 Kafka với Scala | Codex / Claude | kafka-lab (kafka-clients 4.4.0, KRaft nhúng) | 8 (sbt, gồm broker thật) | ✅ 7 test | ✅ review: đạt |
| Ch29 Streaming Pipeline | Codex / Claude | streaming-pipeline-lab (dùng lõi P4: receipt, snapshot nguyên tử, DLT, future guard) | 2 + lõi P4 | ✅ 6 test | ✅ review 2 vòng: 4 MAJOR đã sửa |
| Ch30 Streaming Reliability | Codex / Claude | reliability-lab (commit frontier, poll budget, recovery) + kill JVM thật trong P4 | 3 + lõi P4 | ✅ 6 test | ✅ review 2 vòng |
| **Project 4** Kafka Event Processing | Codex / Claude | p4-kafka-event-processing (producer CLI từ fixture, consumer, DLT, retry, KRaft nhúng) | 14 (sbt, `RUN_KAFKA_IT=1`) | — | ✅ review 2 vòng |

## Phase 7 — Data Platform và Performance

| Chương | Tác giả / Reviewer | Code | Test (thật) | Lời giải | Trạng thái |
|---|---|---|---|---|---|
| Ch8C Lập trình kiểu nâng cao (chuyên sâu) | Codex / Claude | ch08c-lab (inline, match types, Mirror derivation, macro) + 7 ca biên dịch âm | 12 | ✅ 13 test | ✅ review: đạt, 4 MINOR đã sửa |
| Ch31 Data Pipeline Architecture | Claude / Codex | architecture-lab (snapshot input, khóa root, lineage theo phiên bản, status vs freshness, backfill, impact) + Dagster 1.13.26 chạy thật | 17 (sbt + CLI) + 2 shell | ✅ 10 test | ✅ review 2 vòng (vòng 2: 5 MAJOR + 1 MINOR, Codex sửa thay, lead kiểm chứng) |
| Ch32 Advanced Distributed Processing | Codex / Claude | distributed-lab (Spark 4.2.0, Scala 2.13) | 11 (sbt + CLI) | ✅ 7 test | ✅ review 2 vòng (1 MAJOR sư phạm + 5 MINOR đã sửa) |
| Ch33 Data Quality và Testing | Claude / Codex | quality-lab (checks, contract FORWARD, golden, drift, property) | 18 (sbt + CLI) | ✅ 7 test | ✅ review 2 vòng (3 MAJOR + M3 vòng 2 đã sửa) |
| Ch34 Performance và Cost | Codex / Claude | cost-lab (latency/throughput/CPU/alloc đo thật, mô hình sizing, cost/million) | 15 (sbt + CLI) | ✅ 7 test | ✅ review 2 vòng (1 MAJOR sư phạm + 4 MINOR đã sửa) |

Phát hiện thật trong đợt này (đã đưa vào bài):
- **Scala 3.9 + Spark 4.2:** lỗi chỉ ở builder chung (reflection Scala 2 không đọc được stdlib 3.x: `class Array does not have a member apply`); gọi thẳng
  `classic.SparkSession.builder()` chạy được — Codex tìm ra, Claude chạy lại độc lập. Compatibility matrix + Ch26 mục 3.8 đã sửa; giáo trình vẫn dùng 2.13.
- **Windows + Spark 4.2:** `spark.sql.artifact.isolation.enabled=true` (mặc định) làm executor tải class qua RPC, +1.650 socket TIME_WAIT / 60 truy vấn → cạn cổng
  loopback, test lỗi ngẫu nhiên. Các lab batch đặt `false`.
- Review chéo bắt các lỗi "kiểm tra xanh nhưng dữ liệu sai" ở Ch24/P3 (verify chỉ so tổng), partition mồ côi khi giảm `upTo`/rút trọn một ngày, publish nửa chừng
  không phục hồi được, quy tắc compatibility contract sai hướng (Ch33), một event tương lai làm mọi event sau thành `late` vĩnh viễn (P4).
- Vòng 2 của review còn bắt: `AS TIMESTAMP` là alias theo cấu hình phiên (NTZ bỏ offset — Ch24/P3), manifest ghi `exit_code: 0` trước bước có thể hỏng (Ch26),
  ghi nhật ký hỏng sau move làm mất rollback (P3), dấu phẩy cuối dòng lọt contract (Ch33).
- Ch31/P5: "fresh" cục bộ ≠ fresh tận nguồn; impact/provenance phải theo **phiên bản** upstream; kiểm "input đổi trong lúc tính" bằng so hai đầu publish trước khi
  kiểm và không bắt A→B→A → chuyển sang snapshot; CSV tự ghép chuỗi làm mất doanh thu mà gate vẫn xanh (P5); `kill` `scala-cli.bat` trên Windows để lại
  `postgres.exe` mồ côi giữ cổng — lần chạy sau âm thầm dùng database cũ (P5, Claude tự phát hiện khi chạy probe).

## Việc tiếp theo

Không còn việc tồn đọng. Docker đã kiểm chứng 18/10/2026: Ch15 `docker-compose.yml` (DbDemo với `postgres:18.6`) và Project 2 (build, API, SIGTERM, restart giữ dữ liệu).
