# Curriculum Scala 3 cho Backend & Data Engineering

## Cách đọc curriculum

Mỗi chủ đề được gắn một trong ba mức:

| Mức | Ký hiệu | Ý nghĩa |
|---|---|---|
| Bắt buộc | **[BB]** | Mọi người học phải nắm trước khi sang giai đoạn sau. |
| Nên biết | **[NB]** | Cần cho công việc thực tế, có thể học song song hoặc quay lại sau. |
| Chuyên sâu | **[CS]** | Dành cho người muốn đi sâu (library author, tech lead, performance). Có thể bỏ qua lần học đầu. |

Có hai nhánh sau phần nền tảng: **BE** (Backend) và **DE** (Data Engineering).
Người chỉ đi một nhánh vẫn phải học đủ Giai đoạn 1 và 2.

> So với đề cương gốc, một số chủ đề được **dời vị trí** để đúng prerequisite.
> Các thay đổi được ghi ở cuối file (mục "Điều chỉnh so với đề cương").

---

## GIAI ĐOẠN 1 — SCALA 3 CORE (Scala 3.9 LTS, Scala CLI)

Thư mục: `01-scala3-core/`, `02-functional-programming/`, `03-type-system/`

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 1 | Introduction to Scala | Scala là gì, JVM, Scala 2 vs 3, Scala vs Java/Python/SQL, vai trò trong BE/DE | [BB] |
| 2 | Environment Setup | JDK 21, Coursier, Scala CLI, REPL, IDE (IntelliJ/Metals), sbt cơ bản, dependency | [BB] |
| 3 | Variables, Types, Expressions | val/var, kiểu cơ bản, Unit/Nothing/Null, inference, expression, control flow, interpolation, collection cơ bản | [BB] |
| 4 | Functions & Methods | def, lambda, function value, HOF, tham số (default/named/by-name), currying, recursion, `@tailrec`, composition | [BB] |
| 5 | OOP trong Scala | class, object, companion, trait, case class, enum, abstract class, access modifier, composition > inheritance | [BB] |
| 6 | Collections | List/Vector/Array/Set/Map/Tuple, immutable vs mutable, map/flatMap/filter/fold/groupBy/partition/collect, độ phức tạp, `view`/`iterator` | [BB] |
| 7 | Functional Programming | pure function, side effect, referential transparency, Option/Either/Try, pattern matching, ADT, tách logic thuần khỏi I/O | [BB] |
| 8A | Types cốt lõi | generics, type bounds, variance, union/intersection, type alias, opaque type, extension method | [BB] |
| 8B | Contextual Abstractions | given/using, type class, derivation cơ bản | [BB] cho BE, [NB] cho DE |
| 8C | Lập trình kiểu nâng cao | match types, inline, macros, metaprogramming | [CS] — học sau Ch16 |

## GIAI ĐOẠN 2 — PROFESSIONAL ENGINEERING (sbt 1.12, munit)

Thư mục: `04-professional-engineering/`, `12-testing/`

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 9 | Project Structure | package/import, multi-module sbt, dependency, config theo môi trường, logging, conventions, scalafmt, Git workflow, code review | [BB] |
| 10 | Testing | unit test (munit), property-based (ScalaCheck), test double, fixture, boundary, regression | [BB] |
| 11 | JVM Fundamentals | bytecode, class loading, heap/stack, GC, boxing, thread, JVM flags, profiling (JFR, async-profiler) | [NB]; GC tuning [CS] |

## GIAI ĐOẠN 3 — BACKEND ENGINEERING (nhánh BE)

Thư mục: `05-backend/`, `06-database/`, `07-concurrency/`

Stack chính: **Cats Effect 3 + http4s (Ember) + Tapir + circe + Doobie + PostgreSQL**.

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 12 | Backend Fundamentals | HTTP, REST, JSON, status code, routing, middleware, validation, authn/authz, logging, config, observability (khái niệm) | [BB] |
| 16A | Concurrency nền tảng | thread, Future, ExecutionContext, race, deadlock, timeout, cancellation (khái niệm) | [BB] — **học trước Ch13** |
| 16B | Effect system | Cats Effect: IO, Resource, Fiber, Ref, error handling, retry, timeout; so sánh ZIO | [BB] cho BE |
| 13 | Chọn framework | so sánh http4s / Play / Tapir / ZIO HTTP theo tiêu chí; lý do chọn stack | [NB] |
| 14 | Building REST APIs | route, codec, domain/service/repository, validation, pagination, filter, sort, versioning, OpenAPI, API test | [BB] |
| 15 | Database Engineering | SQL, PostgreSQL, schema, key, index, transaction, ACID, pooling (HikariCP), migration (Flyway), SQL injection, query plan; Doobie vs Slick | [BB] |
| 17 | Security & Production | password hashing, JWT/session, secrets, rate limit, CORS, HTTPS, metrics, health check, graceful shutdown | [BB] |
| 18 | Backend Architecture | layered, modular monolith, dependency inversion, functional core/imperative shell, caching, transaction boundary, resilience | [NB]; Clean Architecture đầy đủ [CS] |

## GIAI ĐOẠN 4 — DATA ENGINEERING FOUNDATION (nhánh DE, Scala 3)

Thư mục: `08-data-processing/`

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 19 | Scala cho Data Processing | đọc/parse CSV, JSON, nested record, event log; làm sạch, chuẩn hóa, dedup, dữ liệu thiếu/lỗi | [BB] |
| 20 | Functional Data Transformation | transformation thuần, validation tích lũy lỗi, data quality rule, generic transform, test transformation; **in-memory vs distributed vs streaming** | [BB] |
| 21 | Data Formats & Serialization | JSON, CSV, Avro, Parquet, schema, schema evolution, nullable, type conversion, compatibility | [BB] |

## GIAI ĐOẠN 5 — APACHE SPARK (Scala 2.13 + Spark 4.1.x)

Thư mục: `09-spark/`

> Chuyển sang Scala 2.13 ở đây. Chương 22 mở đầu bằng bài **"Từ Scala 3 về Scala 2.13: những gì thay đổi"**
> (cú pháp ngoặc nhọn, `implicit` thay cho `given`, không có enum/opaque/union type...).

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 22 | Spark Fundamentals | driver/executor/cluster, transformation/action, lazy, DAG, partition, shuffle, narrow/wide, fault tolerance, cache | [BB] |
| 23 | Spark SQL & DataFrames | SparkSession, DataFrame vs Dataset, schema, column expression, join, groupBy, window, null, UDF, Catalyst | [BB] |
| 24 | Batch Data Pipeline | 10 bước pipeline, partitioning, file layout, small files, skew, idempotency, rerun safety | [BB] |
| 25 | Spark Performance | query plan, partition sizing, join strategy, broadcast, skew, cache, memory, GC, serialization | [NB]; AQE chi tiết [CS] |
| 26 | Spark Production | config, logging, metrics, retry, data quality, test, deploy, version compatibility | [BB] |

## GIAI ĐOẠN 6 — STREAMING & KAFKA

Thư mục: `10-kafka-streaming/`

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 27 | Streaming Fundamentals | batch vs stream, event/processing time, ordering, late event, window, state, delivery semantics, backpressure | [BB] |
| 28 | Kafka với Scala | topic, partition, producer, consumer group, offset, serialization, Schema Registry, DLQ, lag, idempotent processing | [BB] |
| 29 | Streaming Pipeline | consumer trực tiếp (Scala 3 + fs2-kafka) vs Spark Structured Streaming (Scala 2.13); so sánh semantics | [BB] |
| 30 | Streaming Reliability | at-most/at-least/exactly-once và **điều kiện chính xác** của từng mức, replay, poison message | [BB]; Kafka transactions chi tiết [CS] |

## GIAI ĐOẠN 7 — DATA PLATFORM & PERFORMANCE

Thư mục: `11-data-platform/`, `13-performance/`

| Ch | Tên | Chủ đề chính | Mức |
|---|---|---|---|
| 31 | Data Pipeline Architecture | raw/staging/serving, data contract, lineage, backfill, orchestration vs processing (Dagster làm ví dụ — công cụ Python riêng) | [NB] |
| 32 | Advanced Distributed Processing | locality, serialization cost, network shuffle, distributed join, throughput vs latency | [CS] |
| 33 | Data Quality & Testing | golden dataset, uniqueness, referential integrity, regression, test late data/duplicate/rerun/backfill | [BB] |
| 34 | Performance & Cost | sizing, parallelism, đo đạc trước khi tối ưu, cost trade-off | [NB] |

## DỰ ÁN (thư mục `14-projects/`)

Xem chi tiết tại [06-projects-overview.md](06-projects-overview.md).

| Project | Sau chương | Scala |
|---|---|---|
| P1 Scala Data Processor | 10 | 3.9 |
| P2 Backend Service | 18 | 3.9 |
| P3 Batch Data Pipeline | 26 | 2.13 (Spark) |
| P4 Kafka Event Processing | 30 | 3.9 |
| P5 Production-Oriented Data Platform | 34 | 3.9 + 2.13 |

---

## Điều chỉnh so với đề cương gốc (và lý do)

1. **Ch16 tách làm 16A/16B và học trước Ch13.** Không thể so sánh http4s, Play, ZIO HTTP một cách
   có ý nghĩa khi chưa hiểu `Future` và effect system — vì sự khác biệt lớn nhất giữa các framework
   chính là mô hình concurrency của chúng.
2. **Ch8 tách làm 8A/8B/8C.** Match types, inline, macros không cần cho công việc BE/DE hằng ngày;
   đặt chúng ở mức [CS] sau khi người học đã dùng type class thực tế trong Cats/circe.
3. **Spark dùng Scala 2.13.** Bắt buộc theo tài liệu chính thức của Spark (xem compatibility matrix).
4. **Ch10 (Testing) gắn với `12-testing/`, Ch11 và Ch34 gắn với `13-performance/`** để giữ cấu trúc file theo yêu cầu.
5. **Mỗi chương từ Ch7 trở đi đều có cả ví dụ BE và DE** để hai nhánh luôn liên hệ được với nền tảng.
