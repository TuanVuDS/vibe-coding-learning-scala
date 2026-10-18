# Thư viện — Phase 8: Các project tổng hợp P1–P5

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| P1: munit | 1.2.4 | `_3` | Test core/app; không có runtime library ngoài | P1 |
| P2: stack backend | CE 3.7.1, http4s 0.23.38, Tapir 1.13.33, Doobie RC13 | `_3` + Java | API + transaction + bảo mật | P2 |
| P3: Spark SQL | 4.2.0 | **Chỉ `_2.13`** | Batch nhiều nguồn, publish/recovery | P3 |
| P4: Kafka clients / embedded-kafka | 4.4.0 | Java / `_3` | Event/receipt/snapshot, broker test | P4 |
| P5: stack data platform | Circe 0.14.17, Kafka 4.4.0, Doobie RC13, http4s 0.23.38 | `_3` + Java | Ingestion → lake → serving | P5 |
| Dagster | 1.13.26 | Python | Điều phối P5 | P5 |
| PostgreSQL server | 18.6 | Ngoài JVM | Lưu dữ liệu P2/P5 | P2/P5 |
| sbt-assembly | 2.3.1 | Plugin sbt 1.x | Đóng gói jar | P2/P3/P5 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

Các project **không thêm họ thư viện Maven mới** so với tooling/phase 4–7. Vì vậy phần dưới liên kết về mô tả đầy đủ, POM và bẫy của từng thư viện; không lặp lại toàn bộ 8 mục cho dependency dùng lại. Bảng dependency là hợp của `project.scala`, `build.sbt`, `project/plugins.sbt` trong **từng project**, giữ cả dependency chỉ có ở sbt; không mặc định hai build đồng nhất.

P2/P3/P4 được tính vào phase 4/5/6 khi xác định thư viện lần đầu. Ở đây chúng xuất hiện để người đọc tra project hoàn chỉnh, không tạo thêm dependency hoặc đổi version. Dagster/PostgreSQL là thành phần ngoài Maven; ghi riêng sau các bảng.

## P1 — Scala Data Processor

**Môi trường:** Scala 3.9.0/JDK 21; chỉ munit ở Test.

P1 tự viết CSV state machine và JSON parser để học functional core, validation và golden test. Không thêm Circe/Commons CSV/fs2 vào danh sách chỉ vì project có đọc các định dạng này; đó là bài thiết kế parser, không phải khuyến nghị tự viết parser cho mọi backend.

[README](../14-projects/p1-scala-data-processor/README.md), [build.sbt](../14-projects/p1-scala-data-processor/build.sbt). Có **35/35 test munit thật** được ghi ở README (chạy lại 16/10/2026); P1 không có project.scala và không nằm trong phạm vi 56 lab của audit T23a. T27 không tạo directive hay chạy sbt để thay đổi phạm vi này.

Nguồn: [14-projects/p1-scala-data-processor/modules/core/src/test/scala/vn/scalaeng/p1/ParserSuite.scala:12](../14-projects/p1-scala-data-processor/modules/core/src/test/scala/vn/scalaeng/p1/ParserSuite.scala#L12). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p1-scala-data-processor/modules/core/src/test/scala/vn/scalaeng/p1/ParserSuite.scala:12-19 -->
```scala
  test("order hợp lệ"):
    val o = OrderParser.parseRow(2, "ORD-1,U-1,AO-01,120000,2,PAID,2026-10-01,2026-10-01T02:00:00Z")
    assertEquals(o.map(x => (x.orderId.value, x.total.dong, x.lineNo)), Right(("ORD-1", 240000L, 2)))

  test("order: TẤT CẢ lỗi được thu thập"):
    val r = OrderParser.parseRow(3, "x,y,z z,abc,0,SHIPPED,2026-13-01,yesterday")
    assertEquals(fieldsOf(r), List("order_id", "user_id", "sku", "unit_price", "quantity", "status", "order_date", "updated_at"))

```

| Dependency/plugin trực tiếp (artifact đầy đủ) | Vai trò và mô tả/POM đã kiểm |
|---|---|
| `org.scalameta:munit_3:1.2.4` | [Tra thư viện](tooling.md#munit) |

## P2 — Backend Service

**Môi trường:** Scala 3.9.0/JDK 21; PostgreSQL 18.6.

P2 ghép domain, service, db, security, http và app thành graph nhiều module. Dependency HTTP/database không nên rò vào domain; HikariCP 7.0.2 là khai báo trực tiếp ở db dù Ch15 đã dùng pool bắc cầu qua Doobie. Assembly phải giữ service loader cho JDBC/Flyway.

[README](../14-projects/p2-backend-service/README.md), [build.sbt](../14-projects/p2-backend-service/build.sbt). Bằng chứng test ở [handoff T07](../.team/handoffs/T07-ch18-p2-backend.md); README mục 6 có Docker build/up, API, SIGTERM và restart thật ngày 18/10/2026. Các bằng chứng đó không được gán thành lệnh T27 đã chạy.

Nguồn: [14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala:32](../14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala#L32). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p2-backend-service/db/src/main/scala/vn/scalaeng/p2/db/Database.scala:32-36 -->
```scala
  def runner(xa: Transactor[IO]): TransactionRunner[ConnectionIO] = new TransactionRunner[ConnectionIO]:
    def run[A](program: ConnectionIO[A]): IO[A] = program.transact(xa)
  def ready(xa: Transactor[IO]): IO[Boolean] =
    (sql"set local statement_timeout = '800ms'".update.run *>
      sql"select 1".query[Int].unique).transact(xa).map(_ == 1).timeout(1.second).handleError(_ => false)
```

| Dependency/plugin trực tiếp (artifact đầy đủ) | Vai trò và mô tả/POM đã kiểm |
|---|---|
| `ch.qos.logback:logback-classic:1.6.5` | [Tra thư viện](phase-04-backend.md#logging) |
| `com.eed3si9n:sbt-assembly_2.12_1.0:2.3.1` | [Tra thư viện](tooling.md#assembly) |
| `com.github.jwt-scala:jwt-circe_3:11.0.4` | [Tra thư viện](phase-04-backend.md#jwt) |
| `com.password4j:password4j:1.8.4` | [Tra thư viện](phase-04-backend.md#password4j) |
| `com.softwaremill.sttp.apispec:openapi-circe-yaml_3:0.11.10` | [Tra thư viện](phase-04-backend.md#apispec) |
| `com.softwaremill.sttp.tapir:tapir-http4s-server_3:1.13.33` | [Tra thư viện](phase-04-backend.md#tapir) |
| `com.softwaremill.sttp.tapir:tapir-json-circe_3:1.13.33` | [Tra thư viện](phase-04-backend.md#tapir) |
| `com.softwaremill.sttp.tapir:tapir-openapi-docs_3:1.13.33` | [Tra thư viện](phase-04-backend.md#tapir) |
| `com.zaxxer:HikariCP:7.0.2` | [Tra thư viện](phase-04-backend.md#hikari) |
| `io.circe:circe-core_3:0.14.17` | [Tra thư viện](phase-04-backend.md#circe) |
| `io.zonky.test:embedded-postgres:2.2.2` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `org.flywaydb:flyway-core:13.10.3` | [Tra thư viện](phase-04-backend.md#flyway) |
| `org.flywaydb:flyway-database-postgresql:13.10.3` | [Tra thư viện](phase-04-backend.md#flyway) |
| `org.http4s:http4s-dsl_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.http4s:http4s-ember-client_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.http4s:http4s-ember-server_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.postgresql:postgresql:42.7.14` | [Tra thư viện](phase-04-backend.md#jdbc) |
| `org.scalameta:munit_3:1.2.4` | [Tra thư viện](tooling.md#munit) |
| `org.typelevel:cats-effect-testkit_3:3.7.1` | [Tra thư viện](phase-04-backend.md#effect-testing) |
| `org.typelevel:cats-effect_3:3.7.1` | [Tra thư viện](phase-04-backend.md#cats-effect) |
| `org.typelevel:doobie-core_3:1.0.0-RC13` | [Tra thư viện](phase-04-backend.md#doobie) |
| `org.typelevel:doobie-hikari_3:1.0.0-RC13` | [Tra thư viện](phase-04-backend.md#doobie) |
| `org.typelevel:doobie-postgres_3:1.0.0-RC13` | [Tra thư viện](phase-04-backend.md#doobie) |
| `org.typelevel:log4cats-slf4j_3:2.8.0` | [Tra thư viện](phase-04-backend.md#logging) |
| `org.typelevel:munit-cats-effect_3:2.2.0` | [Tra thư viện](phase-04-backend.md#effect-testing) |

## P3 — Batch Data Pipeline

**Môi trường:** Scala **2.13.18**/JDK 21; Spark 4.2.0.

P3 dùng Spark cho đọc/join/aggregate nhiều nguồn và kiểm publish/recovery bằng test. Spark ở scope Provided do môi trường submit cấp; file output là ranh giới với ứng dụng Scala 3, không chia sẻ classpath `_3`/`_2.13`.

[README](../14-projects/p3-batch-pipeline/README.md), [build.sbt](../14-projects/p3-batch-pipeline/build.sbt). Bằng chứng [handoff P3](../.team/handoffs/P3-batch-pipeline.md) gồm test và spark-submit WSL2; local test không thay cho kiểm jar đúng scope, mã thoát và publish khi bị lỗi.

| Dependency/plugin trực tiếp (artifact đầy đủ) | Vai trò và mô tả/POM đã kiểm |
|---|---|
| `com.eed3si9n:sbt-assembly_2.12_1.0:2.3.1` | [Tra thư viện](tooling.md#assembly) |
| `com.globalmentor:hadoop-bare-naked-local-fs:0.1.0` | [Tra thư viện](phase-05-data-spark.md#bare-local-fs) |
| `org.apache.spark:spark-sql_2.13:4.2.0` | [Tra thư viện](phase-05-data-spark.md#spark) |
| `org.scalameta:munit_2.13:1.2.4` | [Tra thư viện](tooling.md#munit) |

## P4 — Kafka Event Processing

**Môi trường:** Scala 3.9.0/JDK 21; Kafka client/broker test 4.4.0.

P4 thêm receipt/snapshot bền để tách effect đã xử lý khỏi offset đã commit. Kafka producer idempotence không thay dedup event, DLT ack không cùng transaction với file snapshot; mỗi khoảng lỗi cần bằng chứng riêng, kể cả JVM bị kill giữa chừng.

[README](../14-projects/p4-kafka-event-processing/README.md), [build.sbt](../14-projects/p4-kafka-event-processing/build.sbt). Bằng chứng [handoff T15](../.team/handoffs/T15-ch29-30-p4.md) và [review T15](../.team/reviews/T15-ch29-30-p4-review.md) ghi broker KRaft, kill JVM thật và các sửa lỗi future timestamp. Duplicate JSON key vẫn là giới hạn được ghi ở Ch29 mục 10.

Nguồn: [14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala:34](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala#L34). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala:34-43 -->
```scala
  def restore(raw: String): State = decode[State](raw).fold(
    e => throw new IllegalStateException(s"snapshot corrupt: ${e.getMessage}"), identity)

  def event(raw: Option[String]): Either[String, Event] =
    raw.toRight("null value is not an order event").flatMap { text =>
      parse(text).left.map(_ => "invalid JSON").flatMap { json =>
        val allowed = Set("version", "eventId", "orderId", "eventTime", "amount")
        if !json.asObject.exists(_.keys.toSet == allowed) then Left("schema fields mismatch")
        else json.as[Event].left.map(_ => "schema types mismatch").flatMap { e =>
          if e.version != 1 then Left("unsupported version")
```

| Dependency/plugin trực tiếp (artifact đầy đủ) | Vai trò và mô tả/POM đã kiểm |
|---|---|
| `io.circe:circe-generic_3:0.14.17` | [Tra thư viện](phase-06-streaming.md#circe-generic) |
| `io.circe:circe-parser_3:0.14.17` | [Tra thư viện](phase-04-backend.md#circe) |
| `io.github.embeddedkafka:embedded-kafka_3:4.4.0` | [Tra thư viện](phase-06-streaming.md#embedded-kafka) |
| `org.apache.kafka:kafka-clients:4.4.0` | [Tra thư viện](phase-06-streaming.md#kafka-clients) |
| `org.scalameta:munit_3:1.2.4` | [Tra thư viện](tooling.md#munit) |
| `org.slf4j:slf4j-nop:2.0.20` | [Tra thư viện](phase-05-data-spark.md#slf4j-nop) |

## P5 — Data Platform

**Môi trường:** Scala 3.9.0/JDK 21; PostgreSQL 18.6; Dagster 1.13.26/Python 3.10–3.14.

P5 ghép ingest Kafka/file, lake metadata, transform, quality gate và serving PostgreSQL/HTTP. Dagster gọi CLI Scala qua subprocess; freshness/backfill phải dựa lineage toàn đường chứ không chỉ run xanh. Build sbt giữ dependency theo module, trong khi Scala CLI có thể nhìn toàn bộ source nên không chứng minh được ranh giới module.

[README](../14-projects/p5-data-platform/README.md), [build.sbt](../14-projects/p5-data-platform/build.sbt). Bằng chứng [handoff P5](../.team/handoffs/P5-data-platform.md) và [review P5](../.team/reviews/P5-review.md) có test PostgreSQL/Kafka, toàn luồng Dagster và các lỗi được sửa sau review. Bảng dưới giữ cả gói PostgreSQL Linux/macOS chỉ khai báo ở sbt; directive P5 chỉ khai báo binary Windows.

Nguồn: [14-projects/p5-data-platform/orchestration/defs.py:23](../14-projects/p5-data-platform/orchestration/defs.py#L23). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p5-data-platform/orchestration/defs.py:23-31 -->
```python
def p5(context: dg.AssetExecutionContext, *args: str) -> str:
    cmd = [JAVA, "-Dstdout.encoding=UTF-8", "-jar", JAR, *args, f"--root={LAKE}", f"--run-id=dagster-{context.run_id[:8]}"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    out = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
    if r.returncode != 0:
        # Mã thoát có nghĩa (README mục 3): 3 = file bị cách ly, 4 = quality gate chặn — hiện lên Dagster như một run thất bại có lý do.
        raise dg.Failure(description=f"p5 {args[0]} trả mã {r.returncode}: {out or r.stderr.strip()[-500:]}")
    return out

```

| Dependency/plugin trực tiếp (artifact đầy đủ) | Vai trò và mô tả/POM đã kiểm |
|---|---|
| `com.eed3si9n:sbt-assembly_2.12_1.0:2.3.1` | [Tra thư viện](tooling.md#assembly) |
| `io.circe:circe-generic_3:0.14.17` | [Tra thư viện](phase-06-streaming.md#circe-generic) |
| `io.circe:circe-parser_3:0.14.17` | [Tra thư viện](phase-04-backend.md#circe) |
| `io.github.embeddedkafka:embedded-kafka_3:4.4.0` | [Tra thư viện](phase-06-streaming.md#embedded-kafka) |
| `io.zonky.test:embedded-postgres:2.2.2` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0` | [Tra thư viện](phase-04-backend.md#embedded-postgres) |
| `org.apache.kafka:kafka-clients:4.4.0` | [Tra thư viện](phase-06-streaming.md#kafka-clients) |
| `org.http4s:http4s-circe_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.http4s:http4s-dsl_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.http4s:http4s-ember-client_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.http4s:http4s-ember-server_3:0.23.38` | [Tra thư viện](phase-04-backend.md#http4s) |
| `org.postgresql:postgresql:42.7.14` | [Tra thư viện](phase-04-backend.md#jdbc) |
| `org.scalameta:munit_3:1.2.4` | [Tra thư viện](tooling.md#munit) |
| `org.slf4j:slf4j-nop:2.0.20` | [Tra thư viện](phase-05-data-spark.md#slf4j-nop) |
| `org.typelevel:cats-effect_3:3.7.1` | [Tra thư viện](phase-04-backend.md#cats-effect) |
| `org.typelevel:doobie-core_3:1.0.0-RC13` | [Tra thư viện](phase-04-backend.md#doobie) |
| `org.typelevel:doobie-postgres_3:1.0.0-RC13` | [Tra thư viện](phase-04-backend.md#doobie) |
| `org.typelevel:munit-cats-effect_3:2.2.0` | [Tra thư viện](phase-04-backend.md#effect-testing) |

## Thành phần ngoài Maven và ranh giới triển khai

**PostgreSQL 18.6** — [server, driver và kiểm chứng riêng](phase-04-backend.md#postgres-server); P2 chạy migration Flyway, P5 có schema/load phục vụ riêng, không suy rằng P5 dùng Flyway vì P2 có Flyway.

**Dagster 1.13.26 (Python)** — [mô tả đầy đủ, PyPI, partition/timezone và bẫy](phase-07-data-platform.md#dagster); chỉ P5 có orchestration Dagster trong nhóm P1–P5 này, không phải dependency Scala cần thêm vào build.

**Docker/WSL2/JDK/sbt/Scala CLI** — [tooling](tooling.md); Docker P2 và spark-submit P3 là đường kiểm triển khai riêng. Chạy test bằng Scala CLI không xác nhận `dependsOn`/scope/assembly của sbt; T27 không chạy sbt theo yêu cầu task.

## Quy tắc chọn và debug khi ghép project

Theo dõi dữ liệu ở ranh giới thay vì đoán một thư viện bảo đảm mọi thứ: HTTP JSON cần codec + schema + validation; database cần driver + pool + transaction + migration; Kafka cần offset + sink bền + policy replay; orchestrator cần mã thoát thật và lineage có phiên bản. Khi gặp lỗi, đọc mục 10 của chương thư viện qua các liên kết trên; project README dùng khuôn riêng nên mục lỗi của P1/P2/P5 không nhất thiết mang số 10.

Ví dụ bẫy đã có bằng chứng ở chương: Trích nguyên văn mục 10: “ERROR: current transaction is aborted, commands ignored until end of transaction block” — [06-database/ch15-database-engineering.md:575](../06-database/ch15-database-engineering.md#L575). Bắt exception ở Scala không đưa transaction PostgreSQL về trạng thái dùng tiếp được; xử lý conflict hoặc transaction boundary theo chương.

Không nâng đồng loạt dependency để “thống nhất số”: adapter munit-cats-effect giữ 2.2.0, munit 1.2.4; Spark giữ Scala 2.13; slf4j-nop 2.0.17/2.0.20 là lựa chọn từng lab. Mọi version trong bảng đều lấy từ build, POM đã kiểm HTTP ngày 18/10/2026 ở phần mô tả gốc.
