# Danh sách công nghệ cần học

Nguyên tắc: **học ít công cụ nhưng hiểu sâu**. Mỗi nhu cầu chọn một công cụ chính; các lựa chọn
thay thế chỉ được giới thiệu để so sánh. Phiên bản xem tại [04-compatibility-matrix.md](04-compatibility-matrix.md).

Phân loại để tránh nhầm lẫn (yêu cầu của giáo trình):

- **Ngôn ngữ** = những gì có trong Scala 3 (cú pháp, kiểu, standard library).
- **Thư viện** = artifact bên ngoài, khai báo trong build (Cats Effect, http4s...).
- **Hệ thống** = phần mềm chạy độc lập (PostgreSQL, Kafka broker, Spark cluster).
- **Công cụ** = phục vụ phát triển/vận hành (sbt, Docker, Dagster).

## 1. Nền tảng (mọi người học)

| Công nghệ | Loại | Vai trò | Mức |
|---|---|---|---|
| JDK 21 | Hệ thống | Môi trường chạy (JVM) | [BB] |
| Scala 3.9 LTS + standard library | Ngôn ngữ | Toàn bộ Giai đoạn 1–4, BE, Kafka consumer | [BB] |
| Scala CLI | Công cụ | Chạy file đơn, script, bài tập | [BB] |
| sbt 1.12 | Công cụ | Build project nhiều module | [BB] |
| IntelliJ IDEA + Scala plugin **hoặc** VS Code + Metals | Công cụ | IDE | [BB] |
| Git | Công cụ | Version control | [BB] |
| munit | Thư viện | Unit test | [BB] |
| ScalaCheck (qua munit-scalacheck) | Thư viện | Property-based test | [NB] |
| scalafmt | Công cụ | Format code | [NB] |
| Docker + Docker Compose | Công cụ | PostgreSQL/Kafka local, đóng gói service | [BB] |

## 2. Nhánh Backend

| Công nghệ | Loại | Vai trò | Mức | Thay thế được giới thiệu |
|---|---|---|---|---|
| Cats (core) | Thư viện | Type class, Validated, NonEmptyList | [BB] | — |
| Cats Effect 3 | Thư viện | IO, Resource, Fiber, concurrency | [BB] | ZIO 2 |
| http4s 0.23 (Ember) | Thư viện | HTTP server/client | [BB] | Play, ZIO HTTP |
| Tapir 1.13 | Thư viện | Định nghĩa endpoint, OpenAPI | [BB] | viết route http4s thuần |
| circe | Thư viện | JSON codec | [BB] | jsoniter-scala |
| Doobie 1.0.0-RC | Thư viện | Truy cập SQL | [BB] | Slick |
| HikariCP | Thư viện (Java) | Connection pool | [BB] | — |
| Flyway | Thư viện/Công cụ | Database migration | [BB] | Liquibase |
| PostgreSQL | Hệ thống | Database quan hệ | [BB] | — |
| pureconfig hoặc ciris | Thư viện | Đọc cấu hình | [NB] | Typesafe Config |
| log4cats + logback | Thư viện | Logging | [BB] | — |
| Testcontainers | Thư viện | Integration test với DB thật | [BB] | — |
| Prometheus metrics / OpenTelemetry | Thư viện | Observability | [NB] | — |

**Vì sao chọn stack Typelevel (CE3 + http4s + Tapir + Doobie)?**

| Tiêu chí | Đánh giá |
|---|---|
| Hỗ trợ Scala 3 | Có artifact `_3` cho tất cả thành phần (xem matrix) |
| Tính ổn định | http4s 0.23 và CE3 đã ổn định nhiều năm; **Doobie vẫn RC** — rủi ro được chấp nhận và ghi rõ |
| Tài liệu | Tốt; Tapir sinh OpenAPI giúp API tự tài liệu hóa |
| Phù hợp người mới | **Trung bình** — đường cong học tập dốc hơn Play. Bù lại: Ch16A/16B dạy effect system trước khi dùng |
| Khả năng kiểm thử | Rất tốt: route là function `Request => IO[Response]`, test không cần mở port |
| Production | Ember + CE3 runtime dùng rộng rãi; graceful shutdown qua `Resource` |
| Tích hợp DB | Doobie cùng hệ sinh thái, transaction là giá trị `ConnectionIO` có thể compose |
| Độ phức tạp hệ sinh thái | Cao hơn Play; được kiểm soát bằng cách giới hạn số thư viện ở bảng trên |

Play Framework sẽ được trình bày ở Ch13 như lựa chọn "batteries included" cho đội muốn
mô hình MVC quen thuộc. ZIO được trình bày ở Ch16B như hệ sinh thái effect thay thế.

## 3. Nhánh Data Engineering

| Công nghệ | Loại | Vai trò | Mức |
|---|---|---|---|
| Scala 3 standard library (collections, `scala.io`, `java.nio.file`) | Ngôn ngữ | Xử lý dữ liệu trong bộ nhớ (Ch19–20) | [BB] |
| fs2 + fs2-data-csv / circe | Thư viện | Đọc file lớn dạng stream (Ch19–21) | [NB] |
| Avro, Parquet (thư viện Java) | Thư viện | Định dạng dữ liệu có schema | [BB] |
| **Scala 2.13** | Ngôn ngữ | Bắt buộc cho Spark | [BB] |
| Apache Spark 4.1 (Spark SQL, Structured Streaming) | Hệ thống + thư viện | Xử lý phân tán | [BB] |
| Apache Kafka 4.x (broker, KRaft) | Hệ thống | Event streaming | [BB] |
| kafka-clients 4.2 | Thư viện (Java) | Producer/consumer cơ bản | [BB] |
| fs2-kafka | Thư viện | Consumer/producer theo phong cách functional | [NB] |
| Schema Registry (Confluent hoặc Apicurio) | Hệ thống | Quản lý schema Avro | [NB] |
| MinIO | Hệ thống | Object storage tương thích S3 để chạy local | [NB] |
| Dagster | Công cụ (Python) | Ví dụ orchestration — **không thuộc Scala** | [NB] |
| Apache Flink | Hệ thống | Chỉ so sánh khái niệm (Scala API đã bị xóa từ 2.0) | [CS] |

## 4. Không nằm trong giáo trình

- Frontend: React, Angular, Vue, Scala.js, HTML/CSS — **ngoài phạm vi**.
- Akka (giấy phép BSL) — chỉ nhắc ở mức bối cảnh; Pekko được giới thiệu ở mức tùy chọn.
- Scala Native — ngoài phạm vi.
