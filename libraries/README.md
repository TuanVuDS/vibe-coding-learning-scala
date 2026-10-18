# Thư viện và công cụ của giáo trình

Tài liệu tra cứu: mỗi thư viện/công cụ mà giáo trình **thực sự dùng** (quét từ `project.scala`/`build.sbt` của repo — 55 tọa độ Maven, cộng Dagster,
PostgreSQL, Kafka broker, Docker, WSL2). Mỗi mục có cùng khung: là gì và giải quyết vấn đề gì · phiên bản đã chốt + link POM + mức hỗ trợ Scala 3 · khái niệm cốt
lõi · ví dụ trích từ lab đã chạy test (kèm `file:dòng`) · lựa chọn thay thế · lỗi thường gặp (lỗi thật, dẫn tới mục 10 của chương) · dùng ở chương nào ·
tài liệu chính thức.

Đọc chương trước, dùng trang này khi cần tra nhanh "thư viện này làm gì, bản nào, có Scala 3 không, thay bằng gì".

| Tài liệu | Phase | Nội dung chính |
|---|---|---|
| [tooling.md](tooling.md) | Xuyên suốt | JDK 21, Scala CLI, Coursier, sbt, munit, ScalaCheck (munit-scalacheck), scalafmt, sbt-assembly, Docker, WSL2, quy trình kiểm chứng |
| [phase-03-scala-core.md](phase-03-scala-core.md) | 3 — Scala 3 Core + Professional | dependency trực tiếp của phase, thư viện chuẩn Scala, cách đọc bằng chứng kiểm chứng |
| [phase-04-backend.md](phase-04-backend.md) | 4 — Backend (Ch12–18, P2) | Cats Effect, http4s, circe, Tapir + OpenAPI, ZIO/ZIO HTTP (so sánh), Doobie, Flyway, PostgreSQL JDBC/server, HikariCP, zonky embedded-postgres, Password4j, jwt-circe, log4cats/Logback, Prometheus metrics |
| [phase-05-data-spark.md](phase-05-data-spark.md) | 5 — Data Processing + Spark (Ch19–26, P3) | fs2, fs2-data, Cats, Commons CSV, Avro, Parquet, Hadoop, SLF4J NOP, Spark SQL (chỉ Scala 2.13), hadoop-bare-naked-local-fs |
| [phase-06-streaming.md](phase-06-streaming.md) | 6 — Kafka + Streaming (Ch27–30, P4) | kafka-clients, embedded-kafka, Kafka broker (KRaft), circe-generic |
| [phase-07-data-platform.md](phase-07-data-platform.md) | 7 — Data Platform + Performance (Ch31–34) | Dagster (Python), các thư viện dùng lại theo lab |
| [phase-08-projects.md](phase-08-projects.md) | 8 — Project 1–5 | stack từng project P1–P5, thành phần ngoài Maven, quy tắc chọn/debug khi ghép |

Biên soạn: Codex (task T27, [handoff](../.team/handoffs/T27-libraries.md) có bảng đối chiếu build ↔ tài liệu); review: Claude. Phiên bản và link POM kiểm ngày
18/10/2026 — khi nâng phiên bản trong build, cập nhật cả trang này và [compatibility matrix](../00-roadmap/04-compatibility-matrix.md).
