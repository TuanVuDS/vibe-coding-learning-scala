# Dự án thực hành

Thiết kế tổng thể của 5 project: [00-roadmap/06-projects-overview.md](../00-roadmap/06-projects-overview.md).

| Project | Sau chương | Trạng thái | Kiểm chứng |
|---|---|---|---|
| [P1 — Scala Data Processor](p1-scala-data-processor/) | 11 | ✅ Hoàn thành | Biên dịch `-Werror`, chạy thật, 35/35 test, golden tính tay |
| [P2 — Backend Service](p2-backend-service/) | 18 | ✅ Hoàn thành | 47 test sbt (18 với PostgreSQL thật), review chéo; Docker build/up/API/SIGTERM đã chạy thật (18/10/2026) |
| [P3 — Batch Data Pipeline](p3-batch-pipeline/) (Spark, Scala 2.13) | 26 | 🟡 Đã sửa theo review vòng 1, chờ vòng 2 | 18 test sbt; spark-submit WSL2, 1,2 triệu dòng |
| [P4 — Kafka Event Processing](p4-kafka-event-processing/) | 30 | ✅ Hoàn thành | 14 test sbt (`RUN_KAFKA_IT=1`, KRaft thật, kill JVM thật), review chéo 2 vòng |
| P5 — Production-Oriented Data Platform | 34 | ⏳ | |

Mỗi project có README gồm: mục tiêu, kiến thức tiên quyết, đặc tả, thiết kế và trade-off, cách chạy, output mong đợi,
kiểm thử, lỗi thường gặp, bài tập mở rộng và checklist tự đánh giá.
