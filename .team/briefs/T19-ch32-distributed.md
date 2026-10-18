# Brief T19 — Chương 32 Advanced Distributed Processing

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Ch32 + template 17 mục), `00-roadmap/04-compatibility-matrix.md` (Spark chỉ Scala 2.13; Scala 3.8+ không chạy Spark 4.2),
  Ch22–Ch26 trong `09-spark/` và review `.team/reviews/T10-*`, `T11-*`, `T12-*`: khẳng định đã bị bắt lỗi KHÔNG được lặp lại. Ch25 đã dạy join strategy, skew/salting,
  AQE, spill, cache — Ch32 **không lặp lại** mà nhìn ở tầng hệ phân tán: chi phí dữ liệu di chuyển, mô hình lỗi, và trade-off throughput/latency
  (nối với Ch27–Ch30: so sánh micro-batch vs record-at-a-time ở mức khái niệm).

## Phạm vi được phép ghi
- `11-data-platform/ch32-advanced-distributed-processing.md`
- `11-data-platform/distributed-lab/` (Scala 2.13.18, Spark 4.2.0 `_2.13`, munit 1.2.4; `project.scala` + `build.sbt` + `project/build.properties` sbt 1.12.11, theo spark-performance-lab)
- `solutions/ch32-solutions.md`, `solutions/ch32-code/`, `.team/handoffs/T19-ch32-distributed.md`, `.cache/`
- KHÔNG ghi `11-data-platform/quality-lab`, `ch31-*`, `ch33-*` (Claude đang làm).

## Nội dung bắt buộc
Distributed execution (driver/executor/task, stage boundary), partitioning (hash/range, partitioner được giữ hay mất), data locality (PROCESS_LOCAL/NODE_LOCAL/ANY,
`spark.locality.wait` — nói rõ local mode chỉ thấy một phần), serialization cost (closure capture, task size warning, Java vs Kryo cho RDD, đo bytes thật),
network shuffle (shuffle write/read bytes, số file, fetch), skew (chỉ tóm tắt + link Ch25), memory pressure (execution vs storage memory, spill),
fault tolerance (lineage, tính lại stage, checkpoint vs cache), retry behavior (`local[N,F]`, task vs stage retry, side effect không idempotent bị nhân đôi — thí nghiệm thật),
distributed joins (co-partitioned join tránh shuffle, bucketing — kiểm tra plan thật), aggregation strategies (map-side combine: `reduceByKey` vs `groupByKey`,
partial/final HashAggregate, approx_count_distinct vs count distinct có số đo), throughput vs latency (batch size/partition count ảnh hưởng thời gian đầu tiên vs tổng).

Mỗi chủ đề: vấn đề → vì sao → cách hoạt động → đo bằng gì (plan, task metrics qua listener đồng bộ đúng, shuffle bytes) → thay thế → ưu/nhược → khi nào dùng → lỗi thiết kế.
Ghi rõ local mode một máy: không suy ra số tuyệt đối cho cluster; ghi số CPU của máy khi đo.

## Tiêu chí xong
- Đủ 17 mục; mọi output/plan/số đo là output thật; ≥6 bài tập có lời giải + test.
- `scala-cli.bat test` pass (`--server=false`, COURSIER_CACHE/tmp/SPARK_LOCAL_DIRS trong `.cache/`), 0 cảnh báo; sbt để Claude chạy.
- Handoff đầy đủ (lệnh, output, phiên bản + nguồn + ngày, phần chưa chạy, điểm reviewer cần soi, đề xuất glossary).
