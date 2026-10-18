# Handoff T09 — Chương 20: Functional Data Transformation (tác giả: Claude Code)

## File
- `08-data-processing/ch20-functional-data-transformation.md` (17 mục)
- `08-data-processing/transform-lab/` (project.scala + build.sbt; data/; 12 test gồm 4 property ScalaCheck)
- `solutions/ch20-solutions.md`, `solutions/ch20-code/` (6 test)

## Kiểm chứng (17/10/2026)
- scala-cli test + sbt test: 12/12. Lời giải 6/6. Classpath test: munit_3-1.2.4 (munit-scalacheck 1.2.0 bị nâng munit 1.1.1 → 1.2.4).
- TransformDemo khớp bảng tính tay mục 6. ExecutionBench 1 triệu dòng: số đo thật (máy 12 CPU logic).
- Mutation `min = a.min`: luật trung hòa đỏ (output ScalaCheck trích thật); test tương đương vẫn xanh; golden cũ (count,sum) vẫn xanh → đã sửa golden so cả 4 trường.
- Ví dụ mục 3.2 và 5 đã chạy thật.

## Phiên bản
cats-core 2.13.0 · fs2-core 3.13.0 · munit-scalacheck 1.2.0 (ScalaCheck 1.19.0).

## Điểm cần reviewer soi kỹ
- Bảng mục 3.5 nói phân tán cần "kết hợp + giao hoán" — đúng với reduceByKey của Spark; kiểm tra diễn đạt.
- `Execution.partitioned` chỉ là mô phỏng một JVM — kiểm tra bài không ngụ ý tương đương Spark.
