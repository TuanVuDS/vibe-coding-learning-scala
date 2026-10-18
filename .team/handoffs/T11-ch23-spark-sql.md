# Handoff T11 — Chương 23: Spark SQL & DataFrames (tác giả: Claude Code)

- File: 09-spark/ch23-spark-sql-dataframes.md, 09-spark/spark-sql-lab/ (12 test, sbt + scala-cli), solutions/ch23-* (6 test).
- Kiểm chứng 17/10/2026: mọi plan/bảng/lỗi là output thật. Phát hiện: nullable=false bị nguồn file bỏ qua; != loại dòng NULL; UDF/lambda PushedFilters []; groupByKey không có partial_sum; thiếu --add-opens sun.util.calendar -> EXPRESSION_DECODING_FAILED khi collect timestamp (đã thêm đủ 15 cờ vào mọi lab Spark).
- Lưu ý reviewer: tác giả từng chẩn đoán sai nguyên nhân EXPRESSION_DECODING_FAILED (ghi trong mục 10.2) — hãy kiểm tra lại các khẳng định còn lại.

## Vòng 2 (sau review T11 của Codex, 17/10/2026)

**File sửa:** `09-spark/ch23-spark-sql-dataframes.md`; `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/{SqlLab,SqlDemo}.scala`,
`src/test/.../SqlLabSuite.scala`; `solutions/ch23-solutions.md`; `solutions/ch23-code/src/main/.../{Ch23Solutions,UdfBench}.scala`, `src/test/.../Ch23SolutionsSuite.scala`.

**Đã sửa:** M1 (ANSI overflow là chính sách "tràn = lỗi"; typed dùng multiplyExact/addExact; test cả hai), M2 (latestVersion bằng max + phát hiện xung đột,
`versionConflicts`; runningTotal orderBy + output có sku; test hoán vị + 3 cấu hình AQE/partition), M3 (`duplicateCustomerIds` + require trước join; cột
`order_lines` và `orders = countDistinct`), M4 (hợp đồng chuẩn hóa, UDF Locale.ROOT + chỉ bỏ dấu cách; test từng giá trị dưới locale tr-TR), M5 (lambda null-safe;
test NULL sku 4 cách + NOT_NULL_ASSERT_VIOLATION), M6 (`enforceSchema=false`; test header đổi chỗ + count() pruning), M7 (bài 2/6 dedup trước, so golden),
m1–m3, m5, m6 (văn bản/plan cuối/UdfBench tách khỏi test), m4 (handoff này).

**Kiểm chứng thật:**
- `scala-cli.bat test 09-spark/spark-sql-lab --server=false` → `SqlLabSuite finished: 0 failed, 0 ignored, 18 total`; `sbt.bat -batch test` → `Passed: Total 18, Failed 0`.
- `scala-cli.bat test solutions/ch23-code --server=false` → `Ch23SolutionsSuite finished: 0 failed, 0 ignored, 7 total`.
- `SqlDemo` và `UdfBench` chạy lại; output trong bài đã cập nhật (bảng segment có `order_lines`, running total có `sku`).

**Phiên bản:** Spark 4.2.0 (`spark-sql_2.13`), Scala 2.13.18, munit 1.2.4, hadoop-bare-naked-local-fs 0.1.0 — giống Ch22 (repo1.maven.org, tra 17/10/2026).

**Đề xuất file dùng chung:** glossary: data skew đã có ở Ch22; thêm "grain", "enforceSchema", "version conflict". PROGRESS: Ch23 ✅ sau review vòng 2.

**Chưa kiểm chứng:** mini project CTR (chỉ là đề bài); cluster nhiều máy.
