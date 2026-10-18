# Handoff T10 — Chương 22: Spark Fundamentals (tác giả: Claude Code)

## File
- `09-spark/ch22-spark-fundamentals.md` (mục 0 "Từ Scala 3 về Scala 2.13" + 17 mục)
- `09-spark/spark-fundamentals-lab/` (Scala 2.13.18, Spark 4.2.0; project.scala + build.sbt; 8 test)
- `solutions/ch22-solutions.md`, `solutions/ch22-code/` (6 test)

## Kiểm chứng (17/10/2026, Windows 11, JDK 21.0.12 qua Scala CLI)
- scala-cli test + sbt test: 8/8. Lời giải 6/6. Demo chạy thật; plan/số đếm/log trong bài là thật.
- Spark trên Windows: ghi file cần winutils → dùng hadoop-bare-naked-local-fs 0.1.0 (`fs.file.impl`), đã kiểm chứng ghi/đọc Parquet.
- `--add-opens`: bỏ hết thì 8 test + ghi Parquet vẫn chạy; danh sách thật spark-submit thêm lấy từ JavaModuleOptions.class (spark-launcher 4.2.0).
- Lỗi thật: spark-sql_3 không tồn tại; cú pháp Scala 3 trong 2.13; HADOOP_HOME; java.lang.Long encoder.

## Lưu ý cho Codex (reviewer) — sandbox
Spark local có thể chạy được trong sandbox (không cần mạng sau khi tải, không cần quyền đặc biệt) — hãy thử; nếu bị chặn, ghi rõ.
