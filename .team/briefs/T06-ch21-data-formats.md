# Brief T06 — Chương 21: Data Formats và Serialization

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 21), template, `00-roadmap/04-compatibility-matrix.md` (Avro/Parquet đang ❓),
  `14-projects/p1-scala-data-processor/` (CSV/JSON tự viết), `08-data-processing/ingest-lab/` (Claude đang viết Ch19: JSONL + CSV bằng circe/fs2-data).

## Phạm vi được phép ghi
- `08-data-processing/ch21-data-formats.md`, `08-data-processing/formats-lab/` (project.scala + build.sbt), `solutions/ch21-solutions.md`,
  `solutions/ch21-code/`, `.team/handoffs/T06-ch21-data-formats.md`.

## Nội dung bắt buộc (PROMPT_PROJECT Chương 21)
JSON, CSV, Avro, Parquet; serialization/deserialization; schema; **schema evolution** (thêm trường có default, xóa trường, đổi kiểu, đổi tên —
backward/forward/full compatibility, có thí nghiệm ghi bằng schema v1 đọc bằng schema v2 và ngược lại); nullable fields; type conversion
(tiền, thời gian, decimal, timezone); data compatibility; data validation. Giải thích vì sao schema và serialization quan trọng trong pipeline.

So sánh **có số đo thật** trên cùng một bộ dữ liệu (ví dụ 1 triệu đơn hàng): kích thước file (có/không nén), thời gian ghi/đọc, đọc một cột
(Parquet columnar) vs đọc cả dòng. Ghi rõ máy đo và cách tái lập.

## Thư viện — PHẢI kiểm chứng trước khi dùng
- Avro: `org.apache.avro:avro` (Java) và/hoặc thư viện Scala có `_3` (ví dụ vulcan) — kiểm chứng Scala 3 trên repo1.maven.org (KHÔNG dùng search.maven.org).
- Parquet: thư viện Java `parquet-java` thường kéo Hadoop. Tìm cách tối thiểu, chạy được trên Windows, nêu rõ đánh đổi (dependency nặng, winutils...).
  Nếu không có cách hợp lý, có thể đọc/ghi Parquet bằng công cụ khác để KIỂM CHỨNG (ví dụ DuckDB JDBC) nhưng phải giải thích rõ đó không phải
  API Scala chính. Ghi nhận mọi giới hạn trong bài; cập nhật đề xuất cho compatibility matrix trong handoff.
- Nhánh Spark (Chương 22+) dùng Scala 2.13 — chương này vẫn Scala 3.9.0. Không trộn artifact `_2.13` với `_3`.

## Tiêu chí xong
- `scala-cli.bat test` cho lab và lời giải pass, 0 cảnh báo; mọi output/số đo/lỗi trích trong bài là thật.
- Bài tập ≥6 có lời giải + test; handoff đầy đủ (AGENTS.md mục 2.3).
