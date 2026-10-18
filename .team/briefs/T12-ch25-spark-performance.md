# Brief T12 — Chương 25 Spark Performance

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 25 + yêu cầu chung về template 17 mục, output thật), template bài học,
  `09-spark/ch22-spark-fundamentals.md` + `spark-fundamentals-lab/` và `09-spark/ch23-spark-sql-dataframes.md` + `spark-sql-lab/` (cách build Scala 2.13 +
  Spark 4.2.0, 15 cờ `--add-opens`, `BareLocalFileSystem` cho Windows, cách đọc plan). Đọc `.team/reviews/T10-ch22-review.md`: các khẳng định bị bắt lỗi ở đó
  (narrow/wide, hash repartition không đều, listener race, broadcast ≠ shuffle Exchange, `autoBroadcastJoinThreshold=-1` không buộc SMJ, AQE đổi chiến lược,
  ANSI overflow) KHÔNG được lặp lại — Ch25 đi sâu đúng các chủ đề này.

## Phạm vi được phép ghi
- `09-spark/ch25-spark-performance.md`
- `09-spark/spark-performance-lab/` (Scala 2.13.18, Spark 4.2.0 `_2.13`, munit 1.2.4; `project.scala` cho Scala CLI + `build.sbt`/`project/build.properties`
  sbt 1.12.11 như spark-sql-lab)
- `solutions/ch25-solutions.md`, `solutions/ch25-code/`, `.team/handoffs/T12-ch25-spark-performance.md`
- `.cache/` cho build/temp/local dirs/output.

## Nội dung bắt buộc (PROMPT_PROJECT Ch25)
Lazy evaluation, query plans, partition sizing, shuffle, join strategies, broadcast join, data skew, caching, memory pressure, serialization, GC,
executor failures, performance debugging. **Không dạy kiểu mẹo.** Mỗi kỹ thuật: Vấn đề → Nguyên nhân → Cách xác định (plan/metrics/Spark UI/listener
— số đo THẬT) → Giải pháp → Khi nào KHÔNG nên áp dụng.

Gợi ý thí nghiệm có số đo thật trong local mode (ghi rõ: local[N], một máy — không suy ra số tuyệt đối cho cluster):
- Skew: một key chiếm ~90%; đo thời gian task lớn nhất/trung vị qua listener (đồng bộ đúng — xem M3 review Ch22), so AQE skew join, salting.
- Join strategy: BroadcastHashJoin vs SortMergeJoin vs ShuffledHashJoin — kiểm tra **final executed plan** (AQE), cố định cấu hình/hint.
- Partition sizing: shuffle.partitions 200 vs vài, AQE coalesce; small files khi ghi.
- Cache: storage level, MEMORY_AND_DISK mặc định của Dataset, khi cache làm chậm hơn.
- Serialization: Kryo vs Java cho RDD; Encoder/Tungsten cho Dataset; UDF chặn tối ưu.
- Memory/GC: `-Xmx` nhỏ, spill metrics (`memoryBytesSpilled`/`diskBytesSpilled`), GC time trong task metrics; OOM thật nếu tái hiện được an toàn.
- Executor failures: `local[N,maxFailures]`, task retry, chi phí tính lại.

## Tiêu chí xong
- Bài đủ 17 mục, tiếng Việt cho người mới; mọi output/plan/số đo trong bài là output thật đã chạy (ghi máy/cấu hình); ≥6 bài tập có lời giải + test.
- `scala-cli.bat test` lab và lời giải pass (`--server=false`, `COURSIER_CACHE` trong `.cache/`); 0 cảnh báo compiler. sbt bị chặn trong sandbox → ghi để Claude chạy.
- Handoff đầy đủ: lệnh, output, phần chưa chạy, khẳng định nào cần reviewer soi kỹ.
