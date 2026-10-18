# Handoff T16 — Chương 26: Spark Production Engineering (tác giả: Claude Code)

**File:** `09-spark/ch26-spark-production-engineering.md`; `09-spark/spark-production-lab/` (project.scala, build.sbt, project/{build.properties,plugins.sbt},
deploy/submit.sh, src/main/.../sparkprod/{JobConfig,Observability,Job,Checkpointing}.scala, src/test/.../JobSuite.scala); `09-spark/scala3-spark-probe/`;
`solutions/ch26-solutions.md`, `solutions/ch26-code/`. Cập nhật `00-roadmap/04-compatibility-matrix.md` (lead).

**Kiểm chứng thật (17/10/2026):**
- `scala-cli.bat test 09-spark/spark-production-lab --server=false` → `JobSuite finished: 0 failed, 0 ignored, 9 total`; `sbt -batch test assembly` → `Passed: Total 9`,
  jar 110.004 byte (trước khi loại Scala: 6.246.223).
- `scala-cli.bat test solutions/ch26-code --server=false` → 6/6.
- `spark-submit` (dlcdn.apache.org spark-4.2.0-bin-hadoop3, SHA-512 khớp) trên WSL2 Ubuntu 24.04 + Temurin 21.0.12.1: exit 0 và exit 3 (gate). Trên Windows: lỗi
  winutils (stack trace trong bài mục 10).
- Scala 3 probe: 3.3.8/3.7.3 chạy; 3.8.4/3.9.0 `Cannot find a SparkSession implementation`; `toDS` không compile.
- 0 cảnh báo compiler.

**Reviewer soi kỹ:** phân loại transient (chuỗi cause), retry + idempotency, gate theo lô ngày chạy, manifest nguyên tử, JobMetrics (job group + rào chắn), khẳng định
về Scala 3.8 stdlib (nguyên nhân chính xác ghi "chưa kiểm chứng"), winutils root cause.

## Vòng 2 (17/10/2026) — sửa theo `.team/reviews/T16-ch26-review.md`
- **M1:** `Job.launch` parse trước, tạo session sau; `main` dùng `launch`. Test với `newSession` gọi `fail`. spark-submit WSL2 không tham số → exit 2, 0 dòng SparkContext.
- **M2:** `Failures.classify` chung (Data > Permanent > Transient allowlist > Unknown; `TransientStorageException`); gate nằm trong `attempt`; log/manifest/metric
  sau vòng retry. Test sink log hỏng ở `pipeline_succeeded` → pipeline chạy 1 lần, mã 1.
- **M3:** `JobMetrics.close()` trả `Either`, luôn gỡ listener/job group; manifest `metrics_complete`; bài tách metric vật lý khỏi số nghiệp vụ đã commit.
- **M4:** manifest `<run_id>--<execution_id>.json`, file tạm duy nhất, không ghi đè; finalization lỗi → `finalization_failed`, mã 1 (test `_runs` là file).
- **M5:** một file `_metrics/ch26_orders.prom` (trạng thái hiện tại), bỏ nhãn `run_date` (thành giá trị `job_last_run_date_epoch_day`), escape `\n`.
- **M6:** mục 3.8 + matrix: lỗi là của builder chung (lỗi gốc `class Array does not have a member apply`); `classic.SparkSession.builder()` chạy trên 3.9.0
  (Claude chạy lại độc lập); encoder case class vẫn không có. **M7, m2, m3:** lời giải dùng BigDecimal, bão hòa exp ≥ 63, regex chịu khoảng trắng; **m1:** tham số
  rỗng/lặp là lỗi; **m4:** ví dụ dùng `../batch-pipeline-lab/data`.
- Kiểm chứng: `scala-cli test` 12/12, `sbt test` `Passed: Total 12`, lời giải 6/6, `sbt assembly` 135.937 byte, spark-submit WSL2 (exit 0/3/2) — output ở mục 9.

## Vòng 2b — phản hồi review vòng 2 (R2-M3, R2-M4, R2-m1, R2-m2), 17/10/2026

- **R2-M3:** `metrics.close()` chạy ĐẦU TIÊN trong finalization và luôn gỡ listener + job group (kể cả khi trả lỗi); test assert job group đã được xóa.
- **R2-M4:** log và file metric mỗi bước qua `step(...)`, gom lỗi; manifest ghi CUỐI CÙNG với mã thoát thật + `finalization_errors`. Test: `_metrics` là file → manifest `SUCCEEDED` + `"exit_code":1` + `finalization_errors` có `metrics_file`, job trả 1. Lời giải bài 3 chỉ tính ngày có `SUCCEEDED` VÀ `exit_code` 0.
- **R2-m1:** `solutions/ch26-solutions.md` nói đúng: lời giải viết bộ phân loại độc lập, test ép cùng kết quả với lab trên mọi ca (không phải "dùng chung").
- **R2-m2:** đoạn code Markdown bài 2 khớp source (`exp` tới 63, bão hòa ở `cap`).
- **Kiểm chứng (lead):** Scala CLI lab `13 total, 0 failed`, lời giải `6 total, 0 failed`; `sbt test` → `Passed: Total 13`; `sbt assembly` → jar 136.974 byte; `spark-submit` WSL2 chạy lại 3 lần: exit 0 / 3 / 2, manifest có `exit_code` và `finalization_errors: []`. Chương mục 9 thay bằng output mới.
