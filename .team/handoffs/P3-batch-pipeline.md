# Handoff Project 3 — Batch Data Pipeline (tác giả: Claude Code, reviewer: Codex)

Bổ sung theo review P3 m6 (handoff T14/T16 trước đây không mô tả P3).

## File
`14-projects/p3-batch-pipeline/`: `README.md` (khuôn project), `project.scala`, `build.sbt`, `project/build.properties` (sbt 1.12.11), `project/plugins.sbt`
(sbt-assembly 2.3.1), `deploy/submit.sh`, `data/sample/` (orders CSV, clicks JSONL, customers), `src/main/scala/vn/scalaeng/p3/{Job,Sources,Reports,Publish,
Generator,Json}.scala`, `src/test/scala/vn/scalaeng/p3/PipelineSuite.scala`.

## Phiên bản (tra repo1.maven.org / dlcdn.apache.org ngày 17/10/2026)
Scala 2.13.18 · Spark 4.2.0 `spark-sql_2.13` (`Provided`) · hadoop-bare-naked-local-fs 0.1.0 (chỉ Windows) · munit 1.2.4 · sbt 1.12.11 · sbt-assembly 2.3.1 ·
JDK 21 (Temurin 21.0.12.1 trên WSL2).

## Vòng 2 (17/10/2026) — sửa theo `.team/reviews/P3-review.md`
- **M1:** audit so nội dung: fact `exceptAll` hai chiều với fact đã tính; daily và campaign tính lại từ fact đọc lại (+ click đã dedup) rồi so; grain duy nhất;
  miền giá trị campaign; bốn bảng cách ly đếm lại. Test 4 kiểu sửa staging giữ nguyên tổng.
- **M2:** `Publish` theo kế hoạch di chuyển: preflight, nhật ký `_publish/<execution_id>.journal` (DSYNC từng bước), rollback ngược khi lỗi; rollback hỏng → giữ
  staging/thùng rác/nhật ký, log `publish_partial`, runbook trong README mục 9. Bảng lỗi không còn bị xóa trước khi bản mới vào chỗ. Test lỗi ở bước 3.
- **M3:** snapshot: gỡ mọi partition đang phục vụ không còn trong kết quả (bỏ `scope` theo ngày có dữ liệu hợp lệ). Test rút trọn lô ngày 15.
- **M4:** đọc lại staging với schema tường minh. Test 3 ca kết quả rỗng.
- **M5:** regex cú pháp ngày/timestamp (bắt buộc offset) cho đơn và click; job tự đặt session UTC. Test `2026`, giờ thiếu offset, session `Asia/Ho_Chi_Minh`.
- **M6:** `Job.launch` parse trước session; `execution_id` duy nhất cho staging/thùng rác/nhật ký/manifest; manifest không ghi đè; finalization lỗi → mã 1.
- **m1:** generator từ chối thư mục ra không rỗng, cấp ID cho dòng hỏng. **m2:** `click_conflicts` được stage/publish/audit. **m3:** `munitTimeout` 300 s.
  **m4:** test tìm đúng join theo khóa `[customer_id, day]`; comment/README gọi là plan quan sát được. **m5:** bài tập last-click thêm `UNATTRIBUTED`.
  **m6:** handoff này; README đổi "nguyên văn" → giá trị trường sau parser, test seed 42 gọi là regression một fixture.
- Thêm: `spark.sql.artifact.isolation.enabled=false` trong `Job.session()` (cạn cổng loopback trên Windows, Ch24 mục 10).

## Kiểm chứng
- `scala-cli test .` → 18/18; `sbt test` → `Passed: Total 18, Failed 0`; `sbt assembly` → 125.092 byte.
- WSL2 `spark-submit` (`local[4]`): sinh 3 × 100.000 đơn + 300.000 click seed 2026 → 4 lần chạy (15, 16, 17, 17) đều exit 0; manifest khớp đối soát; staging
  và thùng rác dọn sạch; nhật ký cuối 26 DONE + COMMITTED. Output ở README mục 6.

## Chưa kiểm chứng
S3/MinIO, cluster nhiều máy, crash vật lý giữa hai bước publish (chỉ chèn lỗi bằng exception), hai job publish đồng thời cùng output.

## Vòng 2b — phản hồi review vòng 2 (R2-M2, R2-M5), 17/10/2026

- **R2-M2:** `Publish.execute` ghi `INTENT` trước move, cập nhật danh sách bước đã làm NGAY sau move (trước khi ghi `DONE`), nhật ký trong rollback là best-effort, luôn ném `PublishFailed` (job giữ staging khi rollback hỏng). Test mới: ghi nhật ký hỏng sau move → rollback vẫn chạy; chuỗi nhật ký `INTENT, DONE, INTENT, DONE, FAILED, UNDONE, UNDONE, ROLLED_BACK`. README: mô tả publish + runbook (INTENT không có DONE → kiểm tra trên đĩa).
- **R2-M5:** `Common.instantOf` ép `TIMESTAMP_LTZ` tường minh; test click `06:30+07:00` dưới phiên NTZ vẫn thuộc ngày 15.
- **Kiểm chứng (lead):** Scala CLI `19 total, 0 failed`; `sbt test` → `Passed: Total 19`; `sbt assembly` → jar 125.243 byte; spark-submit WSL2 trên dữ liệu lớn (305.299 đơn + 904.521 click) 4 lần exit 0, đối soát khớp tuyệt đối, nhật ký lần cuối 26 INTENT + 26 DONE + 1 COMMITTED. README mục 6 thay bằng output mới.
