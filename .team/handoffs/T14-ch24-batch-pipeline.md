# Handoff T14 — Chương 24: Batch Data Pipeline (tác giả: Claude Code)

**File:** `09-spark/ch24-batch-data-pipeline.md`; `09-spark/batch-pipeline-lab/` (project.scala, build.sbt, data/ 2 lô + customers,
src/main/scala/vn/scalaeng/batch/{Pipeline,Experiments,PipelineDemo}.scala, src/test/.../PipelineSuite.scala); `solutions/ch24-solutions.md`, `solutions/ch24-code/`.

**Kiểm chứng thật (17/10/2026, Windows 11, JDK 21, Spark 4.2.0 local[2]):**
- `scala-cli.bat test 09-spark/batch-pipeline-lab --server=false` → `PipelineSuite finished: 0 failed, 0 ignored, 14 total`; `sbt.bat -batch test` → `Passed: Total 14, Failed 0`.
- `scala-cli.bat test solutions/ch24-code --server=false` → `Ch24SolutionsSuite finished: 0 failed, 0 ignored, 6 total`.
- `PipelineDemo` chạy, output trong mục 9 là output thật; 4 thông báo lỗi mục 10 tái hiện bằng probe có chủ đích (scratchpad, không commit).
- 0 cảnh báo compiler.

**Phiên bản:** như Ch22/23 (Spark 4.2.0 `_2.13`, Scala 2.13.18, munit 1.2.4, hadoop-bare-naked-local-fs 0.1.0).

**Reviewer soi kỹ:** hợp đồng đối soát (mọi dòng vào đúng một ngăn) có đúng với mọi input không (NULL updated_at? NULL key?); dedup hai tầng; tính lại toàn bộ
lô + dynamic overwrite có thật sự idempotent; verify chỉ so partition của lần chạy; small files / write modes; các khẳng định về partition discovery và pruning.
Một câu đã ghi "Chưa kiểm chứng" (thư mục đặt tên sai định dạng đổi kiểu suy ra).

**Chưa làm:** Project 3 (mở rộng pipeline này).

## Vòng 2 (17/10/2026) — sửa theo `.team/reviews/T14-ch24-review.md`
- **M1:** `readBack` đọc với schema tường minh; thư mục mất → bảng rỗng để verify báo "thiếu N dòng". Test 4 ca output rỗng hợp lệ.
- **M2:** ngữ nghĩa chốt: output = snapshot của input ≤ upTo. `writeSnapshot` ghi động rồi xóa partition mồ côi cho fact/daily/rejected (Hadoop FileSystem API);
  `RunReport.removedPartitions`. Test 3 ca của reviewer (xung đột mới, hoàn tiền xóa daily, giảm upTo). Demo thêm "Lần 4".
- **M3:** verify so multiset fact (`exceptAll` hai chiều), tính lại daily từ fact đọc lại + grain duy nhất, đếm rejected/conflicts. Test 3 kiểu sửa output của reviewer.
- **M4:** regex cú pháp trước `try_cast`; timestamp bắt buộc có offset. Test '2026', giờ thiếu offset, Z ≡ +07:00.
- **m1–m3:** "giá trị trường trước chuẩn hóa" thay "nguyên văn"; `maxRecordsPerFile` không chia task; tỷ lệ DECIMAL/Long ≈ 1,08 × 10¹⁹.
- **Phát hiện thêm:** `spark.sql.artifact.isolation.enabled=true` (mặc định Spark 4) làm executor tải class qua RPC → cạn cổng loopback trên Windows (đo +1.650
  TIME_WAIT/60 truy vấn); `PipelineDemo.session()` đặt `false`; bài mục 10 Lỗi 11; compatibility matrix.
- Kiểm chứng: `scala-cli test` 18/18, `sbt test` `Passed: Total 18`, lời giải 6/6, Ch26 lab (dùng chung code) 12/12.

## Vòng 2b — phản hồi review vòng 2 (M4 còn hở), 17/10/2026

- **M4:** `updated_at` ép bằng `try_cast(... AS TIMESTAMP_LTZ)` tường minh thay vì `AS TIMESTAMP` (alias theo `spark.sql.timestampType`). Test regression chạy pipeline dưới phiên `TIMESTAMP_NTZ` và khôi phục cấu hình trong `finally`. Mục 3.1 của chương giải thích alias và vì sao phải viết LTZ.
- **Kiểm chứng (lead):** `scala-cli test 09-spark/batch-pipeline-lab --server=false` → `18 total, 0 failed`; `sbt test` → `Passed: Total 18`. Ch26 dùng lại Pipeline này: jar Ch26 build lại và spark-submit WSL2 chạy lại (exit 0/3/2).
