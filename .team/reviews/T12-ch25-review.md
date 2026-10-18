# Review T12 — Chương 25 Spark Performance (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **ĐẠT — 0 BLOCKER, 0 MAJOR, 3 MINOR (không chặn).**

## Kiểm chứng chạy thật (máy lead)
- `sbt.bat -batch test` trong `09-spark/spark-performance-lab` → `Passed: Total 13, Failed 0, Errors 0, Passed 13`.
- `scala-cli.bat test solutions/ch25-code --server=false` → `Ch25SolutionsSuite finished: 0 failed, 0 ignored, 6 total`.

## Đã soi kỹ, đúng
- `Measurement.measure`: job group + job marker làm rào FIFO trên shared queue, latch có timeout, khôi phục local properties trong `finally` — đúng bài học M3 của Ch22.
- Join strategy đọc phần Final Plan (`finalSection`), regression SMJ→SHJ khi chỉ tắt broadcast; phân biệt BroadcastExchange với shuffle; byte broadcast không có trong shuffle metrics.
- Salting: một salt xác định/fact, dimension nhân bản, hợp đồng INNER + khóa duy nhất/không null được nêu và lời giải kiểm tra; `exceptAll` hai chiều + tổng tiền.
- SUM hai tầng cast DECIMAL(38,0) trước khi cộng, giữ ANSI; cache tính cả materialization, `unpersist` trong `finally`; "spill cưỡng bức" và "task exception ≠ executor chết" được ghi trung thực.
- Kryo nội bộ cho kiểu đơn giản khi shuffle RDD; `local[N]` mặc định 1 lần thất bại; `Dataset.cache` = MEMORY_AND_DISK vs RDD MEMORY_ONLY — khớp tài liệu Spark 4.2.0.

## MINOR
- **m1 — Định dạng đầu bài khác các chương khác:** dòng phiên bản đứng trước tiêu đề `#`, thiếu đoạn "Vị trí trong lộ trình". Lead sẽ chỉnh lúc QA Phase 9 cho đồng nhất.
- **m2 — Tham chiếu Ch24 trước khi Ch24 tồn tại** (mục 2, mục 15): Ch24 đang được viết; lead kiểm tra lại liên kết sau khi Ch24 xong.
- **m3 — Bảng thời gian một lượt** (mục 9): đã ghi rõ là một lượt/không kiểm soát tải; giữ nguyên, không cần sửa.
