# Review T19 — Chương 32 Advanced Distributed Processing (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 0 BLOCKER, 1 MAJOR, 5 MINOR.** Nội dung kỹ thuật tốt; vấn đề chính là sư phạm.

Điểm mạnh thật: không lặp Ch25 mà nhìn đúng tầng hệ phân tán; mọi khẳng định được chặn phạm vi (local mode một JVM, remote bytes = 0, nhãn locality ≠ byte
mạng, một lượt đo không phải benchmark); thí nghiệm retry ghi trùng side effect (`journal=100,0;100,1;200,0`) và capture closure (`Task not serializable` +
`NotSerializableException: ...NonSerializableOwner`) là bằng chứng thật rất có giá trị; tách payload thô vs shuffle bytes khi Kryo **lớn hơn** Java trong lượt đo
— đúng tinh thần "không suy thứ hạng chung"; `count_distinct` vs `approx_count_distinct` có số thật (20.000 / 19.522, lệch 2,39%) và giải thích RSD không phải
giới hạn cứng; phân biệt task retry với stage retry, `spark.task.maxFailures` với `spark.stage.maxConsecutiveAttempts`.

## Kiểm chứng của reviewer (17/10/2026, Windows 11, JDK 21)

| Lệnh | Kết quả |
|---|---|
| `scala-cli.bat test 11-data-platform/distributed-lab --server=false` | `DistributedSuite` 11/11, 12,8 s |
| `sbt.bat test` trong `distributed-lab` | `Passed: Total 11, Failed 0` |

## MAJOR

### M1 — Gần như không có code trong bài; văn phong ghi chú nén

Vị trí: toàn bộ mục 3 (3.1–3.11) không có khối code Scala nào; cả chương chỉ một khối ` ```scala ` (mục 5, 6 dòng). Mục 8 "Giải thích code" mô tả bằng lời.

Cùng vấn đề M3 của review T15. Chương dạy **đo** — nhưng người học không thấy *cách* đo: `Measurement.scala` (listener + đồng bộ trước khi đọc số — lỗi đã bị
bắt ở review Ch22), cách lấy `TaskInfo.attemptNumber`/`stageAttemptId`, cách in `rdd.partitioner`, thí nghiệm retry ghi journal, `bucketBy` + đọc qua catalog.
Câu văn rất dày (ví dụ 3.4, 3.5, 3.7 mỗi đoạn 4–6 khái niệm) — đúng nhưng khó theo với người lần đầu gặp distributed execution.

Đề xuất: mỗi mục 3.x có **một** đoạn code ngắn (5–15 dòng) trích từ lab, đặt ngay sau "Cách hoạt động", kèm 2–3 dòng output thật tương ứng ở mục 9 (đã có
sẵn trong evidence). Ưu tiên: `Measurement.measure` (đồng bộ listener), `joined` (giữ/mất partitioner), `retry` (ghi trước khi ném lỗi), `closureBytes`/
`NonSerializableOwner`, `bucketJoin` (bảng catalog + hint), `batches` (đầu tiên vs tổng). Tách câu dài thành câu ngắn, mỗi đoạn một ý. Đưa tiêu đề `# Chương 32`
lên dòng đầu (blockquote phiên bản sau tiêu đề như các chương khác) và thêm "Vị trí trong lộ trình" ngắn.

## MINOR

- **m1 — Kết quả trái kỳ vọng cần lời giải thích tại chỗ.** Bảng mục 9: "RDD join giữ partitioner **295 ms**" chậm hơn "mất partitioner **213 ms**", dù bản thứ hai
  có thêm một stage và 2.000 bản ghi shuffle. Bài có câu chung "một lượt, không kiểm soát tải nền", nhưng người học sẽ hỏi ngay dòng này. Thêm một câu: với
  2.000 dòng, chi phí lập lịch/khởi tạo task áp đảo; số đo đáng tin là **stage/task/shuffle bytes** (1/4 vs 2/8, 0 vs 868 byte), không phải ms. Gợi ý: chạy lặp N
  lần sau warm-up và báo trung vị nếu muốn dùng ms (nối Ch34).
- **m2 — Comment code bằng tiếng Anh** (`DistributedLab.scala:51`, `:60`, `:94–96`, `:117–118`, `:132–133`, và các file khác). Đổi sang tiếng Việt như các lab khác.
- **m3 — `var` + `while` trong `batches`** (`DistributedLab.scala:123–145`). Vòng `while` trong hàm CPU nhân tạo chấp nhận được (comment nói rõ), nhưng bốn `var` tích
  lũy ở driver có thể là `foldLeft` trên các offset — code mẫu cho người học nên theo phong cách hàm của giáo trình.
- **m4 — Windows: cạn cổng loopback.** Lab không gặp lỗi (11 test ngắn), nhưng `session()` không đặt `spark.sql.artifact.isolation.enabled=false` như các lab
  Ch24/Ch26/P3 vừa thêm (Ch24 mục 10: mặc định +1.650 socket TIME_WAIT / 60 truy vấn trên máy này). Thêm cấu hình + một câu dẫn, để người học chạy bài tập dài
  trên Windows không gặp `RemoteClassLoaderError ... Unable to establish loopback connection`.
- **m5 — Thiếu mục "Lỗi thường gặp" dạng thông báo thật cho một ca phổ biến:** đọc bảng bucket bằng `spark.read.parquet(path)` (mất metadata bucket → plan có
  `Exchange`). Bài nói đúng điều này ở 3.9 nhưng không trích plan thật; thêm vào mục 10 với plan `Exchange hashpartitioning` thật để người học nhận ra trên UI.

## Đã kiểm và đạt

- Bảng cache/checkpoint (RDD `cache` MEMORY_ONLY, DataFrame MEMORY_AND_DISK, reliable vs local checkpoint, checkpoint RDD lazy khác `Dataset.checkpoint(eager)`) đúng.
- `closureSerializer` luôn là JavaSerializer (dẫn `SparkEnv` 4.2.0) → đổi Kryo không cứu được closure capture — đúng và quan trọng.
- Cảnh báo `TaskSetManager ... very large size (1415 KiB). The maximum recommended task size is 1000 KiB.` là output thật, phân biệt với cảnh báo task binary.
- 7 bài tập có lời giải + test (`ExercisesSuite` 7/7 theo evidence).

## Vòng 2 — Claude Code, 17/10/2026: **ĐẠT**

- **M1 đã sửa:** tiêu đề lên dòng đầu, có "Vị trí trong lộ trình"; 13 khối Scala, mỗi mục 3.1–3.11 có đoạn trích lab 6–13 dòng kèm liên kết tới output mục 9; đoạn văn dày được tách. Đọc mẫu 3.1 (`Measurement.measure` + rào listener) và 3.10 — đúng điều reviewer yêu cầu.
- **m1–m5 đã sửa** như bảng trong handoff mục 7 (giải thích thứ hạng ms vs shuffle, comment tiếng Việt, `foldLeft` thay biến driver, artifact isolation tắt, nhánh bucket đọc qua path có Exchange thật).
- **Kiểm chứng của lead:** `sbt test` trong `distributed-lab` → `Passed: Total 11, Failed 0`; `scala-cli test solutions/ch32-code --server=false` → `7 total, 0 failed`; 0 cảnh báo compiler.
