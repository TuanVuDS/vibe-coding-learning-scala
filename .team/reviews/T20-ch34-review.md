# Review T20 — Chương 34 Performance và Cost (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 0 BLOCKER, 1 MAJOR, 4 MINOR.** Kỹ thuật tốt; sửa sư phạm rồi lead đánh dấu ✅ (không cần vòng 2 nếu sửa đúng).

Điểm mạnh: mọi kỹ thuật gắn với phép đo thật (CSV raw + `analyze.py` tính lại phân vị); giá cloud là **tham số giả định** được ghi rõ, không trích bảng giá;
mục 10.2 có ví dụ đo sai THẬT (kết quả bị bỏ → 0,0063 ms "nhanh" vô lý, FIRST 353,8 ms vs median 41,7 ms, mean 10,06 ms vs p99 19,05 ms, local tạm giữ
80 MB trên frame) và chặn phạm vi kết luận ("chưa xem assembly"); mô hình Little's law, cache break-even, cost/million có công thức + đơn vị.

## Kiểm chứng của reviewer
| Lệnh | Kết quả |
|---|---|
| `sbt.bat test` trong `13-performance/cost-lab` | `Passed: Total 15, Failed 0` |

## MAJOR
### M1 — Gần như không có code trong bài; tiêu đề sau blockquote
Cả chương có **một** khối ` ```scala `. Chương dạy *đo*, nhưng người học không thấy *cách* đo: vòng warm-up + lặp + phân vị trong `CostBench`, cách giữ kết
quả (blackhole/checksum) để tránh dead-code elimination, đo CPU process vs wall-clock, probe GC trong `Probes`, các hàm mô hình (`requiredConsumers`,
`cacheBreakEven`, `costPerMillion`). Đề xuất: mỗi mục 3.x một đoạn code 5–15 dòng trích từ lab ngay sau "Mô hình", kèm 2–3 dòng output thật; tách câu dài;
`# Chương 34` lên dòng 1, blockquote phiên bản sau; thêm "Vị trí trong lộ trình" (nối Ch11 JVM, Ch25, Ch32, P5). Cùng vấn đề M3 (T15) và M1 (T19).

## MINOR
- **m1** Comment code tiếng Anh (nếu có) → tiếng Việt như các lab khác.
- **m2** Mục 10.2 "Process CPU median Avro encode bằng 0 do bộ đếm thô": nói rõ độ phân giải của bộ đếm CPU đã dùng (API nào, bước bao nhiêu ms trên Windows) để
  người học biết khi nào số CPU vô nghĩa.
- **m3** Nêu rõ máy đo (12 CPU logic, đang chạy song song các job khác của team lúc đo?) cạnh từng bảng, vì nhiễu nền ảnh hưởng p99.
- **m4** Thêm một câu dẫn sang JMH (công cụ chuẩn cho microbenchmark JVM) như lựa chọn thay thế khi cần kết luận chắc hơn — chỉ khái niệm, ghi chưa kiểm chứng.

## Vòng 2 — Claude Code, 17/10/2026: **ĐẠT**

- **M1 đã sửa:** `# Chương 34` ở dòng 1, có vị trí lộ trình; 15 khối Scala — mỗi mục 3.x có đoạn đo (warm-up/lặp/phân vị, giữ kết quả qua `@volatile sink`, CPU process vs wall) hoặc hàm mô hình (`concurrency`, cache break-even, cost/million) kèm output thật. Đọc mẫu 3.2.
- **m1–m4 đã sửa** theo handoff "Vòng 2".
- **Kiểm chứng của lead:** `sbt test` trong `cost-lab` → `Passed: Total 15, Failed 0`; `scala-cli test solutions/ch34-code --server=false` → `7 total, 0 failed`; 0 cảnh báo compiler.
