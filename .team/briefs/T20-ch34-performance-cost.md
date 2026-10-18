# Brief T20 — Chương 34 Performance và Cost

- **Tác giả:** Codex (chuyển từ Claude ngày 17/10/2026 để cân bằng khối lượng — xem `.team/plan-phase6-9.md`) · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Ch34 + template 17 mục: "Mỗi kỹ thuật phải gắn với cách đo đạc, không chỉ dựa vào phỏng đoán"),
  `04-professional-engineering/` Ch11 JVM + `jvm-lab` (đo đạc JVM, -Xss/-Xmx, JIT warm-up), Ch25 Spark Performance của bạn, Ch32 (của bạn, đang viết),
  Ch21 (benchmark định dạng 1 triệu dòng), Ch29/P4 (số đo snapshot thật trong review T15: ~10–15 ms/event). KHÔNG lặp lại nội dung Ch25/Ch32 — Ch34 là
  góc nhìn **ra quyết định**: đo → mô hình hóa chi phí → chọn sizing.

## Phạm vi được phép ghi
- `13-performance/ch34-performance-cost.md`, `13-performance/cost-lab/` (Scala 3.9.0 cho phần JVM/collection/streaming; nếu cần Spark thì một module con Scala 2.13.18 + Spark 4.2.0 build riêng như spark-performance-lab)
- `solutions/ch34-solutions.md`, `solutions/ch34-code/`, `.team/handoffs/T20-ch34-performance-cost.md`, `.cache/`

## Nội dung bắt buộc
Resource sizing, CPU và memory, parallelism, partition sizing, data skew, serialization, network transfer, caching, query optimization, throughput, latency,
cost trade-offs. Mỗi kỹ thuật: câu hỏi cần trả lời → đo bằng gì (JMH-style microbench với warm-up đúng, hoặc đo wall-clock lặp lại có phân vị, Spark task
metrics, GC log) → số đo THẬT trên máy này (ghi CPU/RAM/JDK) → mô hình chi phí (ví dụ: `chi phí = node × giờ × đơn giá`, đơn giá là **tham số giả định ghi rõ**,
không trích bảng giá cloud trừ khi đã kiểm chứng link + ngày) → quyết định → khi nào kết luận sai (local vs cluster, dữ liệu nhỏ, JIT chưa warm).
Gợi ý thí nghiệm: throughput vs latency khi đổi batch size (p50/p95/p99 thật); Little's law cho số luồng/consumer; partition sizing → số task × overhead;
serialization JSON vs Avro (nối Ch21) theo byte và CPU; cache có lợi khi nào (đo); một bảng "cost per 1 triệu event" từ số đo + tham số giá giả định.
Có một mục "lỗi đo đạc thường gặp" với ví dụ THẬT: dead-code elimination, không warm-up, đo lần đầu gồm cả JIT/class loading, so trung bình khi phân phối lệch.

## Tiêu chí xong
- Đủ 17 mục; mọi số đo là output thật (log trong `cost-lab/verification/` hoặc tương đương); ≥6 bài tập có lời giải + test (test kiểm logic tính toán/mô hình, không assert thời gian tuyệt đối).
- `scala-cli.bat test` pass (`--server=false`, COURSIER_CACHE/tmp trong `.cache/`), 0 cảnh báo; sbt để Claude chạy.
- Handoff đầy đủ (lệnh, output, phiên bản + nguồn + ngày, phần chưa chạy, điểm reviewer cần soi, đề xuất glossary).
