# Brief T13 — Chương 27 Streaming Fundamentals + Chương 28 Kafka với Scala

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Ch27, Ch28, Project 4 để chuẩn bị nền; yêu cầu chung template 17 mục, output thật), template bài học,
  `08-data-processing/ch19-*` + `ingest-lab/` (click events, dedup, event-time, late events, Locale.ROOT, fingerprint), `08-data-processing/ch20-*`
  (Monoid/CommutativeMonoid, BigInt cho tổng), `07-concurrency/ch16b-*` + `effects-lab/` (Cats Effect, Resource, backpressure với fs2),
  `05-backend/ch17-*` (lỗi review chéo). Đọc mục 10 các chương đó: lỗi đã bị review bắt KHÔNG được lặp lại.

## Phạm vi được phép ghi
- `10-streaming/ch27-streaming-fundamentals.md`, `10-streaming/streaming-lab/` (Scala 3.9.0, fs2 3.13.0, Cats Effect 3.7.1, munit 1.2.4, munit-cats-effect 2.2.0
  — mô phỏng trong JVM: event time vs processing time, watermark/late events, tumbling/sliding/session window, state, at-most/at-least-once khi crash giữa chừng,
  backpressure có số đo; ví dụ click events và order events)
- `10-streaming/ch28-kafka-scala.md`, `10-streaming/kafka-lab/` (Scala 3.9.0; chọn và KIỂM CHỨNG thư viện: kafka-clients Java trực tiếp vs fs2-kafka 4.1.3 —
  tra repo1.maven.org phiên bản mới nhất ổn định của kafka-clients và tương thích với fs2-kafka; broker cho test: thử Kafka KRaft nhúng trong JVM
  (ví dụ `org.apache.kafka:kafka_2.13` server hoặc thư viện embedded) — nếu sandbox chặn socket/broker thì viết đủ test, ghi rõ CHƯA chạy để Claude chạy)
- `solutions/ch27-*`, `solutions/ch28-*`, `.team/handoffs/T13-ch27-28-streaming-kafka.md`, `.cache/`.

## Ch28 phải có
Topic/partition/offset/consumer group bằng thí nghiệm thật (rebalance, lag tính từ end offset − committed offset), serialization (JSON với circe + schema version
trong payload; Schema Registry giải thích ở mức khái niệm, không giả vờ đã chạy), delivery guarantees của PRODUCER (acks, idempotence, retries) tách bạch với
của CONSUMER (commit trước/sau xử lý), retry có giới hạn + dead-letter topic, idempotent processing (khóa event_id), poison message.
**Không nhầm khả năng client với broker** (ví dụ idempotent producer là tính năng broker+client; transaction; retention).

## Tiêu chí xong
- Hai bài đủ 17 mục, tiếng Việt cho người mới; mọi output/số đo trong bài là output thật đã chạy; mỗi chương ≥6 bài tập có lời giải + test.
- `scala-cli.bat test` pass (`--server=false`, `COURSIER_CACHE` trong `.cache/`); 0 cảnh báo compiler. sbt bị chặn → ghi để Claude chạy.
- Handoff đầy đủ: lệnh, output, phiên bản + URL nguồn + ngày tra, phần chưa chạy, khẳng định cần reviewer soi kỹ.
