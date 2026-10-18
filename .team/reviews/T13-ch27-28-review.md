# Review T13 — Ch27 Streaming Fundamentals + Ch28 Kafka (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **ĐẠT — 0 BLOCKER, 0 MAJOR, 3 MINOR (không chặn).**

## Kiểm chứng chạy thật (máy lead)
- `sbt.bat -batch test` — streaming-lab: `Passed: Total 10, Failed 0`.
- `RUN_KAFKA_IT=1 sbt.bat -batch test` — kafka-lab: `CoreSuite ... 7 total`, `BrokerSuite finished: 0 failed, 0 ignored, 1 total 6.452s` (KRaft nhúng thật), `Passed: Total 8, Failed 0`.
- `scala-cli.bat test solutions/ch27-code|ch28-code --server=false` → 7/7 và 7/7.
- `.cache/tmp` do build.sbt tạo nằm dưới `.cache/` đã được `.gitignore` (dòng 15).

## Đã soi kỹ, đúng
- Watermark đơn nguồn không lùi (maxSeen đơn điệu); quy ước `[start,end)`, late `< watermark_cũ`; đóng cửa sổ `end <= watermark_mới`; tổng BigInt.
- Producer: idempotence cần acks=all (lỗi `ConfigException` thật từ client 4.4.0), gửi lại cùng event_id ở tầng ứng dụng vẫn tạo hai record — tách bạch đúng
  khả năng client/broker với dedup nghiệp vụ. Không tuyên bố exactly-once; Kafka transaction chỉ mô tả, ghi rõ chưa chạy.
- Consumer: commit sau xử lý, DLT được ack trước commit; lag = end − committed, chưa commit → `None` (không coi là 0); `assign` vs `subscribe` đúng vai.
- Retry chỉ bắt `Transient`/`Poison` đã phân loại, lỗi bất ngờ truyền lên.

## MINOR
- **m1 — Ch27: event_id dùng lại với event time MỚI sau khi fingerprint bị dọn được nhận lần nữa.** Probe thật (size=100, delay=50):
  `A@100 amount 10`, `B@300`, `A@350 amount 999` → `decisions=Vector(Accepted, Accepted, Accepted)`, cửa sổ [300,400) total = 1000.
  Mục 10 (dòng ~220) chỉ nói "bản cũ được phân loại late" — đúng khi thời gian giữ nguyên, nhưng chưa nêu trường hợp ID bị dùng lại với thời gian mới. Thêm
  một câu + (tùy chọn) test minh họa giới hạn: phát hiện xung đột chỉ trong chân trời state; xuyên thời gian cần store bền (Ch28 mục 3.8).
- **m2 — Định dạng đầu bài khác các chương khác** (dòng phiên bản trước tiêu đề, thiếu "Vị trí trong lộ trình") — lead đồng bộ ở Phase 9 QA.
- **m3 — fs2-kafka chưa dùng:** lựa chọn kafka-clients trực tiếp có lý do; khi Ch29/P4 cần stream hóa, kiểm chứng lại tổ hợp fs2-kafka 4.x + kafka-clients 4.4.0.
