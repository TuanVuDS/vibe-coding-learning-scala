# Review T15 — Ch29 Streaming Pipeline, Ch30 Streaming Reliability, Project 4 (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 4 MAJOR, 7 MINOR, không BLOCKER.**

Điểm mạnh thật: semantics at-most/at-least/"một effect local" được gắn đúng phạm vi và cửa sổ crash; không hứa exactly-once tuyệt đối; thí nghiệm kill JVM con
trên KRaft thật là bằng chứng tốt (so receipt + committed offset, không chỉ total); thứ tự receipt → DLT ack → commit đúng; atomic snapshot + fault injection
trước move có test. Ch27 m1 sửa đúng phạm vi brief.

## Kiểm chứng của reviewer (chạy thật, 17/10/2026, Windows 11, JDK 21)

| Lệnh | Kết quả |
|---|---|
| `RUN_KAFKA_IT=1 sbt.bat test` trong `14-projects/p4-kafka-event-processing` | `Passed: Total 10, Failed 0` (CoreSuite 8, BrokerSuite 2 — KRaft thật, 17,3 s) |
| Probe Scala CLI dùng `Model.scala` + `Store.scala` của P4 (scratchpad, không ghi vào repo) | xem các mục dưới |

Output probe nguyên văn:

```
n=500 ms=5614 msPerEvent=11.228 snapshotBytes=149233
n=1000 ms=9844 msPerEvent=9.844 snapshotBytes=299135
n=2000 ms=29385 msPerEvent=14.6925 snapshotBytes=605545
second store: java.nio.channels.OverlappingFileLockException: null
dupkey: Right(Event(1,e,O,1,5))
accepted
Receipt(Input(orders,0,1,Some(O),Some({"version":1,"eventId":"now","orderId":"O","eventTime":1792022400,"amount":100})),late,Some(event time below previous watermark),0)
after restart maxTime=4000000000 next=late
```

## MAJOR

### M1 — Một event có `eventTime` ở tương lai xa làm MỌI event sau đó thành `late`, vĩnh viễn, kể cả sau restart

Vị trí: `14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Store.scala:70` (điều kiện late), `:78` (`maxTime` cập nhật từ event accepted);
`Model.scala:38` (validate chỉ chặn `eventTime < 0`); `10-streaming/ch29-streaming-pipeline.md:59` (chỉ nhắc "future timestamp có thể đẩy watermark", giao cho bài 3).

Probe: event `f` với `eventTime = 4000000000` (năm 2096) được **accepted**; event thật ngay sau (`1792022400`, 17/10/2026) → `late`; mở lại store: `maxTime=4000000000`,
event tiếp theo vẫn `late`. Vì `maxTime` nằm trong snapshot bền, restart không cứu được; đường phục hồi duy nhất là store mới (mất dedup) — README/chương không mô tả.
Một producer lệch đồng hồ (hoặc gửi mili-giây thay vì giây — lỗi đơn vị rất phổ biến: `1792022400000`) đưa toàn bộ luồng vào DLT.

Đề xuất: tích hợp kiểm tra skew vào **đường chính** (không chỉ helper bài 3): `eventTime <= now + maxFutureSkew` với `now` được truyền vào (`Store(..., clock)`),
vi phạm → receipt `invalid` reason `"eventTime in the future"` → DLT; lưu policy skew vào snapshot như width/delay. Thêm test: (a) event tương lai bị DLT, watermark
không đổi; (b) event mili-giây bị DLT; (c) sau restart vẫn nhận event đúng giờ. Mục 10 Ch29 thêm lỗi này với output thật, README thêm mục runbook "watermark bị đẩy xa".

### M2 — Producer của P4 chỉ gửi một event cố định; người học không tái hiện được các luồng DLT/late/conflict bằng CLI

Vị trí: `Kafka.scala:65–71` (`produce` luôn gửi `Event(1, "event-1", "ORDER-1", 100, 100)`); README mục 5 dòng 112–116.

PROMPT_PROJECT yêu cầu P4 có **Producer**. Hiện tại fixture 8 record (accepted/duplicate/invalid/conflict/late/exhausted) chỉ tồn tại trong `BrokerSuite`; CLI chỉ
tạo được "accepted" rồi "duplicate". Người học chạy theo README không bao giờ thấy invalid JSON, sai key, conflict, late hay DLT đi qua broker.

Đề xuất: `produce <bootstrap> <topic> <file.jsonl>` đọc từng dòng `key<TAB>raw` (cho phép raw hỏng, key sai, `null` value) từ một file fixture trong repo
(chính 8 record của BrokerSuite), in offset từng record; BrokerSuite dùng CLI này thay vì gửi trực tiếp, để đường CLI được test. README: cách mở broker cho
lệnh thủ công (link Kafka quickstart KRaft chính thức, ghi rõ chưa kiểm chứng trên máy này), hoặc một main test-scope khởi động embedded KRaft để học viên chạy
`init/produce/consume` thật mà không cần cài Kafka.

### M3 — Ch29/Ch30 gần như không có code trong bài; văn phong là ghi chú kỹ thuật nén, chưa phải bài giảng

Vị trí: Ch29 mục 5 (dòng 112–114), 6 (116–122), 7 (124–139), 8 (141–147); Ch30 mục 5 (122–126), 6, 8; toàn bộ mục 3 của hai chương.

AGENTS.md mục 3: mỗi chủ đề "vấn đề → vì sao → cách hoạt động → thay thế → ưu/nhược → khi nào dùng → lỗi thiết kế", giọng sư phạm. Hai chương hiện có
**không một khối code Scala nào** — "Ví dụ đơn giản" và "Giải thích code" mô tả bằng lời rồi dẫn link sang P4. Người học phải tự đọc 300 dòng P4 dày đặc
để hiểu. Câu văn nén nhiều thuật ngữ trong một câu (ví dụ Ch29:47, :57, Ch30:44, :46) khó theo với người mới học streaming.

Đề xuất:
- Trích và giải thích từng đoạn cốt lõi: `Store.process` (thứ tự match receipt → schema/key → seen → late → retry), `save` (write tmp → `force` → `ATOMIC_MOVE` →
  cập nhật `current`), `Pipeline.handle` (receipt → publish chờ ack → commit), `Kafka.commit` (`offset + 1` + leader epoch), `CrashWorker` hai mode.
- Mục 5 mỗi chương: một ví dụ code tối giản chạy được (ví dụ 4 dòng `Store.process` in ra kind) kèm output thật.
- Viết lại các đoạn dày nhất thành câu ngắn, mỗi đoạn một ý; thêm sơ đồ thời gian (ASCII) cho cửa sổ crash before/after commit ở Ch30 3.1/3.4.
- Định dạng đầu chương như các chương trước: `# Chương N — …` trước, blockquote phiên bản/trạng thái sau; thêm "Vị trí trong lộ trình" (nối Ch28 → Ch29 → Ch30 → P4 → Ch31/Ch33).

### M4 — Mục 10 "Lỗi thường gặp" thiếu thông báo lỗi thật

Vị trí: Ch29 mục 10 (166–184): chỉ một thông báo thật (`invalid window policy`); Ch30 mục 10 tương tự (kiểm tra lại sau sửa).

AGENTS.md: "mọi thông báo lỗi trong Lỗi thường gặp phải là thông báo thật". Các lỗi mà chương mô tả đều tái hiện được bằng code hiện có nhưng không trích output:
`store already owned` (hai process), `OverlappingFileLockException: null` (hai Store cùng JVM — probe trên), `snapshot policy mismatch`, `source identity reused
with different payload`, `snapshot corrupt: …`, lỗi DLT publish (timeout thật khi topic DLT không tồn tại hoặc broker dừng), dùng `KafkaConsumer` từ hai thread
(`ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access`). Đề xuất: chạy và trích nguyên văn ít nhất 5 lỗi mỗi chương; lỗi
nào chỉ mô tả hành vi (DLT lặp) thì trích log thật của test.

## MINOR

- **m1 — JSON trùng key được nhận, giá trị cuối thắng.** `Model.scala:37–41`; probe: `{"…","amount":1,"amount":5}` → `Right(Event(1,e,O,1,5))`. Ch29:39 gọi đây là
  "schema đóng". Parser khác (hoặc consumer DLT/audit) có thể đọc `amount=1` — với dữ liệu tiền, nên từ chối. circe không báo trùng key; hoặc dùng parser có tùy
  chọn từ chối trùng key, hoặc ghi rõ giới hạn trong bài + bài tập.
- **m2 — Hai Store cùng thư mục trong một JVM ném `OverlappingFileLockException` với message `null`**, không phải "store already owned" (`Store.scala:20–27`).
  Bắt riêng và đổi thành thông báo có nghĩa; thêm vào test one-writer.
- **m3 — Đo chi phí snapshot và đưa số vào bài.** Ch29:49 và README:77 nói "O(state)" nhưng không có số. Số thật trên máy này: ~10–15 ms/event, 2000 event =
  29,4 s, snapshot 605 KB (tăng tuyến tính, tổng thời gian ~bậc hai). Đưa số đo vào Ch29 mục 3.3 + README, và đặt giới hạn cứng (fail closed khi `state.json`
  vượt N byte) để vượt mô hình thì dừng có thông báo thay vì chậm dần tới vượt `max.poll.interval.ms`. Khớp nguyên tắc "gắn với cách đo đạc" của Ch34.
- **m4 — `Kafka.lag` gọi `endOffsets` + `committed` (2 round-trip broker) cho MỖI record** (`Kafka.scala:45–50`, `:94`). Ghi chú chi phí hoặc lấy mẫu mỗi N record / mỗi
  T giây; tách metric khỏi đường xử lý.
- **m5 — Comment trong code bằng tiếng Anh** (`Model.scala:8`, `:55`, `Store.scala:9–11`, `:43`, `:54`, `Kafka.scala:82`, `:106`, `Policy.scala:4`, `:10`). Các lab khác
  của giáo trình comment tiếng Việt; người học đọc code để học — đổi sang tiếng Việt.
- **m6 — `var` + `while` + `return` trong `Retry.run` và `Kafka.one`** (`Model.scala:56–68`, `Kafka.scala:51–56`). Code mẫu cho người học nên là `@tailrec` hoặc
  `Iterator.iterate`/`LazyList`; `return` trong Scala 3 là phong cách không khuyến khích. Giữ nguyên hành vi, có test hiện tại bảo vệ.
- **m7 — README P4 lệch thiết kế trong `00-roadmap/06-projects-overview.md`** (PostgreSQL upsert + Docker Compose). Lựa chọn file snapshot có lý do (README:77) — chấp
  nhận; lead sẽ cập nhật overview. Tác giả chỉ cần thêm một câu nối: "P5 thay sink file bằng PostgreSQL" để người học biết hướng mở rộng.

## Các điểm đã kiểm và đạt

- Thứ tự match trong `Store.process`: receipt nguồn trước → replay không đổi quyết định cũ dù watermark đã tăng (test `recovery repeats DLT…` đúng).
- `save`: `force(true)` trước `ATOMIC_MOVE`, `current` chỉ đổi sau move — đúng; giới hạn power loss/fsync thư mục được nói thẳng.
- Commit `offset + 1` kèm `leaderEpoch`; `enable.auto.commit=false`; DLT `send(...).get` trước commit — đúng ack-before-commit.
- Bảng semantics Ch30 3.1: mỗi hàng có điều kiện + cửa sổ crash + kết quả; Kafka transaction ghi rõ chưa chạy và không bao external DB/API — đạt yêu cầu PROMPT Ch30.
- Bảng consumer/Spark/Flink Ch29 3.7 ghi rõ chưa chạy runtime, không gắn nhãn exactly-once chung — đạt.
- `Policy.frontier` với lỗ offset và prefix chưa hoàn tất — đúng; `withinBudget` dùng BigInt không tràn.
- Bài tập: 6/chương, có code + test, lời giải ghi rõ helper chưa tích hợp.

## Việc tác giả cần làm

Sửa M1–M4 và các MINOR hợp lý; chạy lại Scala CLI (`--server=false`) cho P4 (core + `RUN_KAFKA_IT=1`), hai lab, hai bộ lời giải; cập nhật handoff (vòng 2) với
output mới. Lead sẽ chạy lại `sbt test` + `RUN_KAFKA_IT=1`.

## Vòng 2 (17/10/2026) — Kết luận: **ĐẠT**

Kiểm chứng của reviewer trên bản sửa:

| Lệnh | Kết quả |
|---|---|
| `RUN_KAFKA_IT=1 sbt.bat test` trong `14-projects/p4-kafka-event-processing` | `Passed: Total 14, Failed 0` (CoreSuite 11 + BrokerSuite 3, KRaft thật) |
| Probe lại M1 trên `Store` mới (`clock` cố định 17/10/2026) | event năm 2096 → `Some(eventTime in the future)`; event tính bằng mili-giây → `Some(eventTime in the future)`; event đúng giờ → `accepted maxTime=1792022400` (watermark không bị đẩy) |
| Probe lại m2 (hai `Store` cùng thư mục, cùng JVM) | `IllegalStateException: store already owned` |
| Đếm code trong bài | Ch29: 4 khối Scala, Ch30: 4 khối Scala (trích nguyên source đã compile); tiêu đề lên dòng đầu |

- **M1 đã sửa:** guard `eventTime ≤ clock() + maxFutureSkew` trong đường chính, policy skew lưu trong snapshot format 2, runbook watermark trong README.
- **M2 đã sửa:** `produce` đọc fixture `key<TAB>raw` (có `\N` cho null), BrokerSuite chạy qua CLI, README dẫn quickstart broker và ghi rõ phần chưa chạy.
- **M3 đã sửa:** code cốt lõi + sơ đồ thời gian ở Ch30, văn phong ngắn hơn. Còn dày ở vài đoạn nhưng đủ để theo — không chặn.
- **M4 đã sửa:** mục 10 có thông báo thật (lock, source identity, corrupt, byte limit, future receipt, lỗi Kafka khi DLT topic thiếu).
- **MINOR còn lại, chấp nhận:** m1 (circe nhận trùng key) được ghi rõ là giới hạn + bài mở rộng; m4 (lag mỗi record) ghi rõ chi phí và cách lấy mẫu.
  Snapshot format 2 không migrate format 1 — được ghi rõ, phù hợp fail-closed.

Lead sẽ đánh dấu Ch29, Ch30, P4 ✅ và commit.
