> Scala **3.9.0** · JDK **21** · sbt **1.12.11** · kafka-clients **4.4.0** · circe-parser/generic **0.14.17** · slf4j-nop **2.0.20** · munit **1.2.4** · embedded-kafka **4.4.0**. Exact POMs kiểm tra **17/10/2026**, xem [versions.json](verification/versions.json).

# Project 4 — Kafka Event Processing

Tác giả Codex, reviewer Claude Code. Code dùng chung cho [Ch29](../../10-streaming/ch29-streaming-pipeline.md) và [Ch30](../../10-streaming/ch30-streaming-reliability.md). Đã chạy Scala CLI với `--server=false`, `-Werror`; log và số test ở mục 7. Đã sửa theo review vòng 1, chờ Claude review vòng 2 và chạy sbt cho bản sửa; không đánh dấu hoàn tất thay lead.

## 1. Mục tiêu

Xây producer/topic/consumer cho event đơn hàng có schema validation, transformation sang tổng doanh thu theo cửa sổ event time, dedup bền, retry hữu hạn, error routing vào dead-letter topic (DLT), offset management và monitoring console. Tái hiện hard kill consumer bằng JVM con trong broker KRaft nhúng thật.

Project chú trọng ranh giới dữ liệu: source có thể được đọc lại, effect local không bị cộng lại; DLT vẫn có thể được publish lặp. Không dùng Kafka transaction hoặc tuyên bố exactly-once toàn hệ thống.

## 2. Tiên quyết

Ch27/28 về thời gian/offset/key/commit, Ch29/30 về pipeline/recovery, Ch18/P2 về vòng đời tài nguyên và vận hành. Cài JDK 21 và Scala CLI; lần tải dependency đầu cần Internet. Coursier cache dùng `.cache/coursier` ở repo root, đã được ignore.

Unit test không cần broker/Docker. Integration test opt-in `RUN_KAFKA_IT=1`, khởi động KRaft thật qua embedded-kafka, mở cổng loopback động và subprocess Java, ghi thư mục temp. Đã opt-in mà môi trường không cho chạy thì **fail**, không skip che lỗi. Không cần daemon Docker.

## 3. Đặc tả

### 3.1. Hợp đồng đầu vào

Topic nguồn và DLT do lệnh init tạo: một partition, replication factor 1 cho lab. Không trỏ fixture vào cluster thật. Key = orderId; null key/value không là order event hợp lệ. JSON có đúng năm field:

| Field | Kiểu và quy tắc |
|---|---|
| version | Int = 1 |
| eventId | `[A-Za-z0-9-]{1,64}`, bất biến cho một event nghiệp vụ |
| orderId | Cùng pattern ID, phải bằng Kafka key |
| eventTime | Long >= 0, **giây epoch**, <= clock() + maxFutureSkew (mặc định 300 giây) |
| amount | Long > 0, VND nguyên |

Không dùng Schema Registry, JSON field lạ bị từ chối. Fingerprint là model Event đã decode; whitespace/thứ tự field JSON không tạo conflict. **Giới hạn parser:** field JSON trùng key được circe-parser nhận và lấy giá trị cuối; production xử lý tiền cần parser từ chối duplicate key (probe ở Ch29 mục 10). Chưa có byte limit của message trước parse. Có future-skew guard trong đường chính; timestamp quá xa tương lai/mili-giây tạo invalid/DLT reason `eventTime in the future`, không đổi watermark. Không chấp nhận v2 chỉ vì vẫn parse được vài field.

### 3.2. Hợp đồng phân loại và transformation

Source identity là topic:partition:offset. Sáu loại receipt duy nhất:

| Kind | Điều kiện | Tác động local | DLT |
|---|---|---|---|
| accepted | Schema/key đúng, ID mới, time không late, prepare thành công | Ghi seen + tổng + maxTime + receipt cùng snapshot | Không |
| duplicate | ID đã accepted có payload chuẩn hóa bằng nhau | Chỉ receipt mới | Không |
| invalid | Null/parse/field/type/version/value/key sai hoặc quá future-skew | Chỉ receipt + reason | Có |
| conflict | ID đã accepted khác nội dung | Chỉ receipt + reason | Có |
| late | ID mới, time dưới watermark cũ | Chỉ receipt + reason | Có |
| exhausted | Prepare ném Transient hết 3 attempts | Chỉ receipt + reason/attempts | Có |

Replay cùng source lấy lại receipt, không thêm số input và không tính lại watermark. Payload cùng provenance mà khác nội dung bị từ chối. Không có TTL/cleanup seen/receipts; fingerprint chỉ ghi cho event accepted. Redrive nguồn mới có thể thử lại event chưa accepted; late cũ vẫn có thể late nếu không có backfill policy.

Window width 100 giây, delay 50 giây, `late = time < max(0, maxTime_cũ - delay)`; chỉ accepted mới cập nhật maxTime. Window `[start,start+100)`, start=time/100*100; tổng BigInt theo `(start,orderId)`. Đây là tổng hiện tại, chưa có final-window output/cleanup. Chỉ thiết kế watermark đơn nguồn cho topic một partition fixture, không có multi-partition watermark/idle policy.

### 3.3. Lỗi, retry và DLT

`Retry.run` tối đa 3 attempts, pause 10/20 ms; chỉ retry `Transient` từ hook prepare read-only. CLI mặc định prepare no-op. Cờ `--demo-failures` bật adapter giả lập: eventId retry lỗi một lần rồi thành công; busy lỗi hết ba lần. Cờ chỉ để học fixture, không phải lỗi broker hay adapter production. Sink thật là file snapshot; disk error không chuyển thành exhausted/DLT mà truyền lên để dừng trước source commit.

DLT value encode cả `Receipt`: `input.topic/partition/offset/key/raw`, kind, reason, attempts. DLT key = input.id. Lưu receipt lỗi bền trước, publish chờ broker ack, sau đó commit source. DLT lỗi → source chưa commit; DLT ack xong nhưng commit lỗi → có thể phát DLT trùng. Consumer DLT cần dedup provenance; giữ namespace topic incarnation/cluster khi gom nhiều nguồn. Envelope có raw nghiệp vụ nên production cần retention/quyền đọc phù hợp.

## 4. Thiết kế và trade-off

```text
Main init/produce/consume
  Kafka adapter (send ack, poll, lag, commit)
    Pipeline.handle -> Store.process
      Json.event/key/future-skew -> fingerprint -> old watermark -> Retry.prepare
      snapshot: seen + buckets + maxTime + receipts
    if receipt.dead: DLT send ack
    source commit(offset + 1)
```

- [Model.scala](src/main/scala/vn/scalaeng/p4/Model.scala): schema, model, codec snapshot/DLT, retry policy.
- [Store.scala](src/main/scala/vn/scalaeng/p4/Store.scala): single writer file lock, snapshot replacement, receipt-first replay, điều phối publish/commit.
- [Kafka.scala](src/main/scala/vn/scalaeng/p4/Kafka.scala): topic admin, producer, consumer, CLI và CrashWorker.
- [CoreSuite](src/test/scala/vn/scalaeng/p4/CoreSuite.scala): fault injection và reopen; [BrokerSuite](src/test/scala/vn/scalaeng/p4/BrokerSuite.scala): KRaft end-to-end + actual process kill.

**Storage:** chọn file để không thêm DB/binary và nhìn thấy atomic boundary. Writer giữ `writer.lock`; ghi state.tmp, force bytes, atomic move thay state.json, rồi mới cập nhật RAM. Move không hỗ trợ thì fail, không fallback. Snapshot format **2**, width/delay/**maxFutureSkew** phải khớp khi mở. Format 1 của vòng trước không tự migrate: thiếu field mới gây snapshot corrupt, hoặc format khác gây policy mismatch. Tmp dở bị bỏ qua; JSON snapshot corrupt fail closed. Không tự sửa snapshot, chưa checksum/semantic validator đầy đủ cho sửa tay.

SQLite/PostgreSQL transaction + unique event ID phù hợp khi cần ghi gia tăng/query/index/nhiều writer. File này giữ tất cả state trong RAM và viết lại O(state) mỗi record; không scale ngang. Giới hạn snapshot mặc định **1.048.576 byte**, kiểm trước ghi tmp và trước đọc lại file; vượt ngưỡng ném `snapshot byte limit exceeded`, giữ RAM/disk cũ và không commit nguồn. Việc encode vẫn cần RAM và latency có thể quá budget trước khi đạt ngưỡng byte. Số đo thật và lệnh tái lập ở mục 6. P5 thay sink file bằng PostgreSQL.

Một store chỉ thuộc một pipeline logic và một writer; rebalance sang máy khác không tự mang state file đi. Cần storage chung/partition ownership/fencing hoặc engine nếu mở rộng.

**Failure model:** đã kiểm process kill với store còn nguyên trên cùng máy/filesystem. Chưa fsync thư mục, chưa kiểm mất điện/mất disk/mất node. Không gọi đây là distributed checkpoint. Nếu topic bị xóa/tạo lại, offset có thể tái sử dụng: namespace/store mới. Backup phải lấy nguyên snapshot nhất quán, không ghép seen/buckets từ hai thời điểm.

**Vận hành:** counts là số receipt bền theo kind; replay cùng offset không tăng. Console log lag theo partition bằng end-committed, None nếu chưa commit. Lab gọi endOffsets + committed cho mỗi record, tốn **hai round-trip broker ngoài commit**; khi tăng tải cần lấy mẫu theo N record/T giây hoặc consumer giám sát riêng. Có source identity để debug, không có HTTP metrics/health endpoint, histogram hay state-size gauge. Dòng cuối `stopRequested` là cờ vòng đời, không phải readiness probe. Production cần readiness có deadline kiểm broker/store, liveness độc lập dependency, rate/duration/retry/DLT ratio/state bytes, labels hữu hạn.

**Vòng đời:** `Using.resources` quản lý store/producer/consumer; owner thread là nơi gọi consumer API. Shutdown hook set stop + wakeup rồi chờ owner tối đa 20 giây; hard kill dựa vào replay, không chạy cleanup. SIGTERM/container chưa kiểm chứng. CLI hữu hạn dùng maxRecords + deadline 60 giây cho buổi học; hết deadline có thể handled ít hơn yêu cầu, phải xem output. Chưa là service daemon sẵn sàng production.

## 5. Cách chạy

Từ repo root, đường tái lập đầy đủ không cần broker cài sẵn:

```powershell
python 14-projects/p4-kafka-event-processing/verification/verify_sources.py
python 14-projects/p4-kafka-event-processing/verification/run_checks.py core broker ch29 ch30 sol29 sol30 errors demo simple29 simple30 measure
```

Script đặt COURSIER_CACHE vào `.cache/coursier`, TMP/TEMP/java.io.tmpdir vào `.cache/t15-tmp`, bật RUN_KAFKA_IT **chỉ** cho lượt broker. Log giữ command, output thật và exit code. Core lượt mặc định chạy 11 unit, skip ba broker test; broker lượt riêng chạy 3 test, không skip. Cùng một implementation được lab và lời giải import bằng `using files`.

Chạy thủ công tương đương:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 14-projects/p4-kafka-event-processing --server=false
$env:RUN_KAFKA_IT = '1'
$tmpPath = Join-Path $PWD '.cache/t15-tmp'
New-Item -ItemType Directory -Force $tmpPath | Out-Null
scala-cli.bat test 14-projects/p4-kafka-event-processing --server=false --test-only '*BrokerSuite' --java-opt "-Djava.io.tmpdir=$tmpPath"
Remove-Item Env:RUN_KAFKA_IT
```

### 5.1. Mở broker cho buổi thực hành thủ công

Đường tự động ở trên đã khởi động KRaft nhúng thật. Nếu muốn tự gõ từng lệnh, mở một broker riêng theo [Kafka Quickstart KRaft chính thức 4.2](https://kafka.apache.org/42/getting-started/quickstart/) (đọc ngày 17/10/2026). Tài liệu có cả cách format cluster rồi chạy server từ bản tải về, và Docker image. Ví dụ PowerShell theo nhánh Docker của tài liệu:

```powershell
docker info
docker run --rm --name p4-kafka -p 9092:9092 apache/kafka:4.2.2
```

Giữ terminal đó mở, đợi broker sẵn sàng rồi dùng terminal khác cho CLI dưới đây. Nếu docker info lỗi thì daemon chưa sẵn sàng; dùng đường test KRaft nhúng hoặc chuẩn bị broker trước. **Các lệnh Docker/localhost:9092 này chưa chạy trên máy biên soạn**, chưa kiểm client 4.4.0 với broker Docker 4.2.2; kết quả kiểm chứng của project là broker nhúng 4.4.0. Không coi hướng dẫn quickstart là bằng chứng vận hành Docker.

### 5.2. Producer đọc fixture và consumer xử lý đủ các nhánh

Chạy từ repo root, dùng tên topic/group/store mới. Init không coi TopicExists là thành công:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat run 14-projects/p4-kafka-event-processing --server=false --main-class vn.scalaeng.p4.Main -- init localhost:9092 p4-orders p4-dead
scala-cli.bat run 14-projects/p4-kafka-event-processing --server=false --main-class vn.scalaeng.p4.Main -- produce localhost:9092 p4-orders 14-projects/p4-kafka-event-processing/fixtures/orders.jsonl
scala-cli.bat run 14-projects/p4-kafka-event-processing --server=false --main-class vn.scalaeng.p4.Main -- produce localhost:9092 p4-orders 14-projects/p4-kafka-event-processing/fixtures/invalid-inputs.jsonl
scala-cli.bat run 14-projects/p4-kafka-event-processing --server=false --main-class vn.scalaeng.p4.Main -- consume localhost:9092 p4-orders p4-dead p4-group .cache/p4-state 12 --demo-failures
```

Ba main init/produce/consume đã được test gọi với bootstrap động trên KRaft thật, bao gồm chính hai file fixture này. [orders.jsonl](fixtures/orders.jsonl) chứa tám record để đối soát sáu loại receipt. [invalid-inputs.jsonl](fixtures/invalid-inputs.jsonl) thêm sai key, null value, năm 2096 và timestamp mili-giây. Với --demo-failures: 12 receipt = accepted 3 + duplicate 1 + invalid 5 + conflict 1 + late 1 + exhausted 1; total 300, DLT 8, maxTime 300. Bỏ cờ này thì retry/busy dùng prepare no-op và đều accepted: DLT giảm một, total tăng 100. Đây là lựa chọn demo được ghi rõ, không phải sink bên ngoài bị lỗi thật.

Dù mang đuôi `.jsonl`, file dùng framing **`key<TAB>raw`**, không phải mỗi dòng là JSON thuần. Dấu TAB đầu tiên tách key; phần còn lại gửi nguyên trạng, kể cả `{` hỏng. Ký hiệu `\N` (hai ký tự backslash và N trong file) biểu diễn null cho key hoặc value. Blank line hay dòng thiếu TAB bị từ chối cùng số dòng; dòng fixture không có comment. Producer đọc và kiểm framing cả file trước khi gửi, sau đó in line/topic/offset sau từng ack. Lỗi giữa chừng có thể để lại prefix đã gửi; chạy lại là resend ở offset mới, store còn nguyên sẽ nhận duplicate nghiệp vụ. Không xóa `.cache/p4-state` khi recovery thông thường.

sbt dành cho lead: vào project, đặt JDK 21, `sbt.bat test`; đặt `RUN_KAFKA_IT=1` trước lệnh để chạy cả integration. [build.sbt](build.sbt) pin dependency và fork test, tạo temp trong `.cache/`; [build.properties](project/build.properties) pin sbt 1.12.11. **Chưa chạy sbt trong task** vì sandbox BootServerSocket của máy này; Scala CLI không chứng minh riêng build sbt đã chạy.

## 6. Output

Trích output thực của [broker.txt](verification/broker.txt):

```text
P4 statuses=accepted,duplicate,invalid,conflict,accepted,late,accepted,exhausted total=300 receipts=8 committed=8 lag=0 retryAttempts=2
DLT offsets=2,3,5,7 attempts=0,0,0,3 reasons=4
producer line=1 topic=cli offset=0
source=cli:0:0 kind=accepted lag=Some(11) counts=(accepted,1)
CLI fixture receipts=12 DLT=8 futureInvalid=2 maxTime=300 total=300
```

Fixture 8 record cùng key ORDER: e1@100 amount100, bản trùng, `{`, cùng e1 amount999, ahead@300, late@100, retry@300 (lỗi lần đầu rồi thành công), busy@300 (lỗi hết 3 lần). Tính tay accepted 3 + duplicate 1 + invalid 1 + conflict 1 + late 1 + exhausted 1 = 8; tổng 300, DLT 4. Test đọc **DLT topic thật**, so source offsets/attempts/reason, không chỉ đếm callback.

Thí nghiệm kill hai event 100 ở topic khác:

```text
kill=before childExit=1 restartOffset=1 total=100 receipts=1 committed=2
kill=after childExit=1 restartOffset=0 total=100 receipts=1 committed=1
```

Tổng bằng nhau nhưng nghĩa khác: mode before bỏ event đầu, mode after chỉ phục hồi event đầu an toàn. Parent xác nhận receipt key, group commit và child đã chết; exit code forced kill phụ thuộc OS. Mỗi mode chạy process restart mới, không dùng lại ledger RAM.

Số đo snapshot thật, task `run_checks.py measure`, log [measure.txt](verification/measure.txt):

```text
n=500 ms=5060 msPerEvent=10.12 snapshotBytes=157099
n=1000 ms=12625 msPerEvent=12.625 snapshotBytes=314600
n=2000 ms=30762 msPerEvent=15.381 snapshotBytes=634600
```

Mỗi mẫu mở store mới, ID mới cùng bucket, đo cả encode/force/move; Scala 3.9.0/JDK 21, Windows, ngày 17/10/2026. Một lượt, chưa warmup/phân vị, cùng máy còn chạy kiểm broker nên timing có nhiễu. Snapshot tăng gần tuyến tính; mỗi lần viết lại toàn bộ state làm tổng công việc tăng nhanh hơn số event. Không dùng các số này làm capacity production. Byte limit 1 MiB là điểm dừng có thông báo, chưa thay thế latency/poll budget.

## 7. Kiểm thử

| Nhóm | Nội dung | Lệnh script |
|---|---|---|
| P4 CoreSuite, 11 test | Schema/key, reopen/dedup/conflict, watermark biên, retry, disk failure rollback, DLT/commit failure, corrupt/policy mismatch, writer lock/BigInt, future-skew/restart, byte limit, fixture framing | `run_checks.py core` |
| P4 BrokerSuite, 3 test | Producer CLI đọc fixture + consumer CLI/DLT; timeout DLT thật không commit; actual kill/restart hai mode trên KRaft thật | `run_checks.py broker` |
| Ch29, 2 test | Đối soát mọi nguồn và reason/raw; recovery receipt trước watermark | `run_checks.py ch29` |
| Ch30, 3 test | Frontier có lỗ, budget overflow, sink restart sau commit failure | `run_checks.py ch30` |
| Solutions Ch29/30, 6+6 test | Đủ sáu bài mỗi chương, biên/overflow/error cases | `run_checks.py sol29 sol30` |

Tổng **31 test thực chạy**, không cộng ba skip vào pass. Log [core](verification/core.txt), [broker](verification/broker.txt), [ch29](verification/ch29.txt), [ch30](verification/ch30.txt), [sol29](verification/sol29.txt), [sol30](verification/sol30.txt). Tất cả compile `-deprecation -feature -Wunused:all -Werror`. Trạng thái lượt cuối và kiểm tra nội dung ở [handoff](../../.team/handoffs/T15-ch29-30-p4.md).

## 8. Lỗi thường gặp

Thông báo runtime thật qua ErrorDemo Ch30, xem [errors.txt](verification/errors.txt):

```text
java.lang.IllegalArgumentException: requirement failed: attempts must be in 1..10
java.lang.IllegalArgumentException: requirement failed: offsets must increase
java.lang.IllegalArgumentException: requirement failed: invalid window policy
java.lang.IllegalStateException: store already owned
java.lang.IllegalArgumentException: requirement failed: source identity reused with different payload
java.lang.IllegalArgumentException: requirement failed: snapshot policy mismatch
java.lang.IllegalStateException: snapshot byte limit exceeded
java.lang.IllegalStateException: snapshot corrupt: DecodingFailure at .format: Missing required field
```

Sửa policy cấu hình; không catch rồi commit để bỏ record. Constructor store sai width phải bị chặn trước xử lý.

Timeout DLT topic không tồn tại được chạy thật với auto-create tắt, xem [broker.txt](verification/broker.txt):

```text
DLT publish: java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Topic missing-dead not present in metadata after 10000 ms.
DLT timeout receipts=1 committed=None
```

- **DLT unavailable/commit lỗi:** giữ state và group, restart khi dependency phục hồi; có thể phát DLT lại cùng key. CoreSuite fault-inject IOException để kiểm thứ tự, không bịa thông báo broker outage.
- **Store lock/corrupt/policy mismatch:** giữ file làm bằng chứng, xác nhận writer/policy và backup; không xóa state để làm lỗi biến mất. OS khóa store không thay thế distributed fencing.
- **Topic tạo lại/group sai:** offset identity có thể trùng hoặc record bị bỏ qua; tạo namespace đúng, đối soát trước reset.
- **Lag 0 mà ít doanh thu:** so counts theo kind và DLT; before-mode cho thấy commit sớm có thể mất event mà không còn lag.
- **Timestamp millis đưa vào field giây:** đường chính trả invalid/DLT `eventTime in the future`, không cập nhật maxTime; kiểm clock producer và đơn vị dữ liệu, không tự đổi timestamp để mất raw gốc.
- **Test opt-in fail trước marker:** assertion chỉ temp directory; đọc before.log/after.log/restart.log. Child lỗi classpath trước marker là test fail, không được nhận là hard kill thành công.

### Runbook: watermark bị đẩy xa

Với bản hiện tại, kiểm DLT reason `eventTime in the future` và state.maxTime: record bị chặn không được thay watermark. Nếu maxTime đã rất xa, trước hết dừng writer, giữ nguyên snapshot/log/committed offset làm bằng chứng và kiểm đơn vị giây cùng đồng hồ máy. Guard giả định clock máy đáng tin; clock máy sai xa cũng có thể nhận dữ liệu sai. Không sửa maxTime thủ công rồi coi dedup, receipts và tổng tiền đã nhất quán.

Snapshot **format 1** của vòng trước chưa có guard và thiếu maxFutureSkew, nên bản format 2 dừng khi restore. Chưa có công cụ migration tự động. Phục hồi nguyên backup tốt với đúng binary/policy tương ứng, hoặc dựng pipeline backfill tách biệt bằng format 2 với group/store/output mới. Chỉ đổi sang kết quả mới sau khi đối soát source→receipts→buckets và xử lý DLT đã duyệt; cần nguồn Kafka còn retention hoặc bản archive. Nếu nguồn đã mất, ghi rõ khoảng dữ liệu không thể tái tạo, không xóa state cũ và tuyên bố recovery thành công.

Trường hợp chỉ muốn giữ kết quả đã accepted cần migration có kiểm tra fingerprint/receipt/bucket và policy; đó là phần chưa triển khai. Đổi policy skew cũng phải qua quy trình tương tự. Snapshot mới không tự chữa dữ liệu lịch sử đã sai.

## 9. Bài tập mở rộng

1. Thay parser bằng bộ từ chối duplicate JSON key, giữ lỗi invalid có reason; test amount/version/eventTime lặp ở cả hai thứ tự. Future-skew guard đã có trong đường chính; thêm metric skew bằng labels hữu hạn nếu cần.
2. Chuyển Store sang SQLite hoặc PostgreSQL, giữ atomic seen/total/receipt; test rollback giữa các statement và kill sau DB commit.
3. Thêm endpoint readiness/metrics như P2: readiness kiểm broker và ghi store với deadline, không gọi dependency trong liveness; test failure/draining và shutdown.
4. Multi-partition: watermark min active, idle/resume và ownership state theo partition, test nguồn chậm; không chỉ sửa init thành hai partition.
5. DLT consumer dedup bền theo provenance, redrive có audit và validation. Giữ nguyên stable business ID cho cùng event, có quy trình correction cho conflict/late.
6. Kafka→Kafka transaction với read_committed: chạy test abort/commit/crash thật, nêu rõ external file/DB/API nằm ngoài transaction. Không có implementation này sẵn trong project.

## 10. Checklist

- [ ] Chạy 11 unit và 3 KRaft test với đúng JDK/Scala; phân biệt skip với pass.
- [ ] Đọc source→receipt→DLT→committed để giải thích fixture 8 record và CLI mở rộng 12 record.
- [ ] Mở lại store chứng minh ID cũ không cộng thêm và conflict không overwrite.
- [ ] Chạy hard-kill JVM, thấy offset/receipt khác giữa hai mode dù cùng total.
- [ ] Giải thích schema/version, key order, retry/poison policy và DLT trùng.
- [ ] Không coi file là DB phân tán hay claim chịu power loss; không gọi console flag là readiness.
- [ ] Chạy sáu bài tập mỗi chương; đọc nguồn/version tại [versions.json](verification/versions.json).
- [ ] Review chéo và sbt cuối do Claude thực hiện trước khi cập nhật PROGRESS/commit.
