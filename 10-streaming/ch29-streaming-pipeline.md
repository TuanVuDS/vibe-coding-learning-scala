# Chương 29 — Streaming Pipeline

> Phiên bản: Scala **3.9.0** · JDK **21** · kafka-clients **4.4.0** · circe-parser/generic **0.14.17** · slf4j-nop **2.0.20** · munit **1.2.4** · embedded-kafka **4.4.0** (test). Trạng thái: đã biên dịch & chạy; log: mục 9; bản sửa vòng 2 chờ Claude review và chạy sbt. Nguồn phiên bản kiểm tra 17/10/2026.

**Vị trí trong lộ trình:** [Ch28](ch28-kafka-scala.md) đưa record vào Kafka → [Ch29](ch29-streaming-pipeline.md) ghép pipeline → [Ch30](ch30-streaming-reliability.md) kiểm tra phục hồi → [P4](../14-projects/p4-kafka-event-processing/README.md) thực hành trọn luồng. Sau đó, Ch31 thiết kế kiến trúc data pipeline và Ch33 bàn chất lượng dữ liệu.

## 1. Mục tiêu học tập

Ghép producer → Kafka → consumer Scala → parse → schema validation → dedup → event time → error routing → retry → sink → monitoring → recovery thành một hợp đồng có thể kiểm tra. Sau chương này, bạn có thể giải thích dữ liệu nào đã được lưu, offset nào có thể commit và vì sao một lần replay không được cộng tiền lần nữa.

Đây là consumer trực tiếp, một writer, một partition nguồn trong fixture. Không xem nó như một engine phân tán. [Project 4](../14-projects/p4-kafka-event-processing/README.md) là implementation chung; lab chương này tập trung đối soát và sự bền vững của quyết định.

## 2. Kiến thức tiên quyết

Ôn [Ch27](ch27-streaming-fundamentals.md) về event time/watermark, [Ch28](ch28-kafka-scala.md) về offset và DLT, [Ch24](../09-spark/ch24-batch-data-pipeline.md) về mọi dòng vào đúng một ngăn. Ch18/P2 giải thích vòng đời tài nguyên, readiness và metrics; Ch16B giải thích `Resource`. Cần đọc được `Either`, case class, `Using` và test munit.

JDK 21 và Scala CLI phải có trên PATH. Unit test không cần broker/Docker sau khi tải dependency. Kiểm thử KRaft mở loopback socket, chỉ chạy khi đặt `RUN_KAFKA_IT=1`.

## 3. Lý thuyết

### 3.1. Biết mỗi record đã đi đâu

Giả sử consumer đọc tám record nhưng chỉ cộng được ba khoản tiền. Bạn cần biết năm record còn lại bị trùng, hỏng hay bị bỏ quên. Tổng tiền đúng ở một ví dụ chưa đủ chứng minh pipeline đúng.

Ta gắn mỗi record Kafka với khóa nguồn `(topic, partition, offset)`. `Input.id` biểu diễn khóa đó bằng chuỗi. `Receipt` là biên nhận lưu quyết định của pipeline. Mỗi khóa nguồn có đúng một receipt, thuộc một trong sáu nhóm: `accepted`, `duplicate`, `invalid`, `conflict`, `late`, `exhausted`. Bốn nhóm cuối có lý do lỗi và được gửi tới DLT, tức topic cách ly dữ liệu lỗi.

Đẳng thức sau là hợp đồng thiết kế, không phải output của chương trình:

```text
số khóa nguồn = accepted + duplicate + invalid + conflict + late + exhausted
```

Chỉ `accepted` được cộng tiền. Đọc lại cùng offset lấy receipt cũ; gửi lại cùng event ở offset mới tạo receipt `duplicate` mới. Hai tình huống này có số record nguồn khác nhau, dù cùng không tăng tiền.

Chỉ giữ counter sẽ tốn ít bộ nhớ hơn, nhưng không truy ngược được record đã mất. Receipt đầy đủ phù hợp cho lab và audit nhỏ. Với hệ lớn, cần kho audit có retention và phân quyền, vì receipt giữ cả raw nghiệp vụ. Đừng gộp số lần poll với số nguồn duy nhất: replay sẽ làm counter ấy tăng dù không có dữ liệu mới.

### 3.2. JSON hợp lệ chưa phải event hợp lệ

Một chuỗi JSON có thể parse được nhưng thiếu số tiền, sai version hoặc dùng mili-giây ở field giây. Nếu đưa thẳng vào tổng doanh thu, lỗi producer trở thành lỗi báo cáo.

`Json.event` trả `Either[String, Event]`: bên trái là lý do từ chối, bên phải là event đã decode. Nó kiểm null, cú pháp, đúng năm field, kiểu dữ liệu và quy tắc giá trị. Schema v1 có `version`, `eventId`, `orderId`, `eventTime`, `amount`. Hai ID dài 1..64 ký tự chữ/số/gạch nối; amount là VND nguyên dương; eventTime là giây epoch không âm. `Store.process` kiểm thêm key Kafka bằng `orderId` và giới hạn timestamp tương lai bằng clock được truyền vào.

Event hợp lệ được đổi thành bucket `(start, orderId, total)`. Với width 100, time 150 thuộc cửa sổ `[100,200)`. Công thức `start = eventTime / width * width` tìm đầu cửa sổ. Chuyển amount sang `BigInt` trước khi cộng giúp tổng không tràn `Long`.

JSON dễ đọc khi học; Avro/Protobuf và Schema Registry phù hợp khi cần quản trị nhiều phiên bản, nhưng thêm hạ tầng. Schema ở đây chặn field lạ, song **circe-parser hiện nhận field trùng và lấy giá trị cuối**. Vì vậy chưa được coi là kiểm tra schema an toàn cho dữ liệu tiền production. Mục 10 cho output probe; một mở rộng cần parser từ chối duplicate key. Lab cũng chưa giới hạn byte của từng message trước parse; giới hạn snapshot ở dưới không thay thế giới hạn đầu vào.

### 3.3. Ghi dedup và tổng tiền cùng một lần

Nếu ghi ID vào `seen` rồi crash trước khi cộng tiền, replay sẽ tưởng đã xử lý và bỏ mất khoản tiền. Đảo thứ tự hai phép ghi lại có thể cộng hai lần. Ta cần một ranh giới ghi chung.

`Store` giữ fingerprint (nội dung Event chuẩn hóa), tổng bucket, maxTime và receipts trong cùng `state.json`. Nó ghi file tạm, ép byte xuống filesystem bằng `force(true)`, rồi thay snapshot bằng atomic move. Sau move thành công mới gán `current` trong RAM. Khi test chèn lỗi trước move, cả RAM và snapshot cũ phải giữ nguyên. File tạm còn dở sau crash không được dùng để phục hồi.

ID đã accepted với cùng payload là duplicate; cùng ID nhưng khác amount, order hoặc time là conflict. Chỉ event accepted được thêm vào seen. Event bị từ chối vẫn có receipt nguồn để replay giữ nguyên quyết định. Store giữ seen và receipts không hết hạn; tự xóa chúng có thể làm dữ liệu cũ được áp dụng lần nữa.

File dễ quan sát và không cần DB, phù hợp bài học ít dữ liệu, một writer. Đổi lại mỗi record viết lại toàn bộ state: chi phí mỗi lần ghi là O(state), và tổng công việc khi state tăng có thể gần bậc hai. Xem số đo thật cùng lệnh tái lập ở mục 9. Snapshot bị giới hạn **1.048.576 byte**: vượt ngưỡng thì dừng trước thay file và trước source commit. Giới hạn này chặn phình file vô hạn, chưa bảo đảm latency hay giới hạn heap trong lúc encode.

Số đo ngày 17/10/2026, Scala 3.9.0/JDK 21 trên máy Windows này, mỗi dòng dùng store mới; workload có các ID mới cùng bucket:

| Event | Tổng thời gian (ms) | ms/event | Snapshot (byte) |
|---:|---:|---:|---:|
| 500 | 5060 | 10,12 | 157099 |
| 1000 | 12625 | 12,625 | 314600 |
| 2000 | 30762 | 15,381 | 634600 |

Đây là một lượt đo, không warmup hay phân vị; máy còn chạy lượt kiểm broker nên thời gian có thể dao động. Số byte cho thấy state tăng theo số event, còn thời gian tổng tăng nhanh hơn số event. Đo này giúp chọn lại storage, chưa xác lập capacity production hoặc bảo đảm mỗi record dưới poll budget.

SQLite hoặc PostgreSQL cho phép ghi gia tăng trong transaction và dùng unique index cho event ID. Khi cần nhiều writer hay throughput cao, hãy chuyển thiết kế storage thay vì tăng giới hạn file vô hạn. P5 là bước tiếp theo thay sink file bằng PostgreSQL. Atomic move của lab chỉ được kiểm với process crash trên cùng máy; chưa chứng minh chịu mất điện, mất disk hoặc quyền ghi phân tán.

### 3.4. Không để dữ liệu sai kéo watermark đi xa

Watermark là mốc để quyết định event nào quá trễ. Với `maxTime=300` và delay 50, event mới ở 250 được nhận, ở 249 bị late. Quy tắc dùng **maxTime trước record hiện tại**: `time < max(0, maxTime_cũ - delay)`.

Một timestamp năm 2096 hoặc giá trị mili-giây nhầm đơn vị có thể làm mọi event hiện tại bị late. Đường chính vì thế kiểm `eventTime <= clock() + maxFutureSkew` trước dedup/time. Clock trả giây epoch, mặc định là đồng hồ máy; skew mặc định 300 giây. Phép cộng dùng BigInt để không tràn Long. Vi phạm tạo receipt `invalid` với reason `eventTime in the future`, được DLT như các lỗi validation khác, và không đổi maxTime. Skew là policy bền trong snapshot; đổi skew khi mở lại bị từ chối.

Receipt nguồn luôn được tra trước validation mới. Nếu offset cũ đã accepted, replay không được biến nó thành late vì watermark tăng hoặc clock đổi. Test chặn cả tương lai xa lẫn mili-giây, mở lại store rồi xác nhận event đúng giờ vẫn accepted. Bài 3 tách phép kiểm skew thành hàm thuần để hiểu biên và overflow; guard đã được tích hợp vào P4.

Processing time đơn giản hơn nhưng trả lời lúc xử lý thay vì lúc giao dịch phát sinh. Dùng event time khi báo cáo theo thời điểm nghiệp vụ, và đo lateness trước khi chọn delay. Policy max của lab chỉ dành cho fixture một partition. Với nhiều partition, max toàn cục có thể phạt oan nguồn chậm; bài 2 lấy min các nguồn active và yêu cầu policy idle/resume rõ ràng. Các bucket mới là tổng hiện tại, chưa có tác vụ đóng cửa sổ hay cleanup.

### 3.5. Phân biệt dữ liệu lỗi với hạ tầng lỗi

Retry JSON hỏng mãi sẽ chặn partition. Ngược lại, đưa disk outage vào DLT rồi commit sẽ bỏ mất việc chưa được lưu. Hai loại lỗi cần cách xử lý khác nhau.

`Retry.run` chỉ bắt `Transient` từ `prepare`, tối đa ba attempts, nghỉ 10 rồi 20 ms. Prepare là bước kiểm tra chỉ đọc, không đặt HTTP charge hay tác động ngoài không thể rollback vào đó. Hết lượt tạo receipt `exhausted`; lỗi schema có attempts 0. Ghi snapshot nằm ngoài retry này: disk, lock và lỗi bất ngờ truyền lên để dừng trước commit.

Receipt lỗi được lưu bền trước khi gửi DLT, đóng vai trò outbox nhỏ. `Pipeline.handle` chờ publish được ack rồi mới commit source. Envelope DLT giữ raw, key, provenance, kind, reason và attempts. Nếu publish lỗi, source chưa commit; nếu publish thành công nhưng commit lỗi, replay có thể phát lại cùng DLT. Consumer DLT cần dedup theo provenance. Không có Kafka transaction trong implementation này.

Stop-on-poison giữ quyền quyết định cho operator nhưng làm đứng luồng. DLT cho record sau tiến lên, song có thể phá thứ tự nghiệp vụ. Retry topic giảm chặn đầu hàng, đổi lại event cùng order có thể vượt nhau. Với chuỗi trừ tiền/cập nhật balance, cần quyết định domain trước khi chọn bỏ qua một bước. CLI mặc định prepare no-op; `--demo-failures` chỉ bật lỗi giả lập cho fixture retry/busy, không mô phỏng broker outage.

### 3.6. Quan sát và phục hồi

Lag bằng 0 vẫn có thể đi cùng doanh thu bằng 0 nếu tất cả record vào DLT. Vì vậy cần xem receipt counts, số tiền và DLT cùng với tiến độ broker.

CLI in provenance, kind, counts bền và lag `end - committed`. Chưa có committed offset thì trả `None`. Lag là chênh offset, không luôn bằng số record, vì Kafka log có thể có lỗ. Để dễ quan sát từng bước, lab gọi `endOffsets` và `committed` cho mỗi record: **hai round-trip broker ngoài commit**. Khi tăng tải, nên lấy mẫu theo thời gian hoặc theo N record, qua consumer giám sát riêng nếu cần; không dùng consumer đang xử lý từ thread khác.

`Using.resources` đóng store, producer và consumer khi thân kết thúc hoặc ném lỗi. Một owner thread gọi KafkaConsumer; shutdown hook chỉ yêu cầu dừng bằng cờ và `wakeup`, rồi đợi owner tối đa 20 giây. Hard kill không chạy cleanup và phải dựa vào replay. Ch30 kiểm trường hợp này bằng JVM con thật.

Console đủ cho lab; service dài hạn cần readiness có deadline kiểm broker/store, liveness độc lập dependency, rate và duration, tỷ lệ DLT và số byte state. Không dùng event ID làm metric label. `stopRequested` chỉ là cờ nhận yêu cầu dừng, chưa là readiness endpoint.

Recovery thường giữ nguyên group, store, topic incarnation và policy width/delay/skew. Backfill với policy mới cần output/store/group riêng rồi đối soát. Không tạo store rỗng để làm lỗi biến mất: sẽ mất dedup. Nếu snapshot format 1 của vòng trước đã bị timestamp tương lai làm hỏng watermark, xem runbook P4 trước khi phục hồi.

### 3.7. Khi nào chọn engine streaming

Consumer trực tiếp cho phép nhìn rõ từng bước nhưng bạn phải tự quản lý state, watermark và recovery. Khi nhiều operator, nhiều partition và state phân tán trở thành yêu cầu chính, engine có thể phù hợp hơn. Đổi lại cần hiểu hợp đồng checkpoint và sink của engine, không chỉ đổi thư viện consumer.

| Khía cạnh | P4 consumer trực tiếp | Spark Structured Streaming | Flink |
|---|---|---|---|
| Đơn vị tiến độ | Commit offset từng partition sau receipt/DLT | Offset log và checkpoint theo query/batch | Source offsets gắn với checkpoint phân tán |
| State | Snapshot file, một writer, không cleanup | State store theo operator/checkpoint | Keyed/operator state, backend/checkpoint |
| Event time | Policy max đơn nguồn do ta viết | Watermark/operator/output mode quyết định kết quả | Watermarks, timers, idle sources theo cấu hình |
| Đầu ra lặp | Sink local idempotent; DLT có thể lặp | Tùy sink; `foreachBatch` cần tự dedup theo batch ID để tránh lặp | Tùy connector, transaction/commit phối hợp checkpoint |
| Phạm vi bảo đảm | Crash process với store còn nguyên | Nguồn replay được + checkpoint bền + sink hỗ trợ thích hợp | Checkpoint hoàn tất + nguồn replay được + sink tham gia đúng giao thức |
| Khi chọn | Ít operator, logic riêng, chấp nhận tự vận hành | SQL/DataFrame, hệ Spark, dữ liệu có cấu trúc | Luồng stateful, timers/event time phức tạp |

Bảng là đối chiếu khái niệm theo tài liệu chính thức mục 16; **chưa chạy Spark streaming/Flink trong task này**. Micro-batch Spark có điều kiện khác continuous processing; không gắn một nhãn exactly-once cho mọi chế độ/sink. Thử Spark phải có build Scala **2.13** riêng theo matrix, không thêm Spark vào build Scala 3 hiện tại.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Provenance / receipt | Định danh nguồn / biên nhận quyết định đã bền |
| Fingerprint | Nội dung chuẩn hóa để phân biệt trùng với xung đột ID |
| Atomic replacement | Thay toàn bộ snapshot bằng một thao tác nguyên tử trên filesystem hỗ trợ |
| Outbox / DLT | Bản ghi chờ phát bền vững / topic cách ly lỗi |
| Watermark / lateness | Mốc tiến độ event time / độ trễ sự kiện theo policy |
| Reconciliation / replay | Đối soát mọi đầu vào / đọc lại từ nguồn |
| Checkpoint | Mốc phục hồi bao gồm tiến độ và state theo giao thức của hệ thống |

## 5. Ví dụ đơn giản

Ta bắt đầu với một đơn hàng 100 VND để quan sát dedup mà chưa cần broker. Hai offset khác nhau mang cùng event ID và payload. Dự đoán trước: có hai receipt, chỉ một lần cộng tiền.

Ví dụ sau là toàn bộ `SimpleDemo` trong lab, dùng đúng Store của P4:

```scala
package vn.scalaeng.pipeline

import java.nio.file.Files
import scala.util.Using
import vn.scalaeng.p4.{Event, Input, Json, Pipeline, Store}

object SimpleDemo:
  def main(args: Array[String]): Unit =
    val input = Input("orders", 0, 0, Some("ORDER"), Some(Json.encode(Event(1, "e1", "ORDER", 100, 100))))
    Using.resource(new Store(Files.createTempDirectory("ch29-simple-"))) { store =>
      println(store.process(input).kind)
      println(store.process(input.copy(offset = 1)).kind)
      println(store.state.buckets.map(_.total).sum)
    }
```

Chạy từ repo root:

```powershell
scala-cli.bat run 10-streaming/streaming-pipeline-lab --server=false --main-class vn.scalaeng.pipeline.SimpleDemo
```

Output đã chạy, lưu trong [simple29.txt](../14-projects/p4-kafka-event-processing/verification/simple29.txt):

```text
accepted
duplicate
100
```

Lần đầu thêm fingerprint và bucket. Lần thứ hai có source mới nhưng Event đã seen nên không cộng thêm. Demo đầy đủ ở mục 9 bổ sung JSON hỏng, conflict, late và reopen thật.

## 6. Ví dụ thực tế (BE và DE)

**BE:** consumer thanh toán cần commit sau khi tổng và dedup cùng bền. Producer idempotence chỉ xử lý retry giao thức producer; ứng dụng gửi hai lần vẫn có hai offset. Receipt duplicate cho thấy bản thứ hai đã được xét, thay vì biến mất khỏi audit.

**DE:** báo cáo doanh thu theo event time nhận `e1@100`, rồi event mới @300, rồi event mới @100. Event cuối vào DLT late; nhóm vận hành có thể lập backfill riêng. Không lấy kết quả late bằng cách xóa state production.

Fixture demo 6 record: accepted 2 + duplicate 1 + invalid 1 + conflict 1 + late 1 = 6; total 200, DLT 3. Fixture broker P4 thêm retry thành công và exhausted: 8 record, total 300, DLT 4. Đây là hai tập dữ liệu khác nhau, không đối chiếu nhầm tổng.

## 7. Code đầy đủ

- [Demo.scala](streaming-pipeline-lab/src/main/scala/vn/scalaeng/pipeline/Demo.scala), [PipelineSuite.scala](streaming-pipeline-lab/src/test/scala/vn/scalaeng/pipeline/PipelineSuite.scala), [project.scala](streaming-pipeline-lab/project.scala).
- Lõi dùng chung: [Model.scala](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala), [Store.scala](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Store.scala), [Kafka.scala](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Kafka.scala).
- [CoreSuite](../14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/CoreSuite.scala) và [BrokerSuite](../14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/BrokerSuite.scala) kiểm tra lỗi disk/DLT/commit và broker thật.

Từ repo root:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 10-streaming/streaming-pipeline-lab --server=false
scala-cli.bat test solutions/ch29-code --server=false
scala-cli.bat run 10-streaming/streaming-pipeline-lab --server=false --main-class vn.scalaeng.pipeline.Demo
```

Script giữ temp/cache trong repo và lưu log: `python 14-projects/p4-kafka-event-processing/verification/run_checks.py ch29 sol29 demo`. KRaft và sbt xem README P4; lab dùng Scala CLI directives trỏ tới source chung, không sao chép implementation.

## 8. Giải thích code

### 8.1. Đọc `process` theo thứ tự quyết định

Đừng bắt đầu từ phép cộng tiền. Hãy đọc từ receipt nguồn xuống: nếu đã có receipt thì replay trả quyết định cũ. Với nguồn mới, parse/schema/key và future-skew chặn dữ liệu sai, seen phân biệt duplicate/conflict, rồi watermark mới xét late. Chỉ nhánh còn lại được chạy prepare và cộng tiền.

Đây là thân hàm thật trong Store; `current` là snapshot đã công bố, còn `next` là ứng viên chưa được ghi:

```scala
  def process(in: Input, prepare: Event => Unit = _ => (), maxAttempts: Int = 3,
      pause: Int => Unit = n => Thread.sleep(10L * n)): Receipt =
    require(in.partition >= 0 && in.offset >= 0 && in.topic.matches("[A-Za-z0-9._-]+"), "invalid source identity")
    current.receipts.get(in.id) match
      case Some(old) =>
        require(old.input == in, "source identity reused with different payload")
        old
      case None =>
        var next = current
        val result = Json.event(in.raw) match
          case Left(reason) => Receipt(in, "invalid", Some(reason), 0)
          case Right(e) if in.key != Some(e.orderId) => Receipt(in, "invalid", Some("key must equal orderId"), 0)
          case Right(e) if BigInt(e.eventTime) > BigInt(clock()) + maxFutureSkew =>
            Receipt(in, "invalid", Some("eventTime in the future"), 0)
          case Right(e) => current.seen.get(e.eventId) match
            case Some(old) if old == e => Receipt(in, "duplicate", None, 0)
            case Some(_) => Receipt(in, "conflict", Some("event_id conflict"), 0)
            case None if e.eventTime < (current.maxTime - delay).max(0) =>
              Receipt(in, "late", Some("event time below previous watermark"), 0)
            case None => Retry.run(maxAttempts, pause)(prepare(e)) match
              case Left((reason, attempts)) => Receipt(in, "exhausted", Some(reason), attempts)
              case Right(attempts) =>
                val start = e.eventTime / width * width
                val previous = current.buckets.find(b => b.start == start && b.orderId == e.orderId)
                val updated = Bucket(start, e.orderId, previous.fold(BigInt(0))(_.total) + BigInt(e.amount))
                next = current.copy(maxTime = current.maxTime.max(e.eventTime),
                  seen = current.seen.updated(e.eventId, e), buckets =
                    current.buckets.filterNot(b => b.start == start && b.orderId == e.orderId) :+ updated)
                Receipt(in, "accepted", None, attempts)
        save(next.copy(receipts = next.receipts.updated(in.id, result)))
        result
```

`save` ở cuối luôn ghép receipt với state nghiệp vụ thành một giá trị. Nhánh duplicate cũng có receipt mới nhưng không thêm seen hay amount. Lỗi bất ngờ từ prepare hoặc disk thoát khỏi hàm, nên caller không được commit nguồn.

### 8.2. Khi nào state mới được công bố?

```scala
  private def save(next: State): Unit =
    val encoded = Json.snapshot(next).getBytes(UTF_8)
    checkSize(encoded.length.toLong)
    val tmp = dir.resolve("state.tmp") // Bỏ qua tmp còn dở sau crash, không phục hồi từ nó.
    Using.resource(FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)) { channel =>
      val bytes = ByteBuffer.wrap(encoded)
      while bytes.hasRemaining do channel.write(bytes)
      channel.force(true)
    }
    beforeReplace()
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    current = next // Chỉ công bố state RAM khi thay snapshot thành công.
```

`checkSize` chặn snapshot lớn hơn budget trước khi mở file tạm. Vòng ghi đợi hết ByteBuffer rồi mới force; move là điểm thay snapshot. Nếu `beforeReplace` ném lỗi trong test, dòng `current = next` chưa chạy. Callback đó chỉ dùng chèn lỗi, không đặt cập nhật DB hay gọi mạng vào đây.

### 8.3. Từ snapshot đến DLT và commit

```scala
object Pipeline:
  /** Chờ DLT ack trước source commit; lỗi truyền lên để replay nguồn. */
  def handle(store: Store, input: Input, publish: Receipt => Unit, commit: () => Unit,
      prepare: Event => Unit = _ => (), pause: Int => Unit = n => Thread.sleep(10L * n)): Receipt =
    val result = store.process(input, prepare, pause = pause)
    if result.dead then publish(result)
    commit()
    result
```

`publish` phải chờ ack. Adapter Kafka thực hiện `send(...).get(...)`, nên callback chỉ trả về sau ack hoặc ném lỗi. Đổi thành fire-and-forget sẽ làm commit có thể chạy trước DLT. CoreSuite chèn lỗi publish/commit để kiểm thứ tự; BrokerSuite kiểm thêm timeout Kafka thật khi DLT topic không tồn tại.

## 9. Output mong đợi

Đã chạy demo file thật, 2 test lab và 6 test lời giải, đều exit 0; log [demo.txt](../14-projects/p4-kafka-event-processing/verification/demo.txt), [ch29.txt](../14-projects/p4-kafka-event-processing/verification/ch29.txt), [sol29.txt](../14-projects/p4-kafka-event-processing/verification/sol29.txt). Trích output demo:

```text
offset=0 kind=accepted reason=-
offset=1 kind=duplicate reason=-
offset=2 kind=invalid reason=invalid JSON
offset=3 kind=conflict reason=event_id conflict
offset=4 kind=accepted reason=-
offset=5 kind=late reason=event time below previous watermark
input=6 receipts=6 commits=6 DLT=3 total=200
reopen=duplicate total=200
```

`reopen` tạo Store mới đọc disk, gửi cùng event tại offset mới; tổng không tăng. P4 còn chạy 11 unit và 3 KRaft test, gồm fixture gốc 8 record và CLI mở rộng 12 record, xem [broker.txt](../14-projects/p4-kafka-event-processing/verification/broker.txt). Các build đều bật `-Werror`; sbt và review vòng 2 chờ Claude. Xem kết quả tổng hợp trong [handoff](../.team/handoffs/T15-ch29-30-p4.md).

Đo chi phí snapshot bằng `python 14-projects/p4-kafka-event-processing/verification/run_checks.py measure`. Task chạy main `vn.scalaeng.pipeline.SnapshotMeasure` trong lab bằng Scala CLI, ghi temp/cache trong `.cache/`. Output thật ở [measure.txt](../14-projects/p4-kafka-event-processing/verification/measure.txt):

```text
n=500 ms=5060 msPerEvent=10.12 snapshotBytes=157099
n=1000 ms=12625 msPerEvent=12.625 snapshotBytes=314600
n=2000 ms=30762 msPerEvent=15.381 snapshotBytes=634600
```

## 10. Lỗi thường gặp

Các dòng dưới được tái hiện bởi `ErrorDemo`, không phải thông báo suy đoán. Chạy task `errors` của runner; log đầy đủ ở [errors.txt](../14-projects/p4-kafka-event-processing/verification/errors.txt).

**1. Width bằng 0 hoặc delay âm.** Constructor dừng ngay, tránh khởi động với cách chia cửa sổ khác hợp đồng:

```text
java.lang.IllegalArgumentException: requirement failed: invalid window policy
```

Sửa cấu hình, không tự ép width thành 1.

**2. Hai Store cùng thư mục trong một JVM.** OS file lock chỉ có một writer. Code đổi OverlappingFileLockException sang thông báo có nghĩa, giống trường hợp process khác giữ lock:

```text
java.lang.IllegalStateException: store already owned
```

Đóng owner cũ hoặc chọn store của pipeline khác. Không bỏ cơ chế khóa.

**3. Cùng source identity nhưng sửa payload.** Probe dùng lại offset 0 với raw khác:

```text
java.lang.IllegalArgumentException: requirement failed: source identity reused with different payload
```

Xác nhận topic incarnation và provenance. Correction phải là record mới có audit; không sửa receipt đã bền.

**4. Policy khác khi mở lại.** Probe ghi bằng delay 50 rồi mở bằng 51:

```text
java.lang.IllegalArgumentException: requirement failed: snapshot policy mismatch
```

Giữ policy cũ khi recovery. Đổi width/delay/skew hoặc format cần migration/backfill đã đối soát.

**5. Snapshot sai schema.** Probe ghi `{}` vào file thử, tuyệt đối không vào state đang dùng:

```text
java.lang.IllegalStateException: snapshot corrupt: DecodingFailure at .format: Missing required field
```

Giữ bằng chứng và phục hồi nguyên snapshot từ backup; không ghép từng phần seen/buckets.

**6. Vượt budget snapshot.** Probe mở state lớn hơn giới hạn cấu hình:

```text
java.lang.IllegalStateException: snapshot byte limit exceeded
```

Chương trình dừng trước commit. Đo latency/state rồi chuyển storage; tăng limit chỉ sau đánh giá budget. Xóa seen để giảm byte có thể làm event cũ bị cộng lại.

**7. Timestamp tương lai.** Đây là lỗi dữ liệu được ghi receipt và DLT, nên không ném exception. Probe có clock cố định 100, nhận event đúng giờ rồi thử time 4000000000:

```text
future kind=invalid reason=eventTime in the future maxTime=100
```

MaxTime không đổi. Kiểm đơn vị giây/mili-giây và clock producer; xem runbook P4 nếu dùng snapshot cũ chưa có guard.

**8. JSON trùng key.** Probe thêm field amount lần hai vào event đã encode:

```text
duplicate JSON fields: Right(Event(1,e1,ORDER,100,5))
```

Parser hiện lấy giá trị cuối. Production xử lý tiền cần từ chối ngay duplicate key bằng parser phù hợp, vì các parser khác có thể hiểu khác nhau. Kiểm đúng tập field chưa đủ để phát hiện lỗi này. Đây là giới hạn còn lại được ghi rõ, chưa có strict duplicate-key parser trong P4.

## 11. Cách debug

1. Tra cùng topic/partition/offset trong source, `state.json` và DLT. Kiểm tra event ID riêng để tìm resend nghiệp vụ.
2. So số khóa receipts với tổng sáu nhóm; không cộng số lần poll làm input audit.
3. So maxTime cũ, delay, event time và nhánh dedup trước khi đổi watermark.
4. Khi source chưa commit, kiểm tra snapshot đã có receipt hay chưa, rồi DLT đã ack hay chưa. Không commit thủ công để che lỗi publish.
5. Đo lag từng partition, DLT ratio, state size và thời gian ghi snapshot. Snapshot lớn dần có thể vượt poll budget dù retry nhỏ.
6. Với lỗi startup, xác nhận writer lock, quyền atomic move, policy và topic incarnation; chạy unit trước rồi KRaft opt-in.

## 12. Bài tập (cơ bản → nâng cao)

1. **Đối soát:** nhận vector receipts + số source kỳ vọng; trả counts, chặn source trùng, kind lạ và lỗi thiếu reason. Không đếm DLT publish lặp như input mới.
2. **Watermark nhiều partition:** lấy min watermark các partition active; một active chưa có mốc thì kết quả unknown. Idle set tường minh, tất cả idle cũng unknown. Giải thích nguy cơ khi partition resume.
3. **Future event:** chấp nhận `eventTime <= now + skew`, chặn thời gian âm, không tràn Long. Test đúng biên và sát `Long.MaxValue`. Đồng hồ `now` phải được truyền vào để test.
4. **Đọc DLT:** thu gọn envelope trùng theo provenance. Cùng provenance nhưng nội dung khác phải báo xung đột, không lấy tùy tiện bản cuối.
5. **Budget queue:** tính `partitions * queuedEach * maxRecordBytes` bằng BigInt, so budget. Giải thích vì sao vừa budget queue chưa bảo đảm vừa tổng RAM/state.
6. **Đóng cửa sổ:** với start, width, watermark không âm, trả cửa sổ đã đóng khi end <= watermark. Không tràn Long khi tính end, test biên nửa mở. Chưa thêm cleanup/publish vào store.

## 13. Đáp án và lời giải

Xem [lời giải sáu bài](../solutions/ch29-solutions.md), [code](../solutions/ch29-code/src/main/scala/vn/scalaeng/solutions/ch29/Exercises.scala), [6 test](../solutions/ch29-code/src/test/scala/vn/scalaeng/solutions/ch29/ExercisesSuite.scala). Lời giải phân biệt helper thuần với tính năng đã tích hợp; global watermark vẫn là helper; future-skew guard đã nằm trong P4 Store và có test riêng.

## 14. Câu hỏi kiểm tra

1. Vì sao tổng receipt lỗi bằng 4 nhưng DLT broker có thể có 5 record? (Publish lặp sau commit lỗi.)
2. Khi JSON đổi thứ tự field mà các giá trị giữ nguyên, event có là conflict không? (Fingerprint là Event chuẩn hóa.)
3. Tại sao snapshot phải chứa cả seen và total? (Tránh crash ở giữa hai phép ghi.)
4. Snapshot bền của lab có chứng minh chịu mất điện không? (Chưa, chỉ thử kill process.)
5. Vì sao duplicate dưới watermark không phải late? (Dedup/receipt có ưu tiên, tác động đã được ghi.)
6. Khi nào chuyển sang SQLite/PostgreSQL hoặc engine? (State/throughput/ownership vượt mô hình một file; chọn theo yêu cầu operator và vận hành.)

## 15. Mini project

Chạy P4, thêm meter `snapshot_bytes`, thời gian xử lý và số lần phát DLT, dùng labels hữu hạn. Tạo workload tăng số event ID để thấy chi phí snapshot; so với update SQL trong một transaction nếu chọn mở rộng storage. Không coi benchmark file nhỏ là hiệu năng production.

Tiêu chí: mọi nguồn vào một ngăn, rejected có reason/raw/provenance, replay không tăng tiền, DLT failure chưa commit, test restart mở lại state thật. Nếu thêm multi-partition event time, phải có test partition chậm/idle/resume và mô tả policy mới.

## 16. Nguồn tài liệu chính thức

Đối chiếu **17/10/2026**:

- [KafkaConsumer JavaDoc 4.2](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html): position/commit, thread ownership, pause/poll. [Kafka design](https://kafka.apache.org/42/design/design/): delivery và ranh giới transaction. Tài liệu truy cập bản 4.2; hành vi dùng với client/broker **4.4.0** được kiểm bằng test thật, không suy default 4.4 từ 4.2.
- [Spark Structured Streaming guide 4.0.1](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html): checkpoint, watermark, sink và `foreachBatch`. Chỉ đối chiếu khái niệm, không chọn runtime Spark trong task này; phiên bản Spark chạy được của repo theo compatibility matrix.
- [Flink fault tolerance](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/): state/checkpoint/replay và phối hợp sink; tài liệu nhánh stable tại ngày đọc, không phải pin dependency Flink.
- [Files.move JDK 21](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#move(java.nio.file.Path,java.nio.file.Path,java.nio.file.CopyOption...)), [FileChannel.force](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html#force(boolean)): điều kiện atomic move và force, không mặc định filesystem nào cũng hỗ trợ.
- [Exact release POMs và SHA-256](../14-projects/p4-kafka-event-processing/verification/versions.json), [script tải lại](../14-projects/p4-kafka-event-processing/verification/verify_sources.py): Scala 3.9.0, Kafka 4.4.0, embedded-kafka_3 4.4.0, circe-parser/generic_3 0.14.17, munit_3 1.2.4, slf4j-nop 2.0.20, sbt 1.12.11. Artifact server Kafka `_2.13` là implementation test, không biến source P4 thành Scala 2.13.

## 17. Checklist tự đánh giá

- [ ] Phân biệt source receipt, event fingerprint, sink amount và DLT publish count.
- [ ] Giải thích atomic boundary của snapshot, cả ưu điểm lẫn giới hạn crash process.
- [ ] Kiểm tra parse/schema/key trước dedup/time; không retry poison.
- [ ] Giải thích vì sao receipt replay không tính lại late.
- [ ] Chạy test lab, sáu test lời giải và P4 KRaft opt-in, đọc exit code thật.
- [ ] So sánh semantics consumer/Spark/Flink theo source/state/sink, không dùng cam kết tuyệt đối.
- [ ] Nêu rõ chưa có cleanup, distributed ownership, HTTP metrics/readiness hay multi-partition watermark production.
