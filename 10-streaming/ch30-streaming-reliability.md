# Chương 30 — Streaming Reliability

> Phiên bản: Scala **3.9.0** · JDK **21** · kafka-clients **4.4.0** · circe-parser/generic **0.14.17** · slf4j-nop **2.0.20** · munit **1.2.4** · embedded-kafka **4.4.0** (test). Đã chạy Scala CLI với `-Werror`; trạng thái và log ở mục 9. Bản sửa vòng 2 chờ Claude review và chạy sbt. Nguồn kiểm tra 17/10/2026.

**Vị trí trong lộ trình:** [Ch28](ch28-kafka-scala.md) đưa record vào Kafka → [Ch29](ch29-streaming-pipeline.md) ghép pipeline → [Ch30](ch30-streaming-reliability.md) kiểm tra phục hồi → [P4](../14-projects/p4-kafka-event-processing/README.md) thực hành trọn luồng. Sau đó, Ch31 thiết kế kiến trúc data pipeline và Ch33 bàn chất lượng dữ liệu.

## 1. Mục tiêu học tập

Giải thích at-most-once, at-least-once và exactly-once với phạm vi, điều kiện và cửa sổ lỗi cụ thể. Tự chạy thí nghiệm giết consumer JVM trên KRaft thật, đọc offset sau restart, đối chiếu sink bền và phân biệt “record được giao lại” với “tiền bị cộng lại”. Thiết kế commit frontier, replay, ordering, poison policy, backpressure và tín hiệu vận hành.

## 2. Kiến thức tiên quyết

Học Ch27/28/29: offset là tiến độ theo partition, producer idempotence không khử resend ứng dụng, snapshot P4 chứa fingerprint + tổng + receipt. Ôn Ch18/P2 về resource lifecycle, health/readiness, metrics và shutdown. Đọc được `Using`, Java `ProcessBuilder`, munit và fault injection. Chương này dùng code P4 chung, không có Kafka transaction hay Spark/Flink runtime.

## 3. Lý thuyết

### 3.1. Delivery semantics phải gắn với một biên

Câu “exactly-once” không cho biết HTTP charge, Kafka output hay tổng local nào được bảo vệ. Delivery là việc record được giao tới consumer. Effect là tác động nghiệp vụ, chẳng hạn cộng 100 VND vào doanh thu. Record có thể được giao hai lần mà số tiền chỉ được cộng một lần nếu sink nhận ra lần lặp.

Hãy đọc bảng theo thứ tự: trước hết chọn phần dữ liệu đang quan sát, rồi kiểm tra điều kiện của nó. Chữ “exactly-once” đứng một mình chưa trả lời đủ các câu hỏi ấy.

| Mức bảo đảm | Trình tự và điều kiện | Cửa sổ crash | Kết quả có thể xảy ra |
|---|---|---|---|
| At-most-once delivery của group | Commit tiến độ trước xử lý; không rewind/group mới/resend ứng dụng | Sau commit, trước effect | Record bị bỏ qua; không tự phục hồi bằng restart |
| At-least-once xử lý nguồn | Effect hoặc DLT đã ack rồi mới commit; retention đủ, offset còn tồn tại, worker tiếp tục retry/replay | Sau effect, trước commit | Record được xử lý lại; sink ngoài phải chịu được lặp |
| Một effect local cho event ID | Seen/fingerprint và effect ghi atomically trong cùng snapshot, cùng store còn nguyên, một writer, ID ổn định | Sau snapshot, trước commit | Giao lại nhưng không cộng thêm; không bao trùm DLT/mạng/mất máy |
| Kafka read-process-write transaction (khái niệm) | Transactional producer có ID/epoch thích hợp, output và group offsets trong cùng Kafka transaction, downstream `read_committed` | Transaction chưa commit bị abort/fence | Output Kafka của transaction đã commit được đọc theo isolation; external DB/API chưa tham gia |

Hàng cuối **chưa triển khai/chưa chạy** trong task. Kafka transaction không làm số tiền trong PostgreSQL hoặc request HTTP cùng commit. Với DB, có thể ghi idempotency key và thay đổi nghiệp vụ trong một transaction DB; Kafka offsets riêng vẫn có replay, nhưng unique key làm effect DB idempotent. Muốn dùng chữ “exactly-once” phải nói rõ quan sát, ID, giao thức commit và failure model.

At-most-once giảm việc xử lý lặp nhưng có thể mất dữ liệu; dùng khi bỏ được một số mẫu telemetry. At-least-once + idempotent sink dễ áp dụng cho business event nhưng tốn dedup state. Kafka transaction tốt cho Kafka→Kafka khi connector/isolation đúng, thêm fencing, transaction timeout và vận hành. Lỗi thiết kế là bật producer idempotence rồi gắn nhãn exactly-once cho toàn pipeline.

Hai cửa sổ crash cần nhớ (sơ đồ thiết kế, không phải log):

```plain
commit trước:  poll(0) -> commit(1) -> [CRASH] -> effect chưa có
restart:       poll(1)                         offset 0 bị bỏ qua

ghi trước:     poll(0) -> snapshot  -> [CRASH] -> commit chưa có
restart:       poll(0) -> receipt cũ -> commit(1), không cộng lại
```

### 3.2. Offset management và frontier

`poll` tiến position trước khi ứng dụng xử lý xong. Commit position cả batch khi mới hoàn tất một task sẽ làm mất phần còn lại nếu crash.

P4 tắt auto-commit bằng `enable.auto.commit=false` và chỉ lấy tối đa một record mỗi poll bằng `max.poll.records=1`. Một owner thread xử lý tuần tự. Khi record đã hoàn tất, nó commit riêng partition ở `record.offset + 1`, kèm leader epoch do record cung cấp. Leader epoch là thông tin thế hệ leader của partition; giữ nó cùng offset giúp broker/client kiểm tra tiến độ với ngữ cảnh log phù hợp. 

Kafka committed offset là vị trí đọc kế tiếp, không phải số record cuối đã xử lý. Offset có thể có lỗ; nếu đã xử lý các offset 0,2,5 thì vị trí sau record 5 là 6.

Khi xử lý song song, cần frontier (biên hoàn thành) theo **prefix đã đọc** của mỗi partition. Với 0 xong, 2 chưa xong, 5 xong, chỉ được commit 1. Khi 2 xong, prefix đến 5 hoàn tất, mới commit 6. `Policy.frontier` trả `None` khi record đầu chưa xong; không suy “offset kế tiếp là 1” từ việc thiếu record 1 trong log. Rebalance có thể làm commit thất bại; giữ sink idempotent và để owner mới replay, không cố commit bằng assignment cũ.

Lưu offset trong cùng DB transaction với sink có thể cho ranh giới mạnh hơn, nhưng phải khởi tạo seek từ offset DB và quản lý quyền sở hữu/fencing chính xác. Kafka commit riêng dễ quan sát bằng công cụ group nhưng không atomic với file/DB. Chọn kiểu sau để học replay; không dùng auto-commit như giải pháp cho parallel processing.

### 3.3. Replay và recovery là thao tác có hợp đồng

Đổi group ID có thể đọc từ đầu nhưng không tự biết sink cần giữ kết quả hay tính lại. Dùng store cũ trả receipt cũ; xóa store nhưng giữ commit lại có thể bỏ mất lịch sử.

Recovery thông thường giữ group, store, schema/policy và topic incarnation. Incarnation là một lần tồn tại của topic: xóa rồi tạo topic cùng tên vẫn là lịch sử nguồn khác. Topic mới có thể dùng lại offset 0, nên không được nhầm nó với receipt offset 0 của topic cũ. 

Receipt nguồn được đọc trước dedup/time để quyết định accepted cũ không biến thành late. Retry cùng offset không thêm receipt; event gửi lại tại offset mới tạo receipt duplicate. Snapshot lỗi hoặc policy khác phải dừng để giữ bằng chứng (fail closed). P4 kiểm format 2, width, delay và maxFutureSkew; snapshot format 1 cần migration/backfill có đối soát, không tự mở như dữ liệu mới. 

Retention Kafka là thời gian broker còn giữ dữ liệu để đọc lại. Nó phải dài hơn thời gian ngừng dịch vụ và replay cần thiết; khi offset đã hết hạn, `earliest` chỉ đọc dữ liệu còn lại, không tái tạo dữ liệu đã bị xóa.

Backfill/correction là pipeline riêng: ghi vào output mới, group/store mới, policy được version hóa, đối soát rồi publish. Nếu chỉ muốn drain source mà giữ effect cũ thì dùng group mới + store cũ cũng có thể dedup, nhưng cần xác định cùng pipeline/namespace; không chạy hai group đồng thời cùng file writer. Không sửa raw của record đã có provenance: `Store` từ chối identity dùng lại khác payload.

Seek thủ công cho phép replay nhanh nhưng nguy hiểm nếu vận hành nhầm group/topic. Pipeline mới tốn storage và bước publish nhưng cho kết quả có thể review và rollback. Không xóa snapshot production chỉ vì lag bằng 0 hoặc compiler đã đổi schema.

### 3.4. Thí nghiệm kill thật thay vì đóng consumer lịch sự

Close/reopen cùng JVM còn giữ ledger RAM, không chứng minh process-crash persistence. Ch28 chỉ cho thấy ranh giới effect/commit qua đóng/mở; P4 đi xa hơn bằng process con.

`BrokerSuite` mở KRaft cổng động, tạo topic `crash` một partition, producer gửi 2 record amount 100, ID stable và next. Parent khởi động `CrashWorker` bằng Java 21/classpath test. Child chỉ ghi marker **sau** khi đạt biên cần thử; parent đợi marker rồi gọi `destroyForcibly()` và `waitFor`, không ném exception giả để gọi là crash. Broker tiếp tục chạy trong parent.

- Mode `before`: child đọc offset 0, commit 1, chưa viết sink, rồi đợi bị giết. Child mới cùng group đọc offset 1; sink mới chỉ chứa event next. Tổng 100 nhìn có vẻ hợp lý nhưng event stable bị mất.
- Mode `after`: child đọc offset 0, ghi snapshot total 100 và receipt, chưa commit, rồi bị giết. Child mới cùng group và cùng store đọc offset 0; receipt đã bền nên không tăng tiền, commit 1. Store còn đúng một receipt cho offset 0.

Hai mode có store và group riêng. Assertion kiểm tra marker offset, child thực sự đã chết, exit của restart, receipt key, total và group committed offset trên broker. Child log trong `.cache/t15-tmp/p4-kill-*/`; output tóm tắt lưu ở verification/broker.txt. Exit code của forced termination do Windows trả, không phải business error code; không dùng nó để đo delivery semantics.

`assign` chủ ý loại rebalance khỏi thí nghiệm để cô lập ranh giới ghi/commit; không chứng minh failover ownership giữa hai worker `subscribe`. P4 có test CLI subscribe bình thường; thí nghiệm rebalance thật ở Ch28. Không thử power loss, broker quorum failure, disk corruption giữa force/move, hay SIGTERM container. Một broker RF1 chỉ phù hợp fixture, không chứng minh chịu mất node.

Nhìn theo thời gian, marker nằm sau bước đã hoàn tất:

```plain
before:  commitSync(1) -------- marker -> parent kill -> restart poll(1)
after:   Store.process(0) ---- marker -> parent kill -> restart poll(0)
                              ^ broker vẫn chạy trong parent
```

Vì vậy marker không chỉ báo child đang sống. Nó chứng minh child đã vượt đúng biên trước khi parent kill. Nếu child chết trước marker thì test thất bại; không được gọi lỗi khởi động là thí nghiệm crash thành công.

### 3.5. Ordering theo partition/key

Các transition Paid→Shipped đến ngược hoặc được xử lý song song có thể làm sai state dù mỗi event chỉ áp dụng một lần.

Kafka giữ thứ tự record trong từng partition. Khi producer không chỉ định partition, key tham gia chọn partition. Muốn dựa vào thứ tự của một order, các event cùng key phải dùng cùng quy tắc phân vùng và số partition ổn định. Không có thứ tự chung giữa các partition.

Tăng số partition có thể đổi nơi key được gửi tới. Nhiều producer cũng không tự có sequence (số thứ tự nghiệp vụ) chung. P4 kiểm key bằng orderId, dùng producer idempotence và acks=all; consumer xử lý tuần tự nên giữ thứ tự log trong fixture một partition. Bảo đảm đó chưa nói event nào là Paid hay Shipped theo nghiệp vụ.

Event time không đồng nghĩa offset order; P4 có thể thấy time 300 trước 100. Nếu cho worker pool hoàn tất tùy ý thì cần hàng theo partition/key và commit frontier. Nếu DLT bỏ qua event lỗi hoặc retry topic nhận lại muộn, các event sau vẫn có thể vượt nó.

Một partition cho thứ tự chung nhưng giảm song song; nhiều partition theo orderId tăng throughput nhưng chỉ giữ thứ tự cục bộ. Thêm sequence/version nghiệp vụ cho phép chặn update cũ, đổi lại phải giải quyết thiếu sequence/correction. Bài 3 chỉ kiểm tra sequence theo key; nó không phải implementation của producer partitioner hay idempotent state machine.

### 3.6. Poison messages và retry

Record schema sai không lành lại sau sleep; retry vô hạn có thể làm đứng partition. Outage Kafka/DLT/disk không phải lỗi nghiệp vụ để bỏ qua.

Khi decode, version, key hoặc future-skew sai, P4 ghi receipt invalid cùng lý do rồi đưa vào DLT. Conflict và late cũng đi DLT theo policy đã chọn. Những lỗi dữ liệu ấy không cần sleep để thử lại cùng raw.

Chỉ prepare chỉ đọc ném `Transient` mới được thử tối đa ba lần; hết lượt thành exhausted. Cờ --demo-failures của CLI bật ví dụ retry/busy để người học thấy hai đường này. Nếu không bật, prepare mặc định không tạo lỗi.

Lỗi ghi snapshot hoặc publish/commit được truyền lên để lần chạy sau replay. Publish DLT phải đợi ack rồi mới commit. Bởi vậy có thể phát DLT trùng sau commit failure, nhưng không bỏ mất DLT vì commit chạy trước publish.

Redrive (đưa lại record lỗi) tạo **record nguồn mới** sau khi operator sửa/duyệt; giữ provenance cũ trong audit, giữ event ID nghiệp vụ nếu cùng event. Accepted cũ sẽ dedup; conflict ID đã accepted cần quy trình correction, không đổi ID tùy tiện để né kiểm tra. Lời giải bài 6 chỉ validate dữ liệu redrive, chưa gửi lên Kafka hay cung cấp CLI redrive.

Stop-on-error giữ luồng để operator can thiệp, giảm tính sẵn sàng; DLT cho record sau đi tiếp nhưng có thể phá workflow. Retry topic giảm head-of-line blocking nhưng không giữ order ban đầu. Dùng DLT cho event độc lập hoặc có quy trình bù rõ ràng; đừng bắt mọi Throwable để biến infrastructure failure thành poison.

### 3.7. Backpressure và thời gian poll

Kéo nhiều hơn tốc độ sink gây queue/RAM tăng, cuối cùng mất khả năng poll đúng hạn. Heartbeat nền không thay được yêu cầu `max.poll.interval.ms`.

P4 xử lý đồng bộ mỗi lần một record: sink chậm thì poll tiếp theo cũng chậm. Đây là backpressure tự nhiên của vòng lặp. Client vẫn có bộ đệm fetch riêng, nên max poll 1 chưa giới hạn toàn bộ RAM.

Hãy cộng thời gian của mọi bước giữa hai poll: tối đa ba lần prepare, nghỉ 10/20 ms, DLT send chờ future tối đa 15 giây và commit API timeout 10 giây. Max poll interval được đặt 120 giây. Các con số cấu hình này chưa chứng minh luôn đủ budget: adapter và file write vẫn cần thời gian thực, producer còn có pha chờ metadata.

Snapshot có byte limit 1 MiB nhưng không có timeout ghi cứng. Mỗi lần lại encode và ghi O(state); ngay khi chưa chạm byte limit, latency vẫn có thể quá lớn. Số đo ở Ch29 là lý do cần quan sát state bytes và duration, thay vì chỉ tăng max poll interval.

Nếu thêm worker pool, owner thread vẫn giữ consumer. Queue phải có giới hạn; khi đầy, owner pause partition nhưng tiếp tục poll để duy trì group, rồi resume khi đủ chỗ. Kết quả task phải quay về owner để tiến frontier từng partition. Không chia sẻ KafkaConsumer cho các worker.

`Policy.withinBudget` và bài 4 kiểm phép tính nhận việc: `batch * worstRecord + margin < maxPoll`. Hàm dùng BigInt để không tràn số. Nó chỉ kiểm giả định worstRecord mà bạn cung cấp; đo latency thực vẫn là việc cần làm trước khi tăng tải.

Xử lý đồng bộ dễ đúng nhưng throughput thấp và chặn cả luồng khi một record chậm. Worker pool phức tạp hơn về ownership/ordering/frontier. Engine quản lý scheduling/state nhưng vẫn cần backpressure và sink phù hợp. Chọn cách đơn giản khi tải nhỏ; đo latency, state bytes, lag growth trước khi tăng queue/threads.

### 3.8. Vận hành và shutdown

Restart liên tục không sửa được broker outage; lag 0 không chứng minh không mất event; lỗi DLT có thể dừng cả consumer.

| Tín hiệu cần đọc | Ý nghĩa và phản ứng |
|---|---|
| Committed lag theo partition | Dùng end-committed; unknown khi chưa commit; tăng lâu → so input rate với sink rate |
| Counts theo kind + DLT rate | Counts local bền; invalid/late tăng → so schema/timestamp; DLT publish có thể lặp |
| Retry attempts, processing/commit duration | Budget poll có bị vượt không; chưa có histogram/endpoint trong P4 |
| State bytes, số seen/receipt/bucket | Snapshot không cleanup, dừng tại byte limit; chuyển storage trước khi vượt latency/disk/RAM budget |
| Rebalance, commit/publish failures | Dùng log infrastructure, dừng/replay; chưa có metric counter riêng trong P4 |
| Readiness/liveness | Production: probe dependency ghi/broker có deadline / process sống; P4 chỉ console, chưa có probe endpoint |

P4 `Using` đóng consumer/producer/store khi return hoặc exception. Shutdown hook chỉ đặt stop flag + `wakeup`, owner xử lý cleanup; hook chờ owner tối đa 20 giây. Không commit vị trí chưa xử lý. Nếu dừng giữa snapshot/DLT/commit thì restart dựa vào receipt. Hard kill không gọi hook/close; OS thả lock khi process chết, như test đã chạy.

So với `Resource` trong P2, đây là adapter đồng bộ và không có HTTP server. Graceful shutdown bằng SIGTERM/container **chưa kiểm chứng**; không lấy test hard kill làm bằng chứng chạy finalizer. Production cần termination grace lớn hơn thời gian drain/close, giới hạn retry/deadline, readiness false khi drain, rồi mới force-kill khi hết hạn. Lỗi thường gặp là để shutdown hook commit tất cả position vừa poll hoặc gọi consumer API từ thread khác ngoài `wakeup`.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Delivery / effect | Record được giao / tác động nghiệp vụ được áp dụng |
| At-most / at-least / exactly-once | Nhiều nhất / ít nhất / đúng một lần trong phạm vi đã định nghĩa |
| Commit frontier | Vị trí sau prefix record đã hoàn tất của một partition |
| Replay / backfill / redrive | Đọc lại / tính bổ sung lịch sử / đưa lại record đã cách ly |
| Fencing | Ngăn writer/producer cũ tiếp tục ghi sau khi mất quyền |
| Head-of-line blocking | Một record chậm chặn các record phía sau |
| Backpressure | Tốc độ xử lý phía sau kìm tốc độ nhận phía trước |

## 5. Ví dụ đơn giản

Giả sử poll đã đọc các offset 0, 2 và 5. Kafka cho phép lỗ trong dãy offset. Task 5 xong sớm cũng chưa cho phép bỏ qua task 2. Ta cần prefix hoàn tất: đoạn đầu liên tục của **danh sách đã đọc**, không phải mọi số nguyên từ 0 tới 5.

`Policy.frontier` của lab tính vị trí an toàn để commit. Ví dụ chạy được:

```scala
package vn.scalaeng.reliability

object SimpleDemo:
  def main(args: Array[String]): Unit =
    println(Policy.frontier(Vector(0L -> true, 2L -> false, 5L -> true)))
    println(Policy.frontier(Vector(0L -> true, 2L -> true, 5L -> true)))
```

```powershell
scala-cli.bat run 10-streaming/reliability-lab --server=false --main-class vn.scalaeng.reliability.SimpleDemo
```

Output thật ở [simple30.txt](../14-projects/p4-kafka-event-processing/verification/simple30.txt):

```text
Some(1)
Some(6)
```

Dòng đầu chỉ cho commit sau record 0. Dòng sau cho commit sau record 5, vì mọi record trong prefix đều xong. P4 hiện xử lý tuần tự mỗi lần một record nên không cần scheduler này. Khi tăng parallelism, phải giữ frontier theo partition và xử lý rebalance, thay vì lấy max offset của task bất kỳ đã xong.

## 6. Ví dụ thực tế (BE và DE)

**BE:** event OrderPaid có ID ổn định; file sink atomically ghi total và seen. Crash sau ghi, trước commit → nhận lại nhưng không tăng total. Email/HTTP charge ngoài file vẫn cần idempotency key ở dịch vụ nhận; hàm `prepare` không phải chỗ đặt tác động ngoài không thể rollback.

**DE:** một query replay dữ liệu cũ có thể nhận late do watermark hiện tại. Recovery dùng receipt cũ; tính lại lịch sử có policy mới phải dùng output mới. Giống Ch24, rerun safety phải nói rõ append/overwrite hay idempotent upsert; consumer restart không phải phép tính lại toàn bộ báo cáo.

Thí nghiệm hai event × 100: mode before restart chỉ xử lý event thứ hai, total 100 nhưng mất event đầu; mode after restart vẫn ở event đầu, total 100 và event sau chưa xử lý. Do đó không thể chỉ so total mà kết luận hai mode tương đương: phải so receipt key và committed offset.

## 7. Code đầy đủ

- [Policy.scala + ErrorDemo](reliability-lab/src/main/scala/vn/scalaeng/reliability/Policy.scala), [PolicySuite](reliability-lab/src/test/scala/vn/scalaeng/reliability/PolicySuite.scala), [project.scala](reliability-lab/project.scala).
- [CrashWorker và Kafka adapter](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Kafka.scala), [BrokerSuite kill harness](../14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/BrokerSuite.scala).
- [Store](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Store.scala) và [CoreSuite](../14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/CoreSuite.scala) kiểm tra atomic snapshot và lỗi DLT/commit.

Từ repo root:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 10-streaming/reliability-lab --server=false
scala-cli.bat test solutions/ch30-code --server=false
scala-cli.bat run 10-streaming/reliability-lab --server=false --main-class vn.scalaeng.reliability.ErrorDemo
```

Để giữ cache/temp/log đúng môi trường đã dùng:

```powershell
python 14-projects/p4-kafka-event-processing/verification/run_checks.py ch30 sol30 errors broker
```

Task `broker` đặt `RUN_KAFKA_IT=1` chỉ cho subprocess Scala CLI; khi không bật, BrokerSuite `assume` bỏ qua. Đã opt-in mà lỗi socket/khởi động/child thì test fail, không tự skip che lỗi. Không cần Docker daemon. sbt chạy từ P4, không từ lab policy; Claude sẽ kiểm chứng.

## 8. Giải thích code

### 8.1. Commit vị trí kế tiếp

Đây là adapter thật được CLI và CrashWorker gọi:

```scala
  def commit(c: KafkaConsumer[String, String], r: ConsumerRecord[String, String]): Unit =
    c.commitSync(Map(new TopicPartition(r.topic(), r.partition()) ->
      new OffsetAndMetadata(r.offset() + 1, r.leaderEpoch(), "durable receipt")).asJava)
```

`commitSync` đợi broker phản hồi. Map chỉ chứa partition của record vừa hoàn tất; nó không lấy position của cả batch. Offset cộng 1 có nghĩa “lần sau bắt đầu sau record này”. `leaderEpoch()` được giữ lại trong metadata. Nếu commit thất bại, caller để lỗi truyền lên; receipt bền giúp owner mới replay an toàn.

### 8.2. Đặt marker đúng phía của biên crash

```scala
object CrashWorker:
  def main(args: Array[String]): Unit =
    require(args.length == 7, "bootstrap topic group storeDir marker mode dlt")
    Using.resources(new Store(Path.of(args(3))), Kafka.producer(args(0)), Kafka.consumer(args(0), args(2))) { (store, p, c) =>
      c.assign(java.util.List.of(new TopicPartition(args(1), 0)))
      val r = Kafka.one(c)
      val in = Kafka.input(r)
      args(5) match
        case "before" => Kafka.commit(c, r)
        case "after" => store.process(in)
        case "restart" =>
          Pipeline.handle(store, in, dead => { Kafka.send(p, args(6), dead.input.id, Json.receipt(dead)); () },
            () => Kafka.commit(c, r))
        case _ => throw new IllegalArgumentException("unknown crash mode")
      if args(5) != "restart" then
        Files.writeString(Path.of(args(4)), s"${r.offset()}")
        while true do Thread.sleep(1000) // Parent đợi marker rồi buộc process dừng.
      else println(s"restart offset=${r.offset()} total=${store.state.buckets.map(_.total).sum}")
    }
```

Mode before chỉ commit, không gọi process. Mode after chỉ process, không commit. Cả hai mới ghi marker rồi chờ parent kill. Mode restart tạo Store mới đọc disk và chạy Pipeline.handle bình thường: lưu/lấy receipt, publish DLT nếu cần, sau cùng commit.

Ở mode after, receipt nguồn offset 0 đã bền trước marker. Process mới đọc lại record 0 và lấy receipt ấy, nên không cộng lại. Ở mode before, broker đã ghi commit 1 nhưng state còn rỗng. Process mới đọc record 1; không có phép replay thông thường nào tự quay lại record 0.

Parent dùng `destroyForcibly()` rồi `waitFor` để xác nhận child đã chết. Consumer độc lập kiểm committed offset trên broker; Store mới kiểm receipt identity và total trên disk. Chỉ so total 100 của hai mode sẽ bỏ qua khác biệt quan trọng này.

### 8.3. Nhánh DLT cũng cần chờ ack

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

Snapshot receipt lỗi đã bền khi publish bắt đầu. Publish lỗi thì dòng commit chưa chạy. Commit lỗi sau publish lại có thể khiến replay phát DLT lần nữa. BrokerSuite cố ý không tạo DLT topic và tắt auto-create để chạy timeout thật; CoreSuite chèn lỗi commit để kiểm envelope replay bằng nhau.

`Policy.frontier` và `withinBudget` là helper thuần, chưa nối vào worker pool. Các lời giải bổ sung backoff, sequence, lag và redrive, cũng chưa là một scheduler đã vận hành. Cách tách này giúp kiểm quy tắc trước khi thêm đồng thời và vòng đời phức tạp.

## 9. Output mong đợi

Thí nghiệm KRaft thực tế ở [broker.txt](../14-projects/p4-kafka-event-processing/verification/broker.txt) có hai dòng sau (Windows trả childExit=1 khi forced termination trong lần chạy này):

```text
kill=before childExit=1 restartOffset=1 total=100 receipts=1 committed=2
kill=after childExit=1 restartOffset=0 total=100 receipts=1 committed=1
```

Mode before mất offset 0; mode after chưa xử lý offset 1. Parent kiểm tra receipt identity tương ứng, không chỉ tổng. Kết quả test và version được tổng hợp trong [handoff](../.team/handoffs/T15-ch29-30-p4.md); [ch30.txt](../14-projects/p4-kafka-event-processing/verification/ch30.txt), [sol30.txt](../14-projects/p4-kafka-event-processing/verification/sol30.txt) chứa output runner thật. Lượt mặc định P4 skip ba integration test; lượt broker opt-in chạy cả ba, không đếm skip là pass.

## 10. Lỗi thường gặp

Chạy `ErrorDemo` qua task `errors`; các dòng dưới được lấy từ [errors.txt](../14-projects/p4-kafka-event-processing/verification/errors.txt). Mỗi lỗi có nguyên nhân và cách sửa cụ thể.

**1. Retry không có lần thử hoặc vượt giới hạn.** Retry.run nhận 0:

```text
java.lang.IllegalArgumentException: requirement failed: attempts must be in 1..10
```

Chọn budget 1..10. Tăng budget còn tăng thời gian giữa hai lần poll; không dùng retry vô hạn để giữ consumer “sống”.

**2. Frontier nhận offset giảm hoặc trùng.** Probe đưa 2 rồi 1:

```text
java.lang.IllegalArgumentException: requirement failed: offsets must increase
```

Giữ thứ tự record đã đọc riêng cho từng partition. Task hoàn tất lệch thứ tự chỉ đổi cờ Boolean, không đảo danh sách offset.

**3. Cấu hình poll budget không hợp lệ.** Probe đặt records bằng 0:

```text
java.lang.IllegalArgumentException: requirement failed: invalid poll budget
```

Kiểm số record, worst duration, margin và max poll interval. Hàm trả false khi cấu hình hợp lệ nhưng thiếu budget; exception này dành cho tham số sai.

**4. Mở lại snapshot với policy khác.** Probe đổi delay:

```text
java.lang.IllegalArgumentException: requirement failed: snapshot policy mismatch
```

Recovery giữ format/width/delay/skew cũ. Backfill với policy mới cần store/group/output riêng và đối soát, không sửa JSON đang dùng.

**5. Hai writer dùng chung store.** Probe mở hai Store trong cùng JVM:

```text
java.lang.IllegalStateException: store already owned
```

Một OS lock chỉ bảo vệ một filesystem, chưa là fencing giữa máy. Khi scale worker cần thiết kế ownership/state mới.

**6. Publish DLT hỏng.** ErrorDemo chèn IOException có tên rõ ràng để kiểm thứ tự; đây là fault injection, chưa phải broker outage:

```text
java.io.IOException: injected DLT failure
```

BrokerSuite còn chạy lỗi Kafka thật: topic missing-dead không tồn tại và broker tắt auto-create. Output từ [broker.txt](../14-projects/p4-kafka-event-processing/verification/broker.txt):

```text
DLT publish: java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Topic missing-dead not present in metadata after 10000 ms.
DLT timeout receipts=1 committed=None
```

`send(...).get(...)` bọc lỗi Kafka trong ExecutionException; cause là TimeoutException. Giữ group/store, tạo đúng DLT hoặc phục hồi broker/quyền rồi restart. Receipt đã bền, source chưa commit. Không catch publish failure rồi tự commit để bỏ qua.

**7. Total bằng nhau chưa chứng minh không mất event.** Đây là lỗi thiết kế không ném exception. Thí nghiệm kill ở mục 9 cho hai total 100 nhưng receipt và committed offset khác. Tương tự, DLT lặp sau commit failure là hành vi có chủ đích; dùng provenance để dedup. Không gán một thông báo Kafka giả cho các hành vi ấy.

## 11. Cách debug

1. Xác định biên crash: trước snapshot, sau snapshot trước DLT, sau DLT trước commit hay sau commit. Kiểm tra receipt và committed độc lập.
2. Nếu event lặp, phân biệt cùng offset replay với offset mới resend; so fingerprint, không chỉ ID.
3. Khi mất record, so group/store/topic incarnation/retention; không reset offset trước khi lưu bằng chứng.
4. Khi xử lý đứng, so DLT publish error, disk latency, poll interval, queue occupancy và commit/rebalance; đừng tăng retry vô hạn.
5. Test kill thất bại trước marker: đọc `before.log`/`after.log` ở temp path được assertion chỉ ra; kiểm Java/classpath/socket. Không coi process chết sớm do lỗi classpath là crash thành công.
6. Khi replay tính lại khác, xác nhận policy version và input; recovery cùng source phải trả receipt, backfill cần store mới.

## 12. Bài tập (cơ bản → nâng cao)

1. **Frontier:** dùng helper của lab cho offsets có lỗ và task hoàn thành lệch thứ tự. Test empty, prefix dở và offset trùng/giảm. Vì sao map max offset đã xong là sai?
2. **Backoff:** trả `maxAttempts-1` khoảng nghỉ gấp đôi, có cap, không tràn Long; maxAttempts 1..10. Test không nghỉ sau lần cuối.
3. **Ordering:** nhận `(key, sequence)` và phát hiện sequence giảm theo từng key khi input xen kẽ. Cho phép duplicate sequence. Nêu vì sao duplicate sequence cùng payload khác vẫn cần fingerprint.
4. **Poll budget:** tính worst duration từ attempts × thời gian mỗi attempt + waits, rồi nhân số record và thêm margin. Dùng BigInt chống overflow, so chặt `< max.poll.interval`. Test biên thiếu budget và cấu hình lớn.
5. **Lag:** tổng end-committed của nhiều partition bằng BigInt. Thiếu committed ở một partition hoặc không có assignment thì unknown; committed lớn hơn end là lỗi cần điều tra, không clamp về 0.
6. **Redrive:** chỉ nhận receipt DLT, validate raw đã sửa, key khớp orderId; trả Event giữ ID nghiệp vụ. Test raw vẫn sai, key sai, receipt không phải DLT. Giải thích vì sao phải publish record mới và giữ audit provenance cũ.

## 13. Đáp án và lời giải

Xem [lời giải sáu bài](../solutions/ch30-solutions.md), [code](../solutions/ch30-code/src/main/scala/vn/scalaeng/solutions/ch30/Exercises.scala), [6 test](../solutions/ch30-code/src/test/scala/vn/scalaeng/solutions/ch30/ExercisesSuite.scala). Các helper không thay thế broker test. Ordering bài 3 không kiểm tra partition mapping, redrive bài 6 không tự publish Kafka.

## 14. Câu hỏi kiểm tra

1. Vì sao at-least-once vẫn cộng đúng một lần vào sink file? (Atomic dedup+effect và store còn nguyên.)
2. Vì sao transaction Kafka chưa bao trùm HTTP? (HTTP không tham gia Kafka transaction.)
3. Commit 6 có an toàn khi record 2 chưa xong, dù 5 đã xong? (Không, vượt frontier.)
4. `destroyForcibly` khác `close` thế nào trong bằng chứng? (Không chạy cleanup, mất RAM child.)
5. Khi nào `earliest` không cứu được dữ liệu? (Retention đã xóa hoặc đã commit trước effect.)
6. DLT policy thay đổi ordering ra sao? (Record sau có thể vượt event bị cách ly.)
7. Tăng max poll interval có giải quyết snapshot phình mãi không? (Chỉ nới budget tạm; cần giới hạn/chuyển storage.)

## 15. Mini project

Mở rộng P4 với một sink PostgreSQL: unique event ID + fingerprint, tổng và receipt trong một transaction DB; chèn lỗi giữa các statement để chứng minh rollback. Kafka commit vẫn sau transaction DB; kill JVM ở biên DB commit/Kafka commit phải replay không cộng lại. Chưa triển khai DB trong task này.

Tiêu chí: tách unit/DB opt-in, đọc DB từ connection mới sau restart, conflict không overwrite fingerprint, DLT provenance và receipt audit còn đủ. Nếu chọn Kafka transaction thay DB, phải có transactional producer, `sendOffsetsToTransaction`, downstream read_committed, test abort/crash thật và mô tả rõ output Kafka được bảo vệ; không dùng mock làm bằng chứng.

## 16. Nguồn tài liệu chính thức

Kiểm tra **17/10/2026**:

- [KafkaConsumer JavaDoc](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html): position, commit, assignment, poll, thread safety. [Kafka design: delivery](https://kafka.apache.org/42/design/design/) và [producer configs](https://kafka.apache.org/42/configuration/producer-configs/). Đây là tài liệu 4.2; artifact 4.4.0 được xác nhận POM và hành vi đã dùng bằng test, không coi mọi default hai bản là giống nhau.
- [KafkaProducer JavaDoc](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html): giao thức transaction và group offsets, chỉ trình bày khái niệm trong task này.
- [Process.destroyForcibly JDK 21](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Process.html#destroyForcibly()), [Runtime shutdown hooks](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Runtime.html#addShutdownHook(java.lang.Thread)): forced termination/shutdown khác nhau; parent còn phải waitFor.
- [embedded-kafka](https://github.com/embeddedkafka/embedded-kafka), [POM Scala 3 4.4.0](https://repo1.maven.org/maven2/io/github/embeddedkafka/embedded-kafka_3/4.4.0/embedded-kafka_3-4.4.0.pom).
- [9 POM release, ngày và SHA-256](../14-projects/p4-kafka-event-processing/verification/versions.json), [script kiểm tra](../14-projects/p4-kafka-event-processing/verification/verify_sources.py), [script chạy test](../14-projects/p4-kafka-event-processing/verification/run_checks.py).

## 17. Checklist tự đánh giá

- [ ] Nêu delivery/effect, failure model và phạm vi trước khi dùng “exactly-once”.
- [ ] Chạy kill process thật; kiểm receipt identity, sink total và committed offset riêng.
- [ ] Phân biệt position/frontier/committed; không commit vượt task chưa xong.
- [ ] Replay với cùng store/policy; backfill có kế hoạch output/đối soát riêng.
- [ ] Giải thích key/partition order, DLT/retry topic và sequence nghiệp vụ.
- [ ] Có retry budget, poll budget, queue/state budget và tín hiệu vận hành.
- [ ] Chạy 3 test lab + 6 test lời giải; không đếm broker skip là pass.
- [ ] Ghi rõ Kafka transaction, SIGTERM/container, power loss và broker failover chưa kiểm chứng.
