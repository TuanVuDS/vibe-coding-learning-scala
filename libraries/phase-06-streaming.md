# Thư viện — Phase 6: Kafka và streaming

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| Kafka clients | 4.4.0 | Java | Producer/consumer/admin | Ch28–30, P4 |
| embedded-kafka | 4.4.0 | `_3` | Broker KRaft thật trong test | Ch28, P4 |
| Kafka broker | 4.4.0 trong test | Ngoài JVM app | Log phân vùng, group/offset | Ch28, P4 |
| Circe generic | 0.14.17 | `_3` | Derive codec event/state | Ch29–30, P4 |
| Circe parser | 0.14.17 | `_3` | Parse JSON | Ch28–30, P4 |
| slf4j-nop | 2.0.20 | Java | Provider im lặng cho lab | Ch28–30, P4 |
| fs2 core | 3.13.0 | `_3` | Minh họa stream trong JVM | Ch27 |
| Cats Effect / munit-cats-effect | 3.7.1 / 2.2.0 | `_3` | Runtime/test Ch27 | Ch27 |
| munit | 1.2.4 | `_3` | Unit/integration test | Ch27–30, P4 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

Dependency dùng lại: **fs2 core 3.13.0** — [phase 5](phase-05-data-spark.md#fs2); **Cats Effect 3.7.1** và **munit-cats-effect 2.2.0** — [phase 4](phase-04-backend.md#cats-effect); **Circe parser 0.14.17** — [phase 4](phase-04-backend.md#circe); **slf4j-nop 2.0.20** — [hai phiên bản ở phase 5](phase-05-data-spark.md#slf4j-nop); **munit 1.2.4** — [tooling](tooling.md#munit). Không có fs2-kafka, Spark Structured Streaming hay Flink trong build các lab này; chúng chỉ được so sánh về mô hình/semantics trong chương.

Ch29/30 có directive nhưng không có build.sbt riêng; lab dùng nguồn P4 qua cấu hình file. Không vì thế bỏ chúng khỏi bảng đối chiếu. Bằng chứng chạy: [T13/Ch27–28](../.team/handoffs/T13-ch27-28-streaming-kafka.md), [T15/Ch29–30/P4](../.team/handoffs/T15-ch29-30-p4.md), [review T15](../.team/reviews/T15-ch29-30-p4-review.md). Ch28/P4 có broker thật và P4 có thử kill JVM thật theo báo cáo; T27 không chạy lại broker hay kill process.

<a id="kafka-clients"></a>
## Apache Kafka clients

### 1. Là gì / giải quyết vấn đề gì

Kafka clients là thư viện Java cung cấp producer, consumer và admin để ứng dụng giao tiếp broker. Nó giúp pipeline lưu/đọc sự kiện theo partition nhưng không tự làm tác động lên sink trở thành exactly-once; ứng dụng vẫn phải quyết định commit, retry và dedup.

### 2. Phiên bản trong giáo trình

- `org.apache.kafka:kafka-clients:4.4.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.pom); HTTP 200, 18/10/2026.

Java API, không hậu tố Scala; giáo trình gọi trực tiếp từ Scala 3.9.0/JDK 21. Build dùng **4.4.0**, không thay bằng bản 4.2.x còn ghi trong một dòng matrix cũ. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`KafkaProducer`, `ProducerRecord`, `KafkaConsumer`, `poll`, `commitSync`, `TopicPartition` và `OffsetAndMetadata` là API chính. Offset commit là vị trí **kế tiếp** sẽ đọc; producer idempotence bảo vệ gửi lặp trong phạm vi giao thức, không thay dedup nghiệp vụ. KafkaConsumer phải tuân thủ mô hình thread sử dụng của client; logic xử lý lâu có thể vượt poll budget.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [10-streaming/kafka-lab/src/main/scala/vn/scalaeng/kafka/KafkaIO.scala:12](../10-streaming/kafka-lab/src/main/scala/vn/scalaeng/kafka/KafkaIO.scala#L12). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 10-streaming/kafka-lab/src/main/scala/vn/scalaeng/kafka/KafkaIO.scala:12-19 -->
```scala
  def producerProperties(bootstrap: String): Properties =
    val p = new Properties()
    Map("bootstrap.servers" -> bootstrap, "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer", "acks" -> "all",
      "enable.idempotence" -> "true", "retries" -> "5", "max.in.flight.requests.per.connection" -> "5",
      "delivery.timeout.ms" -> "10000", "request.timeout.ms" -> "3000", "max.block.ms" -> "10000")
      .foreach((k, v) => p.setProperty(k, v))
    p
```

### 5. Lựa chọn thay thế

fs2-kafka hợp khi chọn stream effect xuyên suốt; client Java trực tiếp làm rõ poll/commit và ranh giới guarantee. Streaming engine quản state/checkpoint theo mô hình riêng, phù hợp bài toán lớn hơn nhưng cần học semantics của engine trước khi thay consumer.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “org.apache.kafka.common.config.ConfigException: Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.” — [10-streaming/ch28-kafka-scala.md:252](../10-streaming/ch28-kafka-scala.md#L252). Giữ acks=all và sửa cấu hình mâu thuẫn; không tự tắt idempotence để né lỗi.

### 7. Dùng ở đâu trong giáo trình

[10-streaming/ch28-kafka-scala.md](../10-streaming/ch28-kafka-scala.md); [10-streaming/ch29-streaming-pipeline.md](../10-streaming/ch29-streaming-pipeline.md); [10-streaming/ch30-streaming-reliability.md](../10-streaming/ch30-streaming-reliability.md); [14-projects/p4-kafka-event-processing](../14-projects/p4-kafka-event-processing); P5 dùng lại.

### 8. Tài liệu chính thức

[Kafka documentation](https://kafka.apache.org/documentation/), [Kafka client source](https://github.com/apache/kafka/tree/trunk/clients). Tọa độ phiên bản 4.4.0 được kiểm riêng bằng POM ở mục 2; mã trên nhánh trunk có thể đã thay đổi.

<a id="embedded-kafka"></a>
## embedded-kafka

### 1. Là gì / giải quyết vấn đề gì

embedded-kafka khởi động Kafka broker thật trong JVM test để kiểm partition, rebalance, offset và replay. Nó giảm công cài cluster/Docker cho integration test nhưng vẫn mở cổng và tạo log storage; không phải mock consumer.

### 2. Phiên bản trong giáo trình

- `io.github.embeddedkafka:embedded-kafka_3:4.4.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/github/embeddedkafka/embedded-kafka_3/4.4.0/embedded-kafka_3-4.4.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`EmbeddedKafkaConfig`, cổng broker/controller ngẫu nhiên và `withRunningKafkaOnFoundPort` tạo môi trường KRaft có lifecycle gắn với test. `RUN_KAFKA_IT=1` là opt-in của repo, không phải option của thư viện. Một broker replication factor 1 không chứng minh tính sẵn sàng khi nhiều broker mất kết nối.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala:57](../10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala#L57). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala:57-67 -->
```scala
  test("KRaft: partitions, group rebalance, lag, crash restart, retries and DLT") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "set RUN_KAFKA_IT=1 for embedded KRaft")
    val config = EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0,
      customBrokerProperties = Map("group.initial.rebalance.delay.ms" -> "0",
        "offsets.topic.replication.factor" -> "1", "offsets.topic.num.partitions" -> "1",
        "transaction.state.log.replication.factor" -> "1", "transaction.state.log.min.isr" -> "1"))
    EmbeddedKafka.withRunningKafkaOnFoundPort(config) { actual =>
      val bootstrap = s"localhost:${actual.kafkaPort}"
      val topics = Vector(new NewTopic("orders", 2, 1.toShort), new NewTopic("crash", 1, 1.toShort),
        new NewTopic("input", 1, 1.toShort), new NewTopic("dead", 1, 1.toShort),
        new NewTopic("cli", 1, 1.toShort), new NewTopic("cli-dead", 1, 1.toShort))
```

### 5. Lựa chọn thay thế

Broker Docker hoặc cluster riêng phù hợp kiểm deployment/network gần production; embedded broker thích hợp test protocol lặp lại ở máy dev/CI. Fake producer/consumer chỉ phù hợp unit test đường xử lý, không xác nhận rebalance/restart.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Commit trước effect: test restart trả offset 1, record 0 chưa tạo tác động.” — [10-streaming/ch28-kafka-scala.md:276](../10-streaming/ch28-kafka-scala.md#L276). Đây là phát hiện qua test broker của lab, không phải exception embedded-kafka. Bật integration test chỉ khi môi trường cho phép; skip không là pass.

### 7. Dùng ở đâu trong giáo trình

[10-streaming/ch28-kafka-scala.md](../10-streaming/ch28-kafka-scala.md); [14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/BrokerSuite.scala](../14-projects/p4-kafka-event-processing/src/test/scala/vn/scalaeng/p4/BrokerSuite.scala); [14-projects/p5-data-platform/ingest/src/test/scala/vn/scalaeng/p5/KafkaLandingSuite.scala](../14-projects/p5-data-platform/ingest/src/test/scala/vn/scalaeng/p5/KafkaLandingSuite.scala)

### 8. Tài liệu chính thức

[embedded-kafka](https://github.com/embeddedkafka/embedded-kafka).

<a id="kafka-broker"></a>
## Kafka broker (dịch vụ ngoài ứng dụng)

### 1. Là gì / giải quyết vấn đề gì

Kafka broker lưu append-only log phân vùng và phục vụ các client theo topic/partition. Broker là dịch vụ lưu sự kiện, khác thư viện kafka-clients trong app; version broker và client cần ghi riêng kể cả khi bài kiểm chọn cùng số.

### 2. Phiên bản trong giáo trình

**4.4.0** trong bộ kiểm tích hợp của giáo trình: [POM embedded-kafka](https://repo.maven.apache.org/maven2/io/github/embeddedkafka/embedded-kafka_3/4.4.0/embedded-kafka_3-4.4.0.pom) và [POM broker Kafka](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.13/4.4.0/kafka_2.13-4.4.0.pom), ngày đối chiếu 18/10/2026. Broker phía JVM dùng Scala 2.13 trong bản phân phối; app Scala 3 giao tiếp bằng protocol qua client Java, không nhét broker `_2.13` vào code ứng dụng để suy hỗ trợ Spark/Scala 3. POM broker là dependency bắc cầu của test, không khai báo trực tiếp mới.

### 3. Khái niệm cốt lõi

Topic/partition, replication, consumer group/committed offset, retention và KRaft controller là các thành phần chính. Test một broker không mô phỏng mất quorum; retention giới hạn khả năng replay. Exactly-once của transaction Kafka có phạm vi, không tự bao cả file snapshot/PostgreSQL bên ngoài.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala:59](../10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala#L59). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 10-streaming/kafka-lab/src/test/scala/vn/scalaeng/kafka/BrokerSuite.scala:59-68 -->
```scala
    val config = EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0,
      customBrokerProperties = Map("group.initial.rebalance.delay.ms" -> "0",
        "offsets.topic.replication.factor" -> "1", "offsets.topic.num.partitions" -> "1",
        "transaction.state.log.replication.factor" -> "1", "transaction.state.log.min.isr" -> "1"))
    EmbeddedKafka.withRunningKafkaOnFoundPort(config) { actual =>
      val bootstrap = s"localhost:${actual.kafkaPort}"
      val topics = Vector(new NewTopic("orders", 2, 1.toShort), new NewTopic("crash", 1, 1.toShort),
        new NewTopic("input", 1, 1.toShort), new NewTopic("dead", 1, 1.toShort),
        new NewTopic("cli", 1, 1.toShort), new NewTopic("cli-dead", 1, 1.toShort))
      Using.resource(Admin.create(Map[String, Object]("bootstrap.servers" -> bootstrap).asJava)) { admin =>
```

### 5. Lựa chọn thay thế

Dịch vụ Kafka managed giảm vận hành broker nhưng vẫn cần quản quota/schema/retention. Queue đơn giản phù hợp xử lý không cần replay log; không suy semantics Kafka sang mọi message broker. Docker/embedded là cách triển khai, không phải loại broker khác.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Dùng ledger RAM và tuyên bố chịu được JVM restart: chưa có persistence.” — [10-streaming/ch28-kafka-scala.md:280](../10-streaming/ch28-kafka-scala.md#L280). Broker còn record không có nghĩa sink đã bền hoặc effect không lặp; P4 bổ sung receipt/snapshot và thử recovery riêng.

### 7. Dùng ở đâu trong giáo trình

[10-streaming/ch28-kafka-scala.md](../10-streaming/ch28-kafka-scala.md); [14-projects/p4-kafka-event-processing/README.md](../14-projects/p4-kafka-event-processing/README.md); P5.

### 8. Tài liệu chính thức

[Kafka architecture/documentation](https://kafka.apache.org/documentation/), [downloads/releases](https://kafka.apache.org/downloads/).

<a id="circe-generic"></a>
## Circe generic

### 1. Là gì / giải quyết vấn đề gì

Circe generic sinh Encoder/Decoder cho kiểu product như case class để không phải viết mapping từng trường. Pipeline cần lưu event, receipt và toàn bộ snapshot nhất quán; derive giúp giữ mapping đồng bộ với kiểu Scala nhưng không tự định nghĩa migration định dạng.

### 2. Phiên bản trong giáo trình

- `io.circe:circe-generic_3:0.14.17` — [POM Maven Central](https://repo.maven.apache.org/maven2/io/circe/circe-generic_3/0.14.17/circe-generic_3-0.14.17.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`deriveEncoder`, `deriveDecoder`, given và `asJson` nối kiểu Scala với codec. Semiauto tạo instance tường minh để kiểm soát phạm vi; parse JSON và validate nghiệp vụ là bước khác. Snapshot có field format/policy để từ chối cấu hình hoặc dữ liệu không tương thích.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala:19](../14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala#L19). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p4-kafka-event-processing/src/main/scala/vn/scalaeng/p4/Model.scala:19-29 -->
```scala
object Json:
  given Encoder[Event] = deriveEncoder
  given Decoder[Event] = deriveDecoder
  given Encoder[Input] = deriveEncoder
  given Decoder[Input] = deriveDecoder
  given Encoder[Receipt] = deriveEncoder
  given Decoder[Receipt] = deriveDecoder
  given Encoder[Bucket] = deriveEncoder
  given Decoder[Bucket] = deriveDecoder
  given Encoder[State] = deriveEncoder
  given Decoder[State] = deriveDecoder
```

### 5. Lựa chọn thay thế

Codec thủ công tốt khi wire format khác domain hoặc cần tương thích lâu dài; derive hợp mô hình nội bộ được version hóa rõ. `derives Codec.AsObject` dùng ở phase 4, còn module generic là dependency trực tiếp bổ sung ở phase 6.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “duplicate JSON fields: Right(Event(1,e1,ORDER,100,5))” — [10-streaming/ch29-streaming-pipeline.md:359](../10-streaming/ch29-streaming-pipeline.md#L359). Parser lấy giá trị cuối trong probe của P4; derive codec và kiểm tập field không phát hiện duplicate key. Đây là giới hạn đã ghi, không nhận là đã sửa trong T27.

### 7. Dùng ở đâu trong giáo trình

[10-streaming/ch29-streaming-pipeline.md](../10-streaming/ch29-streaming-pipeline.md); [10-streaming/ch30-streaming-reliability.md](../10-streaming/ch30-streaming-reliability.md); [14-projects/p4-kafka-event-processing](../14-projects/p4-kafka-event-processing); Ch31/P5 dùng lại.

### 8. Tài liệu chính thức

[Circe semiauto derivation](https://circe.github.io/circe/codecs/semiauto-derivation.html), [Circe parsing](https://circe.github.io/circe/parsing.html).

