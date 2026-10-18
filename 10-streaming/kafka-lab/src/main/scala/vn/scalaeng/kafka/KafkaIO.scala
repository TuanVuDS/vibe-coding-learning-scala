package vn.scalaeng.kafka

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import scala.jdk.CollectionConverters.*

object Clients:
  def producerProperties(bootstrap: String): Properties =
    val p = new Properties()
    Map("bootstrap.servers" -> bootstrap, "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer", "acks" -> "all",
      "enable.idempotence" -> "true", "retries" -> "5", "max.in.flight.requests.per.connection" -> "5",
      "delivery.timeout.ms" -> "10000", "request.timeout.ms" -> "3000", "max.block.ms" -> "10000")
      .foreach((k, v) => p.setProperty(k, v))
    p

  def producer(bootstrap: String): KafkaProducer[String, String] = new KafkaProducer(producerProperties(bootstrap))

  def consumer(bootstrap: String, group: String): KafkaConsumer[String, String] =
    val p = new Properties()
    Map("bootstrap.servers" -> bootstrap, "group.id" -> group, "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest", "max.poll.records" -> "1", "session.timeout.ms" -> "6000",
      "heartbeat.interval.ms" -> "1000", "group.protocol" -> "classic",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
      .foreach((k, v) => p.setProperty(k, v))
    new KafkaConsumer[String, String](p)

  def send(p: KafkaProducer[String, String], topic: String, key: String, raw: String,
      partition: Int = -1): RecordMetadata =
    val record = if partition < 0 then new ProducerRecord[String, String](topic, key, raw)
      else new ProducerRecord[String, String](topic, Integer.valueOf(partition), key, raw)
    p.send(record).get(15, TimeUnit.SECONDS)

  def commit(c: KafkaConsumer[String, String], record: ConsumerRecord[String, String]): Unit =
    c.commitSync(Map(new TopicPartition(record.topic(), record.partition()) ->
      new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), "processed")).asJava)

  def lag(end: Long, committed: Option[Long]): Option[BigInt] =
    committed.map { offset =>
      require(end >= offset && offset >= 0, "invalid offset range")
      BigInt(end) - offset
    }

object Processor:
  /** DLT publish must be acknowledged before commit. Unexpected infrastructure failures propagate. */
  def handle(record: ConsumerRecord[String, String], applyEvent: OrderEvent => Unit,
      publishDead: DeadLetter => Unit, commit: () => Unit,
      maxAttempts: Int = 3, pause: Int => Unit = n => Thread.sleep(n.toLong * 10)): String =
    val outcome = Protocol.decode(record.value()) match
      case Left(errors) => Left((errors, 0))
      case Right(event) => Retry.run(maxAttempts, pause)(applyEvent(event))
        .left.map { (e, n) => (Vector(e.getMessage), n) }
    val status = outcome match
      case Right(_) => "processed"
      case Left((errors, attempts)) =>
        publishDead(DeadLetter(record.topic(), record.partition(), record.offset(), Option(record.key()),
          Option(record.value()), errors, attempts))
        "dead-letter"
    commit()
    status

object ConsumerMain:
  /** Bounded teaching executable: args bootstrap topic dlt group maxRecords; normal/error cleanup via Using. */
  def main(args: Array[String]): Unit =
    require(args.length == 5, "usage: bootstrap topic dlt group maxRecords")
    val maxRecords = args(4).toInt
    require(maxRecords > 0, "maxRecords must be positive")
    val ledger = new MemoryLedger
    scala.util.Using.resources(Clients.producer(args(0)), Clients.consumer(args(0), args(3))) { (p, c) =>
      c.subscribe(java.util.List.of(args(1)))
      var count = 0
      val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
      while count < maxRecords && System.nanoTime() < deadline do
        c.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
          val status = Processor.handle(record, e => { ledger(e); () },
            d => { Clients.send(p, args(2), s"${d.topic}:${d.partition}:${d.offset}", d.json); () },
            () => Clients.commit(c, record))
          println(s"partition=${record.partition()} offset=${record.offset()} $status")
          count += 1
        }
      println(s"handled=$count totals=${ledger.totals}")
    }

object ProducerMain:
  def main(args: Array[String]): Unit =
    require(args.length == 2, "usage: bootstrap topic")
    scala.util.Using(Clients.producer(args(0))) { p =>
      val e = OrderEvent(1, "event-1", "ORDER-1", 100)
      val m = Clients.send(p, args(1), e.orderId, Protocol.encode(e))
      println(s"partition=${m.partition()} offset=${m.offset()}")
    }.get
