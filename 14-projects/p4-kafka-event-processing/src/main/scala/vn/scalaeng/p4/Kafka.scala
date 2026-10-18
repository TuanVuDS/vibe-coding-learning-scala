package vn.scalaeng.p4

import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import scala.jdk.CollectionConverters.*
import scala.util.Using

object Kafka:
  private def properties(values: Map[String, String]): Properties =
    val p = new Properties()
    values.foreach((k, v) => p.setProperty(k, v))
    p
  def producer(bootstrap: String): KafkaProducer[String, String] = new KafkaProducer(properties(Map(
    "bootstrap.servers" -> bootstrap, "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer", "acks" -> "all",
    "enable.idempotence" -> "true", "delivery.timeout.ms" -> "10000", "request.timeout.ms" -> "3000",
    "max.block.ms" -> "10000")))
  def consumer(bootstrap: String, group: String): KafkaConsumer[String, String] = new KafkaConsumer(properties(Map(
    "bootstrap.servers" -> bootstrap, "group.id" -> group, "enable.auto.commit" -> "false",
    "auto.offset.reset" -> "earliest", "max.poll.records" -> "1", "default.api.timeout.ms" -> "10000",
    "max.poll.interval.ms" -> "120000", "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")))
  def topics(bootstrap: String, names: Vector[String], partitions: Int = 1): Unit =
    Using.resource(Admin.create(Map[String, Object]("bootstrap.servers" -> bootstrap).asJava)) { a =>
      a.createTopics(names.map(n => new NewTopic(n, partitions, 1.toShort)).asJava).all().get(20, TimeUnit.SECONDS)
    }
  def send(p: KafkaProducer[String, String], topic: String, key: String, raw: String,
      partition: Int = -1): Long =
    val r = if partition < 0 then new ProducerRecord[String, String](topic, key, raw)
      else new ProducerRecord[String, String](topic, Integer.valueOf(partition), key, raw)
    p.send(r).get(15, TimeUnit.SECONDS).offset()
  def input(r: ConsumerRecord[String, String]): Input =
    Input(r.topic(), r.partition(), r.offset(), Option(r.key()), Option(r.value()))
  def commit(c: KafkaConsumer[String, String], r: ConsumerRecord[String, String]): Unit =
    c.commitSync(Map(new TopicPartition(r.topic(), r.partition()) ->
      new OffsetAndMetadata(r.offset() + 1, r.leaderEpoch(), "durable receipt")).asJava)
  def lag(c: KafkaConsumer[String, String], tp: TopicPartition): Option[BigInt] =
    val end = c.endOffsets(java.util.List.of(tp)).get(tp).longValue()
    Option(c.committed(java.util.Set.of(tp)).get(tp)).map { committed =>
      require(committed.offset() <= end, "committed exceeds end")
      BigInt(end) - committed.offset()
    }
  def one(c: KafkaConsumer[String, String]): ConsumerRecord[String, String] =
    val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
    @scala.annotation.tailrec
    def poll(): ConsumerRecord[String, String] =
      if System.nanoTime() >= deadline then throw new IllegalStateException("record deadline exceeded")
      val records = c.poll(Duration.ofMillis(100)).iterator().asScala
      if records.hasNext then records.next() else poll()
    poll()

object Fixture:
  /** Mỗi dòng key<TAB>raw; ký hiệu \\N biểu diễn null, giữ nguyên JSON hỏng để học DLT. */
  def read(path: Path): Vector[(String, String)] =
    Files.readAllLines(path).asScala.toVector.zipWithIndex.map { (line, index) =>
      val split = line.indexOf('\t')
      require(split >= 0, s"fixture line ${index + 1}: expected key<TAB>raw")
      def nullable(text: String): String = if text == "\\N" then null else text
      (nullable(line.take(split)), nullable(line.drop(split + 1)))
    }

  /** Chỉ bật bằng --demo-failures: lỗi giả lập adapter, không phải broker outage. */
  def demoPrepare(): Event => Unit =
    var retried = false
    event =>
      if event.eventId == "busy" then throw Transient("adapter busy")
      if event.eventId == "retry" && !retried then
        retried = true
        throw Transient("adapter busy")

object Main:
  def main(args: Array[String]): Unit =
    require(args.nonEmpty, "usage: init|produce|consume ...")
    args(0) match
      case "init" =>
        require(args.length == 4, "init bootstrap source dlt")
        Kafka.topics(args(1), Vector(args(2), args(3)))
      case "produce" =>
        require(args.length == 4, "produce bootstrap source fixture.jsonl")
        val records = Fixture.read(Path.of(args(3)))
        Using.resource(Kafka.producer(args(1))) { p =>
          records.zipWithIndex.foreach { case ((key, raw), index) =>
            val offset = Kafka.send(p, args(2), key, raw)
            println(s"producer line=${index + 1} topic=${args(2)} offset=$offset")
          }
        }
      case "consume" =>
        require(args.length == 7 || (args.length == 8 && args(7) == "--demo-failures"),
          "consume bootstrap source dlt group storeDir maxRecords [--demo-failures]")
        val limit = args(6).toInt
        require(limit > 0, "maxRecords must be positive")
        val prepare: Event => Unit = if args.length == 8 then Fixture.demoPrepare() else _ => ()
        Using.resources(new Store(Path.of(args(5))), Kafka.producer(args(1)), Kafka.consumer(args(1), args(4))) { (store, p, c) =>
          val running = new AtomicBoolean(true)
          val owner = Thread.currentThread()
          val hook = new Thread(() => {
            running.set(false)
            c.wakeup()
            owner.join(20000) // Đợi owner xử lý/đóng; hard kill vẫn phục hồi bằng replay.
          }, "p4-stop")
          Runtime.getRuntime.addShutdownHook(hook)
          try
            c.subscribe(java.util.List.of(args(2)))
            var count = 0
            val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
            while running.get() && count < limit && System.nanoTime() < deadline do
              c.poll(Duration.ofMillis(100)).iterator().asScala.foreach { r =>
                val result = Pipeline.handle(store, Kafka.input(r), dead => {
                  Kafka.send(p, args(3), dead.input.id, Json.receipt(dead)); ()
                }, () => Kafka.commit(c, r), prepare)
                // Hai lần hỏi broker mỗi record: cố ý giữ để quan sát lab nhỏ (mục 3.6 Ch29).
                val lag = Kafka.lag(c, new TopicPartition(r.topic(), r.partition()))
                println(s"source=${result.input.id} kind=${result.kind} lag=$lag counts=${store.state.counts.toVector.sorted.mkString(",")}")
                count += 1
              }
            println(s"handled=$count stopRequested=${!running.get()} buckets=${store.state.buckets}")
          catch case _: WakeupException if !running.get() => println("stopping: replay any uncommitted receipt")
          finally
            try Runtime.getRuntime.removeShutdownHook(hook)
            catch case _: IllegalStateException => () // JVM đã bắt đầu shutdown.
        }
      case _ => throw new IllegalArgumentException("unknown command")

/** Process con cho thí nghiệm kill/restart thật, không dùng exception để giả crash. */
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
