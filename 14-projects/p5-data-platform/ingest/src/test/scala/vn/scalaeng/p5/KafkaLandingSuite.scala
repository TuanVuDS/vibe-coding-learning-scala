package vn.scalaeng.p5

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Kafka → raw với broker KRaft THẬT (nhúng). Opt-in: RUN_KAFKA_IT=1 (mở socket loopback, khởi động broker ~ vài giây). */
class KafkaLandingSuite extends munit.FunSuite:
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(180, "s")
  private val d15 = LocalDate.parse("2026-10-15")

  private def broker(test: String => Unit): Unit =
    val config = EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0, customBrokerProperties = Map(
      "offsets.topic.replication.factor" -> "1", "offsets.topic.num.partitions" -> "1", "group.initial.rebalance.delay.ms" -> "0",
      "auto.create.topics.enable" -> "false"))
    EmbeddedKafka.withRunningKafkaOnFoundPort(config)(actual => test(s"localhost:${actual.kafkaPort}"))

  private def produce(bootstrap: String, values: Seq[String]): Unit =
    Using.resource(Admin.create(Map[String, Object]("bootstrap.servers" -> bootstrap).asJava)) { a =>
      a.createTopics(java.util.List.of(new NewTopic("orders.events", 1, 1.toShort))).all().get(20, TimeUnit.SECONDS)
    }
    Using.resource(new KafkaProducer[String, String](Map[String, Object]("bootstrap.servers" -> bootstrap, "acks" -> "all",
      "enable.idempotence" -> "true").asJava, new StringSerializer, new StringSerializer)) { p =>
      values.foreach(v => p.send(new ProducerRecord("orders.events", "k", v)).get(15, TimeUnit.SECONDS))
    }

  private def lake() =
    val root = Files.createTempDirectory("p5-kafka")
    Files.createDirectories(root.resolve("dimension"))
    Files.copy(Paths.get("data/dimension/customers.csv"), root.resolve("dimension/customers.csv"))
    Lake(root)

  test("Kafka → raw: record hợp lệ thành một file, record sai contract vào quarantine, commit SAU khi ghi; lag về 0") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "đặt RUN_KAFKA_IT=1 để chạy Kafka KRaft nhúng")
    broker { bootstrap =>
      val events = Io.lines(Paths.get("data/drop/orders-2026-10-15.jsonl"))
      produce(bootstrap, events :+ """{"order_id":"X"}""")
      val l = lake()
      Using.resource(KafkaLanding.consumer(bootstrap, "landing")) { c =>
        c.subscribe(java.util.List.of("orders.events"))
        // Poll tới khi đã thấy đủ 8 record (tối đa 10 lần) — lần poll đầu có thể rỗng trong lúc group đang được gán partition.
        val batches = Iterator.iterate(Vector.empty[KafkaLanding.Batch])(bs => bs :+ KafkaLanding.pollOnce(l, c, d15))
          .dropWhile(bs => bs.map(_.records).sum < 8 && bs.size < 10).next()
        val landed = batches.flatMap(_.files)
        assertEquals((batches.map(_.records).sum, batches.map(_.quarantined).sum), (8, 1))
        assertEquals(landed, Vector("raw/orders/ingest_date=2026-10-15/kafka-orders.events-p0-0-7.jsonl"))
        assertEquals(KafkaLanding.lag(c, "orders.events"), Map(0 -> 0L))
        // Quarantine: phong bì JSON có lý do + giá trị gốc nguyên văn (review P5 N3).
        val q = Io.lines(l.root.resolve("quarantine/orders/ingest_date=2026-10-15/kafka-orders.events-p0-0-7.jsonl"))
          .map(x => io.circe.parser.parse(x).toOption.get.hcursor)
        assertEquals(q.map(h => (h.get[String]("topic"), h.get[Int]("partition"), h.get[String]("value")).toString),
          Vector((Right("orders.events"), Right(0), Right("""{"order_id":"X"}""")).toString))
        assert(q.head.get[String]("reason").exists(_.startsWith("bộ trường sai: thiếu")), q.head.value.noSpaces)
      }
    }
  }

  test("chết SAU khi ghi file, TRƯỚC commit → đọc lại cùng offset; raw có bản sao nhưng staging bỏ sự kiện trùng — doanh thu không đổi") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "đặt RUN_KAFKA_IT=1 để chạy Kafka KRaft nhúng")
    broker { bootstrap =>
      produce(bootstrap, Io.lines(Paths.get("data/drop/orders-2026-10-15.jsonl")))
      val l = lake()
      Using.resource(KafkaLanding.consumer(bootstrap, "landing", maxPollRecords = 4)) { c =>
        c.subscribe(java.util.List.of("orders.events"))
        val first = Iterator.continually(KafkaLanding.pollOnce(l, c, d15)).dropWhile(_.records == 0).next()
        assertEquals(first.files, List("raw/orders/ingest_date=2026-10-15/kafka-orders.events-p0-0-3.jsonl"))
      }
      // Tiến trình thứ hai cùng group: lô 4..6 được GHI rồi "chết" trước commit.
      Using.resource(KafkaLanding.consumer(bootstrap, "landing", maxPollRecords = 4)) { c =>
        c.subscribe(java.util.List.of("orders.events"))
        intercept[IllegalStateException] {
          Iterator.continually(KafkaLanding.pollOnce(l, c, d15, beforeCommit = () => throw new IllegalStateException("kill -9 (giả lập)")))
            .dropWhile(_.records == 0).next()
        }
      }
      // Tiến trình thứ ba: đọc lại từ offset đã commit (4) — với max.poll.records=2 ranh giới lô KHÁC → file khác tên → bản sao trong raw.
      Using.resource(KafkaLanding.consumer(bootstrap, "landing", maxPollRecords = 2)) { c =>
        c.subscribe(java.util.List.of("orders.events"))
        val again = Iterator.continually(KafkaLanding.pollOnce(l, c, d15)).dropWhile(_.records == 0).take(2).toVector
        assertEquals(again.flatMap(_.files), Vector("raw/orders/ingest_date=2026-10-15/kafka-orders.events-p0-4-5.jsonl",
          "raw/orders/ingest_date=2026-10-15/kafka-orders.events-p0-6-6.jsonl"))
      }
      assertEquals(l.rawFiles("orders").size, 4) // 0-3, 4-6 (của tiến trình chết), 4-5, 6-6
      Orchestrator.backfill(l, Assets.all, d15, d15, "r")
      // Chỉ có lô 15 (chưa có hoàn tiền của lô 16): Retail = O1/KB-01 900.000 + O1/MS-01 250.000 — giống hệt khi không có sự cố.
      assertEquals(Io.lines(l.partFile(DailyRevenue, d15)).drop(1), Vector("Retail,1150000,2,1", "UNKNOWN,450000,1,1", "Wholesale,20000000,1,1"))
      val c = Lineage.latest(l, StagingOrders.name, d15).get.counts
      // E1503 do producer gửi 2 lần + E1505 bị land lại; E1504 (quantity 0) bị land lại nên bị loại 2 lần; E1506 (ngày sai) không thuộc ngày 15.
      assertEquals((c("duplicate_events"), c("rejected"), c("kept")), (2, 2, 4))
    }
  }
