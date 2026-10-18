package vn.scalaeng.kafka

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** No implicit skip on failure: opt-in skips only when RUN_KAFKA_IT is absent. */
class BrokerSuite extends munit.FunSuite:
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(120, "s")

  def eventually(label: String)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos
    while !condition && System.nanoTime() < deadline do Thread.sleep(25)
    assert(condition, s"deadline: $label")

  def readOne(c: KafkaConsumer[String, String]): ConsumerRecord[String, String] =
    var result: Option[ConsumerRecord[String, String]] = None
    eventually("record available") {
      if result.isEmpty then result = c.poll(Duration.ofMillis(100)).iterator().asScala.toVector.headOption
      result.nonEmpty
    }
    result.get

  final class Member(bootstrap: String, group: String, topic: String) extends AutoCloseable:
    val assignment = new AtomicReference(Set.empty[TopicPartition])
    val revokes = new AtomicInteger(0)
    val error = new AtomicReference[Throwable]()
    private val running = new AtomicBoolean(true)
    private val stopped = new CountDownLatch(1)
    private val thread = new Thread(() => {
      try Using.resource(Clients.consumer(bootstrap, group)) { c =>
        c.subscribe(java.util.List.of(topic), new ConsumerRebalanceListener:
          def onPartitionsRevoked(parts: java.util.Collection[TopicPartition]): Unit =
            if !parts.isEmpty then revokes.incrementAndGet()
            assignment.set(Set.empty)
          def onPartitionsAssigned(parts: java.util.Collection[TopicPartition]): Unit =
            assignment.set(parts.asScala.toSet)
        )
        while running.get() do c.poll(Duration.ofMillis(100))
      }
      catch case e: Throwable => error.set(e)
      finally stopped.countDown()
    }, "kafka-lab-member")
    thread.setDaemon(true)
    thread.start()
    def close(): Unit =
      running.set(false)
      assert(stopped.await(15, TimeUnit.SECONDS), "consumer thread did not stop")
      Option(error.get()).foreach(e => throw e)

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
      Using.resource(Admin.create(Map[String, Object]("bootstrap.servers" -> bootstrap).asJava)) { admin =>
        admin.createTopics(topics.asJava).all().get(20, TimeUnit.SECONDS)
      }
      Using.resource(Clients.producer(bootstrap)) { p =>
        val metadata = Vector.tabulate(6)(i => Clients.send(p, "orders", s"o${i % 2}", s"event$i", i % 2))
        assertEquals(metadata.filter(_.partition() == 0).map(_.offset()), Vector(0L, 1L, 2L))
        assertEquals(metadata.filter(_.partition() == 1).map(_.offset()), Vector(0L, 1L, 2L))
        println("partitions: p0 offsets=0,1,2; p1 offsets=0,1,2")

        Using.resource(new Member(bootstrap, "workers", "orders")) { first =>
          eventually("first member owns 2")(first.assignment.get().size == 2)
          println(s"group before: A=${first.assignment.get().size}")
          Using.resource(new Member(bootstrap, "workers", "orders")) { second =>
            eventually("two members split partitions") {
              val a = first.assignment.get(); val b = second.assignment.get()
              a.size == 1 && b.size == 1 && (a intersect b).isEmpty && (a union b).size == 2
            }
            assert(first.revokes.get() >= 1)
            println(s"group joined: A=${first.assignment.get().size} B=${second.assignment.get().size} revoked=${first.revokes.get()}")
          }
          eventually("first member regains 2")(first.assignment.get().size == 2)
          println(s"group left: A=${first.assignment.get().size}")
        }

        val tp = new TopicPartition("orders", 0)
        Using.resource(Clients.consumer(bootstrap, "lag-demo")) { c =>
          c.assign(java.util.List.of(tp))
          c.commitSync(Map(tp -> new OffsetAndMetadata(1L)).asJava)
          val end = c.endOffsets(java.util.List.of(tp)).get(tp).longValue()
          val committed = c.committed(java.util.Set.of(tp)).get(tp).offset()
          assertEquals(Clients.lag(end, Some(committed)), Some(BigInt(2)))
          println(s"lag: end=$end committed=$committed lag=${end - committed}")
        }

        (0 until 3).foreach(i => Clients.send(p, "crash", s"o$i", s"e$i", 0))
        val crashTp = new TopicPartition("crash", 0)
        Using.resource(Clients.consumer(bootstrap, "at-most")) { c =>
          c.assign(java.util.List.of(crashTp))
          val r = readOne(c); assertEquals(r.offset(), 0L)
          Clients.commit(c, r) // crash boundary BEFORE side effect
        }
        Using.resource(Clients.consumer(bootstrap, "at-most")) { c =>
          c.assign(java.util.List.of(crashTp))
          val r = readOne(c); assertEquals(r.offset(), 1L)
          println(s"at-most restart: first offset=${r.offset()} skipped=0")
        }
        val ledger = new MemoryLedger
        val e = OrderEvent(1, "stable-event", "ORDER", 100)
        Using.resource(Clients.consumer(bootstrap, "at-least")) { c =>
          c.assign(java.util.List.of(crashTp))
          val r = readOne(c); assertEquals(r.offset(), 0L)
          c.commitSync(Map(crashTp -> new OffsetAndMetadata(0L)).asJava)
          assertEquals(ledger(e), Applied.New) // crash boundary AFTER side effect, BEFORE commit
        }
        Using.resource(Clients.consumer(bootstrap, "at-least")) { c =>
          c.assign(java.util.List.of(crashTp))
          val r = readOne(c); assertEquals(r.offset(), 0L)
          assertEquals(ledger(e), Applied.Duplicate)
          Clients.commit(c, r)
          assertEquals(ledger.totals, Map("ORDER" -> BigInt(100)))
          println(s"at-least restart: first offset=${r.offset()} sink duplicate=true total=100")
        }

        // Application resend is not eliminated by idempotent producer.
        val sent = Vector.fill(2)(Clients.send(p, "input", "ORDER", Protocol.encode(e), 0))
        assertEquals(sent.map(_.offset()), Vector(0L, 1L))
        Clients.send(p, "input", "bad", "{", 0)
        Clients.send(p, "input", "retry", Protocol.encode(e.copy(eventId = "retry")), 0)
        Clients.send(p, "input", "ORDER", Protocol.encode(e.copy(amount = 200)), 0)
        val sink = new MemoryLedger
        var attempts = 0
        val inputTp = new TopicPartition("input", 0)
        Using.resource(Clients.consumer(bootstrap, "pipeline")) { c =>
          c.assign(java.util.List.of(inputTp))
          val statuses = Vector.fill(5) {
            val r = readOne(c)
            Processor.handle(r, event => {
              if event.eventId == "retry" then { attempts += 1; throw Transient("sink busy") }
              sink(event); ()
            }, dead => { Clients.send(p, "dead", s"${dead.topic}:${dead.partition}:${dead.offset}", dead.json, 0); () },
              () => Clients.commit(c, r), pause = _ => ())
          }
          assertEquals(statuses, Vector("processed", "processed", "dead-letter", "dead-letter", "dead-letter"))
          assertEquals(attempts, 3)
          assertEquals(sink.totals, Map("ORDER" -> BigInt(100)))
          assertEquals(c.committed(java.util.Set.of(inputTp)).get(inputTp).offset(), 5L)
          println(s"pipeline: statuses=${statuses.mkString(",")} attempts=$attempts total=100 committed=5")
        }
        Using.resource(Clients.consumer(bootstrap, "inspect-dead")) { c =>
          c.assign(java.util.List.of(new TopicPartition("dead", 0)))
          val dead = Vector.fill(3)(io.circe.parser.parse(readOne(c).value()).toOption.get)
          assertEquals(dead.map(_.hcursor.get[Int]("attempts").toOption.get), Vector(0, 3, 1))
          assertEquals(dead.map(_.hcursor.get[Long]("source_offset").toOption.get), Vector(2L, 3L, 4L))
          println("DLT: source_offsets=2,3,4 attempts=0,3,1")
        }
        ProducerMain.main(Array(bootstrap, "cli"))
        ConsumerMain.main(Array(bootstrap, "cli", "cli-dead", "cli-group", "1"))
        Using.resource(Clients.consumer(bootstrap, "cli-group")) { c =>
          val cliTp = new TopicPartition("cli", 0)
          assertEquals(c.committed(java.util.Set.of(cliTp)).get(cliTp).offset(), 1L)
        }
      }
    }
  }
