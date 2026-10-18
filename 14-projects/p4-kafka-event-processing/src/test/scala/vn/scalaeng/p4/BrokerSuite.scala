package vn.scalaeng.p4

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.TopicPartition
import scala.util.Using

class BrokerSuite extends munit.FunSuite:
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(180, "s")
  def fixture(name: String): String =
    Vector(Path.of("fixtures", name), Path.of("14-projects/p4-kafka-event-processing/fixtures", name))
      .find(Files.exists(_)).getOrElse(throw new IllegalStateException(s"fixture not found: $name")).toString
  def broker(test: String => Unit): Unit =
    val config = EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0, customBrokerProperties = Map(
      "offsets.topic.replication.factor" -> "1", "offsets.topic.num.partitions" -> "1",
      "group.initial.rebalance.delay.ms" -> "0", "auto.create.topics.enable" -> "false"))
    EmbeddedKafka.withRunningKafkaOnFoundPort(config)(actual => test(s"localhost:${actual.kafkaPort}"))

  test("P4 e2e KRaft: producer -> schema/dedup/time/retry -> sink/DLT -> commit/lag") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "set RUN_KAFKA_IT=1 for embedded KRaft")
    broker { bootstrap =>
      Kafka.topics(bootstrap, Vector("orders", "dead"))
      val e = Event(1, "e1", "ORDER", 100, 100)
      val count = Fixture.read(Path.of(fixture("orders.jsonl"))).size
      Main.main(Array("produce", bootstrap, "orders", fixture("orders.jsonl")))
      val dir = Files.createTempDirectory("p4-e2e-")
      Using.resources(new Store(dir), Kafka.producer(bootstrap), Kafka.consumer(bootstrap, "pipeline")) { (s, p, c) =>
        val tp = new TopicPartition("orders", 0)
        c.assign(java.util.List.of(tp))
        assertEquals(Kafka.lag(c, tp), None)
        var retry = 0
        val statuses = Vector.fill(count) {
          val r = Kafka.one(c)
          Pipeline.handle(s, Kafka.input(r), dead => { Kafka.send(p, "dead", dead.input.id, Json.receipt(dead), 0); () },
            () => Kafka.commit(c, r), event => {
              if event.eventId == "retry" then
                retry += 1
                if retry == 1 then throw Transient("adapter busy")
              if event.eventId == "busy" then throw Transient("adapter busy")
            }, pause = _ => ()).kind
        }
        assertEquals(statuses, Vector("accepted", "duplicate", "invalid", "conflict", "accepted", "late", "accepted", "exhausted"))
        assertEquals(s.state.buckets.map(_.total).sum, BigInt(300))
        assertEquals(s.state.receipts.size, count)
        assertEquals(s.state.counts.values.sum, count)
        assertEquals(Kafka.lag(c, tp), Some(BigInt(0)))
        assertEquals(c.committed(java.util.Set.of(tp)).get(tp).offset(), 8L)
        println(s"P4 statuses=${statuses.mkString(",")} total=300 receipts=8 committed=8 lag=0 retryAttempts=$retry")
      }
      Using.resource(Kafka.consumer(bootstrap, "inspect")) { c =>
        c.assign(java.util.List.of(new TopicPartition("dead", 0)))
        val dead = Vector.fill(4)(io.circe.parser.parse(Kafka.one(c).value()).toOption.get)
        val offsets = dead.map(_.hcursor.downField("input").get[Long]("offset").toOption.get)
        val attempts = dead.map(_.hcursor.get[Int]("attempts").toOption.get)
        assertEquals(offsets, Vector(2L, 3L, 5L, 7L))
        assertEquals(attempts, Vector(0, 0, 0, 3))
        assert(dead.forall(_.hcursor.get[String]("reason").toOption.exists(_.nonEmpty)))
        println(s"DLT offsets=${offsets.mkString(",")} attempts=${attempts.mkString(",")} reasons=4")
      }
      Using.resource(new Store(dir)) { s =>
        assertEquals(s.process(Input("orders", 0, 8, Some("ORDER"), Some(Json.encode(e)))).kind, "duplicate")
        assertEquals(s.state.buckets.map(_.total).sum, BigInt(300))
      }
      // Chạy cả producer đọc file và consumer subscribe qua CLI, gồm nhánh DLT thật.
      Main.main(Array("init", bootstrap, "cli", "cli-dead"))
      Main.main(Array("produce", bootstrap, "cli", fixture("orders.jsonl")))
      Main.main(Array("produce", bootstrap, "cli", fixture("invalid-inputs.jsonl")))
      val cliDir = Files.createTempDirectory("p4-cli-")
      Main.main(Array("consume", bootstrap, "cli", "cli-dead", "cli-group",
        cliDir.toString, "12", "--demo-failures"))
      Using.resource(new Store(cliDir)) { s =>
        assertEquals(s.state.counts, Map("accepted" -> 3, "duplicate" -> 1, "invalid" -> 5,
          "conflict" -> 1, "late" -> 1, "exhausted" -> 1))
        assertEquals(s.state.maxTime, 300L)
        assertEquals(s.state.buckets.map(_.total).sum, BigInt(300))
      }
      Using.resource(Kafka.consumer(bootstrap, "cli-inspect")) { c =>
        c.assign(java.util.List.of(new TopicPartition("cli-dead", 0)))
        val dead = Vector.fill(8)(io.circe.parser.parse(Kafka.one(c).value()).toOption.get)
        assertEquals(dead.map(_.hcursor.downField("input").get[Long]("offset").toOption.get),
          Vector(2L, 3L, 5L, 7L, 8L, 9L, 10L, 11L))
        assertEquals(dead.takeRight(2).map(_.hcursor.get[String]("reason").toOption.get),
          Vector.fill(2)("eventTime in the future"))
        println("CLI fixture receipts=12 DLT=8 futureInvalid=2 maxTime=300 total=300")
      }
    }
  }

  test("missing DLT topic raises real Kafka timeout and prevents source commit") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "set RUN_KAFKA_IT=1 for embedded KRaft")
    broker { bootstrap =>
      Kafka.topics(bootstrap, Vector("bad-source"))
      Using.resources(new Store(Files.createTempDirectory("p4-dlt-timeout-")),
          Kafka.producer(bootstrap), Kafka.consumer(bootstrap, "bad-group")) { (s, p, c) =>
        Kafka.send(p, "bad-source", "ORDER", "{", 0)
        val tp = new TopicPartition("bad-source", 0)
        c.assign(java.util.List.of(tp))
        val r = Kafka.one(c)
        val error = intercept[java.util.concurrent.ExecutionException] {
          Pipeline.handle(s, Kafka.input(r), dead => {
            Kafka.send(p, "missing-dead", dead.input.id, Json.receipt(dead)); ()
          }, () => Kafka.commit(c, r))
        }
        assert(error.getCause.isInstanceOf[org.apache.kafka.common.errors.TimeoutException])
        assertEquals(s.state.receipts.size, 1)
        assertEquals(c.committed(java.util.Set.of(tp)).get(tp), null)
        println(s"DLT publish: ${error.getClass.getName}: ${error.getMessage}")
        println("DLT timeout receipts=1 committed=None")
      }
    }
  }

  def classpath: String =
    def urls(loader: ClassLoader): Vector[String] =
      if loader == null then Vector.empty
      else (loader match
        case u: java.net.URLClassLoader => u.getURLs.toVector.filter(_.getProtocol == "file").map(u => Path.of(u.toURI).toString)
        case _ => Vector.empty) ++ urls(loader.getParent)
    (sys.props("java.class.path").split(java.io.File.pathSeparator).toVector ++ urls(getClass.getClassLoader))
      .distinct.mkString(java.io.File.pathSeparator)

  def child(bootstrap: String, topic: String, group: String, dir: Path, marker: Path, mode: String): Process =
    val command = Vector(Path.of(sys.props("java.home"), "bin", "java").toString, "-cp", classpath,
      "vn.scalaeng.p4.CrashWorker", bootstrap, topic, group, dir.toString, marker.toString, mode, "crash-dead")
    new ProcessBuilder(command*).redirectErrorStream(true)
      .redirectOutput(dir.resolve(s"$mode.log").toFile).start()

  def killAtMarker(p: Process, marker: Path): Unit =
    try
      val deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos
      while !Files.exists(marker) && p.isAlive && System.nanoTime() < deadline do Thread.sleep(25)
      assert(Files.exists(marker), s"worker failed before marker; inspect ${marker.getParent}")
    finally
      p.destroyForcibly()
      assert(p.waitFor(15, TimeUnit.SECONDS), "child did not die")

  test("real JVM kill on KRaft: commit-before loses; durable-before-commit replays safely") {
    assume(sys.env.get("RUN_KAFKA_IT").contains("1"), "set RUN_KAFKA_IT=1 for embedded KRaft")
    broker { bootstrap =>
      Kafka.topics(bootstrap, Vector("crash", "crash-dead"))
      val e = Event(1, "stable", "ORDER", 100, 100)
      Using.resource(Kafka.producer(bootstrap)) { p =>
        Kafka.send(p, "crash", "ORDER", Json.encode(e), 0)
        Kafka.send(p, "crash", "ORDER", Json.encode(e.copy(eventId = "next")), 0)
      }
      Vector("before", "after").foreach { mode =>
        val dir = Files.createTempDirectory(s"p4-kill-$mode-")
        val marker = dir.resolve("boundary")
        val p = child(bootstrap, "crash", mode, dir, marker, mode)
        killAtMarker(p, marker)
        assertEquals(Files.readString(marker), "0")
        val restart = child(bootstrap, "crash", mode, dir, marker, "restart")
        try
          assert(restart.waitFor(40, TimeUnit.SECONDS), "restart deadline")
          assertEquals(restart.exitValue(), 0, Files.readString(dir.resolve("restart.log")))
        finally if restart.isAlive then restart.destroyForcibly()
        val offset = if mode == "before" then 1L else 0L
        Using.resource(new Store(dir)) { s =>
          assertEquals(s.state.buckets.map(_.total).sum, BigInt(100))
          assertEquals(s.state.receipts.keySet, Set(s"crash:0:$offset"))
        }
        Using.resource(Kafka.consumer(bootstrap, mode)) { c =>
          val tp = new TopicPartition("crash", 0)
          assertEquals(c.committed(java.util.Set.of(tp)).get(tp).offset(), offset + 1)
        }
        println(s"kill=$mode childExit=${p.exitValue()} restartOffset=$offset total=100 receipts=1 committed=${offset + 1}")
      }
    }
  }
