package vn.scalaeng.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig

class CoreSuite extends munit.FunSuite:
  val event = OrderEvent(1, "e1", "o1", Long.MaxValue)
  def record(raw: String): ConsumerRecord[String, String] = new ConsumerRecord("orders", 1, 42L, "o1", raw)

  test("JSON round trip escaping, additive fields, unsupported schema and accumulating validation") {
    val e = event.copy(eventId = "e\"\\\n")
    assertEquals(Protocol.decode(Protocol.encode(e)), Right(e))
    assertEquals(Protocol.decode(Protocol.encode(event).dropRight(1) + ",\"extra\":true}"), Right(event))
    assertEquals(Protocol.decode("{}" ).left.toOption.get.size, 4)
    assert(Protocol.decode(Protocol.encode(event.copy(schemaVersion = 2))).isLeft)
    assert(Protocol.decode("{").isLeft)
    assertEquals(Protocol.decode(null), Left(Vector("tombstone unsupported")))
  }
  test("identity preserves case; full fingerprint detects conflicts; BigInt totals") {
    val sink = new MemoryLedger
    assertEquals(sink(event), Applied.New)
    assertEquals(sink(event), Applied.Duplicate)
    sink(event.copy(eventId = "E1"))
    assertEquals(sink.totals, Map("o1" -> BigInt(Long.MaxValue) * 2))
    interceptMessage[Poison]("event_id conflict: e1")(sink(event.copy(amount = 0)))
    assertEquals(sink.totals("o1"), BigInt(Long.MaxValue) * 2)
  }
  test("retry bounded, fresh attempts; poison not retried; unexpected error propagates") {
    var attempts = 0
    val delays = scala.collection.mutable.ArrayBuffer.empty[Int]
    val r = Retry.run(3, n => { delays += n; () }) { attempts += 1; throw Transient("busy") }
    assertEquals(attempts, 3)
    assertEquals(delays.toVector, Vector(1, 2))
    assertEquals(r.left.toOption.get._2, 3)
    assertEquals(Retry.run(3, _ => fail("no pause"))(throw Poison("bad")).left.toOption.get._2, 1)
    intercept[IllegalStateException](Retry.run(3, _ => ())(throw IllegalStateException("db down")))
    intercept[IllegalArgumentException](Retry.run(0, _ => ())(1))
  }
  test("DLT failure leaves source uncommitted; raw payload and origin retained") {
    var committed = false
    var dead: Option[DeadLetter] = None
    intercept[java.io.IOException] {
      Processor.handle(record("{"), _ => fail("must not apply"), d => { dead = Some(d); throw java.io.IOException("DLT down") },
        () => committed = true)
    }
    assert(!committed)
    assertEquals(dead.get.raw, Some("{"))
    assertEquals((dead.get.partition, dead.get.offset, dead.get.attempts), (1, 42L, 0))
  }
  test("successful retry commits after effect; exhausted retry publishes before commit") {
    var log = Vector.empty[String]
    var count = 0
    val status = Processor.handle(record(Protocol.encode(event)), _ => {
      count += 1; log :+= s"try$count"; if count < 3 then throw Transient("busy")
    }, _ => fail("no DLT"), () => log :+= "commit", pause = _ => ())
    assertEquals(status, "processed")
    assertEquals(log, Vector("try1", "try2", "try3", "commit"))
    var dead: Option[DeadLetter] = None
    Processor.handle(record(Protocol.encode(event)), _ => throw Transient("busy"), d => { dead = Some(d); log :+= "DLT" },
      () => log :+= "commit", pause = _ => ())
    assertEquals(dead.get.attempts, 3)
    assertEquals(log.takeRight(2), Vector("DLT", "commit"))
  }
  test("lag distinguishes no committed offset from zero; invalid ranges fail") {
    assertEquals(Clients.lag(8, Some(3)), Some(BigInt(5)))
    assertEquals(Clients.lag(8, None), None)
    intercept[IllegalArgumentException](Clients.lag(2, Some(3)))
  }
  test("producer configuration validates idempotence and rejects conflicting acks") {
    val p = Clients.producerProperties("localhost:9092")
    assert(new ProducerConfig(p).getBoolean("enable.idempotence"))
    p.setProperty("acks", "0")
    intercept[org.apache.kafka.common.config.ConfigException](new ProducerConfig(p))
  }
