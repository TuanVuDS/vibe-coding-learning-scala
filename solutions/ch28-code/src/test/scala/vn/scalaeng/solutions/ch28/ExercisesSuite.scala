package vn.scalaeng.solutions.ch28

import vn.scalaeng.kafka.*

class ExercisesSuite extends munit.FunSuite:
  val e = OrderEvent(1, "event-1", "ORDER", 100)
  test("1 explicit v2 adapter, unknown currency rejected") {
    val v2 = Protocol.encode(e.copy(schemaVersion = 2)).dropRight(1) + ",\"currency\":\"VND\"}"
    assertEquals(Exercises.decodeV2(v2), Right(e))
    assert(Exercises.decodeV2(v2.replace("VND", "USD")).isLeft)
    assert(Exercises.decodeV2(Protocol.encode(e)).isLeft)
    assert(Exercises.decodeV2("{").isLeft)
  }
  test("2 total lag unknown partitions and BigInt") {
    assertEquals(Exercises.totalLag(Vector((8, Some(3)), (4, Some(1)))), Some(BigInt(8)))
    assertEquals(Exercises.totalLag(Vector((8, None))), None)
    assertEquals(Exercises.totalLag(Vector.fill(2)((Long.MaxValue, Some(0L)))), Some(BigInt(Long.MaxValue) * 2))
  }
  test("3 completed prefix cannot skip unfinished offset, gaps supported") {
    assertEquals(Exercises.commitFrontier(Vector((0, true), (2, false), (5, true))), Some(1L))
    assertEquals(Exercises.commitFrontier(Vector((0, true), (2, true), (5, false))), Some(3L))
    assertEquals(Exercises.commitFrontier(Vector((2, false), (5, true))), None)
    assertEquals(Exercises.commitFrontier(Vector.empty), None)
    intercept[IllegalArgumentException](Exercises.commitFrontier(Vector((1, true), (1, true))))
  }
  test("4 capped backoff without overflow, initial attempt not counted") {
    assertEquals(Exercises.backoff(10, 25, 4), Vector(10L, 20L, 25L, 25L))
    assertEquals(Exercises.backoff(Long.MaxValue / 2 + 1, Long.MaxValue, 3),
      Vector(Long.MaxValue / 2 + 1, Long.MaxValue, Long.MaxValue))
    assertEquals(Exercises.backoff(10, 10, 0), Vector.empty)
    intercept[IllegalArgumentException](Exercises.backoff(0, 10, 3))
  }
  test("5 snapshot restored across simulated restart; conflicting ID never changes totals") {
    val saved = Exercises.applyEvent(Exercises.empty, e).toOption.get
    val restored = saved.copy()
    assertEquals(Exercises.applyEvent(restored, e), Right(saved))
    assertEquals(Exercises.applyEvent(restored, e.copy(amount = 200)), Left("conflict: event-1"))
    assertEquals(restored.totals, Map("ORDER" -> BigInt(100)))
  }
  test("6 poison replay remains invalid; fixed raw accepted, null preserved") {
    val dead = DeadLetter("input", 0, 2, Some("ORDER"), Some("{"), Vector("invalid JSON"), 0)
    assert(Exercises.replay(dead.json).isLeft)
    assertEquals(Exercises.replay(dead.copy(raw = Some(Protocol.encode(e))).json), Right(e))
    assert(Exercises.replay(dead.copy(raw = None).json).isLeft)
  }
  test("6 commit failure after acknowledged DLT duplicates DLT on restart") {
    val record = new org.apache.kafka.clients.consumer.ConsumerRecord[String, String]("input", 0, 2L, "bad", "{")
    var keys = Vector.empty[String]
    def publish(d: DeadLetter): Unit = keys :+= s"${d.topic}:${d.partition}:${d.offset}"
    intercept[IllegalStateException] {
      Processor.handle(record, _ => fail("poison"), publish, () => throw IllegalStateException("lost generation"))
    }
    Processor.handle(record, _ => fail("poison"), publish, () => ())
    assertEquals(keys, Vector("input:0:2", "input:0:2"))
  }
