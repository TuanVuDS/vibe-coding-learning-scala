package vn.scalaeng.p4

import java.nio.file.Files
import scala.util.Using

class CoreSuite extends munit.FunSuite:
  val event = Event(1, "e1", "ORDER", 100, 10)
  def in(offset: Long, e: Event = event): Input = Input("orders", 0, offset, Some(e.orderId), Some(Json.encode(e)))
  def fresh(test: Store => Unit): Unit = Using.resource(new Store(Files.createTempDirectory("p4-core-")))(test)

  test("strict schema, null, malformed, version, values and key") {
    assertEquals(Json.event(None), Left("null value is not an order event"))
    assertEquals(Json.event(Some("{")), Left("invalid JSON"))
    assertEquals(Json.event(Some("{}")), Left("schema fields mismatch"))
    assertEquals(Json.event(Some(Json.encode(event.copy(version = 2)))), Left("unsupported version"))
    assert(Json.event(Some(Json.encode(event.copy(amount = 0)))).isLeft)
    fresh(s => assertEquals(s.process(in(0).copy(key = Some("wrong"))).kind, "invalid"))
  }
  test("durable atomic fingerprint, buckets, receipt survive reopen") {
    val dir = Files.createTempDirectory("p4-reopen-")
    Using.resource(new Store(dir)) { s =>
      assertEquals(s.process(in(0)).kind, "accepted")
      assertEquals(s.process(in(1)).kind, "duplicate")
      assertEquals(s.process(in(2, event.copy(amount = 99))).kind, "conflict")
    }
    Using.resource(new Store(dir)) { s =>
      assertEquals(s.process(in(0)).kind, "accepted") // Replay receipt, không phải input mới.
      assertEquals(s.state.receipts.size, 3)
      assertEquals(s.state.buckets.map(_.total).sum, BigInt(10))
      assertEquals(s.process(in(3, event.copy(eventTime = 999))).kind, "conflict")
    }
  }
  test("event time boundary uses old watermark and half open windows") {
    fresh { s =>
      s.process(in(0))
      s.process(in(1, event.copy(eventId = "e2", eventTime = 300)))
      assertEquals(s.process(in(2, event.copy(eventId = "e3", eventTime = 250))).kind, "accepted")
      assertEquals(s.process(in(3, event.copy(eventId = "e4", eventTime = 249))).kind, "late")
      assertEquals(s.state.maxTime, 300L)
      assertEquals(s.state.buckets.map(_.start).sorted, Vector(100L, 200L, 300L))
    }
  }
  test("retry classified only, bounded, counted; unexpected failure stops") {
    fresh { s =>
      var n = 0
      val r = s.process(in(0), _ => { n += 1; if n < 3 then throw Transient("busy") }, pause = _ => ())
      assertEquals(r.attempts, 3)
      val failed = s.process(in(1, event.copy(eventId = "busy")), _ => throw Transient("busy"), pause = _ => ())
      assertEquals((failed.kind, failed.attempts), ("exhausted", 3))
      interceptMessage[IllegalStateException]("disk offline") {
        s.process(in(2), _ => throw new IllegalStateException("disk offline")) // Duplicate bỏ qua prepare.
        s.process(in(3, event.copy(eventId = "new")), _ => throw new IllegalStateException("disk offline"))
      }
      assert(!s.state.receipts.contains("orders:0:3"))
    }
  }
  test("failed snapshot replacement cannot mutate in-memory or restored state") {
    val dir = Files.createTempDirectory("p4-fault-")
    Using.resource(new Store(dir))(_.process(in(0)))
    Using.resource(new Store(dir, beforeReplace = () => throw new java.io.IOException("injected write failure"))) { s =>
      intercept[java.io.IOException](s.process(in(1, event.copy(eventId = "e2"))))
      assertEquals(s.state.receipts.size, 1)
    }
    Using.resource(new Store(dir))(s => assertEquals(s.state.receipts.size, 1))
  }
  test("DLT failure prevents commit; commit failure permits identical DLT replay") {
    fresh { s =>
      val bad = in(0).copy(raw = Some("{"))
      var commits = 0
      intercept[java.io.IOException] {
        Pipeline.handle(s, bad, _ => throw new java.io.IOException("DLT unavailable"), () => commits += 1)
      }
      assertEquals(commits, 0)
      var dead = Vector.empty[String]
      val publish: Receipt => Unit = r => dead :+= Json.receipt(r)
      intercept[java.io.IOException] {
        Pipeline.handle(s, bad, publish, () => throw new java.io.IOException("commit unavailable"))
      }
      Pipeline.handle(s, bad, publish, () => commits += 1)
      assertEquals(dead.distinct.size, 1)
      assertEquals(dead.size, 2)
      assertEquals(commits, 1)
    }
  }
  test("receipt provenance immutable; policy incompatible and corrupt snapshots fail closed") {
    val dir = Files.createTempDirectory("p4-policy-")
    Using.resource(new Store(dir)) { s =>
      s.process(in(0))
      intercept[IllegalArgumentException](s.process(in(0, event.copy(amount = 2))))
    }
    intercept[IllegalArgumentException](new Store(dir, width = 200))
    Files.writeString(dir.resolve("state.json"), "broken")
    intercept[IllegalStateException](new Store(dir))
  }
  test("one writer lock; BigInt totals do not overflow") {
    val dir = Files.createTempDirectory("p4-lock-")
    Using.resource(new Store(dir)) { s =>
      interceptMessage[IllegalStateException]("store already owned")(new Store(dir))
      s.process(in(0, event.copy(amount = Long.MaxValue)))
      s.process(in(1, event.copy(eventId = "e2", amount = Long.MaxValue)))
      assertEquals(s.state.buckets.head.total, BigInt(Long.MaxValue) * 2)
    }
  }
  test("future seconds and millis route to DLT without poisoning watermark, including restart") {
    val dir = Files.createTempDirectory("p4-future-")
    val now = 1792022400L
    var published = Vector.empty[Receipt]
    var commits = 0
    Using.resource(new Store(dir, clock = () => now, maxFutureSkew = 30)) { s =>
      s.process(in(0, event.copy(eventTime = now)))
      Vector(4000000000L, now * 1000).zipWithIndex.foreach { (time, index) =>
        val bad = in(index.toLong + 1, event.copy(eventId = s"future-$index", eventTime = time))
        val r = Pipeline.handle(s, bad, r => published :+= r, () => commits += 1)
        assertEquals((r.kind, r.reason), ("invalid", Some("eventTime in the future")))
        assertEquals(s.state.maxTime, now)
      }
      assertEquals(published.size, 2)
      assertEquals(commits, 2)
    }
    Using.resource(new Store(dir, clock = () => now, maxFutureSkew = 30)) { s =>
      assertEquals(s.process(in(3, event.copy(eventId = "on-time", eventTime = now))).kind, "accepted")
      assertEquals(s.process(in(4, event.copy(eventId = "boundary", eventTime = now + 30))).kind, "accepted")
      assertEquals(s.process(in(5, event.copy(eventId = "over", eventTime = now + 31))).kind, "invalid")
      assertEquals(s.state.maxTime, now + 30)
    }
    interceptMessage[IllegalArgumentException]("requirement failed: snapshot policy mismatch") {
      new Store(dir, clock = () => now, maxFutureSkew = 31)
    }
    Using.resource(new Store(Files.createTempDirectory("p4-clock-overflow-"), clock = () => Long.MaxValue)) { s =>
      assertEquals(s.process(in(0, event.copy(eventTime = Long.MaxValue))).kind, "accepted")
    }
  }
  test("snapshot byte limit fails before changing RAM/disk and also guards restore") {
    val dir = Files.createTempDirectory("p4-byte-limit-")
    Using.resource(new Store(dir))(_.process(in(0)))
    val target = dir.resolve("state.json")
    val bytes = Files.readString(target)
    Using.resource(new Store(dir, maxSnapshotBytes = Files.size(target))) { s =>
      interceptMessage[IllegalStateException]("snapshot byte limit exceeded")(s.process(in(1)))
      assertEquals(s.state.receipts.size, 1)
      assertEquals(Files.readString(target), bytes)
    }
    interceptMessage[IllegalStateException]("snapshot byte limit exceeded") {
      new Store(dir, maxSnapshotBytes = 1)
    }
  }
  test("fixture format preserves raw invalid JSON, wrong key and nulls") {
    val file = Files.createTempFile("p4-fixture-", ".jsonl")
    Files.writeString(file, "wrong\t{\n\\N\t\\N\n")
    val parsed = Fixture.read(file)
    assertEquals(parsed, Vector(("wrong", "{"), (null, null)))
    Files.writeString(file, "missing tab")
    interceptMessage[IllegalArgumentException]("requirement failed: fixture line 1: expected key<TAB>raw") {
      Fixture.read(file)
    }
  }
