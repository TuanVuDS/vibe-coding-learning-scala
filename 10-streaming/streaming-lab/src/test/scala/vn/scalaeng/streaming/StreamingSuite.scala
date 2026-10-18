package vn.scalaeng.streaming

class StreamingSuite extends munit.FunSuite:
  test("event clock stable under replay; processing clock shifts buckets") {
    val first = Vector(Arrival(Event("a", "c", 8), 100), Arrival(Event("b", "c", 12), 101))
    val replay = first.map(a => a.copy(processedAt = a.processedAt + 100))
    assertEquals(Clocks.buckets(first, 10, true), Map(Window(0, 10) -> BigInt(1), Window(10, 20) -> BigInt(1)))
    assertEquals(Clocks.buckets(first, 10, true), Clocks.buckets(replay, 10, true))
    assertEquals(Clocks.buckets(first, 10, false), Map(Window(100, 110) -> BigInt(2)))
    assertEquals(Clocks.buckets(replay, 10, false), Map(Window(200, 210) -> BigInt(2)))
  }
  test("half-open tumbling boundaries and no Long overflow") {
    assertEquals(Windows.tumbling(9, 10), Window(0, 10))
    assertEquals(Windows.tumbling(10, 10), Window(10, 20))
    assert(Windows.tumbling(Long.MaxValue, 10).end > BigInt(Long.MaxValue))
    intercept[IllegalArgumentException](Windows.tumbling(0, 0))
  }
  test("sliding contains time in every emitted window") {
    assertEquals(Windows.sliding(12, 20, 10), Vector(Window(10, 30), Window(0, 20)))
    assertEquals(Windows.sliding(20, 20, 10), Vector(Window(20, 40), Window(10, 30)))
    assertEquals(Windows.sliding(0, 20, 10), Vector(Window(0, 20)))
    intercept[IllegalArgumentException](Windows.sliding(1, 20, 0))
  }
  test("sessions sort, merge bridge events, split at exact gap, isolate keys") {
    val events = Vector(Event("a", "u", 0), Event("b", "u", 18), Event("bridge", "u", 9),
      Event("c", "u", 28), Event("d", "v", 5))
    assertEquals(Windows.sessions(events, 10), Vector(Result("u", Window(0, 28), 3, 3),
      Result("u", Window(28, 38), 1, 1), Result("v", Window(5, 15), 1, 1)))
    assertEquals(Windows.sessions(events.reverse, 10), Windows.sessions(events, 10))
  }
  test("late uses previous watermark; equality accepted; watermark never decreases") {
    val first = EventTime.step(State(), Event("a", "c", 12), 10, 5)
    val equal = EventTime.step(first.state, Event("b", "c", 7), 10, 5)
    assertEquals(equal.decision, Decision.Accepted)
    assertEquals(equal.state.watermark, Some(BigInt(7)))
    val late = EventTime.step(equal.state, Event("c", "c", 6), 10, 5)
    assertEquals(late.decision, Decision.Late)
    assertEquals(late.state, equal.state)
  }
  test("closed state evicted; late cannot reopen window; idle input does not advance time") {
    val a = EventTime.step(State(), Event("a", "c", 2), 10, 5)
    val b = EventTime.step(a.state, Event("b", "c", 21), 10, 5)
    assertEquals(b.emitted, Vector(Result("c", Window(0, 10), 1, 1)))
    assertEquals(b.state.seen.keySet, Set("b"))
    assertEquals(EventTime.step(b.state, Event("a", "c", 2), 10, 5).decision, Decision.Late)
    assertEquals(EventTime.finish(b.state), Vector(Result("c", Window(20, 30), 1, 1)))
  }
  test("dedup compares full payload and totals cannot overflow") {
    val e = Event("a", "o", 12, BigInt(Long.MaxValue))
    val a = EventTime.step(State(), e, 10, 5)
    assertEquals(EventTime.step(a.state, e, 10, 5).decision, Decision.Duplicate)
    assertEquals(EventTime.step(a.state, e.copy(amount = 0), 10, 5).decision, Decision.Conflict)
    val b = EventTime.step(a.state, e.copy(id = "b"), 10, 5)
    assertEquals(EventTime.finish(b.state), Vector(Result("o", Window(10, 20), 2, BigInt(Long.MaxValue) * 2)))
  }
  test("golden clicks: closed vs open and late loss are explicit") {
    val steps = StreamingDemo.clicks.scanLeft(Step(State(), Decision.Accepted, Vector.empty))((s, e) =>
      EventTime.step(s.state, e, 10, 5)).tail
    assertEquals(steps.map(_.decision), Vector(Decision.Accepted, Decision.Accepted, Decision.Accepted, Decision.Accepted, Decision.Late))
    assertEquals(steps.flatMap(_.emitted), Vector(Result("CMP-1", Window(0, 10), 2, 2)))
    assertEquals(EventTime.finish(steps.last.state), Vector(Result("CMP-1", Window(10, 20), 1, 1), Result("CMP-1", Window(20, 30), 1, 1)))
  }
  test("crash simulations lose vs duplicate; dedup sink survives restart") {
    val ids = Vector("o1", "o2", "o3")
    assertEquals(Crash.run(ids, Delivery.AtMostOnce, 1, false).effects, Vector("o1", "o3"))
    assertEquals(Crash.run(ids, Delivery.AtLeastOnce, 1, false).effects, Vector("o1", "o2", "o2", "o3"))
    assertEquals(Crash.run(ids, Delivery.AtLeastOnce, 1, true).effects, ids)
  }

class PressureSuite extends munit.CatsEffectSuite:
  test("bounded queue blocks third offer and drains all events") {
    Backpressure.measure.map { p =>
      assert(p.blocked)
      assertEquals(p.peak, 2)
      assertEquals(p.consumed, 12)
      assert(p.maxOfferMs >= 30)
    }
  }
