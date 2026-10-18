package vn.scalaeng.solutions.ch27

import vn.scalaeng.streaming.*

class ExercisesSuite extends munit.FunSuite:
  test("1 clocks differ when replay time changes") {
    val e = Event("e", "c", 8)
    assertEquals(Exercises.clocks(e, 100, 10), (Window(0, 10), Window(100, 110)))
    assertEquals(Exercises.clocks(e, 200, 10), (Window(0, 10), Window(200, 210)))
  }
  test("2 sliding totals golden, half-open and BigInt") {
    val events = Vector(Event("a", "o", 12, BigInt(Long.MaxValue)), Event("b", "o", 20, 5))
    assertEquals(Exercises.slidingTotals(events, 20, 10), Map(
      ("o", Window(0, 20)) -> BigInt(Long.MaxValue),
      ("o", Window(10, 30)) -> (BigInt(Long.MaxValue) + 5), ("o", Window(20, 40)) -> BigInt(5)))
    assertEquals(Exercises.slidingTotals(events.reverse, 20, 10), Exercises.slidingTotals(events, 20, 10))
  }
  test("3 unknown active source stalls, idle excluded, all idle gives no advance") {
    assertEquals(Exercises.globalWatermark(Vector((false, Some(10)), (false, Some(4)))), Some(4L))
    assertEquals(Exercises.globalWatermark(Vector((false, Some(10)), (true, None))), Some(10L))
    assertEquals(Exercises.globalWatermark(Vector((false, Some(10)), (false, None))), None)
    assertEquals(Exercises.globalWatermark(Vector((true, Some(4)))), None)
    assertEquals(Exercises.globalWatermark(Vector.empty), None)
  }
  test("4 late bridge merges two provisional sessions") {
    val previews = Exercises.sessionPreviews(Vector(Event("a", "u", 0), Event("b", "u", 18), Event("c", "u", 9)), 10)
    assertEquals(previews.map(_.size), Vector(1, 2, 1))
    assertEquals(previews.last, Vector(Result("u", Window(0, 28), 3, 3)))
  }
  test("5 audit conservation, conflict and dedup; event-time eviction does not bound hot-key cardinality") {
    val a = Event("a", "c", 2)
    val audit = Exercises.audit(Vector(a, a, a.copy(amount = 2), Event("b", "c", 21), Event("late", "c", 0)), 10, 5)
    assertEquals(audit.decisions, Map(Decision.Accepted -> 2, Decision.Duplicate -> 1, Decision.Conflict -> 1, Decision.Late -> 1))
    assertEquals(audit.decisions.values.sum, 5)
    assertEquals(audit.emitted, Vector(Result("c", Window(0, 10), 1, 1)))
    val hot = Exercises.audit(Vector.tabulate(1000)(i => Event(s"e$i", s"key$i", 5)), 10, 5)
    assertEquals(hot.state.open.size, 1000)
    assertEquals(hot.state.seen.size, 1000)
  }
  test("6 backlog no overflow and no negative backlog") {
    assertEquals(Exercises.backlog(1000, 800, 60), BigInt(12000))
    assertEquals(Exercises.backlog(10, 20, 60), BigInt(0))
    assertEquals(Exercises.backlog(Long.MaxValue, 0, Long.MaxValue), BigInt(Long.MaxValue).pow(2))
    intercept[IllegalArgumentException](Exercises.backlog(-1, 0, 1))
  }

class BackpressureExerciseSuite extends munit.CatsEffectSuite:
  test("6 bounded source blocks and all twelve events still complete") {
    Backpressure.measure.map { p => assert(p.blocked); assertEquals(p.consumed, 12); assertEquals(p.peak, 2) }
  }
