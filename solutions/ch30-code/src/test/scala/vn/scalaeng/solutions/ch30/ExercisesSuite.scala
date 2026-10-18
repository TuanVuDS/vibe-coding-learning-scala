package vn.scalaeng.solutions.ch30

import vn.scalaeng.p4.{Event, Input, Json, Receipt}

class ExercisesSuite extends munit.FunSuite:
  test("1 commit frontier with holes and out of order task completion") {
    assertEquals(Exercises.frontier(Vector(0L -> true, 2L -> false, 5L -> true)), Some(1L))
    assertEquals(Exercises.frontier(Vector(0L -> true, 2L -> true, 5L -> true)), Some(6L))
    assertEquals(Exercises.frontier(Vector.empty), None)
    intercept[IllegalArgumentException](Exercises.frontier(Vector(2L -> true, 1L -> true)))
  }
  test("2 bounded backoff, no Long overflow or extra pause after final attempt") {
    assertEquals(Exercises.backoff(4, 10, 25), Vector(10L, 20L, 25L))
    assertEquals(Exercises.backoff(1, 10, 25), Vector.empty)
    assertEquals(Exercises.backoff(3, Long.MaxValue - 1, Long.MaxValue), Vector(Long.MaxValue - 1, Long.MaxValue))
  }
  test("3 sequence per key detects regressions despite interleaving") {
    assert(Exercises.ordered(Vector("a" -> 1L, "b" -> 99L, "a" -> 2L, "a" -> 2L)))
    assert(!Exercises.ordered(Vector("a" -> 2L, "b" -> 1L, "a" -> 1L)))
  }
  test("4 retry and full batch included in poll budget") {
    assert(Exercises.admitted(1, 3, 1000, Vector(10, 20), 10000, 1000))
    assert(!Exercises.admitted(3, 3, 1000, Vector(10, 20), 10000, 1000))
    assert(!Exercises.admitted(1, 3, Long.MaxValue, Vector(10, 20), Long.MaxValue, 0))
  }
  test("5 unknown lag remains unknown; BigInt sum and impossible offset fail") {
    assertEquals(Exercises.totalLag(Vector(10L -> None)), None)
    assertEquals(Exercises.totalLag(Vector.fill(2)(Long.MaxValue -> Some(0L))), Some(BigInt(Long.MaxValue) * 2))
    intercept[IllegalArgumentException](Exercises.totalLag(Vector(1L -> Some(2L))))
  }
  test("6 corrected DLT preserves business ID; schema/key must still pass") {
    val e = Event(1, "stable", "ORDER", 100, 100)
    val dead = Receipt(Input("orders", 0, 1, Some("ORDER"), Some("{")), "invalid", Some("invalid JSON"), 0)
    assertEquals(Exercises.redrive(dead, Json.encode(e)), Right(e))
    assert(Exercises.redrive(dead, "{").isLeft)
    assert(Exercises.redrive(dead, Json.encode(e.copy(orderId = "other"))).isLeft)
    assert(Exercises.redrive(dead.copy(reason = None), Json.encode(e)).isLeft)
  }
