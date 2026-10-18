package vn.scalaeng.solutions.ch29

import vn.scalaeng.p4.{Input, Receipt}

class ExercisesSuite extends munit.FunSuite:
  val in = Input("orders", 0, 0, Some("ORDER"), Some("{"))
  val dead = Receipt(in, "invalid", Some("invalid JSON"), 0)
  test("1 reconciliation proves every source occupies exactly one bin") {
    assertEquals(Exercises.reconcile(Vector(dead), 1), Map("invalid" -> 1))
    intercept[IllegalArgumentException](Exercises.reconcile(Vector(dead, dead), 2))
    intercept[IllegalArgumentException](Exercises.reconcile(Vector(dead.copy(reason = None)), 1))
  }
  test("2 active partitions min, unknown blocks, all idle unknown") {
    val watermarks = Map(0 -> Some(100L), 1 -> None, 2 -> Some(50L))
    assertEquals(Exercises.globalWatermark(watermarks, Set.empty), None)
    assertEquals(Exercises.globalWatermark(watermarks, Set(1)), Some(50L))
    assertEquals(Exercises.globalWatermark(watermarks, Set(0, 1, 2)), None)
  }
  test("3 future guard boundary and arithmetic near Long.MaxValue") {
    assert(Exercises.futureAllowed(110, 100, 10))
    assert(!Exercises.futureAllowed(111, 100, 10))
    assert(Exercises.futureAllowed(Long.MaxValue, Long.MaxValue - 5, 10))
  }
  test("4 duplicate DLT copies collapse but changed payload requires investigation") {
    assertEquals(Exercises.uniqueDead(Vector(dead, dead)), Vector(dead))
    intercept[IllegalArgumentException](Exercises.uniqueDead(Vector(dead, dead.copy(attempts = 3))))
  }
  test("5 byte budget multiplication does not overflow; queue only, not entire state") {
    assert(Exercises.fitsMemory(2, 5, 100, 1000))
    assert(!Exercises.fitsMemory(Int.MaxValue, Int.MaxValue, Long.MaxValue, BigInt(Long.MaxValue)))
  }
  test("6 close half-open window only when end <= watermark, without overflow") {
    assert(Exercises.closed(100, 100, 200))
    assert(!Exercises.closed(100, 100, 199))
    assert(!Exercises.closed(Long.MaxValue - 1, 100, Long.MaxValue))
  }
