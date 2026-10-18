package vn.scalaeng.cost.solutions

class ExercisesSuite extends munit.FunSuite:
  import Exercises.*

  test("B1: skewed samples keep p99 visible and reject missing data") {
    val summary = distribution(Vector.fill(99)(1.0) :+ 1000.0)
    assertEquals(summary.mean, 10.99)
    assertEquals(summary.p50, 1.0)
    assertEquals(summary.p99, 1.0)
    assertEquals(distribution(Vector.fill(98)(1.0) ++ Vector(1000.0, 1000.0)).p99, 1000.0)
    intercept[IllegalArgumentException](distribution(Vector.empty))
  }
  test("B2: Little law uses mean and rejects invalid utilization") {
    assertEquals(requiredConsumers(100, Vector(10, 30), .5), 4)
    assertEquals(requiredConsumers(100, Vector(0), .5), 0)
    intercept[IllegalArgumentException](requiredConsumers(100, Vector(20), 1.1))
  }
  test("B3: partition target is not a cure for a hot key") {
    val balanced = layout(401, 100, Vector(10, 10, 10, 10), 2, 1)
    val skewed = layout(401, 100, Vector(37, 1, 1, 1), 2, 1)
    assertEquals(balanced.partitions, BigInt(5))
    assertEquals(balanced.schedulingMs, 22.0)
    assertEquals(skewed.schedulingMs, 38.0)
    assertEquals(layout(0, 100, Vector.empty, 2, 1).schedulingMs, 0.0)
  }
  test("B4: wire model keeps units and rounds once for total bytes") {
    val cost = wireCost(10, 3, 1000000, 1000000, BigDecimal("0.05"))
    assertEquals(cost.bytes, BigInt(3333334))
    assertEquals(cost.transferSeconds, 3.333334)
    assertEquals(wireCost(0, 1, 0, 1, BigDecimal(0)).charge, BigDecimal(0))
    intercept[IllegalArgumentException](wireCost(10, 0, 1, 1, BigDecimal(1)))
  }
  test("B5: cache saving includes materialization and all reads") {
    assertEquals(cacheSaving(150, 90, 20, 2), -10.0)
    assertEquals(cacheSaving(150, 90, 20, 3), 60.0)
    assertEquals(cacheSaving(10, 20, 30, 3), -40.0)
    intercept[IllegalArgumentException](cacheSaving(1, 2, 1, 0))
  }
  test("B6: choose cost only among candidates meeting throughput and tail SLO") {
    val slow = Candidate("slow", 1, 100, 1, BigDecimal("0.01"))
    val late = Candidate("late", 1, 1000, 100, BigDecimal("0.01"))
    val a = Candidate("a", 2, 1000, 2, BigDecimal("0.12"))
    val b = Candidate("b", 1, 1000, 2, BigDecimal("0.12"))
    assertEquals(cheapest(Vector(slow, late, a, b), 500, 5), Some(b))
    assertEquals(cheapest(Vector(slow, late), 500, 5), None)
    assertEquals(cheapest(Vector.empty, 500, 5), None)
    intercept[IllegalArgumentException](cheapest(Vector(b.copy(p99Ms = Double.NaN)), 500, 5))
  }
  test("B7: memory and CPU cap each other without exceeding zero or BigInt") {
    assertEquals(feasibleSlots(1024, 256, 256, 128, 8), BigInt(4))
    assertEquals(feasibleSlots(1024, 256, 256, 128, 2), BigInt(2))
    assertEquals(feasibleSlots(100, 200, 0, 1, 8), BigInt(0))
    assertEquals(feasibleSlots(BigInt(2).pow(100), 0, 0, 1, 12), BigInt(12))
  }
