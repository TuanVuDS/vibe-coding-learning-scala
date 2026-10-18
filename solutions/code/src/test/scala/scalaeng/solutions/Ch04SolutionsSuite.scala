package scalaeng.solutions

import munit.FunSuite

class Ch04SolutionsSuite extends FunSuite:
  import Ch04Solutions.*

  test("bài 1"):
    val f = compose3[String, String, Int, Boolean](_.trim, _.length, _ > 3)
    assertEquals(f("  abcd "), true)
    assertEquals(f(" ab "), false)

  test("bài 2"):
    assertEquals(countWhere(List(1, 2, 3, 4, 5))(_ % 2 == 0), 2)
    assertEquals(countWhere(List.empty[Int])(_ => true), 0)
    assertEquals(countWhere(List.fill(100_000)(1))(_ == 1), 100_000)

  test("bài 3"):
    assertEquals(paginateSafe(total = 95, pageSize = 50, page = 3), (2, 2))
    assertEquals(paginateSafe(total = 95, page = 0), (1, 5))
    assertEquals(paginateSafe(total = 0), (1, 1))
    intercept[IllegalArgumentException](paginateSafe(total = 10, pageSize = 0))

  test("bài 4"):
    val events = List("u1" -> 100L, "u1" -> 300L, "u2" -> 50L)
    assertEquals(averageDurationByUser(events), Map("u1" -> 200.0, "u2" -> 50.0))
    assertEquals(averageDurationByUser(Nil), Map.empty[String, Double])

  test("bài 5"):
    assertEquals(countSessions(Nil, 1000), 0)
    assertEquals(countSessions(List(0L), 1000), 1)
    assertEquals(countSessions(List(0L, 500L, 1500L, 5000L, 5100L), 1000), 2)
    assertEquals(countSessions(List(0L, 1000L, 2000L), 1000), 1) // đúng bằng gap: cùng session
