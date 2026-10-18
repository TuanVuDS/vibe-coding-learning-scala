package scalaeng.solutions

import munit.FunSuite

class Ch06SolutionsSuite extends FunSuite:
  import Ch06Solutions.*

  test("bài 1: tiếng Việt có dấu"):
    assertEquals(wordFrequency("Đơn hàng, đơn HÀNG! giao-hàng 2 lần"),
      Map("đơn" -> 2, "hàng" -> 3, "giao" -> 1, "2" -> 1, "lần" -> 1))
    assertEquals(wordFrequency("  ,, "), Map.empty[String, Int])

  test("bài 2"):
    assertEquals(topN(Map("b" -> 5L, "a" -> 5L, "c" -> 9L), 2), List(("c", 9L), ("a", 5L)))

  test("bài 3"):
    assertEquals(mergeInventories(Map("A" -> 1, "B" -> 2), Map("B" -> 3, "C" -> 4)), Map("A" -> 1, "B" -> 5, "C" -> 4))

  test("bài 4"):
    val rs = List(("2", 10L, "x"), ("1", 5L, "old"), ("1", 7L, "new"), ("2", 3L, "y"))
    assertEquals(dedupLatest(rs), List(("1", 7L, "new"), ("2", 10L, "x")))
    assertEquals(dedupLatest(Nil), Nil)

  test("bài 5"):
    val s = List(200, 500, 503, 200, 200, 502, 504)
    assertEquals(errorBurstStarts(s, 3, 2), List(0, 1, 4))
    assertEquals(errorBurstStarts(List(500), 3, 1), Nil)
    intercept[IllegalArgumentException](errorBurstStarts(s, 0, 1))
