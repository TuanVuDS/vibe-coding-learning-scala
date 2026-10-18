package scalaeng.solutions

import munit.FunSuite
import scalaeng.ch08b.{Monoid, combineAll}
import Ch08bSolutions.*
import Ch08bSolutions.given   // BẮT BUỘC: `.*` không import given (Chương 8B, mục 10, lỗi 2)

class Ch08bSolutionsSuite extends FunSuite:

  test("bài 1"):
    assertEquals(display(List(1, 2)), "[1, 2]")
    assertEquals(display(List("a")), "[\"a\"]")
    assertEquals(display(true), "có")

  test("bài 2: sắp xếp không cần import"):
    val vs = List(Version(1, 10, 0), Version(1, 2, 3), Version(0, 9, 9))
    assertEquals(vs.sorted, List(Version(0, 9, 9), Version(1, 2, 3), Version(1, 10, 0)))
    assertEquals(vs.max, Version(1, 10, 0))

  test("bài 3"):
    assertEquals(List(Some(1), None, Some(4)).combineAll, Some(5))
    assertEquals(List[Option[Int]](None, None).combineAll, None)
    assertEquals(List((1, 10L), (2, 20L)).combineAll, (3, 30L))

  test("bài 4"):
    val xs = List(Signup("An", 20), Signup(" ", 10), Signup("Chi", 30), Signup("D", 5))
    assertEquals(validateAll(xs), Map(1 -> List("name rỗng", "tuổi 10 < 13"), 3 -> List("tuổi 5 < 13")))

  test("bài 5: gộp từng phần = gộp một lần"):
    val batches = List(List("a" -> 1, "b" -> 2, "a" -> 3), Nil, List("b" -> 5, "c" -> 1))
    val oneShot = batches.flatten.groupMapReduce(_._1)(_._2)(_ + _)
    assertEquals(aggregateByKey(batches), oneShot)
    assertEquals(aggregateByKey(batches), Map("a" -> 4, "b" -> 7, "c" -> 1))
