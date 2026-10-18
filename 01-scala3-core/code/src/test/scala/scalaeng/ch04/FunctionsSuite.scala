package scalaeng.ch04

import munit.FunSuite

class FunctionsSuite extends FunSuite:
  import TextCleaning.*, HigherOrder.*, Recursion.*, ClickAnalytics.*

  test("normalizeName bỏ khoảng trắng thừa"):
    assertEquals(normalizeName("  Lê   Văn  C "), "Lê Văn C")

  test("andThen và compose cho cùng kết quả với email"):
    val raw = "  A@B.COM "
    assertEquals(normalizeEmail(raw), "a@b.com")
    assertEquals(normalizeEmailCompose(raw), normalizeEmail(raw))

  test("pipeline rỗng là identity"):
    assertEquals(pipeline(Nil)("  x "), "  x ")

  test("pipeline áp dụng các bước theo thứ tự"):
    val clean = pipeline(List(trim, lower, collapseSpaces))
    assertEquals(clean("  HELLO   WORLD "), "hello world")

  test("applyTwice và multiplier"):
    assertEquals(applyTwice(_ * 2, 5), 20)
    assertEquals(multiplier(4)(5), 20)

  test("discount với currying"):
    assertEquals(discount(BigDecimal("0.2"))(BigDecimal(100)), BigDecimal(80))

  test("paginate: boundary cases"):
    assertEquals(paginate(total = 0), "page 1/0 (size=20, total=0)")
    assertEquals(paginate(total = 20), "page 1/1 (size=20, total=20)")
    assertEquals(paginate(total = 21), "page 1/2 (size=20, total=21)")

  test("timed dùng đồng hồ giả để test được thời gian"):
    val ticks = Iterator(100L, 175L)
    val (result, elapsed) = timed(() => ticks.next())(21 * 2)
    assertEquals(result, 42)
    assertEquals(elapsed, 75L)

  test("factorial"):
    assertEquals(factorial(0), BigInt(1))
    assertEquals(factorial(5), BigInt(120))

  test("totalDuration không tràn stack với list lớn"):
    val big = List.fill(100_000)(ClickEvent("u", "/p", 2))
    assertEquals(totalDuration(big), 200_000L)

  test("totalDuration và bản naive giống nhau trên list nhỏ"):
    val small = List(ClickEvent("a", "/x", 10), ClickEvent("b", "/y", 5))
    assertEquals(totalDuration(small), totalDurationNaive(small))

  test("durationByPage gộp theo trang"):
    val events = List(
      ClickEvent("u1", "/a", 10),
      ClickEvent("u2", "/a", 5),
      ClickEvent("u3", "/b", 7)
    )
    assertEquals(durationByPage(events), Map("/a" -> 15L, "/b" -> 7L))
    assertEquals(longClicks(events, 7).map(_.userId), List("u1", "u3"))
