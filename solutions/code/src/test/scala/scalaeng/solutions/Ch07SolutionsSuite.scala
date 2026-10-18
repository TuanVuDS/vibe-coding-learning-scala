package scalaeng.solutions

import munit.FunSuite
import java.time.LocalDate

class Ch07SolutionsSuite extends FunSuite:
  import Ch07Solutions.*

  test("bài 1"):
    assertEquals(parsePositiveInt(" 12 "), Some(12))
    assertEquals(parsePositiveInt("0"), None)
    assertEquals(parsePositiveInt("x"), None)

  test("bài 2"):
    val db = Map(
      "u1" -> User("u1", Some("m1"), None),
      "m1" -> User("m1", None, Some("boss@x.vn")),
      "u2" -> User("u2", Some("ghost"), None),
      "u3" -> User("u3", None, None)
    )
    assertEquals(managerEmail(db, "u1"), Some("boss@x.vn"))
    assertEquals(managerEmail(db, "u2"), None)
    assertEquals(managerEmail(db, "u3"), None)
    assertEquals(managerEmail(db, "nobody"), None)

  test("bài 3"):
    val today = LocalDate.of(2026, 10, 14)
    assertEquals(parseBirthDate("2000-01-31", today), Right(LocalDate.of(2000, 1, 31)))
    assertEquals(parseBirthDate("  ", today), Left(DateError.Empty))
    assertEquals(parseBirthDate("31/01/2000", today), Left(DateError.BadFormat("31/01/2000")))
    assertEquals(parseBirthDate("2026-10-15", today), Left(DateError.InFuture(LocalDate.of(2026, 10, 15))))
    assertEquals(parseBirthDate("2026-10-14", today), Right(today))

  test("bài 4: trả về TẤT CẢ lỗi"):
    import SignupError.*
    assertEquals(validateSignup(" An ", "An@X.vn", "20"), Right(Signup("An", "an@x.vn", 20)))
    assertEquals(validateSignup("", "bad", "abc"), Left(List(EmptyName, InvalidEmail("bad"), InvalidAge("abc"))))
    assertEquals(validateSignup("B", "b@x", "10"), Left(List(TooYoung(10))))
    assertEquals(validateSignup("C", "@x", "20"), Left(List(InvalidEmail("@x"))))

  test("bài 5"):
    import StockEvent.*, StockError.*
    assertEquals(applyAll(Map.empty, List(Received("A", 5), Reserved("A", 2), Released("A", 1))), Right(Map("A" -> 4)))
    assertEquals(
      applyAll(Map("A" -> 1), List(Reserved("A", 1), Reserved("A", 1), Received("A", -1))),
      Left(Insufficient(1, "A", 0, 1))
    )
    assertEquals(applyAll(Map.empty, List(Received("A", 0))), Left(NonPositiveQty(0)))
    assertEquals(applyAll(Map("A" -> 1), Nil), Right(Map("A" -> 1)))
