package vn.scalaeng.ch08c.solutions

import scala.compiletime.testing.typeCheckErrors
import vn.scalaeng.ch08c.{CsvEncoder, CsvWriter}

final class ExercisesSuite extends munit.FunSuite:
  test("BT1: bounded batch nhận cả hai biên") {
    assertEquals(BatchPolicy.bounded(1), 1)
    assertEquals(BatchPolicy.bounded(10000), 10000)
  }
  test("BT1: chặn ngoài biên và tham số runtime ngay lúc compile") {
    assert(typeCheckErrors("vn.scalaeng.ch08c.solutions.BatchPolicy.bounded(0)")
      .exists(_.message.contains("batch size must be in [1, 10000]")))
    assert(typeCheckErrors("vn.scalaeng.ch08c.solutions.BatchPolicy.bounded(10001)").nonEmpty)
    assert(typeCheckErrors("def f(n: Int) = vn.scalaeng.ch08c.solutions.BatchPolicy.bounded(n)").nonEmpty)
  }
  test("BT2: Leaf bóc nhiều lớp và giữ nguyên kiểu lá") {
    summon[TupleExercises.Leaf[List[Option[List[String]]]] =:= String]
    summon[TupleExercises.Leaf[Option[Int]] =:= Int]
    summon[TupleExercises.Leaf[Long] =:= Long]
    val value: TupleExercises.Leaf[List[Option[String]]] = "ORD-1"
    assertEquals(value, "ORD-1")
  }
  test("BT2: Leaf[A] không thể gán từ A khi A chưa cụ thể") {
    assert(typeCheckErrors("def f[A](a: A): vn.scalaeng.ch08c.solutions.TupleExercises.Leaf[A] = a").nonEmpty)
  }
  test("BT3: arity đệ quy qua EmptyTuple và cons *:") {
    assertEquals(TupleExercises.arity[EmptyTuple], 0)
    assertEquals(TupleExercises.arity[Tuple1[String]], 1)
    assertEquals(TupleExercises.arity[(String, Int, Boolean)], 3)
  }
  test("BT3: Tuple.Map bảo toàn từng kiểu khi bọc Either") {
    val row: (Either[String, String], Either[String, Int]) = TupleExercises.acceptedRow(("ORD-1", 2))
    assertEquals(row, (Right("ORD-1"), Right(2)))
    assertEquals(TupleExercises.acceptedRow(EmptyTuple), EmptyTuple)
    assert(typeCheckErrors("""
      val bad: (Either[String, Int], Either[String, String]) =
        vn.scalaeng.ch08c.solutions.TupleExercises.acceptedRow(("ORD-1", 2))
    """).nonEmpty)
  }
  test("BT4: EventReport derives CSV theo đúng thứ tự và encode các trường") {
    val encoder = CsvEncoder[EventReport]
    assertEquals(encoder.header, List("eventId", "attempts", "accepted", "note"))
    assertEquals(encoder.encode(EventReport("EV-1", 2, false, None)), List("EV-1", "2", "false", ""))
    assertEquals(CsvWriter.write(List(EventReport("EV-1", 2, true, Some("a,b")))),
      "eventId,attempts,accepted,note\r\nEV-1,2,true,\"a,b\"")
  }
  test("BT4: product rỗng và trường không được hỗ trợ") {
    assertEquals(CsvEncoder[EmptyReport].header, Nil)
    assertEquals(CsvEncoder[EmptyReport].encode(EmptyReport()), Nil)
    assert(typeCheckErrors("""
      case class Bad(payload: Map[String, Int]) derives vn.scalaeng.ch08c.CsvEncoder
    """).exists(_.message.contains("CellEncoder[Map[String, Int]]")))
  }
  test("BT5: Show derives nhãn kiểu và nhãn trường") {
    assertEquals(summon[Show[OrderSummary]].show(OrderSummary("ORD-1", 2)),
      "OrderSummary(orderId=ORD-1,quantity=2)")
    assertEquals(summon[Show[EmptyReport]].show(EmptyReport()), "EmptyReport()")
  }
  test("BT5: thiếu Show của trường phải lỗi compile") {
    assert(typeCheckErrors("""
      case class Bad(amount: BigDecimal) derives vn.scalaeng.ch08c.solutions.Show
    """).exists(_.message.contains("Show[BigDecimal]")))
  }
  test("BT6: macro percent nhận 0, 100 và chính sách runtime cùng biên") {
    assertEquals(PercentLiteral.checked(0), 0)
    assertEquals(PercentLiteral.checked(100), 100)
    assertEquals(PercentLiteral.checked(25), 25)
    List(0, 25, 100).foreach(n => assertEquals(PercentLiteral.parse(n), Right(n)))
    List(-1, 101).foreach(n => assertEquals(PercentLiteral.parse(n), Left("percent must be in [0, 100]")))
  }
  test("BT6: macro percent chặn literal sai và giá trị runtime") {
    assert(typeCheckErrors("vn.scalaeng.ch08c.solutions.PercentLiteral.checked(-1)")
      .exists(_.message.contains("percent literal must be in [0, 100]")))
    assert(typeCheckErrors("vn.scalaeng.ch08c.solutions.PercentLiteral.checked(101)").nonEmpty)
    assert(typeCheckErrors("def f(n: Int) = vn.scalaeng.ch08c.solutions.PercentLiteral.checked(n)")
      .exists(_.message.contains("percent must be a constant integer")))
  }
  test("mini project: export metadata và CSV; input rỗng vẫn có header") {
    val result = EventExport.render(List(EventReport("EV-1", 1, true, Some("ok, retry"))))
    assertEquals(result.topic, "orders.audit")
    assertEquals(result.batchSize, 100)
    assertEquals(result.csv, "eventId,attempts,accepted,note\r\nEV-1,1,true,\"ok, retry\"")
    assertEquals(EventExport.render(Nil).csv, "eventId,attempts,accepted,note")
  }
