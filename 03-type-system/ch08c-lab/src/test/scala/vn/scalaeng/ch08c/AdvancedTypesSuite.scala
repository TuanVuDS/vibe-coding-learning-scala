package vn.scalaeng.ch08c

import java.time.LocalDate
import scala.compiletime.testing.typeCheckErrors

final class AdvancedTypesSuite extends munit.FunSuite:
  test("inline if, inline val, inline match và constValue") {
    assertEquals(InlineRules.positiveBatchSize(InlineRules.defaultBatchSize), 100)
    assertEquals(InlineRules.formatName("csv"), "text/csv")
    assertEquals(InlineRules.formatName("json"), "application/json")
    assertEquals(InlineRules.partitionCount[4], 4)
  }

  test("tham số thường chỉ đánh giá một lần, inline parameter có thể lặp biểu thức") {
    var calls = 0
    def next(): Int = { calls += 1; calls }
    assertEquals(InlineRules.twice(next()), 2)
    assertEquals(calls, 1)
    calls = 0
    assertEquals(InlineRules.twiceInline(next()), 3)
    assertEquals(calls, 2)
  }

  test("compiletime.error chặn batch không dương và format không hỗ trợ") {
    val batchErrors = typeCheckErrors("vn.scalaeng.ch08c.InlineRules.positiveBatchSize(0)")
    assert(batchErrors.exists(_.message.contains("batch size must be positive")))
    val formatErrors = typeCheckErrors("vn.scalaeng.ch08c.InlineRules.formatName(\"xml\")")
    assert(formatErrors.exists(_.message.contains("format must be csv or json")))
  }

  test("inline if không chấp nhận giá trị runtime; constValue cần singleton") {
    assert(typeCheckErrors("def f(n: Int) = vn.scalaeng.ch08c.InlineRules.positiveBatchSize(n)").nonEmpty)
    assert(typeCheckErrors("vn.scalaeng.ch08c.InlineRules.partitionCount[Int]").nonEmpty)
  }

  test("match type reduce khi biết kiểu cụ thể") {
    summon[TypeTools.Payload[Option[String]] =:= String]
    summon[TypeTools.Payload[List[Int]] =:= Int]
    summon[TypeTools.Payload[Long] =:= Long]
    val payload: TypeTools.Payload[Option[String]] = "ORD-1"
    assertEquals(payload, "ORD-1")
  }

  test("match type còn abstract không đủ để gán A vào Payload[A]") {
    val errors = typeCheckErrors("def broken[A](a: A): vn.scalaeng.ch08c.TypeTools.Payload[A] = a")
    assert(errors.exists(_.message.contains("Payload[A]")), errors.map(_.message).mkString("\n"))
  }

  test("tuple labels, EmptyTuple và Tuple.Map giữ kiểu từng cột") {
    assertEquals(TypeTools.labels[("orderId", "total")], List("orderId", "total"))
    assertEquals(TypeTools.labels[EmptyTuple], Nil)
    val row: (Option[String], Option[Int]) = TypeTools.optionalRow(("ORD-1", 2))
    assertEquals(row, (Some("ORD-1"), Some(2)))
    assertEquals(TypeTools.optionalRow(EmptyTuple), EmptyTuple)
  }

  test("Mirror giữ thứ tự trường, BigDecimal plain và Option") {
    val encoder = CsvEncoder[OrderReport]
    assertEquals(encoder.header, List("orderId", "customer", "total", "paidOn"))
    val report = OrderReport("ORD-1", "An", BigDecimal("1E+3"), Some(LocalDate.of(2026, 10, 1)))
    assertEquals(encoder.encode(report), List("ORD-1", "An", "1000", "2026-10-01"))
    assertEquals(encoder.encode(report.copy(paidOn = None)).last, "")
  }

  test("CSV escape dấu phẩy, quote, LF, CR và dòng rỗng") {
    assertEquals(CsvWriter.escape("a,b"), "\"a,b\"")
    assertEquals(CsvWriter.escape("a\"b"), "\"a\"\"b\"")
    assertEquals(CsvWriter.escape("a\nb"), "\"a\nb\"")
    assertEquals(CsvWriter.escape("a\rb"), "\"a\rb\"")
    assertEquals(CsvWriter.escape(""), "")
    assertEquals(CsvWriter.write(List.empty[OrderReport]), "orderId,customer,total,paidOn")
    assertEquals(CsvWriter.write(List(OrderReport("ORD-2", "ABC, HN", BigDecimal(2), None))),
      "orderId,customer,total,paidOn\r\nORD-2,\"ABC, HN\",2,")
  }

  test("derivation báo thiếu CellEncoder thay vì âm thầm toString trường lạ") {
    val errors = typeCheckErrors("""
      case class Unsupported(value: java.net.URI) derives vn.scalaeng.ch08c.CsvEncoder
    """)
    assert(errors.exists(_.message.contains("CellEncoder[java.net.URI]")))
  }

  test("macro chấp nhận constant hợp lệ và từ chối literal sai / runtime input") {
    assertEquals(TopicLiteral.checked("orders.created"), "orders.created")
    val invalid = typeCheckErrors("vn.scalaeng.ch08c.TopicLiteral.checked(\"bad topic\")")
    assert(invalid.exists(_.message.contains("invalid topic literal")))
    val dynamic = typeCheckErrors("def f(s: String) = vn.scalaeng.ch08c.TopicLiteral.checked(s)")
    assert(dynamic.exists(_.message.contains("topic must be a constant string")))
  }

  test("runtime parser chia sẻ chính sách; kiểm tra biên độ dài và ASCII") {
    List("orders.created", "ORDERS_01-v2", "a" * 249).foreach { value =>
      assertEquals(TopicLiteral.parse(value), Right(value))
    }
    List("", ".", "..", "a" * 250, "đơn-hàng", "bad topic", "a/b").foreach { value =>
      assertEquals(TopicLiteral.parse(value), Left("invalid topic name"))
    }
  }
