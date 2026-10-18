package vn.scalaeng.order.core

import munit.FunSuite
import java.time.LocalDate

class CoreSuite extends FunSuite:
  private val header = "order_id,sku,unit_price,quantity,order_date"

  test("CsvLine.split theo RFC 4180"):
    assertEquals(CsvLine.split("a,b,c"), Right(List("a", "b", "c")))
    assertEquals(CsvLine.split("a,\"b,c\",d"), Right(List("a", "b,c", "d")))
    assertEquals(CsvLine.split("\"say \"\"hi\"\"\",x"), Right(List("say \"hi\"", "x")))
    assertEquals(CsvLine.split("a,,"), Right(List("a", "", "")))
    assert(CsvLine.split("\"open").isLeft)

  test("escape rồi split trả lại giá trị gốc (round-trip)"):
    val values = List("plain", "a,b", "q\"uote", "", "both,\"x\"")
    assertEquals(CsvLine.split(values.map(CsvLine.escape).mkString(",")), Right(values))

  test("run: header sai hoặc file rỗng"):
    assert(OrderPipeline.run(Nil).isLeft)
    assert(OrderPipeline.run(List("id,sku")).isLeft)

  test("run: cân bằng và doanh thu"):
    val lines = List(
      header,
      "ORD-1,A,100,2,2026-01-01",
      "ORD-2,B,x,1,2026-01-01",
      "",
      "ORD-1,A,100,2,2026-01-01",
      "ORD-3,\"C,D\",50,1,2026-01-02"
    )
    val r = OrderPipeline.run(lines).toOption.get
    assertEquals(r.inputRows, 4)
    assertEquals(r.valid.size, 2)
    assertEquals(r.rejected.map(_.lineNo), List(3))
    assertEquals(r.duplicates.map(_.lineNo), List(5))
    assert(r.isBalanced)
    assertEquals(r.daily.map(_.revenue), List(200L, 50L))
    assertEquals(r.daily.map(_.date), List(LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2)))

  test("rejectedCsv escape dữ liệu gốc"):
    val r = OrderPipeline.run(List(header, "ORD-9,\"X,Y\",abc,1,2026-01-01")).toOption.get
    assertEquals(Reports.rejectedCsv(r.rejected), "line_no,reason,raw\n2,đơn giá không phải số nguyên: 'abc',\"ORD-9,\"\"X,Y\"\",abc,1,2026-01-01\"\n")
