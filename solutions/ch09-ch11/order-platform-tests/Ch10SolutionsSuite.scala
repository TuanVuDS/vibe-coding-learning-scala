package vn.scalaeng.pipeline

import munit.FunSuite
import java.nio.file.Files
import java.nio.charset.StandardCharsets.UTF_8
import vn.scalaeng.app.FileSink
import vn.scalaeng.testkit.*
import vn.scalaeng.testkit.Fixtures.*

/** Lời giải Ch10 bài 1, 2, 3, 5. Đặt trong modules/pipeline/src/test/scala/vn/scalaeng/pipeline/
  * (bài 4 cần FileSink của module app nên đặt ở modules/app/src/test — xem Ch10ContractSuite bên dưới).
  */
class Ch10SolutionsSuite extends FunSuite:

  // Bài 1
  test("topCustomers: n lớn hơn số khách hàng và danh sách rỗng"):
    assertEquals(Transform.topCustomers(List(line("ORD-1", "C-1")), 10).size, 1)
    assertEquals(Transform.topCustomers(Nil, 3), Nil)

  // Bài 2
  test("property: topCustomers có tối đa n phần tử và giảm dần"):
    val gen = for
      ls <- Gen.listOf(genLine)
      n  <- Gen.choose(1, 6)
    yield (ls, n)
    val failure = Prop.check(gen) { (ls, n) =>
      val top = Transform.topCustomers(ls, n).map(_._2.dong)
      top.size <= n && top == top.sorted(using Ordering.Long.reverse)
    }
    assertEquals(failure, None)

  // Bài 3
  test("property: validRows = số order_id khác nhau"):
    assertEquals(Prop.check(Gen.listOf(genLine))(ls => Report.build(Nil, ls, 3).validRows == ls.map(_.orderId).distinct.size), None)

  test("generator thật sự sinh dữ liệu có trùng (property bài 3 không vô nghĩa)"):
    val rnd = scala.util.Random(42L)
    val samples = List.fill(200)(Gen.listOf(genLine).sample(rnd))
    val withDup = samples.count(ls => ls.map(_.orderId).distinct.size < ls.size)
    assert(withDup > 100, s"chỉ $withDup/200 mẫu có trùng")

  // Bài 5
  test("golden: file chỉ có header"):
    val sink = InMemorySink()
    val result = Runner(InMemorySource(List(OrderCsv.Header.mkString(","))), sink, maxErrorRate = 0.0).run()
    assertEquals(result, Right(Summary(0, 0, 0, 0, 0.0)))
    assertEquals(sink.written("revenue_by_date.csv"), "date,revenue_vnd\n")
    assertEquals(sink.written("rejected.csv"), "line_no,error_code,message\n")
