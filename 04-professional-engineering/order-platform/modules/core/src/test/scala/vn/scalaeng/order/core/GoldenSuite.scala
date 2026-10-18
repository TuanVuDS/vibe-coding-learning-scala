package vn.scalaeng.order.core

import munit.FunSuite
import scala.io.Source

/** Golden test: so sánh TOÀN BỘ output với file kết quả đã được người duyệt.
  * Khi output thay đổi có chủ đích: xem diff, duyệt, rồi cập nhật file expected trong cùng commit.
  */
class GoldenSuite extends FunSuite:
  private def resource(name: String): String =
    val src = Source.fromInputStream(getClass.getResourceAsStream(s"/golden/$name"), "UTF-8")
    try src.mkString finally src.close()

  private lazy val result = OrderPipeline.run(resource("input.csv").linesIterator.toList).toOption.get

  test("daily revenue khớp golden file"):
    assertNoDiff(Reports.dailyCsv(result.daily), resource("expected_daily_revenue.csv"))

  test("rejected khớp golden file"):
    assertNoDiff(Reports.rejectedCsv(result.rejected ++ result.duplicates), resource("expected_rejected.csv"))

  test("regression: header có BOM UTF-8 (file xuất từ Excel) vẫn được chấp nhận"):
    val withBom = "\uFEFF" + OrderPipeline.ExpectedHeader.mkString(",")
    assert(OrderPipeline.run(List(withBom, "ORD-1,A,1,1,2026-01-01")).isRight)
