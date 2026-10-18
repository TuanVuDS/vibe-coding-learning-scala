package vn.scalaeng.p1

import munit.FunSuite
import vn.scalaeng.p1.parse.OrderParser
import vn.scalaeng.p1.pipeline.Processor
import vn.scalaeng.p1.report.Reports

class ProcessorSuite extends FunSuite:
  private val header = OrderParser.Header.mkString(",")
  private val users = """[{"user_id":"U-1","name":"An","city":"HCM"},{"user_id":"U-2","name":"Bình","city":"HN"}]"""

  test("header sai, BOM, file rỗng"):
    assert(Processor.run(Nil, users).isLeft)
    assert(Processor.run(List("order_id,sku"), users).isLeft)
    assert(Processor.run(List("\uFEFF" + header), users).isRight)
    assert(Processor.run(List(header), "{").isLeft)

  test("dedup: giữ updated_at mới nhất; tie giữ dòng đầu và ghi lý do"):
    val r = Processor.run(List(header,
      "ORD-1,U-1,A,100,1,PAID,2026-01-01,2026-01-01T01:00:00Z",
      "ORD-1,U-1,A,100,5,PAID,2026-01-01,2026-01-01T09:00:00Z",
      "ORD-2,U-2,A,100,1,PAID,2026-01-01,2026-01-01T01:00:00Z",
      "ORD-2,U-2,A,100,2,PAID,2026-01-01,2026-01-01T01:00:00Z"), users).toOption.get
    assertEquals(r.report.map(x => (x.city.value, x.items)), List(("Hà Nội", 1), ("Hồ Chí Minh", 5)))
    assertEquals(r.rejected.map(x => (x.position, x.issues.head.message)), List(
      (2, "bị thay bởi dòng 3 (updated_at mới hơn)"),
      (5, "xung đột: cùng updated_at với dòng 4 nhưng khác nội dung, giữ dòng xuất hiện trước")))

  test("chỉ PAID tính doanh thu; user không tồn tại -> UNKNOWN"):
    val r = Processor.run(List(header,
      "ORD-1,U-1,A,100,1,CANCELLED,2026-01-01,2026-01-01T01:00:00Z",
      "ORD-2,U-9,A,100,2,PAID,2026-01-01,2026-01-01T01:00:00Z"), users).toOption.get
    assertEquals(r.report.map(x => (x.city.value, x.revenue.dong)), List(("UNKNOWN", 200L)))
    assertEquals((r.stats.cancelled, r.stats.orphanOrders), (1, 1))

  // ---- property-based
  private val row: Gen[String] = for
    id  <- Gen.choose(1, 15)
    u   <- Gen.oneOf("U-1", "U-2", "U-3")
    q   <- Gen.choose(1, 5)
    st  <- Gen.oneOf("PAID", "PAID", "CANCELLED", "REFUNDED")
    day <- Gen.choose(1, 3)
    hr  <- Gen.choose(0, 23)
    min <- Gen.choose(0, 59)
  yield f"ORD-$id,$u,SKU-$id,${id * 1000},$q,$st,2026-03-0$day,2026-03-0${day}T$hr%02d:$min%02d:00Z"
  private val broken = Gen.oneOf("ORD-x,U-1,A,1,1,PAID,2026-03-01,2026-03-01T00:00:00Z", "a,b", "\"open", "ORD-1,U-1,A,0,1,PAID,2026-03-01,2026-03-01T00:00:00Z")
  private val rows: Gen[List[String]] = Gen.listOf(30, Gen.choose(1, 10).flatMap(n => if n <= 8 then row else broken))

  test("bất biến cân bằng với mọi input"):
    assertEquals(Prop.forAll(rows)(rs => Processor.run(header :: rs, users).exists(_.stats.balanced)), None)

  test("bảo toàn: tổng doanh thu báo cáo = tổng PAID sau dedup"):
    assertEquals(Prop.forAll(rows)(rs => Processor.run(header :: rs, users).exists(r =>
      r.stats.paidRevenue == r.report.map(_.revenue.dong).sum && r.stats.paid == r.report.map(_.paidOrders).sum)), None)

  test("báo cáo KHÔNG phụ thuộc thứ tự dòng khi updated_at không trùng"):
    // mỗi order_id một updated_at duy nhất -> không có tie -> thứ tự không được ảnh hưởng kết quả
    val unique: Gen[List[String]] = Gen.listOf(25, row).map(_.zipWithIndex.map((l, i) =>
      l.replaceAll("T\\d\\d:\\d\\d:00Z$", f"T${i % 24}%02d:${i % 60}%02d:${i % 60}%02dZ")))
    val gen = unique.flatMap(rs => Gen.shuffle(rs).map(rs -> _))
    assertEquals(Prop.forAll(gen, runs = 150) { (a, b) =>
      Processor.run(header :: a, users).map(r => Reports.reportCsv(r.report)) ==
        Processor.run(header :: b, users).map(r => Reports.reportCsv(r.report))
    }, None)

  test("chạy lại cho cùng kết quả"):
    assertEquals(Prop.forAll(rows, runs = 50)(rs => Processor.run(header :: rs, users) == Processor.run(header :: rs, users)), None)
