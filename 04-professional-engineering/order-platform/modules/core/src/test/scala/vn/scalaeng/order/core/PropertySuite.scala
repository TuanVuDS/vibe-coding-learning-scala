package vn.scalaeng.order.core

import munit.FunSuite
import vn.scalaeng.order.testkit.{Gen, Prop}

class PropertySuite extends FunSuite:
  // Ký tự "nguy hiểm" cho CSV được chọn có chủ đích; '\n' bị loại vì CsvLine không hỗ trợ trường nhiều dòng (giới hạn đã ghi).
  private val trickyChar: Gen[Char] = Gen.oneOf('a', 'Z', 'đ', 'ệ', '1', ' ', ',', '"', ';')
  private val field: Gen[String] = Gen.stringOf(6, trickyChar)
  private val fields: Gen[List[String]] = Gen.choose(1, 6).flatMap(Gen.listOfN(_, field))

  test("round-trip: split(join(escape(fields))) == fields"):
    val failure = Prop.forAll(fields, runs = 500) { fs =>
      CsvLine.split(fs.map(CsvLine.escape).mkString(",")) == Right(fs)
    }
    assertEquals(failure, None)

  test("một property SAI bị phát hiện, kèm seed để tái hiện"):
    // Giả định ngây thơ: số cột = số dấu phẩy + 1. Sai khi có dấu phẩy trong dấu nháy kép.
    val failure = Prop.forAll(fields) { fs =>
      val line = fs.map(CsvLine.escape).mkString(",")
      CsvLine.split(line).map(_.size) == Right(line.count(_ == ',') + 1)
    }
    assert(failure.isDefined)
    assertEquals(failure.map(_.seed), Some(42L))

  // Sinh dòng dữ liệu hợp lệ, dòng lỗi và dòng trùng
  private val validRow: Gen[String] = for
    id    <- Gen.choose(1, 30)
    price <- Gen.choose(1, 500_000)
    qty   <- Gen.choose(1, 5)
    day   <- Gen.choose(1, 28)
  yield f"ORD-$id,SKU-$id,$price,$qty,2026-02-$day%02d"
  private val brokenRow: Gen[String] = Gen.oneOf("ORD-1,,1,1,2026-01-01", "ORD-x,A,1,1,2026-01-01", "ORD-2,A,-5,1,2026-01-01", "a,b", "\"open")
  private val rows: Gen[List[String]] = Gen.listOf(40, Gen.frequency(7 -> validRow, 3 -> brokenRow))
  private val header = OrderPipeline.ExpectedHeader.mkString(",")

  test("bất biến: input = valid + rejected + duplicates với mọi dữ liệu"):
    assertEquals(Prop.forAll(rows) { rs => OrderPipeline.run(header :: rs).exists(_.isBalanced) }, None)

  test("bất biến: không còn order_id trùng sau dedup"):
    assertEquals(Prop.forAll(rows) { rs =>
      OrderPipeline.run(header :: rs).exists(r => r.valid.map(_.orderId).distinct.size == r.valid.size)
    }, None)

  test("bất biến: tổng doanh thu theo ngày = tổng doanh thu các dòng hợp lệ"):
    assertEquals(Prop.forAll(rows) { rs =>
      OrderPipeline.run(header :: rs).exists(r => r.daily.map(_.revenue).sum == r.valid.map(_.total.dong).sum)
    }, None)

  test("chạy lại cho cùng kết quả (deterministic)"):
    assertEquals(Prop.forAll(rows, runs = 50) { rs => OrderPipeline.run(header :: rs) == OrderPipeline.run(header :: rs) }, None)
