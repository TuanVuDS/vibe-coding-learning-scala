package vn.scalaeng.transform

import cats.Monoid
import cats.syntax.all.*
import fs2.Stream
import java.time.LocalDate
import munit.ScalaCheckSuite
import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop.forAll

/** Test THUỘC TÍNH với ScalaCheck: sinh nhiều đầu vào (mặc định 100 lượt mỗi property, trong MIỀN của generator), kiểm tra một QUAN HỆ.
  * Đây là kiểm thử trên nhiều mẫu, không phải chứng minh. Khi thất bại, ScalaCheck CỐ thu nhỏ (shrink) đầu vào nếu kiểu có shrinker.
  */
class PropertySuite extends ScalaCheckSuite:
  // Gồm cả giá trị BIÊN (Long.MinValue/MaxValue) — bản đầu chỉ sinh ±1.000.000 nên không thấy lỗi tràn số (review chéo, M3).
  private val genStats: Gen[Stats] =
    Gen.frequency(4 -> Gen.choose(-1_000_000L, 1_000_000L), 1 -> Gen.oneOf(Long.MinValue, Long.MaxValue, -1L, 0L, 1L)).map(Stats.of)
  private val genStatsOrEmpty      = Gen.frequency(1 -> Gen.const(Monoid[Stats].empty), 5 -> genStats)

  property("Monoid[Stats]: kết hợp — (a |+| b) |+| c == a |+| (b |+| c)"):
    forAll(genStatsOrEmpty, genStatsOrEmpty, genStatsOrEmpty)((a, b, c) => ((a |+| b) |+| c) == (a |+| (b |+| c)))

  property("Monoid[Stats]: giao hoán — a |+| b == b |+| a (điều kiện cho partition không thứ tự)"):
    forAll(genStatsOrEmpty, genStatsOrEmpty)((a, b) => (a |+| b) == (b |+| a))

  property("Monoid[Stats]: phần tử trung hòa — empty |+| a == a == a |+| empty"):
    forAll(genStatsOrEmpty)(a => (Monoid[Stats].empty |+| a) == a && (a |+| Monoid[Stats].empty) == a)

  private val genOrder: Gen[Order] =
    for
      id   <- Gen.choose(1, 50).map(n => s"O$n")
      cust <- Gen.oneOf("C1", "C2", "C3")
      day  <- Gen.choose(0, 3).map(d => LocalDate.parse("2026-10-14").plusDays(d.toLong))
      st   <- Gen.oneOf(Status.values.toSeq)
      sku  <- Gen.oneOf("KB-01", "MS-01", "HP-01")
      qty  <- Gen.choose(1, 1000)
      p    <- Gen.choose(1L, 5_000_000L)
    yield Order(id, cust, day, st, sku, qty, p)

  private def key(o: Order)   = (o.customerId, o.day)
  private def value(o: Order) = Stats.of(o.revenueVnd)

  property("trong bộ nhớ == theo luồng (mọi kích thước khối) == chia n partition (mọi n, mọi cách chia)"):
    forAll(Gen.listOf(genOrder), Gen.choose(1, 64), Gen.choose(1, 8), Gen.long) { (orders, chunk, n, seed) =>
      val expected = Execution.inMemory(orders)(key)(value)
      Prop.all(
        Execution.streaming(Stream.emits(orders), chunk)(key)(value) == expected,
        Execution.partitioned(orders, n, seed)(key)(value) == expected
      )
    }

  property("phản ví dụ: trung bình của các trung bình KHÔNG khớp trung bình thật với một số cách chia"):
    // Prop.exists: chỉ cần TÌM ĐƯỢC một cách chia làm sai — đủ để kết luận phép gộp này không an toàn cho dữ liệu phân tán.
    Prop.exists(Gen.choose(1, 4)) { cut =>
      val data  = List(100L, 100L, 100L, 100L, 1000L)
      val parts = List(data.take(cut), data.drop(cut))
      Aggregate.averageOfAverages(parts) != data.map(Stats.of).combineAll.mean.get
    }
