package vn.scalaeng.quality

import munit.ScalaCheckSuite
import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop.forAll

/** Golden kiểm tra MỘT bộ dữ liệu; property test kiểm tra một QUY LUẬT trên nhiều bộ sinh ngẫu nhiên (mặc định 100 lượt mỗi property).
  * Bộ sinh cố ý dùng ít khóa và ít mốc thời gian để TRÙNG, HÒA (xung đột) và ĐẾN TRỄ xảy ra thường xuyên — nơi lỗi ẩn nấp.
  */
class PropertySuite extends ScalaCheckSuite:
  private val segs = Map("C1" -> "Retail", "C2" -> "Wholesale")
  private val times = Vector("2026-10-15T01:00:00Z", "2026-10-15T05:00:00Z", "2026-10-16T09:00:00Z")
  private val batches = Vector("2026-10-15", "2026-10-16", "2026-10-17")

  private val genRaw: Gen[RawOrder] =
    for
      id    <- Gen.choose(1, 6).map(n => s"O$n")
      sku   <- Gen.oneOf("KB-01", "MS-01")
      cust  <- Gen.oneOf("C1", "C2", "C9")
      day   <- Gen.oneOf("2026-10-15", "2026-10-16")
      st    <- Gen.frequency(5 -> "PAID", 2 -> "REFUNDED", 1 -> "CANCELLED", 1 -> "SHIPPED") // SHIPPED: dòng hỏng
      qty   <- Gen.frequency(9 -> Gen.choose(1, 5).map(_.toString), 1 -> Gen.const("0"))
      price <- Gen.oneOf("100", "250")
      ts    <- Gen.oneOf(times)
      b     <- Gen.oneOf(batches)
    yield RawOrder(b, id, cust, day, st, sku, qty, price, ts)

  private val genInput: Gen[Vector[RawOrder]] = Gen.listOf(genRaw).map(_.toVector)

  /** Phần output mà người đọc dùng — không gồm số đếm "bao nhiêu bản trùng" (cái đó ĐƯỢC PHÉP đổi khi thêm bản trùng). */
  private def served(o: Output) = (o.facts, o.daily, o.conflicts)

  property("thứ tự đến không ảnh hưởng: mọi hoán vị input cho cùng output (kể cả số đếm)") {
    forAll(genInput, Gen.long) { (xs, seed) =>
      Pipeline.run(new scala.util.Random(seed).shuffle(xs), segs) == Pipeline.run(xs, segs)
    }
  }

  property("chạy lại (rerun): đưa cùng input hai lần → output phục vụ y hệt") {
    forAll(genInput)(xs => served(Pipeline.run(xs ++ xs, segs)) == served(Pipeline.run(xs, segs)))
  }

  property("gửi lại ở lô SAU (duplicate events): output phục vụ không đổi, chỉ số bản trùng tăng") {
    forAll(genInput, Gen.choose(0, 10)) { (xs, k) =>
      val resent = xs.take(k).map(_.copy(batch = "2026-10-18"))
      val (a, b) = (Pipeline.run(xs, segs), Pipeline.run(xs ++ resent, segs))
      val validResent = resent.count(r => Pipeline.validate(r).isRight)
      Prop.all(served(b) == served(a), b.exactDuplicates == a.exactDuplicates + validResent)
    }
  }

  property("đến trễ: thêm bản CŨ HƠN mọi bản hiện có (updated_at 14/10, đến ở lô 18) của các dòng hàng hợp lệ → output phục vụ không đổi") {
    forAll(genInput) { xs =>
      val stale = xs.filter(r => Pipeline.validate(r).isRight)
        .map(r => r.copy(status = "CANCELLED", updatedAt = "2026-10-14T00:00:00Z", batch = "2026-10-18"))
      served(Pipeline.run(xs ++ stale, segs)) == served(Pipeline.run(xs, segs))
    }
  }

  property("backfill / replay: trạng thái tăng dần (gọn) qua nhiều cách chia lô và thứ tự lô sinh ngẫu nhiên ≡ tính lại toàn bộ (cả xung đột)") {
    forAll(genInput, Gen.choose(1, 5), Gen.long) { (xs, k, seed) =>
      val valid  = xs.flatMap(r => Pipeline.validate(r).toOption)
      val rnd    = new scala.util.Random(seed)
      val chunks = rnd.shuffle(valid.grouped(math.max(1, valid.size / k + 1)).toVector)
      val state  = chunks.foldLeft(Map.empty: Incremental.State)(Incremental.step)
      val full   = Pipeline.dedup(valid)
      Prop.collect(if full.conflicts.nonEmpty then "có xung đột" else "không xung đột")(
        Incremental.latest(state) == full.latest && Incremental.conflicts(state) == full.conflicts)
    }
  }

  property("bảo toàn: mọi dòng input vào ĐÚNG MỘT ngăn (rejected + trùng + thay thế + xung đột + fact)") {
    forAll(genInput) { xs =>
      val o = Pipeline.run(xs, segs)
      o.rejected.size + o.exactDuplicates + o.superseded + o.conflicts.size + o.facts.size == xs.size
    }
  }
