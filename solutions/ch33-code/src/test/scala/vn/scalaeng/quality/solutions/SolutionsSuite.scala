package vn.scalaeng.quality.solutions

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import vn.scalaeng.quality.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate

class SolutionsSuite extends ScalaCheckSuite:
  private val lab = Paths.get("../../11-data-platform/quality-lab")
  private val raws = DataFiles.batches(lab.resolve("data/input")).map(DataFiles.toRaw.tupled)
  private val segs = Pipeline.segments(DataFiles.customers(lab.resolve("data/customers.csv"))).toOption.get
  private val out  = Pipeline.run(raws, segs)
  private def contract(v: Int) =
    Contract.parse(new String(Files.readAllBytes(lab.resolve(s"contracts/orders.v$v.json")), StandardCharsets.UTF_8)).toOption.get

  test("bài 1: 1/6 fact là UNKNOWN = 16,67% — qua ngưỡng 20%, đỏ ở ngưỡng 10%") {
    val unknown = (f: Fact) => f.segment == "UNKNOWN"
    assert(Solutions.maxShare[Fact]("unknown ≤ 20%", Severity.Warn, BigDecimal("0.20"))(unknown).run(out.facts).passed)
    val r = Solutions.maxShare[Fact]("unknown ≤ 10%", Severity.Warn, BigDecimal("0.10"))(unknown).run(out.facts)
    assertEquals((r.failed, r.samples), (1, List("1/6 = 16.67% > 10.00%")))
    assert(Solutions.maxShare[Fact]("rỗng", Severity.Warn, BigDecimal(0))(unknown).run(Nil).passed)
  }

  test("bài 2: accepted values bắt segment viết sai hoa thường ở dimension") {
    val allowed = Set("Retail", "Wholesale", "UNKNOWN")
    val check = Solutions.acceptedValues[Fact]("segment hợp lệ", Severity.Error, allowed)(_.segment)
    assert(check.run(out.facts).passed)
    val typo = out.facts.map(f => if f.segment == "Retail" then f.copy(segment = "retail") else f)
    // 3 fact Retail: O1/KB-01 (REFUNDED — fact giữ mọi trạng thái, chỉ daily lọc PAID), O1/MS-01, O8/KB-01.
    assertEquals((check.run(typo).failed, check.run(typo).samples), (3, List("retail", "retail", "retail")))
  }

  test("bài 3: bản ghi sinh từ contract luôn hợp lệ với chính contract đó (v1 và v2)") {
    for v <- List(1, 2); i <- 0 until 50 do
      assertEquals(Contract.validate(contract(v), Solutions.sampleRecord(contract(v), i)), Nil, s"v$v i=$i")
    // Bản ghi của v2 có trường `channel` → vi phạm v1: producer nâng cấp trước consumer là phát hiện được.
    assert(Contract.validate(contract(1), Solutions.sampleRecord(contract(2), 0)).exists(_.startsWith("channel: trường không có")))
  }

  test("bài 4: đối soát đạt trên dữ liệu mẫu (13 = 3 + 1 + 1 + 2 + 6); một ngăn bị mất là đỏ") {
    assert(Solutions.reconciliation(raws.size).run(Seq(out)).passed)
    val lost = out.copy(facts = out.facts.drop(1))
    assertEquals(Solutions.reconciliation(raws.size).run(Seq(lost)).samples, List("input=13, ngăn=3+1+1+2+5"))
  }

  test("bài 5: thêm 10 dòng hỏng (nguồn đổi định dạng) → tỷ lệ bị loại 23,08% → 56,52%") {
    val base = Solutions.rejectRate(out, raws.size)
    val broken = raws ++ (1 to 10).map(i => RawOrder("2026-10-17", s"B$i", "C1", "17/10/2026", "PAID", "KB-01", "1", "100", "2026-10-17T01:00:00Z"))
    val now = Solutions.rejectRate(Pipeline.run(broken, segs), broken.size)
    assertEquals((base, now), (BigDecimal("0.2308"), BigDecimal("0.5652")))
    assertEquals(Solutions.rejectDrift(base, now, BigDecimal("0.05")), Some("tỷ lệ bị loại tăng 23.0800% → 56.5200% (ngưỡng +5.00%)"))
    assertEquals(Solutions.rejectDrift(base, base, BigDecimal("0.05")), None)
  }

  private val genOrder: Gen[Order] =
    for
      id  <- Gen.choose(1, 4).map(n => s"O$n")
      st  <- Gen.oneOf(Status.values.toSeq)
      q   <- Gen.choose(1, 3)
      ts  <- Gen.oneOf("2026-10-15T01:00:00Z", "2026-10-16T09:00:00Z")
      b   <- Gen.oneOf("2026-10-15", "2026-10-16")
    yield Pipeline.validate(RawOrder(b, id, "C1", "2026-10-15", st.toString, "KB-01", q.toString, "100", ts)).toOption.get

  property("bài 6: Incremental.step idempotent — áp lại cùng lô (replay) không đổi trạng thái") {
    forAll(Gen.listOf(genOrder), Gen.listOf(genOrder)) { (history, batch) =>
      val s = Incremental.step(Map.empty, history)
      Incremental.step(Incremental.step(s, batch), batch) == Incremental.step(s, batch)
    }
  }

  test("bài 7: backfill ngày 15/10 sau khi hoàn tiền O1 đến muộn — ngày 16/10 không bị động tới") {
    // Bảng đang phục vụ được tính TRƯỚC khi lô 16/10 đến: chỉ có lô 15/10.
    val before  = Pipeline.run(raws.filter(_.batch == "2026-10-15"), segs).daily
    val full    = Pipeline.run(raws, segs).daily
    val d15     = LocalDate.parse("2026-10-15")
    val d16     = LocalDate.parse("2026-10-16")
    assertEquals(before.find(r => r.day == d15 && r.segment == "Retail").map(_.revenueVnd), Some(BigInt(1_150_000)))
    val serving = before ++ full.filter(_.day == d16) // chạy hằng ngày 16/10 chỉ ghi partition 16/10
    assertNotEquals(serving, full)                    // ngày 15/10 đang sai: O1/KB-01 đã hoàn tiền, O8 đến trễ
    assertEquals(Solutions.backfill(serving, full, Set(d15)), full)
    assertEquals(Solutions.backfill(serving, full, Set(d15)).filter(_.day == d16), serving.filter(_.day == d16))
  }
