package vn.scalaeng.platform.solutions

import vn.scalaeng.platform.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.{Duration, Instant, LocalDate}

class SolutionsSuite extends munit.FunSuite:
  private val (d15, d16, d17) = (LocalDate.parse("2026-10-15"), LocalDate.parse("2026-10-16"), LocalDate.parse("2026-10-17"))
  private val lab = Paths.get("../../11-data-platform/architecture-lab/data")
  private def fresh(withBatch17: Boolean = true): Platform =
    val root = Files.createTempDirectory("ch31-sol")
    ArchitectureDemo.copyTree(lab, root, f => !withBatch17 && f.toString.contains("ingest_date=2026-10-17"))
    Platform(root)
  private def built(): Platform = { val p = fresh(); Orchestrator.backfill(p, Assets.all, d15, d17, "r"); p }

  test("bài 1: asset many-to-one — tổng theo segment cả 3 ngày; một ngày daily đổi làm nó cũ") {
    val p = built()
    val totals = Solutions.SegmentTotals(d15, d17)
    Orchestrator.materialize(p, totals, d17, "t")
    assertEquals(Files2.lines(p.partitionFile("serving", "segment_totals", d17)).drop(1), Vector("Retail,2590000", "UNKNOWN,450000", "Wholesale,20000000"))
    Files.write(p.root.resolve("customers.csv"), "C9,Retail,ĐN\n".getBytes(UTF_8), StandardOpenOption.APPEND)
    Orchestrator.backfill(p, Assets.all, d15, d17, "r2")
    assertEquals(Orchestrator.status(p, totals, d17), Status.Stale(List("input đổi daily_revenue@2026-10-15")))
  }

  test("bài 2: provenance daily_revenue@16 → hai file raw có ngày 16 + customers.csv") {
    assertEquals(Solutions.provenance(built(), "daily_revenue", d16),
      Vector("customers.csv", "raw/orders/ingest_date=2026-10-16/orders.csv", "raw/orders/ingest_date=2026-10-17/orders.csv"))
  }

  test("bài 2: provenance theo PHIÊN BẢN — staging tính lại riêng không đổi nguồn gốc của serving chưa tính lại") {
    val p = built()
    val before = Solutions.provenance(p, "daily_revenue", d15)
    // Lô muộn chạm ngày 15, CHỈ staging tính lại: serving@15 chưa từng đọc lô này.
    val late = Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-18")).resolve("late.csv")
    Files.write(late, s"${RawOrders.Header}\nO99,C1,2026-10-15,PAID,KB-01,1,1,2026-10-18T00:00:00Z\n".getBytes(UTF_8))
    Orchestrator.materialize(p, StagingOrders, d15, "only-staging")
    assertEquals(Solutions.provenance(p, "daily_revenue", d15), before)
    assert(Solutions.provenance(p, "staging_orders", d15).contains("raw/orders/ingest_date=2026-10-18/late.csv"))
    // Output upstream bị sửa tay: từ chối publish downstream trước khi phục hồi upstream.
    Files.write(p.partitionFile("staging", "staging_orders", d15), s"${StagingOrders.Header}\n".getBytes(UTF_8))
    intercept[IllegalArgumentException](Orchestrator.materialize(p, DailyRevenue, d15, "after-edit"))
    assertEquals(Solutions.provenance(p, "daily_revenue", d15), before)
  }

  test("bài 3: SLA — partition cũ, chưa tính và tính quá lâu đều bị liệt kê") {
    val p = fresh(withBatch17 = false)
    Orchestrator.backfill(p, Assets.all, d15, d16, "r")
    val now = Instant.now()
    assertEquals(Solutions.slaViolations(p, Assets.all, d15, d17, now, Duration.ofHours(24)),
      Vector("2026-10-17 staging_orders: chưa tính", "2026-10-17 daily_revenue: chưa tính"))
    assertEquals(Solutions.slaViolations(p, Assets.all, d15, d15, now.plus(Duration.ofHours(30)), Duration.ofHours(24)).size, 2)
    // Lô 17 đến (hoàn tiền của ngày 16), CHƯA backfill: serving@16 khớp input trực tiếp nhưng staging@16 đã cũ → vẫn là vi phạm SLA.
    ArchitectureDemo.copyTree(lab.resolve("raw/orders/ingest_date=2026-10-17"), p.root.resolve("raw/orders/ingest_date=2026-10-17"))
    assertEquals(Solutions.slaViolations(p, Assets.all, d16, d16, now, Duration.ofHours(24)), Vector(
      "2026-10-16 staging_orders: cũ: input mới raw/orders/ingest_date=2026-10-17/orders.csv",
      "2026-10-16 daily_revenue: cũ: upstream staging_orders@2026-10-16 stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv"))
  }

  test("bài 4: cửa sổ 1 ngày — chỉ tính ngày 17; hoàn tiền muộn của ngày 16 nằm NGOÀI cửa sổ được liệt kê, không bị bỏ qua âm thầm") {
    val p = fresh(withBatch17 = false)
    Orchestrator.backfill(p, Assets.all, d15, d16, "r")
    ArchitectureDemo.copyTree(lab.resolve("raw/orders/ingest_date=2026-10-17"), p.root.resolve("raw/orders/ingest_date=2026-10-17"))
    val w = Solutions.windowedBackfill(p, Assets.all, today = d17, windowDays = 1, earliest = d15, runId = "w")
    assertEquals(w.steps.map(s => s"${s.day} ${s.asset}"), Vector("2026-10-17 staging_orders", "2026-10-17 daily_revenue"))
    assertEquals(w.lateOutsideWindow, Vector("2026-10-16 staging_orders: input mới raw/orders/ingest_date=2026-10-17/orders.csv"))
  }

  test("bài 5: file raw sai header bị cách ly trước khi pipeline thấy; partition không bị ảnh hưởng") {
    val p = built()
    val bad = Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-18")).resolve("orders.csv")
    Files.write(bad, "customer_id,order_id,day\nC1,O50,2026-10-15\n".getBytes(UTF_8))
    assertEquals(Solutions.quarantineBadHeaders(p, "orders", RawOrders.Header), Vector("raw_quarantine/orders/ingest_date=2026-10-18/orders.csv"))
    assert(Orchestrator.backfill(p, Assets.all, d15, d17, "r2").forall(_.action == "skip"))
  }

  test("bài 6: kappa replay từ raw ra đúng serving; sửa tay một partition serving bị phát hiện") {
    val p = built()
    assertEquals(Solutions.replayMatches(p, Assets.all, d15, d17, Files.createTempDirectory("replay")), Vector.empty)
    Files.write(p.partitionFile("serving", "daily_revenue", d15), "segment,revenue_vnd,order_lines,orders\n".getBytes(UTF_8))
    assertEquals(Solutions.replayMatches(p, Assets.all, d15, d17, Files.createTempDirectory("replay")),
      Vector("2026-10-15 daily_revenue: serving khác bản tính lại từ raw"))
  }

  test("bài 7: dry-run phân loại — staging 16 cũ ⇒ daily 16 CÓ THỂ chạy (tùy hash output staging)") {
    val p = fresh(withBatch17 = false)
    Orchestrator.backfill(p, Assets.all, d15, d16, "r")
    ArchitectureDemo.copyTree(lab.resolve("raw/orders/ingest_date=2026-10-17"), p.root.resolve("raw/orders/ingest_date=2026-10-17"))
    assertEquals(Solutions.plan(p, Assets.all, d15, d17), Vector(
      "2026-10-16 staging_orders: chắc chạy — stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv",
      "2026-10-16 daily_revenue: có thể chạy — nếu output của staging_orders khác hash partition này đã dùng",
      "2026-10-17 staging_orders: chắc chạy — missing", "2026-10-17 daily_revenue: chắc chạy — missing"))
    assertEquals(Lineage.all(p).size, 4) // dry-run không ghi gì
  }

  test("bài 7: \"có thể chạy\" thật sự có thể KHÔNG chạy — input mới toàn dòng bị loại, staging tính lại ra byte y hệt, daily skip") {
    val p = built()
    val late = Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-18")).resolve("late.csv")
    Files.write(late, s"${RawOrders.Header}\nO98,C1,2026-10-15,PAID,KB-01,1,1,2026-10-18T00:00:00Z,cột-thứ-9\n".getBytes(UTF_8))
    assertEquals(Solutions.plan(p, Assets.all, d15, d15), Vector(
      "2026-10-15 staging_orders: chắc chạy — stale: input mới raw/orders/ingest_date=2026-10-18/late.csv",
      "2026-10-15 daily_revenue: có thể chạy — nếu output của staging_orders khác hash partition này đã dùng"))
    assertEquals(Orchestrator.backfill(p, Assets.all, d15, d15, "r2").map(s => s"${s.asset} ${s.action}"),
      Vector("staging_orders materialize", "daily_revenue skip"))
  }

  test("R2-MAJOR-04: downstream đang stale vẫn có điều kiện khi xung đột append-only khôi phục hash cũ") {
    val p = built()
    val original = Lineage.latest(p, StagingOrders.name, d15).get.outputHash
    val dir = Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-18"))
    val paid = "O99,C1,2026-10-15,PAID,KB-01,1,1,2026-10-18T00:00:00Z"
    Files.write(dir.resolve("paid.csv"), s"${RawOrders.Header}\n$paid\n".getBytes(UTF_8))
    Orchestrator.materialize(p, StagingOrders, d15, "only-staging")
    assert(Orchestrator.status(p, DailyRevenue, d15).isInstanceOf[Status.Stale])
    Files.write(dir.resolve("refund.csv"), s"${RawOrders.Header}\n${paid.replace(",PAID,", ",REFUNDED,")}\n".getBytes(UTF_8))
    val history = Lineage.all(p)
    val plan = Solutions.plan(p, Assets.all, d15, d15)
    assert(plan.last.contains("daily_revenue: có thể chạy"), plan)
    assert(plan.last.contains("khác hash partition này đã dùng"))
    assertEquals(Lineage.all(p), history)
    assertEquals(Orchestrator.backfill(p, Assets.all, d15, d15, "resolve-conflict").map(_.action), Vector("materialize", "skip"))
    assertEquals(Lineage.latest(p, StagingOrders.name, d15).get.outputHash, original)
  }
