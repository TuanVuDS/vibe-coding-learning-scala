package vn.scalaeng.platform

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class PlatformSuite extends munit.FunSuite:
  private val (d15, d16, d17) = (LocalDate.parse("2026-10-15"), LocalDate.parse("2026-10-16"), LocalDate.parse("2026-10-17"))
  private val batch17 = Paths.get("data/raw/orders/ingest_date=2026-10-17")

  /** Bản sao dữ liệu mẫu; `withBatch17 = false` mô phỏng thời điểm lô 17 chưa đến. */
  private def fresh(withBatch17: Boolean = true): Platform =
    val root = Files.createTempDirectory("ch31-test")
    ArchitectureDemo.copyTree(Paths.get("data"), root, f => !withBatch17 && f.startsWith(batch17))
    Platform(root)
  private def daily(p: Platform, d: LocalDate): Vector[String] = Files2.lines(p.partitionFile("serving", "daily_revenue", d)).drop(1)
  private def actions(steps: Vector[Orchestrator.Step]): Vector[String] = steps.map(s => s"${s.day} ${s.asset} ${s.action}")

  test("golden tính tay sau 3 lô: hoàn tiền muộn làm ngày 16 rỗng, đơn ngày 17 vào đúng ngày") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    assertEquals(daily(p, d15), Vector("Retail,700000,2,2", "UNKNOWN,450000,1,1", "Wholesale,20000000,1,1"))
    assertEquals(daily(p, d16), Vector.empty)
    assertEquals(daily(p, d17), Vector("Retail,1890000,2,2"))
  }

  test("idempotent: chạy lại khi không có gì đổi → mọi bước skip, output byte-for-byte giữ nguyên") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "a")
    val before = Files.readAllBytes(p.partitionFile("serving", "daily_revenue", d15)).toSeq
    assert(Orchestrator.backfill(p, Assets.all, d15, d17, "b").forall(_.action == "skip"))
    assertEquals(Files.readAllBytes(p.partitionFile("serving", "daily_revenue", d15)).toSeq, before)
  }

  test("dữ liệu đến muộn: lô 17 chỉ làm cũ các partition nó CHẠM (16, 17); ngày 15 không bị tính lại") {
    val p = fresh(withBatch17 = false)
    Orchestrator.backfill(p, Assets.all, d15, d16, "a")
    assertEquals(daily(p, d16), Vector("Wholesale,750000,1,1"))
    ArchitectureDemo.copyTree(Paths.get("data/raw/orders/ingest_date=2026-10-17"), p.root.resolve("raw/orders/ingest_date=2026-10-17"))
    assertEquals(actions(Orchestrator.backfill(p, Assets.all, d15, d17, "b")), Vector(
      "2026-10-15 staging_orders skip", "2026-10-15 daily_revenue skip",
      "2026-10-16 staging_orders materialize", "2026-10-16 daily_revenue materialize",
      "2026-10-17 staging_orders materialize", "2026-10-17 daily_revenue materialize"))
    assertEquals(daily(p, d16), Vector.empty)
  }

  test("impact analysis: một file raw → mọi partition phụ thuộc, bắc cầu qua downstream") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    assertEquals(Orchestrator.impact(p, "raw/orders/ingest_date=2026-10-15/orders.csv"), Vector("daily_revenue@2026-10-15", "staging_orders@2026-10-15"))
    assertEquals(Orchestrator.impact(p, "customers.csv"), Vector("daily_revenue@2026-10-15", "daily_revenue@2026-10-16", "daily_revenue@2026-10-17"))
  }

  test("reprocessing: tăng codeVersion của daily_revenue → mọi partition daily cũ, staging không bị đụng") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "v1")
    val v2 = new Asset:
      val name = DailyRevenue.name; val layer = DailyRevenue.layer; val codeVersion = "v2"; val upstream = DailyRevenue.upstream
      def sources(pl: Platform, d: LocalDate) = DailyRevenue.sources(pl, d)
      def compute(d: LocalDate, in: List[InputFile]) = DailyRevenue.compute(d, in)
    assertEquals(Orchestrator.status(p, v2, d15), Status.Stale(List("code v1 → v2")))
    val steps = Orchestrator.backfill(p, List(StagingOrders, v2), d15, d17, "v2")
    assertEquals(steps.filter(_.action == "materialize").map(_.asset).distinct, Vector("daily_revenue"))
  }

  test("dimension đổi (customers.csv): chỉ daily_revenue cũ; output trên đĩa bị sửa tay cũng bị phát hiện") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    Files.write(p.root.resolve("customers.csv"), "C9,Retail,Đà Nẵng\n".getBytes(UTF_8), StandardOpenOption.APPEND)
    assertEquals(Orchestrator.status(p, DailyRevenue, d15), Status.Stale(List("input đổi customers.csv")))
    assertEquals(Orchestrator.status(p, StagingOrders, d15), Status.Fresh)
    Orchestrator.backfill(p, Assets.all, d15, d17, "r2")
    assertEquals(daily(p, d15), Vector("Retail,1150000,3,3", "Wholesale,20000000,1,1")) // C9 giờ là Retail
    Files.write(p.partitionFile("serving", "daily_revenue", d17), "segment,revenue_vnd,order_lines,orders\nRetail,1,1,1\n".getBytes(UTF_8))
    assertEquals(Orchestrator.status(p, DailyRevenue, d17), Status.Stale(List("output trên đĩa khác lần tính gần nhất")))
  }

  private val late18 = s"${RawOrders.Header}\nO99,C1,2026-10-15,PAID,KB-01,1,1,2026-10-18T00:00:00Z\n"
  private def addLate18(p: Platform): Unit =
    Files.write(Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-18")).resolve("orders.csv"), late18.getBytes(UTF_8)): Unit

  test("snapshot: lô raw đến TRONG lúc tính không lọt vào output lẫn lineage; lần status sau báo input mới") {
    val p = fresh()
    val racy = new Asset:
      val name = "racy_staging"; val layer = "staging"; val codeVersion = "v1"; val upstream = Nil
      def sources(pl: Platform, d: LocalDate) = StagingOrders.sources(pl, d)
      override def relevant(d: LocalDate, f: List[InputFile]) = StagingOrders.relevant(d, f)
      def compute(d: LocalDate, in: List[InputFile]) = { addLate18(p); StagingOrders.compute(d, in) }
    val m = Orchestrator.materialize(p, racy, d15, "r")
    assert(!Files2.lines(p.partitionFile("staging", "racy_staging", d15)).exists(_.startsWith("O99,")))
    assert(!m.inputs.exists(_.ref.contains("2026-10-18")), m.inputs)
    assertEquals(Orchestrator.status(p, racy, d15), Status.Stale(List("input mới raw/orders/ingest_date=2026-10-18/orders.csv")))
  }

  test("snapshot: dimension đổi rồi đổi lại (A → B → A) trong lúc tính — output vẫn tính từ A, nên fresh là ĐÚNG") {
    val p = fresh()
    Orchestrator.backfill(p, List(StagingOrders), d15, d15, "s")
    val customers = p.root.resolve("customers.csv")
    val original = Files.readAllBytes(customers)
    val racy = new Asset:
      val name = "racy_revenue"; val layer = "serving"; val codeVersion = "v1"; val upstream = DailyRevenue.upstream
      def sources(pl: Platform, d: LocalDate) = DailyRevenue.sources(pl, d)
      def compute(d: LocalDate, in: List[InputFile]) =
        Files.write(customers, "C9,Retail,Đà Nẵng\n".getBytes(UTF_8), StandardOpenOption.APPEND) // B: C9 thành Retail
        val out = DailyRevenue.compute(d, in)
        Files.write(customers, original)                                                         // trở lại A
        out
    Orchestrator.materialize(p, racy, d15, "r")
    assertEquals(Files2.lines(p.partitionFile("serving", "racy_revenue", d15)).drop(1),
      Vector("Retail,700000,2,2", "UNKNOWN,450000,1,1", "Wholesale,20000000,1,1")) // C9 vẫn UNKNOWN như trong A
    assertEquals(Orchestrator.status(p, racy, d15), Status.Fresh)
  }

  test("freshness tận nguồn: raw mới chạm ngày 15, staging chưa tính lại → daily@15 khớp cục bộ nhưng KHÔNG fresh; CLI status báo đúng") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    addLate18(p)
    val why = "stale: input mới raw/orders/ingest_date=2026-10-18/orders.csv"
    assertEquals(Orchestrator.status(p, DailyRevenue, d15), Status.Fresh)
    assertEquals(Orchestrator.freshness(p, Assets.all, DailyRevenue, d15), Status.Stale(List(s"upstream staging_orders@2026-10-15 $why")))
    val out = ArrayBuffer.empty[String]
    assertEquals(Main.run(Seq("status", s"--root=${p.root}", "--from=2026-10-15", "--to=2026-10-15"), out += _), 0)
    assertEquals(out.toList, List(s"2026-10-15 staging_orders: $why", s"2026-10-15 daily_revenue: stale: upstream staging_orders@2026-10-15 $why"))
    Orchestrator.backfill(p, Assets.all, d15, d15, "r2")
    assertEquals(Orchestrator.freshness(p, Assets.all, DailyRevenue, d15), Status.Fresh)
  }

  test("impact theo PHIÊN BẢN: staging tính lại không còn dùng file raw, serving chưa tính lại vẫn chứa đóng góp của nó → vẫn bị liệt kê") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    val raw15 = "raw/orders/ingest_date=2026-10-15/orders.csv"
    Files.move(p.root.resolve(raw15), Files.createDirectories(p.root.resolve("raw_quarantine")).resolve("orders-15.csv"))
    Orchestrator.materialize(p, StagingOrders, d15, "only-staging")
    assertEquals(Orchestrator.impact(p, raw15), Vector("daily_revenue@2026-10-15"))
    // Lô muộn chỉ làm staging tính lại: impact của nó KHÔNG gồm serving — serving hiện hành chưa từng đọc nó.
    addLate18(p)
    Orchestrator.materialize(p, StagingOrders, d15, "only-staging-2")
    assertEquals(Orchestrator.impact(p, "raw/orders/ingest_date=2026-10-18/orders.csv"), Vector("staging_orders@2026-10-15"))
  }

  test("tích hợp batch + streaming: speed layer cộng PAID khi đến lệch batch — trùng, bản cũ bị thay, xung đột, hoàn tiền muộn") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    val speed = SpeedLayer.runningPaid(p.rawFiles("orders").flatMap(f => Files2.lines(f).drop(1)))
    val batch = Seq(d15, d16, d17).map(d => d -> SpeedLayer.batchPaid(p, d)).toMap
    assertEquals(SpeedLayer.reconcile(speed, batch), List(
      "2026-10-15: speed=42050000 batch=21150000 lệch=20900000", "2026-10-16: speed=1740000 batch=0 lệch=1740000"))
  }

  test("CLI: sai tham số → mã 2; materialize in một dòng lineage JSON; status đọc được") {
    val p = fresh()
    val out = ArrayBuffer.empty[String]
    assertEquals(Main.run(Seq("materialize", s"--root=${p.root}"), out += _), 2)
    assertEquals(Main.run(Seq("materialize", s"--root=${p.root}", "--asset=staging_orders", "--day=2026-10-17", "--run-id=cli"), out += _), 0)
    assert(out.last.startsWith("""{"asset":"staging_orders","day":"2026-10-17","codeVersion":"v1","inputs":[{"kind":"raw","ref":"raw/orders/ingest_date=2026-10-17/orders.csv""""),
      out.last)
    out.clear()
    assertEquals(Main.run(Seq("status", s"--root=${p.root}", "--from=2026-10-17", "--to=2026-10-17"), out += _), 0)
    assertEquals(out.toList, List("2026-10-17 staging_orders: fresh", "2026-10-17 daily_revenue: missing"))
  }

  private def touch(p: Path): Unit = { Files.write(p, Array.emptyByteArray); () }
  test("R2-MAJOR-03: cửa sổ chết upstream bị chặn; lineage None cũ làm impact fail-closed và backfill sửa được") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d15, "initial")
    val oldDaily = Lineage.latest(p, DailyRevenue.name, d15).get
    val output = p.partitionFile("serving", DailyRevenue.name, d15)
    val before = Files.readAllBytes(output).toSeq
    val history = Files.readAllBytes(p.lineageFile).toSeq
    addLate18(p)
    // Mô phỏng chết sau rename staging, trước append lineage.
    val stage = StagingOrders.compute(d15, Orchestrator.snapshot(p, StagingOrders, d15))._1
    Files2.writeAtomically(p.partitionFile("staging", StagingOrders.name, d15), stage)
    intercept[IllegalArgumentException](Orchestrator.materialize(p, DailyRevenue, d15, "unrecorded-upstream"))
    assertEquals(Files.readAllBytes(output).toSeq, before)
    assertEquals(Files.readAllBytes(p.lineageFile).toSeq, history)
    Orchestrator.materialize(p, StagingOrders, d15, "repair-upstream")
    Orchestrator.materialize(p, DailyRevenue, d15, "valid")
    // Dữ liệu lineage từ phiên bản lab cũ có thể đã ghi None; không đoán ID từ hash.
    val good = Lineage.latest(p, DailyRevenue.name, d15).get
    val legacy = good.copy(id = "legacy-none", inputs = good.inputs.map(i =>
      if i.kind == "asset" then i.copy(materializationId = None) else i))
    Lineage.append(p, legacy)
    assert(Orchestrator.status(p, DailyRevenue, d15).isInstanceOf[Status.Stale])
    assert(intercept[IllegalArgumentException](Orchestrator.impact(p, "raw/orders/ingest_date=2026-10-18/orders.csv"))
      .getMessage.contains("impact không đầy đủ"))
    assertEquals(Orchestrator.backfill(p, Assets.all, d15, d15, "repair-legacy").map(_.action), Vector("skip", "materialize"))
    assertEquals(Orchestrator.impact(p, "raw/orders/ingest_date=2026-10-18/orders.csv"),
      Vector("daily_revenue@2026-10-15", "staging_orders@2026-10-15"))
    assertEquals(Orchestrator.freshness(p, Assets.all, DailyRevenue, d15), Status.Fresh)
    assertNotEquals(good.outputHash, oldDaily.outputHash)
  }

  test("R2-MINOR-01: ref ngày lỗi bị từ chối trước publish; freshness lineage cũ trả Stale có lý do") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d15, "initial")
    val broken = new Asset:
      val name = "broken"; val layer = "serving"; val codeVersion = "v1"; val upstream = List(StagingOrders.name)
      def sources(pl: Platform, d: LocalDate) = List(Source("asset", "staging_orders@not-a-date", pl.partitionFile("staging", StagingOrders.name, d)))
      def compute(d: LocalDate, in: List[InputFile]) = (Vector("header"), 0)
    intercept[IllegalArgumentException](Orchestrator.materialize(p, broken, d15, "bad-ref"))
    assert(!Files.exists(p.partitionFile(broken.layer, broken.name, d15)))
    val m = Lineage.latest(p, DailyRevenue.name, d15).get
    Lineage.append(p, m.copy(id = "legacy-bad-ref", inputs = m.inputs.map(i =>
      if i.kind == "asset" then i.copy(ref = "staging_orders@not-a-date") else i)))
    val result = Orchestrator.freshness(p, Assets.all, DailyRevenue, d15)
    assert(Orchestrator.describe(result).contains("tham chiếu không hợp lệ"))
  }
  test("R2-MAJOR-02: run B cùng partition bị từ chối khi A giữ snapshot; mỗi run hash đúng output riêng") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d15, "initial")
    val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
    val a = new Asset:
      val name = DailyRevenue.name; val layer = DailyRevenue.layer; val codeVersion = DailyRevenue.codeVersion
      val upstream = DailyRevenue.upstream
      def sources(pl: Platform, d: LocalDate) = DailyRevenue.sources(pl, d)
      def compute(d: LocalDate, in: List[InputFile]) =
        val dim = p.root.resolve("customers.csv")
        val original = Files.readAllBytes(dim)
        Files.write(dim, "C9,Retail,Đà Nẵng\n".getBytes(UTF_8), StandardOpenOption.APPEND)
        try
          val attempt = pool.submit(new java.util.concurrent.Callable[Throwable]:
            def call() = scala.util.Try(Orchestrator.materialize(p, DailyRevenue, d15, "B")).failed.get)
          val err = attempt.get(10, java.util.concurrent.TimeUnit.SECONDS)
          assert(err.isInstanceOf[IllegalStateException])
          assert(err.getMessage.contains("materialize khác"))
        finally { Files.write(dim, original); () }
        DailyRevenue.compute(d, in)
    try
      val m = Orchestrator.materialize(p, a, d15, "A")
      assertEquals(m.outputHash, Files2.hash(p.partitionFile("serving", "daily_revenue", d15)))
      assertEquals(daily(p, d15), Vector("Retail,700000,2,2", "UNKNOWN,450000,1,1", "Wholesale,20000000,1,1"))
      assertEquals(Orchestrator.freshness(p, Assets.all, DailyRevenue, d15), Status.Fresh)
      assert(!Lineage.all(p).exists(_.runId == "B"))
      Orchestrator.materialize(p, DailyRevenue, d15, "B-retry") // khóa đã được nhả.
    finally { pool.shutdownNow(); () }
  }
  test("R2-MAJOR-01: lỗi đọc dimension không được publish hoặc ghi lineage; giám sát không báo fresh") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d15, "initial")
    val output = p.partitionFile("serving", "daily_revenue", d15)
    val before = Files.readAllBytes(output).toSeq
    val history = Files.readAllBytes(p.lineageFile).toSeq
    val dim = p.root.resolve("customers.csv")
    Files.move(dim, p.root.resolve("customers.backup"))
    Files.createDirectory(dim) // readAllBytes thật ném IOException trên Windows và Unix.
    intercept[java.io.IOException](Orchestrator.materialize(p, DailyRevenue, d15, "bad-read"))
    intercept[java.io.IOException](Orchestrator.freshness(p, Assets.all, DailyRevenue, d15))
    assertEquals(Files.readAllBytes(output).toSeq, before)
    assertEquals(Files.readAllBytes(p.lineageFile).toSeq, history)
    Files.delete(dim)
    intercept[java.nio.file.NoSuchFileException](Orchestrator.materialize(p, DailyRevenue, d15, "missing-required"))
    assertEquals(Files.readAllBytes(output).toSeq, before)
    assertEquals(Files.readAllBytes(p.lineageFile).toSeq, history)
  }
  test("raw là append-only: file raw mới rỗng/không chạm ngày nào không làm partition nào cũ") {
    val p = fresh()
    Orchestrator.backfill(p, Assets.all, d15, d17, "r")
    touch(Files.createDirectories(p.root.resolve("raw/orders/ingest_date=2026-10-19")).resolve("orders.csv"))
    assert(Orchestrator.backfill(p, Assets.all, d15, d17, "r2").forall(_.action == "skip"))
  }
