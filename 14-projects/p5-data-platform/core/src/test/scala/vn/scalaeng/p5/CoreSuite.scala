package vn.scalaeng.p5

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate

/** Lake dựng từ dữ liệu mẫu trong thư mục tạm — dùng chung cho test core và test e2e của app. */
object Fixture:
  val (d15, d16, d17) = (LocalDate.parse("2026-10-15"), LocalDate.parse("2026-10-16"), LocalDate.parse("2026-10-17"))
  val drop: Path = Paths.get("data/drop")
  def newLake(): Lake =
    val root = Files.createTempDirectory("p5-lake")
    Files.createDirectories(root.resolve("dimension"))
    Files.copy(Paths.get("data/dimension/customers.csv"), root.resolve("dimension/customers.csv"))
    Lake(root)
  /** Lô của ngày d (file thả vào lúc d) — như bên gửi thả hằng ngày. */
  def landDay(l: Lake, d: LocalDate): FileLanding.Landed = FileLanding.land(l, drop.resolve(s"orders-$d.jsonl"), d)

class CoreSuite extends munit.FunSuite:
  import Fixture.*
  private def daily(l: Lake, d: LocalDate) = Io.lines(l.partFile(DailyRevenue, d)).drop(1)

  test("contract ở cửa ngõ: file sai bộ trường bị CÁCH LY kèm lý do, không vào raw") {
    val l = newLake()
    val r = FileLanding.land(l, drop.resolve("bad-schema-2026-10-16.jsonl"), d16)
    assertEquals(r.rawFile, None)
    assertEquals(r.reason, Some("dòng 1: bộ trường sai: thiếu event_id"))
    assert(Files.exists(l.root.resolve(r.quarantined.get)) && l.rawFiles("orders").isEmpty)
  }

  test("thả lại cùng file là no-op: tên file raw = hash nội dung") {
    val l = newLake()
    val a = landDay(l, d15); val b = landDay(l, d15)
    assertEquals(a.rawFile, b.rawFile)
    assertEquals(l.rawFiles("orders").size, 1)
  }

  test("golden tính tay sau 3 lô + đối soát từng ngày: rows = rejected + event_conflicts + trùng + thay thế + xung đột + kept") {
    val l = newLake()
    Seq(d15, d16, d17).foreach(landDay(l, _))
    Orchestrator.backfill(l, Assets.all, d15, d17, "r")
    assertEquals(daily(l, d15), Vector("Retail,700000,2,2", "UNKNOWN,450000,1,1", "Wholesale,20000000,1,1"))
    assertEquals(daily(l, d16), Vector.empty)
    assertEquals(daily(l, d17), Vector("Retail,1890000,2,2"))
    def counts(d: LocalDate) = Lineage.latest(l, StagingOrders.name, d).get.counts
    assertEquals(counts(d15), Map("rows" -> 8, "rejected" -> 1, "event_conflicts" -> 0, "duplicate_events" -> 1, "superseded" -> 1, "conflicts" -> 0, "kept" -> 5))
    assertEquals(counts(d16), Map("rows" -> 4, "rejected" -> 0, "event_conflicts" -> 0, "duplicate_events" -> 0, "superseded" -> 1, "conflicts" -> 2, "kept" -> 1))
    assertEquals(Io.lines(l.partitionDir(StagingOrders.name, d15).resolve("rejected.jsonl")), Vector("""{"event_id":"E1504","errors":["quantity: 1..1000"]}"""))
    assertEquals(Reconcile.unassigned(l), Vector("raw/orders/ingest_date=2026-10-15/" + l.rawFiles("orders").head.getFileName + ": E1506 day='2026-13-01'"))
    val r = Reconcile.report(l)
    assertEquals((r.rawRows, r.assigned.values.sum, r.unassigned.size, r.balanced), (r.assigned.values.sum + 1, r.rawRows - 1, 1, true))
  }

  test("quality gate: đạt khi mọi thứ mới; daily bị sửa tay hoặc partition cũ → chặn publish") {
    val l = newLake()
    Seq(d15, d16).foreach(landDay(l, _))
    Orchestrator.backfill(l, Assets.all, d15, d16, "r")
    assertEquals(Quality.check(l, d15), Nil)
    Files.write(l.partFile(DailyRevenue, d15), "segment,revenue_vnd,order_lines,orders\nRetail,1,1,1\n".getBytes(UTF_8))
    assert(Quality.check(l, d15).contains("daily trên đĩa khác bảng tính lại từ staging"), Quality.check(l, d15).toString)
    landDay(l, d17) // lô 17 chạm ngày 16 → ngày 16 cũ
    assertEquals(Quality.check(l, d16), List("partition cũ — tính lại trước khi publish"))
  }

  test("dữ liệu đến muộn: lô 17 chỉ làm cũ ngày 16 và 17") {
    val l = newLake()
    Seq(d15, d16).foreach(landDay(l, _))
    Orchestrator.backfill(l, Assets.all, d15, d16, "a")
    landDay(l, d17)
    assertEquals(Orchestrator.backfill(l, Assets.all, d15, d17, "b").filter(_.action == "materialize").map(s => s"${s.day} ${s.asset}"),
      Vector("2026-10-16 staging_orders", "2026-10-16 daily_revenue", "2026-10-17 staging_orders", "2026-10-17 daily_revenue"))
  }

  /** Lake chỉ có một file raw tự viết (ngày 15) — cho các ca biên. */
  private def lakeWith(events: String*): Lake =
    val l = newLake()
    Io.writeAtomically(l.rawDir("orders").resolve("ingest_date=2026-10-15").resolve("t.jsonl"), events.mkString("", "\n", "\n"))
    l
  private def ev(id: String, order: String, price: Long, ts: String = "2026-10-15T01:00:00Z", day: String = "2026-10-15") =
    io.circe.Json.obj("event_id" -> io.circe.Json.fromString(id), "order_id" -> io.circe.Json.fromString(order),
      "customer_id" -> io.circe.Json.fromString("C1"), "day" -> io.circe.Json.fromString(day), "status" -> io.circe.Json.fromString("PAID"),
      "sku" -> io.circe.Json.fromString("SKU"), "quantity" -> io.circe.Json.fromInt(1), "unit_price_vnd" -> io.circe.Json.fromLong(price),
      "updated_at" -> io.circe.Json.fromString(ts)).noSpaces

  test("review P5 M1: order_id chứa dấu phẩy, ngoặc kép, xuống dòng đi qua staging nguyên vẹn; gate đối soát PAID độc lập") {
    val l = lakeWith(ev("A", "O,comma", 100), ev("B", "O\"quote", 100), ev("C", "O\nline", 100))
    Orchestrator.backfill(l, Assets.all, d15, d15, "r")
    assertEquals(daily(l, d15), Vector("Retail,300,3,3"))
    assertEquals(Quality.check(l, d15), Nil)
    // daily mất dòng mà vẫn "đúng định dạng" → gate chặn bằng phép đối soát không dùng lại phép join.
    Files.write(l.partFile(DailyRevenue, d15), "segment,revenue_vnd,order_lines,orders\n".getBytes(UTF_8))
    assert(Quality.check(l, d15).contains("daily không khớp staging: PAID 300 VND / 3 dòng, daily 0 VND / 0 dòng"), Quality.check(l, d15).toString)
  }

  test("review P5 M2: cùng event_id khác nội dung → cách ly CẢ nhóm, không phụ thuộc thứ tự; cùng nội dung → trùng bình thường") {
    def run(events: String*) =
      val l = lakeWith(events*)
      Orchestrator.backfill(l, Assets.all, d15, d15, "r")
      val rejected = l.partitionDir(StagingOrders.name, d15).resolve("rejected.jsonl") // chỉ tồn tại khi có dòng bị loại
      (daily(l, d15), Lineage.latest(l, StagingOrders.name, d15).get.counts, if Files.exists(rejected) then Io.lines(rejected) else Vector.empty)
    val forward = run(ev("X", "O1", 100), ev("X", "O1", 900))
    val reverse = run(ev("X", "O1", 900), ev("X", "O1", 100))
    assertEquals(forward, reverse)
    assertEquals(forward._1, Vector.empty)
    assertEquals((forward._2("event_conflicts"), forward._2("kept"), forward._2("duplicate_events")), (2, 0, 0))
    assertEquals(forward._3, Vector("""{"event_id":"X","errors":["event_id trùng nhưng 2 nội dung khác nhau — cách ly cả nhóm"]}"""))
    // Cùng event_id, khác order_id hoặc khác timestamp cũng là xung đột; cùng nội dung (kể cả timestamp viết khác múi giờ) là gửi lại.
    assertEquals(run(ev("X", "O1", 100), ev("X", "O2", 100))._2("event_conflicts"), 2)
    assertEquals(run(ev("X", "O1", 100), ev("X", "O1", 100, ts = "2026-10-15T02:00:00Z"))._2("event_conflicts"), 2)
    val resent = run(ev("X", "O1", 100), ev("X", "O1", 100, ts = "2026-10-15T08:00:00+07:00"))
    assertEquals((resent._1, resent._2("duplicate_events"), resent._2("event_conflicts")), (Vector("Retail,100,1,1"), 1, 0))
  }

  test("review P5 M3: MỘT quy tắc ngày — mỗi dòng raw thuộc đúng một ngày hoặc unassigned; tổng raw khớp") {
    val l = lakeWith(ev("A", "O1", 100), ev("B", "O2", 100, day = "+02026-10-15"), ev("C", "O3", 100, day = "2026-02-30"),
      ev("D", "O4", 100, day = ""), ev("E", "O5", 100, day = " 2026-10-15 "))
    Orchestrator.backfill(l, Assets.all, d15, d15, "r")
    assertEquals(Lineage.latest(l, StagingOrders.name, d15).get.counts("rows"), 2) // A và E (khoảng trắng được trim ở mọi nơi)
    val r = Reconcile.report(l)
    assertEquals((r.rawRows, r.assigned, r.balanced), (5, Map(d15 -> 2), true))
    assertEquals(r.unassigned.map(_.dropWhile(_ != ' ').trim), Vector("B day='+02026-10-15'", "C day='2026-02-30'", "D day=''"))
  }

  test("review P5 N2: thiếu artifact hoặc daily sai định dạng → vi phạm có lý do, không exception") {
    val l = newLake()
    assertEquals(Quality.check(l, d15), List("thiếu staging_orders/daily_revenue của 2026-10-15 — chưa tính đủ"))
    landDay(l, d15)
    Orchestrator.backfill(l, Assets.all, d15, d15, "r")
    Files.write(l.partFile(DailyRevenue, d15), "segment,revenue_vnd,order_lines,orders\nRetail\n".getBytes(UTF_8))
    val problems = Quality.check(l, d15)
    assert(problems.contains("daily dòng 2 sai định dạng: 'Retail'"), problems.toString)
  }

  private def assertCollisionDays(l: Lake): Unit =
    Seq(d15, d16).foreach { d =>
      val c = Lineage.latest(l, StagingOrders.name, d).get.counts
      assertEquals((c("rows"), c("event_conflicts"), c("kept"), c("rejected")), (1, 1, 0, 0))
      assertEquals(daily(l, d), Vector.empty)
      assertEquals(Quality.check(l, d), Nil)
      assert(Io.lines(l.partitionDir(StagingOrders.name, d).resolve("rejected.jsonl")).head.contains("cách ly cả nhóm"))
    }

  test("R2-M1: cùng ID khác ngày trong cùng file, đảo thứ tự vẫn cách ly cả hai ngày") {
    val rows = Vector(ev("X", "O1", 100), ev("X", "O1", 900, day = d16.toString))
    Seq(rows, rows.reverse).foreach { rs =>
      val l = lakeWith(rs*)
      Orchestrator.backfill(l, Assets.all, d15, d16, "r")
      assertCollisionDays(l)
    }
  }

  private def sequentialCollision(reverse: Boolean): Unit =
    val rows = Vector(ev("X", "O1", 100), ev("X", "O1", 900, day = d16.toString))
    val ordered = if reverse then rows.reverse else rows
    val firstDay = if reverse then d16 else d15
    val l = newLake()
    def land(row: String): Unit =
      val dropFile = l.root.resolve("drop").resolve(s"${Io.hashText(row)}.jsonl")
      Io.writeAtomically(dropFile, row + "\n")
      assert(FileLanding.land(l, dropFile, d17).rawFile.nonEmpty)
    land(ordered.head)
    Orchestrator.backfill(l, Assets.all, firstDay, firstDay, "before")
    assertEquals(Orchestrator.status(l, StagingOrders, firstDay), Status.Fresh)
    land(ordered.last)
    assert(Orchestrator.status(l, StagingOrders, firstDay).isInstanceOf[Status.Stale])
    assert(Quality.check(l, firstDay).nonEmpty)
    val steps = Orchestrator.backfill(l, Assets.all, d15, d16, "after")
    assertEquals(steps.count(_.action == "materialize"), 4)
    assertCollisionDays(l)
    assert(Orchestrator.backfill(l, Assets.all, d15, d16, "again").forall(_.action == "skip"))

  test("R2-M1: hai lô đến lần lượt, xung đột ngày 16 làm stale ngày 15") {
    sequentialCollision(reverse = false)
  }

  test("R2-M1: đảo thứ tự hai lô, xung đột ngày 15 làm stale ngày 16") {
    sequentialCollision(reverse = true)
  }

  test("R2-M1: bản sai nghiệp vụ/ngày vẫn làm xung đột ID trên toàn raw; đối soát từng ngày cân") {
    val invalid = ev("X", "O1", 0, day = "+02026-10-16")
    val l = lakeWith(ev("X", "O1", 100), invalid)
    Orchestrator.backfill(l, Assets.all, d15, d15, "r")
    val c = Lineage.latest(l, StagingOrders.name, d15).get.counts
    assertEquals((c("rows"), c("event_conflicts"), c("rejected"), c("kept")), (1, 1, 0, 0))
    assertEquals(Reconcile.report(l).unassigned.size, 1)
    assertEquals(Quality.check(l, d15), Nil)
  }

  test("R2-M2: rematerialize upstream xen giữa snapshot và refs vẫn giữ ID khớp byte cũ") {
    val l = lakeWith(ev("X", "O1", 100))
    val old = Orchestrator.materialize(l, StagingOrders, d15, "old")
    // Hai lần cùng byte: chọn lần khớp hash gần nhất, không chọn lần đầu hay latest khác byte.
    val same = Orchestrator.materialize(l, StagingOrders, d15, "same-bytes")
    assertEquals(old.outputHash, same.outputHash)
    val interleaved = new Asset:
      val name = DailyRevenue.name
      val codeVersion = DailyRevenue.codeVersion
      val upstream = DailyRevenue.upstream
      val fileName = DailyRevenue.fileName
      def sources(l: Lake, day: LocalDate): List[Source] = DailyRevenue.sources(l, day)
      override def relevant(day: LocalDate, files: List[InputFile]): List[InputFile] =
        Io.writeAtomically(l.rawFiles("orders").head, ev("X", "O1", 900) + "\n")
        Orchestrator.materialize(l, StagingOrders, day, "interleaved")
        files
      def compute(day: LocalDate, inputs: List[InputFile]): Output = DailyRevenue.compute(day, inputs)
    val m = Orchestrator.materialize(l, interleaved, d15, "downstream")
    val ref = m.inputs.find(_.kind == "asset").get
    assertEquals((ref.hash, ref.materializationId), (same.outputHash, Some(same.id)))
    assertEquals(daily(l, d15), Vector("Retail,100,1,1"))
    assert(Orchestrator.status(l, DailyRevenue, d15).isInstanceOf[Status.Stale])
    assertEquals(Lineage.latest(l, StagingOrders.name, d15).get.runId, "interleaved")
  }

  test("R2-M2: không tìm được lịch sử khớp hash upstream → fail rõ, không publish với None") {
    val l = lakeWith(ev("X", "O1", 100))
    Orchestrator.materialize(l, StagingOrders, d15, "old")
    Io.writeAtomically(l.partFile(StagingOrders, d15), "untracked bytes\n")
    val e = intercept[IllegalStateException](Orchestrator.materialize(l, DailyRevenue, d15, "bad"))
    assert(e.getMessage.contains("không xác định được provenance cho staging_orders@2026-10-15"))
    assert(!Files.exists(l.partFile(DailyRevenue, d15)))
    assertEquals(Lineage.latest(l, DailyRevenue.name, d15), None)
    Files.delete(l.partFile(StagingOrders, d15))
    assert(intercept[IllegalStateException](Orchestrator.materialize(l, DailyRevenue, d15, "missing")).getMessage.contains("hash=missing"))
  }
