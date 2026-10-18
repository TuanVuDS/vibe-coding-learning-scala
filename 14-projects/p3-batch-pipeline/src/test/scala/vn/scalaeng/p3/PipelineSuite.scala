package vn.scalaeng.p3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class PipelineSuite extends munit.FunSuite {
  // Test tích hợp: một test chạy job Spark nhiều lần (staging + audit đọc lại + publish). 30 giây mặc định của munit quá sát (review P3 m3).
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(300, "s")
  private var spark: SparkSession = _
  override def beforeAll(): Unit = { spark = Job.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  private val d15 = LocalDate.parse("2026-10-15")
  private val d16 = LocalDate.parse("2026-10-16")
  private val sample = Paths.get("data/sample")

  /** Bản sao dữ liệu mẫu để test được phép sửa. */
  private def copySample(): Path = {
    val dst = Files.createTempDirectory("p3-in")
    val s = Files.walk(sample)
    try s.iterator().asScala.foreach { p =>
      val t = dst.resolve(sample.relativize(p).toString)
      if (Files.isDirectory(p)) Files.createDirectories(t) else Files.copy(p, t)
    } finally s.close()
    dst
  }
  private def args(in: Path, out: Path, date: LocalDate, extra: String*): Seq[String] =
    Seq(s"--orders=$in/orders", s"--clicks=$in/clicks", s"--customers=$in/customers.csv", s"--output=$out", s"--run-date=$date") ++
      (if (extra.exists(_.startsWith("--max-reject-rate="))) Nil else Seq("--max-reject-rate=0.3")) ++ extra
  private def run(a: Seq[String], tamper: Path => Unit = _ => (), beforeMove: Publish.Move => Unit = _ => ()): (Int, Vector[String]) = {
    val logs = ArrayBuffer.empty[String]
    (Job.run(spark, a, l => { logs += l; () }, tamper, beforeMove), logs.toVector)
  }
  private def manifest(out: Path, runId: String): String = {
    val st = Files.list(out.resolve("_runs"))
    val f = try st.iterator().asScala.filter(_.getFileName.toString.startsWith(s"$runId--")).toList finally st.close()
    assertEquals(f.size, 1, f.toString)
    new String(Files.readAllBytes(f.head), StandardCharsets.UTF_8)
  }
  private def children(dir: Path): List[String] =
    if (!Files.isDirectory(dir)) Nil else { val st = Files.list(dir); try st.iterator().asScala.map(_.getFileName.toString).toList.sorted finally st.close() }
  /** Thay nội dung một bảng partition theo `day` trong staging (mô phỏng bug/ai đó ghi sai TRƯỚC audit). */
  private def rewriteStaged(st: Path, table: String)(f: DataFrame => DataFrame): Unit = {
    val p = st.resolve(table); val tmp = st.resolve(table + "_x")
    f(spark.read.parquet(p.toString)).write.partitionBy("day").parquet(tmp.toString)
    Publish.deleteTree(p); Files.move(tmp, p)
    ()
  }
  private def rows(out: Path, table: String, cols: String*): List[List[String]] =
    spark.read.parquet(out.resolve(table).toString).select(cols.map(col): _*).orderBy(cols.map(col): _*).collect()
      .map(r => r.toSeq.map(String.valueOf).toList).toList

  private def clicks(upTo: LocalDate = d16): DataFrame = Clicks.validate(Clicks.read(spark, "data/sample/clicks", upTo))

  test("config: gom mọi lỗi; đường dẫn và ngày chạy bắt buộc") {
    assertEquals(Config.parse(Seq("--run-date=2026-02-30", "--x=1", "sai")).left.toOption.get, List(
      "tham số 'sai' phải có dạng --key=value", "tham số lạ --x", "thiếu --orders", "thiếu --clicks", "thiếu --customers", "thiếu --output",
      "--run-date='2026-02-30': ngày dạng yyyy-MM-dd"))
  }

  test("click: chuẩn hóa (c1 → C1, ' sale10 ' → SALE10), JSON hỏng/thiếu campaign/ts sai bị loại kèm lý do") {
    val v = Common.split(clicks())
    val k2 = v.accepted.filter(col("event_id") === "K2").select("customer_id", "campaign").first()
    assertEquals((k2.getString(0), k2.getString(1)), ("C1", "SALE10"))
    val reasons = v.rejected.select(col("errors")).collect().map(_.getSeq[String](0).toList).toSet
    val tsRule = "ts: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)"
    assertEquals(reasons, Set(List("campaign: bắt buộc"), List("JSON hỏng", "event_id: bắt buộc", "customer_id: bắt buộc", "campaign: bắt buộc",
      tsRule), List(tsRule)))
  }

  test("click: K6 đến ở lô 16 nhưng thuộc NGÀY 15 (theo ts, UTC); K3 gửi lại bị bỏ; cùng event_id khác nội dung = xung đột") {
    val (kept, conflicts, dup) = Clicks.dedup(Common.split(clicks()).accepted)
    assertEquals((kept.count(), dup, conflicts.count()), (7L, 1L, 0L))
    val k6 = kept.filter(col("event_id") === "K6").select(col("ingest_date").cast("string"), col("day").cast("string")).first()
    assertEquals((k6.getString(0), k6.getString(1)), ("2026-10-16", "2026-10-15"))
    val clash = kept.limit(1).union(kept.limit(1).withColumn("campaign", lit("KHÁC")))
    assertEquals(Clicks.dedup(clash)._2.count(), 2L)
  }

  test("campaign_daily: golden tính tay; assisted revenue cộng các chiến dịch LỚN HƠN doanh thu thật") {
    val od    = Orders.dedup(Common.split(Orders.validate(Orders.read(spark, "data/sample/orders", d16))).accepted)
    val cust  = spark.read.option("header", "true").csv("data/sample/customers.csv").select("customer_id", "segment")
    val facts = Reports.orderFacts(od.latest, cust)
    val camp  = Reports.campaignDaily(Clicks.dedup(Common.split(clicks()).accepted)._1, facts)
    val got = camp.orderBy("day", "campaign").collect().map(r => (r.getAs[java.sql.Date]("day").toString, r.getAs[String]("campaign"),
      r.getAs[Long]("clicks"), r.getAs[Long]("users"), r.getAs[Long]("converted_users"), BigDecimal(r.getAs[java.math.BigDecimal]("assisted_revenue_vnd")),
      BigDecimal(r.getAs[java.math.BigDecimal]("conversion_rate")))).toList
    assertEquals(got, List(
      ("2026-10-15", "NEWBIE", 2L, 2L, 1L, BigDecimal(700000), BigDecimal("0.5000")),
      ("2026-10-15", "SALE10", 3L, 2L, 2L, BigDecimal(20700000), BigDecimal("1.0000")),
      ("2026-10-16", "SALE10", 2L, 2L, 1L, BigDecimal(750000), BigDecimal("0.5000"))))
    val assisted = got.map(_._6).sum
    val actual   = BigDecimal(Reports.dailyRevenue(facts).agg(sum("revenue_vnd")).first().getDecimal(0))
    assertEquals((assisted, actual), (BigDecimal(22150000), BigDecimal(21900000))) // C1 (700.000) được tính ở CẢ NEWBIE và SALE10
  }

  private def finalPlan(df: DataFrame): String = {
    df.collect()
    (df.queryExecution.executedPlan match { case a: AdaptiveSparkPlanExec => a.executedPlan; case p => p }).toString
  }

  test("plan: join khách là BroadcastHashJoin; join click × doanh thu: dữ liệu nhỏ → broadcast, giả lập lớn (tắt broadcast tự động) → SortMergeJoin") {
    val od    = Orders.dedup(Common.split(Orders.validate(Orders.read(spark, "data/sample/orders", d16))).accepted)
    val cust  = spark.read.option("header", "true").csv("data/sample/customers.csv").select("customer_id", "segment")
    val facts = Reports.orderFacts(od.latest, cust)
    assert(finalPlan(facts).contains("BroadcastHashJoin"))
    val kept = Clicks.dedup(Common.split(clicks()).accepted)._1
    // Tìm ĐÚNG join click × doanh thu (khóa customer_id + day), tách khỏi join khách (chỉ customer_id) — review P3 m4.
    val clickJoin = """(\w+Join) \[customer_id#\d+, day#\d+\]""".r
    def joinOf(plan: String) = clickJoin.findFirstMatchIn(plan).map(_.group(1))
    assertEquals(joinOf(finalPlan(Reports.campaignDaily(kept, facts))), Some("BroadcastHashJoin"))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1"); spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    try {
      val plan = finalPlan(Reports.campaignDaily(kept, facts))
      println(s"[plan] join click × doanh thu khi tắt broadcast: ${plan.linesIterator.filter(_.contains("Join")).map(_.trim).mkString(" | ")}")
      assertEquals(joinOf(plan), Some("SortMergeJoin"), plan)
    } finally { spark.conf.unset("spark.sql.autoBroadcastJoinThreshold"); spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold") }
  }

  test("e2e: chạy ngày 15 rồi ngày 16 — golden, manifest, log published; chạy lại ngày 16 cho y hệt") {
    val in = copySample(); val out = Files.createTempDirectory("p3-out")
    assertEquals(run(args(in, out, d15, "--run-id=r15"))._1, ExitCode.Succeeded)
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd").head, List("2026-10-15", "Retail", "1150000"))
    val (code, logs) = run(args(in, out, d16, "--run-id=r16"))
    assertEquals(code, ExitCode.Succeeded)
    val golden = List(List("2026-10-15", "Retail", "700000"), List("2026-10-15", "UNKNOWN", "450000"),
      List("2026-10-15", "Wholesale", "20000000"), List("2026-10-16", "Wholesale", "750000"))
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd"), golden)
    assert(logs.exists(l => l.contains("\"table\":\"daily_revenue\"") && l.contains("\"replaced\":[\"day=2026-10-15\"]") && l.contains("\"added\":[\"day=2026-10-16\"]")),
      logs.mkString("\n"))
    val m16 = manifest(out, "r16")
    assert(m16.contains("\"status\":\"SUCCEEDED\"") && m16.contains("\"clicksKept\":7") && m16.contains("\"factLines\":6"), m16)
    val camp1 = rows(out, "campaign_daily", "day", "campaign", "clicks", "converted_users")
    assertEquals(run(args(in, out, d16, "--run-id=r16b"))._1, ExitCode.Succeeded)
    assertEquals((rows(out, "daily_revenue", "day", "segment", "revenue_vnd"), rows(out, "campaign_daily", "day", "campaign", "clicks", "converted_users")), (golden, camp1))
    assertEquals((children(out.resolve("_staging")), children(out.resolve("_trash"))), (Nil, Nil), "staging và thùng rác phải được dọn")
    assertEquals(children(out).filterNot(_.startsWith("_")), List("campaign_daily", "click_conflicts", "daily_revenue", "fact_order_lines",
      "order_conflicts", "rejected_clicks", "rejected_orders"))
  }

  test("audit thất bại (staging bị sửa) → mã 4, thư mục phục vụ GIỮ NGUYÊN bản trước") {
    val in = copySample(); val out = Files.createTempDirectory("p3-audit")
    assertEquals(run(args(in, out, d15, "--run-id=ok"))._1, 0)
    val before = rows(out, "daily_revenue", "day", "segment", "revenue_vnd")
    val tamper: Path => Unit = st => spark.read.parquet(st.resolve("fact_order_lines").toString).limit(1)
      .write.mode("append").partitionBy("day").parquet(st.resolve("fact_order_lines").toString)
    val (code, logs) = run(args(in, out, d16, "--run-id=bad"), tamper)
    assertEquals(code, ExitCode.AuditFailed)
    assert(logs.exists(_.contains("khóa (order_id, sku) trùng")), logs.mkString("\n"))
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd"), before)
  }

  test("partition mồ côi: ngày 16 không còn fact hợp lệ → partition day=2026-10-16 cũ bị gỡ khỏi fact và daily_revenue") {
    val in = copySample(); val out = Files.createTempDirectory("p3-stale")
    assertEquals(run(args(in, out, d16, "--run-id=a"))._1, 0)
    assert(Files.exists(out.resolve("fact_order_lines/day=2026-10-16")))
    // O6 (dòng hàng duy nhất của ngày 16 còn hợp lệ) bị gửi thêm một bản CÙNG giờ khác trạng thái → xung đột → ngày 16 không còn fact.
    Files.write(in.resolve("orders/ingest_date=2026-10-16/orders.csv"),
      "O6,C2,2026-10-16,CANCELLED,MS-01,3,250000,2026-10-16T06:00:00Z\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
    val (code, logs) = run(args(in, out, d16, "--run-id=b"))
    assertEquals(code, 0)
    assert(!Files.exists(out.resolve("fact_order_lines/day=2026-10-16")) && !Files.exists(out.resolve("daily_revenue/day=2026-10-16")))
    assert(logs.exists(l => l.contains("\"table\":\"fact_order_lines\"") && l.contains("\"removed\":[\"day=2026-10-16\"]")), logs.mkString("\n"))
    assert(Files.exists(out.resolve("campaign_daily/day=2026-10-16")), "click ngày 16 vẫn còn nên campaign_daily ngày 16 vẫn còn")
  }

  test("gate theo lô: lô thiếu hoặc tỷ lệ loại vượt ngưỡng → mã 3, KHÔNG có output; dimension trùng → mã 3") {
    val in = copySample()
    val out1 = Files.createTempDirectory("p3-g1")
    val (c1, l1) = run(args(in, out1, d16, "--max-reject-rate=0.2"))
    assertEquals(c1, ExitCode.DataRejected)
    assert(l1.exists(_.contains("clicks: lô 2026-10-16 có 1/4 dòng bị loại (25.0%) > ngưỡng 20.0%")), l1.mkString("\n"))
    assert(!Files.exists(out1.resolve("daily_revenue")))
    assertEquals(run(args(in, Files.createTempDirectory("p3-g2"), LocalDate.parse("2026-10-20")))._1, ExitCode.DataRejected)
    Files.write(in.resolve("customers.csv"), "C2,Retail,HN\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
    val (c3, l3) = run(args(in, Files.createTempDirectory("p3-g3"), d16))
    assertEquals(c3, ExitCode.DataRejected)
    assert(l3.exists(_.contains("customers trùng customer_id: C2")), l3.mkString("\n"))
  }
  test("review P3 M1: audit so NỘI DUNG — sửa giá giữ khóa, sửa segment giữ tổng, sửa campaign giữ tổng click, xóa bản ghi lỗi — đều chặn publish") {
    val in = copySample()
    val mutations: Seq[(String, Path => Unit, String)] = Seq(
      ("price", st => rewriteStaged(st, "fact_order_lines")(_.withColumn("unit_price_vnd", lit(1L)).withColumn("revenue_vnd", lit(0).cast("decimal(38,0)"))),
        "fact staging khác fact đã tính"),
      ("segment", st => rewriteStaged(st, "daily_revenue")(_.withColumn("segment", lit("WRONG")).withColumn("orders", lit(999L))),
        "daily_revenue staging khác bảng tính lại từ fact"),
      ("campaign", st => rewriteStaged(st, "campaign_daily")(_.withColumn("campaign", lit("WRONG")).withColumn("assisted_revenue_vnd", lit(0).cast("decimal(38,0)"))),
        "campaign_daily staging khác bảng tính lại"),
      ("rejected", st => { val p = st.resolve("rejected_orders"); val df = spark.read.parquet(p.toString).limit(0)
        df.write.parquet(p.toString + "_x"); Publish.deleteTree(p); Files.move(Paths.get(p.toString + "_x"), p); () }, "rejected_orders staging 0 != 3"))
    mutations.foreach { case (name, tamper, expected) =>
      val out = Files.createTempDirectory("p3-m1")
      val (code, logs) = run(args(in, out, d16, s"--run-id=$name"), tamper)
      assertEquals(code, ExitCode.AuditFailed, s"$name: ${logs.mkString("\n")}")
      assert(logs.exists(_.contains(expected)), s"$name: ${logs.mkString("\n")}")
      assert(!Files.exists(out.resolve("daily_revenue")), s"$name: không được publish")
    }
  }

  test("review P3 M2: publish hỏng giữa chừng → rollback, thư mục phục vụ về đúng bản trước, nhật ký ghi lại; chạy lại CÙNG run-id thành công") {
    val in = copySample(); val out = Files.createTempDirectory("p3-m2")
    assertEquals(run(args(in, out, d15, "--run-id=day"))._1, 0)
    val before = (rows(out, "fact_order_lines", "order_id", "sku", "status"), rows(out, "daily_revenue", "day", "segment", "revenue_vnd"))
    var n = 0
    val failThird: Publish.Move => Unit = m => { n += 1; if (n == 3) throw new java.io.IOException(s"đĩa đầy (giả lập) khi chuyển ${m.from.getFileName}") }
    val (code, logs) = run(args(in, out, d16, "--run-id=day"), beforeMove = failThird)
    assertEquals(code, ExitCode.Failed)
    assert(logs.exists(_.contains("\"event\":\"publish_failed_rolled_back\"")), logs.mkString("\n"))
    assertEquals((rows(out, "fact_order_lines", "order_id", "sku", "status"), rows(out, "daily_revenue", "day", "segment", "revenue_vnd")), before)
    val journal = new String(Files.readAllBytes(Files.list(out.resolve("_publish")).iterator().asScala.toList.sortBy(_.toString).last), StandardCharsets.UTF_8)
    assertEquals(journal.linesIterator.map(_.takeWhile(_ != '\t')).toList, List("INTENT", "DONE", "INTENT", "DONE", "FAILED", "UNDONE", "UNDONE", "ROLLED_BACK"))
    assertEquals((children(out.resolve("_staging")), children(out.resolve("_trash"))), (Nil, Nil))
    assertEquals(run(args(in, out, d16, "--run-id=day"))._1, ExitCode.Succeeded) // cùng run-id: execution_id mới, không đụng thùng rác cũ
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd").map(_(2)), List("700000", "450000", "20000000", "750000"))
  }

  test("review P3 vòng 2 (R2-M2): ghi NHẬT KÝ hỏng giữa chừng vẫn rollback đủ; staging không bị xóa khi chưa chứng minh phục hồi") {
    val in = copySample(); val out = Files.createTempDirectory("p3-journal")
    assertEquals(run(args(in, out, d15, "--run-id=j"))._1, 0)
    val before = rows(out, "daily_revenue", "day", "segment", "revenue_vnd")
    var n = 0
    // Ở bước thứ 2: biến file nhật ký thành THƯ MỤC → mọi lần ghi nhật ký sau đó hỏng (giả lập đĩa đầy/mất quyền đúng lúc publish).
    val breakJournal: Publish.Move => Unit = _ => {
      n += 1
      if (n == 2) {
        val st = Files.list(out.resolve("_publish"))
        val j = try st.iterator().asScala.toList.maxBy(_.getFileName.toString) finally st.close()
        Files.delete(j); Files.createDirectories(j); ()
      }
    }
    val (code, logs) = run(args(in, out, d16, "--run-id=j"), beforeMove = breakJournal)
    assertEquals(code, ExitCode.Failed)
    assert(logs.exists(_.contains("\"event\":\"publish_failed_rolled_back\"")), logs.mkString("\n"))
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd"), before)
    assertEquals((children(out.resolve("_staging")), children(out.resolve("_trash"))), (Nil, Nil))
  }

  test("review P3 M3: rút TRỌN một ngày khỏi input (lô 15 thành rỗng) → snapshot gỡ partition ngày 15 ở mọi bảng") {
    val in = Files.createTempDirectory("p3-m3"); val out = Files.createTempDirectory("p3-m3-out")
    def write(rel: String, body: String): Unit = { val f = in.resolve(rel); Files.createDirectories(f.getParent); Files.write(f, body.getBytes(StandardCharsets.UTF_8)); () }
    val oh = Orders.Columns.mkString(",") + "\n"
    write("customers.csv", "customer_id,segment,city\nC1,Retail,HN\n")
    write("orders/ingest_date=2026-10-15/o.csv", oh + "A,C1,2026-10-15,PAID,KB-01,1,100,2026-10-15T01:00:00Z\n")
    write("orders/ingest_date=2026-10-16/o.csv", oh + "B,C1,2026-10-16,PAID,KB-01,1,100,2026-10-16T01:00:00Z\n")
    write("clicks/ingest_date=2026-10-15/c.jsonl", "{\"event_id\":\"K1\",\"customer_id\":\"C1\",\"campaign\":\"S\",\"ts\":\"2026-10-15T00:30:00Z\"}\n")
    write("clicks/ingest_date=2026-10-16/c.jsonl", "{\"event_id\":\"K2\",\"customer_id\":\"C1\",\"campaign\":\"S\",\"ts\":\"2026-10-16T00:30:00Z\"}\n")
    val a = Seq(s"--orders=$in/orders", s"--clicks=$in/clicks", s"--customers=$in/customers.csv", s"--output=$out", "--run-date=2026-10-16")
    assertEquals(run(a :+ "--run-id=one")._1, 0)
    write("orders/ingest_date=2026-10-15/o.csv", oh)  // lô 15 bị bên gửi rút lại: chỉ còn header
    write("clicks/ingest_date=2026-10-15/c.jsonl", "")
    val (code, logs) = run(a :+ "--run-id=two")
    assertEquals(code, 0, logs.mkString("\n"))
    assertEquals(Seq("fact_order_lines", "daily_revenue", "campaign_daily").map(t => children(out.resolve(t))),
      Seq(List("day=2026-10-16"), List("day=2026-10-16"), List("day=2026-10-16")))
    assertEquals(rows(out, "daily_revenue", "day", "revenue_vnd"), List(List("2026-10-16", "100")))
  }

  test("review P3 M4: kết quả rỗng hợp lệ (chỉ REFUNDED / mọi đơn bị loại / mọi click xung đột) → mã 0, không UNABLE_TO_INFER_SCHEMA") {
    val oh = Orders.Columns.mkString(",") + "\n"
    val k = "{\"event_id\":\"K1\",\"customer_id\":\"C1\",\"campaign\":\"S\",\"ts\":\"2026-10-16T00:30:00Z\"}\n"
    val kClash = k + "{\"event_id\":\"K1\",\"customer_id\":\"C1\",\"campaign\":\"T\",\"ts\":\"2026-10-16T00:30:00Z\"}\n"
    val cases = Seq(
      "REFUNDED" -> (oh + "A,C1,2026-10-16,REFUNDED,KB-01,1,100,2026-10-16T01:00:00Z\n", k),
      "loại hết" -> (oh + "A,C1,2026-10-16,PAID,KB-01,0,100,2026-10-16T01:00:00Z\n", k),
      "click xung đột" -> (oh + "A,C1,2026-10-16,PAID,KB-01,1,100,2026-10-16T01:00:00Z\n", kClash))
    cases.foreach { case (name, (orders, clicksBody)) =>
      val in = Files.createTempDirectory("p3-m4")
      def write(rel: String, body: String): Unit = { val f = in.resolve(rel); Files.createDirectories(f.getParent); Files.write(f, body.getBytes(StandardCharsets.UTF_8)); () }
      write("customers.csv", "customer_id,segment,city\nC1,Retail,HN\n"); write("orders/ingest_date=2026-10-16/o.csv", orders)
      write("clicks/ingest_date=2026-10-16/c.jsonl", clicksBody)
      val out = Files.createTempDirectory("p3-m4-out")
      val (code, logs) = run(Seq(s"--orders=$in/orders", s"--clicks=$in/clicks", s"--customers=$in/customers.csv", s"--output=$out",
        "--run-date=2026-10-16", "--max-reject-rate=1"))
      assertEquals(code, ExitCode.Succeeded, s"$name: ${logs.mkString("\n")}")
    }
  }

  test("review P3 M5: '2026' và timestamp thiếu múi giờ bị loại ở cả đơn và click; ngày của click theo UTC dù session đặt múi giờ khác") {
    val in = Files.createTempDirectory("p3-m5")
    def write(rel: String, body: String): Unit = { val f = in.resolve(rel); Files.createDirectories(f.getParent); Files.write(f, body.getBytes(StandardCharsets.UTF_8)); () }
    write("orders/ingest_date=2026-10-16/o.csv", Orders.Columns.mkString(",") + "\nA,C1,2026,PAID,KB-01,1,100,2026-10-16T01:00:00Z\nB,C1,2026-10-16,PAID,KB-01,1,100,2026-10-16T01:00:00\n")
    write("clicks/ingest_date=2026-10-16/c.jsonl", "{\"event_id\":\"K1\",\"customer_id\":\"C1\",\"campaign\":\"S\",\"ts\":\"2026\"}\n")
    val o = Common.split(Orders.validate(Orders.read(spark, s"$in/orders", d16))).rejected.select(col("raw.order_id"), col("errors")).collect()
      .map(r => r.getString(0) -> r.getSeq[String](1).toList).toMap
    assertEquals(o, Map("A" -> List("day: phải là ngày yyyy-MM-dd"), "B" -> List("updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)")))
    assertEquals(Common.split(Clicks.validate(Clicks.read(spark, s"$in/clicks", d16))).rejected.count(), 1L)
    // Review vòng 2: dưới spark.sql.timestampType=TIMESTAMP_NTZ, click 23:30Z ngày 15 vẫn thuộc ngày 15 (ép TIMESTAMP_LTZ tường minh).
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    try {
      val ntz = Files.createTempDirectory("p3-ntz")
      val f = Files.createDirectories(ntz.resolve("ingest_date=2026-10-16")).resolve("c.jsonl")
      Files.write(f, "{\"event_id\":\"KX\",\"customer_id\":\"C1\",\"campaign\":\"S\",\"ts\":\"2026-10-16T06:30:00+07:00\"}\n".getBytes(StandardCharsets.UTF_8))
      val k = Common.split(Clicks.validate(Clicks.read(spark, ntz.toString, d16))).accepted.select(col("day").cast("string")).first().getString(0)
      assertEquals(k, "2026-10-15") // 06:30+07:00 = 23:30Z ngày 15; NTZ (bỏ offset) sẽ ra ngày 16
    } finally spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
    // Người gọi đưa session múi giờ Việt Nam: job vẫn chốt UTC ở ranh giới (click 23:30Z ngày 15 thuộc ngày 15, không phải 16).
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    try {
      val src = copySample(); val out = Files.createTempDirectory("p3-m5-out")
      assertEquals(run(args(src, out, d16, "--run-id=tz"))._1, 0)
      assertEquals(spark.conf.get("spark.sql.session.timeZone"), "UTC")
      assertEquals(rows(out, "campaign_daily", "day", "campaign", "clicks"),
        List(List("2026-10-15", "NEWBIE", "2"), List("2026-10-15", "SALE10", "3"), List("2026-10-16", "SALE10", "2")))
    } finally spark.conf.set("spark.sql.session.timeZone", "UTC")
  }

  test("review P3 M6: cấu hình sai → mã 2 KHÔNG tạo session; manifest mỗi lần thực thi, cùng run-id không ghi đè") {
    val logs = ArrayBuffer.empty[String]
    assertEquals(Job.launch(Seq("--orders=x"), l => { logs += l; () }, () => fail("không được tạo session"), stopAfter = true), ExitCode.ConfigInvalid)
    assertEquals(Config.parse(Seq("--orders=a", "--orders=b", "--max-reject-rate=")).left.toOption.get.take(2),
      List("tham số --orders lặp lại", "--max-reject-rate= rỗng"))
    val in = copySample(); val out = Files.createTempDirectory("p3-m6")
    assertEquals(run(args(in, out, d16, "--run-id=same"))._1, 0)
    assertEquals(run(args(in, out, d16, "--run-id=same", "--max-reject-rate=0.2"))._1, ExitCode.DataRejected)
    assertEquals(children(out.resolve("_runs")).count(_.startsWith("same--")), 2)
  }

  test("generator: từ chối thư mục ra đã có dữ liệu (không để lô cũ lẫn vào)") {
    val d = Files.createTempDirectory("g-used")
    Generator.generate(d, d15, 1, 10, 10, seed = 1, customers = 5)
    val e = intercept[IllegalArgumentException](Generator.generate(d, d15, 1, 10, 10, seed = 1, customers = 5))
    assert(e.getMessage.contains("không rỗng"), e.getMessage)
  }

  private def tree(root: Path): Map[String, Seq[Byte]] = {
    val s = Files.walk(root)
    try s.iterator().asScala.filter(Files.isRegularFile(_)).map(f => root.relativize(f).toString -> Files.readAllBytes(f).toSeq).toMap finally s.close()
  }

  test("generator: cùng seed → cùng từng byte; seed khác → khác") {
    val (a, b, c) = (Files.createTempDirectory("g1"), Files.createTempDirectory("g2"), Files.createTempDirectory("g3"))
    Seq(a -> 7L, b -> 7L, c -> 8L).foreach { case (d, s) => Generator.generate(d, d15, 2, 200, 300, s, customers = 50) }
    assertEquals(tree(a), tree(b))
    assertNotEquals(tree(a), tree(c))
  }

  test("dữ liệu sinh ngẫu nhiên 3 ngày (seed 42): job qua audit, chạy lại y hệt — regression trên một fixture, không phải chứng minh cho mọi input") {
    val in = Files.createTempDirectory("p3-gen"); val out = Files.createTempDirectory("p3-gen-out")
    val s = Generator.generate(in, d15, 3, 500, 2000, seed = 42, customers = 200)
    val a = Seq(s"--orders=$in/orders", s"--clicks=$in/clicks", s"--customers=$in/customers.csv", s"--output=$out",
      "--run-date=2026-10-17", "--max-reject-rate=0.05")
    val (code, logs) = run(a :+ "--run-id=g1")
    assertEquals(code, ExitCode.Succeeded, logs.mkString("\n"))
    println(s"[generator] $s")
    val first = rows(out, "daily_revenue", "day", "segment", "revenue_vnd", "order_lines")
    assertEquals(run(a :+ "--run-id=g2")._1, ExitCode.Succeeded)
    assertEquals(rows(out, "daily_revenue", "day", "segment", "revenue_vnd", "order_lines"), first)
  }
}
