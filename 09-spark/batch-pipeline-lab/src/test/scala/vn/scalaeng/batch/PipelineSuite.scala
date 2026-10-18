package vn.scalaeng.batch

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.file.{Files, Path}
import java.time.LocalDate

class PipelineSuite extends munit.FunSuite {
  // Test tích hợp: một test chạy pipeline Spark tới 5 lần (mỗi lần có verify đọc lại + exceptAll). 30 giây mặc định của munit quá sát trên laptop.
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(180, "s")
  private var spark: SparkSession = _
  private val d15 = LocalDate.parse("2026-10-15")
  private val d16 = LocalDate.parse("2026-10-16")

  override def beforeAll(): Unit = { spark = PipelineDemo.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  private def tmp(prefix: String): Path = Files.createTempDirectory(prefix)
  private def validated(upTo: LocalDate = d16) = Pipeline.validate(Pipeline.readRaw(spark, "data/input", upTo))
  private def dailyRows(out: String): List[(String, String, BigDecimal, Long, Long)] =
    spark.read.parquet(Pipeline.Paths(out).daily).orderBy("day", "segment").collect().map { r =>
      (r.getAs[java.sql.Date]("day").toString, r.getAs[String]("segment"), BigDecimal(r.getAs[java.math.BigDecimal]("revenue_vnd")),
        r.getAs[Long]("order_lines"), r.getAs[Long]("orders"))
    }.toList

  /** Thư mục input tạm với một lô có nội dung tùy ý — để thử trường hợp mà dữ liệu mẫu không có. */
  private def inputWith(content: String): String = {
    val root = tmp("ch24-in")
    val dir  = Files.createDirectories(root.resolve("ingest_date=2026-10-20"))
    Files.write(dir.resolve("orders.csv"), content.getBytes("UTF-8"))
    root.toString
  }
  private val header = Pipeline.Columns.mkString(",") + "\n"

  test("bước 1–2: partition discovery cho ingest_date kiểu DATE; lọc upTo chỉ đọc các lô cần (PartitionFilters)") {
    val raw = Pipeline.readRaw(spark, "data/input", d15)
    assertEquals(raw.schema("ingest_date").dataType, DateType)
    assertEquals((raw.count(), Pipeline.readRaw(spark, "data/input", d16).count()), (7L, 13L))
    assert(Pipeline.checkSchema(raw).isRight)
    assert(raw.queryExecution.executedPlan.toString.contains("PartitionFilters: [isnotnull(ingest_date"), raw.queryExecution.executedPlan.toString)
  }

  test("bước 2: header sai thứ tự cột làm job LỖI (enforceSchema=false), không tráo giá trị âm thầm") {
    val in = inputWith("customer_id,order_id,day,status,sku,quantity,unit_price_vnd,updated_at\nC1,X,2026-10-20,PAID,KB-01,1,1,2026-10-20T01:00:00Z\n")
    val e  = intercept[Exception](Pipeline.readRaw(spark, in, LocalDate.parse("2026-10-20")).collect())
    val causes = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
    assert(causes.contains("CSV header does not conform to the schema"), causes)
  }

  test("bước 3: gom MỌI lỗi của một dòng; dòng sai số cột bị bắt bằng _corrupt_record") {
    val o5 = Pipeline.rejected(validated()).filter(col("raw.order_id") === "O5").select("errors").first().getSeq[String](0)
    assertEquals(o5.size, 6)
    assert(o5.contains("customer_id: bắt buộc") && o5.contains("updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)"), o5.toString)
    val in  = inputWith(header + "X,C1,2026-10-20,PAID,KB-01,1,1,2026-10-20T01:00:00Z,THỪA\n")
    val bad = Pipeline.rejected(Pipeline.validate(Pipeline.readRaw(spark, in, LocalDate.parse("2026-10-20")))).select("errors").first().getSeq[String](0)
    assertEquals(bad, Seq("dòng sai số cột"))
  }

  test("bước 4: chuẩn hóa bằng hàm có sẵn — ' kb-01 ' -> KB-01, 'paid' -> PAID, 'c1' -> C1") {
    val acc = Pipeline.accepted(validated(d15))
    val o1  = acc.filter(col("order_id") === "O1").select("customer_id", "sku").collect().map(r => (r.getString(0), r.getString(1))).toSet
    assertEquals(o1, Set(("C1", "KB-01"), ("C1", "MS-01")))
    assertEquals(acc.filter(col("order_id") === "O2").select("status").distinct().collect().map(_.getString(0)).toList, List("PAID"))
  }

  test("bước 6: trùng tuyệt đối bỏ 1; hoàn tiền O1 thay bản PAID; O7 cùng giờ khác trạng thái = XUNG ĐỘT") {
    val d = Pipeline.dedup(Pipeline.accepted(validated()))
    assertEquals(d.exactDuplicates.select("order_id").collect().map(_.getString(0)).toList, List("O2"))
    assertEquals(d.superseded.select("order_id", "status").collect().map(r => (r.getString(0), r.getString(1))).toList, List(("O1", "PAID")))
    assertEquals(d.conflicts.select("status").collect().map(_.getString(0)).toSet, Set("PAID", "CANCELLED"))
    assertEquals(d.latest.filter(col("order_id") === "O1" && col("sku") === "KB-01").select("status").first().getString(0), "REFUNDED")
  }

  test("bước 6: CÙNG nội dung đến ở hai lô khác nhau vẫn là trùng tuyệt đối (ingest_date không thuộc nội dung)") {
    val root = tmp("ch24-twice")
    val line = "X,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T01:00:00Z\n"
    Seq("2026-10-20", "2026-10-21").foreach { d =>
      Files.write(Files.createDirectories(root.resolve(s"ingest_date=$d")).resolve("orders.csv"), (header + line).getBytes("UTF-8"))
    }
    val d = Pipeline.dedup(Pipeline.accepted(Pipeline.validate(Pipeline.readRaw(spark, root.toString, LocalDate.parse("2026-10-21")))))
    assertEquals((d.exactDuplicates.count(), d.latest.count(), d.conflicts.count()), (1L, 1L, 0L))
  }

  test("bước 7: customer_id trùng trong dimension → từ chối; join dùng BroadcastHashJoin") {
    val customers = Pipeline.readCustomers(spark, "data/customers.csv")
    val latest    = Pipeline.dedup(Pipeline.accepted(validated())).latest
    val e = intercept[IllegalArgumentException](Pipeline.enrich(latest, customers.union(customers.filter(col("customer_id") === "C2"))))
    assert(e.getMessage.contains("C2"), e.getMessage)
    assertEquals(Experiments.joinStrategy(Pipeline.enrich(latest, customers)), "BroadcastHashJoin")
  }

  test("bước 8: DECIMAL(38,0) — tổng vượt Long không tràn, ANSI vẫn bật") {
    val schema = StructType(Seq(StructField("order_id", StringType), StructField("customer_id", StringType), StructField("day", DateType),
      StructField("status", StringType), StructField("quantity", IntegerType), StructField("unit_price_vnd", LongType)))
    val rows = java.util.Arrays.asList(
      Row("A", "C1", java.sql.Date.valueOf("2026-10-20"), "PAID", 1000, Long.MaxValue),
      Row("B", "C1", java.sql.Date.valueOf("2026-10-20"), "PAID", 1000, Long.MaxValue))
    val facts = Pipeline.enrich(spark.createDataFrame(rows, schema), Pipeline.readCustomers(spark, "data/customers.csv"))
    val total = BigDecimal(Pipeline.dailyRevenue(facts).first().getAs[java.math.BigDecimal]("revenue_vnd"))
    assertEquals(total, BigDecimal(Long.MaxValue) * 2000)
  }

  test("golden tính tay sau hai lô; lô 2 SỬA partition ngày 15 (hoàn tiền O1, đơn trễ O8)") {
    val out = tmp("ch24-golden").toString
    val r1  = Pipeline.run(spark, "data/input", "data/customers.csv", out, d15)
    assertEquals(dailyRows(out).head, ("2026-10-15", "Retail", BigDecimal(1150000), 2L, 1L))
    val r2  = Pipeline.run(spark, "data/input", "data/customers.csv", out, d16)
    assertEquals(dailyRows(out), List(
      ("2026-10-15", "Retail", BigDecimal(700000), 2L, 2L),
      ("2026-10-15", "UNKNOWN", BigDecimal(450000), 1L, 1L),
      ("2026-10-15", "Wholesale", BigDecimal(20000000), 1L, 1L),
      ("2026-10-16", "Wholesale", BigDecimal(750000), 1L, 1L)))
    assertEquals((r1.paidRevenueVnd, r2.paidRevenueVnd), (BigDecimal(21600000), BigDecimal(21900000)))
    assertEquals((r2.inputRows, r2.rejectedRows, r2.exactDuplicateRows, r2.supersededRows, r2.conflictRows, r2.factLines), (13L, 3L, 1L, 1L, 2L, 6L))
    assert(r1.ok && r2.ok, (r1.failedChecks ++ r2.failedChecks).toString)
  }

  test("rerun safety: chạy lại cùng input → cùng output, cùng số file") {
    val out = tmp("ch24-rerun").toString
    Pipeline.run(spark, "data/input", "data/customers.csv", out, d16)
    val (rows1, files1) = (dailyRows(out), Experiments.filesPerPartition(Pipeline.Paths(out).facts))
    val again = Pipeline.run(spark, "data/input", "data/customers.csv", out, d16)
    assertEquals((dailyRows(out), Experiments.filesPerPartition(Pipeline.Paths(out).facts)), (rows1, files1))
    assertEquals(files1.values.toSet, Set(1))
    assert(again.ok, again.failedChecks.toString)
  }

  /** Chạy pipeline và trả kèm fact đã tính (để gọi lại `verify` sau khi cố ý làm hỏng output). */
  private def runAndExpected(out: String): (Pipeline.RunReport, DataFrame, StructType) = {
    val r   = Pipeline.run(spark, "data/input", "data/customers.csv", out, d16)
    val v   = validated()
    val exp = Pipeline.enrich(Pipeline.dedup(Pipeline.accepted(v)).latest, Pipeline.readCustomers(spark, "data/customers.csv")).localCheckpoint()
    (r, exp, Pipeline.rejected(v).schema)
  }

  /** Thay nội dung một bảng partition theo `day` bằng `f(bảng cũ)` — mô phỏng ai đó/bug khác ghi đè output. */
  private def rewrite(path: String)(f: DataFrame => DataFrame): Unit = {
    val staged = path + "_tampered"
    f(spark.read.parquet(path)).write.mode("overwrite").partitionBy("day").parquet(staged)
    val fs = new org.apache.hadoop.fs.Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(path), true)
    fs.rename(new org.apache.hadoop.fs.Path(staged), new org.apache.hadoop.fs.Path(path))
    ()
  }

  test("bước 10: đối soát phát hiện output bị ghi thêm (ví dụ ai đó chạy append)") {
    val out = tmp("ch24-tamper").toString
    val (r, exp, rejSchema) = runAndExpected(out)
    val paths = Pipeline.Paths(out)
    val extra = spark.read.parquet(paths.facts).filter(col("order_id") === "O2")
    extra.write.mode("append").partitionBy("day").parquet(paths.facts)
    val failed = Pipeline.verify(spark, paths, r, exp, rejSchema)
    assert(failed.exists(_.contains("khóa (order_id, sku) trùng")) && failed.exists(_.contains("fact đọc lại khác fact đã tính: thiếu 0, thừa 1")),
      failed.toString)
  }

  test("bước 10 (review T14 M3): sửa NỘI DUNG giữ nguyên số dòng/tổng, daily sai grain, mất bảng rejected — đều bị bắt") {
    val out = tmp("ch24-tamper2").toString
    val (r, exp, rejSchema) = runAndExpected(out)
    val paths = Pipeline.Paths(out)
    rewrite(paths.facts)(_.withColumn("unit_price_vnd", lit(1L)))
    assert(Pipeline.verify(spark, paths, r, exp, rejSchema).exists(_.startsWith("fact đọc lại khác fact đã tính: thiếu 6, thừa 6")))

    val out2 = tmp("ch24-tamper3").toString
    val (r2, exp2, rej2) = runAndExpected(out2)
    val p2 = Pipeline.Paths(out2)
    rewrite(p2.daily)(_.withColumn("segment", lit("WRONG")).withColumn("orders", lit(999L))) // tổng revenue KHÔNG đổi
    val f2 = Pipeline.verify(spark, p2, r2, exp2, rej2)
    assert(f2.exists(_.startsWith("daily đọc lại khác daily tính từ fact đọc lại")) && f2.exists(_.contains("(day, segment) trùng")), f2.toString)
    assert(!f2.exists(_.startsWith("doanh thu đọc lại")), f2.toString)

    val fs = new org.apache.hadoop.fs.Path(p2.rejects).getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(p2.rejects), true)
    assert(Pipeline.verify(spark, p2, r2, exp2, rej2).contains("rejected đọc lại 0 dòng != đã tính 3"))
  }

  /** Input tạm với nhiều lô: (ngày lô, nội dung CSV không header). */
  private def inputBatches(batches: (String, String)*): String = {
    val root = tmp("ch24-multi")
    batches.foreach { case (d, body) =>
      Files.write(Files.createDirectories(root.resolve(s"ingest_date=$d")).resolve("orders.csv"), (header + body).getBytes("UTF-8"))
    }
    root.toString
  }
  private def runOn(in: String, out: String, upTo: String) = Pipeline.run(spark, in, "data/customers.csv", out, LocalDate.parse(upTo))
  /** Số dòng của một bảng; bảng không còn file Parquet nào (snapshot rỗng) là 0 — `read.parquet` không suy được schema cho nó. */
  private def count(path: String): Long = {
    val s = Files.walk(java.nio.file.Paths.get(path))
    val hasData = try s.anyMatch(_.getFileName.toString.endsWith(".parquet")) finally s.close()
    if (hasData) spark.read.parquet(path).count() else 0L
  }

  test("review T14 M1: output rỗng hợp lệ — chỉ header, mọi dòng bị loại, chỉ có REFUNDED, upTo trước mọi lô — trả báo cáo, không ném lỗi") {
    val cases = Seq(
      "chỉ header"    -> (inputBatches("2026-10-20" -> ""), "2026-10-20"),
      "NULL updated"  -> (inputBatches("2026-10-20" -> "X,C1,2026-10-20,PAID,KB-01,1,100,\n"), "2026-10-20"),
      "chỉ REFUNDED"  -> (inputBatches("2026-10-20" -> "X,C1,2026-10-20,REFUNDED,KB-01,1,100,2026-10-20T01:00:00Z\n"), "2026-10-20"),
      "upTo quá sớm"  -> ("data/input", "2026-10-01"))
    cases.foreach { case (name, (in, upTo)) =>
      val r = runOn(in, tmp("ch24-empty").toString, upTo)
      assert(r.ok, s"$name: ${r.failedChecks}")
      assertEquals(r.paidRevenueVnd, BigDecimal(0), name)
    }
  }

  test("review T14 M2: snapshot — xung đột mới xóa fact cũ; hoàn tiền xóa partition daily; GIẢM upTo xóa ngày 16") {
    val pay = "X,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T01:00:00Z\n"
    // (a) lô sau thêm bản CANCELLED CÙNG updated_at → cả hai thành xung đột → không còn fact nào được phục vụ.
    val in1 = inputBatches("2026-10-20" -> pay, "2026-10-21" -> "X,C1,2026-10-20,CANCELLED,KB-01,1,100,2026-10-20T01:00:00Z\n")
    val o1  = tmp("ch24-snap1").toString
    assert(runOn(in1, o1, "2026-10-20").ok)
    val r1 = runOn(in1, o1, "2026-10-21")
    assert(r1.ok, r1.failedChecks.toString)
    assertEquals((count(Pipeline.Paths(o1).facts), count(Pipeline.Paths(o1).conflicts)), (0L, 2L))
    assertEquals(r1.removedPartitions, List("daily_revenue/day=2026-10-20", "fact_order_lines/day=2026-10-20"))

    // (b) hoàn tiền muộn: fact còn (REFUNDED) nhưng daily ngày 20 không còn dòng PAID → partition daily phải biến mất.
    val in2 = inputBatches("2026-10-20" -> pay, "2026-10-21" -> "X,C1,2026-10-20,REFUNDED,KB-01,1,100,2026-10-21T01:00:00Z\n")
    val o2  = tmp("ch24-snap2").toString
    runOn(in2, o2, "2026-10-20")
    val r2 = runOn(in2, o2, "2026-10-21")
    assert(r2.ok, r2.failedChecks.toString)
    assertEquals((count(Pipeline.Paths(o2).facts), count(Pipeline.Paths(o2).daily)), (1L, 0L))

    // (c) dữ liệu mẫu: upTo=16 rồi upTo=15 trên CÙNG output → đúng snapshot của lô 15 (21.600.000), ngày 16 bị xóa.
    val o3 = tmp("ch24-snap3").toString
    Pipeline.run(spark, "data/input", "data/customers.csv", o3, d16)
    val r3 = Pipeline.run(spark, "data/input", "data/customers.csv", o3, d15)
    assert(r3.ok, r3.failedChecks.toString)
    assertEquals(dailyRows(o3).map(_._3).sum, BigDecimal(21600000))
    assert(r3.removedPartitions.contains("daily_revenue/day=2026-10-16") && r3.removedPartitions.contains("rejected/ingest_date=2026-10-16"),
      r3.removedPartitions.toString)
  }

  test("review T14 M4: cú pháp ngày/timestamp — '2026' và giờ không múi giờ bị loại; Z và +07:00 cùng instant là trùng tuyệt đối") {
    val in = inputBatches("2026-10-20" -> (
      "A,C1,2026,PAID,KB-01,1,100,2026-10-20T01:00:00Z\n" +
      "B,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T01:00:00\n" +
      "C,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T01:00:00Z\n" +
      "C,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T08:00:00+07:00\n"))
    val v = Pipeline.validate(Pipeline.readRaw(spark, in, LocalDate.parse("2026-10-20")))
    val errs = Pipeline.rejected(v).select(col("raw.order_id"), col("errors")).collect().map(r => r.getString(0) -> r.getSeq[String](1)).toMap
    assertEquals(errs, Map("A" -> Seq("day: phải là ngày yyyy-MM-dd"), "B" -> Seq("updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)")))
    val d = Pipeline.dedup(Pipeline.accepted(v))
    assertEquals((d.exactDuplicates.count(), d.latest.count(), d.conflicts.count()), (1L, 1L, 0L))
    // Review vòng 2: `AS TIMESTAMP` là alias theo spark.sql.timestampType — với NTZ, offset bị BỎ QUA và hai cách viết cùng một instant
    // thành 01:00 và 08:00. Ép TIMESTAMP_LTZ tường minh thì kết quả không phụ thuộc cấu hình phiên.
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    try {
      val v2 = Pipeline.validate(Pipeline.readRaw(spark, in, LocalDate.parse("2026-10-20")))
      val d2 = Pipeline.dedup(Pipeline.accepted(v2))
      assertEquals((d2.exactDuplicates.count(), d2.latest.count(), d2.conflicts.count()), (1L, 1L, 0L))
    } finally spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
  }

  private def threeDays: DataFrame = spark.range(0, 10000, 1, 8).select(
    (col("id") % 3).cast("int").as("k"), date_add(lit(java.sql.Date.valueOf("2026-10-15")), (col("id") % 3).cast("int")).as("day"))

  test("small files: 8 task ghi thẳng → 8 file mỗi ngày; repartition(day) trước → 1 file mỗi ngày") {
    assertEquals(Experiments.smallFiles(threeDays, 8, tmp("sf1"), repartitionByDay = false).values.toSet, Set(8))
    assertEquals(Experiments.smallFiles(threeDays, 8, tmp("sf2"), repartitionByDay = true).values.toSet, Set(1))
  }

  test("chế độ ghi: append nhân đôi khi chạy lại; overwrite tĩnh xóa ngày khác; overwrite động giữ ngày khác") {
    val all = Set("day=2026-10-15", "day=2026-10-16", "day=2026-10-17")
    assertEquals(Experiments.writeMode(threeDays, tmp("m1"), "append"), Experiments.ModeResult(10000, 20000, all))
    assertEquals(Experiments.writeMode(threeDays, tmp("m2"), "static"), Experiments.ModeResult(10000, 10000, Set("day=2026-10-16")))
    assertEquals(Experiments.writeMode(threeDays, tmp("m3"), "dynamic"), Experiments.ModeResult(10000, 10000, all))
  }

  test("đọc output theo ngày: PartitionFilters — chỉ mở thư mục day=2026-10-16") {
    val out = tmp("ch24-prune").toString
    Pipeline.run(spark, "data/input", "data/customers.csv", out, d16)
    val scan = Experiments.prunedScan(spark, Pipeline.Paths(out).facts, "2026-10-16")
    assert(scan.contains("PartitionFilters: [isnotnull(day") && scan.contains("= 2026-10-16)]"), scan)
  }
}
