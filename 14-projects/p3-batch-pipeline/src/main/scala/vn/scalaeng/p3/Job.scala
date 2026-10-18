package vn.scalaeng.p3

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.util.Try

final case class Config(orders: String, clicks: String, customers: String, output: String, runDate: LocalDate, maxRejectRate: Double, runId: String)

object Config {
  val Keys = Seq("orders", "clicks", "customers", "output", "run-date", "max-reject-rate", "run-id")

  /** `--key=value`; đường dẫn và ngày chạy BẮT BUỘC; gom mọi lỗi (Chương 26). Tham số rỗng hoặc lặp lại là lỗi, không âm thầm dùng mặc định. */
  def parse(args: Seq[String]): Either[List[String], Config] = {
    val (pairs, bad) = args.partition(a => a.startsWith("--") && a.contains("="))
    val kvs = pairs.map { a => val (k, v) = a.drop(2).span(_ != '='); k -> v.drop(1).trim }
    val kv = kvs.toMap
    val duplicated = kvs.groupBy(_._1).collect { case (k, vs) if vs.size > 1 => s"tham số --$k lặp lại" }.toList.sorted
    val empty = kvs.collect { case (k, "") if Keys.contains(k) => k }.distinct
    def req(k: String) = kv.get(k).filter(_.nonEmpty).toRight(s"thiếu --$k")
    val date = req("run-date").flatMap(v => Try(LocalDate.parse(v)).toOption.toRight(s"--run-date='$v': ngày dạng yyyy-MM-dd"))
    val rate = kv.get("max-reject-rate").filter(_.nonEmpty).fold[Either[String, Double]](Right(0.05))(v =>
      v.toDoubleOption.filter(r => !r.isNaN && r >= 0 && r <= 1).toRight(s"--max-reject-rate='$v': số thực trong [0, 1]"))
    val runId = kv.get("run-id").filter(_.nonEmpty).fold[Either[String, String]](Right(UUID.randomUUID().toString))(v =>
      Option(v).filter(_.matches("[A-Za-z0-9_.-]{1,64}")).toRight(s"--run-id='$v': 1..64 ký tự [A-Za-z0-9_.-]"))
    val fields = List(req("orders"), req("clicks"), req("customers"), req("output"), date, rate, runId)
    val errors = bad.map(a => s"tham số '$a' phải có dạng --key=value").toList ++ (kv.keySet -- Keys).toList.sorted.map(k => s"tham số lạ --$k") ++
      duplicated ++ empty.map(k => s"--$k= rỗng") ++ fields.collect { case Left(e) if !empty.exists(k => e == s"thiếu --$k") => e }
    if (errors.nonEmpty) Left(errors)
    else Right(Config(kv("orders"), kv("clicks"), kv("customers"), kv("output"), date.toOption.get, rate.toOption.get, runId.toOption.get))
  }
}

object ExitCode { val Succeeded = 0; val Failed = 1; val ConfigInvalid = 2; val DataRejected = 3; val AuditFailed = 4 }

/** Đếm của MỘT lần chạy — vào manifest, và là cơ sở của audit. */
final case class Counts(
  ordersInput: Long, ordersRejected: Long, ordersExactDup: Long, ordersSuperseded: Long, ordersConflict: Long, factLines: Long,
  clicksInput: Long, clicksRejected: Long, clicksDup: Long, clicksConflict: Long, clicksKept: Long, paidRevenueVnd: BigDecimal)

object Job {
  private def log(runId: String, sink: String => Unit, level: String, event: String, fields: (String, Any)*): Unit =
    sink(Json.obj(Seq("ts" -> Instant.now().toString, "level" -> level, "run_id" -> runId, "event" -> event) ++ fields: _*))

  private def rate(df: DataFrame, d: LocalDate): (Long, Long) = {
    val r = df.filter(col("ingest_date") === lit(java.sql.Date.valueOf(d))).agg(count(lit(1)), sum(when(size(col("errors")) > 0, 1).otherwise(0))).first()
    (r.getLong(0), if (r.isNullAt(1)) 0L else r.getLong(1))
  }

  val PartitionedTables: Seq[String] = Seq("fact_order_lines", "daily_revenue", "campaign_daily")
  val WholeTables: Seq[String]       = Seq("rejected_orders", "rejected_clicks", "order_conflicts", "click_conflicts")

  /** Thứ audit cần để so NỘI DUNG: fact đã tính, click đã dedup, và schema của từng bảng (để đọc lại bảng rỗng). */
  final case class Expected(facts: DataFrame, keptClicks: DataFrame, schemas: Map[String, StructType])

  /** Đọc lại một bảng staging với schema TƯỜNG MINH: bảng rỗng là kết quả hợp lệ, không có file để suy schema (review P3 M4). */
  private def readBack(spark: SparkSession, p: Path, schema: StructType): DataFrame =
    if (Files.exists(p)) spark.read.schema(schema).parquet(p.toString)
    else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  private def diff(expected: DataFrame, actual: DataFrame): Long = {
    val a = actual.select(expected.columns.toSeq.map(col): _*)
    expected.exceptAll(a).count() + a.exceptAll(expected).count()
  }

  /** Audit đọc lại STAGING — chưa ai thấy dữ liệu này — và so NỘI DUNG, không chỉ tổng (review P3 M1: sửa giá, segment, campaign mà giữ nguyên
    * tổng thì bản đầu vẫn publish). Bảng tổng hợp được TÍNH LẠI từ fact đọc lại rồi so từng dòng; bảng cách ly được đếm lại.
    */
  def audit(spark: SparkSession, staging: Path, c: Counts, e: Expected): List[String] = {
    def back(t: String) = readBack(spark, staging.resolve(t), e.schemas(t))
    val facts = back("fact_order_lines").select(e.facts.columns.toSeq.map(col): _*)
    val factDiff = diff(e.facts, facts)
    val dupKey = facts.groupBy("order_id", "sku").count().filter(col("count") > 1).count()

    val dailyExp = Reports.dailyRevenue(facts)
    val daily    = back("daily_revenue")
    val dailyDiff = diff(dailyExp, daily)
    val dailyGrain = daily.groupBy("day", "segment").count().filter(col("count") > 1).count()
    val rev = Option(daily.agg(sum("revenue_vnd")).first().getDecimal(0)).map(BigDecimal(_)).getOrElse(BigDecimal(0))

    val campExp = Reports.campaignDaily(e.keptClicks, facts)
    val camp    = back("campaign_daily")
    val campDiff = diff(campExp, camp)
    val campGrain = camp.groupBy("day", "campaign").count().filter(col("count") > 1).count()
    val campRange = camp.filter(col("users") < 1 || col("converted_users") < 0 || col("converted_users") > col("users") || col("clicks") < col("users") ||
      col("assisted_revenue_vnd") < 0 || col("conversion_rate") < 0 || col("conversion_rate") > 1).count()
    val clicks = Option(camp.agg(sum("clicks")).first().get(0)).map(_.toString.toLong).getOrElse(0L)

    def rows(t: String) = back(t).count()
    List(
      Option.when(c.ordersInput != c.ordersRejected + c.ordersExactDup + c.ordersSuperseded + c.ordersConflict + c.factLines)(
        s"đơn: input ${c.ordersInput} != loại + trùng + thay thế + xung đột + fact"),
      Option.when(c.clicksInput != c.clicksRejected + c.clicksDup + c.clicksConflict + c.clicksKept)(s"click: input ${c.clicksInput} không khớp các ngăn"),
      Option.when(factDiff > 0)(s"fact staging khác fact đã tính: $factDiff dòng lệch"),
      Option.when(dupKey > 0)(s"fact staging có $dupKey khóa (order_id, sku) trùng"),
      Option.when(dailyDiff > 0)(s"daily_revenue staging khác bảng tính lại từ fact: $dailyDiff dòng lệch"),
      Option.when(dailyGrain > 0)(s"daily_revenue có $dailyGrain cặp (day, segment) trùng"),
      Option.when(rev != c.paidRevenueVnd)(s"doanh thu staging $rev != ${c.paidRevenueVnd}"),
      Option.when(campDiff > 0)(s"campaign_daily staging khác bảng tính lại từ click + fact: $campDiff dòng lệch"),
      Option.when(campGrain > 0)(s"campaign_daily có $campGrain cặp (day, campaign) trùng"),
      Option.when(campRange > 0)(s"campaign_daily có $campRange dòng ngoài miền (users, converted_users, clicks, rate)"),
      Option.when(clicks != c.clicksKept)(s"tổng click staging $clicks != ${c.clicksKept}"),
      Option.when(rows("rejected_orders") != c.ordersRejected)(s"rejected_orders staging ${rows("rejected_orders")} != ${c.ordersRejected}"),
      Option.when(rows("rejected_clicks") != c.clicksRejected)(s"rejected_clicks staging ${rows("rejected_clicks")} != ${c.clicksRejected}"),
      Option.when(rows("order_conflicts") != c.ordersConflict)(s"order_conflicts staging ${rows("order_conflicts")} != ${c.ordersConflict}"),
      Option.when(rows("click_conflicts") != c.clicksConflict)(s"click_conflicts staging ${rows("click_conflicts")} != ${c.clicksConflict}")
    ).flatten
  }

  /** Ghi atomic, KHÔNG ghi đè (manifest là lịch sử): file tạm tên duy nhất rồi ATOMIC_MOVE; đích tồn tại → lỗi. */
  private def writeNew(p: Path, s: String): Unit = {
    Files.createDirectories(p.getParent)
    val tmp = Files.createTempFile(p.getParent, p.getFileName.toString + ".", ".tmp")
    try { Files.write(tmp, s.getBytes(StandardCharsets.UTF_8)); Files.move(tmp, p, StandardCopyOption.ATOMIC_MOVE); () }
    finally Files.deleteIfExists(tmp)
  }

  /** Điểm vào cho test (session có sẵn). `tamper` sửa staging trước audit; `beforeMove` chèn lỗi vào publish — CHỈ dành cho test. */
  def run(spark: SparkSession, args: Seq[String], sink: String => Unit, tamper: Path => Unit = _ => (),
      beforeMove: Publish.Move => Unit = _ => ()): Int =
    launch(args, sink, () => spark, stopAfter = false, tamper, beforeMove)

  /** Parse TRƯỚC, tạo session SAU (review P3 M6 / Ch26 M1): cấu hình sai không khởi động Spark. */
  def launch(args: Seq[String], sink: String => Unit, newSession: () => SparkSession, stopAfter: Boolean,
      tamper: Path => Unit = _ => (), beforeMove: Publish.Move => Unit = _ => ()): Int =
    Config.parse(args) match {
      case Left(es) => log("-", sink, "ERROR", "config_invalid", "errors" -> es); ExitCode.ConfigInvalid
      case Right(c) =>
        val spark = newSession()
        try runConfigured(spark, c, sink, tamper, beforeMove) finally if (stopAfter) spark.stop()
    }

  private def runConfigured(spark: SparkSession, c: Config, sink: String => Unit, tamper: Path => Unit, beforeMove: Publish.Move => Unit): Int = {
    val t0 = System.nanoTime()
    val executionId = s"${Instant.now().toString.replace(":", "").replace(".", "")}-${UUID.randomUUID().toString.take(8)}"
    val out = Paths.get(c.output)
    val staging = out.resolve("_staging").resolve(executionId)
    val trash   = out.resolve("_trash").resolve(executionId)
    val journal = out.resolve("_publish").resolve(s"$executionId.journal")
    // `day` của click = ngày theo múi giờ PHIÊN của `ts`: chốt UTC ở ranh giới job, không tin session do người gọi đưa vào (review P3 M5).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    log(c.runId, sink, "INFO", "job_started", "execution_id" -> executionId, "run_date" -> c.runDate.toString, "spark" -> spark.version)
    var details = Map.empty[String, Any]
    var keepStaging = false
    val (code, status) = try {
      // `validated` được đọc nhiều lần (gate, tách, đếm, dedup) → cache MỘT lần; gỡ trong `finally`.
      val oAll = Orders.validate(Orders.read(spark, c.orders, c.runDate)).cache()
      val cAll = Clicks.validate(Clicks.read(spark, c.clicks, c.runDate)).cache()
      try {
        val (ov, cv) = (Common.split(oAll), Common.split(cAll))
        val gates = Seq("orders" -> oAll, "clicks" -> cAll).map { case (name, v) => val (n, bad) = rate(v, c.runDate); (name, n, bad) }
        val blocked = gates.collect {
          case (name, 0L, _) => s"$name: không có lô ingest_date=${c.runDate}"
          case (name, n, bad) if bad.toDouble / n > c.maxRejectRate =>
            f"$name: lô ${c.runDate} có $bad/$n dòng bị loại (${bad * 100.0 / n}%.1f%%) > ngưỡng ${c.maxRejectRate * 100}%.1f%%"
        }
        if (blocked.nonEmpty) {
          blocked.foreach(r => log(c.runId, sink, "ERROR", "quality_gate_blocked", "reason" -> r))
          details = Map("blocked" -> blocked); (ExitCode.DataRejected, "DATA_REJECTED")
        } else {
          gates.foreach { case (n, rows, bad) => log(c.runId, sink, "INFO", "quality_gate_passed", "source" -> n, "rows" -> rows, "rejected" -> bad) }
          val customers = spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "FAILFAST")
            .schema("customer_id STRING, segment STRING, city STRING").csv(c.customers)
            .select(upper(trim(col("customer_id"))).as("customer_id"), trim(col("segment")).as("segment"))
          val od = Orders.dedup(ov.accepted)
          val (clicks, clickConflicts, clickDup) = Clicks.dedup(cv.accepted)
          val facts = Reports.orderFacts(od.latest, customers).cache()
          val kept  = clicks.cache()
          try {
            val daily = Reports.dailyRevenue(facts)
            val camp  = Reports.campaignDaily(kept, facts)
            Publish.stage(facts, staging, "fact_order_lines")
            Publish.stage(daily, staging, "daily_revenue")
            Publish.stage(camp, staging, "campaign_daily")
            val whole = Seq("rejected_orders" -> ov.rejected, "rejected_clicks" -> cv.rejected, "order_conflicts" -> od.conflicts,
              "click_conflicts" -> clickConflicts) // click xung đột được LƯU để truy vết như đơn xung đột (review P3 m2)
            whole.foreach { case (t, df) => df.write.mode("errorifexists").parquet(staging.resolve(t).toString) }
            val counts = Counts(
              ordersInput = ov.accepted.count() + ov.rejected.count(), ordersRejected = ov.rejected.count(), ordersExactDup = od.exactDuplicates.count(),
              ordersSuperseded = od.superseded.count(), ordersConflict = od.conflicts.count(), factLines = facts.count(),
              clicksInput = cv.accepted.count() + cv.rejected.count(), clicksRejected = cv.rejected.count(), clicksDup = clickDup,
              clicksConflict = clickConflicts.count(), clicksKept = kept.count(),
              paidRevenueVnd = Option(daily.agg(sum("revenue_vnd")).first().getDecimal(0)).map(BigDecimal(_)).getOrElse(BigDecimal(0)))
            details = Map("counts" -> counts.productElementNames.zip(counts.productIterator).toMap)
            tamper(staging)
            val schemas = Map("fact_order_lines" -> facts.schema, "daily_revenue" -> daily.schema, "campaign_daily" -> camp.schema) ++
              whole.map { case (t, df) => t -> df.schema }
            val problems = audit(spark, staging, counts, Expected(facts, kept, schemas))
            if (problems.nonEmpty) {
              problems.foreach(p => log(c.runId, sink, "ERROR", "audit_failed", "problem" -> p))
              details += ("audit" -> problems); (ExitCode.AuditFailed, "AUDIT_FAILED")
            } else {
              // Output là SNAPSHOT của input ≤ run-date: mọi partition đang phục vụ mà không còn trong kết quả đều bị gỡ (review P3 M3).
              val planned = PartitionedTables.map(t => Publish.planPartitioned(staging, out, trash, t))
              val moves   = planned.flatMap(_._1).toList ++ WholeTables.flatMap(t => Publish.planWhole(staging, out, trash, t))
              val pre     = Publish.preflight(moves, staging, trash)
              if (pre.nonEmpty) {
                pre.foreach(p => log(c.runId, sink, "ERROR", "publish_preflight_failed", "problem" -> p))
                details += ("publish" -> pre); (ExitCode.Failed, "PUBLISH_PREFLIGHT_FAILED")
              } else {
                PartitionedTables.foreach(t => Files.createDirectories(out.resolve(t))) // bảng rỗng vẫn có thư mục để người đọc mở được
                try {
                  Publish.execute(moves, journal, beforeMove)
                  planned.map(_._2).foreach(p => log(c.runId, sink, "INFO", "published", "table" -> p.table, "replaced" -> p.replaced.toSeq.sorted,
                    "added" -> p.added.toSeq.sorted, "removed" -> p.removed.toSeq.sorted))
                  Publish.deleteTree(trash) // bản cũ đã được thay thành công; nhật ký `_publish/` giữ lại làm bằng chứng
                  (ExitCode.Succeeded, "SUCCEEDED")
                } catch {
                  case f: Publish.PublishFailed if f.rolledBack =>
                    log(c.runId, sink, "ERROR", "publish_failed_rolled_back", "error" -> f.getMessage, "journal" -> f.journal.toString)
                    Publish.deleteTree(trash); details += ("publish" -> f.getMessage); (ExitCode.Failed, "PUBLISH_ROLLED_BACK")
                  case f: Publish.PublishFailed =>
                    keepStaging = true // đảo ngược hỏng: GIỮ staging + thùng rác + nhật ký để phục hồi bằng tay (runbook trong README)
                    log(c.runId, sink, "ERROR", "publish_partial", "error" -> f.getMessage, "journal" -> f.journal.toString,
                      "staging" -> staging.toString, "trash" -> trash.toString)
                    details += ("publish" -> f.getMessage); (ExitCode.Failed, "PUBLISH_PARTIAL")
                }
              }
            }
          } finally { facts.unpersist(blocking = true); kept.unpersist(blocking = true); () }
        }
      } finally { oAll.unpersist(blocking = true); cAll.unpersist(blocking = true); () }
    } catch {
      case e: Throwable if Iterator.iterate(e)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[IllegalArgumentException]) =>
        log(c.runId, sink, "ERROR", "data_contract_violation", "error" -> e.toString); details = Map("error" -> e.toString)
        (ExitCode.DataRejected, "DATA_REJECTED")
      case e: Throwable =>
        log(c.runId, sink, "ERROR", "job_failed", "error" -> e.toString); details = Map("error" -> e.toString)
        (ExitCode.Failed, "FAILED")
    } finally if (!keepStaging) Try(Publish.deleteTree(staging))

    // Finalization: manifest một file mỗi lần THỰC THI, không ghi đè. Hỏng (đĩa đầy, sai quyền) → mã 1 có log, không ném ra ngoài.
    val ms = (System.nanoTime() - t0) / 1000000
    Try {
      writeNew(out.resolve("_runs").resolve(s"${c.runId}--$executionId.json"), Json.obj(
        "run_id" -> c.runId, "execution_id" -> executionId, "status" -> status, "exit_code" -> code, "duration_ms" -> ms,
        "config" -> Map[String, Any]("orders" -> c.orders, "clicks" -> c.clicks, "customers" -> c.customers, "output" -> c.output,
          "run_date" -> c.runDate.toString, "max_reject_rate" -> c.maxRejectRate),
        "versions" -> Map("spark" -> spark.version, "scala" -> scala.util.Properties.versionNumberString, "java" -> System.getProperty("java.version")),
        "details" -> details))
      log(c.runId, sink, "INFO", "job_finished", "status" -> status, "exit_code" -> code, "duration_ms" -> ms, "execution_id" -> executionId)
    }.fold(e => { Try(log(c.runId, sink, "ERROR", "finalization_failed", "status" -> status, "error" -> e.toString)); ExitCode.Failed }, _ => code)
  }

  def session(): SparkSession = {
    val b = SparkSession.builder().appName("p3-batch-pipeline").config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.artifact.isolation.enabled", "false") // một session/job; tránh tải class qua RPC (Chương 24 mục 10)
    if (!sys.props.contains("spark.master")) b.master("local[2]").config("spark.ui.enabled", "false")
    if (System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win"))
      b.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    b.getOrCreate()
  }

  def main(args: Array[String]): Unit = sys.exit(launch(args.toSeq, println, () => session(), stopAfter = true))
}
