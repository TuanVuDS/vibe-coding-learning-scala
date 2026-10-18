package vn.scalaeng.batch

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.{Path => HPath}

import java.time.LocalDate

/** Pipeline batch 10 bước cho đơn hàng (Chương 24):
  *   1 read → 2 kiểm tra schema → 3 validate → 4 chuẩn hóa → 5 tách bản ghi lỗi → 6 dedup → 7 join → 8 aggregate → 9 write → 10 kiểm tra kết quả.
  *
  * Thiết kế chính:
  *   - Lớp RAW đọc MỌI cột là String: lỗi kiểu thành LÝ DO có cấu trúc thay vì null âm thầm (PERMISSIVE) hay dừng cả job (FAILFAST).
  *   - Mỗi lần chạy TÍNH LẠI từ toàn bộ input tới `upTo` rồi ghi mỗi bảng thành SNAPSHOT (ghi đè động + xóa partition mồ côi) → chạy lại cùng
  *     input cho cùng output (idempotent), và một bản cập nhật đến muộn (hoàn tiền O1 ở lô 16/10) sửa đúng partition ngày 15/10.
  */
object Pipeline {
  val Columns: Seq[String] = Seq("order_id", "customer_id", "day", "status", "sku", "quantity", "unit_price_vnd", "updated_at")
  val Statuses: Seq[String] = Seq("PAID", "REFUNDED", "CANCELLED")
  val CorruptColumn = "_corrupt_record"

  /** Schema RAW: 8 cột String + cột giữ nguyên văn dòng sai số cột. `ingest_date` KHÔNG khai báo: Spark tự phát hiện từ tên thư mục
    * `ingest_date=YYYY-MM-DD` (partition discovery).
    */
  val rawSchema: StructType =
    StructType(Columns.map(StructField(_, StringType, nullable = true)) :+ StructField(CorruptColumn, StringType, nullable = true))

  val customerSchema: StructType = StructType(Seq(
    StructField("customer_id", StringType, nullable = true),
    StructField("segment", StringType, nullable = true),
    StructField("city", StringType, nullable = true)
  ))

  // ---------- 1. Read + 2. Schema ----------

  /** Bước 1 + 2. `enforceSchema=false`: Spark so TÊN cột trong header của từng file với schema — file đổi thứ tự/thiếu cột làm job lỗi
    * (`CSV header does not conform to the schema`) thay vì tráo giá trị âm thầm (Chương 23 mục 3.1).
    * Lọc `ingest_date <= upTo` trên cột partition → Spark chỉ mở các thư mục cần (PartitionFilters).
    */
  def readRaw(spark: SparkSession, inputRoot: String, upTo: LocalDate): DataFrame =
    spark.read
      .option("header", "true")
      .option("enforceSchema", "false")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", CorruptColumn)
      .schema(rawSchema)
      .csv(inputRoot)
      .filter(col("ingest_date") <= lit(java.sql.Date.valueOf(upTo)))

  /** Bước 2 (phần còn lại): cột partition phải được phát hiện đúng kiểu — `ingest_date` là DATE, không phải String. */
  def checkSchema(raw: DataFrame): Either[String, DataFrame] = {
    val missing = (Columns :+ "ingest_date").filterNot(raw.columns.contains)
    if (missing.nonEmpty) Left(s"thiếu cột: ${missing.mkString(", ")}")
    else if (raw.schema("ingest_date").dataType != DateType) Left(s"ingest_date phải là date, nhận ${raw.schema("ingest_date").dataType.simpleString}")
    else Right(raw)
  }

  def readCustomers(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "FAILFAST").schema(customerSchema).csv(path)
      .select(upper(trim(col("customer_id"))).as("customer_id"), trim(col("segment")).as("segment"), trim(col("city")).as("city"))

  // ---------- 3. Validate + 4. Normalize ----------

  private def blank(c: Column): Column = c.isNull || trim(c) === ""

  /** Hợp đồng CÚ PHÁP, kiểm tra TRƯỚC `try_cast`: cast của Spark rộng hơn hợp đồng — nhận `"2026"` thành 2026-01-01 và timestamp không có múi giờ
    * (hiểu theo `spark.sql.session.timeZone`, nên cùng chuỗi cho instant khác nhau giữa hai cấu hình). `updated_at` quyết định phiên bản thắng khi
    * dedup, nên BẮT BUỘC có offset (`Z` hoặc `±HH:MM`): khi đó instant không phụ thuộc múi giờ phiên (review T14 M4).
    */
  val DayPattern: String     = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
  val InstantPattern: String = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$"

  /** Bước 3 + 4 trong MỘT lần chiếu: chuẩn hóa (trim, upper — hàm có sẵn, không UDF), kiểm tra cú pháp rồi ép kiểu bằng `try_cast` (giá trị hỏng
    * → NULL thay vì ném lỗi dưới ANSI, ví dụ 2026-02-30), rồi ghi LÝ DO cho từng vi phạm vào mảng `errors`. Một dòng có thể có nhiều lỗi — gom
    * hết như `Validated` của Chương 20.
    */
  def validate(raw: DataFrame): DataFrame = {
    val typed = raw.select(
      col("ingest_date"),
      col(CorruptColumn),
      struct(Columns.map(col): _*).as("raw"),
      trim(col("order_id")).as("order_id"),
      upper(trim(col("customer_id"))).as("customer_id"),
      when(trim(col("day")).rlike(DayPattern), expr("try_cast(trim(day) AS DATE)")).as("day"),
      upper(trim(col("status"))).as("status"),
      upper(trim(col("sku"))).as("sku"),
      expr("try_cast(trim(quantity) AS INT)").as("quantity"),
      expr("try_cast(trim(unit_price_vnd) AS BIGINT)").as("unit_price_vnd"),
      when(trim(col("updated_at")).rlike(InstantPattern), expr("try_cast(trim(updated_at) AS TIMESTAMP_LTZ)")).as("updated_at")
    )
    def rule(failed: Column, reason: String): Column = when(failed, lit(reason))
    val reasons = Seq(
      rule(col(CorruptColumn).isNotNull, "dòng sai số cột"),
      rule(blank(col("raw.order_id")), "order_id: bắt buộc"),
      rule(blank(col("raw.customer_id")), "customer_id: bắt buộc"),
      rule(col("day").isNull, "day: phải là ngày yyyy-MM-dd"),
      rule(!col("status").isin(Statuses: _*) || col("status").isNull, s"status: phải là một trong ${Statuses.mkString("/")}"),
      rule(blank(col("raw.sku")), "sku: bắt buộc"),
      rule(col("quantity").isNull || col("quantity") < 1 || col("quantity") > 1000, "quantity: số nguyên 1..1000"),
      rule(col("unit_price_vnd").isNull || col("unit_price_vnd") <= 0, "unit_price_vnd: số nguyên dương"),
      rule(col("updated_at").isNull, "updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)")
    )
    // filter(isNotNull) bỏ các `when` không khớp (NULL) → chỉ còn lý do của vi phạm thật.
    typed.withColumn("errors", filter(array(reasons: _*), _.isNotNull))
  }

  // ---------- 5. Bản ghi lỗi ----------

  def accepted(validated: DataFrame): DataFrame =
    validated.filter(size(col("errors")) === 0).drop("errors", CorruptColumn, "raw")

  /** Bước 5: dòng lỗi giữ GIÁ TRỊ TRƯỜNG trước chuẩn hóa (`raw` — đã qua CSV parser nên mất dấu nháy, dấu phân cách, xuống dòng) + lý do + lô
    * nhập — đủ để người sửa nguồn hành động và để đối soát số dòng. Cần đúng từng byte gốc để truy vết thì giữ thêm tên file + vị trí record.
    */
  def rejected(validated: DataFrame): DataFrame =
    validated.filter(size(col("errors")) > 0).select(col("ingest_date"), col("raw"), col(CorruptColumn), col("errors"))

  // ---------- 6. Dedup ----------

  val LineKey: Seq[String] = Seq("order_id", "sku")
  private val Payload: Seq[String] = Seq("customer_id", "day", "status", "quantity", "unit_price_vnd")

  final case class Deduped(latest: DataFrame, conflicts: DataFrame, exactDuplicates: DataFrame, superseded: DataFrame)

  /** Bước 6, hai tầng:
    *   (a) TRÙNG TUYỆT ĐỐI (cùng mọi trường nghiệp vụ + updated_at — ví dụ gửi lại cùng file): giữ một, đếm số bị bỏ;
    *   (b) PHIÊN BẢN: mỗi dòng hàng (order_id, sku) giữ bản `updated_at` lớn nhất. Hai bản mới nhất KHÁC nội dung → XUNG ĐỘT (không đoán,
    *       tách ra cách ly) — bài học review chéo Chương 23. `ingest_date` không thuộc so sánh: cùng nội dung đến ở hai lô vẫn là trùng.
    */
  def dedup(acc: DataFrame): Deduped = {
    val business = LineKey ++ Payload :+ "updated_at"
    val byRecord = Window.partitionBy(business.map(col): _*).orderBy(col("ingest_date"))
    val numbered = acc.withColumn("copy", row_number().over(byRecord))
    val exactDuplicates = numbered.filter(col("copy") > 1).drop("copy")
    val unique          = numbered.filter(col("copy") === 1).drop("copy")

    val byLine = Window.partitionBy(LineKey.map(col): _*)
    val ranked = unique
      .withColumn("max_updated_at", max("updated_at").over(byLine))
      .withColumn("is_top", col("updated_at") === col("max_updated_at"))
      .withColumn("top_variants", size(collect_set(when(col("is_top"), struct(Payload.map(col): _*))).over(byLine)))
    Deduped(
      latest = ranked.filter(col("is_top") && col("top_variants") === 1).drop("max_updated_at", "is_top", "top_variants"),
      conflicts = ranked.filter(col("is_top") && col("top_variants") > 1).drop("max_updated_at", "is_top", "top_variants"),
      exactDuplicates = exactDuplicates,
      superseded = ranked.filter(!col("is_top")).drop("max_updated_at", "is_top", "top_variants")
    )
  }

  // ---------- 7. Join ----------

  /** Bước 7. Khóa dimension phải DUY NHẤT trước join — trùng khóa nhân doanh thu (Chương 23, review M3). Bảng khách nhỏ → `broadcast`:
    * gửi nguyên bảng tới mọi task, không shuffle bảng đơn hàng (Chương 25). Left join: đơn của khách không tồn tại → segment UNKNOWN.
    */
  def enrich(latest: DataFrame, customers: DataFrame): DataFrame = {
    val dups = customers.groupBy("customer_id").count().filter(col("count") > 1).limit(3).collect().map(_.getString(0))
    require(dups.isEmpty, s"customers trùng customer_id: ${dups.mkString(", ")}")
    latest.join(broadcast(customers.select("customer_id", "segment")), Seq("customer_id"), "left")
      .withColumn("segment", coalesce(col("segment"), lit("UNKNOWN")))
      .withColumn("revenue_vnd", col("quantity").cast(DecimalType(38, 0)) * col("unit_price_vnd"))
  }

  // ---------- 8. Aggregate ----------

  /** Bước 8. Chỉ dòng PAID. Tiền là DECIMAL(38,0): tổng không tràn như Long và ANSI vẫn bật (Chương 22, 25).
    * Grain đầu ra: một dòng = (day, segment); `order_lines` đếm dòng hàng, `orders` đếm ĐƠN khác nhau.
    */
  def dailyRevenue(facts: DataFrame): DataFrame =
    facts.filter(col("status") === "PAID")
      .groupBy("day", "segment")
      .agg(sum("revenue_vnd").as("revenue_vnd"), count(lit(1)).as("order_lines"), countDistinct("order_id").as("orders"))

  // ---------- 9. Write ----------

  /** Bước 9. Parquet, partition theo `day`.
    *   - `partitionOverwriteMode=dynamic`: chỉ thay các partition CÓ trong dữ liệu mới; partition khác giữ nguyên.
    *   - `repartition(col(part))` trước khi ghi: mọi dòng của một ngày về CÙNG một task → một file mỗi partition (với `maxRecordsPerFile` mặc định
    *     0 = không giới hạn), thay vì tới (số task × số ngày) file nhỏ. Đổi lại một shuffle, và một ngày rất lớn thành MỘT task lớn — chia nó cần
    *     thêm khóa phụ vào `repartition`, không phải `maxRecordsPerFile` (cấu hình đó chỉ cắt file, không chia công việc của task).
    */
  def writeByDay(df: DataFrame, path: String): Unit = writePartitioned(df, path, "day")

  def writePartitioned(df: DataFrame, path: String, part: String): Unit =
    df.repartition(col(part)).write
      .mode("overwrite")
      .option("partitionOverwriteMode", "dynamic")
      .partitionBy(part)
      .parquet(path)

  /** Các giá trị partition đang có trên storage (tên thư mục `part=value`). Dùng Hadoop FileSystem API — chạy được với local, HDFS, S3A. */
  def partitionsOnDisk(spark: SparkSession, path: String, part: String): Set[String] = {
    val p  = new HPath(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(p)) Set.empty
    else fs.listStatus(p).iterator.filter(s => s.isDirectory && s.getPath.getName.startsWith(s"$part=")).map(_.getPath.getName.stripPrefix(s"$part=")).toSet
  }

  /** Ghi một bảng thành SNAPSHOT đầy đủ: ghi động các partition có dữ liệu mới, rồi XÓA các partition không còn trong kết quả (partition mồ côi).
    *
    * Vì sao không dùng overwrite TĨNH (xóa cả thư mục rồi ghi lại)? Overwrite tĩnh có một khoảng thời gian bảng trống, và job chết giữa chừng là
    * mất cả bảng. Cách này giữ mỗi partition ở bản cũ hoặc bản mới; chỉ partition đã không còn dữ liệu mới bị xóa — SAU khi ghi xong. Vẫn KHÔNG
    * phải transaction nhiều partition: người đọc giữa chừng có thể thấy ngày 15 mới + ngày 16 cũ (cần table format như Delta/Iceberg, Chương 31).
    */
  def writeSnapshot(spark: SparkSession, df: DataFrame, path: String, part: String): Set[String] = {
    val expected = df.select(col(part).cast("string")).distinct().collect().map(_.getString(0)).toSet
    writePartitioned(df, path, part)
    val orphans = partitionsOnDisk(spark, path, part) -- expected
    val fs = new HPath(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    orphans.foreach(v => fs.delete(new HPath(path, s"$part=$v"), true))
    orphans
  }

  // ---------- Toàn bộ pipeline + 10. Kiểm tra ----------

  final case class RunReport(
    inputRows: Long,
    rejectedRows: Long,
    acceptedRows: Long,
    exactDuplicateRows: Long,
    supersededRows: Long,
    conflictRows: Long,
    factLines: Long,
    paidRevenueVnd: BigDecimal,
    removedPartitions: List[String],
    failedChecks: List[String]
  ) {
    def ok: Boolean = failedChecks.isEmpty
  }

  final case class Paths(output: String) {
    val facts     = s"$output/fact_order_lines"
    val daily     = s"$output/daily_revenue"
    val rejects   = s"$output/rejected"
    val conflicts = s"$output/conflicts"
  }

  /** Ngữ nghĩa của một lần chạy: output = SNAPSHOT của toàn bộ input có `ingest_date <= upTo`. Chạy lại cùng `upTo` → cùng output; tăng `upTo` →
    * snapshot mới (dữ liệu trễ sửa đúng ngày cũ); GIẢM `upTo` → quay về snapshot cũ hơn, kể cả xóa ngày không còn dữ liệu (review T14 M2).
    * Đổi lại, mỗi lần chạy tính lại TOÀN BỘ lịch sử — đúng cho lab và bảng vừa phải; bảng lớn tính lại theo phạm vi ngày bị chạm (bài 5, Chương 31).
    */
  def run(spark: SparkSession, inputRoot: String, customersPath: String, output: String, upTo: LocalDate): RunReport = {
    val paths = Paths(output)
    val raw = checkSchema(readRaw(spark, inputRoot, upTo)).fold(e => throw new IllegalArgumentException(e), identity)
    // cache: `validated` được đọc bởi nhiều nhánh (accepted, rejected, các phép đếm) — không cache thì CSV bị parse lại mỗi nhánh (Chương 22).
    val validated = validate(raw).cache()
    try {
      val acc   = accepted(validated)
      val rej   = rejected(validated)
      val d     = dedup(acc)
      val facts = enrich(d.latest, readCustomers(spark, customersPath)).cache()
      try {
        val daily = dailyRevenue(facts)

        val removed =
          writeSnapshot(spark, facts, paths.facts, "day").map(v => s"fact_order_lines/day=$v") ++
            writeSnapshot(spark, daily, paths.daily, "day").map(v => s"daily_revenue/day=$v") ++
            writeSnapshot(spark, rej, paths.rejects, "ingest_date").map(v => s"rejected/ingest_date=$v")
        d.conflicts.write.mode("overwrite").parquet(paths.conflicts) // nhỏ, không partition: luôn là danh sách xung đột HIỆN TẠI

        val report = RunReport(
          inputRows = validated.count(),
          rejectedRows = rej.count(),
          acceptedRows = acc.count(),
          exactDuplicateRows = d.exactDuplicates.count(),
          supersededRows = d.superseded.count(),
          conflictRows = d.conflicts.count(),
          factLines = facts.count(),
          paidRevenueVnd = BigDecimal(Option(daily.agg(sum("revenue_vnd")).first().getDecimal(0)).getOrElse(java.math.BigDecimal.ZERO)),
          removedPartitions = removed.toList.sorted,
          failedChecks = Nil
        )
        report.copy(failedChecks = verify(spark, paths, report, facts, rej.schema))
      } finally { facts.unpersist(blocking = true); () }
    } finally { validated.unpersist(blocking = true); () }
  }

  /** Đọc lại một bảng đã ghi với schema TƯỜNG MINH: bảng rỗng (chỉ có `_SUCCESS`, không file Parquet) không suy được schema
    * (`UNABLE_TO_INFER_SCHEMA`) — nhưng bảng rỗng là kết quả HỢP LỆ (mọi dòng bị loại, không có đơn PAID...). Review T14 M1.
    */
  private def readBack(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    val p = new HPath(path)
    // Thư mục bảng biến mất (bị xóa nhầm, ghi lỗi) KHÔNG phải lỗi của verify: đọc như bảng rỗng để phép đối soát báo đúng "thiếu N dòng".
    if (p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)) spark.read.schema(schema).parquet(path)
    else spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
  }

  /** Bước 10. Đối soát — mỗi dòng input phải đi vào ĐÚNG MỘT ngăn, và ĐỌC LẠI output đã ghi (không tin dữ liệu trong bộ nhớ):
    *   (1) input = rejected + accepted; accepted = trùng tuyệt đối + bị thay thế + xung đột + fact;
    *   (2) fact đọc lại BẰNG fact đã tính — so cả nội dung (multiset, `exceptAll` hai chiều), không chỉ số dòng; không trùng (order_id, sku);
    *   (3) daily đọc lại BẰNG daily tính lại từ fact ĐỌC LẠI — bắt daily bị sửa/ghi thêm/thiếu; grain (day, segment) duy nhất;
    *   (4) rejected và conflicts đọc lại đủ số dòng — bản ghi cách ly cũng là output, mất nó là mất dấu vết;
    *   (5) tổng doanh thu đọc lại khớp tổng đã tính.
    * Vì output là snapshot đầy đủ, so TOÀN BỘ bảng: partition mồ côi còn sót hiện ra ở (2), (3) dưới dạng dòng "thừa".
    * Chi phí: `exceptAll` là một shuffle trên cả bảng — với bảng rất lớn, so theo từng partition bằng tổng kiểm (count, sum, hash) thay vì từng dòng.
    */
  def verify(spark: SparkSession, paths: Paths, r: RunReport, expectedFacts: DataFrame, rejectedSchema: StructType): List[String] = {
    val cols         = expectedFacts.columns.toSeq.map(col)
    val written      = readBack(spark, paths.facts, expectedFacts.schema).select(cols: _*)
    val missingFacts = expectedFacts.exceptAll(written).count()
    val extraFacts   = written.exceptAll(expectedFacts).count()
    val dupKeys      = written.groupBy(LineKey.map(col): _*).count().filter(col("count") > 1).count()

    val dailyExpected = dailyRevenue(written)
    val dailyCols     = dailyExpected.columns.toSeq.map(col)
    val dailyWritten  = readBack(spark, paths.daily, dailyExpected.schema).select(dailyCols: _*)
    val dailyDiff     = dailyExpected.exceptAll(dailyWritten).count() + dailyWritten.exceptAll(dailyExpected).count()
    val dailyDupGrain = dailyWritten.groupBy("day", "segment").count().filter(col("count") > 1).count()
    val readBackRevenue = Option(dailyWritten.agg(sum("revenue_vnd")).first().getDecimal(0)).map(BigDecimal(_)).getOrElse(BigDecimal(0))

    val rejectedBack  = readBack(spark, paths.rejects, rejectedSchema).count()
    val conflictsBack = spark.read.parquet(paths.conflicts).count() // luôn có file Parquet (ghi không partition, kể cả khi rỗng)

    List(
      Option.when(r.inputRows != r.rejectedRows + r.acceptedRows)(s"input ${r.inputRows} != rejected ${r.rejectedRows} + accepted ${r.acceptedRows}"),
      Option.when(r.acceptedRows != r.exactDuplicateRows + r.supersededRows + r.conflictRows + r.factLines)(
        s"accepted ${r.acceptedRows} != trùng ${r.exactDuplicateRows} + thay thế ${r.supersededRows} + xung đột ${r.conflictRows} + fact ${r.factLines}"),
      Option.when(missingFacts + extraFacts > 0)(s"fact đọc lại khác fact đã tính: thiếu $missingFacts, thừa $extraFacts dòng"),
      Option.when(dupKeys > 0)(s"fact đọc lại có $dupKeys khóa (order_id, sku) trùng"),
      Option.when(dailyDiff > 0)(s"daily đọc lại khác daily tính từ fact đọc lại: $dailyDiff dòng lệch"),
      Option.when(dailyDupGrain > 0)(s"daily đọc lại có $dailyDupGrain cặp (day, segment) trùng"),
      Option.when(readBackRevenue != r.paidRevenueVnd)(s"doanh thu đọc lại $readBackRevenue != đã tính ${r.paidRevenueVnd}"),
      Option.when(rejectedBack != r.rejectedRows)(s"rejected đọc lại $rejectedBack dòng != đã tính ${r.rejectedRows}"),
      Option.when(conflictsBack != r.conflictRows)(s"conflicts đọc lại $conflictsBack dòng != đã tính ${r.conflictRows}")
    ).flatten
  }
}
