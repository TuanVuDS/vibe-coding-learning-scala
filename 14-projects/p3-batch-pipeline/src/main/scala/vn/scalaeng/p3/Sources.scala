package vn.scalaeng.p3

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate

/** Kết quả đọc + validate một nguồn: dòng hợp lệ (đã chuẩn hóa, đã ép kiểu) và dòng bị loại (giá trị trường sau parser + lý do). */
final case class Validated(accepted: DataFrame, rejected: DataFrame)

object Common {
  val Corrupt = "_corrupt_record"

  def blank(c: Column): Column = c.isNull || trim(c) === ""

  /** Một quy tắc = (vi phạm?, lý do). Gom mọi lý do của một dòng vào mảng `errors`, bỏ NULL. */
  def errors(rules: (Column, String)*): Column = filter(array(rules.map { case (bad, why) => when(bad, lit(why)) }: _*), _.isNotNull)

  /** Hợp đồng CÚ PHÁP kiểm tra TRƯỚC `try_cast` (review P3 M5, cùng lỗi Ch24 M4): cast của Spark nhận `"2026"` thành 2026-01-01 và timestamp
    * không múi giờ theo `spark.sql.session.timeZone`. Timestamp quyết định phiên bản thắng và NGÀY của click → bắt buộc có offset.
    */
  val DayPattern: String     = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
  val InstantPattern: String = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$"
  def dateOf(c: String): Column    = when(trim(col(c)).rlike(DayPattern), expr(s"try_cast(trim($c) AS DATE)"))
  def instantOf(c: String): Column = when(trim(col(c)).rlike(InstantPattern), expr(s"try_cast(trim($c) AS TIMESTAMP_LTZ)"))

  def upTo(df: DataFrame, d: LocalDate): DataFrame = df.filter(col("ingest_date") <= lit(java.sql.Date.valueOf(d)))

  def split(v: DataFrame): Validated = Validated(
    v.filter(size(col("errors")) === 0).drop("errors", "raw", Corrupt),
    v.filter(size(col("errors")) > 0).select(col("ingest_date"), col("raw"), col(Corrupt), col("errors")))
}

/** Nguồn ĐƠN HÀNG: CSV theo lô thư mục `orders/ingest_date=YYYY-MM-DD/` chứa file .csv. Lớp RAW toàn String, `try_cast`, header kiểm tra theo tên (Chương 24). */
object Orders {
  import Common._

  val Columns: Seq[String] = Seq("order_id", "customer_id", "day", "status", "sku", "quantity", "unit_price_vnd", "updated_at")
  val Statuses: Seq[String] = Seq("PAID", "REFUNDED", "CANCELLED")
  val rawSchema: StructType = StructType(Columns.map(StructField(_, StringType)) :+ StructField(Corrupt, StringType))

  def read(spark: SparkSession, root: String, d: LocalDate): DataFrame =
    upTo(spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", Corrupt).schema(rawSchema).csv(root), d)

  /** DataFrame có thêm cột `errors` (mảng lý do); tách hợp lệ/bị loại bằng `Common.split`. */
  def validate(raw: DataFrame): DataFrame = {
    val t = raw.select(
      col("ingest_date"), col(Corrupt), struct(Columns.map(col): _*).as("raw"),
      trim(col("order_id")).as("order_id"),
      upper(trim(col("customer_id"))).as("customer_id"),
      dateOf("day").as("day"),
      upper(trim(col("status"))).as("status"),
      upper(trim(col("sku"))).as("sku"),
      expr("try_cast(trim(quantity) AS INT)").as("quantity"),
      expr("try_cast(trim(unit_price_vnd) AS BIGINT)").as("unit_price_vnd"),
      instantOf("updated_at").as("updated_at"))
    t.withColumn("errors", errors(
      col(Corrupt).isNotNull -> "dòng sai số cột",
      blank(col("raw.order_id")) -> "order_id: bắt buộc",
      blank(col("raw.customer_id")) -> "customer_id: bắt buộc",
      col("day").isNull -> "day: phải là ngày yyyy-MM-dd",
      (col("status").isNull || !col("status").isin(Statuses: _*)) -> s"status: phải là một trong ${Statuses.mkString("/")}",
      blank(col("raw.sku")) -> "sku: bắt buộc",
      (col("quantity").isNull || col("quantity") < 1 || col("quantity") > 1000) -> "quantity: số nguyên 1..1000",
      (col("unit_price_vnd").isNull || col("unit_price_vnd") <= 0) -> "unit_price_vnd: số nguyên dương",
      col("updated_at").isNull -> "updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)"))
  }

  final case class Deduped(latest: DataFrame, conflicts: DataFrame, exactDuplicates: DataFrame, superseded: DataFrame)

  private val LineKey = Seq("order_id", "sku")
  private val Payload = Seq("customer_id", "day", "status", "quantity", "unit_price_vnd")

  /** Trùng tuyệt đối → bỏ; mỗi dòng hàng giữ bản `updated_at` lớn nhất; nhiều bản mới nhất khác nội dung → xung đột (cách ly). */
  def dedup(acc: DataFrame): Deduped = {
    val numbered = acc.withColumn("copy", row_number().over(Window.partitionBy((LineKey ++ Payload :+ "updated_at").map(col): _*).orderBy("ingest_date")))
    val unique = numbered.filter(col("copy") === 1).drop("copy")
    val byLine = Window.partitionBy(LineKey.map(col): _*)
    val ranked = unique
      .withColumn("is_top", col("updated_at") === max("updated_at").over(byLine))
      .withColumn("variants", size(collect_set(when(col("is_top"), struct(Payload.map(col): _*))).over(byLine)))
    def clean(df: DataFrame) = df.drop("is_top", "variants")
    Deduped(clean(ranked.filter(col("is_top") && col("variants") === 1)), clean(ranked.filter(col("is_top") && col("variants") > 1)),
      numbered.filter(col("copy") > 1).drop("copy"), clean(ranked.filter(!col("is_top"))))
  }
}

/** Nguồn CLICK: JSON Lines theo lô thư mục `clicks/ingest_date=YYYY-MM-DD/` chứa file .jsonl. Cùng nguyên tắc: đọc mọi trường là String, tự ép kiểu, ghi lý do. */
object Clicks {
  import Common._

  val Fields: Seq[String] = Seq("event_id", "customer_id", "campaign", "ts")
  val rawSchema: StructType = StructType(Fields.map(StructField(_, StringType)) :+ StructField(Corrupt, StringType))

  def read(spark: SparkSession, root: String, d: LocalDate): DataFrame =
    upTo(spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", Corrupt).schema(rawSchema).json(root), d)

  /** `day` của click = ngày UTC của `ts` (đơn hàng cũng dùng ngày theo UTC của hệ thống nguồn). Đổi sang giờ Việt Nam là một QUYẾT ĐỊNH
    * nghiệp vụ phải áp cho CẢ HAI nguồn cùng lúc — lệch múi giờ giữa hai nguồn làm click 23:30 và đơn 00:10 (giờ VN) rơi vào hai ngày khác nhau.
    */
  def validate(raw: DataFrame): DataFrame = {
    val t = raw.select(
      col("ingest_date"), col(Corrupt), struct(Fields.map(col): _*).as("raw"),
      trim(col("event_id")).as("event_id"),
      upper(trim(col("customer_id"))).as("customer_id"),
      upper(trim(col("campaign"))).as("campaign"),
      instantOf("ts").as("ts"))
      .withColumn("day", to_date(col("ts")))
    t.withColumn("errors", errors(
      col(Corrupt).isNotNull -> "JSON hỏng",
      blank(col("raw.event_id")) -> "event_id: bắt buộc",
      blank(col("raw.customer_id")) -> "customer_id: bắt buộc",
      blank(col("raw.campaign")) -> "campaign: bắt buộc",
      col("ts").isNull -> "ts: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)"))
  }

  /** Click trùng `event_id` (gửi lại): giữ bản đến sớm nhất; cùng ID khác nội dung là xung đột — đếm riêng, không tính vào báo cáo. */
  def dedup(acc: DataFrame): (DataFrame, DataFrame, Long) = {
    val byId = Window.partitionBy("event_id")
    val v = acc.withColumn("variants", size(collect_set(struct(col("customer_id"), col("campaign"), col("ts"))).over(byId)))
      .withColumn("n", row_number().over(byId.orderBy("ingest_date")))
    val ok = v.filter(col("variants") === 1)
    (ok.filter(col("n") === 1).drop("variants", "n"), v.filter(col("variants") > 1).drop("variants", "n"), ok.filter(col("n") > 1).count())
  }
}
