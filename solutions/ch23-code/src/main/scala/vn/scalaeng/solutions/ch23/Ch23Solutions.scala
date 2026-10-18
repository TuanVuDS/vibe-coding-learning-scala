package vn.scalaeng.solutions.ch23

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import vn.scalaeng.sparksql.{OrderRow, Schemas, SqlLab}

/** Bài 1 — PERMISSIVE + cột `_corrupt_record`: giữ dòng hỏng NGUYÊN VĂN để tách ra, thay vì âm thầm thành null. */
object CorruptRecords {
  val schemaWithCorrupt: StructType = Schemas.orders.add(StructField("_corrupt_record", StringType, nullable = true))

  def split(spark: SparkSession, path: String): (DataFrame, DataFrame) = {
    val all = spark.read.option("header", "true").option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record").schema(schemaWithCorrupt).csv(path)
      .cache() // Spark không cho truy vấn chỉ cột _corrupt_record trên dữ liệu CSV chưa cache
    (all.filter(col("_corrupt_record").isNull).drop("_corrupt_record"), all.filter(col("_corrupt_record").isNotNull).select("_corrupt_record"))
  }
}

/** Bài 2 — cùng truy vấn bằng SQL và DataFrame API: Catalyst ra CÙNG kế hoạch tối ưu.
  *
  * Đầu vào là `SqlLab.latestVersion(orders)` — bản đầu đọc thẳng lịch sử 7 dòng rồi lọc PAID, nên tính cả bản PAID CŨ của O1 (đã REFUNDED):
  * C1 = 900.000. Hai cách "cùng kết quả" vẫn cùng SAI nghiệp vụ; test so nhau mà không có golden nên không thấy (review chéo, M7).
  * Dedup TRƯỚC, lọc PAID SAU.
  */
object SqlVsDataFrame {
  def viaSql(spark: SparkSession, latest: DataFrame): DataFrame = {
    latest.createOrReplaceTempView("orders")
    spark.sql(
      """SELECT customer_id, SUM(CAST(quantity AS BIGINT) * unit_price_vnd) AS revenue_vnd
        |FROM orders WHERE status = 'PAID' AND unit_price_vnd IS NOT NULL
        |GROUP BY customer_id""".stripMargin)
  }

  def viaApi(latest: DataFrame): DataFrame =
    latest.filter(col("status") === "PAID" && col("unit_price_vnd").isNotNull)
      .groupBy("customer_id").agg(sum(col("quantity").cast(LongType) * col("unit_price_vnd")).as("revenue_vnd"))

  /** So kế hoạch đã tối ưu, bỏ các số định danh `#123` vốn khác nhau giữa hai lần dựng. */
  def normalizedPlan(df: DataFrame): String = df.queryExecution.optimizedPlan.toString.replaceAll("#\\d+L?", "")
}

/** Bài 3 — top-N mỗi nhóm: dense_rank theo doanh thu trong từng ngày. */
object TopSkuPerDay {
  def top(orders: DataFrame, n: Int): DataFrame = {
    val revenue = SqlLab.latestVersion(orders).filter(col("status") === "PAID" && col("unit_price_vnd").isNotNull)
      .groupBy("day", "sku").agg(sum(col("quantity").cast(LongType) * col("unit_price_vnd")).as("revenue_vnd"))
    val w = Window.partitionBy("day").orderBy(col("revenue_vnd").desc)
    revenue.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= n).orderBy("day", "rank", "sku")
  }
}

/** Bài 4 — thay UDF bằng hàm có sẵn và đo.
  *
  * HỢP ĐỒNG chuẩn hóa (phải chốt TRƯỚC khi so hai cách): bỏ dấu cách ' ' ở hai đầu (không bỏ tab), viết hoa không phụ thuộc locale, NULL giữ NULL.
  * Bản đầu dùng `s.trim.toUpperCase` trong UDF: Java `trim` bỏ cả tab/ký tự điều khiển còn Spark `trim` chỉ bỏ dấu cách ("\tsku\t" → "SKU" vs "\tSKU\t");
  * `toUpperCase()` theo locale JVM — locale tr-TR biến "i" thành "İ" (U+0130) còn `upper` của Spark cho "I". Test cũ chỉ so SỐ giá trị khác nhau nên
  * không thấy (review chéo, M4). Giờ UDF cài đúng hợp đồng và test so TỪNG giá trị.
  */
object UdfVsBuiltin {
  private def stripSpaces(s: String): String = s.dropWhile(_ == ' ').reverse.dropWhile(_ == ' ').reverse

  val normalizeUdf = udf((s: String) => if (s == null) null else stripSpaces(s).toUpperCase(java.util.Locale.ROOT))

  def builtin(df: DataFrame): DataFrame = df.withColumn("sku_norm", upper(trim(col("sku"))))
  def withUdf(df: DataFrame): DataFrame = df.withColumn("sku_norm", normalizeUdf(col("sku")))

  def timeMs(df: DataFrame): Long = {
    val t0 = System.nanoTime
    df.agg(countDistinct("sku_norm")).collect()
    (System.nanoTime - t0) / 1000000
  }
}

/** Bài 5 — join trên khóa có thể NULL: `===` không bao giờ khớp NULL với NULL; `<=>` thì có. */
object NullSafeJoin {
  def counts(spark: SparkSession): (Long, Long) = {
    import spark.implicits._
    val left  = Seq((Some("A"), 1), (None, 2)).toDF("k", "l")
    val right = Seq((Some("A"), 10), (None, 20)).toDF("k", "r")
    (left.join(right, left("k") === right("k")).count(), left.join(right, left("k") <=> right("k")).count())
  }
}

/** Bài 6 — doanh thu bằng Dataset typed (groupByKey + mapGroups) so với DataFrame agg.
  *
  * Đầu vào là dữ liệu ĐÃ dedup (`latestVersion`) và đã qua quality gate (quantity không NULL — `OrderRow.quantity: Int`).
  * Tràn số: lambda chạy số học Scala — `*` và `sum` của Long quay vòng thành số âm mà ANSI KHÔNG bảo vệ (ANSI chỉ áp cho biểu thức SQL). Bản đầu trả
  * C1 = -2 cho 2 × Long.MaxValue trong khi DataFrame ném ARITHMETIC_OVERFLOW (review chéo, M1). Dùng `multiplyExact`/`addExact` để cả hai cùng
  * chính sách "tràn = lỗi". Với miền dữ liệu (số hạng không âm), tràn xảy ra khi và chỉ khi tổng thật vượt Long — không phụ thuộc thứ tự cộng.
  */
object TypedVsUntyped {
  def typed(spark: SparkSession, ds: Dataset[OrderRow]): Dataset[(String, Long)] = {
    import spark.implicits._
    ds.filter(o => o.status == "PAID" && o.unit_price_vnd.isDefined)
      .groupByKey(_.customer_id)
      .mapGroups((k, rows) => (k, rows.foldLeft(0L)((acc, o) => Math.addExact(acc, Math.multiplyExact(o.quantity.toLong, o.unit_price_vnd.get)))))
  }
}
