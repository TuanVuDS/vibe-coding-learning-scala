package vn.scalaeng.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.{Instant, LocalDate}

/** Dataset TYPED: mỗi dòng là một case class. `Option[Long]` cho cột có thể null.
  * Chú ý: `quantity: Int` KHÔNG nhận null (đọc dòng quantity NULL → NOT_NULL_ASSERT_VIOLATION), còn `String` vẫn có thể chứa `null` của JVM.
  * Encoder không tự biến cột nullable thành Option — dữ liệu phải qua quality gate TRƯỚC `.as[OrderRow]`, hoặc khai báo Option cho trường được phép thiếu.
  */
final case class OrderRow(
    order_id: String,
    customer_id: String,
    day: LocalDate,
    status: String,
    sku: String,
    quantity: Int,
    unit_price_vnd: Option[Long],
    updated_at: Instant
)

object Schemas {
  /** Schema TƯỜNG MINH: kiểu và khả năng null do ta quyết định, không do Spark đoán từ vài dòng dữ liệu. */
  val orders: StructType = StructType(Seq(
    StructField("order_id", StringType, nullable = false),
    StructField("customer_id", StringType, nullable = false),
    StructField("day", DateType, nullable = false),
    StructField("status", StringType, nullable = false),
    StructField("sku", StringType, nullable = false),
    StructField("quantity", IntegerType, nullable = false),
    StructField("unit_price_vnd", LongType, nullable = true),
    StructField("updated_at", TimestampType, nullable = false)
  ))

  val customers: StructType = StructType(Seq(
    StructField("customer_id", StringType, nullable = false),
    StructField("segment", StringType, nullable = false),
    StructField("city", StringType, nullable = true),
    StructField("signup_date", DateType, nullable = true)
  ))
}

object SqlLab {
  /** `mode = FAILFAST`: trường không parse được làm job lỗi thay vì âm thầm thành null (mặc định PERMISSIVE) — nhưng CHỈ với trường mà query
    * thật sự đọc: CSV cắt cột (column pruning), nên `count()` hay `select("order_id")` không parse cột `quantity` và vẫn "xanh" (review chéo, M6).
    *
    * `enforceSchema = false`: kiểm tra TÊN cột trong header khớp schema. Mặc định (true) Spark áp schema THEO VỊ TRÍ và bỏ qua header — file đổi thứ tự
    * `customer_id,order_id,...` được đọc thành công nhưng tráo giá trị hai cột (cùng kiểu String nên FAILFAST cũng không cứu).
    */
  def readOrders(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "FAILFAST").schema(Schemas.orders).csv(path)

  def readOrdersInferred(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  def readCustomers(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "FAILFAST").schema(Schemas.customers).csv(path)

  private val lineKey = Seq("order_id", "sku")

  /** Các bản ghi có `updated_at` LỚN NHẤT của mỗi dòng hàng, kèm số "phiên bản khác nội dung" (`variants`) trong nhóm đó. */
  private def topVersions(orders: DataFrame): DataFrame = {
    val byLine  = Window.partitionBy(lineKey.map(col): _*)
    val payload = struct(orders.columns.toSeq.filterNot(lineKey.contains).map(col): _*)
    orders
      .withColumn("max_updated_at", max("updated_at").over(byLine))
      .filter(col("updated_at") === col("max_updated_at"))
      .withColumn("variants", size(collect_set(payload).over(byLine)))
      .drop("max_updated_at")
  }

  /** Dedup "bản mới nhất" của mỗi dòng hàng (order_id, sku).
    *
    * Bản đầu dùng `row_number() over (orderBy updated_at desc)`: khi HAI bản cùng `updated_at` (PAID và REFUNDED), bản được chọn phụ thuộc thứ tự đọc —
    * đảo input là đổi kết quả (review chéo, M2). `row_number` chỉ xác định khi thứ tự sắp xếp phân biệt được mọi bản. Chính sách bây giờ:
    *   - bản mới nhất DUY NHẤT (hoặc nhiều bản giống hệt nhau) → giữ một;
    *   - nhiều bản mới nhất KHÁC nội dung → XUNG ĐỘT: không đoán, loại khỏi kết quả và trả ở `versionConflicts` để người xử lý.
    * Dòng có `updated_at` NULL không bao giờ là "mới nhất" (so sánh NULL không true) — kiểm tra NULL thuộc quality gate trước bước này.
    */
  def latestVersion(orders: DataFrame): DataFrame =
    topVersions(orders).filter(col("variants") === 1).dropDuplicates(lineKey).drop("variants")

  /** Dòng hàng có nhiều bản mới nhất mâu thuẫn — đầu vào cho báo cáo chất lượng, không âm thầm bỏ qua. */
  def versionConflicts(orders: DataFrame): DataFrame =
    topVersions(orders).filter(col("variants") > 1).select((lineKey :+ "updated_at").map(col): _*).distinct()

  /** Số khách hàng bị TRÙNG customer_id trong bảng dimension. Phải bằng 0 trước khi join (xem `revenueBySegment`). */
  def duplicateCustomerIds(customers: DataFrame): DataFrame =
    customers.groupBy("customer_id").count().filter(col("count") > 1)

  /** Doanh thu dòng hàng PAID theo segment. Left join: đơn của khách không tồn tại vẫn được giữ, segment = "UNKNOWN" —
    * thay vì inner join làm doanh thu "biến mất" mà không ai biết.
    *
    * Left join giữ dòng KHÔNG khớp nhưng không giữ được quan hệ nhiều-một: nếu `customers` có hai dòng C2, mỗi đơn của C2 bị NHÂN ĐÔI
    * (Wholesale 20.750.000 → 41.500.000, review chéo M3). Vì vậy kiểm tra khóa duy nhất TRƯỚC — một action riêng, chạy ngay khi gọi hàm.
    * Không dùng `dropDuplicates` để che: hai dòng C2 khác segment là lỗi dữ liệu cần người quyết định.
    *
    * Grain: một dòng là một DÒNG HÀNG (order_id, sku). `order_lines` đếm dòng hàng; `orders` đếm ĐƠN khác nhau (bản đầu gọi số dòng hàng là "orders").
    * Tràn số: `cast(LongType)` chỉ để phép nhân làm trên Long; tích/tổng vượt Long → ANSI (bật mặc định ở Spark 4) ném ARITHMETIC_OVERFLOW — lỗi to,
    * không phải số âm (review chéo M1). Miền dữ liệu: quantity 1..1000, đơn giá > 0 — mọi số hạng không âm nên tràn xảy ra khi và chỉ khi tổng thật vượt
    * Long, không phụ thuộc cách chia partition.
    */
  def revenueBySegment(orders: DataFrame, customers: DataFrame): DataFrame = {
    val dups = duplicateCustomerIds(customers).limit(3).collect().map(_.getString(0))
    require(dups.isEmpty, s"customers có customer_id trùng: ${dups.mkString(", ")} — join sẽ nhân doanh thu")
    latestVersion(orders)
      .filter(col("status") === "PAID" && col("unit_price_vnd").isNotNull)
      .join(customers.select("customer_id", "segment"), Seq("customer_id"), "left")
      .groupBy(coalesce(col("segment"), lit("UNKNOWN")).as("segment"))
      .agg(
        sum(col("quantity").cast(LongType) * col("unit_price_vnd")).as("revenue_vnd"),
        count(lit(1)).as("order_lines"),
        countDistinct("order_id").as("orders")
      )
      .orderBy("segment")
  }

  /** Anti join: đơn có customer_id KHÔNG tồn tại (bản ghi mồ côi). Semi join: chỉ đơn có khách tồn tại, không thêm cột. */
  def orphanOrders(orders: DataFrame, customers: DataFrame): DataFrame =
    orders.join(customers, Seq("customer_id"), "left_anti")

  def ordersOfKnownCustomers(orders: DataFrame, customers: DataFrame): DataFrame =
    orders.join(customers, Seq("customer_id"), "left_semi")

  /** Tổng lũy kế theo ngày cho từng khách: window có khung (frame) từ đầu đến dòng hiện tại.
    *
    * Thứ tự trong window phải PHÂN BIỆT được mọi dòng: bản đầu chỉ `orderBy("day", "order_id")`, nên các sku của cùng một đơn "hòa" và tổng lũy kế
    * gắn vào dòng nào thay đổi theo AQE/số partition (100→400 cho cùng dòng, review chéo M2). Thêm `sku` vào cả orderBy lẫn output.
    */
  def runningTotal(orders: DataFrame): DataFrame = {
    val w = Window.partitionBy("customer_id").orderBy("day", "order_id", "sku").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    latestVersion(orders)
      .filter(col("status") === "PAID" && col("unit_price_vnd").isNotNull)
      .withColumn("revenue_vnd", col("quantity").cast(LongType) * col("unit_price_vnd"))
      .withColumn("running_total", sum("revenue_vnd").over(w))
      .select("customer_id", "day", "order_id", "sku", "revenue_vnd", "running_total")
      .orderBy("customer_id", "day", "order_id", "sku")
  }

  final case class NullCounts(eqNull: Long, nullSafeEqNull: Long, isNullCount: Long, notEqHanoi: Long)

  /** Logic ba trị của SQL: so sánh với NULL cho NULL (không phải true/false), nên `city = NULL` không bao giờ khớp. */
  def nullSemantics(customers: DataFrame): NullCounts =
    NullCounts(
      customers.filter(col("city") === lit(null).cast(StringType)).count(),
      customers.filter(col("city") <=> lit(null).cast(StringType)).count(),
      customers.filter(col("city").isNull).count(),
      customers.filter(col("city") =!= "Hà Nội").count() // dòng city NULL KHÔNG nằm trong kết quả!
    )

  /** UDF: Spark coi như hộp đen → không đẩy điều kiện xuống nguồn Parquet. Hàm có sẵn thì đẩy được. */
  val isKeyboard = udf((sku: String) => sku != null && sku.startsWith("KB"))

  /** Kế hoạch vật lý. Với AQE, gọi TRƯỚC action cho plan ban đầu (`isFinalPlan=false`); gọi SAU action trên CÙNG Dataset cho plan đã chạy. */
  def planOf(df: DataFrame): String = df.queryExecution.executedPlan.toString

  /** Chỉ plan CUỐI (sau khi AQE điều chỉnh) — `toString` của AdaptiveSparkPlan sau khi chạy in cả Final lẫn Initial Plan. */
  def finalPlanOf(df: DataFrame): String = (df.queryExecution.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other                    => other
  }).toString

  def withBuiltin(spark: SparkSession, parquet: String): DataFrame =
    spark.read.parquet(parquet).filter(col("sku").startsWith("KB")).select("order_id", "sku")

  def withUdf(spark: SparkSession, parquet: String): DataFrame =
    spark.read.parquet(parquet).filter(isKeyboard(col("sku"))).select("order_id", "sku")

  /** Dataset typed + lambda: an toàn kiểu lúc biên dịch, nhưng lambda cũng là hộp đen với Catalyst (như UDF).
    * `sku` là `String` — có thể là `null` của JVM. Bản đầu `_.sku.startsWith("KB")` ném NullPointerException với sku NULL trong khi ba cách kia trả 0 dòng
    * (review chéo M5). "An toàn kiểu" không có nghĩa an toàn null.
    */
  def typedLambda(spark: SparkSession, parquet: String): Dataset[OrderRow] = {
    import spark.implicits._
    spark.read.parquet(parquet).as[OrderRow].filter(o => o.sku != null && o.sku.startsWith("KB"))
  }

  def typedColumn(spark: SparkSession, parquet: String): Dataset[OrderRow] = {
    import spark.implicits._
    spark.read.parquet(parquet).as[OrderRow].filter(col("sku").startsWith("KB"))
  }
}
