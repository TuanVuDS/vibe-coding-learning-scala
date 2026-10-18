package vn.scalaeng.p3

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

/** Các bảng đầu ra. Mọi hàm nhận/trả DataFrame — test từng bước bằng DataFrame nhỏ dựng tay. */
object Reports {
  val Money = DecimalType(38, 0)

  /** Khóa dimension phải duy nhất trước join (trùng → nhân doanh thu). Một action riêng, chạy ngay. */
  def requireUniqueCustomers(customers: DataFrame): Unit = {
    val dups = customers.groupBy("customer_id").count().filter(col("count") > 1).limit(3).collect().map(_.getString(0))
    require(dups.isEmpty, s"customers trùng customer_id: ${dups.mkString(", ")}")
  }

  /** Fact dòng hàng + segment. Bảng khách nhỏ → broadcast (plan: BroadcastHashJoin, không shuffle bảng đơn). */
  def orderFacts(latest: DataFrame, customers: DataFrame): DataFrame = {
    requireUniqueCustomers(customers)
    latest.join(broadcast(customers.select("customer_id", "segment")), Seq("customer_id"), "left")
      .withColumn("segment", coalesce(col("segment"), lit("UNKNOWN")))
      .withColumn("revenue_vnd", col("quantity").cast(Money) * col("unit_price_vnd"))
  }

  /** Grain: (day, segment). */
  def dailyRevenue(facts: DataFrame): DataFrame =
    facts.filter(col("status") === "PAID").groupBy("day", "segment")
      .agg(sum("revenue_vnd").as("revenue_vnd"), count(lit(1)).as("order_lines"), countDistinct("order_id").as("orders"))

  /** Grain: (day, campaign). "Khách chuyển đổi" = khách có click chiến dịch đó VÀ có dòng hàng PAID trong CÙNG ngày.
    *
    * Đây là ASSISTED conversion: khách click hai chiến dịch rồi mua được tính ở CẢ HAI — `assisted_revenue_vnd` cộng các chiến dịch lại
    * có thể LỚN HƠN doanh thu thật (test kiểm tra điều này). Muốn chia doanh thu không trùng cần mô hình ghi nhận (last-click, linear...) — một
    * quyết định nghiệp vụ, không phải kỹ thuật.
    *
    * Join hai bảng (click × doanh thu theo khách-ngày) theo (customer_id, day). Plan QUAN SÁT ĐƯỢC trên Spark 4.2 local với cấu hình mặc định: dữ
    * liệu mẫu nhỏ → BroadcastHashJoin; tắt broadcast tự động → SortMergeJoin. Không phải luật: AQE có thể chọn ShuffledHashJoin tùy kích thước/cấu hình.
    * Tổng hợp doanh thu theo (customer_id, day) TRƯỚC khi join để mỗi khách-ngày chỉ một dòng — join không nhân dòng click.
    */
  def campaignDaily(clicks: DataFrame, facts: DataFrame): DataFrame = {
    val paidByCustomerDay = facts.filter(col("status") === "PAID").groupBy("customer_id", "day").agg(sum("revenue_vnd").as("paid_vnd"))
    val clickers = clicks.groupBy("day", "campaign", "customer_id").agg(count(lit(1)).as("clicks"))
    clickers.join(paidByCustomerDay, Seq("customer_id", "day"), "left")
      .groupBy("day", "campaign")
      .agg(
        sum("clicks").as("clicks"),
        countDistinct("customer_id").as("users"),
        countDistinct(when(col("paid_vnd").isNotNull, col("customer_id"))).as("converted_users"),
        coalesce(sum("paid_vnd"), lit(0).cast(Money)).as("assisted_revenue_vnd"))
      .withColumn("conversion_rate", (col("converted_users").cast(DecimalType(20, 4)) / col("users")).cast(DecimalType(10, 4)))
  }
}
