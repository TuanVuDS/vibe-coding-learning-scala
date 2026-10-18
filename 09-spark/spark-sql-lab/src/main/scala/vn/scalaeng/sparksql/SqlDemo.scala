package vn.scalaeng.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.Files

object SqlDemo {
  def session(): SparkSession =
    SparkSession.builder().master("local[2]").appName("spark-sql")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC") // in timestamp theo UTC, không theo múi giờ của máy
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val orders    = SqlLab.readOrders(spark, "data/orders.csv")
      val customers = SqlLab.readCustomers(spark, "data/customers.csv")

      println("== Schema tường minh")
      println(orders.schema.treeString)
      println("== Schema do Spark ĐOÁN (inferSchema)")
      println(SqlLab.readOrdersInferred(spark, "data/orders.csv").schema.treeString)

      println("== Bản mới nhất theo (order_id, sku) — window max(updated_at) + phát hiện xung đột")
      SqlLab.latestVersion(orders).orderBy("order_id").select("order_id", "status", "updated_at").show(truncate = false)

      println("== Doanh thu PAID theo segment (left join, khách không tồn tại -> UNKNOWN)")
      SqlLab.revenueBySegment(orders, customers).show()

      println("== Đơn mồ côi (left_anti)")
      SqlLab.orphanOrders(orders, customers).select("order_id", "customer_id").show()

      println("== Tổng lũy kế theo khách (window frame)")
      SqlLab.runningTotal(orders).show()

      println(s"== NULL: ${SqlLab.nullSemantics(customers)}")

      val pq = Files.createTempDirectory("ch23").resolve("orders.parquet").toString
      orders.write.mode("overwrite").parquet(pq)
      def scanLine(plan: String) = plan.linesIterator.find(_.contains("FileScan")).getOrElse("(không thấy FileScan)")
      println("== Hàm có sẵn startsWith:"); println(scanLine(SqlLab.planOf(SqlLab.withBuiltin(spark, pq))))
      println("== UDF:"); println(scanLine(SqlLab.planOf(SqlLab.withUdf(spark, pq))))
      println("== Dataset + lambda:"); println(scanLine(SqlLab.planOf(SqlLab.typedLambda(spark, pq).toDF())))
      println("== Dataset + Column:"); println(scanLine(SqlLab.planOf(SqlLab.typedColumn(spark, pq).toDF())))

      println("== Catalyst: kế hoạch logic đã tối ưu (filter được đẩy xuống dưới join)")
      val q = orders.join(customers, Seq("customer_id")).filter(col("segment") === "Retail" && col("quantity") > 1).select("order_id", "city")
      println(q.queryExecution.optimizedPlan.toString)
    } finally spark.stop()
  }
}
