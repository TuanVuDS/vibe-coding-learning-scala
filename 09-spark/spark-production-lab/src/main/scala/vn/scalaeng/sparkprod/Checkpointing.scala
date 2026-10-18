package vn.scalaeng.sparkprod

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Checkpoint cắt LINEAGE/plan: ghi dữ liệu ra lưu trữ bền rồi coi bản ghi đó là nguồn mới.
  * Vấn đề nó giải quyết: vòng lặp (thuật toán lặp, cập nhật trạng thái theo ngày) làm plan dài thêm mỗi vòng — Catalyst phân tích/tối ưu
  * plan ngày càng lâu, và khi mất một partition Spark phải tính lại từ đầu chuỗi.
  */
object Checkpointing {
  final case class Run(iterations: Int, planNodes: Int, planningMs: Long, result: Long)

  /** Số node trong logical plan đã phân tích — thước đo độ dài lineage của DataFrame. */
  def planNodes(df: DataFrame): Int = df.queryExecution.analyzed.collect { case p => p }.size

  /** `iterations` vòng `withColumn` + `filter`; cứ `every` vòng thì checkpoint (0 = không bao giờ). */
  def iterate(spark: SparkSession, iterations: Int, every: Int, checkpointDir: String): Run = {
    spark.sparkContext.setCheckpointDir(checkpointDir)
    var df = spark.range(0, 1000, 1, 4).toDF("v")
    (1 to iterations).foreach { i =>
      df = df.withColumn("v", col("v") + 1).filter(col("v") > i - 10000)
      if (every > 0 && i % every == 0) df = df.checkpoint(eager = true) // ghi ra checkpointDir NGAY, plan mới bắt đầu từ dữ liệu đã ghi
    }
    val t0 = System.nanoTime()
    df.queryExecution.executedPlan // buộc phân tích + tối ưu + lập plan vật lý, chưa chạy
    val planningMs = (System.nanoTime() - t0) / 1000000
    Run(iterations, planNodes(df), planningMs, df.agg(sum("v")).first().getLong(0))
  }
}
