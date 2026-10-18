package vn.scalaeng.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path, Paths => JPaths}
import scala.jdk.CollectionConverters._

/** Các thí nghiệm về FILE LAYOUT và RERUN SAFETY, mỗi cái trả về số đo để test kiểm tra. */
object Experiments {

  /** Số file Parquet trong từng thư mục partition `day=...` — đọc thẳng hệ thống file, không qua Spark. */
  def filesPerPartition(dir: String): Map[String, Int] = {
    val root = JPaths.get(dir)
    val parts = Files.list(root)
    try parts.iterator().asScala.filter(p => Files.isDirectory(p) && p.getFileName.toString.contains("=")).map { p =>
      val files = Files.list(p)
      try p.getFileName.toString -> files.iterator().asScala.count(_.getFileName.toString.endsWith(".parquet"))
      finally files.close()
    }.toMap
    finally parts.close()
  }

  /** SMALL FILES: ghi `partitionBy("day")` thẳng từ dữ liệu có `n` partition → mỗi task ghi một file cho MỖI ngày nó gặp,
    * tối đa n × số ngày file. `repartition(col("day"))` trước → một file mỗi ngày.
    */
  def smallFiles(df: DataFrame, n: Int, out: Path, repartitionByDay: Boolean): Map[String, Int] = {
    val spread = df.repartition(n) // mô phỏng dữ liệu đến từ n file/task đầu vào
    val toWrite = if (repartitionByDay) spread.repartition(col("day")) else spread
    toWrite.write.mode("overwrite").partitionBy("day").parquet(out.toString)
    filesPerPartition(out.toString)
  }

  final case class ModeResult(rowsAfterFirst: Long, rowsAfterRerun: Long, daysAfterPartialRerun: Set[String])

  /** RERUN SAFETY của ba chế độ ghi, cùng dữ liệu `df` (2 ngày):
    *   - append:              chạy lại → NHÂN ĐÔI dòng;
    *   - overwrite tĩnh:      chạy lại một phần (chỉ ngày 16) → XÓA MẤT ngày 15;
    *   - overwrite động:      chạy lại một phần → chỉ thay ngày 16, ngày 15 còn nguyên.
    */
  def writeMode(df: DataFrame, out: Path, mode: String): ModeResult = {
    def write(d: DataFrame, first: Boolean): Unit = {
      val w = d.write.partitionBy("day")
      mode match {
        case "append"  => w.mode("append").parquet(out.toString)
        case "static"  => w.mode("overwrite").option("partitionOverwriteMode", "static").parquet(out.toString)
        case "dynamic" => w.mode("overwrite").option("partitionOverwriteMode", "dynamic").parquet(out.toString)
        case other     => throw new IllegalArgumentException(s"mode lạ: $other (first=$first)")
      }
    }
    val spark = df.sparkSession
    write(df, first = true)
    val first = spark.read.parquet(out.toString).count()
    write(df, first = false)
    val rerun = spark.read.parquet(out.toString).count()
    write(df.filter(col("day") === lit(java.sql.Date.valueOf("2026-10-16"))), first = false)
    ModeResult(first, rerun, filesPerPartition(out.toString).keySet)
  }

  /** PARTITION PRUNING: lọc theo cột partition → dòng `FileScan` có `PartitionFilters`, Spark chỉ mở thư mục của ngày đó. */
  def prunedScan(spark: SparkSession, factsDir: String, day: String): String =
    spark.read.parquet(factsDir).filter(col("day") === lit(java.sql.Date.valueOf(day)))
      .queryExecution.executedPlan.toString.linesIterator.find(_.contains("FileScan")).getOrElse("")

  /** Join trong pipeline dùng broadcast: tìm chiến lược trong plan vật lý. */
  def joinStrategy(df: DataFrame): String = {
    val plan = df.queryExecution.executedPlan.toString
    Seq("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin").find(plan.contains).getOrElse("không có join")
  }
}
