package vn.scalaeng.solutions.ch24

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import vn.scalaeng.batch.{Experiments, Pipeline}

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
import scala.jdk.CollectionConverters._

/** Bài 1 — quy tắc LIÊN TRƯỜNG: cập nhật (`updated_at`) không thể xảy ra TRƯỚC ngày đặt hàng (`day`). Quy tắc trên một trường không bắt được. */
object CrossFieldRule {
  val Reason = "updated_at: không được trước ngày đặt hàng"

  def validate(raw: DataFrame): DataFrame = {
    val v = Pipeline.validate(raw)
    // Chỉ áp khi cả hai trường parse được — dòng đã sai kiểu đã có lý do riêng, không cộng thêm lỗi "phái sinh".
    val violated = col("day").isNotNull && col("updated_at").isNotNull && to_date(col("updated_at")) < col("day")
    v.withColumn("errors", when(violated, concat(col("errors"), array(lit(Reason)))).otherwise(col("errors")))
  }
}

/** Bài 2 — báo cáo chất lượng theo LÔ: đếm dòng bị loại theo từng lý do. Một dòng nhiều lỗi được đếm ở MỖI lý do của nó, nên tổng cột
  * `rows` có thể lớn hơn số dòng bị loại — báo cáo kèm cả hai con số.
  */
object QualityByBatch {
  def report(validated: DataFrame): DataFrame =
    Pipeline.rejected(validated)
      .select(col("ingest_date"), explode(col("errors")).as("reason"))
      .groupBy("ingest_date", "reason").agg(count(lit(1)).as("rows"))
      .orderBy(col("ingest_date"), col("rows").desc, col("reason"))
}

/** Bài 3 — QUALITY GATE theo lô: lô có tỷ lệ dòng bị loại vượt ngưỡng → dừng TRƯỚC khi ghi bất cứ thứ gì.
  * Ngưỡng là cấu hình: NaN hoặc ngoài [0, 1] là lỗi khởi động (bài học review chéo Chương 20, M4).
  */
object BatchGate {
  final case class BatchRate(ingestDate: String, rows: Long, rejected: Long) {
    def rate: Double = if (rows == 0) 0.0 else rejected.toDouble / rows
  }

  def rates(validated: DataFrame): List[BatchRate] =
    validated.groupBy("ingest_date")
      .agg(count(lit(1)).as("rows"), sum(when(size(col("errors")) > 0, 1).otherwise(0)).as("rejected"))
      .orderBy("ingest_date").collect().toList
      .map(r => BatchRate(r.getDate(0).toString, r.getLong(1), r.getLong(2)))

  def check(validated: DataFrame, maxRate: Double): Either[List[BatchRate], List[BatchRate]] = {
    require(!maxRate.isNaN && maxRate >= 0.0 && maxRate <= 1.0, s"ngưỡng phải trong [0, 1], nhận $maxRate")
    val all = rates(validated)
    val bad = all.filter(_.rate > maxRate)
    if (bad.nonEmpty) Left(bad) else Right(all)
  }
}

/** Bài 4 — WRITE → AUDIT → PUBLISH: ghi vào thư mục staging, kiểm tra, rồi mới đổi tên sang thư mục người đọc dùng.
  * Đọc giữa chừng không bao giờ thấy output dở dang; audit thất bại thì bản đang phục vụ giữ nguyên.
  *
  * Giới hạn: `Files.move(ATOMIC_MOVE)` chỉ nguyên tử trên CÙNG một hệ thống file cục bộ. Object storage (S3...) không có rename nguyên tử —
  * ở đó dùng table format (Delta/Iceberg) hoặc con trỏ "phiên bản hiện tại".
  */
object WriteAuditPublish {
  private def deleteTree(p: Path): Unit =
    if (Files.exists(p)) {
      val s = Files.walk(p)
      try s.sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(Files.delete) finally s.close()
    }

  def publish(df: DataFrame, target: Path)(audit: DataFrame => List[String]): Either[List[String], Path] = {
    val staging = target.resolveSibling(target.getFileName.toString + ".staging")
    deleteTree(staging)
    df.write.mode("errorifexists").parquet(staging.toString)
    val problems = audit(df.sparkSession.read.parquet(staging.toString))
    if (problems.nonEmpty) { deleteTree(staging); Left(problems) }
    else {
      val old = target.resolveSibling(target.getFileName.toString + ".old")
      deleteTree(old)
      if (Files.exists(target)) Files.move(target, old, StandardCopyOption.ATOMIC_MOVE)
      Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE)
      deleteTree(old)
      Right(target)
    }
  }
}

/** Bài 5 — dynamic overwrite KHÔNG xóa partition không còn trong dữ liệu mới. Nếu tính lại làm một ngày biến mất (mọi dòng của ngày đó
  * thành xung đột, hoặc bị sửa sang ngày khác), partition cũ vẫn nằm đó và người đọc thấy số liệu cũ. Sửa: sau khi ghi, xóa các partition
  * NẰM TRONG phạm vi lần tính lại nhưng không còn dữ liệu.
  */
object StalePartitions {
  def staleDays(dir: String, expected: Set[String], scope: Set[String]): Set[String] =
    Experiments.filesPerPartition(dir).keySet.map(_.stripPrefix("day=")).filter(scope.contains) -- expected

  /** Ghi động rồi dọn partition mồ côi TRONG phạm vi `scope` (các ngày mà input của lần chạy này bao phủ). Trả về các ngày đã xóa. */
  def writeAndPrune(df: DataFrame, dir: String, scope: Set[String]): Set[String] = {
    val expected = df.select("day").distinct().collect().map(_.getDate(0).toString).toSet
    Pipeline.writeByDay(df, dir)
    val stale = staleDays(dir, expected, scope)
    stale.foreach { d =>
      val p = Paths.get(dir, s"day=$d")
      val s = Files.walk(p)
      try s.sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(Files.delete) finally s.close()
    }
    stale
  }
}

/** Bài 6 — COMPACTION một partition nhiều file nhỏ thành `n` file, không đổi nội dung: đọc → repartition(n) → ghi staging → đổi chỗ. */
object Compaction {
  def compact(spark: SparkSession, partitionDir: Path, n: Int): Int = {
    require(n > 0, "n phải > 0")
    val staging = partitionDir.resolveSibling(partitionDir.getFileName.toString + ".compacting")
    // Đọc THẲNG thư mục partition: cột `day` không có trong file (nó nằm ở tên thư mục) nên không bị ghi trùng vào file.
    spark.read.parquet(partitionDir.toString).repartition(n).write.mode("errorifexists").parquet(staging.toString)
    val s = Files.walk(partitionDir)
    try s.sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(Files.delete) finally s.close()
    Files.move(staging, partitionDir, StandardCopyOption.ATOMIC_MOVE)
    val files = Files.list(partitionDir)
    try files.iterator().asScala.count(_.getFileName.toString.endsWith(".parquet")) finally files.close()
  }
}
