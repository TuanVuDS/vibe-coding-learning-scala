package vn.scalaeng.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.Files
import java.time.LocalDate

object PipelineDemo {
  def session(): SparkSession =
    SparkSession.builder().master("local[2]").appName("batch-pipeline")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "4") // dữ liệu nhỏ: 200 partition shuffle chỉ là chi phí (Chương 22, 25)
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
      // Spark 4 bật cô lập artifact theo session (cho Spark Connect): executor tải class qua RPC, mỗi lần mở một Pipe — trên Windows là cặp
      // socket loopback. Đo thật: +1.650 socket TIME_WAIT mỗi 60 truy vấn, suite dài làm cạn cổng ("Unable to establish loopback
      // connection"). Job batch một session không cần cô lập này. Cấu hình `internal` của Spark 4.2 — xem mục 10 của bài.
      .config("spark.sql.artifact.isolation.enabled", "false")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val out = Files.createTempDirectory("ch24-out").toString
      val paths = Pipeline.Paths(out)

      def show(title: String, r: Pipeline.RunReport): Unit = {
        println(s"== $title")
        println(s"   input=${r.inputRows} rejected=${r.rejectedRows} accepted=${r.acceptedRows} trùng=${r.exactDuplicateRows} " +
          s"thay-thế=${r.supersededRows} xung-đột=${r.conflictRows} fact=${r.factLines} doanh-thu-PAID=${r.paidRevenueVnd}")
        println(s"   kiểm tra: ${if (r.ok) "OK" else r.failedChecks.mkString("; ")}")
        spark.read.parquet(paths.daily).orderBy("day", "segment").show(truncate = false)
      }

      show("Lần 1: chỉ lô 2026-10-15", Pipeline.run(spark, "data/input", "data/customers.csv", out, LocalDate.parse("2026-10-15")))
      show("Lần 2: lô 15 + 16 (hoàn tiền O1 và đơn trễ O8 sửa partition ngày 15)",
        Pipeline.run(spark, "data/input", "data/customers.csv", out, LocalDate.parse("2026-10-16")))
      show("Lần 3: chạy LẠI y hệt lần 2", Pipeline.run(spark, "data/input", "data/customers.csv", out, LocalDate.parse("2026-10-16")))

      println("== Dòng bị loại (giá trị trường + lý do)")
      spark.read.parquet(paths.rejects).select(col("ingest_date"), col("raw.order_id"), col("errors")).orderBy("ingest_date", "order_id")
        .show(truncate = false)
      println("== Xung đột phiên bản (cách ly, không tính doanh thu)")
      spark.read.parquet(paths.conflicts).select("order_id", "sku", "status", "updated_at").orderBy("status").show(truncate = false)

      println("== File layout của fact_order_lines")
      Experiments.filesPerPartition(paths.facts).toSeq.sorted.foreach { case (p, n) => println(s"   $p: $n file") }
      println("== Partition pruning khi đọc ngày 2026-10-16")
      println(Experiments.prunedScan(spark, paths.facts, "2026-10-16"))
      println("== Partition pruning trên INPUT (chỉ lô <= 2026-10-15)")
      println(Pipeline.readRaw(spark, "data/input", LocalDate.parse("2026-10-15")).queryExecution.executedPlan.toString
        .linesIterator.find(_.contains("FileScan")).getOrElse(""))

      // Output là SNAPSHOT của input <= upTo: giảm upTo thì các ngày/lô không còn dữ liệu bị XÓA, không nằm lại với số liệu cũ.
      val back = Pipeline.run(spark, "data/input", "data/customers.csv", out, LocalDate.parse("2026-10-15"))
      show("Lần 4: GIẢM upTo về 2026-10-15 trên cùng output (quay về snapshot lần 1)", back)
      println(s"   partition đã xóa: ${back.removedPartitions.mkString(", ")}")

      val validated = Pipeline.validate(Pipeline.readRaw(spark, "data/input", LocalDate.parse("2026-10-16")))
      val facts = Pipeline.enrich(Pipeline.dedup(Pipeline.accepted(validated)).latest, Pipeline.readCustomers(spark, "data/customers.csv"))
      println(s"== Chiến lược join: ${Experiments.joinStrategy(facts)}")

      val many = spark.range(0, 10000, 1, 8).select(
        (col("id") % 3).cast("int").as("k"),
        date_add(lit(java.sql.Date.valueOf("2026-10-15")), (col("id") % 3).cast("int")).as("day"))
      println(s"== Small files: 8 task ghi thẳng      -> ${Experiments.smallFiles(many, 8, Files.createTempDirectory("sf1"), repartitionByDay = false).toSeq.sorted}")
      println(s"== Small files: repartition(day) trước -> ${Experiments.smallFiles(many, 8, Files.createTempDirectory("sf2"), repartitionByDay = true).toSeq.sorted}")
      Seq("append", "static", "dynamic").foreach { m =>
        println(s"== Ghi chế độ $m: ${Experiments.writeMode(many, Files.createTempDirectory(s"mode-$m"), m)}")
      }
    } finally spark.stop()
  }
}
