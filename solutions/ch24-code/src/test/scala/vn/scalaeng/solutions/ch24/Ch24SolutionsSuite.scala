package vn.scalaeng.solutions.ch24

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import vn.scalaeng.batch.{Experiments, Pipeline, PipelineDemo}

import java.nio.file.Files
import java.time.LocalDate

class Ch24SolutionsSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  private val input = "../../09-spark/batch-pipeline-lab/data/input"
  private val upTo  = LocalDate.parse("2026-10-16")

  override def beforeAll(): Unit = { spark = PipelineDemo.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  private def validated = Pipeline.validate(Pipeline.readRaw(spark, input, upTo))

  test("bài 1: updated_at trước ngày đặt hàng bị loại; dòng đã sai kiểu không bị cộng thêm lỗi phái sinh") {
    val root = Files.createTempDirectory("ch24-b1")
    val dir  = Files.createDirectories(root.resolve("ingest_date=2026-10-20"))
    Files.write(dir.resolve("o.csv"), (Pipeline.Columns.mkString(",") + "\n" +
      "X,C1,2026-10-20,PAID,KB-01,1,100,2026-10-19T23:00:00Z\n" +
      "Y,C1,2026-10-20,PAID,KB-01,1,100,2026-10-20T01:00:00Z\n" +
      "Z,C1,2026-13-01,PAID,KB-01,1,100,2026-10-19T23:00:00Z\n").getBytes("UTF-8"))
    val v = CrossFieldRule.validate(Pipeline.readRaw(spark, root.toString, LocalDate.parse("2026-10-20")))
    val errs = v.select(col("raw.order_id"), col("errors")).collect().map(r => r.getString(0) -> r.getSeq[String](1)).toMap
    assertEquals(errs("X"), Seq(CrossFieldRule.Reason))
    assertEquals(errs("Y"), Seq.empty[String])
    assertEquals(errs("Z"), Seq("day: phải là ngày yyyy-MM-dd"))
  }

  test("bài 2: báo cáo chất lượng theo lô và lý do") {
    val rows = QualityByBatch.report(validated).collect().map(r => (r.getDate(0).toString, r.getString(1), r.getLong(2))).toList
    assert(rows.contains(("2026-10-15", "quantity: số nguyên 1..1000", 1L)), rows.toString)
    assert(rows.contains(("2026-10-16", "quantity: số nguyên 1..1000", 1L)), rows.toString)
    assertEquals(rows.filter(_._1 == "2026-10-15").map(_._3).sum, 7L) // O3 (1 lỗi) + O5 (6 lỗi): 2 dòng, 7 lần vi phạm
  }

  test("bài 3: quality gate theo lô — 2/7 = 28,6% chặn ở ngưỡng 20%; 1/6 = 16,7% qua; NaN bị từ chối") {
    val v = validated
    val blocked = BatchGate.check(v, 0.20)
    assertEquals(blocked.left.map(_.map(_.ingestDate)), Left(List("2026-10-15")))
    assert(BatchGate.check(v, 0.30).isRight)
    intercept[IllegalArgumentException](BatchGate.check(v, Double.NaN))
  }

  test("bài 4: write-audit-publish — audit thất bại thì bản đang phục vụ giữ nguyên") {
    val target = Files.createTempDirectory("ch24-b4").resolve("daily")
    val good   = spark.range(0, 10).toDF("x")
    assert(WriteAuditPublish.publish(good, target)(_ => Nil).isRight)
    val bad = WriteAuditPublish.publish(spark.range(0, 3).toDF("x"), target)(d => if (d.count() < 10) List("thiếu dòng") else Nil)
    assertEquals(bad, Left(List("thiếu dòng")))
    assertEquals(spark.read.parquet(target.toString).count(), 10L)
    assert(!Files.exists(target.resolveSibling("daily.staging")))
  }

  test("bài 5: dynamic overwrite để lại partition mồ côi; writeAndPrune xóa nó (chỉ trong phạm vi)") {
    val dir = Files.createTempDirectory("ch24-b5").resolve("facts").toString
    val two = spark.range(0, 4).select(date_add(lit(java.sql.Date.valueOf("2026-10-15")), (col("id") % 2).cast("int")).as("day"), col("id"))
    Pipeline.writeByDay(two, dir)
    val onlyFifteen = two.filter(col("day") === lit(java.sql.Date.valueOf("2026-10-15")))
    Pipeline.writeByDay(onlyFifteen, dir)
    assertEquals(Experiments.filesPerPartition(dir).keySet, Set("day=2026-10-15", "day=2026-10-16")) // ngày 16 cũ vẫn còn!
    val removed = StalePartitions.writeAndPrune(onlyFifteen, dir, scope = Set("2026-10-15", "2026-10-16"))
    assertEquals((removed, Experiments.filesPerPartition(dir).keySet), (Set("2026-10-16"), Set("day=2026-10-15")))
  }

  test("bài 6: compaction 8 file nhỏ → 2 file, nội dung (multiset) không đổi") {
    val dir = Files.createTempDirectory("ch24-b6").resolve("facts")
    val df  = spark.range(0, 1000, 1, 8).select(lit(java.sql.Date.valueOf("2026-10-15")).as("day"), col("id"), (col("id") % 7).as("v"))
    df.write.partitionBy("day").parquet(dir.toString)
    val part = dir.resolve("day=2026-10-15")
    assertEquals(Experiments.filesPerPartition(dir.toString)("day=2026-10-15"), 8)
    val before = spark.read.parquet(dir.toString).cache()
    before.count()
    assertEquals(Compaction.compact(spark, part, 2), 2)
    val after = spark.read.parquet(dir.toString)
    assertEquals((before.exceptAll(after).count(), after.exceptAll(before).count(), after.count()), (0L, 0L, 1000L))
    before.unpersist(blocking = true)
  }
}
