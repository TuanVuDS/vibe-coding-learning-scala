package vn.scalaeng.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Một SparkSession cho cả suite (tạo SparkSession tốn vài giây). `maxFailures = 2` để thí nghiệm fault tolerance chạy lại được task. */
class FundamentalsSuite extends munit.FunSuite {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = LocalSpark.session("fundamentals-test", cores = 2, maxFailures = 2)
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  test("phiên bản: Spark 4.2.0 chạy trên Scala 2.13") {
    assertEquals(spark.version, "4.2.0")
    assert(scala.util.Properties.versionNumberString.startsWith("2.13."))
  }

  test("lazy: transformation không chạy gì; action chạy đúng một lần cho mỗi phần tử") {
    val r = Experiments.lazyEvaluation(spark, 300000L)
    assertEquals((r.callsAfterTransform, r.callsAfterAction, r.rows), (0L, 300000L, 100000L))
  }

  test("cache: hai action không cache tính lại 2 lần; có cache chỉ tính 1 lần") {
    assertEquals(Experiments.cacheEffect(spark, 100000L), Experiments.CacheResult(200000L, 100000L))
  }

  test("narrow (filter/select) không có Exchange; wide (groupBy) có Exchange hashpartitioning") {
    assert(!Experiments.hasShuffleExchange(Experiments.narrowPlan(spark)), Experiments.narrowPlan(spark))
    assert(Experiments.widePlan(spark).contains("Exchange hashpartitioning"), Experiments.widePlan(spark))
  }

  test("shuffle tách stage: filter+collect 1 stage; count() và groupBy đều có shuffle -> 2 stage") {
    val narrow = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).filter(col("id") < 100).collect(): Unit)
    val count  = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).filter(col("id") < 100).count(): Unit)
    val wide   = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).groupBy(col("id") % 10).count().collect(): Unit)
    assertEquals((narrow, count, wide), (1, 2, 2))
  }

  test("review M3: listener CHẬM đứng trước trên cùng hàng đợi — stagesOf vẫn đếm đúng (bản sleep 500 ms trả 0)") {
    val slow = new org.apache.spark.scheduler.SparkListener {
      override def onStageCompleted(e: org.apache.spark.scheduler.SparkListenerStageCompleted): Unit = Thread.sleep(1500)
    }
    spark.sparkContext.addSparkListener(slow)
    try assertEquals(Experiments.stagesOf(spark)(spark.range(0, 1000, 1, 1).collect(): Unit), 1)
    finally spark.sparkContext.removeSparkListener(slow)
  }

  test("review M3: job khác chạy CÙNG LÚC ở thread khác không bị đếm vào") {
    import scala.concurrent.{Await, Future}
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._
    val other = Future((1 to 5).foreach(_ => spark.range(0, 200000, 1, 4).groupBy(col("id") % 7).count().collect()))
    val n     = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).filter(col("id") < 100).collect(): Unit)
    Await.result(other, 2.minutes)
    assertEquals(n, 1)
  }

  test("review M1: coalesce là NARROW dù 4 cha -> 1 con; 1 partition thì groupBy không shuffle; broadcast join không shuffle") {
    assertEquals(
      Experiments.dependencyFacts(spark),
      Experiments.DependencyFacts(
        coalesceIsNarrow = true, coalesceParentsOfPartition0 = 4, onePartitionGroupByHasShuffle = false, onePartitionCountStages = 1,
        broadcastJoinPlanHasBroadcastExchange = true, broadcastJoinPlanHasShuffle = false
      )
    )
  }

  test("review M2: repartition theo hash dồn MỌI dòng của khóa nóng vào một partition") {
    val sizes = Experiments.hashPartitionSizes(spark, rows = 1000, hotKeyShare = 0.9, n = 8)
    assertEquals(sizes.sum, 1000L)
    assert(sizes.max >= 900L, sizes.toString)
  }

  test("repartition luôn shuffle; coalesce chỉ gộp, không shuffle") {
    assertEquals(Experiments.partitions(spark), Experiments.PartitionResult(4, 8, 2, repartitionHasExchange = true, coalesceHasExchange = false))
  }

  test("AQE gộp 200 partition shuffle mặc định thành ít partition khi dữ liệu nhỏ") {
    val grouped = spark.range(0, 100000, 1, 4).groupBy((col("id") % 10).as("k")).count()
    grouped.collect()
    assertEquals(spark.conf.get("spark.sql.shuffle.partitions"), "200")
    assert(grouped.rdd.getNumPartitions < 200, grouped.rdd.getNumPartitions.toString)
  }

  test("fault tolerance: task thất bại được chạy lại từ lineage, kết quả vẫn đúng") {
    val r = Experiments.faultTolerance(spark)
    assertEquals((r.sum, r.failedAttempts), ((1L to 1000L).map(_ * 2).sum, 1))
    assert(r.lineage.contains("ParallelCollectionRDD"), r.lineage)
  }
}
