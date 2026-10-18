package vn.scalaeng.solutions.ch25

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel
import vn.scalaeng.sparkperf.{PerformanceLab, TaskSample}

class Ch25SolutionsSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = PerformanceLab.session("Ch25 Solutions")
    spark.sparkContext.setLogLevel("ERROR")
  }
  override def afterAll(): Unit = spark.stop()

  test("bài 1: final strategy differs from initial plan, all three real plans classified") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "134217728") {
      val df = PerformanceLab.orders(spark, 1000).join(PerformanceLab.customers(spark), Seq("customer_id"))
      intercept[IllegalArgumentException] { Ch25Solutions.joinStrategy(df.queryExecution.executedPlan.toString) }
      val run = PerformanceLab.execute(df)
      assert(run.initialPlan.contains("SortMergeJoin"))
      assertEquals(Ch25Solutions.joinStrategy(run.finalPlan), "ShuffledHashJoin")
      Vector("broadcast" -> "BroadcastHashJoin", "merge" -> "SortMergeJoin", "shuffle_hash" -> "ShuffledHashJoin")
        .foreach { case (hint, expected) =>
          val q = PerformanceLab.execute(PerformanceLab.join(spark, hint))
          assertEquals(Ch25Solutions.joinStrategy(q.finalPlan), expected)
        }
    }
  }

  test("bài 2: stage report handles failed attempt, empty input and zero median") {
    def sample(stage: Int, duration: Long, failed: Boolean = false): TaskSample =
      TaskSample(stage, 0, 0, failed, duration, 0L, 0L, 0L, 0L, 0L)
    val reports = Ch25Solutions.stages(Vector(sample(1, 10), sample(1, 20), sample(1, 100),
      sample(1, 300, failed = true), sample(2, 0), sample(3, 5, failed = true)))
    assertEquals(reports.head, StageReport(1, 3, 1, 20L, 100L, Some(5.0)))
    assertEquals(reports(1).ratio, None)
    assertEquals(reports(2).successful, 0)
    assertEquals(Ch25Solutions.stages(Vector.empty), Vector.empty)
  }

  test("bài 3: byte sizing rounds up, bounds counts and avoids Long overflow") {
    assertEquals(Ch25Solutions.initialPartitions(129L, 64L, 200), 3)
    assertEquals(Ch25Solutions.initialPartitions(0L, 64L, 200), 1)
    assertEquals(Ch25Solutions.initialPartitions(Long.MaxValue, 1L, 200), 200)
    intercept[IllegalArgumentException] { Ch25Solutions.initialPartitions(1L, 0L, 200) }
    intercept[IllegalArgumentException] { Ch25Solutions.initialPartitions(-1L, 64L, 200) }
  }

  test("bài 4: salted revenue keeps totals, rejects duplicate/null dimension keys, handles empty facts") {
    val facts = PerformanceLab.orders(spark, 1000)
    val dim = PerformanceLab.customers(spark)
    val expected = PerformanceLab.revenue(facts.join(dim, Seq("customer_id"))).collect().toVector
    assertEquals(Ch25Solutions.checkedSaltedRevenue(facts, dim, 8).collect().toVector, expected)
    intercept[IllegalArgumentException] { Ch25Solutions.checkedSaltedRevenue(facts, dim.union(dim.limit(1)), 8) }
    val nullDim = dim.withColumn("customer_id", lit(null).cast("long"))
    intercept[IllegalArgumentException] { Ch25Solutions.checkedSaltedRevenue(facts, nullDim, 8) }
    assertEquals(Ch25Solutions.checkedSaltedRevenue(facts.limit(0), dim, 8).count(), 0L)
  }

  test("bài 5: two-phase monetary totals survive Long.MaxValue + 1 and preserve null/empty behavior") {
    val ss = spark
    import ss.implicits._
    val facts = Seq((1L, 0L, Long.MaxValue), (2L, 0L, 1L), (3L, 1L, 7L))
      .toDF("order_id", "customer_id", "amount").repartition(2)
    val expected = facts.groupBy("customer_id").agg(sum(col("amount").cast(DecimalType(38, 0))).as("total"))
    val actual = Ch25Solutions.saltedTotals(facts, 8)
    assertEquals(actual.exceptAll(expected).count(), 0L)
    assertEquals(expected.exceptAll(actual).count(), 0L)
    assertEquals(actual.filter(col("customer_id") === 0).first().getDecimal(1).toPlainString, "9223372036854775808")
    assertEquals(Ch25Solutions.saltedTotals(facts.limit(0), 8).count(), 0L)
    val nullAmounts = facts.withColumn("amount", lit(null).cast("long"))
    assert(Ch25Solutions.saltedTotals(nullAmounts, 8).collect().forall(_.isNullAt(1)))
  }

  test("bài 6: cache ownership cleanup on success, consumer failure, and double ownership rejection") {
    val df = PerformanceLab.orders(spark, 1000).withColumn("expensive", sha2(col("order_id").cast("string"), 256))
    val before = spark.sparkContext.getPersistentRDDs.keySet
    assertEquals(Ch25Solutions.withCache(df)(cached => cached.count() + cached.count()), 2000L)
    assertEquals(df.storageLevel, StorageLevel.NONE)
    assertEquals(spark.sparkContext.getPersistentRDDs.keySet, before)
    intercept[IllegalStateException] {
      Ch25Solutions.withCache(df) { cached =>
        cached.count()
        throw new IllegalStateException("consumer failed after materialization")
      }
    }
    assertEquals(df.storageLevel, StorageLevel.NONE)
    assertEquals(spark.sparkContext.getPersistentRDDs.keySet, before)
    df.cache()
    try intercept[IllegalArgumentException] { Ch25Solutions.withCache(df)(_.count()) }
    finally df.unpersist(blocking = true)
  }
}
