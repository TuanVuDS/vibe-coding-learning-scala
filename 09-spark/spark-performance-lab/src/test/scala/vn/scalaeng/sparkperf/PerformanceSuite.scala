package vn.scalaeng.sparkperf

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class PerformanceSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = PerformanceLab.session("Ch25 Tests")
    spark.sparkContext.setLogLevel("ERROR")
  }
  override def afterAll(): Unit = spark.stop()

  test("lazy RDD: transformations do no work until an action") {
    val calls = spark.sparkContext.longAccumulator("ch25 lazy")
    val rdd = spark.sparkContext.parallelize(1 to 100, 2).map { i => calls.add(1); i * 2 }
    assertEquals(calls.value.longValue(), 0L)
    assertEquals(rdd.sum().toLong, 10100L)
    assertEquals(calls.value.longValue(), 100L)
  }

  test("barrier awaits a slow listener; marker task excluded; group properties restored") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val blocker = new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
        if (entered.getCount > 0) {
          entered.countDown()
          require(release.await(30, TimeUnit.SECONDS))
        }
      }
    }
    val pool = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    spark.sparkContext.addSparkListener(blocker)
    try {
      val pending = Future {
        spark.sparkContext.setJobGroup("outer-group", "outer description")
        val measured = Measurement.measure(spark.sparkContext) {
          spark.sparkContext.parallelize(Seq(1, 2), 2).count()
        }
        assertEquals(spark.sparkContext.getLocalProperty("spark.jobGroup.id"), "outer-group")
        measured
      }
      assert(entered.await(30, TimeUnit.SECONDS))
      assert(!pending.isCompleted)
      release.countDown()
      val result = Await.result(pending, 60.seconds)
      assertEquals(result.value, 2L)
      assertEquals(result.tasks.size, 2)
    } finally {
      release.countDown()
      spark.sparkContext.removeSparkListener(blocker)
      pool.shutdownNow()
    }
  }

  test("unrelated group excluded and properties restored even when action throws") {
    val sc = spark.sparkContext
    val result = Measurement.measure(sc) {
      sc.parallelize(Seq(1), 1).count()
      val measuredGroup = sc.getLocalProperty("spark.jobGroup.id")
      sc.setJobGroup("unrelated", "other job")
      try sc.parallelize(1 to 10, 3).count()
      finally sc.setJobGroup(measuredGroup, "measured again")
    }
    assertEquals(result.tasks.size, 1)
    val before = sc.getLocalProperty("spark.jobGroup.id")
    intercept[IllegalArgumentException] {
      Measurement.measure(sc) { throw new IllegalArgumentException("driver error") }
    }
    assertEquals(sc.getLocalProperty("spark.jobGroup.id"), before)
  }

  test("three hints produce three final executed join strategies with identical rows") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "0") {
      val runs = Vector("broadcast" -> "BroadcastHashJoin", "merge" -> "SortMergeJoin",
        "shuffle_hash" -> "ShuffledHashJoin").map { case (hint, operator) =>
        val q = PerformanceLab.execute(PerformanceLab.join(spark, hint))
        assert(q.finalPlan.contains("isFinalPlan=true"), q.finalPlan)
        assert(PerformanceLab.finalSection(q.finalPlan).contains(operator), q.finalPlan)
        assertEquals(q.rows.size, 100000)
        q
      }
      assertEquals(runs(0).rows.toSet, runs(1).rows.toSet)
      assertEquals(runs(1).rows.toSet, runs(2).rows.toSet)
      val broadcast = PerformanceLab.finalSection(runs.head.finalPlan)
      assert(broadcast.contains("BroadcastExchange"))
      assert(!broadcast.contains("Exchange hashpartitioning"), broadcast)
    }
  }

  test("AQE can change initial sort merge to shuffled hash when automatic broadcast is disabled") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "134217728") {
      val q = PerformanceLab.execute(PerformanceLab.orders(spark).join(PerformanceLab.customers(spark), Seq("customer_id")))
      assert(q.initialPlan.contains("SortMergeJoin"), q.initialPlan)
      assert(PerformanceLab.finalSection(q.finalPlan).contains("ShuffledHashJoin"), q.finalPlan)
    }
  }

  test("200 vs 8 shuffle partitions and AQE coalescing preserve grouped counts") {
    val runs = Vector((200, false), (8, false), (200, true)).map { case (n, aqe) =>
      PerformanceLab.withSql(spark, "spark.sql.shuffle.partitions" -> n.toString,
          "spark.sql.adaptive.enabled" -> aqe.toString) {
        val df = PerformanceLab.orders(spark, 1000).groupBy("customer_id").count()
        val q = PerformanceLab.execute(df)
        val partitions = df.rdd.getNumPartitions
        if (aqe) {
          assert(partitions < 200)
          assert(PerformanceLab.finalSection(q.finalPlan).contains("AQEShuffleRead coalesced"), q.finalPlan)
        } else assertEquals(partitions, n)
        q.rows.toSet
      }
    }
    assertEquals(runs(0), runs(1))
    assertEquals(runs(1), runs(2))
  }

  test("90 percent hot key remains hot after hash repartition; deterministic salt spreads it") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "false") {
      val facts = PerformanceLab.orders(spark)
      val hash = PerformanceLab.partitionRows(facts.repartition(8, col("customer_id")))
      val salted = facts.withColumn("salt", pmod(xxhash64(col("order_id")), lit(8)))
      val spread = PerformanceLab.partitionRows(salted.repartition(8, col("customer_id"), col("salt")))
      assertEquals(hash.sum, 100000L)
      assert(hash.max >= 90000L)
      assertEquals(spread.sum, 100000L)
      assert(spread.max < hash.max)
    }
  }

  test("salted inner join preserves row multiset and revenue, not just count") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "false") {
      val facts = PerformanceLab.orders(spark, 1000)
      val dim = PerformanceLab.customers(spark)
      val plain = facts.join(dim, Seq("customer_id"))
      val salted = PerformanceLab.saltedJoin(facts, dim, 8).select(plain.columns.toIndexedSeq.map(col): _*)
      assertEquals(plain.exceptAll(salted).count(), 0L)
      assertEquals(salted.exceptAll(plain).count(), 0L)
      assertEquals(PerformanceLab.revenue(plain).collect().toVector, PerformanceLab.revenue(salted).collect().toVector)
      intercept[IllegalArgumentException] { PerformanceLab.saltedJoin(facts, dim, 0) }
    }
  }

  test("cache default has memory and disk; consumers keep same result and unpersist cleans up") {
    val df = spark.range(10).cache()
    try {
      assert(df.storageLevel.useMemory)
      assert(df.storageLevel.useDisk)
      assert(df.storageLevel.deserialized)
    } finally df.unpersist(blocking = true)
    val before = spark.sparkContext.getPersistentRDDs.keySet
    assertEquals(PerformanceLab.cacheTrial(spark, false, 2).value, 25600000L)
    assertEquals(PerformanceLab.cacheTrial(spark, true, 2).value, 25600000L)
    assertEquals(spark.sparkContext.getPersistentRDDs.keySet, before)
  }

  test("writing repartition(8) vs repartition(2) changes data file count in this unpartitioned write") {
    PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "false") {
      Seq(8, 2).foreach { n =>
        val dir = Files.createTempDirectory("ch25-test-files").resolve("output")
        assertEquals(PerformanceLab.writeFiles(PerformanceLab.orders(spark, 1000).repartition(n), dir), n.toLong)
        assertEquals(spark.read.parquet(dir.toString).count(), 1000L)
      }
    }
  }

  test("serializer payload byte counts are measured, not a speed contract") {
    assert(PerformanceLab.serializedBytes(false) > 0)
    assert(PerformanceLab.serializedBytes(true) > 0)
  }

  test("Parquet builtin comparison is pushed down but Scala UDF comparison is opaque") {
    val dir = Files.createTempDirectory("ch25-udf-test").resolve("orders")
    PerformanceLab.orders(spark, 1000).write.parquet(dir.toString)
    val source = spark.read.parquet(dir.toString)
    val isSmall = udf((id: Long) => id < 100L)
    val builtin = PerformanceLab.execute(source.filter(col("order_id") < 100L))
    val custom = PerformanceLab.execute(source.filter(isSmall(col("order_id"))))
    assert(builtin.finalPlan.contains("LessThan(order_id,100)"), builtin.finalPlan)
    assert(!custom.finalPlan.contains("LessThan(order_id,100)"), custom.finalPlan)
    assertEquals(builtin.rows.toSet, custom.rows.toSet)
    assertEquals(builtin.rows.size, 100)
  }

  test("local[2,3] retries failed task once and records failed attempt before barrier") {
    val m = PerformanceLab.retry(spark)
    assertEquals(m.value, 5050L)
    assertEquals(m.tasks.count(_.failed), 1)
    assert(m.tasks.exists(t => t.partition == 0 && t.attempt == 1 && !t.failed))
    assertEquals(m.successful.size, 2)
  }
}
