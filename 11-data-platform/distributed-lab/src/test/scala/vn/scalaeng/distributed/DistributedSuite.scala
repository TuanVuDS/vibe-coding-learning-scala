package vn.scalaeng.distributed

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.apache.spark.{HashPartitioner, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class DistributedSuite extends munit.FunSuite {
  override val munitTimeout: Duration = 120.seconds
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = DistributedLab.session("Ch32 tests", Map(
      "spark.sql.sources.bucketing.enabled" -> "true",
      "spark.sql.sources.bucketing.autoBucketedScan.enabled" -> "false"))
    spark.sparkContext.setLogLevel("ERROR")
  }
  override def afterAll(): Unit = spark.stop()

  test("partitioner retained by mapValues/filter, lost by map; ranges ordered") {
    val p = new HashPartitioner(4)
    val input = DistributedLab.pairs(spark, 200).partitionBy(p)
    assertEquals(input.mapValues(_ + 1L).partitioner, Some(p))
    assertEquals(input.filter(_._1 < 10).partitioner, Some(p))
    assertEquals(input.map(identity).partitioner, None)
    val keys = DistributedLab.rangePairs(input, 4).mapPartitions(it => Iterator(it.map(_._1).toVector)).collect().toVector
    assert(keys.forall(k => k == k.sorted))
    assertEquals(keys.flatten, keys.flatten.sorted)
    assertEquals(keys.flatten.size, 200)
  }
  test("copartitioned materialized join has no new shuffle; lost metadata shuffles same rows") {
    val kept = DistributedLab.joined(spark, false)
    val lost = DistributedLab.joined(spark, true)
    assertEquals(kept.value.sortBy(_._1), lost.value.sortBy(_._1))
    assertEquals(kept.value.size, 2000)
    assertEquals(kept.writeBytes, 0L)
    assertEquals(kept.readBytes, 0L)
    assert(lost.writeBytes > 0L)
  }
  test("catalog bucket metadata avoids shuffle in this actual sort merge plan") {
    val q = DistributedLab.bucketJoin(spark, Files.createTempDirectory("ch32-bucket-test"))
    assertEquals(q.rows.size, 200)
    assert(q.plan.contains("SortMergeJoin"), q.plan)
    assert(q.plan.contains("Bucketed: true"), q.plan)
    assert(!q.plan.contains("Exchange"), q.plan)
    assertEquals(q.metrics.writeBytes, 0L)
    val path = DistributedLab.bucketJoin(spark, Files.createTempDirectory("ch32-bucket-path-test"), readViaPath = true)
    assertEquals(path.rows.sortBy(_.toString), q.rows.sortBy(_.toString))
    assert(path.plan.contains("Exchange hashpartitioning"), path.plan)
    assert(!path.plan.contains("Bucketed: true"), path.plan)
    assert(path.metrics.writeBytes > 0L)
  }
  test("map side combine preserves sums and sends fewer shuffle records") {
    val in = DistributedLab.pairs(spark, 6000)
    val p = new HashPartitioner(4)
    val a = Measurement.measure(spark.sparkContext) { in.reduceByKey(p, _ + _).collect().toMap }
    val b = Measurement.measure(spark.sparkContext) { in.groupByKey(p).mapValues(_.sum).collect().toMap }
    assertEquals(a.value, (0L until 20L).map(_ -> 300L).toMap)
    assertEquals(a.value, b.value)
    assert(a.tasks.map(_.writeRecords).sum < b.tasks.map(_.writeRecords).sum)
    assertEquals(a.tasks.map(_.readRemote).sum, 0L)
  }
  test("partial and final hash aggregate; exact and approximate cardinality execute") {
    val df = spark.range(0, 4000, 1, 4).select((col("id") % 400).as("customer_id"))
    val q = DistributedLab.execute(df.groupBy("customer_id").count())
    assert(q.plan.contains("partial_count"), q.plan)
    assert(q.plan.contains("Exchange hashpartitioning"), q.plan)
    assertEquals(q.rows.size, 400)
    assert(q.rows.forall(_.getLong(1) == 10L))
    val exact = DistributedLab.execute(df.agg(count_distinct(col("customer_id"))))
    val approx = DistributedLab.execute(df.agg(approx_count_distinct(col("customer_id"), 0.05)))
    assertEquals(exact.rows.head.getLong(0), 400L)
    assert(approx.rows.head.getLong(0) > 0L) // RSD không phải giới hạn sai số tất định
    assert(approx.plan.contains("partial_approx_count_distinct"), approx.plan)
  }
  test("bytes measured for both serializers and captured closures; unsafe owner fails") {
    assert(DistributedLab.payloadBytes(false) > 0)
    assert(DistributedLab.payloadBytes(true) > 0)
    assert(DistributedLab.closureBytes(1048576) > DistributedLab.closureBytes(16) + 1000000)
    val error = intercept[SparkException] {
      new NonSerializableOwner().broken(spark.sparkContext.parallelize(Seq(1), 1)).collect()
    }
    assert(error.getMessage.contains("Task not serializable"), error.toString)
  }
  test("retry after append duplicates external effect but task output has two records") {
    val r = DistributedLab.retry(spark, Files.createTempDirectory("ch32-retry-test"))
    assertEquals(r.result.value.sorted, Vector(100L, 200L))
    assertEquals(r.journal, Vector("100,0", "100,1", "200,0"))
    assertEquals(r.result.tasks.count(_.failed), 1)
    assert(r.result.tasks.exists(t => t.partition == 0 && t.attempt == 1 && !t.failed))
    assert(r.result.tasks.forall(_.stageAttempt == 0))
  }
  test("reliable checkpoint severs lineage; cache eviction recomputes") {
    val sc = spark.sparkContext
    sc.setCheckpointDir(Files.createTempDirectory("ch32-checkpoint-test").toUri.toString)
    val calls = sc.longAccumulator("ch32 test calls")
    val input = sc.range(0, 100, 1, 2).map { i => calls.add(1); i }.cache()
    try {
      input.count(); input.count()
      assertEquals(calls.value.longValue(), 100L)
      input.unpersist(blocking = true)
      input.count()
      assertEquals(calls.value.longValue(), 200L)
    } finally input.unpersist(blocking = true)
    // Phải yêu cầu RDD.checkpoint TRƯỚC job đầu tiên trên RDD đó.
    val fresh = sc.range(0, 100, 1, 2).map { i => calls.add(1); i }.cache()
    try {
      fresh.checkpoint()
      fresh.count()
      assert(fresh.isCheckpointed)
      assert(fresh.toDebugString.contains("ReliableCheckpointRDD"))
      val before = calls.value.longValue()
      fresh.unpersist(blocking = true)
      assertEquals(fresh.count(), 100L)
      assertEquals(calls.value.longValue(), before)
    } finally fresh.unpersist(blocking = true)
  }
  test("batch submission keeps row count including final partial batch and rejects invalid sizes") {
    val b = DistributedLab.batches(spark, 23, 10, 2)
    assertEquals(b.rows, 23L)
    assertEquals(b.jobs, 3)
    assertEquals(b.checksum, DistributedLab.batches(spark, 23, 23, 4).checksum)
    assert(b.firstMs <= b.totalMs)
    intercept[IllegalArgumentException] { DistributedLab.batches(spark, 1, 0, 2) }
  }
  test("listener waits for delayed callback; excludes marker and restores properties") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val blocker = new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (entered.getCount > 0) {
        entered.countDown()
        require(release.await(30, TimeUnit.SECONDS))
      }
    }
    val pool = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    val sc = spark.sparkContext
    sc.addSparkListener(blocker)
    try {
      val pending = Future {
        sc.setJobGroup("outer", "restore me")
        val m = Measurement.measure(sc) { sc.parallelize(Seq(1, 2), 2).count() }
        assertEquals(sc.getLocalProperty("spark.jobGroup.id"), "outer")
        m
      }
      assert(entered.await(30, TimeUnit.SECONDS))
      assert(!pending.isCompleted)
      release.countDown()
      assertEquals(Await.result(pending, 60.seconds).tasks.size, 2)
    } finally {
      release.countDown()
      sc.removeSparkListener(blocker)
      pool.shutdownNow()
    }
  }
  test("listener excludes unrelated group and restores properties on exception") {
    val sc = spark.sparkContext
    val m = Measurement.measure(sc) {
      sc.parallelize(Seq(1), 1).count()
      val group = sc.getLocalProperty("spark.jobGroup.id")
      sc.setJobGroup("other", "unrelated")
      try sc.parallelize(1 to 6, 3).count()
      finally sc.setJobGroup(group, "measured")
    }
    assertEquals(m.tasks.size, 1)
    val before = sc.getLocalProperty("spark.jobGroup.id")
    intercept[IllegalArgumentException] {
      Measurement.measure(sc) { throw new IllegalArgumentException("CH32_DRIVER_ERROR") }
    }
    assertEquals(sc.getLocalProperty("spark.jobGroup.id"), before)
  }
}
