package vn.scalaeng.distributed.solutions

import java.nio.file.Files
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import vn.scalaeng.distributed.{DistributedLab, Measurement}
import scala.concurrent.duration._

class ExercisesSuite extends munit.FunSuite {
  override val munitTimeout: Duration = 120.seconds
  private var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = DistributedLab.session("Ch32 solutions")
    spark.sparkContext.setLogLevel("ERROR")
  }
  override def afterAll(): Unit = spark.stop()

  test("E1 value mapping retains partitioner, including empty input") {
    val input = Exercises.prepare(DistributedLab.pairs(spark, 20), 4)
    val mapped = Exercises.doubleValues(input)
    assertEquals(mapped.partitioner, Some(new HashPartitioner(4)))
    assertEquals(mapped.collect().toMap, (0L until 20L).map(_ -> 2L).toMap)
    assertEquals(Exercises.doubleValues(DistributedLab.pairs(spark, 0)).count(), 0L)
  }
  test("E2 reuse prepared partitioner avoids a new shuffle") {
    val in = Exercises.prepare(DistributedLab.pairs(spark, 100), 4).persist()
    try {
      in.count()
      val m = Measurement.measure(spark.sparkContext) {
        in.join(Exercises.doubleValues(in), new HashPartitioner(4)).count()
      }
      assertEquals(m.value, 500L) // mỗi phía 5 dòng/khóa: 20 * 5 * 5 = 500
      assertEquals(m.writeBytes, 0L)
      intercept[IllegalArgumentException] { Exercises.prepare(in, 0) }
    } finally in.unpersist(blocking = true)
  }
  test("E3 exact totals with overflow, cancellation, multiple partitions and empty input") {
    val in = spark.sparkContext.parallelize(Seq(1L -> Long.MaxValue, 1L -> 1L,
      2L -> Long.MinValue, 2L -> Long.MaxValue, 2L -> 1L), 4)
    assertEquals(Exercises.totals(in, 3).collect().toMap,
      Map(1L -> (BigInt(Long.MaxValue) + 1), 2L -> BigInt(0)))
    assertEquals(Exercises.totals(DistributedLab.pairs(spark, 0), 2).count(), 0L)
  }
  test("E4 real retry journal reveals duplicate; successful output alone hides it") {
    val run = DistributedLab.retry(spark, Files.createTempDirectory("ch32-solution-journal"))
    assertEquals(run.result.value.sorted, Vector(100L, 200L))
    assertEquals(Exercises.duplicateIds(run.journal), Set(100L))
    assertEquals(Exercises.duplicateIds(Vector.empty), Set.empty[Long])
  }
  test("E5 checkpoint retains values after dropping cache without upstream calls") {
    val sc = spark.sparkContext
    sc.setCheckpointDir(Files.createTempDirectory("ch32-solution-checkpoint").toUri.toString)
    val calls = sc.longAccumulator("solution upstream calls")
    val in = sc.range(0, 30, 1, 3).map { i => calls.add(1); i }
    val done = Exercises.materializeCheckpoint(in)
    val before = calls.value.longValue()
    assertEquals(done.collect().toVector.sorted, (0L until 30L).toVector)
    assertEquals(calls.value.longValue(), before)
    assert(done.toDebugString.contains("ReliableCheckpointRDD"))
  }
  test("E6 exact and approximate distinct handle NULL and empty input") {
    val input = spark.sql("SELECT CAST(id AS BIGINT) customer_id FROM VALUES (1), (1), (2), (NULL) t(id)")
    assertEquals(Exercises.cardinality(input, false).first().getLong(0), 2L)
    assertEquals(Exercises.cardinality(input, true).first().getLong(0), 2L)
    assertEquals(Exercises.cardinality(input.limit(0), false).first().getLong(0), 0L)
    assertEquals(Exercises.cardinality(input.limit(0), true).first().getLong(0), 0L)
  }
  test("E7 throughput units and zero duration; final partial batch preserved") {
    assertEquals(Exercises.throughput(2500, 500), Some(5000.0))
    assertEquals(Exercises.throughput(1, 0), None)
    intercept[IllegalArgumentException] { Exercises.throughput(-1, 20) }
    val b = DistributedLab.batches(spark, 25, 12, 2)
    assertEquals(b.rows, 25L)
    assertEquals(b.jobs, 3)
    assert(b.firstMs <= b.totalMs)
  }
}
