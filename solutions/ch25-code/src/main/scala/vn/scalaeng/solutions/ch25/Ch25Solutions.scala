package vn.scalaeng.solutions.ch25

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel
import vn.scalaeng.sparkperf.{PerformanceLab, TaskSample}

final case class StageReport(stage: Int, successful: Int, failed: Int,
    medianMs: Long, maxMs: Long, ratio: Option[Double])

object Ch25Solutions {
  // Bài 1: read ONLY final section; reject an AQE plan captured before action.
  def joinStrategy(plan: String): String = {
    require(!plan.contains("AdaptiveSparkPlan") || plan.contains("isFinalPlan=true"), "AQE plan is not final")
    val finalPlan = PerformanceLab.finalSection(plan)
    val found = Vector("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin").filter(finalPlan.contains)
    require(found.size == 1, "expected one supported join strategy")
    found.head
  }

  // Bài 2: compare task durations WITHIN a stage, retaining failed attempt count.
  // Upper median for an even number of samples; no divide-by-zero/empty maximum.
  def stages(tasks: Vector[TaskSample]): Vector[StageReport] =
    tasks.groupBy(_.stage).toVector.sortBy(_._1).map { case (stage, attempts) =>
      val times = attempts.filterNot(_.failed).map(_.durationMs).sorted
      val median = if (times.isEmpty) 0L else times(times.size / 2)
      val maximum = times.lastOption.getOrElse(0L)
      StageReport(stage, times.size, attempts.count(_.failed), median, maximum,
        if (median == 0L) None else Some(maximum.toDouble / median))
    }

  // Bài 3: first estimate from bytes, NOT a guarantee of balanced partition sizes.
  // BigInt prevents overflow for bytes close to Long.MaxValue.
  def initialPartitions(bytes: Long, targetBytes: Long, maxPartitions: Int): Int = {
    require(bytes >= 0 && targetBytes > 0 && maxPartitions > 0, "invalid partition sizing inputs")
    ((BigInt(bytes) + targetBytes - 1) / targetBytes).max(BigInt(1)).min(BigInt(maxPartitions)).toInt
  }

  // Bài 4: make the dimension contract executable before applying salting.
  // Small dimension only: validation itself scans/shuffles, so include its cost.
  def checkedSaltedRevenue(facts: DataFrame, dimension: DataFrame, buckets: Int): DataFrame = {
    require(dimension.filter(col("customer_id").isNull).limit(1).count() == 0L, "null dimension key")
    require(dimension.groupBy("customer_id").count().filter(col("count") > 1).limit(1).count() == 0L,
      "duplicate dimension key")
    PerformanceLab.revenue(PerformanceLab.saltedJoin(facts, dimension, buckets))
  }

  // Bài 5: two-phase SUM only; not average-of-averages or count-distinct.
  // Monetary integers are promoted BEFORE aggregation. Contract: total fits DECIMAL(38,0), ANSI on.
  def saltedTotals(facts: DataFrame, buckets: Int): DataFrame = {
    require(buckets > 0)
    val salted = facts.withColumn("salt", pmod(xxhash64(col("order_id")), lit(buckets)))
    val partial = salted.groupBy("customer_id", "salt")
      .agg(sum(col("amount").cast(DecimalType(38, 0))).as("partial"))
    partial.groupBy("customer_id").agg(sum("partial").as("total"))
  }

  // Bài 6: caller transfers ownership of an initially uncached DataFrame to this scope.
  def withCache[A](df: DataFrame)(consume: DataFrame => A): A = {
    require(df.storageLevel == StorageLevel.NONE, "input already cached")
    val cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    try consume(cached) finally cached.unpersist(blocking = true)
  }
}
