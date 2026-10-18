package vn.scalaeng.spark

import org.apache.spark.sql.functions._

object FundamentalsDemo {
  def main(args: Array[String]): Unit = {
    val spark = LocalSpark.session("fundamentals", cores = 2, maxFailures = 2)
    spark.sparkContext.setLogLevel("ERROR")
    try {
      println(s"Spark ${spark.version} · Scala ${scala.util.Properties.versionNumberString} · master=${spark.sparkContext.master}")

      val l = Experiments.lazyEvaluation(spark, 1000000L)
      println(s"[1] lazy: sau transformation map đã chạy ${l.callsAfterTransform} lần; sau action ${l.callsAfterAction} lần; kết quả ${l.rows} dòng")

      val c = Experiments.cacheEffect(spark, 1000000L)
      println(s"[2] cache: 2 action KHÔNG cache -> map chạy ${c.callsWithoutCache} lần; CÓ cache -> ${c.callsWithCache} lần")

      println("[3] kế hoạch vật lý — chỉ filter/select (narrow):")
      println(Experiments.narrowPlan(spark))
      println("[3] kế hoạch vật lý — groupBy (wide):")
      println(Experiments.widePlan(spark))

      val narrowStages = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).filter(col("id") < 100).collect(): Unit)
      val countStages  = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).filter(col("id") < 100).count(): Unit)
      val wideStages   = Experiments.stagesOf(spark)(spark.range(0, 100000, 1, 4).groupBy(col("id") % 10).count().collect(): Unit)
      println(s"[4] stage hoàn thành: filter+collect = $narrowStages; filter+count = $countStages; groupBy+collect = $wideStages")
      val d = Experiments.dependencyFacts(spark)
      println(s"[4] phản ví dụ: coalesce(1) narrow=${d.coalesceIsNarrow}, partition con 0 có ${d.coalesceParentsOfPartition0} cha; " +
        s"1 partition: groupBy có shuffle=${d.onePartitionGroupByHasShuffle}, count = ${d.onePartitionCountStages} stage; " +
        s"broadcast join: BroadcastExchange=${d.broadcastJoinPlanHasBroadcastExchange}, shuffle=${d.broadcastJoinPlanHasShuffle}")

      val p = Experiments.partitions(spark)
      println(s"[5] partition: ban đầu ${p.initial}; repartition(8) -> ${p.afterRepartition} (Exchange=${p.repartitionHasExchange}); " +
        s"coalesce(2) -> ${p.afterCoalesce} (Exchange=${p.coalesceHasExchange})")
      println(s"[5] repartition(8, k) với khóa nóng 90%: số dòng mỗi partition = ${Experiments.hashPartitionSizes(spark, 1000, 0.9, 8).mkString(", ")}")
      val grouped = spark.range(0, 100000, 1, 4).groupBy((col("id") % 10).as("k")).count()
      grouped.collect()
      println(s"[5] spark.sql.shuffle.partitions = ${spark.conf.get("spark.sql.shuffle.partitions")}; " +
        s"AQE = ${spark.conf.get("spark.sql.adaptive.enabled")}; số partition sau groupBy (sau khi chạy) = ${grouped.rdd.getNumPartitions}")

      val f = Experiments.faultTolerance(spark)
      println(s"[6] fault tolerance: tổng = ${f.sum}; số lần task thất bại rồi được chạy lại = ${f.failedAttempts}")
      println("[6] lineage:")
      println(f.lineage)
    } finally spark.stop()
  }
}
