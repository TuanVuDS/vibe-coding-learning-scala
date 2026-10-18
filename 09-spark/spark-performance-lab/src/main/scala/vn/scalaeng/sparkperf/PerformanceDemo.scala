package vn.scalaeng.sparkperf

import java.nio.file.Files
import org.apache.spark.sql.functions._

object PerformanceDemo {
  def main(args: Array[String]): Unit = {
    val spark = PerformanceLab.session("Ch25 Performance")
    spark.sparkContext.setLogLevel("ERROR")
    try {
      println(s"ENV os=${System.getProperty("os.name")} arch=${System.getProperty("os.arch")} " +
        s"java=${System.getProperty("java.version")} cpus=${Runtime.getRuntime.availableProcessors()} " +
        s"maxHeap=${Runtime.getRuntime.maxMemory()} master=${spark.sparkContext.master}")
      spark.range(10000).count() // small warm-up, NOT a benchmark methodology
      val calls = spark.sparkContext.longAccumulator("demo lazy")
      val lazyRdd = spark.sparkContext.parallelize(1 to 100, 2).map { i => calls.add(1); i * 2 }
      println(s"LAZY before=${calls.value}")
      println(s"LAZY sum=${lazyRdd.sum().toLong} after=${calls.value}")
      PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "0") {
        Seq("broadcast", "merge", "shuffle_hash").foreach { hint =>
          val q = PerformanceLab.execute(PerformanceLab.join(spark, hint))
          println(s"JOIN $hint rows=${q.rows.size} ${q.metrics.summary}")
          println(q.finalPlan)
        }
      }
      Seq((200, false), (8, false), (200, true)).foreach { case (partitions, adaptive) =>
        PerformanceLab.withSql(spark, "spark.sql.shuffle.partitions" -> partitions.toString,
            "spark.sql.adaptive.enabled" -> adaptive.toString) {
          val df = PerformanceLab.orders(spark).groupBy("customer_id").count()
          val q = PerformanceLab.execute(df)
          println(s"PARTITIONS initial=$partitions aqe=$adaptive resultPartitions=${df.rdd.getNumPartitions} ${q.metrics.summary}")
          println(q.finalPlan)
        }
      }
      PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "false") {
        val facts = PerformanceLab.orders(spark)
        println("SKEW hash=" + PerformanceLab.partitionRows(facts.repartition(8, col("customer_id"))).mkString(","))
        val salted = facts.withColumn("salt", pmod(xxhash64(col("order_id")), lit(8)))
        println("SKEW salted=" + PerformanceLab.partitionRows(salted.repartition(8, col("customer_id"), col("salt"))).mkString(","))
      }
      Seq(false, true).foreach { aqe =>
        PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> aqe.toString,
            "spark.sql.autoBroadcastJoinThreshold" -> "-1",
            "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
            "spark.sql.adaptive.coalescePartitions.enabled" -> "false",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "2",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "1024",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "4096",
            "spark.sql.adaptive.forceOptimizeSkewedJoin" -> "true") {
          val q = PerformanceLab.execute(PerformanceLab.join(spark, "merge"))
          println(s"SKEWJOIN aqe=$aqe rows=${q.rows.size} ${q.metrics.summary}")
          println("SKEWSTAGES " + q.metrics.stageSummary)
          println(q.finalPlan)
          if (!aqe) {
            val salted = PerformanceLab.execute(PerformanceLab.saltedJoin(
              PerformanceLab.orders(spark), PerformanceLab.customers(spark), 8))
            println(s"SKEWJOIN salted rows=${salted.rows.size} ${salted.metrics.summary}")
            println("SALTSTAGES " + salted.metrics.stageSummary)
          }
        }
      }
      Seq(1, 2).foreach { uses => Seq(false, true).foreach { cached =>
        val trial = PerformanceLab.cacheTrial(spark, cached, uses)
        println(s"CACHE uses=$uses cached=$cached total=${trial.value} ${trial.summary}")
      } }
      val cached = spark.range(100).cache()
      try println("CACHE storage=" + cached.storageLevel) finally cached.unpersist(blocking = true)
      PerformanceLab.withSql(spark, "spark.sql.adaptive.enabled" -> "false") {
        Seq(8, 2).foreach { n =>
          val dir = Files.createTempDirectory("ch25-files-").resolve("output")
          val files = PerformanceLab.writeFiles(PerformanceLab.orders(spark).repartition(n), dir)
          println(s"FILES partitions=$n parquet=$files")
        }
      }
      println(s"SERIALIZE javaBytes=${PerformanceLab.serializedBytes(false)} kryoBytes=${PerformanceLab.serializedBytes(true)}")
      val parquet = Files.createTempDirectory("ch25-udf").resolve("orders")
      PerformanceLab.orders(spark, 1000).write.parquet(parquet.toString)
      val source = spark.read.parquet(parquet.toString)
      val lessThan100 = udf((id: Long) => id < 100L)
      Seq("builtin" -> source.filter(col("order_id") < 100L),
        "udf" -> source.filter(lessThan100(col("order_id")))).foreach { case (name, df) =>
        val q = PerformanceLab.execute(df)
        println(s"FILTER $name rows=${q.rows.size} ${q.metrics.summary}")
        println(q.finalPlan)
      }
      val retry = PerformanceLab.retry(spark)
      println(s"RETRY sum=${retry.value} ${retry.summary}")
    } finally spark.stop()
  }
}

/** Run in its own bounded JVM: --java-opt=-Xmx768m. Forced spill threshold is
  * a diagnostic stress knob, NOT a production tuning recommendation.
  */
object MemoryDemo {
  def main(args: Array[String]): Unit = {
    val spark = PerformanceLab.session("Ch25 Memory", Map(
      "spark.memory.fraction" -> "0.2", "spark.shuffle.spill.numElementsForceSpillThreshold" -> "10000"))
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val rdd = spark.sparkContext.parallelize(0 until 200000, 2).map(i => (i % 1000, "x" * 128))
      val result = Measurement.measure(spark.sparkContext) {
        rdd.repartitionAndSortWithinPartitions(new org.apache.spark.HashPartitioner(2)).count()
      }
      println(s"MEMORY heap=${Runtime.getRuntime.maxMemory()} rows=${result.value} ${result.summary}")
    } finally spark.stop()
  }
}

/** New process for each serializer, because spark.serializer is a SparkContext setting. */
object SerializationDemo {
  def main(args: Array[String]): Unit = {
    val kind = args.headOption.getOrElse(System.getProperty("ch25.serializer", "Java"))
    require(Set("Java", "Kryo")(kind))
    val spark = PerformanceLab.session("Ch25 Serializer", Map(
      "spark.serializer" -> s"org.apache.spark.serializer.${kind}Serializer",
      "spark.kryo.classesToRegister" -> "vn.scalaeng.sparkperf.EventRecord"))
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val rdd = spark.sparkContext.parallelize(0 until 10000, 4)
        .map(i => (i % 10, EventRecord(i, s"C${i % 10}", 100L)))
      val m = Measurement.measure(spark.sparkContext) { rdd.partitionBy(new org.apache.spark.HashPartitioner(4)).count() }
      println(s"RDD serializer=$kind rows=${m.value} ${m.summary}")
    } finally spark.stop()
  }
}
