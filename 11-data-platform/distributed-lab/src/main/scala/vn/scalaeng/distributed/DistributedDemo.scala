package vn.scalaeng.distributed

import java.nio.file.{Files, Paths}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.functions._

object DistributedDemo {
  def main(args: Array[String]): Unit = {
    val spark = DistributedLab.session("Ch32 distributed experiments", Map(
      "spark.sql.sources.bucketing.enabled" -> "true",
      "spark.sql.sources.bucketing.autoBucketedScan.enabled" -> "false"))
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try {
      println(s"ENV spark=${spark.version} scala=${scala.util.Properties.versionNumberString} " +
        s"java=${System.getProperty("java.version")} cpu=${Runtime.getRuntime.availableProcessors()} " +
        s"master=${sc.master} heap=${Runtime.getRuntime.maxMemory()} " +
        s"artifactIsolation=${spark.conf.get("spark.sql.artifact.isolation.enabled")}")
      sc.parallelize(1 to 100, 2).count() // khởi động trước; chưa phải benchmark có kiểm soát
      val p = new HashPartitioner(4)
      val hash = DistributedLab.pairs(spark, 200L).partitionBy(p)
      println(s"PARTITION hash=${hash.partitioner} mapValues=${hash.mapValues(_ + 1L).partitioner} " +
        s"filter=${hash.filter(_._1 < 10L).partitioner} map=${hash.map(identity).partitioner}")
      val ranges = DistributedLab.rangePairs(DistributedLab.pairs(spark, 200L), 4)
      println("RANGE " + ranges.mapPartitionsWithIndex { (index, it) =>
        val keys = it.map(_._1).toVector
        Iterator(s"$index:${keys.minOption}-${keys.maxOption}:${keys.size}")
      }.collect().mkString(" "))
      Vector(false, true).foreach { lost =>
        val m = DistributedLab.joined(spark, lost)
        println(s"JOIN lostPartitioner=$lost rows=${m.value.size} ${m.summary}")
      }
      val bucket = DistributedLab.bucketJoin(spark, Files.createTempDirectory("ch32-buckets"))
      println(s"BUCKET rows=${bucket.rows.size} ${bucket.metrics.summary}\n${bucket.plan}")
      val pathJoin = DistributedLab.bucketJoin(spark, Files.createTempDirectory("ch32-bucket-path"), readViaPath = true)
      require(pathJoin.rows.sortBy(_.toString) == bucket.rows.sortBy(_.toString))
      println(s"BUCKET_PATH rows=${pathJoin.rows.size} ${pathJoin.metrics.summary}\n${pathJoin.plan}")
      val pairs = DistributedLab.pairs(spark)
      val combined = Measurement.measure(sc) { pairs.reduceByKey(p, _ + _).collect().toVector.sortBy(_._1) }
      val grouped = Measurement.measure(sc) {
        pairs.groupByKey(p).mapValues(_.sum).collect().toVector.sortBy(_._1)
      }
      require(combined.value == grouped.value)
      println(s"COMBINE ${combined.summary}")
      println(s"GROUP ${grouped.summary}")
      val input = spark.range(0, 120000, 1, 4).select((col("id") % 20000).as("customer_id"))
      val counts = DistributedLab.execute(input.groupBy("customer_id").count())
      println(s"HASH_AGG groups=${counts.rows.size} ${counts.metrics.summary}\n${counts.plan}")
      val exact = DistributedLab.execute(input.agg(count_distinct(col("customer_id")).as("customers")))
      val approx = DistributedLab.execute(input.agg(approx_count_distinct(col("customer_id"), 0.05).as("customers")))
      val real = exact.rows.head.getLong(0)
      val estimate = approx.rows.head.getLong(0)
      println(s"DISTINCT exact=$real ${exact.metrics.summary}")
      println(s"DISTINCT approx=$estimate relativeError=${math.abs(estimate - real).toDouble / real} ${approx.metrics.summary}")
      println(s"EXACT_PLAN\n${exact.plan}\nAPPROX_PLAN\n${approx.plan}")
      println(s"SERIALIZER java=${DistributedLab.payloadBytes(false)} kryo=${DistributedLab.payloadBytes(true)}")
      println(s"CLOSURE small=${DistributedLab.closureBytes(16)} captured1MiB=${DistributedLab.closureBytes(1048576)}")
      val large = (0 until 180000).map(_.toLong).toVector
      println(s"LARGE_TASK count=${sc.parallelize(large, 1).count()}")
      try {
        new NonSerializableOwner().broken(sc.parallelize(Seq(1), 1)).collect()
      } catch { case e: org.apache.spark.SparkException =>
        Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null)
          .foreach(cause => println("CAPTURE_ERROR " + cause.toString))
      }

      val calls = sc.longAccumulator("ch32 recomputation")
      val cached = sc.range(0, 100, 1, 2).map { i => calls.add(1); i + 1L }.cache()
      try {
        cached.count(); cached.count()
        println(s"CACHE calls=${calls.value}")
        cached.unpersist(blocking = true)
        cached.count()
        println(s"RECOMPUTE calls=${calls.value}")
      } finally cached.unpersist(blocking = true)
      val checkpointDir = Files.createTempDirectory("ch32-checkpoint")
      sc.setCheckpointDir(checkpointDir.toUri.toString)
      val reliable = sc.range(0, 100, 1, 2).map { i => calls.add(1); i + 1L }.cache()
      try {
        reliable.checkpoint()
        reliable.count()
        val before = calls.value.longValue()
        reliable.unpersist(blocking = true)
        reliable.count()
        println(s"CHECKPOINT set=${reliable.isCheckpointed} upstreamDelta=${calls.value.longValue() - before}\n${reliable.toDebugString}")
      } finally reliable.unpersist(blocking = true)
      val retry = DistributedLab.retry(spark, Files.createTempDirectory("ch32-journal"))
      println(s"RETRY output=${retry.result.value.sorted.mkString(",")} journal=${retry.journal.mkString(";")} ${retry.result.summary}")
      retry.result.tasks.foreach(t => println(s"ATTEMPT stage=${t.stage} stageAttempt=${t.stageAttempt} partition=${t.partition} taskAttempt=${t.attempt} failed=${t.failed}"))
      DistributedLab.batches(spark, 2000, 2000, 2) // khởi động nhánh CPU; ngoài số dòng/thời gian báo cáo
      val batchRuns = Vector((6000, 2), (30000, 2), (6000, 8), (30000, 8)).map { case (batch, partitions) =>
        val b = DistributedLab.batches(spark, 60000, batch, partitions)
        println(s"BATCH size=$batch partitions=$partitions rows=${b.rows} jobs=${b.jobs} firstMs=${b.firstMs} totalMs=${b.totalMs} checksum=${b.checksum}")
        b
      }
      require(batchRuns.map(_.checksum).distinct.size == 1)
      val files = Files.walk(Paths.get(sys.env("SPARK_LOCAL_DIRS")))
      try {
        val names = files.toArray.map(_.asInstanceOf[java.nio.file.Path].getFileName.toString)
        println(s"SHUFFLE_FILES retainedData=${names.count(n => n.startsWith("shuffle_") && n.endsWith(".data"))} " +
          s"retainedIndex=${names.count(n => n.startsWith("shuffle_") && n.endsWith(".index"))}")
      } finally files.close()
    } finally spark.stop()
  }
}

object MemoryDemo {
  def main(args: Array[String]): Unit = {
    val spark = DistributedLab.session("Ch32 forced spill", Map("spark.shuffle.spill.numElementsForceSpillThreshold" -> "1000"))
    spark.sparkContext.setLogLevel("WARN")
    try {
      val input = spark.sparkContext.range(0, 50000, 1, 4).map(i => (50000L - i, i))
      val m = Measurement.measure(spark.sparkContext) { input.sortByKey(numPartitions = 4).count() }
      println(s"FORCED_SPILL rows=${m.value} ${m.summary}")
      require(m.value == 50000L && m.tasks.map(_.diskSpill).sum > 0L, "forced spill not observed")
    } finally spark.stop()
  }
}

object SerializerDemo {
  def main(args: Array[String]): Unit = {
    val kind = sys.props.getOrElse("ch32.serializer", "Java")
    require(Set("Java", "Kryo")(kind))
    val spark = DistributedLab.session("Ch32 RDD serializer " + kind, Map(
      "spark.serializer" -> s"org.apache.spark.serializer.${kind}Serializer",
      "spark.kryo.classesToRegister" -> classOf[OrderEvent].getName))
    spark.sparkContext.setLogLevel("WARN")
    try {
      val input = spark.sparkContext.range(0, 20000, 1, 4)
        .map(i => (i % 20, OrderEvent(i, s"C${i % 20}", 100L)))
      val m = Measurement.measure(spark.sparkContext) {
        input.groupByKey(new HashPartitioner(4)).mapValues(_.size).collect().toMap
      }
      require(m.value.size == 20 && m.value.values.forall(_ == 1000))
      println(s"RDD_SERIALIZER kind=$kind keys=${m.value.size} rows=${m.value.values.sum} ${m.summary}")
    } finally spark.stop()
  }
}
