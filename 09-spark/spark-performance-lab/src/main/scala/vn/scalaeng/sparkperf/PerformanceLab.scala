package vn.scalaeng.sparkperf

import java.nio.file.{Files, Path}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

final case class EventRecord(orderId: Long, customer: String, amount: Long) extends Serializable
final case class QueryRun(rows: Vector[Row], initialPlan: String, finalPlan: String,
    metrics: Measurement[Unit])

object PerformanceLab {
  def session(app: String, extra: Map[String, String] = Map.empty): SparkSession = {
    val builder = SparkSession.builder().master("local[2,3]").appName(app)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    extra.foreach { case (k, v) => builder.config(k, v) }
    builder.getOrCreate()
  }

  def withSql[A](spark: SparkSession, settings: (String, String)*)(action: => A): A = {
    val old = settings.map { case (k, _) => k -> spark.conf.getOption(k) }
    try {
      settings.foreach { case (k, v) => spark.conf.set(k, v) }
      action
    } finally old.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None) => spark.conf.unset(k)
    }
  }

  /** collect() the SAME DataFrame whose executedPlan is inspected, not df.count()'s different query. */
  def execute(df: DataFrame): QueryRun = {
    val initial = df.queryExecution.executedPlan.toString
    var rows = Vector.empty[Row]
    val metrics = Measurement.measure(df.sparkSession.sparkContext) { rows = df.collect().toVector }
    QueryRun(rows, initial, df.queryExecution.executedPlan.toString, metrics)
  }

  /** With AQE, the printed plan includes the initial plan too. Inspect only the final section. */
  def finalSection(plan: String): String = plan.split("== Initial Plan ==", 2).head

  def orders(spark: SparkSession, n: Long = 100000L): DataFrame =
    spark.range(0, n, 1, 8).select(col("id").as("order_id"),
      when(col("id") % 10 < 9, lit(0L)).otherwise(col("id") % 100 + 1).as("customer_id"),
      lit(100L).as("amount"))

  def customers(spark: SparkSession): DataFrame =
    spark.range(0, 101, 1, 4).select(col("id").as("customer_id"),
      concat(lit("S"), (col("id") % 3).cast("string")).as("segment"))

  def join(spark: SparkSession, hint: String): DataFrame =
    orders(spark).join(customers(spark).hint(hint), Seq("customer_id"))

  def revenue(df: DataFrame): DataFrame =
    df.groupBy("segment").agg(sum("amount").as("revenue")).orderBy("segment")

  /** Semantics restricted to INNER equijoin with UNIQUE, non-null dimension keys.
    * Each fact gets ONE deterministic salt; dimension is replicated across salts.
    * No amount aggregation here, so there is no averaging/count-distinct trap.
    */
  def saltedJoin(facts: DataFrame, dimension: DataFrame, buckets: Int): DataFrame = {
    require(buckets > 0, "buckets must be positive")
    val saltedFacts = facts.withColumn("salt", pmod(xxhash64(col("order_id")), lit(buckets)))
    val replicated = dimension.withColumn("salt", explode(sequence(lit(0), lit(buckets - 1))))
    saltedFacts.hint("merge").join(replicated.hint("merge"), Seq("customer_id", "salt")).drop("salt")
  }

  def partitionRows(df: DataFrame): Vector[Long] =
    df.rdd.mapPartitions(it => Iterator.single(it.foldLeft(0L)((n, _) => n + 1L))).collect().toVector

  def writeFiles(df: DataFrame, dir: Path): Long = {
    df.write.mode("errorifexists").parquet(dir.toString)
    val paths = Files.walk(dir)
    try paths.filter(p => p.getFileName.toString.endsWith(".parquet")).count()
    finally paths.close()
  }

  /** Same two consumers, including cache materialization cost. No timing assertion. */
  def cacheTrial(spark: SparkSession, cached: Boolean, uses: Int): Measurement[Long] = {
    require(uses > 0)
    val input = orders(spark, 200000).withColumn("payload", sha2(col("order_id").cast("string"), 256))
    val df = if (cached) input.cache() else input
    try Measurement.measure(spark.sparkContext) {
      (1 to uses).map(_ => df.agg(sum(length(col("payload")))).first().getLong(0)).sum
    } finally if (cached) df.unpersist(blocking = true)
  }

  /** Byte count for an object payload, NOT proof of end-to-end RDD shuffle speed. */
  def serializedBytes(kryo: Boolean): Int = {
    val conf = new SparkConf(false)
      .registerKryoClasses(Array(classOf[EventRecord], classOf[Array[EventRecord]]))
    val serializer = if (kryo) new KryoSerializer(conf) else new JavaSerializer(conf)
    serializer.newInstance().serialize(Array.tabulate(1000)(i => EventRecord(i, s"C${i % 10}", 100L))).remaining()
  }

  /** Failure before returning any record, then a successful retry of the whole partition.
    * This is task failure in one JVM, NOT simulated executor/process death.
    */
  def retry(spark: SparkSession): Measurement[Long] = Measurement.measure(spark.sparkContext) {
    spark.sparkContext.parallelize(1L to 100L, 2).mapPartitions { it =>
      val ctx = TaskContext.get()
      val data = it.toVector
      if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0)
        throw new IllegalStateException("CH25_TRANSIENT_TASK_FAILURE")
      data.iterator
    }.sum().toLong
  }
}
