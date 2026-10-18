package vn.scalaeng.distributed

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

final case class OrderEvent(id: Long, customer: String, amount: Long) extends Serializable
final case class QueryRun(rows: Vector[Row], plan: String, metrics: Measurement[Vector[Row]])
final case class RetryRun(result: Measurement[Vector[Long]], journal: Vector[String])
final case class BatchRun(rows: Long, batch: Int, partitions: Int, firstMs: Long, totalMs: Long, jobs: Int,
    checksum: Long)

object DistributedLab {
  def session(app: String, extra: Map[String, String] = Map.empty): SparkSession = {
    val b = SparkSession.builder().appName(app).master("local[2,3]")
      .config("spark.ui.enabled", "false")
      // Lab dùng chung classpath; tắt isolation để giảm socket loopback trên Windows.
      .config("spark.sql.artifact.isolation.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    extra.foreach { case (k, v) => b.config(k, v) }
    b.getOrCreate()
  }

  def execute(df: DataFrame): QueryRun = {
    val m = Measurement.measure(df.sparkSession.sparkContext) { df.collect().toVector }
    QueryRun(m.value, df.queryExecution.executedPlan.toString, m)
  }

  def pairs(spark: SparkSession, n: Long = 60000L, partitions: Int = 4): RDD[(Long, Long)] =
    spark.sparkContext.range(0L, n, 1L, partitions).map(i => (i % 20L, 1L))

  def rangePairs(input: RDD[(Long, Long)], n: Int): RDD[(Long, Long)] = {
    require(n > 0, "partitions must be positive")
    input.repartitionAndSortWithinPartitions(new RangePartitioner(n, input))
  }

  def joined(spark: SparkSession, lost: Boolean): Measurement[Vector[(Long, (Long, String))]] = {
    val p = new HashPartitioner(4)
    val facts = pairs(spark, 2000L).partitionBy(p).persist()
    val dim = spark.sparkContext.parallelize((0L until 20L).map(k => k -> s"C$k"), 2)
      .partitionBy(p).persist()
    try {
      facts.count()
      dim.count() // shuffle chuẩn bị và materialize nằm ngoài phép đo consumer
      val left = if (lost) facts.map { case (k, v) => (k, v) } else facts.mapValues(_ + 0L)
      Measurement.measure(spark.sparkContext) { left.join(dim, p).collect().toVector }
    } finally {
      facts.unpersist(blocking = true)
      dim.unpersist(blocking = true)
    }
  }

  /** Path tường minh giữ dữ liệu trong thư mục tạm của bên gọi; chỉ dùng session catalog. */
  def bucketJoin(spark: SparkSession, root: Path, readViaPath: Boolean = false): QueryRun = {
    val suffix = java.util.UUID.randomUUID().toString.replace("-", "")
    val a = "ch32_orders_" + suffix
    val b = "ch32_customers_" + suffix
    try {
      spark.range(0, 200, 1, 4).select(col("id").as("order_id"), (col("id") % 20).as("customer_id"))
        .write.format("parquet").option("path", root.resolve("orders").toString)
        .bucketBy(4, "customer_id").sortBy("customer_id").saveAsTable(a)
      spark.range(20).select(col("id").as("customer_id"))
        .write.format("parquet").option("path", root.resolve("customers").toString)
        .bucketBy(4, "customer_id").sortBy("customer_id").saveAsTable(b)
      val left = if (readViaPath) spark.read.parquet(root.resolve("orders").toString) else spark.table(a)
      val right = if (readViaPath) spark.read.parquet(root.resolve("customers").toString) else spark.table(b)
      execute(left.hint("merge").join(right.hint("merge"), Seq("customer_id")))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $a")
      spark.sql(s"DROP TABLE IF EXISTS $b")
    }
  }

  def payloadBytes(kryo: Boolean): Int = {
    val conf = new SparkConf(false).registerKryoClasses(Array(classOf[OrderEvent], classOf[Array[OrderEvent]]))
    val serializer = if (kryo) new KryoSerializer(conf) else new JavaSerializer(conf)
    serializer.newInstance().serialize(Array.tabulate(1000)(i => OrderEvent(i, s"C${i % 20}", 100L))).remaining()
  }

  def captured(size: Int): Int => Int = {
    require(size > 0, "capture size must be positive")
    val data = Array.tabulate[Byte](size)(i => (i % 127).toByte)
    (i: Int) => data(i % data.length).toInt
  }

  def closureBytes(size: Int): Int =
    new JavaSerializer(new SparkConf(false)).newInstance().serialize(captured(size)).remaining()

  /** Ghi TRƯỚC khi ném lỗi: Spark không rollback được hiệu ứng bên ngoài này.
    * Mỗi partition một file; demo local chỉ có một attempt ghi vào mỗi file tại một thời điểm.
    * Journal local này chưa phải sink phân tán có transaction cho production.
    */
  def retry(spark: SparkSession, root: Path): RetryRun = {
    Files.createDirectories(root)
    val dir = root.toAbsolutePath.toString
    val measured = Measurement.measure(spark.sparkContext) {
      spark.sparkContext.parallelize(Seq(100L, 200L), 2).mapPartitions { it =>
        val ctx = TaskContext.get()
        val data = it.toVector
        val text = data.map(id => s"$id,${ctx.attemptNumber()}\n").mkString
        Files.write(Paths.get(dir, s"part-${ctx.partitionId()}.csv"), text.getBytes(UTF_8),
          StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0)
          throw new IllegalStateException("CH32_AFTER_EXTERNAL_APPEND")
        data.iterator
      }.collect().toVector
    }
    val journal = (0 until 2).toVector.flatMap(p => Files.readAllLines(root.resolve(s"part-$p.csv"), UTF_8).asScala)
    RetryRun(measured, journal)
  }

  /** Input đã sẵn sàng: đo xử lý từ submission, chưa gồm thời gian chờ nguồn.
    * firstMs là lúc batch đầu HOÀN TẤT, chưa phải độ trễ task/record/event đầu tiên.
    */
  def batches(spark: SparkSession, total: Int, batch: Int, partitions: Int): BatchRun = {
    require(total > 0 && batch > 0 && partitions > 0, "sizes must be positive")
    val start = System.nanoTime()
    val initial = BatchRun(0L, batch, partitions, 0L, 0L, 0, 0L)
    val completed = (0 until total by batch).foldLeft(initial) { (acc, offset) =>
      val until = math.min(total.toLong, offset.toLong + batch.toLong)
      val result = spark.sparkContext.range(offset.toLong, until, 1L, partitions)
        .map(i => (1L, cpuChecksum(i)))
        .fold((0L, 0L)) { case ((n1, c1), (n2, c2)) => (n1 + n2, c1 ^ c2) }
      val first = if (acc.jobs == 0) (System.nanoTime() - start) / 1000000L else acc.firstMs
      acc.copy(rows = acc.rows + result._1, firstMs = first,
        jobs = acc.jobs + 1, checksum = acc.checksum ^ result._2)
    }
    completed.copy(totalMs = (System.nanoTime() - start) / 1000000L)
  }

  private def cpuChecksum(i: Long): Long = {
    var state = i
    var step = 0
    // Công việc CPU nhân tạo để quan sát chi phí xử lý.
    // Overflow trong checksum là số học modulo có chủ đích; không dùng cho tiền.
    while (step < 2048) {
      state = (state ^ (state >>> 13)) * 6364136223846793005L + 1L
      step += 1
    }
    state
  }
}

final class NonSerializableOwner {
  private val worker = new Thread()
  def broken(input: RDD[Int]): RDD[String] = input.map(i => s"$i:${worker.getName}")
}
