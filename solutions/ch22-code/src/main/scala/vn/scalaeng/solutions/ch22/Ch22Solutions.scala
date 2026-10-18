package vn.scalaeng.solutions.ch22

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions._
import vn.scalaeng.spark.Experiments

import java.util.concurrent.atomic.AtomicInteger

/** Bài 1 — mô hình đơn hàng Chương 15 viết bằng Scala 2.13. */
object Model213 {
  /** Thay `enum OrderStatus` của Scala 3: sealed trait + case object; `values` tự liệt kê. */
  sealed trait OrderStatus extends Product with Serializable { def dbName: String }
  object OrderStatus {
    case object Pending   extends OrderStatus { val dbName = "PENDING" }
    case object Paid      extends OrderStatus { val dbName = "PAID" }
    case object Cancelled extends OrderStatus { val dbName = "CANCELLED" }
    val values: List[OrderStatus] = List(Pending, Paid, Cancelled)
    def fromDb(s: String): Either[String, OrderStatus] = values.find(_.dbName == s).toRight(s"trạng thái không hợp lệ: $s")
  }

  /** Thay `opaque type OrderId = Long`: value class. Khác opaque type — có thể bị boxing (ví dụ khi nằm trong List). */
  final case class OrderId(value: Long) extends AnyVal
  final case class UserId(value: Long) extends AnyVal

  final case class Order(id: OrderId, userId: UserId, status: OrderStatus, totalVnd: Long)

  /** Thay `extension (o: Order) def isOpen`: implicit class. */
  implicit class OrderOps(private val o: Order) extends AnyVal {
    def isOpen: Boolean = o.status == OrderStatus.Pending
  }
}

/** Bài 2 — đếm stage của ba job để kiểm chứng dự đoán. */
object StageCounts {
  def writeOnly(spark: SparkSession, out: String): Int =
    Experiments.stagesOf(spark)(spark.range(0, 10000, 1, 4).select((col("id") * 2).as("x")).write.mode("overwrite").parquet(out))

  def distinctCount(spark: SparkSession): Int =
    Experiments.stagesOf(spark)(spark.range(0, 10000, 1, 4).select((col("id") % 100).as("k")).distinct().count(): Unit)

  final case class JoinRun(stages: Int, finalStrategy: String)

  /** Join khi TẮT broadcast tự động. `autoBroadcastJoinThreshold = -1` KHÔNG "buộc" sort-merge join: nó chỉ loại broadcast tự động; AQE vẫn có thể
    * đổi sang shuffled hash join lúc chạy (review chéo, M5). Vì vậy (1) cố định cả cấu hình liên quan, (2) đọc chiến lược từ plan CUỐI sau khi chạy.
    * `extraConf` cho phép thí nghiệm đổi cấu hình và thấy chiến lược đổi theo.
    */
  def joinWithoutBroadcast(spark: SparkSession, extraConf: Map[String, String] = Map.empty): JoinRun = {
    val conf = Map(
      "spark.sql.autoBroadcastJoinThreshold"                   -> "-1",
      "spark.sql.adaptive.autoBroadcastJoinThreshold"          -> "-1",
      "spark.sql.join.preferSortMergeJoin"                     -> "true",
      "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "0"
    ) ++ extraConf
    val prev = conf.keys.map(k => k -> spark.conf.getOption(k)).toMap
    conf.foreach { case (k, v) => spark.conf.set(k, v) }
    try {
      val a = spark.range(0, 10000, 1, 4).select((col("id") % 100).as("k"), col("id").as("a"))
      val b = spark.range(0, 100, 1, 2).select(col("id").as("k"), (col("id") * 10).as("b"))
      val q = a.join(b, "k").groupBy().count() // chính là việc `count()` làm, nhưng giữ lại `q` để đọc plan sau khi chạy
      val stages = Experiments.stagesOf(spark)(q.collect(): Unit)
      // Sau khi chạy, AdaptiveSparkPlan in CẢ "Final Plan" lẫn "Initial Plan" — tìm chuỗi trên toàn bộ sẽ thấy SortMergeJoin của plan ban đầu.
      // Lấy riêng plan cuối.
      val plan = (q.queryExecution.executedPlan match {
        case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
        case other                    => other
      }).toString
      val strategy = Seq("SortMergeJoin", "ShuffledHashJoin", "BroadcastHashJoin").find(s => plan.contains(s)).getOrElse(plan)
      JoinRun(stages, strategy)
    } finally prev.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None)    => spark.conf.unset(k)
    }
  }
}

/** Bài 3 — một Dataset dùng cho HAI nhánh: không cache thì tính lại cho mỗi nhánh. */
object TwoBranches {
  def mapCalls(spark: SparkSession, cache: Boolean): Long = {
    import spark.implicits._
    val calls = spark.sparkContext.longAccumulator("calls")
    // spark.range trả Dataset[java.lang.Long] (không phải scala.Long) — `longValue` đổi sang Long của Scala.
    val clean = spark.range(0, 100000, 1, 4).map { x => calls.add(1); x.longValue }
      .select((col("value") % 3).as("campaign"), (col("value") % 7).as("day"))
    val ds = if (cache) clean.cache() else clean
    ds.groupBy("campaign").count().collect()
    ds.groupBy("day").count().collect()
    if (cache) ds.unpersist(blocking = true)
    calls.value
  }
}

/** Bài 4 — số partition sau shuffle khi TẮT AQE: đúng bằng spark.sql.shuffle.partitions. */
object ShufflePartitions {
  final case class Run(shufflePartitions: Int, partitionsAfter: Int, millis: Long)

  def measure(spark: SparkSession, shufflePartitions: Int): Run = {
    val prevAqe = spark.conf.get("spark.sql.adaptive.enabled")
    val prevSp  = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions.toString)
    try {
      val df: DataFrame = spark.range(0, 1000000, 1, 4).groupBy((col("id") % 100).as("k")).agg(sum("id"))
      val t0 = System.nanoTime
      df.collect()
      Run(shufflePartitions, df.rdd.getNumPartitions, (System.nanoTime - t0) / 1000000)
    } finally {
      spark.conf.set("spark.sql.adaptive.enabled", prevAqe)
      spark.conf.set("spark.sql.shuffle.partitions", prevSp)
    }
  }
}

/** Bài 5 — lỗi DỮ LIỆU: dòng xấu làm MỌI lần thử lỗi → job thất bại sau maxFailures lần. */
object PoisonRecord {
  val attempts = new AtomicInteger(0)

  def run(spark: SparkSession): Either[String, Long] = {
    attempts.set(0)
    val rdd = spark.sparkContext.parallelize(1 to 100, 2).map { x =>
      if (x == 42) { attempts.incrementAndGet(); throw new IllegalArgumentException(s"dòng xấu: $x (lần thử ${TaskContext.get().attemptNumber()})") }
      x.toLong
    }
    try Right(rdd.sum().toLong)
    catch { case e: org.apache.spark.SparkException => Left(e.getMessage.linesIterator.next()) }
  }
}

/** Bài 6 — "chia partition + map-side combine + shuffle" của Chương 20, bằng RDD API.
  *
  * Bản đầu cộng `Long`: với A = Long.MaxValue và 1, RDD trả -9223372036854775808 (tràn lặng lẽ) còn DataFrame ném
  * `SparkArithmeticException [ARITHMETIC_OVERFLOW]` (ANSI bật mặc định ở Spark 4) — hai cách "giống nhau" lại khác hành vi (review chéo, M4).
  * Chính sách chung: tổng là SỐ NGUYÊN KHÔNG TRÀN — `BigInt` ở RDD, `decimal(38,0)` ở DataFrame. Giống Chương 20: phép gộp phải toàn phần.
  */
object RddAggregate {
  def revenueBySku(spark: SparkSession, rows: Seq[(String, Long)]): Map[String, BigInt] =
    spark.sparkContext.parallelize(rows, 4)
      .mapPartitions(it => it.toSeq.groupMapReduce(_._1)(r => BigInt(r._2))(_ + _).iterator) // (b) tổng hợp cục bộ từng partition
      .reduceByKey(_ + _)                                                                     // (c)+(d) shuffle theo khóa rồi gộp
      .collect().toMap

  def viaDataFrame(spark: SparkSession, rows: Seq[(String, Long)]): Map[String, BigInt] = {
    import spark.implicits._
    rows.toDF("sku", "amount").groupBy("sku").agg(sum(col("amount").cast("decimal(38,0)")).as("total"))
      .collect().map(r => r.getString(0) -> BigInt(r.getDecimal(1).toBigIntegerExact)).toMap
  }

  /** Bản Long gốc của DataFrame — giữ lại để test thấy ANSI ném lỗi thay vì trả số sai. */
  def viaDataFrameLong(spark: SparkSession, rows: Seq[(String, Long)]): Map[String, Long] = {
    import spark.implicits._
    rows.toDF("sku", "amount").groupBy("sku").agg(sum("amount")).as[(String, Long)].collect().toMap
  }
}
