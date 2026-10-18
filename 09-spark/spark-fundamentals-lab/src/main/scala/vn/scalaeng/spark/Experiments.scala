package vn.scalaeng.spark

import org.apache.spark.{NarrowDependency, ShuffleDependency, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger

/** Mỗi thí nghiệm trả về SỐ ĐO (không chỉ in ra) để test kiểm tra được. */
object Experiments {

  final case class LazyResult(callsAfterTransform: Long, callsAfterAction: Long, rows: Long)

  /** 1. Lazy evaluation: transformation chỉ MÔ TẢ việc cần làm; chỉ action mới chạy. Đếm số lần hàm `map` thực sự chạy
    * bằng LongAccumulator (biến cộng dồn từ executor về driver).
    */
  def lazyEvaluation(spark: SparkSession, n: Long): LazyResult = {
    import spark.implicits._
    val calls   = spark.sparkContext.longAccumulator("map calls")
    val doubled = spark.range(0, n).map { x => calls.add(1); x * 2 } // transformation: CHƯA chạy
    val before  = calls.value
    val rows    = doubled.filter($"value" % 3 === 0).count()        // action: chạy cả chuỗi
    LazyResult(before, calls.value, rows)
  }

  final case class CacheResult(callsWithoutCache: Long, callsWithCache: Long)

  /** 2. Cache: không cache thì MỖI action tính lại từ đầu theo lineage; có cache thì action sau đọc từ bộ nhớ. */
  def cacheEffect(spark: SparkSession, n: Long): CacheResult = {
    import spark.implicits._
    def run(cache: Boolean): Long = {
      val calls = spark.sparkContext.longAccumulator("calls")
      val ds0   = spark.range(0, n).map { x => calls.add(1); x + 1 }
      val ds    = if (cache) ds0.cache() else ds0
      ds.count()
      ds.filter($"value" > 10).count()
      if (cache) ds.unpersist(blocking = true)
      calls.value
    }
    CacheResult(run(cache = false), run(cache = true))
  }

  /** 3. Narrow vs wide: chuỗi chỉ filter/select không cần trao đổi dữ liệu giữa partition; groupBy thì THƯỜNG cần — kế hoạch vật lý có
    * toán tử shuffle `Exchange hashpartitioning(...)`. "Thường": planner chỉ chèn shuffle khi phân phối dữ liệu hiện có CHƯA đáp ứng (xem `dependencyFacts`).
    */
  def physicalPlan(df: DataFrame): String = df.queryExecution.executedPlan.toString

  /** Có toán tử SHUFFLE trong plan không? `BroadcastExchange` cũng chứa chữ "Exchange" nhưng KHÔNG phải shuffle: nó gửi nguyên bảng nhỏ tới
    * mọi executor. Tìm chuỗi con "Exchange" sẽ nhầm (review chéo, M1).
    */
  def hasShuffleExchange(plan: String): Boolean =
    plan.linesIterator.exists(l => l.contains("Exchange ") && !l.contains("BroadcastExchange"))

  def narrowPlan(spark: SparkSession): String =
    physicalPlan(spark.range(0, 1000).filter(col("id") % 2 === 0).select((col("id") * 10).as("x")))

  def widePlan(spark: SparkSession): String =
    physicalPlan(spark.range(0, 1000).groupBy((col("id") % 10).as("k")).agg(count(lit(1)).as("n")))

  /** 4. Job → stage: đếm số stage HOÀN THÀNH của các job do `action` sinh ra. Shuffle là ranh giới giữa các stage.
    *
    * Listener nhận sự kiện BẤT ĐỒNG BỘ, qua hàng đợi. Bản đầu "chờ 500 ms rồi đọc": review chéo chèn một listener chậm đứng trước trên cùng hàng đợi
    * → action đã xong mà số đếm vẫn là 0 (M3); bản đầu còn đếm cả stage của job KHÁC chạy cùng lúc. Bản này:
    *   - gắn job group duy nhất cho `action` → chỉ đếm stage thuộc job của group đó;
    *   - sau action, chạy một job "rào chắn" nhỏ. Sự kiện trên một hàng đợi được xử lý THEO THỨ TỰ, nên khi listener nhận được
    *     JobEnd của rào chắn thì mọi sự kiện của `action` (xảy ra trước) chắc chắn đã được xử lý;
    *   - chờ có HẠN (`timeoutSeconds`), hết hạn thì NÉM LỖI — không trả một con số trông hợp lệ nhưng sai.
    * Ghi đè job group hiện có của thread (rồi xóa) — đủ cho bài học; Chương 26 dùng metric của Spark.
    */
  def stagesOf(spark: SparkSession, timeoutSeconds: Long = 60)(action: => Unit): Int = {
    val sc             = spark.sparkContext
    val group          = s"ch22-${UUID.randomUUID()}"
    val barrier        = s"ch22-barrier-${UUID.randomUUID()}"
    val groupStages    = ConcurrentHashMap.newKeySet[Int]()
    val barrierJobs    = ConcurrentHashMap.newKeySet[Int]()
    val completed      = new AtomicInteger(0)
    val barrierDone    = new CountDownLatch(1)
    def groupOf(e: SparkListenerJobStart) = Option(e.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = groupOf(e) match {
        case Some(`group`)   => e.stageIds.foreach(groupStages.add)
        case Some(`barrier`) => barrierJobs.add(e.jobId); ()
        case _               => ()
      }
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit =
        if (groupStages.contains(e.stageInfo.stageId)) { completed.incrementAndGet(); () }
      override def onJobEnd(e: SparkListenerJobEnd): Unit = if (barrierJobs.contains(e.jobId)) barrierDone.countDown()
    }
    sc.addSparkListener(listener)
    try {
      sc.setJobGroup(group, "đếm stage", interruptOnCancel = false)
      action
      sc.setJobGroup(barrier, "rào chắn sự kiện", interruptOnCancel = false)
      sc.parallelize(Seq(1), 1).count()
      if (!barrierDone.await(timeoutSeconds, TimeUnit.SECONDS))
        throw new TimeoutException(s"listener chưa xử lý xong sự kiện sau $timeoutSeconds s — không trả số đếm dở dang")
      completed.get()
    } finally {
      sc.clearJobGroup()
      sc.removeSparkListener(listener)
    }
  }

  final case class DependencyFacts(
    coalesceIsNarrow: Boolean,
    coalesceParentsOfPartition0: Int,
    onePartitionGroupByHasShuffle: Boolean,
    onePartitionCountStages: Int,
    broadcastJoinPlanHasBroadcastExchange: Boolean,
    broadcastJoinPlanHasShuffle: Boolean
  )

  /** 4b. Phản ví dụ cho các quy tắc "đoán theo tên hàm" (review chéo, M1):
    *   - `coalesce(1)` gộp 4 partition cha vào 1 partition con nhưng vẫn là NARROW dependency: không phân tán lại bản ghi theo khóa.
    *   - groupBy trên dữ liệu 1 partition: dữ liệu đã "cùng chỗ" → planner KHÔNG chèn shuffle; `count()` chỉ 1 stage.
    *   - broadcast join: có `BroadcastExchange` nhưng KHÔNG có shuffle.
    */
  def dependencyFacts(spark: SparkSession): DependencyFacts = {
    val coalesced = spark.sparkContext.parallelize(1 to 100, 4).coalesce(1)
    val dep       = coalesced.dependencies.head
    val parents   = dep match {
      case n: NarrowDependency[_] => n.getParents(0).size
      case _                      => -1
    }
    val big   = spark.range(0, 10000, 1, 4).select((col("id") % 100).as("k"), col("id").as("v"))
    val small = spark.range(0, 100, 1, 1).select(col("id").as("k"), (col("id") * 10).as("w"))
    val bplan = physicalPlan(big.join(broadcast(small), "k"))
    DependencyFacts(
      coalesceIsNarrow = dep.isInstanceOf[NarrowDependency[_]] && !dep.isInstanceOf[ShuffleDependency[_, _, _]],
      coalesceParentsOfPartition0 = parents,
      onePartitionGroupByHasShuffle = hasShuffleExchange(physicalPlan(spark.range(0, 1000, 1, 1).groupBy((col("id") % 3).as("k")).count())),
      onePartitionCountStages = stagesOf(spark)(spark.range(0, 1000, 1, 1).count(): Unit),
      broadcastJoinPlanHasBroadcastExchange = bplan.contains("BroadcastExchange"),
      broadcastJoinPlanHasShuffle = hasShuffleExchange(bplan)
    )
  }

  /** 4c. Hash partitioning KHÔNG bảo đảm đều: mọi bản ghi cùng khóa về CÙNG một partition. Trả số dòng của từng partition (review chéo, M2). */
  def hashPartitionSizes(spark: SparkSession, rows: Int, hotKeyShare: Double, n: Int): List[Long] = {
    val hot = (rows * hotKeyShare).toInt
    val df  = spark.range(0, rows.toLong, 1, 4).select(when(col("id") < hot, lit("HOT")).otherwise(col("id").cast("string")).as("k"))
    val counts = df.repartition(n, col("k")).groupBy(spark_partition_id().as("p")).count().collect()
      .map(r => r.getInt(0) -> r.getLong(1)).toMap
    (0 until n).map(i => counts.getOrElse(i, 0L)).toList
  }

  final case class PartitionResult(initial: Int, afterRepartition: Int, afterCoalesce: Int, repartitionHasExchange: Boolean, coalesceHasExchange: Boolean)

  /** 5. Partition: repartition(n) luôn SHUFFLE (có thể tăng hoặc giảm); coalesce(n) chỉ GỘP partition sẵn có, không shuffle. */
  def partitions(spark: SparkSession): PartitionResult = {
    val base = spark.range(0, 100000, 1, numPartitions = 4)
    val rep  = base.repartition(8)
    val coa  = base.coalesce(2)
    PartitionResult(
      base.rdd.getNumPartitions,
      rep.rdd.getNumPartitions,
      coa.rdd.getNumPartitions,
      hasShuffleExchange(physicalPlan(rep.toDF())),
      hasShuffleExchange(physicalPlan(coa.toDF()))
    )
  }

  final case class FaultResult(sum: Long, failedAttempts: Int, lineage: String)

  /** 6. Fault tolerance: task đầu tiên của partition 0 cố ý THẤT BẠI. Spark chạy lại task đó (lần 2) bằng cách tính lại
    * partition từ LINEAGE — chuỗi transformation đã ghi nhận — và job vẫn ra đúng kết quả.
    * Cần session `maxFailures >= 2`. Đếm lần thất bại bằng biến của DRIVER vì (ở local mode) task chạy cùng JVM.
    */
  def faultTolerance(spark: SparkSession): FaultResult = {
    FaultCounter.failed.set(0)
    val rdd = spark.sparkContext.parallelize(1L to 1000L, numSlices = 4).map { x =>
      val ctx = TaskContext.get()
      if (ctx.partitionId() == 0 && ctx.attemptNumber() == 0 && x == 1L) {
        FaultCounter.failed.incrementAndGet()
        throw new IllegalStateException("executor 'chết' giữa chừng (giả lập)")
      }
      x * 2
    }
    FaultResult(rdd.sum().toLong, FaultCounter.failed.get(), rdd.toDebugString)
  }
}

/** Biến đếm dùng chung trong JVM — chỉ có nghĩa ở local mode (trên cluster, task chạy ở JVM khác). */
object FaultCounter {
  val failed = new AtomicInteger(0)
}
