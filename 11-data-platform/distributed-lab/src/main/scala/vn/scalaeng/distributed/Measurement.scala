package vn.scalaeng.distributed

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

final case class TaskSample(stage: Int, stageAttempt: Int, partition: Int, attempt: Int,
    failed: Boolean, locality: String, durationMs: Long, deserializeMs: Long,
    readLocal: Long, readRemote: Long, blocks: Long, fetchWaitMs: Long,
    writeBytes: Long, writeRecords: Long, memorySpill: Long, diskSpill: Long)

final case class Measurement[A](value: A, elapsedMs: Long, tasks: Vector[TaskSample]) {
  def readBytes: Long = tasks.map(t => t.readLocal + t.readRemote).sum
  def writeBytes: Long = tasks.map(_.writeBytes).sum
  def summary: String = s"ms=$elapsedMs stages=${tasks.map(_.stage).distinct.size} attempts=${tasks.size} " +
    s"failed=${tasks.count(_.failed)} read=$readBytes remote=${tasks.map(_.readRemote).sum} " +
    s"write=$writeBytes records=${tasks.map(_.writeRecords).sum} blocks=${tasks.map(_.blocks).sum} " +
    s"fetchWaitMs=${tasks.map(_.fetchWaitMs).sum} spillMem=${tasks.map(_.memorySpill).sum} " +
    s"spillDisk=${tasks.map(_.diskSpill).sum} locality=${tasks.map(_.locality).distinct.sorted.mkString(",") }"
}

object Measurement {
  /** Chỉ đo action đồng bộ trên thread gọi. JobEnd của marker là rào FIFO
    * trên hàng đợi listener dùng chung; không đoán bằng sleep. Không đo công việc marker.
    * Không dùng khi các job đồng thời chia sẻ stage hoặc hàng đợi đã bỏ event.
    */
  def measure[A](sc: SparkContext, timeoutSeconds: Long = 60)(action: => A): Measurement[A] = {
    val group = "ch32-" + UUID.randomUUID().toString
    val marker = group + "-barrier"
    val done = new CountDownLatch(1)
    val stages = mutable.Set.empty[Int]
    val markers = mutable.Set.empty[Int]
    val tasks = mutable.ArrayBuffer.empty[TaskSample]
    val keys = Vector("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    val saved = keys.map(k => k -> sc.getLocalProperty(k))
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = {
        val id = Option(e.properties).map(_.getProperty("spark.jobGroup.id")).orNull
        if (id == group) stages ++= e.stageIds
        if (id == marker) markers += e.jobId
      }
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
        if (stages(e.stageId) && e.taskMetrics != null) {
          val m = e.taskMetrics
          val r = m.shuffleReadMetrics
          val w = m.shuffleWriteMetrics
          tasks += TaskSample(e.stageId, e.stageAttemptId, e.taskInfo.index,
            e.taskInfo.attemptNumber, e.taskInfo.failed, e.taskInfo.taskLocality.toString,
            e.taskInfo.duration, m.executorDeserializeTime, r.localBytesRead, r.remoteBytesRead,
            r.totalBlocksFetched, r.fetchWaitTime, w.bytesWritten, w.recordsWritten,
            m.memoryBytesSpilled, m.diskBytesSpilled)
        }
      }
      override def onJobEnd(e: SparkListenerJobEnd): Unit = {
        if (markers(e.jobId)) done.countDown()
      }
    }
    sc.addSparkListener(listener)
    try {
      sc.setJobGroup(group, "Ch32 measurement")
      val start = System.nanoTime()
      val value = action
      val elapsed = (System.nanoTime() - start) / 1000000L
      sc.setJobGroup(marker, "Ch32 listener barrier")
      sc.parallelize(Seq(1), 1).count()
      require(done.await(timeoutSeconds, TimeUnit.SECONDS), "CH32_LISTENER_TIMEOUT: incomplete metrics")
      Measurement(value, elapsed, tasks.toVector)
    } finally {
      saved.foreach { case (k, v) => sc.setLocalProperty(k, v) }
      sc.removeSparkListener(listener)
    }
  }
}
