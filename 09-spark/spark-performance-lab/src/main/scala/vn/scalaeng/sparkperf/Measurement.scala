package vn.scalaeng.sparkperf

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

final case class TaskSample(stage: Int, partition: Int, attempt: Int, failed: Boolean,
    durationMs: Long, gcMs: Long, memorySpill: Long, diskSpill: Long,
    shuffleRead: Long, shuffleWrite: Long)

final case class Measurement[A](value: A, elapsedMs: Long, tasks: Vector[TaskSample]) {
  def successful: Vector[TaskSample] = tasks.filterNot(_.failed)
  def stageSummary: String = successful.groupBy(_.stage).toVector.sortBy(_._1).map {
    case (stage, group) =>
      val times = group.map(_.durationMs).sorted
      s"stage=$stage tasks=${times.size} medianMs=${times(times.size / 2)} maxMs=${times.last}"
  }.mkString("; ")
  def summary: String = {
    val durations = successful.map(_.durationMs).sorted
    val median = if (durations.isEmpty) 0L else durations(durations.size / 2)
    s"ms=$elapsedMs tasks=${tasks.size} failed=${tasks.count(_.failed)} " +
      s"taskMedianMs=$median taskMaxMs=${durations.lastOption.getOrElse(0L)} " +
      s"gcMs=${tasks.map(_.gcMs).sum} memorySpill=${tasks.map(_.memorySpill).sum} " +
      s"diskSpill=${tasks.map(_.diskSpill).sum} " +
      s"shuffleRead=${tasks.map(_.shuffleRead).sum} shuffleWrite=${tasks.map(_.shuffleWrite).sum}"
  }
}

object Measurement {
  /** A marker job is submitted AFTER the synchronous action returns. Its JobEnd is a
    * FIFO barrier on the SAME shared listener queue, including earlier TaskEnd events.
    * Marker work is outside elapsedMs and excluded from the sample. No sleep/polling.
    * Intended for synchronous actions in this thread, not unjoined Future actions.
    */
  def measure[A](sc: SparkContext, timeoutSeconds: Long = 60)(action: => A): Measurement[A] = {
    val group = "ch25-" + UUID.randomUUID().toString
    val marker = group + "-barrier"
    val done = new CountDownLatch(1)
    val stages = mutable.Set.empty[Int]
    val markerJobs = mutable.Set.empty[Int]
    val samples = mutable.ArrayBuffer.empty[TaskSample]
    val properties = Vector("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    val saved = properties.map(k => k -> sc.getLocalProperty(k))
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = {
        val id = Option(e.properties).map(_.getProperty("spark.jobGroup.id")).orNull
        if (id == group) stages ++= e.stageIds
        if (id == marker) markerJobs += e.jobId
      }
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
        if (stages(e.stageId) && e.taskMetrics != null) {
          val m = e.taskMetrics
          samples += TaskSample(e.stageId, e.taskInfo.index, e.taskInfo.attemptNumber,
            e.taskInfo.failed, e.taskInfo.duration, m.jvmGCTime, m.memoryBytesSpilled,
            m.diskBytesSpilled, m.shuffleReadMetrics.totalBytesRead, m.shuffleWriteMetrics.bytesWritten)
        }
      }
      override def onJobEnd(e: SparkListenerJobEnd): Unit = {
        if (markerJobs(e.jobId)) done.countDown()
      }
    }
    sc.addSparkListener(listener)
    try {
      sc.setJobGroup(group, "Ch25 measured action")
      val start = System.nanoTime()
      val value = action
      val elapsed = (System.nanoTime() - start) / 1000000L
      sc.setJobGroup(marker, "Ch25 listener barrier")
      sc.parallelize(Seq(1), 1).count()
      require(done.await(timeoutSeconds, TimeUnit.SECONDS), "Listener barrier timed out; metrics are incomplete")
      Measurement(value, elapsed, samples.toVector)
    } finally {
      saved.foreach { case (k, v) => sc.setLocalProperty(k, v) }
      sc.removeSparkListener(listener)
    }
  }
}
