package vn.scalaeng.sparkprod

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.LongAdder

/** JSON tối giản, đủ cho log và manifest — tránh thêm thư viện vào job Spark (mỗi dependency là một rủi ro xung đột classpath với cụm). */
object Json {
  def str(s: String): String = {
    val b = new StringBuilder("\"")
    s.foreach {
      case '"'          => b ++= "\\\""
      case '\\'         => b ++= "\\\\"
      case '\n'         => b ++= "\\n"
      case '\r'         => b ++= "\\r"
      case '\t'         => b ++= "\\t"
      case c if c < ' ' => b ++= f"\\u${c.toInt}%04x"
      case c            => b += c
    }
    b += '"'
    b.toString
  }

  def value(v: Any): String = v match {
    case null                 => "null"
    case s: String            => str(s)
    case b: Boolean           => b.toString
    case n: Int               => n.toString
    case n: Long              => n.toString
    case n: Double            => if (n.isNaN || n.isInfinite) str(n.toString) else n.toString
    case n: BigDecimal        => n.bigDecimal.toPlainString
    case o: Option[_]         => o.fold("null")(value)
    case m: Map[_, _]         => obj(m.toSeq.map { case (k, x) => k.toString -> x }: _*)
    case xs: Iterable[_]      => xs.map(value).mkString("[", ",", "]")
    case other                => str(other.toString)
  }

  def obj(fields: (String, Any)*): String = fields.map { case (k, v) => s"${str(k)}:${value(v)}" }.mkString("{", ",", "}")
}

/** Log CÓ CẤU TRÚC: mỗi dòng một JSON có `run_id` — hệ thống log (Loki, Elasticsearch, CloudWatch) lọc theo trường thay vì grep chuỗi.
  * Không bao giờ log dữ liệu khách hàng hay bí mật; chỉ số đếm, đường dẫn, mã lỗi.
  */
final class JobLog(runId: String, sink: String => Unit) {
  def info(event: String, fields: (String, Any)*): Unit  = emit("INFO", event, fields)
  def warn(event: String, fields: (String, Any)*): Unit  = emit("WARN", event, fields)
  def error(event: String, fields: (String, Any)*): Unit = emit("ERROR", event, fields)
  private def emit(level: String, event: String, fields: Seq[(String, Any)]): Unit =
    sink(Json.obj(Seq("ts" -> Instant.now().toString, "level" -> level, "run_id" -> runId, "event" -> event) ++ fields: _*))
}

/** Metric của job lấy từ SparkListener: đếm task thành công/thất bại, bản ghi đọc/ghi, byte shuffle. Gắn với MỘT job group;
  * `close()` chạy job "rào chắn" để chắc chắn hàng đợi sự kiện đã xử lý hết (Chương 22 mục 3.4) rồi mới đọc số.
  */
final class JobMetrics private (sc: SparkContext, group: String) extends SparkListener {
  val tasksSucceeded, tasksFailed, recordsRead, recordsWritten, bytesWritten, shuffleRead, shuffleWrite, stagesCompleted = new LongAdder
  @volatile private var stageIds = Set.empty[Int]
  @volatile private var barrierJob = -1
  private val barrier = s"$group-barrier"
  private val drained = new CountDownLatch(1)

  private def groupOf(e: SparkListenerJobStart) = Option(e.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))

  override def onJobStart(e: SparkListenerJobStart): Unit = groupOf(e) match {
    case Some(`group`)   => stageIds = stageIds ++ e.stageIds
    case Some(`barrier`) => barrierJob = e.jobId
    case _               => ()
  }
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit =
    if (stageIds(e.stageInfo.stageId)) stagesCompleted.increment()
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
    if (stageIds(e.stageId)) {
      if (e.taskInfo.successful) tasksSucceeded.increment() else tasksFailed.increment()
      Option(e.taskMetrics).foreach { m =>
        recordsRead.add(m.inputMetrics.recordsRead)
        recordsWritten.add(m.outputMetrics.recordsWritten)
        bytesWritten.add(m.outputMetrics.bytesWritten)
        shuffleRead.add(m.shuffleReadMetrics.totalBytesRead)
        shuffleWrite.add(m.shuffleWriteMetrics.bytesWritten)
      }
    }
  override def onJobEnd(e: SparkListenerJobEnd): Unit = if (e.jobId == barrierJob) drained.countDown()

  def asMap: Map[String, Long] = Map(
    "spark_tasks_succeeded" -> tasksSucceeded.sum(), "spark_tasks_failed" -> tasksFailed.sum(),
    "spark_stages_completed" -> stagesCompleted.sum(), "spark_records_read" -> recordsRead.sum(),
    "spark_records_written" -> recordsWritten.sum(), "spark_bytes_written" -> bytesWritten.sum(),
    "spark_shuffle_read_bytes" -> shuffleRead.sum(), "spark_shuffle_write_bytes" -> shuffleWrite.sum())

  /** Dừng đo: chạy job rào chắn, chờ có hạn. `Left(lý do)` khi không chắc số đã đầy đủ (rào chắn lỗi, hết hạn) — người gọi QUYẾT ĐỊNH ghi
    * "metric không đầy đủ", không được nhận số dở dang như số thật (review T16 M3). Listener và job group LUÔN được gỡ, kể cả khi lỗi.
    */
  def close(timeoutSeconds: Long = 60): Either[String, Map[String, Long]] =
    try {
      sc.setJobGroup(barrier, "metrics barrier", interruptOnCancel = false)
      sc.parallelize(Seq(1), 1).count()
      if (drained.await(timeoutSeconds, TimeUnit.SECONDS)) Right(asMap)
      else Left(s"listener chưa xử lý xong sự kiện sau ${timeoutSeconds}s — metric không đầy đủ")
    } catch {
      case e: Exception => Left(s"không chạy được job rào chắn: $e")
    } finally {
      scala.util.Try(sc.clearJobGroup())
      sc.removeSparkListener(this)
    }
}

object JobMetrics {
  /** Bắt đầu đo: mọi action chạy trên thread này sau lời gọi đều thuộc group của lần chạy. */
  def start(sc: SparkContext, runId: String): JobMetrics = {
    val group = s"ch26-$runId-${UUID.randomUUID().toString.take(8)}"
    val m = new JobMetrics(sc, group)
    sc.addSparkListener(m)
    sc.setJobGroup(group, s"Ch26 job $runId", interruptOnCancel = false)
    m
  }

  /** Định dạng text của Prometheus (cho node_exporter textfile collector hoặc Pushgateway) — job batch không có endpoint để scrape.
    * Giá trị nhãn escape `\`, `"` và xuống dòng `\n` theo exposition format.
    */
  def prometheus(metrics: Map[String, Long], labels: Map[String, String]): String = {
    val l = labels.toSeq.sorted.map { case (k, v) => s"""$k="${v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")}"""" }.mkString(",")
    metrics.toSeq.sorted.map { case (k, v) => s"# TYPE $k gauge\n$k{$l} $v" }.mkString("", "\n", "\n")
  }
}
