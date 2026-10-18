package vn.scalaeng.sparkprod

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import vn.scalaeng.batch.Pipeline

import java.io.{EOFException, FileNotFoundException}
import java.net.{ConnectException, SocketException, SocketTimeoutException, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.nio.file.{AccessDeniedException, FileAlreadyExistsException, Files, NoSuchFileException, Paths, StandardCopyOption}
import java.time.Instant
import java.util.UUID
import java.util.zip.ZipException

/** Mã thoát có nghĩa: scheduler (Airflow, cron, Kubernetes Job) quyết định chạy lại hay báo người dựa vào nó. */
object ExitCode {
  val Succeeded          = 0
  val Failed             = 1 // lỗi bất ngờ (kể cả ghi manifest/metric thất bại SAU khi dữ liệu đã ghi) — cần người xem
  val ConfigInvalid      = 2 // sửa cấu hình, chạy lại vô ích
  val DataRejected       = 3 // dữ liệu vi phạm hợp đồng (gate, khóa trùng, thiếu lô) — báo bên gửi, KHÔNG retry
  val VerificationFailed = 4 // output không qua đối soát
  val RetriesExhausted   = 5 // lỗi tạm thời lặp lại quá số lần cho phép
}

/** Phân loại lỗi để quyết định RETRY — MỘT bộ phân loại dùng chung cho job và lời giải (review T16 M2).
  *
  * Thứ tự ưu tiên khi chuỗi `getCause` có nhiều loại: Data > Permanent > Transient > Unknown. Transient là DANH SÁCH CHO PHÉP (allowlist): chỉ
  * những nguyên nhân đã hiểu là "lần sau có thể khác" — timeout/mất kết nối mạng, và `TransientStorageException` mà adapter storage ném ra
  * khi nhận mã lỗi tạm thời (HTTP 503, throttling). KHÔNG suy "có IOException ⇒ tạm thời": ZIP/Parquet hỏng, file đã tồn tại, thiếu quyền
  * đều là IOException và lần sau vẫn hỏng y hệt. Lỗi chưa phân loại (Unknown) KHÔNG retry — cần người xem.
  */
object Failures {
  sealed trait Kind
  case object Data      extends Kind
  case object Permanent extends Kind
  case object Transient extends Kind
  case object Unknown   extends Kind

  def chain(e: Throwable): List[Throwable] = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toList

  private def data(c: Throwable): Boolean = c.isInstanceOf[IllegalArgumentException] || c.isInstanceOf[AnalysisException]
  private def permanent(c: Throwable): Boolean = c match {
    case _: NoSuchFileException | _: AccessDeniedException | _: FileAlreadyExistsException | _: FileNotFoundException | _: ZipException |
        _: EOFException | _: UnknownHostException => true
    case _ => false
  }
  private def transient(c: Throwable): Boolean = c match {
    case _: SocketTimeoutException | _: ConnectException | _: SocketException | _: TransientStorageException => true
    case _                                                                                                      => false
  }

  def classify(e: Throwable): Kind = {
    val cs = chain(e)
    if (cs.exists(data)) Data
    else if (cs.exists(permanent)) Permanent
    else if (cs.exists(transient)) Transient
    else Unknown
  }
}

/** Adapter storage ném lỗi này khi nhận mã lỗi mà nhà cung cấp tài liệu hóa là tạm thời (503 SlowDown, throttling). */
final class TransientStorageException(message: String, cause: Throwable = null) extends java.io.IOException(message, cause)

/** Job production bọc pipeline Chương 24: cấu hình → (gate theo lô → chạy, retry lỗi tạm thời) → manifest + metric. */
object Job {

  def isTransient(e: Throwable): Boolean = Failures.classify(e) == Failures.Transient

  final case class BatchRate(ingestDate: String, rows: Long, rejected: Long) {
    def rate: Double = if (rows == 0) 0.0 else rejected.toDouble / rows
  }

  /** Gate chỉ xét LÔ CỦA NGÀY CHẠY: lô cũ từng tệ không được chặn mọi lần chạy sau. Không có lô cho ngày chạy = dữ liệu chưa đến — báo lỗi,
    * không "thành công rỗng".
    */
  def gate(spark: SparkSession, c: JobConfig): Either[String, BatchRate] = {
    val v = Pipeline.validate(Pipeline.readRaw(spark, c.input, c.runDate))
      .filter(col("ingest_date") === lit(java.sql.Date.valueOf(c.runDate)))
    val r = v.agg(count(lit(1)), sum(when(size(col("errors")) > 0, 1).otherwise(0))).first()
    val rate = BatchRate(c.runDate.toString, r.getLong(0), if (r.isNullAt(1)) 0L else r.getLong(1))
    if (rate.rows == 0) Left(s"không có lô ingest_date=${c.runDate} trong ${c.input}")
    else if (rate.rate > c.maxRejectRate) Left(f"lô ${c.runDate}: ${rate.rejected}/${rate.rows} dòng bị loại (${rate.rate * 100}%.1f%%) > ngưỡng ${c.maxRejectRate * 100}%.1f%%")
    else Right(rate)
  }

  /** Ghi atomic: file tạm TÊN DUY NHẤT trong cùng thư mục rồi ATOMIC_MOVE. `replace = false` → đích đã có là lỗi (lịch sử không bị ghi đè).
    * Chỉ cho filesystem cục bộ/chia sẻ (java.nio); output trên HDFS/S3 cần Hadoop FileSystem API và ngữ nghĩa rename của storage đó.
    */
  private def writeAtomically(path: java.nio.file.Path, content: String, replace: Boolean): Unit = {
    Files.createDirectories(path.getParent)
    val tmp = Files.createTempFile(path.getParent, path.getFileName.toString + ".", ".tmp")
    try {
      Files.write(tmp, content.getBytes(StandardCharsets.UTF_8))
      if (replace) Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
      else Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE) // đích tồn tại → FileAlreadyExistsException
      ()
    } finally Files.deleteIfExists(tmp)
  }

  /** Điểm vào cho TEST: dùng `spark` có sẵn. Cấu hình sai thì không động tới session. */
  def run(spark: SparkSession, args: Seq[String], env: Map[String, String], sink: String => Unit,
      faults: Int => Unit = _ => ()): Int =
    launch(args, env, sink, () => spark, stopAfter = false, faults)

  /** Điểm vào chung: PARSE TRƯỚC, chỉ tạo session khi cấu hình hợp lệ (review T16 M1) — cấu hình sai không tốn khởi động Spark, không chạm
    * filesystem/mạng. `newSession` là factory để test chứng minh điều đó.
    */
  def launch(args: Seq[String], env: Map[String, String], sink: String => Unit, newSession: () => SparkSession, stopAfter: Boolean,
      faults: Int => Unit = _ => ()): Int =
    JobConfig.parse(args, env, () => UUID.randomUUID().toString) match {
      case Left(errors) =>
        new JobLog("-", sink).error("config_invalid", "errors" -> errors)
        ExitCode.ConfigInvalid
      case Right(c) =>
        val spark = newSession()
        try runConfigured(spark, c, sink, faults) finally if (stopAfter) spark.stop()
    }

  /** `finalEvent` = sự kiện log ghi SAU vòng retry (mức, tên); None khi đã log bên trong (gate chặn). */
  private final case class Outcome(code: Int, status: String, details: Map[String, Any], finalEvent: Option[(String, String)])

  private def runConfigured(spark: SparkSession, c: JobConfig, sink: String => Unit, faults: Int => Unit): Int = {
    val executionId = s"${Instant.now().toString.replace(":", "").replace(".", "")}-${UUID.randomUUID().toString.take(8)}"
    val log     = new JobLog(c.runId, sink)
    val started = System.nanoTime()
    spark.conf.set("spark.sql.shuffle.partitions", c.shufflePartitions.toString)
    log.info("job_started", "execution_id" -> executionId, "run_date" -> c.runDate.toString, "input" -> c.input, "output" -> c.output,
      "spark" -> spark.version, "scala" -> scala.util.Properties.versionNumberString, "java" -> System.getProperty("java.version"))
    val metrics = JobMetrics.start(spark.sparkContext, c.runId)

    var attempts = 0
    /** MỘT lần thử = đọc + gate + tính + ghi + verify — đúng phần cần làm lại khi storage chập chờn (gate cũng đọc storage). Không có log
      * "thành công" hay side effect nào bên trong: lỗi ở sink log/metric SAU khi dữ liệu đã ghi không được kích hoạt chạy lại pipeline.
      */
    def attempt(n: Int): Outcome = {
      attempts = n
      try {
        faults(n)
        gate(spark, c) match {
          case Left(reason) =>
            log.error("quality_gate_blocked", "reason" -> reason)
            Outcome(ExitCode.DataRejected, "DATA_REJECTED", Map("reason" -> reason), None)
          case Right(rate) =>
            log.info("quality_gate_passed", "rows" -> rate.rows, "rejected" -> rate.rejected)
            val r = Pipeline.run(spark, c.input, c.customers, c.output, c.runDate)
            val counts = Map[String, Any]("gate_rows" -> rate.rows, "gate_rejected" -> rate.rejected, "input_rows" -> r.inputRows,
              "rejected_rows" -> r.rejectedRows, "fact_lines" -> r.factLines, "conflict_rows" -> r.conflictRows,
              "paid_revenue_vnd" -> r.paidRevenueVnd, "removed_partitions" -> r.removedPartitions, "failed_checks" -> r.failedChecks)
            if (r.ok) Outcome(ExitCode.Succeeded, "SUCCEEDED", counts, Some("INFO" -> "pipeline_succeeded"))
            else Outcome(ExitCode.VerificationFailed, "VERIFICATION_FAILED", counts, Some("ERROR" -> "verification_failed"))
        }
      } catch {
        case e: Throwable if isTransient(e) && n < c.maxAttempts =>
          log.warn("attempt_failed_transient", "attempt" -> n, "error" -> e.toString)
          Thread.sleep(200L * n) // backoff tuyến tính, đủ cho bài học; production dùng lũy thừa + jitter (bài 2)
          attempt(n + 1)
      }
    }

    val outcome =
      try attempt(1)
      catch {
        case e: Throwable =>
          Failures.classify(e) match {
            case Failures.Transient =>
              Outcome(ExitCode.RetriesExhausted, "RETRIES_EXHAUSTED", Map("error" -> e.toString), Some("ERROR" -> "retries_exhausted"))
            case Failures.Data =>
              Outcome(ExitCode.DataRejected, "DATA_REJECTED", Map("error" -> e.toString), Some("ERROR" -> "data_contract_violation"))
            case _ => Outcome(ExitCode.Failed, "FAILED", Map("error" -> e.toString), Some("ERROR" -> "job_failed"))
          }
      }

    // FINALIZATION — mọi việc SAU khi dữ liệu đã có kết quả. Thứ tự có chủ đích (review T16 vòng 2, R2-M3/R2-M4):
    //   1. đóng metric TRƯỚC TIÊN: gỡ listener + job group dù các bước sau hỏng (sink log hỏng không được để listener "rò" sang job khác);
    //   2. log kết quả, ghi file Prometheus — mỗi bước bắt lỗi riêng, gom vào `problems`;
    //   3. ghi manifest CUỐI CÙNG, kèm mã thoát cuối và danh sách lỗi finalization — manifest là bản ghi "chốt": không bao giờ có manifest
    //      nói SUCCEEDED/exit 0 trong khi job trả 1.
    // Lỗi ở bất kỳ bước nào → mã 1 (dữ liệu có thể đã ghi đúng; chỉ bằng chứng chưa đủ) — không để exception thoát khỏi bảng mã thoát.
    val (m, metricsComplete) = scala.util.Try(metrics.close()).toEither.left.map(_.toString).flatten match {
      case Right(values) => (values, true)
      case Left(err)     => scala.util.Try(log.error("metrics_incomplete", "error" -> err)); (metrics.asMap, false)
    }
    val durationMs = (System.nanoTime() - started) / 1000000
    def step(name: String)(f: => Unit): Option[String] = scala.util.Try(f).failed.toOption.map(e => s"$name: $e")
    val problems = List(
      // Log kết quả SAU vòng retry: sink log hỏng ở đây là lỗi finalization, không phải lý do tính lại dữ liệu.
      step("log") {
        outcome.finalEvent.foreach {
          case ("INFO", event) => log.info(event, outcome.details.toSeq: _*)
          case (_, event)      => log.error(event, ("attempts" -> attempts) +: outcome.details.toSeq: _*)
        }
      },
      // Prometheus: MỘT file ổn định cho trạng thái HIỆN TẠI của job — nhiều file cùng series sẽ trùng (review T16 M5).
      step("metrics_file") {
        writeAtomically(Paths.get(c.output, "_metrics", "ch26_orders.prom"),
          JobMetrics.prometheus(m ++ Map("job_exit_code" -> outcome.code.toLong, "job_duration_ms" -> durationMs, "job_attempts" -> attempts.toLong,
            "job_metrics_complete" -> (if (metricsComplete) 1L else 0L), "job_last_run_date_epoch_day" -> c.runDate.toEpochDay),
            Map("job" -> "ch26_orders")), replace = true)
      }
    ).flatten
    val exitCode = if (problems.isEmpty) outcome.code else ExitCode.Failed
    // Manifest: một file cho MỖI lần thực thi; `status` là kết quả DỮ LIỆU, `exit_code` là mã job thực sự trả, `finalization_errors` là
    // những gì chưa làm được. "Ngày đã chạy thành công" = status SUCCEEDED VÀ exit_code 0 (lời giải bài 3).
    val manifestProblem = step("manifest") {
      writeAtomically(Paths.get(c.output, "_runs").resolve(s"${c.runId}--$executionId.json"), Json.obj(
        "run_id" -> c.runId, "execution_id" -> executionId, "status" -> outcome.status, "exit_code" -> exitCode, "attempts" -> attempts,
        "duration_ms" -> durationMs,
        "config" -> Map[String, Any]("input" -> c.input, "customers" -> c.customers, "output" -> c.output, "run_date" -> c.runDate.toString,
          "max_reject_rate" -> c.maxRejectRate, "shuffle_partitions" -> c.shufflePartitions, "max_attempts" -> c.maxAttempts),
        "versions" -> Map("spark" -> spark.version, "scala" -> scala.util.Properties.versionNumberString, "java" -> System.getProperty("java.version")),
        "details" -> outcome.details, "metrics_complete" -> metricsComplete, "finalization_errors" -> problems, "spark_physical_metrics" -> m),
        replace = false)
    }
    val allProblems = problems ++ manifestProblem.toList
    if (allProblems.nonEmpty) {
      scala.util.Try(log.error("finalization_failed", "status" -> outcome.status, "errors" -> allProblems))
      ExitCode.Failed
    } else {
      scala.util.Try(log.info("job_finished", "status" -> outcome.status, "exit_code" -> outcome.code, "duration_ms" -> durationMs,
        "attempts" -> attempts, "execution_id" -> executionId))
      outcome.code
    }
  }

  /** Không đặt `master` khi chạy dưới spark-submit (nó truyền `spark.master` qua system property); chỉ mặc định local khi chạy trực tiếp. */
  def session(): SparkSession = {
    val b = SparkSession.builder().appName("ch26-orders-job").config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.artifact.isolation.enabled", "false") // một session/job; tránh tải class qua RPC (Chương 24 mục 10)
    if (!sys.props.contains("spark.master")) b.master("local[2]").config("spark.ui.enabled", "false")
    if (System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win"))
      b.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    b.getOrCreate()
  }

  def main(args: Array[String]): Unit =
    sys.exit(launch(args.toSeq, sys.env, println, () => session(), stopAfter = true))
}
