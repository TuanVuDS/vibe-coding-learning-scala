package vn.scalaeng.order.app

import vn.scalaeng.order.core.{OrderPipeline, Reports}
import java.nio.charset.StandardCharsets.UTF_8
import java.io.PrintStream
import java.nio.file.{Files, NoSuchFileException}
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Exit code là "hợp đồng" với scheduler/orchestrator (cron, Dagster, Kubernetes Job). */
object ExitCode:
  val Ok = 0
  val ConfigError = 1
  val DataQualityFailed = 2
  val IoError = 3

object Main:
  /** Toàn bộ logic shell nằm trong `run` trả về exit code -> test được mà không gọi sys.exit.
    * `err` và `now` là điểm "nối" (seam) để test thay bằng luồng ghi vào bộ nhớ và đồng hồ cố định.
    */
  def run(env: Map[String, String], err: PrintStream = System.err, now: () => Instant = () => Instant.now()): Int =
    AppConfig.load(env) match
      case Left(errors) =>
        val bootstrap = JsonLogger(LogLevel.Info, err, now)
        errors.foreach(e => bootstrap.error("config error", "detail" -> e))
        ExitCode.ConfigError
      case Right(cfg) =>
        val log = JsonLogger(cfg.logLevel, err, now)
        log.info("starting", "env" -> cfg.env.toString, "input" -> cfg.inputPath.toString)
        val result = Try(Files.readAllLines(cfg.inputPath, UTF_8).asScala.toList)
        result match
          case Failure(_: NoSuchFileException) =>
            log.error("input file not found", "path" -> cfg.inputPath.toString)
            ExitCode.IoError
          case Failure(e) =>
            log.error("cannot read input", "error" -> e.toString)
            ExitCode.IoError
          case Success(lines) =>
            OrderPipeline.run(lines) match
              case Left(err) =>
                log.error("invalid input file", "detail" -> err)
                ExitCode.DataQualityFailed
              case Right(r) =>
                Files.createDirectories(cfg.outputDir)
                Files.writeString(cfg.outputDir.resolve("daily_revenue.csv"), Reports.dailyCsv(r.daily), UTF_8)
                Files.writeString(cfg.outputDir.resolve("rejected.csv"), Reports.rejectedCsv(r.rejected ++ r.duplicates), UTF_8)
                log.info("finished",
                  "input_rows" -> r.inputRows.toString, "valid" -> r.valid.size.toString,
                  "rejected" -> r.rejected.size.toString, "duplicates" -> r.duplicates.size.toString,
                  "balanced" -> r.isBalanced.toString)
                if r.rejectRatio > cfg.maxRejectRatio then
                  log.warn("reject ratio above threshold",
                    "ratio" -> f"${r.rejectRatio}%.3f", "max" -> cfg.maxRejectRatio.toString)
                  ExitCode.DataQualityFailed
                else ExitCode.Ok

  def main(args: Array[String]): Unit =
    val code = run(sys.env)
    if code != ExitCode.Ok then sys.exit(code)
