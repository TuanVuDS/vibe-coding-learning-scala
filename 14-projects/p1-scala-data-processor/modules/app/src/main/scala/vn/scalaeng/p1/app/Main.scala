package vn.scalaeng.p1.app

import vn.scalaeng.p1.json.Json
import vn.scalaeng.p1.json.Json.*
import vn.scalaeng.p1.pipeline.Processor
import vn.scalaeng.p1.report.Reports
import java.io.PrintStream
import java.nio.charset.{MalformedInputException, StandardCharsets}
import java.nio.file.{AtomicMoveNotSupportedException, Files, NoSuchFileException, Path, Paths, StandardCopyOption}
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.Try

object ExitCode:
  val Ok = 0
  val Usage = 1
  val DataQuality = 2
  val Io = 3

final case class Args(orders: Path, users: Path, out: Path, maxRejectRatio: Double)

object Args:
  val Usage = "cách dùng: --orders <file.csv> --users <file.json> --out <thư mục> [--max-reject-ratio 0.1]"

  /** Parse tham số dòng lệnh THUẦN, tích lũy lỗi. */
  def parse(args: List[String]): Either[List[String], Args] =
    val pairs = args.grouped(2).toList
    val malformed = pairs.collect { case List(k, _) if !k.startsWith("--") => s"tham số không hợp lệ: $k"; case List(k) => s"thiếu giá trị cho $k" }
    val m = pairs.collect { case List(k, v) if k.startsWith("--") => k -> v }.toMap
    val unknown = (m.keySet -- Set("--orders", "--users", "--out", "--max-reject-ratio")).toList.sorted.map(k => s"tham số không biết: $k")
    def path(k: String) = m.get(k).filter(_.trim.nonEmpty).map(Paths.get(_)).toRight(s"thiếu $k")
    val ratio = m.get("--max-reject-ratio") match
      case None    => Right(0.1)
      case Some(v) => v.toDoubleOption.filter(r => r >= 0 && r <= 1).toRight(s"--max-reject-ratio phải trong [0, 1]: '$v'")
    (path("--orders"), path("--users"), path("--out"), ratio) match
      case (Right(o), Right(u), Right(d), Right(r)) if malformed.isEmpty && unknown.isEmpty => Right(Args(o, u, d, r))
      case t => Left(malformed ++ unknown ++ t.productIterator.collect { case Left(e: String) => e }.toList)

final class Log(out: PrintStream, now: () => Instant):
  private def emit(level: String, msg: String, fields: Seq[(String, Json)]): Unit =
    out.println(JObj(Vector("ts" -> JStr(now().toString), "level" -> JStr(level), "msg" -> JStr(msg)) ++ fields).render)
  def info(msg: String, fields: (String, Json)*): Unit  = emit("INFO", msg, fields)
  def warn(msg: String, fields: (String, Json)*): Unit  = emit("WARN", msg, fields)
  def error(msg: String, fields: (String, Json)*): Unit = emit("ERROR", msg, fields)

object Main:
  /** Ghi file nguyên tử: ghi file tạm cùng thư mục rồi đổi tên — không bao giờ để lại file ghi dở. */
  def writeAtomically(target: Path, content: String): Unit =
    val tmp = target.resolveSibling(target.getFileName.toString + ".tmp")
    Files.writeString(tmp, content, StandardCharsets.UTF_8)
    try Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    catch case _: AtomicMoveNotSupportedException => Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
    ()

  def run(args: List[String], err: PrintStream = System.err, now: () => Instant = () => Instant.now()): Int =
    val log = Log(err, now)
    Args.parse(args) match
      case Left(errors) =>
        errors.foreach(e => log.error("invalid arguments", "detail" -> JStr(e)))
        log.error(Args.Usage)
        ExitCode.Usage
      case Right(a) =>
        val input = Try((
          Files.readAllLines(a.orders, StandardCharsets.UTF_8).asScala.toList,
          Files.readString(a.users, StandardCharsets.UTF_8)
        )).toEither
        input match
          case Left(e: NoSuchFileException) =>
            log.error("input file not found", "path" -> JStr(e.getFile)); ExitCode.Io
          case Left(_: MalformedInputException) =>
            log.error("input is not valid UTF-8"); ExitCode.DataQuality
          case Left(e) =>
            log.error("cannot read input", "error" -> JStr(e.toString)); ExitCode.Io
          case Right((orderLines, usersText)) =>
            Processor.run(orderLines, usersText) match
              case Left(fatal) =>
                log.error("invalid input file", "detail" -> JStr(fatal)); ExitCode.DataQuality
              case Right(r) =>
                val written = Try {
                  Files.createDirectories(a.out)
                  writeAtomically(a.out.resolve("report.csv"), Reports.reportCsv(r.report))
                  writeAtomically(a.out.resolve("rejected.csv"), Reports.rejectedCsv(r.rejected))
                  writeAtomically(a.out.resolve("summary.json"), Reports.summaryJson(r.stats))
                }.toEither
                written match
                  case Left(e) =>
                    log.error("cannot write output", "error" -> JStr(e.toString)); ExitCode.Io
                  case Right(_) =>
                    val s = r.stats
                    def n(i: Int) = JNum(BigDecimal(i))
                    log.info("finished",
                      "order_rows" -> n(s.orderRows), "orders_valid" -> n(s.ordersValid), "orders_rejected" -> n(s.ordersRejected),
                      "orders_superseded" -> n(s.ordersSuperseded), "users_valid" -> n(s.usersValid),
                      "users_rejected" -> n(s.usersRejected), "orphan_orders" -> n(s.orphanOrders), "balanced" -> JBool(s.balanced))
                    if s.orphanOrders > 0 then log.warn("orders reference unknown users", "count" -> n(s.orphanOrders))
                    if s.orderRejectRatio > a.maxRejectRatio then
                      log.warn("order reject ratio above threshold",
                        "ratio" -> JNum(BigDecimal(s.orderRejectRatio).setScale(3, BigDecimal.RoundingMode.HALF_UP)),
                        "max" -> JNum(BigDecimal(a.maxRejectRatio)))
                      ExitCode.DataQuality
                    else ExitCode.Ok

  def main(args: Array[String]): Unit =
    val code = run(args.toList)
    if code != ExitCode.Ok then sys.exit(code)
