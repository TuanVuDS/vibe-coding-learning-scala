package vn.scalaeng.order.app

import vn.scalaeng.order.core.Logger
import java.io.PrintStream
import java.time.Instant

/** Structured logging: mỗi dòng là một JSON object -> hệ thống thu thập log (Loki, ELK, CloudWatch) lọc được theo field.
  * Ghi ra stderr để stdout dành cho dữ liệu/kết quả.
  * Production thường dùng SLF4J + Logback (xem Chương 9); nhờ trait Logger, đổi implementation không ảnh hưởng core.
  */
final class JsonLogger(minLevel: LogLevel, out: PrintStream, now: () => Instant) extends Logger:
  private def esc(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c    => c.toString
    }

  private def log(level: LogLevel, message: String, fields: Seq[(String, String)]): Unit =
    if level.ordinal >= minLevel.ordinal then
      val extra = fields.map((k, v) => s""","${esc(k)}":"${esc(v)}"""").mkString
      // JSON Lines dùng '\n' trên mọi OS; println sẽ ghi "\r\n" trên Windows.
      out.print(s"""{"ts":"${now()}","level":"${level.toString.toUpperCase}","msg":"${esc(message)}"$extra}""" + "\n")
      out.flush()

  def info(message: String, fields: (String, String)*): Unit  = log(LogLevel.Info, message, fields)
  def warn(message: String, fields: (String, String)*): Unit  = log(LogLevel.Warn, message, fields)
  def error(message: String, fields: (String, String)*): Unit = log(LogLevel.Error, message, fields)
