package vn.scalaeng.formats

import io.circe.parser.{decode, parse}
import io.circe.syntax.*
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import scala.jdk.CollectionConverters.*
import scala.util.Using

object TextOrders:
  private val headers = Array("id", "userId", "amountMinor", "currency", "occurredAt", "notePresent", "note")
  private val csv = CSVFormat.RFC4180.builder().setHeader(headers*).get()
  private val csvRead = CSVFormat.RFC4180.builder().setHeader()
    .setSkipHeaderRecord(true).get()

  private def withWriter[A](path: Path, gzip: Boolean)(f: BufferedWriter => A): A =
    Using.Manager { use =>
      val raw = use(Files.newOutputStream(path))
      val bytes = if gzip then use(GZIPOutputStream(raw)) else raw
      val text = use(BufferedWriter(OutputStreamWriter(bytes, UTF_8)))
      f(text)
    }.get

  private def withReader[A](path: Path, gzip: Boolean)(f: BufferedReader => A): A =
    Using.Manager { use =>
      val raw = use(Files.newInputStream(path))
      val bytes = if gzip then use(GZIPInputStream(raw)) else raw
      val text = use(BufferedReader(InputStreamReader(bytes, UTF_8)))
      f(text)
    }.get

  def writeJson(path: Path, rows: Iterator[Order], gzip: Boolean): Unit =
    withWriter(path, gzip) { out =>
      rows.foreach { row =>
        out.write(Order.checked(row).asJson.noSpaces)
        out.newLine()
      }
    }

  def scanJson(path: Path, gzip: Boolean, consume: Order => Unit): Unit =
    withReader(path, gzip) { in =>
      in.lines().iterator().asScala.foreach { line =>
        consume(Order.checked(decode[Order](line).fold(throw _, identity)))
      }
    }

  def sumJson(path: Path, gzip: Boolean): Long =
    withReader(path, gzip) { in =>
      in.lines().iterator().asScala.map { line =>
        parse(line).flatMap(_.hcursor.get[Long]("amountMinor")).fold(throw _, identity)
      }.sum
    }

  def writeCsv(path: Path, rows: Iterator[Order], gzip: Boolean): Unit =
    withWriter(path, gzip) { text =>
      Using.resource(CSVPrinter(text, csv)) { out =>
        rows.foreach { raw =>
          val o = Order.checked(raw)
          out.printRecord(List(o.id.toString, o.userId, o.amountMinor.toString, o.currency,
            o.occurredAt.toString, o.note.isDefined.toString, o.note.getOrElse("")).asJava)
        }
      }
    }

  private def csvRecords[A](path: Path, gzip: Boolean)(f: Iterator[org.apache.commons.csv.CSVRecord] => A): A =
    withReader(path, gzip) { in =>
      Using.resource(csvRead.parse(in)) { parser =>
        require(parser.getHeaderNames.asScala.toList == headers.toList, "unexpected CSV header")
        // Empty setHeader() reads the physical header before this comparison.
        f(parser.iterator().asScala)
      }
    }

  def scanCsv(path: Path, gzip: Boolean, consume: Order => Unit): Unit =
    csvRecords(path, gzip) { records =>
      records.foreach { r =>
        require(r.size() == headers.length, s"expected ${headers.length} CSV fields, got ${r.size()}")
        val present = r.get("notePresent") match
          case "true" => true
          case "false" => false
          case other => throw IllegalArgumentException(s"invalid notePresent: $other")
        require(present || r.get("note").isEmpty, "absent note must have empty payload")
        consume(Order.checked(Order(r.get("id").toLong, r.get("userId"), r.get("amountMinor").toLong,
          r.get("currency"), Instant.parse(r.get("occurredAt")), Option.when(present)(r.get("note")))))
      }
    }

  def sumCsv(path: Path, gzip: Boolean): Long =
    csvRecords(path, gzip)(_.map(_.get("amountMinor").toLong).sum)
