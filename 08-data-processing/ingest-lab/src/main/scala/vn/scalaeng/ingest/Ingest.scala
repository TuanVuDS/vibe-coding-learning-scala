package vn.scalaeng.ingest

import cats.effect.{IO, Ref}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import io.circe.Json
import io.circe.syntax.*
import java.time.{Instant, LocalDate}

final case class Summary(lines: Long, accepted: Long, rejected: Long, duplicates: Long, orphanClicks: Long, reportRows: Int)

/** Quyết định cho MỘT dòng — hàm thuần, dùng chung cho chương trình đọc file và cho test (không cần file). */
object Pipeline:
  def decide(campaigns: Map[String, Campaign], now: Instant)(s: State, raw: String, line: Long): (State, List[Rejected]) =
    Normalize.event(line, raw, now).flatMap { e =>
      if campaigns.contains(e.campaignId) then Right(e)
      else Left(Rejected(line, List(s"campaign không tồn tại: ${e.campaignId}"), raw))
    } match
      case Left(rej) => (s, List(rej))
      case Right(e)  => Aggregate.step(s, e, line, raw)

  /** Chạy toàn bộ trong bộ nhớ: (trạng thái cuối, mọi dòng bị loại kể cả click mồ côi). */
  def runAll(lines: List[String], campaigns: Map[String, Campaign], now: Instant): (State, List[Rejected]) =
    val (finalState, rejected) = lines.zipWithIndex.filter(_._1.trim.nonEmpty).foldLeft((State.empty, Vector.empty[Rejected])) {
      case ((s, acc), (raw, idx)) =>
        val (s2, rejs) = decide(campaigns, now)(s, raw, idx + 1L)
        (s2, acc ++ rejs)
    }
    (finalState, (rejected ++ Aggregate.orphans(finalState)).toList)

/** Lớp "vỏ" (I/O): đọc file theo luồng, gọi các hàm thuần, ghi kết quả. Toàn bộ quyết định nằm ở Normalize/Aggregate. */
object Ingest:
  // CSV ↔ case class: tên cột lấy từ header. Ghi `day` dạng yyyy-MM-dd, CTR dạng số thập phân.
  private given CellDecoder[String] = CellDecoder.stringDecoder
  given CsvRowDecoder[Campaign, String] = deriveCsvRowDecoder[Campaign]
  given CellEncoder[LocalDate]       = CellEncoder.stringEncoder.contramap(_.toString)
  given CellEncoder[BigDecimal]      = CellEncoder.stringEncoder.contramap(_.bigDecimal.toPlainString)
  given CsvRowEncoder[ReportRow, String] = deriveCsvRowEncoder[ReportRow]

  def readCampaigns(path: Path): IO[Map[String, Campaign]] =
    Files[IO].readUtf8(path)
      .through(decodeUsingHeaders[Campaign]())
      .map(c => c.copy(campaignId = c.campaignId.trim.toUpperCase(java.util.Locale.ROOT)))
      .compile.toList
      .map(_.map(c => c.campaignId -> c).toMap)

  private def rejectedJson(r: Rejected): String =
    Json.obj("line" -> r.line.asJson, "reasons" -> r.reasons.asJson, "raw" -> r.raw.asJson).noSpaces

  /** Đọc events theo DÒNG — bộ nhớ không tăng theo kích thước file. Ghi rejected.jsonl song song khi đọc. */
  def run(events: Path, campaignsCsv: Path, outDir: Path, now: Instant): IO[Summary] =
    for
      campaigns <- readCampaigns(campaignsCsv)
      _         <- Files[IO].createDirectories(outDir)
      state     <- Ref.of[IO, (State, Long, Long)]((State.empty, 0L, 0L)) // (trạng thái, số dòng, số dòng bị loại)
      _ <- Files[IO].readUtf8Lines(events)
             .zipWithIndex
             .filter((text, _) => text.trim.nonEmpty)
             .evalMap { (raw, idx) =>
               state.modify { case (s, n, bad) =>
                 val (s2, rejs) = Pipeline.decide(campaigns, now)(s, raw, idx + 1)
                 ((s2, n + 1, bad + rejs.size), rejs)
               }
             }
             .flatMap(Stream.emits)
             .map(r => rejectedJson(r) + "\n")
             .through(text.utf8.encode)
             .through(Files[IO].writeAll(outDir / "rejected.jsonl"))
             .compile.drain
      (finalState, lines, rejected) <- state.get
      orphans = Aggregate.orphans(finalState)
      _      <- Stream.emits(orphans).covary[IO].map(r => rejectedJson(r) + "\n").through(text.utf8.encode)
                  .through(Files[IO].writeAll(outDir / "rejected.jsonl", fs2.io.file.Flags.Append)).compile.drain
      rows    = Aggregate.report(finalState, campaigns)
      _      <- Stream.emits(rows).covary[IO].through(encodeUsingFirstHeaders[ReportRow]()).through(text.utf8.encode)
                  .through(Files[IO].writeAll(outDir / "report.csv")).compile.drain
      accepted = lines - rejected - finalState.duplicates - orphans.size
    yield Summary(lines, accepted, rejected + orphans.size, finalState.duplicates, orphans.size.toLong, rows.size)
