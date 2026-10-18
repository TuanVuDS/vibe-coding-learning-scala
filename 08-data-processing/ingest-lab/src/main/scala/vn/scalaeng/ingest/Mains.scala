package vn.scalaeng.ingest

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Chạy pipeline: events.jsonl + campaigns.csv → out/report.csv + out/rejected.jsonl. */
object IngestMain extends IOApp:
  def run(args: List[String]): IO[ExitCode] = args match
    case events :: campaigns :: out :: Nil =>
      for
        t0      <- IO.monotonic
        summary <- Ingest.run(Path(events), Path(campaigns), Path(out), Instant.parse("2026-10-17T00:00:00Z"))
        t1      <- IO.monotonic
        _       <- IO.println(s"$summary trong ${(t1 - t0).toMillis} ms")
      yield ExitCode.Success
    case _ => IO.println("dùng: IngestMain <events.jsonl> <campaigns.csv> <thư-mục-ra>").as(ExitCode(2))

/** Sinh N sự kiện giả có chủ ý chứa dữ liệu bẩn (để thử tải và thí nghiệm bộ nhớ). Xác định: cùng N → cùng file. */
object GenerateEvents extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val (out, n) = args match
      case p :: count :: Nil => (Path(p), count.toLong)
      case _                 => (Path("big.jsonl"), 1_000_000L)
    def line(i: Long): String =
      val user = s"U-${i % 20000}"
      // Click (i % 10 == 9) trỏ tới impression i-1 → PHẢI cùng campaign với impression đó.
      // (Bản đầu tính campaign theo `i % 3` riêng cho từng dòng: click lệch campaign với impression của nó → 40% dòng bị loại.
      //  Đó là lỗi của BỘ SINH DỮ LIỆU, không phải pipeline — kiểm tra dữ liệu thử cũng cần cẩn thận như kiểm tra code.)
      val cmp  = s"cmp-${1 + (i / 10) % 2}" // nhóm 10 sự kiện liên tiếp cùng campaign
      val ts   = 1792022400000L + i * 50 // từ 2026-10-14 ~ mỗi sự kiện cách nhau 50 ms
      if i % 10 == 9 then
        s"""{"event_id":"c$i","type":"click","ts":$ts,"user":{"id":"$user"},"campaign_id":"$cmp","impression_id":"i${i - 1}","geo":{"country":"vn"}}"""
      else if i % 1000 == 500 then """{"event_id":"broken","type":"impression"""" // dòng hỏng
      else
        s"""{"event_id":"i$i","type":"impression","ts":$ts,"user":{"id":"$user"},"campaign_id":"$cmp","device":{"os":"Android"},"geo":{"country":"VN","city":"Hà Nội"}}"""
    Stream.iterable(0L until n).map(i => line(i) + "\n").through(text.utf8.encode).through(Files[IO].writeAll(out)).compile.drain
      .flatMap(_ => IO.println(s"đã ghi $n dòng vào $out")).as(ExitCode.Success)

/** CỐ Ý SAI cho thí nghiệm bộ nhớ: đọc TOÀN BỘ file vào List rồi mới xử lý. */
object NaiveIngest extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val path = java.nio.file.Path.of(args.headOption.getOrElse("big.jsonl"))
    IO.blocking {
      val lines  = java.nio.file.Files.readAllLines(path).asScala.toList   // cả file nằm trong heap
      val events = lines.zipWithIndex.map((l, i) => Normalize.event(i + 1L, l, Instant.parse("2026-10-17T00:00:00Z")))
      val ok     = events.count(_.isRight)
      println(s"đọc ${lines.size} dòng, hợp lệ $ok; heap dùng ~${(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory) / 1_048_576} MiB")
    }.as(ExitCode.Success)
