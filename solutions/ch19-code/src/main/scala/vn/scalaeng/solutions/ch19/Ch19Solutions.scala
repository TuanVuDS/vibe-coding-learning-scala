package vn.scalaeng.solutions.ch19

import cats.effect.IO
import fs2.data.csv.*
import fs2.data.csv.generic.CsvName
import fs2.data.csv.generic.semiauto.*
import fs2.{Pipe, Stream}
import io.circe.Json
import java.security.MessageDigest
import java.time.{Duration, Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.HexFormat
import scala.util.Try
import vn.scalaeng.ingest.*

/** Bài 1 — SDK cũ gửi "yyyy-MM-dd HH:mm:ss" KHÔNG có múi giờ, nhưng tài liệu SDK nói đó là giờ Việt Nam.
  * Thêm định dạng này như một PHƯƠNG ÁN SAU CÙNG, sau các định dạng có múi giờ rõ ràng.
  */
object LegacyTimestamp:
  /** STRICT + `uuuu`: "2026-02-30" là LỖI. Mặc định (SMART) âm thầm sửa thành 28/02 — dữ liệu sai đi vào như dữ liệu sạch
    * (review chéo, M7). `uuuu` (năm, không era) vì STRICT với `yyyy` (năm của era) đòi thêm trường era.
    */
  private val Legacy = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", java.util.Locale.ROOT).withResolverStyle(java.time.format.ResolverStyle.STRICT)

  def parse(raw: Option[Json]): Either[String, Instant] =
    Normalize.timestamp(raw).orElse {
      raw.flatMap(_.asString).flatMap(s => Try(LocalDateTime.parse(s.trim, Legacy).atZone(Normalize.Vietnam).toInstant).toOption)
        .toRight(s"ts không đọc được: ${raw.map(_.noSpaces).getOrElse("")}")
    }

/** Bài 2 — số impression theo (quốc gia, ngày). Viết trên List[AdEvent] đã sạch: một groupMapReduce. */
object ByCountry:
  def impressions(events: List[AdEvent]): List[(String, LocalDate, Long)] =
    events.filter(_.eventType == EventType.Impression)
      .groupMapReduce(e => (e.country, e.day))(_ => 1L)(_ + _)
      .toList.map { case ((c, d), n) => (c, d, n) }
      .sortBy((c, d, _) => (c, d.toEpochDay))

/** Bài 3 — dedup khi event_id KHÔNG đáng tin (SDK sinh id mới mỗi lần gửi lại).
  * Khóa nội dung = băm của các trường xác định "cùng một sự kiện"; thời gian cắt về GIÂY vì lần gửi lại có thể lệch mili giây.
  * Đánh đổi: hai sự kiện thật sự khác nhau nhưng trùng mọi trường trong cùng một giây sẽ bị gộp nhầm.
  */
object ContentKey:
  def of(e: AdEvent): String =
    val material = List(e.eventType.toString, e.userId, e.campaignId, e.impressionId.getOrElse(""), e.ts.getEpochSecond.toString).mkString("|")
    HexFormat.of.formatHex(MessageDigest.getInstance("SHA-256").digest(material.getBytes("UTF-8")))

  def dedup(events: List[AdEvent]): List[AdEvent] =
    events.foldLeft((Set.empty[String], Vector.empty[AdEvent])) { case ((seen, acc), e) =>
      val k = of(e)
      if seen.contains(k) then (seen, acc) else (seen + k, acc :+ e)
    }._2.toList

/** Bài 4 — dedup với BỘ NHỚ GIỚI HẠN: chỉ nhớ id trong cửa sổ `window` tính theo thời gian sự kiện.
  * Giả định: sự kiện đến GẦN theo thứ tự thời gian. Bản trùng cách nhau hơn `window` sẽ lọt qua — đánh đổi
  * độ chính xác lấy bộ nhớ ổn định. Chương 27–30 (streaming) làm việc này với watermark.
  */
final case class WindowedDedup(window: Duration, seen: Map[String, Instant], newest: Instant):
  /** (trạng thái mới, true nếu là bản trùng). Dọn id cũ hơn (newest - window) ở mỗi bước. */
  def check(e: AdEvent): (WindowedDedup, Boolean) =
    val newest2 = if e.ts.isAfter(newest) then e.ts else newest
    val cutoff  = newest2.minus(window)
    val kept    = seen.filter((_, t) => !t.isBefore(cutoff))
    if kept.contains(e.eventId) then (copy(seen = kept, newest = newest2), true)
    else (copy(seen = kept.updated(e.eventId, e.ts), newest = newest2), false)

object WindowedDedup:
  def empty(window: Duration): WindowedDedup = WindowedDedup(window, Map.empty, Instant.EPOCH)

  def run(window: Duration)(events: List[AdEvent]): (List[AdEvent], Int) =
    val (_, kept, maxSize) = events.foldLeft((empty(window), Vector.empty[AdEvent], 0)) { case ((st, acc, mx), e) =>
      val (st2, dup) = st.check(e)
      (st2, if dup then acc else acc :+ e, mx.max(st2.seen.size))
    }
    (kept.toList, maxSize) // maxSize: số id lớn nhất từng phải nhớ cùng lúc

/** Bài 5 — sessionization: các hoạt động của một user cách nhau > 30 phút thuộc hai phiên khác nhau. */
object Sessions:
  final case class Session(userId: String, start: Instant, end: Instant, events: Int):
    def duration: Duration = Duration.between(start, end)

  def of(events: List[AdEvent], gap: Duration = Duration.ofMinutes(30)): List[Session] =
    events.groupBy(_.userId).toList.sortBy(_._1).flatMap { (user, es) =>
      es.sortBy(_.ts).foldLeft(List.empty[Session]) {
        case (current :: done, e) if !e.ts.isAfter(current.end.plus(gap)) =>
          current.copy(end = e.ts, events = current.events + 1) :: done
        case (acc, e) => Session(user, e.ts, e.ts, 1) :: acc
      }.reverse
    }

/** Bài 6 — doanh thu theo ngày từ CSV đơn hàng, đọc THEO LUỒNG bằng fs2-data-csv: bộ nhớ chỉ giữ Map theo ngày. */
final case class OrderRow(
    @CsvName("order_id") orderId: String,
    @CsvName("order_date") orderDate: String,
    @CsvName("amount_vnd") amountVnd: Long,
    status: String
)

object RevenueByDay:
  given CsvRowDecoder[OrderRow, String] = deriveCsvRowDecoder[OrderRow]

  /** Dòng hỏng (thiếu/thừa cột, amount không phải số, ngày sai, số âm) được ĐẾM, không làm dừng cả file.
    *
    * `lenient.attemptDecodeUsingHeaders` trả `Either` cho TỪNG dòng, kể cả lỗi số cột. Bản đầu dùng `lowlevel.headers`:
    * dòng thiếu cột ném `HeaderSizeError` và dừng cả luồng TRƯỚC khi tới bước giải mã (review chéo, M4).
    * Lỗi cú pháp CSV mức thấp (dấu ngoặc kép không đóng) vẫn dừng luồng — không thể biết dòng kết thúc ở đâu.
    *
    * Tổng dùng `BigInt`: hai đơn hợp lệ Long.MaxValue + 1 tràn thành số âm nếu cộng bằng Long (review chéo, M5).
    */
  def apply: Pipe[IO, String, (Map[LocalDate, BigInt], Long)] = text =>
    text.through(lenient.attemptDecodeUsingHeaders[OrderRow]())
      .fold((Map.empty[LocalDate, BigInt], 0L)) {
        case ((acc, bad), Right(o)) if o.status == "PAID" =>
          Try(LocalDate.parse(o.orderDate)).toOption.filter(_ => o.amountVnd > 0) match
            case Some(d) => (acc.updated(d, acc.getOrElse(d, BigInt(0)) + o.amountVnd), bad)
            case None    => (acc, bad + 1)
        case ((acc, bad), Right(_)) => (acc, bad) // đơn chưa thanh toán: không tính doanh thu
        case ((acc, bad), Left(_))  => (acc, bad + 1)
      }

  def fromString(csv: String): IO[(Map[LocalDate, BigInt], Long)] =
    Stream.emit(csv).covary[IO].through(apply).compile.lastOrError
