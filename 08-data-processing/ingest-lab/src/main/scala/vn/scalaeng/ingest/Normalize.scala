package vn.scalaeng.ingest

import io.circe.parser
import java.time.{Instant, ZoneId}
import java.util.Locale
import scala.util.Try

/** Các hàm THUẦN, nhỏ, mỗi hàm một việc: parse → chuẩn hóa từng trường → kiểm tra liên trường.
  * Không đọc file, không đọc đồng hồ hệ thống (nhận `now` làm tham số), không phụ thuộc LOCALE của máy → test được bằng dữ liệu cố định.
  */
object Normalize:
  val Vietnam: ZoneId = ZoneId.of("Asia/Ho_Chi_Minh")

  private val EarliestValid = Instant.parse("2020-01-01T00:00:00Z")
  private val MaxClockSkew  = java.time.Duration.ofMinutes(5)
  private val CampaignId    = "CMP-[0-9]{1,6}".r
  private val UserId        = "U-[0-9]{1,10}".r

  /** Mã ISO 3166-1 alpha-2 do JDK cung cấp. "Hai chữ cái" chưa đủ: `ZZ` hay `ÅÅ` không phải mã quốc gia (review chéo, M3). */
  private val IsoCountries: Set[String] = Locale.getISOCountries.toSet

  /** Đổi hoa/thường theo quy tắc CỐ ĐỊNH. `toUpperCase()` không tham số dùng locale của máy: với locale Thổ Nhĩ Kỳ,
    * "in" thành "İN" và "IMPRESSION" hạ thành "ımpressıon" — cùng dữ liệu, khác máy, khác kết quả (review chéo, M6).
    */
  private def upper(s: String): String = s.trim.toUpperCase(Locale.ROOT)
  private def lower(s: String): String = s.trim.toLowerCase(Locale.ROOT)

  /** Bước 1: chuỗi → JSON → RawEvent. Lỗi ở đây chỉ còn là: không phải JSON, hoặc JSON không phải object. */
  def parse(line: Long, text: String): Either[Rejected, RawEvent] =
    parser.decode[RawEvent](text).left.map {
      case e: io.circe.ParsingFailure => Rejected(line, List(s"JSON không hợp lệ: ${e.message.take(80)}"), text)
      case e                          => Rejected(line, List(e.getMessage.take(80)), text)
    }

  /** `ts` có thể là "2026-10-15T08:00:01Z", "2026-10-15T15:00:01+07:00" hoặc số epoch millis 1792051201000. */
  def timestamp(raw: Option[io.circe.Json]): Either[String, Instant] =
    raw match
      case None => Left("thiếu ts")
      case Some(j) =>
        j.asNumber.flatMap(_.toLong).map(Instant.ofEpochMilli)
          .orElse(j.asString.flatMap(s => Try(java.time.OffsetDateTime.parse(s.trim).toInstant).toOption))
          .toRight(s"ts không đọc được: ${j.noSpaces}")

  /** Mã chiến dịch: trim + chữ hoa; chấp nhận "cmp-7" và " CMP-7 " là cùng một chiến dịch. */
  def campaignId(raw: Option[String]): Either[String, String] =
    raw.map(upper).filter(_.nonEmpty) match
      case None                    => Left("thiếu campaign_id")
      case Some(id @ CampaignId()) => Right(id)
      case Some(other)             => Left(s"campaign_id sai định dạng: $other")

  def userId(raw: Option[String]): Either[String, String] =
    raw.map(upper).filter(_.nonEmpty) match
      case None                => Left("thiếu user.id")
      case Some(id @ UserId()) => Right(id)
      case Some(other)         => Left(s"user.id sai định dạng: $other")

  /** Quốc gia: vài cách viết hay gặp được ánh xạ về mã chuẩn, rồi phải là một mã ISO 3166-1 alpha-2 THẬT. */
  def country(raw: Option[String]): Either[String, String] =
    raw.map(upper).filter(_.nonEmpty) match
      case None                                 => Left("thiếu geo.country")
      case Some("VIETNAM" | "VIET NAM" | "VNM") => Right("VN")
      case Some(c) if IsoCountries.contains(c)  => Right(c)
      case Some(other)                          => Left(s"geo.country không hợp lệ: $other")

  /** Kiểm tra ĐỦ mọi trường rồi mới kết luận — trả về tất cả lý do, không dừng ở lỗi đầu tiên. */
  def validate(line: Long, text: String, r: RawEvent, now: Instant): Either[Rejected, AdEvent] =
    val typeError = r.typeErrors.toMap
    /** Trường sai kiểu báo lỗi KIỂU (thay cho "thiếu ..."), để người sửa SDK biết đúng vấn đề. */
    def checked[A](path: String)(rule: => Either[String, A]): Either[String, A] = typeError.get(path).toLeft(()).flatMap(_ => rule)

    val id   = checked("event_id")(r.eventId.map(_.trim).filter(_.nonEmpty).toRight("thiếu event_id"))
    val kind = checked("type")(r.eventType.map(lower) match
      case Some("impression") => Right(EventType.Impression)
      case Some("click")      => Right(EventType.Click)
      case Some(other)        => Left(s"type không hỗ trợ: $other")
      case None               => Left("thiếu type"))
    val ts = timestamp(r.ts).flatMap { t =>
      if t.isBefore(EarliestValid) then Left(s"ts quá cũ: $t")
      else if t.isAfter(now.plus(MaxClockSkew)) then Left(s"ts ở tương lai: $t")
      else Right(t)
    }
    val user = checked("user")(checked("user.id")(userId(r.userId)))
    val cmp  = checked("campaign_id")(campaignId(r.campaignId))
    val geo  = checked("geo")(checked("geo.country")(country(r.country)))
    val fields = (id, kind, ts, user, cmp, geo)
    val errors = fields.toList.collect { case Left(e: String) => e }
    // Trường TÙY CHỌN sai kiểu vẫn là vi phạm hợp đồng dữ liệu: báo lỗi, không âm thầm coi như thiếu.
    val optionalTypeErrors = List("impression_id", "device", "device.os", "geo.city").flatMap(typeError.get)
    // Ràng buộc liên trường: click PHẢI trỏ tới một impression.
    val linkError = kind match
      case Right(EventType.Click) if r.impressionId.forall(_.trim.isEmpty) && !typeError.contains("impression_id") =>
        List("click thiếu impression_id")
      case _ => Nil
    (fields, errors ++ optionalTypeErrors ++ linkError) match
      case ((Right(i), Right(k), Right(t), Right(u), Right(c), Right(co)), Nil) =>
        Right(AdEvent(
          eventId = i,
          eventType = k,
          ts = t,
          day = t.atZone(Vietnam).toLocalDate,
          userId = u,
          campaignId = c,
          impressionId = if k == EventType.Click then r.impressionId.map(_.trim) else None,
          os = r.os.map(_.trim).filter(_.nonEmpty).getOrElse("unknown"),
          country = co,
          city = r.city.map(_.trim).filter(_.nonEmpty)
        ))
      case (_, allErrors) => Left(Rejected(line, allErrors, text))

  def event(line: Long, text: String, now: Instant): Either[Rejected, AdEvent] =
    parse(line, text).flatMap(validate(line, text, _, now))
