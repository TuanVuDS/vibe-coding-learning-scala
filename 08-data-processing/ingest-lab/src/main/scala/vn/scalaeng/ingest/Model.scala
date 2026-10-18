package vn.scalaeng.ingest

import fs2.data.csv.generic.CsvName
import io.circe.{Decoder, Json}
import java.time.{Instant, LocalDate}

/** Dạng THÔ — khớp với cái SDK tracking thực sự gửi lên: mọi trường đều có thể thiếu, kiểu có thể lệch.
  * Decode lỏng ở bước này (chỉ đòi JSON hợp lệ), rồi mới kiểm tra chặt ở `Normalize`. Tách hai bước để:
  *  - một trường sai không làm mất thông tin về các trường khác (báo đủ lỗi một lần),
  *  - thông báo lỗi là của NGHIỆP VỤ ("thiếu user.id") chứ không phải của thư viện JSON.
  */
final case class RawEvent(
    eventId: Option[String],
    eventType: Option[String],
    ts: Option[Json],              // chuỗi ISO-8601 HOẶC số epoch millis — hai phiên bản SDK gửi khác nhau
    userId: Option[String],
    campaignId: Option[String],
    impressionId: Option[String],
    os: Option[String],
    country: Option[String],
    city: Option[String],
    typeErrors: List[(String, String)] = Nil // (đường dẫn trường, lỗi kiểu) theo thứ tự gặp ("event_id phải là chuỗi, nhận Number")
)

object RawEvent:
  /** Đọc cấu trúc LỒNG NHAU (`user.id`, `device.os`, `geo.country`) và làm phẳng thành RawEvent.
    *
    * Decoder KHÔNG BAO GIỜ thất bại với một JSON object: mỗi trường được đọc riêng; thiếu hoặc `null` → `None`;
    * SAI KIỂU (số thay cho chuỗi, chuỗi thay cho object) → `None` + một lỗi trong `typeErrors`. Nhờ vậy `validate` vẫn chạy
    * và báo MỌI lỗi của dòng. Bản đầu dùng `c.get[Option[String]]` trong for-comprehension: trường sai kiểu đầu tiên làm cả dòng
    * dừng với một thông báo của thư viện (review chéo, M2); còn `.orElse(Right(None))` ở trường lồng nhau lại NUỐT lỗi kiểu.
    */
  given Decoder[RawEvent] = Decoder.instance { c =>
    val errors = scala.collection.mutable.LinkedHashMap.empty[String, String]

    def obj(path: String, cursor: io.circe.ACursor): Option[io.circe.ACursor] = cursor.focus match
      case None                    => None
      case Some(j) if j.isNull     => None
      case Some(j) if j.isObject   => Some(cursor)
      case Some(j)                 => errors(path) = s"$path phải là object, nhận ${j.name}"; None

    def str(path: String, cursor: Option[io.circe.ACursor], field: String): Option[String] =
      cursor.flatMap(_.downField(field).focus) match
        case None                => None
        case Some(j) if j.isNull => None
        case Some(j) =>
          j.asString match
            case some @ Some(_) => some
            case None           => errors(path) = s"$path phải là chuỗi, nhận ${j.name}"; None

    val root   = Some(c)
    val user   = obj("user", c.downField("user"))
    val device = obj("device", c.downField("device"))
    val geo    = obj("geo", c.downField("geo"))
    val raw = RawEvent(
      eventId = str("event_id", root, "event_id"),
      eventType = str("type", root, "type"),
      ts = c.downField("ts").focus.filterNot(_.isNull),
      userId = str("user.id", user, "id"),
      campaignId = str("campaign_id", root, "campaign_id"),
      impressionId = str("impression_id", root, "impression_id"),
      os = str("device.os", device, "os"),
      country = str("geo.country", geo, "country"),
      city = str("geo.city", geo, "city")
    )
    if c.focus.exists(_.isObject) then Right(raw.copy(typeErrors = errors.toList))
    else Left(io.circe.DecodingFailure("dòng phải là một JSON object", c.history))
  }

enum EventType:
  case Impression, Click

/** Dạng SẠCH — mọi bất biến đã được kiểm tra; code phía sau không cần kiểm tra lại. */
final case class AdEvent(
    eventId: String,
    eventType: EventType,
    ts: Instant,
    day: LocalDate,                // ngày theo giờ Việt Nam — đơn vị báo cáo
    userId: String,
    campaignId: String,
    impressionId: Option[String],  // chỉ click mới có
    os: String,                    // "unknown" nếu thiếu: thiếu OS không làm hỏng báo cáo theo campaign
    country: String,
    city: Option[String]           // thật sự không bắt buộc
)

final case class Campaign(@CsvName("campaign_id") campaignId: String, name: String, advertiser: String)

/** Một dòng bị loại, kèm số dòng trong file gốc và MỌI lý do. Không bao giờ âm thầm bỏ dữ liệu. */
final case class Rejected(line: Long, reasons: List[String], raw: String)

final case class ReportRow(
    @CsvName("campaign_id") campaignId: String,
    @CsvName("campaign_name") campaignName: String,
    day: LocalDate,
    impressions: Long,
    clicks: Long,
    @CsvName("unique_users") uniqueUsers: Long,
    @CsvName("ctr_percent") ctrPercent: BigDecimal
)
