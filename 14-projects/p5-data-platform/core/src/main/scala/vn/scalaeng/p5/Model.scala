package vn.scalaeng.p5

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.parse

import java.time.{Instant, LocalDate}
import scala.util.Try

/** Sự kiện đơn hàng — cùng một định dạng cho mọi đường vào (Kafka, thả file). Đây là HỢP ĐỒNG với producer: đổi nó là đổi phiên bản. */
final case class OrderEvent(event_id: String, order_id: String, customer_id: String, day: String, status: String, sku: String,
    quantity: Int, unit_price_vnd: Long, updated_at: String)

object OrderEvent:
  given Encoder[OrderEvent] = deriveEncoder
  given Decoder[OrderEvent] = deriveDecoder
  val Fields: Set[String] = Set("event_id", "order_id", "customer_id", "day", "status", "sku", "quantity", "unit_price_vnd", "updated_at")

  /** CONTRACT ở cửa ngõ (Chương 33): JSON object, ĐÚNG bộ trường, đúng kiểu. Giá trị nghiệp vụ (quantity = 0...) để staging xử lý —
    * contract chỉ trả lời "producer có gửi đúng hình dạng không".
    */
  def conform(line: String): Either[String, OrderEvent] =
    parse(line).left.map(_ => "không phải JSON").flatMap { (json: Json) =>
      json.asObject.toRight("không phải JSON object").flatMap { obj =>
        val keys = obj.keys.toSet
        val parts = List("thiếu" -> (Fields -- keys), "thừa" -> (keys -- Fields)).collect { case (k, s) if s.nonEmpty => s"$k ${s.toList.sorted.mkString(",")}" }
        if keys != Fields then Left(s"bộ trường sai: ${parts.mkString(", ")}")
        else json.as[OrderEvent].left.map(e => s"sai kiểu: ${e.getMessage}")
      }
    }

/** Đơn đã chuẩn hóa ở staging. */
final case class Order(eventId: String, orderId: String, customerId: String, day: LocalDate, status: String, sku: String, quantity: Int,
    priceVnd: Long, updatedAt: Instant):
  def payload: (String, LocalDate, String, Int, Long) = (customerId, day, status, quantity, priceVnd)

object Order:
  val Statuses: Set[String] = Set("PAID", "REFUNDED", "CANCELLED")
  private val InstantPattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$"
  private val DayPattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

  /** MỘT quy tắc ngày cho mọi nơi — validate, chọn partition, đối soát dòng không thuộc ngày nào. Bản đầu dùng ba quy tắc khác nhau
    * (so chuỗi, regex + parse, chỉ `LocalDate.parse`): `+02026-10-15` được Java parse nhưng regex từ chối → dòng rơi khỏi MỌI đối soát
    * (review P5 M3).
    */
  def dayOf(raw: String): Option[LocalDate] = Option(raw).map(_.trim).filter(_.matches(DayPattern)).flatMap(d => Try(LocalDate.parse(d)).toOption)

  /** Validate + chuẩn hóa. Gom MỌI lỗi (Chương 20); timestamp bắt buộc có múi giờ (Chương 24 M4). */
  def validate(e: OrderEvent): Either[List[String], Order] =
    val day = dayOf(e.day)
    val ts  = Option(e.updated_at.trim).filter(_.matches(InstantPattern)).flatMap(t => Try(Instant.parse(t)).toOption)
    val status = e.status.trim.toUpperCase(java.util.Locale.ROOT)
    val errors = List(
      Option.when(e.event_id.trim.isEmpty)("event_id: bắt buộc"),
      Option.when(e.order_id.trim.isEmpty)("order_id: bắt buộc"),
      Option.when(e.customer_id.trim.isEmpty)("customer_id: bắt buộc"),
      Option.when(day.isEmpty)("day: phải là ngày yyyy-MM-dd"),
      Option.when(!Statuses(status))("status: phải là PAID/REFUNDED/CANCELLED"),
      Option.when(e.sku.trim.isEmpty)("sku: bắt buộc"),
      Option.when(e.quantity < 1 || e.quantity > 1000)("quantity: 1..1000"),
      Option.when(e.unit_price_vnd <= 0)("unit_price_vnd: phải dương"),
      Option.when(ts.isEmpty)("updated_at: ISO-8601 có múi giờ")
    ).flatten
    if errors.nonEmpty then Left(errors)
    else Right(Order(e.event_id.trim, e.order_id.trim, e.customer_id.trim.toUpperCase(java.util.Locale.ROOT), day.get, status,
      e.sku.trim.toUpperCase(java.util.Locale.ROOT), e.quantity, e.unit_price_vnd, ts.get))

/** Một dòng staging. Lưu dạng JSON Lines (circe), KHÔNG phải CSV tự ghép chuỗi: bản đầu nội suy thẳng `order_id` vào CSV nên `O,comma` thành 8 cột
  * và bước sau âm thầm bỏ dòng — mất doanh thu mà gate vẫn xanh (review P5 M1). Codec có escape thì mọi chuỗi hợp lệ đi qua nguyên vẹn.
  */
final case class StagingRow(order_id: String, customer_id: String, status: String, sku: String, quantity: Int, unit_price_vnd: Long, updated_at: String)

object StagingRow:
  given Encoder[StagingRow] = deriveEncoder
  given Decoder[StagingRow] = deriveDecoder
  def of(o: Order): StagingRow = StagingRow(o.orderId, o.customerId, o.status, o.sku, o.quantity, o.priceVnd, o.updatedAt.toString)

/** Một sự kiện bị loại ở staging, kèm mọi lý do. */
final case class RejectedRow(event_id: String, errors: List[String])

object RejectedRow:
  given Encoder[RejectedRow] = deriveEncoder
  given Decoder[RejectedRow] = deriveDecoder
