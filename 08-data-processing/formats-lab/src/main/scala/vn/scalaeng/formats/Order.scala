package vn.scalaeng.formats

import io.circe.{Decoder, Encoder}
import java.time.{Instant, OffsetDateTime, ZoneId}
import scala.util.Try

/** Wire model: amountMinor is an integer in the currency's smallest unit.
  * Timestamps have millisecond precision; validation rejects silent truncation.
  */
final case class Order(
    id: Long,
    userId: String,
    amountMinor: Long,
    currency: String,
    occurredAt: Instant,
    note: Option[String]
)

object Order:
  given Encoder[Order] = Encoder.forProduct6(
    "id", "userId", "amountMinor", "currency", "occurredAt", "note"
  )(o => (o.id, o.userId, o.amountMinor, o.currency, o.occurredAt.toString, o.note))

  given Decoder[Order] = Decoder.instance { c =>
    for
      id <- c.get[Long]("id")
      user <- c.get[String]("userId")
      amount <- c.get[Long]("amountMinor")
      currency <- c.get[String]("currency")
      text <- c.get[String]("occurredAt")
      instant <- Decoder.decodeString.emap(s => Try(Instant.parse(s)).toEither.left.map(_.getMessage))
        .decodeJson(io.circe.Json.fromString(text))
      note <- c.get[Option[String]]("note")
    yield Order(id, user, amount, currency, instant, note)
  }

  def validate(order: Order): Either[List[String], Order] =
    val errors = List(
      Option.when(order.id <= 0)("id must be positive"),
      Option.when(order.userId.trim.isEmpty)("userId must be non-empty"),
      Option.when(order.amountMinor < 0)("amountMinor must be non-negative"),
      Option.when(!Set("USD", "VND").contains(order.currency))("currency must be USD or VND"),
      Option.when(order.occurredAt.getNano % 1000000 != 0)("occurredAt must have millisecond precision")
    ).flatten
    if errors.isEmpty then Right(order) else Left(errors)

  def checked(order: Order): Order =
    validate(order).fold(errors => throw IllegalArgumentException(errors.mkString("; ")), identity)

  def generated(n: Int): Iterator[Order] =
    Iterator.range(1, n + 1).map { i =>
      Order(i.toLong, s"U-${i % 10000}", 100L + i % 100000, "USD",
        Instant.ofEpochMilli(1790812800000L + i * 1000L),
        if i % 5 == 0 then None else Some(s"Đơn ${i % 100}, \"paid\""))
    }

object Conversions:
  def minor(text: String, scale: Int): Either[String, Long] =
    if scale < 0 || scale > 9 then Left("scale must be in 0..9")
    else Try(BigDecimal(text).bigDecimal.movePointRight(scale).longValueExact())
      .toEither.left.map(_.getMessage)

  /** Offset is mandatory: a wall clock without zone is not an instant. */
  def instant(text: String): Either[String, Instant] =
    Try(OffsetDateTime.parse(text).toInstant).toEither.left.map(_.getMessage)

  def businessDay(instant: Instant, zone: ZoneId): java.time.LocalDate =
    instant.atZone(zone).toLocalDate
