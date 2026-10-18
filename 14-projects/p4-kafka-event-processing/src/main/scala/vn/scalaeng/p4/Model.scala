package vn.scalaeng.p4

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.{decode, parse}
import io.circe.syntax.*

/** Thời gian là giây epoch; số tiền VND dương, chỉ nhận version 1. */
case class Event(version: Int, eventId: String, orderId: String, eventTime: Long, amount: Long)
case class Input(topic: String, partition: Int, offset: Long, key: Option[String], raw: Option[String]):
  def id: String = s"$topic:$partition:$offset"
case class Receipt(input: Input, kind: String, reason: Option[String], attempts: Int):
  def dead: Boolean = reason.nonEmpty
case class Bucket(start: Long, orderId: String, total: BigInt)
case class State(format: Int, width: Long, delay: Long, maxFutureSkew: Long, maxTime: Long,
    seen: Map[String, Event], buckets: Vector[Bucket], receipts: Map[String, Receipt]):
  def counts: Map[String, Int] = receipts.values.toVector.groupMapReduce(_.kind)(_ => 1)(_ + _)

object Json:
  given Encoder[Event] = deriveEncoder
  given Decoder[Event] = deriveDecoder
  given Encoder[Input] = deriveEncoder
  given Decoder[Input] = deriveDecoder
  given Encoder[Receipt] = deriveEncoder
  given Decoder[Receipt] = deriveDecoder
  given Encoder[Bucket] = deriveEncoder
  given Decoder[Bucket] = deriveDecoder
  given Encoder[State] = deriveEncoder
  given Decoder[State] = deriveDecoder

  def encode(event: Event): String = event.asJson.noSpaces
  def receipt(value: Receipt): String = value.asJson.noSpaces
  def snapshot(value: State): String = value.asJson.noSpaces
  def restore(raw: String): State = decode[State](raw).fold(
    e => throw new IllegalStateException(s"snapshot corrupt: ${e.getMessage}"), identity)

  def event(raw: Option[String]): Either[String, Event] =
    raw.toRight("null value is not an order event").flatMap { text =>
      parse(text).left.map(_ => "invalid JSON").flatMap { json =>
        val allowed = Set("version", "eventId", "orderId", "eventTime", "amount")
        if !json.asObject.exists(_.keys.toSet == allowed) then Left("schema fields mismatch")
        else json.as[Event].left.map(_ => "schema types mismatch").flatMap { e =>
          if e.version != 1 then Left("unsupported version")
          else if !e.eventId.matches("[A-Za-z0-9-]{1,64}") || !e.orderId.matches("[A-Za-z0-9-]{1,64}") then
            Left("invalid identifier")
          else if e.eventTime < 0 || e.amount <= 0 then Left("eventTime must be nonnegative and amount positive")
          else Right(e)
        }
      }
    }

final case class Transient(message: String) extends RuntimeException(message)

object Retry:
  /** Chỉ lỗi adapter đã phân loại Transient mới được retry đến hết lượt. */
  def run(maxAttempts: Int, pause: Int => Unit)(action: => Unit): Either[(String, Int), Int] =
    require(maxAttempts > 0 && maxAttempts <= 10, "attempts must be in 1..10")
    @scala.annotation.tailrec
    def loop(attempt: Int): Either[(String, Int), Int] =
      val failure = try
        action
        None
      catch case e: Transient => Some(e.getMessage)
      failure match
        case None => Right(attempt)
        case Some(reason) if attempt == maxAttempts => Left((reason, attempt))
        case Some(_) =>
          pause(attempt)
          loop(attempt + 1)
    loop(1)
