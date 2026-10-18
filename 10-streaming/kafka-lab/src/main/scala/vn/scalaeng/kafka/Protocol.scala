package vn.scalaeng.kafka

import io.circe.Json
import io.circe.parser.parse

final case class OrderEvent(schemaVersion: Int, eventId: String, orderId: String, amount: Long)

object Protocol:
  def encode(e: OrderEvent): String = Json.obj(
    "schema_version" -> Json.fromInt(e.schemaVersion), "event_id" -> Json.fromString(e.eventId),
    "order_id" -> Json.fromString(e.orderId), "amount" -> Json.fromLong(e.amount)).noSpaces

  /** Unknown fields accepted for additive evolution; identifiers preserve case and whitespace. */
  def decode(raw: String): Either[Vector[String], OrderEvent] =
    Option(raw).toRight(Vector("tombstone unsupported")).flatMap(text =>
      parse(text).left.map(e => Vector(s"invalid JSON: ${e.message}"))).flatMap { json =>
      val c = json.hcursor
      val version = c.get[Int]("schema_version").toOption
      val id = c.get[String]("event_id").toOption.filter(_.trim.nonEmpty)
      val order = c.get[String]("order_id").toOption.filter(_.trim.nonEmpty)
      val amount = c.get[Long]("amount").toOption.filter(_ >= 0)
      val errors = Vector(
        Option.when(!version.contains(1))("schema_version must be 1"),
        Option.when(id.isEmpty)("event_id must be nonblank string"),
        Option.when(order.isEmpty)("order_id must be nonblank string"),
        Option.when(amount.isEmpty)("amount must be nonnegative Long")).flatten
      if errors.nonEmpty then Left(errors)
      else Right(OrderEvent(version.get, id.get, order.get, amount.get))
    }

enum Applied:
  case New, Duplicate

/** Pedagogical atomic in-memory sink. Lifetime is one process, NOT durable across JVM restart. */
final class MemoryLedger:
  private var seen = Map.empty[String, OrderEvent]
  private var sums = Map.empty[String, BigInt]

  def apply(e: OrderEvent): Applied = synchronized {
    seen.get(e.eventId) match
      case Some(old) if old == e => Applied.Duplicate
      case Some(_) => throw Poison(s"event_id conflict: ${e.eventId}")
      case None =>
        seen = seen.updated(e.eventId, e)
        sums = sums.updated(e.orderId, sums.getOrElse(e.orderId, BigInt(0)) + e.amount)
        Applied.New
  }
  def totals: Map[String, BigInt] = synchronized(sums)

final case class Poison(reason: String) extends RuntimeException(reason)
final case class Transient(reason: String) extends RuntimeException(reason)

final case class DeadLetter(topic: String, partition: Int, offset: Long, key: Option[String],
    raw: Option[String], errors: Vector[String], attempts: Int):
  def json: String = Json.obj(
    "source_topic" -> Json.fromString(topic), "source_partition" -> Json.fromInt(partition),
    "source_offset" -> Json.fromLong(offset), "key" -> key.fold(Json.Null)(Json.fromString),
    "raw" -> raw.fold(Json.Null)(Json.fromString),
    "errors" -> Json.arr(errors.map(Json.fromString)*), "attempts" -> Json.fromInt(attempts)).noSpaces

object Retry:
  /** maxAttempts counts first attempt; only explicitly transient failures are retried. */
  def run[A](maxAttempts: Int, pause: Int => Unit)(action: => A): Either[(Throwable, Int), A] =
    require(maxAttempts > 0, "maxAttempts must be positive")
    def loop(n: Int): Either[(Throwable, Int), A] =
      try Right(action)
      catch
        case _: Transient if n < maxAttempts => pause(n); loop(n + 1)
        case e: Transient => Left((e, n))
        case e: Poison => Left((e, n))
    loop(1)
