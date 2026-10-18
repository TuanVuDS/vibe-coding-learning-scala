package vn.scalaeng.solutions.ch28

import vn.scalaeng.kafka.*

object Exercises:
  // 1. Add a v2 decoder without silently accepting unsupported currencies.
  def decodeV2(raw: String): Either[Vector[String], OrderEvent] =
    io.circe.parser.parse(raw).left.map(e => Vector(e.message)).flatMap { j =>
      if !j.hcursor.get[Int]("schema_version").contains(2) then Left(Vector("expected version 2"))
      else if !j.hcursor.get[String]("currency").contains("VND") then Left(Vector("currency must be VND"))
      else Protocol.decode(j.mapObject(_.add("schema_version", io.circe.Json.fromInt(1))).noSpaces)
    }

  // 2. Missing committed offset stays unknown: no fabricated zero lag.
  def totalLag(partitions: Vector[(Long, Option[Long])]): Option[BigInt] =
    partitions.foldLeft(Option(BigInt(0))) { case (acc, (end, committed)) =>
      for a <- acc; b <- Clients.lag(end, committed) yield a + b
    }

  // 3. Within ONE partition: commit only the completed prefix of polled records; gaps are legal.
  def commitFrontier(polled: Vector[(Long, Boolean)]): Option[Long] =
    require(polled.forall(_._1 >= 0) && polled.map(_._1).sliding(2).forall(p => p.size < 2 || p(0) < p(1)),
      "offsets must be nonnegative and strictly increasing")
    polled.takeWhile(_._2).lastOption.map { (offset, _) => Math.addExact(offset, 1L) }

  // 4. Capped exponential backoff; retries count excludes initial attempt.
  def backoff(baseMs: Long, capMs: Long, retries: Int): Vector[Long] =
    require(baseMs > 0 && capMs >= baseMs && retries >= 0 && retries <= 1000, "invalid backoff")
    Vector.iterate(BigInt(baseMs), retries)(d => (d * 2).min(BigInt(capMs))).map(_.toLong)

  // 5. Snapshot represents data that a production DB must persist in the SAME transaction.
  final case class Snapshot(seen: Map[String, OrderEvent], totals: Map[String, BigInt])
  val empty: Snapshot = Snapshot(Map.empty, Map.empty)
  def applyEvent(s: Snapshot, e: OrderEvent): Either[String, Snapshot] = s.seen.get(e.eventId) match
    case Some(old) if old == e => Right(s)
    case Some(_) => Left(s"conflict: ${e.eventId}")
    case None => Right(Snapshot(s.seen.updated(e.eventId, e),
      s.totals.updated(e.orderId, s.totals.getOrElse(e.orderId, BigInt(0)) + e.amount)))

  // 6. DLT replay preflight: validation can succeed only after poison is repaired.
  def replay(rawDead: String): Either[Vector[String], OrderEvent] =
    io.circe.parser.parse(rawDead).left.map(e => Vector(e.message)).flatMap { j =>
      j.hcursor.get[String]("raw").left.map(_ => Vector("missing raw payload")).flatMap(Protocol.decode)
    }
