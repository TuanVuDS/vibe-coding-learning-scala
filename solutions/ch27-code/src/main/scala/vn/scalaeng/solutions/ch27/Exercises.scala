package vn.scalaeng.solutions.ch27

import vn.scalaeng.streaming.*

object Exercises:
  // 1. Processing time is injected, not read from a wall clock inside a pure function.
  def clocks(event: Event, receivedAt: Long, size: Long): (Window, Window) =
    (Windows.tumbling(event.eventTime, size), Windows.tumbling(receivedAt, size))

  // 2. Every sliding window gets the FULL event amount, not a fraction.
  def slidingTotals(events: Vector[Event], size: Long, slide: Long): Map[(String, Window), BigInt] =
    events.foldLeft(Map.empty[(String, Window), BigInt]) { (acc, e) =>
      Windows.sliding(e.eventTime, size, slide).foldLeft(acc) { (s, w) =>
        val k = (e.key, w)
        s.updated(k, s.getOrElse(k, BigInt(0)) + e.amount)
      }
    }

  // 3. Active source without a watermark stalls progress; all idle means no new watermark.
  def globalWatermark(sources: Vector[(Boolean, Option[Long])]): Option[Long] =
    val active = sources.filterNot(_._1).map(_._2)
    if active.isEmpty || active.exists(_.isEmpty) then None else Some(active.flatten.min)

  // 4. Reference online preview: keeps all input, recomputes sessions; no bounded-state claim.
  def sessionPreviews(events: Vector[Event], gap: Long): Vector[Vector[Result]] =
    events.inits.toVector.reverse.tail.map(prefix => Windows.sessions(prefix, gap))

  // 5. Audit every record, including rejected/conflicting inputs. Outputs are not kept in State.
  final case class Audit(state: State, decisions: Map[Decision, Int], emitted: Vector[Result], peakWindows: Int)
  def audit(events: Vector[Event], size: Long, delay: Long): Audit =
    events.foldLeft(Audit(State(), Map.empty, Vector.empty, 0)) { (acc, e) =>
      val r = EventTime.step(acc.state, e, size, delay)
      Audit(r.state, acc.decisions.updated(r.decision, acc.decisions.getOrElse(r.decision, 0) + 1),
        acc.emitted ++ r.emitted, acc.peakWindows.max(r.state.open.size))
    }

  // 6. Capacity planning for constant rates; ignores bursts, bytes/event and external storage.
  def backlog(arrivalPerSecond: Long, servicePerSecond: Long, seconds: Long): BigInt =
    require(arrivalPerSecond >= 0 && servicePerSecond >= 0 && seconds >= 0, "negative rate or time")
    (BigInt(arrivalPerSecond) - servicePerSecond).max(BigInt(0)) * seconds
