package vn.scalaeng.streaming

final case class Arrival(event: Event, processedAt: Long):
  require(processedAt >= 0, "negative processing time")

object Clocks:
  /** Replay changes processing-time buckets even when business timestamps do not change. */
  def buckets(arrivals: Vector[Arrival], size: Long, useEventTime: Boolean): Map[Window, BigInt] =
    arrivals.foldLeft(Map.empty[Window, BigInt]) { (acc, a) =>
      val time = if useEventTime then a.event.eventTime else a.processedAt
      val w = Windows.tumbling(time, size)
      acc.updated(w, acc.getOrElse(w, BigInt(0)) + 1)
    }
