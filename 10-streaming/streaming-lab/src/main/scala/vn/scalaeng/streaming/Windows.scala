package vn.scalaeng.streaming

final case class Event(id: String, key: String, eventTime: Long, amount: BigInt = BigInt(1)):
  require(id.nonEmpty && key.nonEmpty && eventTime >= 0, "invalid event")

final case class Window(start: BigInt, end: BigInt)
final case class Result(key: String, window: Window, count: BigInt, total: BigInt)
final case class Aggregate(count: BigInt = BigInt(0), total: BigInt = BigInt(0)):
  def add(e: Event): Aggregate = Aggregate(count + 1, total + e.amount)

object Windows:
  def tumbling(time: Long, size: Long): Window =
    require(time >= 0 && size > 0, "invalid window")
    val start = BigInt(time) / size * size
    Window(start, start + size)

  /** Half-open windows; finite fan-out prevents accidental million-window events. */
  def sliding(time: Long, size: Long, slide: Long): Vector[Window] =
    require(time >= 0 && slide > 0 && size >= slide && size % slide == 0 && size / slide <= 10000,
      "invalid sliding window")
    val last = BigInt(time) / slide * slide
    Vector.tabulate((size / slide).toInt)(i => last - BigInt(i) * slide)
      .filter(_ >= 0).map(start => Window(start, start + size)).filter(w => BigInt(time) < w.end)

  /** Finite batch reference: a gap exactly equal to gap starts a new session. */
  def sessions(events: Vector[Event], gap: Long): Vector[Result] =
    require(gap > 0, "gap must be positive")
    events.groupBy(_.key).toVector.sortBy(_._1).flatMap { (key, values) =>
      values.sortBy(e => (e.eventTime, e.id)).foldLeft(Vector.empty[Result]) { (out, e) =>
        out.lastOption match
          case Some(last) if BigInt(e.eventTime) < last.window.end =>
            out.init :+ last.copy(window = last.window.copy(end = BigInt(e.eventTime) + gap),
              count = last.count + 1, total = last.total + e.amount)
          case _ => out :+ Result(key, Window(BigInt(e.eventTime), BigInt(e.eventTime) + gap), 1, e.amount)
      }
    }

enum Decision:
  case Accepted, Duplicate, Conflict, Late

final case class State(
    maxSeen: Option[Long] = None,
    watermark: Option[BigInt] = None,
    seen: Map[String, Event] = Map.empty,
    open: Map[(String, Window), Aggregate] = Map.empty)
final case class Step(state: State, decision: Decision, emitted: Vector[Result])

object EventTime:
  /** One input source, tumbling windows, zero extra allowed lateness after watermark. */
  def step(s: State, e: Event, size: Long, delay: Long): Step =
    require(size > 0 && delay >= 0, "invalid watermark config")
    if s.watermark.exists(w => BigInt(e.eventTime) < w) then Step(s, Decision.Late, Vector.empty)
    else s.seen.get(e.id) match
      case Some(previous) => Step(s, if previous == e then Decision.Duplicate else Decision.Conflict, Vector.empty)
      case None =>
        val maxSeen = s.maxSeen.fold(e.eventTime)(_.max(e.eventTime))
        val watermark = BigInt(maxSeen) - delay
        val window = Windows.tumbling(e.eventTime, size)
        val k = (e.key, window)
        val updated = s.open.updated(k, s.open.getOrElse(k, Aggregate()).add(e))
        val (closed, open) = updated.partition { case ((_, w), _) => w.end <= watermark }
        val seen = s.seen.updated(e.id, e).filter { case (_, v) => BigInt(v.eventTime) >= watermark }
        Step(State(Some(maxSeen), Some(watermark), seen, open), Decision.Accepted, results(closed))

  def finish(s: State): Vector[Result] = results(s.open)

  private def results(windows: Map[(String, Window), Aggregate]): Vector[Result] =
    windows.toVector.map { case ((key, w), a) => Result(key, w, a.count, a.total) }
      .sortBy(r => (r.window.start, r.key))
