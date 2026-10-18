package vn.scalaeng.streaming

enum Delivery:
  case AtMostOnce, AtLeastOnce

final case class CrashResult(committed: Int, effects: Vector[String])

object Crash:
  /** Simulated crash exactly once at crashAt; committed cursor and sink survive restart. */
  def run(ids: Vector[String], mode: Delivery, crashAt: Int, dedup: Boolean): CrashResult =
    require(crashAt >= 0 && crashAt < ids.size, "invalid crash index")
    def write(out: Vector[String], id: String): Vector[String] =
      if dedup && out.contains(id) then out else out :+ id
    val before = ids.take(crashAt)
    val partial = before.foldLeft(Vector.empty[String])(write)
    val (committed, sink) = mode match
      case Delivery.AtMostOnce => (crashAt + 1, partial) // commit, CRASH, effect never runs
      case Delivery.AtLeastOnce => (crashAt, write(partial, ids(crashAt))) // effect, CRASH, no commit
    val after = ids.drop(committed).foldLeft(sink)(write)
    CrashResult(ids.size, after)
