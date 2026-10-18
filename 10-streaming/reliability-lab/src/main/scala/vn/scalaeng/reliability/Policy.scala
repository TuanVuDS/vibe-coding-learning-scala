package vn.scalaeng.reliability

object Policy:
  /** Kafka cho phép lỗ offset; chỉ commit prefix đã hoàn tất trong các record đã đọc. */
  def frontier(records: Vector[(Long, Boolean)]): Option[Long] =
    require(records.forall(_._1 >= 0) && records.map(_._1).sliding(2).forall(v => v.size < 2 || v(0) < v(1)),
      "offsets must increase")
    records.takeWhile(_._2).lastOption.map(r => Math.addExact(r._1, 1))

  /** Budget gồm cả batch poll; đây là phép tính nhận việc, chưa phải số đo độ trễ. */
  def withinBudget(records: Int, worstRecordMillis: Long, maxPollMillis: Long, marginMillis: Long): Boolean =
    require(records > 0 && worstRecordMillis >= 0 && maxPollMillis > 0 && marginMillis >= 0, "invalid poll budget")
    BigInt(records) * worstRecordMillis + marginMillis < maxPollMillis

object ErrorDemo:
  def main(args: Array[String]): Unit =
    import java.nio.file.Files
    import scala.util.Using
    import vn.scalaeng.p4.{Event, Input, Json, Pipeline, Store}
    def show(action: => Unit): Unit =
      try action
      catch case e: Exception => println(s"${e.getClass.getName}: ${e.getMessage}")
    show { vn.scalaeng.p4.Retry.run(0, _ => ())(()); () }
    show { Policy.frontier(Vector(2L -> true, 1L -> false)); () }
    show { new vn.scalaeng.p4.Store(java.nio.file.Files.createTempDirectory("invalid-policy-"), width = 0); () }
    show { Policy.withinBudget(0, 1, 100, 0); () }
    val dir = Files.createTempDirectory("ch30-errors-")
    val e = Event(1, "e1", "ORDER", 100, 100)
    println(s"duplicate JSON fields: ${Json.event(Some(Json.encode(e).dropRight(1) + ",\"amount\":5}"))}")
    val in = Input("orders", 0, 0, Some("ORDER"), Some(Json.encode(e)))
    Using.resource(new Store(dir, clock = () => 100)) { store =>
      store.process(in)
      show { new Store(dir); () }
      show { store.process(in.copy(raw = Some("{}"))); () }
      val future = store.process(in.copy(offset = 1, raw = Some(Json.encode(e.copy(eventTime = 4000000000L)))))
      println(s"future kind=${future.kind} reason=${future.reason.get} maxTime=${store.state.maxTime}")
      show {
        Pipeline.handle(store, in.copy(offset = 2, raw = Some("{")),
          _ => throw new java.io.IOException("injected DLT failure"), () => ()); ()
      }
    }
    show { new Store(dir, delay = 51); () }
    show { new Store(dir, maxSnapshotBytes = 1); () }
    Files.writeString(dir.resolve("state.json"), "{}")
    show { new Store(dir); () }

object SimpleDemo:
  def main(args: Array[String]): Unit =
    println(Policy.frontier(Vector(0L -> true, 2L -> false, 5L -> true)))
    println(Policy.frontier(Vector(0L -> true, 2L -> true, 5L -> true)))
