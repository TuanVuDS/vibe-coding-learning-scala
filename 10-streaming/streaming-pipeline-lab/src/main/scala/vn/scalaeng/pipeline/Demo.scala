package vn.scalaeng.pipeline

import java.nio.file.Files
import scala.util.Using
import vn.scalaeng.p4.{Event, Input, Json, Pipeline, Store}

object SimpleDemo:
  def main(args: Array[String]): Unit =
    val input = Input("orders", 0, 0, Some("ORDER"), Some(Json.encode(Event(1, "e1", "ORDER", 100, 100))))
    Using.resource(new Store(Files.createTempDirectory("ch29-simple-"))) { store =>
      println(store.process(input).kind)
      println(store.process(input.copy(offset = 1)).kind)
      println(store.state.buckets.map(_.total).sum)
    }

object SnapshotMeasure:
  def main(args: Array[String]): Unit =
    Vector(500, 1000, 2000).foreach { count =>
      val dir = Files.createTempDirectory("ch29-measure-")
      Using.resource(new Store(dir)) { store =>
        val start = System.nanoTime()
        (0 until count).foreach { i =>
          val e = Event(1, s"e$i", "ORDER", 100, 100)
          store.process(Input("orders", 0, i.toLong, Some("ORDER"), Some(Json.encode(e))))
        }
        val millis = (System.nanoTime() - start) / 1000000
        println(s"n=$count ms=$millis msPerEvent=${millis.toDouble / count} snapshotBytes=${Files.size(dir.resolve("state.json"))}")
      }
    }

object Demo:
  def main(args: Array[String]): Unit =
    val dir = Files.createTempDirectory("ch29-demo-")
    val e = Event(1, "e1", "ORDER", 100, 100)
    val input = Vector(Json.encode(e), Json.encode(e), "{", Json.encode(e.copy(amount = 999)),
      Json.encode(e.copy(eventId = "future", eventTime = 300)), Json.encode(e.copy(eventId = "late", eventTime = 100)))
    var published = Vector.empty[String]
    var commits = 0
    Using.resource(new Store(dir)) { store =>
      input.zipWithIndex.foreach { (raw, i) =>
        val in = Input("orders", 0, i.toLong, Some("ORDER"), Some(raw))
        val r = Pipeline.handle(store, in, d => published :+= d.input.id, () => commits += 1)
        println(s"offset=$i kind=${r.kind} reason=${r.reason.getOrElse("-")}")
      }
      println(s"input=${input.size} receipts=${store.state.receipts.size} commits=$commits DLT=${published.size} total=${store.state.buckets.map(_.total).sum}")
    }
    Using.resource(new Store(dir)) { store =>
      val r = store.process(Input("orders", 0, 6, Some("ORDER"), Some(Json.encode(e))))
      println(s"reopen=${r.kind} total=${store.state.buckets.map(_.total).sum}")
    }
