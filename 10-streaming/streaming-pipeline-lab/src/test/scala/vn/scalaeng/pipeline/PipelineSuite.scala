package vn.scalaeng.pipeline

import java.nio.file.Files
import scala.util.Using
import vn.scalaeng.p4.{Event, Input, Json, Pipeline, Store}

class PipelineSuite extends munit.FunSuite:
  test("all input records go to exactly one persisted category; reject carries raw and reason") {
    Using.resource(new Store(Files.createTempDirectory("ch29-reconcile-"))) { s =>
      val e = Event(1, "e", "ORDER", 100, 100)
      val raws = Vector(Json.encode(e), Json.encode(e), "{", Json.encode(e.copy(amount = 999)))
      var dlt = Vector.empty[String]
      raws.zipWithIndex.foreach { (raw, i) =>
        Pipeline.handle(s, Input("orders", 0, i.toLong, Some("ORDER"), Some(raw)),
          r => dlt :+= Json.receipt(r), () => ())
      }
      assertEquals(s.state.counts, Map("accepted" -> 1, "duplicate" -> 1, "invalid" -> 1, "conflict" -> 1))
      assertEquals(s.state.counts.values.sum, raws.size)
      assertEquals(dlt.size, 2)
      assert(s.state.receipts.values.filter(_.dead).forall(r => r.reason.exists(_.nonEmpty) && r.input.raw.nonEmpty))
    }
  }
  test("recovery repeats DLT before source commit without recalculating old watermark decision") {
    val dir = Files.createTempDirectory("ch29-recovery-")
    val e = Event(1, "a", "ORDER", 100, 100)
    val old = Input("orders", 0, 0, Some("ORDER"), Some(Json.encode(e)))
    Using.resource(new Store(dir)) { s =>
      s.process(old)
      s.process(old.copy(offset = 1, raw = Some(Json.encode(e.copy(eventId = "b", eventTime = 300)))))
    }
    Using.resource(new Store(dir)) { s =>
      assertEquals(s.process(old).kind, "accepted")
      assertEquals(s.process(old.copy(offset = 2)).kind, "duplicate")
      assertEquals(s.state.buckets.map(_.total).sum, BigInt(200))
    }
  }
