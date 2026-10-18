package vn.scalaeng.reliability

import java.nio.file.Files
import scala.util.Using
import vn.scalaeng.p4.{Event, Input, Json, Pipeline, Store}

class PolicySuite extends munit.FunSuite:
  test("never commit past incomplete fetched record, even with offset holes") {
    assertEquals(Policy.frontier(Vector(0L -> true, 2L -> false, 5L -> true)), Some(1L))
    assertEquals(Policy.frontier(Vector(0L -> false, 2L -> true)), None)
    assertEquals(Policy.frontier(Vector(0L -> true, 2L -> true)), Some(3L))
    intercept[IllegalArgumentException](Policy.frontier(Vector(1L -> true, 1L -> true)))
  }
  test("bounded batch plus retry duration must fit poll interval with headroom") {
    assert(Policy.withinBudget(1, 40000, 120000, 10000))
    assert(!Policy.withinBudget(3, 40000, 120000, 10000))
    assert(!Policy.withinBudget(Int.MaxValue, Long.MaxValue, Long.MaxValue, 0))
  }
  test("durable effect before failed commit is applied only once after store restart") {
    val dir = Files.createTempDirectory("ch30-replay-")
    val in = Input("orders", 0, 0, Some("ORDER"), Some(Json.encode(Event(1, "e", "ORDER", 100, 100))))
    Using.resource(new Store(dir)) { s =>
      interceptMessage[java.io.IOException]("commit failed") {
        Pipeline.handle(s, in, _ => (), () => throw new java.io.IOException("commit failed"))
      }
    }
    Using.resource(new Store(dir)) { s =>
      var commits = 0
      Pipeline.handle(s, in, _ => fail("valid event in DLT"), () => commits += 1)
      assertEquals(commits, 1)
      assertEquals(s.state.buckets.map(_.total).sum, BigInt(100))
    }
  }
