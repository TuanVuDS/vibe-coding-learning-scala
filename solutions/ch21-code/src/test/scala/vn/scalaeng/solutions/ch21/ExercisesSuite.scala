package vn.scalaeng.solutions.ch21

import io.circe.{Json, parser}
import io.circe.syntax.*
import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneId}
import scala.jdk.CollectionConverters.*
import scala.util.Using
import vn.scalaeng.formats.{Conversions, Evolution, Order, ParquetOrders}

class ExercisesSuite extends munit.FunSuite:
  private def temp[A](f: Path => A): A =
    val dir = Files.createTempDirectory("ch21-solutions-")
    try f(dir)
    finally Using.resource(Files.walk(dir)) { paths =>
      paths.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(p => Files.delete(p))
    }

  test("exercise 1: exact USD/VND, zero and boundary") {
    assertEquals(Money.parse("12.30", "USD"), Right(1230L))
    assertEquals(Money.parse("12000", "VND"), Right(12000L))
    assertEquals(Money.parse("0", "USD"), Right(0L))
    assertEquals(Money.parse("9223372036854775807", "VND"), Right(Long.MaxValue))
  }
  test("exercise 1: no implicit rounding, currency guesses, negatives or overflow") {
    for (value, currency) <- List("1.001" -> "USD", "1.1" -> "VND", "1" -> "EUR",
      "1e2" -> "USD", "-1" -> "USD", "9223372036854775808" -> "VND") do
      assert(Money.parse(value, currency).isLeft)
  }

  test("exercise 2: missing/null/empty/value remain distinct") {
    assertEquals(Presence.note(Json.obj()), Right(Presence.Missing))
    assertEquals(Presence.note(Json.obj("note" -> Json.Null)), Right(Presence.ExplicitNull))
    assertEquals(Presence.note(Json.obj("note" -> Json.fromString(""))), Right(Presence.Value("")))
    assertEquals(Presence.note(Json.obj("note" -> Json.fromString("paid"))), Right(Presence.Value("paid")))
  }
  test("exercise 2: reject wrong JSON shape/types") {
    assert(Presence.note(Json.arr()).isLeft)
    assert(Presence.note(Json.obj("note" -> Json.fromInt(1))).isLeft)
  }

  test("exercise 3: Avro decimal bytes round-trip signed exact values") {
    for value <- List("12.30", "-12.30", "0.00", "9999999999.99") do
      assertEquals(DecimalWire.roundTrip(value), Right(BigDecimal(value)))
  }
  test("exercise 3: decimal rejects precision or scale overflow") {
    assert(DecimalWire.roundTrip("10000000000.00").isLeft)
    assert(DecimalWire.roundTrip("1.001").isLeft)
  }

  test("exercise 4: offset/timezone semantics and daylight-saving overlap") {
    val instant = TimestampWire.parse("2026-10-01T00:30:00+07:00").toOption.get
    assertEquals(instant, Instant.parse("2026-09-30T17:30:00Z"))
    assertEquals(Conversions.businessDay(instant, ZoneId.of("Asia/Ho_Chi_Minh")).toString, "2026-10-01")
    val first = TimestampWire.parse("2026-11-01T01:30:00-04:00").toOption.get
    val second = TimestampWire.parse("2026-11-01T01:30:00-05:00").toOption.get
    assertEquals(java.time.Duration.between(first, second).toHours, 1L)
  }
  test("exercise 4: reject unzoned and sub-millisecond timestamps") {
    assert(TimestampWire.parse("2026-10-01T00:30:00").isLeft)
    assert(TimestampWire.parse("2026-10-01T00:30:00.123456Z").isLeft)
  }

  test("exercise 5: bridge has full pairwise compatibility and dual-writes values") {
    assert(Evolution.compatible(RenameBridge.schema, Evolution.v1))
    assert(Evolution.compatible(Evolution.v1, RenameBridge.schema))
    temp { dir =>
      val path = dir.resolve("bridge.avro")
      val bridge = RenameBridge.upgrade(Evolution.sample(Evolution.v1))
      Evolution.write(path, RenameBridge.schema, bridge)
      assertEquals(Evolution.read(path, Evolution.v1).get("label").toString, "paid")
      val current = Evolution.read(path, RenameBridge.schema)
      assertEquals(current.get("label").toString, current.get("name").toString)
    }
  }
  test("exercise 5: historical data gets explicit empty name; alias-only is not full") {
    temp { dir =>
      val path = dir.resolve("old.avro")
      Evolution.write(path, Evolution.v1, Evolution.sample(Evolution.v1))
      assertEquals(Evolution.read(path, RenameBridge.schema).get("name").toString, "")
      assert(!Evolution.compatible(Evolution.v1, Evolution.renamed))
    }
  }

  test("exercise 6: migrate valid JSONL and quarantine syntax/domain errors with positions") {
    temp { dir =>
      val input = dir.resolve("input.jsonl")
      val output = dir.resolve("orders.parquet")
      val rejects = dir.resolve("rejected.jsonl")
      val order = Order.generated(1).next()
      val bad = order.copy(id = -1, amountMinor = -1)
      Files.writeString(input, List(order.asJson.noSpaces, "{", bad.asJson.noSpaces).mkString("\n"))
      assertEquals(Quarantine.migrate(input, output, rejects), MigrationReport(1, 2))
      val result = Vector.newBuilder[Order]
      ParquetOrders.scan(output, result += _)
      assertEquals(result.result(), Vector(order))
      val rejected = Files.readAllLines(rejects).asScala.map(line => parser.parse(line).toOption.get)
      assertEquals(rejected.map(_.hcursor.get[Long]("line").toOption.get).toList, List(2L, 3L))
      assertEquals(rejected.last.hcursor.get[List[String]]("errors").toOption.get.size, 2)
    }
  }
  test("exercise 6: zero accepted rows still produces readable empty Parquet; reject path alias") {
    temp { dir =>
      val input = dir.resolve("input.jsonl")
      val output = dir.resolve("orders.parquet")
      val rejects = dir.resolve("rejected.jsonl")
      Files.writeString(input, "{}\n")
      assertEquals(Quarantine.migrate(input, output, rejects), MigrationReport(0, 1))
      assertEquals(ParquetOrders.sum(output), 0L)
      intercept[IllegalArgumentException](Quarantine.migrate(input, input, rejects))
    }
  }
