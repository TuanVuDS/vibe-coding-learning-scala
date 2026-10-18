package vn.scalaeng.formats

import io.circe.parser.decode
import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneId}
import org.apache.avro.AvroTypeException
import org.apache.parquet.io.LocalInputFile
import scala.jdk.CollectionConverters.*
import scala.util.Using

class FormatsSuite extends munit.FunSuite:
  private def temp[A](f: Path => A): A =
    val dir = Files.createTempDirectory("formats-test-")
    try f(dir)
    finally
      Using.resource(Files.walk(dir)) { paths =>
        paths.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(p => Files.delete(p))
      }

  private val rows = Vector(
    Order(1, "U-1", 12345, "USD", Instant.parse("2026-10-01T00:00:00.123Z"), None),
    Order(2, "U-2", 0, "VND", Instant.EPOCH, Some("")),
    Order(3, "U-3", 1000, "USD", Instant.parse("1969-12-31T23:59:59.999Z"), Some("Đơn, \"đã trả\"\r\nxong")),
    Order(4, "U-4", Long.MaxValue, "VND", Instant.EPOCH, Some("\\N"))
  )

  for format <- List("json", "csv", "avro", "parquet"); compressed <- List(false, true) do
    test(s"$format compressed=$compressed preserves values and nullable/empty/sentinel notes") {
      temp { dir =>
        val path = dir.resolve("orders.data")
        val result = Vector.newBuilder[Order]
        format match
          case "json" =>
            TextOrders.writeJson(path, rows.iterator, compressed)
            TextOrders.scanJson(path, compressed, result += _)
          case "csv" =>
            TextOrders.writeCsv(path, rows.iterator, compressed)
            TextOrders.scanCsv(path, compressed, result += _)
          case "avro" =>
            AvroOrders.write(path, rows.iterator, compressed)
            AvroOrders.scan(path, result += _)
          case _ =>
            ParquetOrders.write(path, rows.iterator, compressed)
            ParquetOrders.scan(path, result += _)
        assertEquals(result.result(), rows)
      }
    }

  test("single-column sum agrees for all formats and codecs") {
    temp { dir =>
      val data = Order.generated(300).toVector
      val expected = data.map(_.amountMinor).sum
      for compressed <- List(false, true) do
        val path = dir.resolve(s"orders-$compressed")
        TextOrders.writeJson(path, data.iterator, compressed)
        assertEquals(TextOrders.sumJson(path, compressed), expected)
        TextOrders.writeCsv(path, data.iterator, compressed)
        assertEquals(TextOrders.sumCsv(path, compressed), expected)
        AvroOrders.write(path, data.iterator, compressed)
        assertEquals(AvroOrders.sum(path), expected)
        ParquetOrders.write(path, data.iterator, compressed)
        assertEquals(ParquetOrders.sum(path), expected)
    }
  }

  private val expectedCompatibility = List(
    "add-default" -> (true, true), "add-required" -> (false, true), "remove" -> (true, false),
    "int-long" -> (true, false), "int-string" -> (false, false), "rename-alias" -> (true, false),
    "rename-no-alias" -> (false, false), "nullable" -> (true, false)
  ).toMap

  for (name, v2) <- Evolution.cases do
    test(s"schema evolution $name checks compatibility AND real files in both directions") {
      val (backward, forward) = expectedCompatibility(name)
      assertEquals(Evolution.compatible(v2, Evolution.v1), backward)
      assertEquals(Evolution.compatible(Evolution.v1, v2), forward)
      temp { dir =>
        val oldFile = dir.resolve("v1.avro")
        val newFile = dir.resolve("v2.avro")
        Evolution.write(oldFile, Evolution.v1, Evolution.sample(Evolution.v1))
        Evolution.write(newFile, v2, Evolution.sample(v2, nullLabel = v2 == Evolution.nullable))
        if backward then
          val r = Evolution.read(oldFile, v2)
          if name == "add-default" then assertEquals(r.get("source").toString, "web")
          if name == "rename-alias" then assertEquals(r.get("name").toString, "paid")
          if name == "int-long" then assertEquals(r.get("id"), java.lang.Long.valueOf(7))
        else intercept[AvroTypeException](Evolution.read(oldFile, v2))
        if forward then assertEquals(Evolution.read(newFile, Evolution.v1).get("label").toString, "paid")
        else intercept[AvroTypeException](Evolution.read(newFile, Evolution.v1))
      }
    }

  test("validation accumulates errors and rejects timestamp precision loss") {
    val invalid = rows.head.copy(id = 0, userId = " ", amountMinor = -1, currency = "XYZ",
      occurredAt = Instant.parse("2026-01-01T00:00:00.123456Z"))
    assertEquals(Order.validate(invalid).swap.toOption.get.size, 5)
  }

  test("money conversion is exact and rejects rounding and overflow") {
    assertEquals(Conversions.minor("12.30", 2), Right(1230L))
    assertEquals(Conversions.minor("12000", 0), Right(12000L))
    assert(Conversions.minor("1.001", 2).isLeft)
    assert(Conversions.minor("9223372036854775808", 0).isLeft)
    assert(Conversions.minor("NaN", 2).isLeft)
  }

  test("timezone conversion preserves instant and uses explicit business day") {
    val instant = Conversions.instant("2026-10-01T00:30:00+07:00").toOption.get
    assertEquals(instant, Instant.parse("2026-09-30T17:30:00Z"))
    assertEquals(Conversions.businessDay(instant, ZoneId.of("Asia/Ho_Chi_Minh")).toString, "2026-10-01")
    assert(Conversions.instant("2026-10-01T00:30:00").isLeft)
  }

  test("JSON missing and null optional fields map to None; wrong types fail") {
    val prefix = """{"id":1,"userId":"U-1","amountMinor":100,"currency":"USD","occurredAt":"2026-01-01T00:00:00Z" """.trim
    assertEquals(decode[Order](prefix + "}").toOption.get.note, None)
    assertEquals(decode[Order](prefix + """, "note":null}""").toOption.get.note, None)
    assert(decode[Order](prefix + """, "note":42}""").isLeft)
  }

  test("chapter text example preserves absent, empty and literal sentinel notes") {
    temp(ChapterExamples.textRoundTrip)
  }

  test("chapter evolution example supplies the new reader default from a real v1 file") {
    temp { dir =>
      val record = ChapterExamples.readOldWithNew(dir)
      assertEquals(record.get("id"), java.lang.Integer.valueOf(7))
      assertEquals(record.get("label").toString, "paid")
      assertEquals(record.get("source").toString, "web")
    }
  }

  test("CSV rejects swapped physical header rather than silently using positions") {
    temp { dir =>
      val path = dir.resolve("bad.csv")
      Files.writeString(path, "userId,id,amountMinor,currency,occurredAt,notePresent,note\r\n")
      intercept[IllegalArgumentException](TextOrders.scanCsv(path, false, _ => ()))
    }
  }

  test("Parquet projection returns equal sum while reading fewer API bytes") {
    temp { dir =>
      val path = dir.resolve("wide.parquet")
      val data = Order.generated(1000).map(o => o.copy(note = Some(s"${o.id}-" + "x" * 1000))).toVector
      ParquetOrders.write(path, data.iterator, compressed = false)
      val full = CountingInputFile(LocalInputFile(path))
      val projected = CountingInputFile(LocalInputFile(path))
      val digest = Digest.of(consume => ParquetOrders.scanFile(full, consume))
      assertEquals(ParquetOrders.sumFile(projected), digest.amount)
      assertEquals(digest.count, 1000L)
      assert(projected.bytesRead < full.bytesRead / 2,
        s"projection read ${projected.bytesRead}, full read ${full.bytesRead}")
    }
  }

  test("all format variants preserve an empty dataset") {
    temp { dir =>
      Format.values.foreach { format =>
        val path = dir.resolve(format.fileName)
        format.write(path, Iterator.empty)
        assertEquals(Digest.of(consume => format.scan(path, consume)), Digest(0, 0, 0))
        assertEquals(format.sum(path), 0L)
      }
    }
  }

  test("all writers reject sub-millisecond precision instead of truncating") {
    temp { dir =>
      Format.values.foreach { format =>
        intercept[IllegalArgumentException] {
          format.write(dir.resolve(format.fileName), Iterator.single(rows.head.copy(
            occurredAt = Instant.parse("2026-01-01T00:00:00.000001Z"))))
        }
      }
    }
  }
