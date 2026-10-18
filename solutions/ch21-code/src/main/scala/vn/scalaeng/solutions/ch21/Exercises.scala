package vn.scalaeng.solutions.ch21

import io.circe.{Json, parser}
import java.math.RoundingMode
import java.nio.file.{Files, Path}
import java.time.Instant
import org.apache.avro.{Conversions as AvroConversions, LogicalTypes, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.io.Source
import scala.util.{Try, Using}
import vn.scalaeng.formats.{Conversions, Evolution, Order, ParquetOrders}

object Money:
  def parse(text: String, currency: String): Either[String, Long] =
    for
      scale <- Map("USD" -> 2, "VND" -> 0).get(currency).toRight("unsupported currency")
      _ <- Either.cond(text.matches("[0-9]+(?:\\.[0-9]+)?"), (), "expected unsigned decimal")
      amount <- Conversions.minor(text, scale)
    yield amount

enum Presence:
  case Missing, ExplicitNull
  case Value(text: String)

object Presence:
  def note(json: Json): Either[String, Presence] =
    json.asObject.toRight("expected JSON object").flatMap { obj =>
      obj("note") match
        case None => Right(Missing)
        case Some(value) if value.isNull => Right(ExplicitNull)
        case Some(value) => value.asString.map(Value.apply).toRight("note must be string or null")
    }

object DecimalWire:
  val logical: LogicalTypes.Decimal = LogicalTypes.decimal(12, 2)
  val schema: Schema = logical.addToSchema(Schema.create(Schema.Type.BYTES))
  private val codec = AvroConversions.DecimalConversion()

  def roundTrip(text: String): Either[String, BigDecimal] =
    Try {
      val value = java.math.BigDecimal(text).setScale(2, RoundingMode.UNNECESSARY)
      require(value.precision() <= 12, "precision exceeds 12")
      val bytes = codec.toBytes(value, schema, logical)
      BigDecimal(codec.fromBytes(bytes, schema, logical))
    }.toEither.left.map(_.getMessage)

object TimestampWire:
  def parse(text: String): Either[String, Instant] =
    Conversions.instant(text).flatMap { instant =>
      Either.cond(instant.getNano % 1000000 == 0, instant, "timestamp loses precision in millis")
    }

object RenameBridge:
  val schema: Schema = Evolution.schema("""{"name":"id","type":"int"},
    {"name":"label","type":"string"},{"name":"name","type":"string","default":""}""")

  def upgrade(old: GenericRecord): GenericRecord =
    val result = GenericData.Record(schema)
    result.put("id", old.get("id"))
    result.put("label", old.get("label"))
    result.put("name", old.get("label"))
    result

final case class MigrationReport(accepted: Long, rejected: Long)
object Quarantine:
  /** A small streaming migration: no Docker, network, or in-memory rejected-row collection. */
  def migrate(input: Path, parquet: Path, rejected: Path): MigrationReport =
    require(Set(input.toAbsolutePath.normalize(), parquet.toAbsolutePath.normalize(), rejected.toAbsolutePath.normalize()).size == 3,
      "input, parquet and rejected paths must differ")
    Using.Manager { use =>
      val source = use(Source.fromFile(input.toFile, "UTF-8"))
      val rejects = use(Files.newBufferedWriter(rejected))
      var accepted = 0L
      var rejectedCount = 0L
      val rows = source.getLines().zipWithIndex.flatMap { (raw, index) =>
        val decoded = parser.decode[Order](raw).left.map(e => List(e.getMessage)).flatMap(Order.validate)
        decoded match
          case Right(order) =>
            accepted += 1
            Some(order)
          case Left(errors) =>
            rejectedCount += 1
            val json = Json.obj("line" -> Json.fromLong(index.toLong + 1), "raw" -> Json.fromString(raw),
              "errors" -> Json.arr(errors.map(Json.fromString)*))
            rejects.write(json.noSpaces)
            rejects.newLine()
            None
      }
      ParquetOrders.write(parquet, rows, compressed = true)
      MigrationReport(accepted, rejectedCount)
    }.get
