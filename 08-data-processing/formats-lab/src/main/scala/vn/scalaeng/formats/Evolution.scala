package vn.scalaeng.formats

import java.nio.file.{Files, Path}
import org.apache.avro.{Schema, SchemaCompatibility}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import scala.jdk.CollectionConverters.*
import scala.util.{Try, Using}

object Evolution:
  def schema(fields: String): Schema = Schema.Parser().parse(
    s"""{"type":"record","name":"Event","namespace":"vn.scalaeng.formats.evolution","fields":[$fields]}""")

  val v1: Schema = schema("""{"name":"id","type":"int"},{"name":"label","type":"string"}""")
  val added: Schema = schema("""{"name":"id","type":"int"},{"name":"label","type":"string"},
    {"name":"source","type":"string","default":"web"}""")
  val required: Schema = schema("""{"name":"id","type":"int"},{"name":"label","type":"string"},
    {"name":"source","type":"string"}""")
  val removed: Schema = schema("""{"name":"id","type":"int"}""")
  val widened: Schema = schema("""{"name":"id","type":"long"},{"name":"label","type":"string"}""")
  val changed: Schema = schema("""{"name":"id","type":"string"},{"name":"label","type":"string"}""")
  val renamed: Schema = schema("""{"name":"id","type":"int"},
    {"name":"name","type":"string","aliases":["label"]}""")
  val renameWithoutAlias: Schema = schema("""{"name":"id","type":"int"},{"name":"name","type":"string"}""")
  val nullable: Schema = schema("""{"name":"id","type":"int"},{"name":"label","type":["null","string"],"default":null}""")
  val cases: List[(String, Schema)] = List("add-default" -> added, "add-required" -> required,
    "remove" -> removed, "int-long" -> widened, "int-string" -> changed,
    "rename-alias" -> renamed, "rename-no-alias" -> renameWithoutAlias, "nullable" -> nullable)

  def sample(s: Schema, nullLabel: Boolean = false): GenericRecord =
    val record = GenericData.Record(s)
    s.getFields.asScala.foreach { field =>
      val value: AnyRef = field.name() match
        case "id" if field.schema().getType == Schema.Type.LONG => java.lang.Long.valueOf(7L)
        case "id" if field.schema().getType == Schema.Type.STRING => "7"
        case "id" => java.lang.Integer.valueOf(7)
        case "label" if nullLabel => null
        case "label" | "name" => "paid"
        case "source" => "mobile"
        case other => throw IllegalArgumentException(other)
      record.put(field.name(), value)
    }
    record

  def write(path: Path, writerSchema: Schema, record: GenericRecord): Unit =
    Using.resource(DataFileWriter[GenericRecord](GenericDatumWriter[GenericRecord](writerSchema))) { writer =>
      writer.create(writerSchema, path.toFile)
      writer.append(record)
    }

  def read(path: Path, readerSchema: Schema): GenericRecord =
    Using.resource(DataFileReader[GenericRecord](path.toFile, GenericDatumReader[GenericRecord](null, readerSchema)))(_.next())

  def compatible(reader: Schema, writer: Schema): Boolean =
    SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType ==
      SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE

  def attempt(path: Path, writer: Schema, reader: Schema): String =
    write(path, writer, sample(writer, nullLabel = writer == nullable))
    Try(read(path, reader)).fold(e => s"${e.getClass.getSimpleName}: ${e.getMessage}", _.toString)

object EvolutionMain:
  def main(args: Array[String]): Unit =
    require(args.length == 1, "usage: EvolutionMain <output-directory>")
    val dir = Files.createDirectories(Path.of(args(0)))
    Evolution.cases.foreach { (label, v2) =>
      println(s"$label backward=${Evolution.compatible(v2, Evolution.v1)} forward=${Evolution.compatible(Evolution.v1, v2)}")
      println(s"  v1->v2 ${Evolution.attempt(dir.resolve(s"$label-v1.avro"), Evolution.v1, v2)}")
      println(s"  v2->v1 ${Evolution.attempt(dir.resolve(s"$label-v2.avro"), v2, Evolution.v1)}")
    }
