package vn.scalaeng.formats

import java.nio.file.Path
import java.time.Instant
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import scala.util.Using

object AvroOrders:
  val schema: Schema = Schema.Parser().parse("""{
    "type":"record","name":"Order","namespace":"vn.scalaeng.formats.wire",
    "fields":[
      {"name":"id","type":"long"},
      {"name":"userId","type":"string"},
      {"name":"amountMinor","type":"long"},
      {"name":"currency","type":"string"},
      {"name":"occurredAt","type":{"type":"long","logicalType":"timestamp-millis"}},
      {"name":"note","type":["null","string"],"default":null}
    ]} """)

  def record(order: Order): GenericRecord =
    val r = GenericData.Record(schema)
    r.put("id", order.id)
    r.put("userId", order.userId)
    r.put("amountMinor", order.amountMinor)
    r.put("currency", order.currency)
    r.put("occurredAt", order.occurredAt.toEpochMilli)
    r.put("note", order.note.orNull)
    r

  def order(r: GenericRecord): Order = Order.checked(Order(
    r.get("id").asInstanceOf[Long], r.get("userId").toString,
    r.get("amountMinor").asInstanceOf[Long], r.get("currency").toString,
    Instant.ofEpochMilli(r.get("occurredAt").asInstanceOf[Long]),
    Option(r.get("note")).map(_.toString)
  ))

  def write(path: Path, rows: Iterator[Order], compressed: Boolean): Unit =
    Using.resource(DataFileWriter[GenericRecord](GenericDatumWriter[GenericRecord](schema))) { writer =>
      writer.setCodec(if compressed then CodecFactory.deflateCodec(6) else CodecFactory.nullCodec())
      writer.create(schema, path.toFile)
      rows.foreach(o => writer.append(record(Order.checked(o))))
    }

  def scan(path: Path, consume: Order => Unit): Unit =
    Using.resource(DataFileReader[GenericRecord](path.toFile, GenericDatumReader[GenericRecord](schema))) { reader =>
      while reader.hasNext do consume(order(reader.next()))
    }

  /** Projection avoids building other fields, but the row-oriented file is still scanned. */
  def sum(path: Path): Long =
    val readerSchema = Schema.Parser().parse("""{"type":"record","name":"Order",
      "namespace":"vn.scalaeng.formats.wire","fields":[{"name":"amountMinor","type":"long"}]}""")
    Using.resource(DataFileReader[GenericRecord](path.toFile,
      GenericDatumReader[GenericRecord](null, readerSchema))) { reader =>
      var total = 0L
      while reader.hasNext do total += reader.next().get("amountMinor").asInstanceOf[Long]
      total
    }
