package vn.scalaeng.formats

import java.nio.file.Path
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{InputFile, LocalInputFile, LocalOutputFile}
import scala.util.Using

/** Java API called from Scala 3; local NIO paths bypass Hadoop FileSystem / winutils. */
object ParquetOrders:
  def write(path: Path, rows: Iterator[Order], compressed: Boolean): Unit =
    val builder = AvroParquetWriter.builder[GenericRecord](LocalOutputFile(path))
      .withConf(PlainParquetConfiguration())
      .withSchema(AvroOrders.schema)
      .withDataModel(GenericData.get())
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withRowGroupSize(8L * 1024 * 1024)
      .withPageSize(64 * 1024)
      .withCompressionCodec(if compressed then CompressionCodecName.GZIP else CompressionCodecName.UNCOMPRESSED)
    Using.resource(builder.build()) { writer =>
      rows.foreach(o => writer.write(AvroOrders.record(Order.checked(o))))
    }

  def scan(path: Path, consume: Order => Unit): Unit =
    scanFile(LocalInputFile(path), consume)

  private[formats] def scanFile(file: InputFile, consume: Order => Unit): Unit =
    Using.resource(AvroParquetReader.builder[GenericRecord](file, PlainParquetConfiguration())
      .withDataModel(GenericData.get()).build()) { reader =>
      var record = reader.read()
      while record != null do
        consume(AvroOrders.order(record))
        record = reader.read()
    }

  def sum(path: Path): Long =
    sumFile(LocalInputFile(path))

  private[formats] def sumFile(file: InputFile): Long =
    val conf = PlainParquetConfiguration()
    val projection = """{"type":"record","name":"Order","namespace":"vn.scalaeng.formats.wire",
      "fields":[{"name":"amountMinor","type":"long"}]}"""
    conf.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, projection)
    conf.set("parquet.avro.read.schema", projection)
    Using.resource(AvroParquetReader.builder[GenericRecord](file, conf)
      .withDataModel(GenericData.get()).build()) { reader =>
      var total = 0L
      var record = reader.read()
      while record != null do
        total += record.get("amountMinor").asInstanceOf[Long]
        record = reader.read()
      total
    }
