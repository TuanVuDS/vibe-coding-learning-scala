package vn.scalaeng.cost

import io.circe.Json
import io.circe.parser.parse
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

final case class Order(id: Long, userId: String, amountMinor: Long, note: String)

object Orders:
  def fixture(i: Int): Order = Order(i.toLong, s"U-${i % 1000}", 100L + i % 10000, s"đơn-$i-" + "x" * 48)

  def json(rows: Vector[Order]): Array[Byte] = rows.map { row =>
    Json.obj("id" -> Json.fromLong(row.id), "userId" -> Json.fromString(row.userId),
      "amountMinor" -> Json.fromLong(row.amountMinor), "note" -> Json.fromString(row.note)).noSpaces
  }.mkString("", "\n", if rows.isEmpty then "" else "\n").getBytes(UTF_8)

  def readJson(bytes: Array[Byte]): Vector[Order] = String(bytes, UTF_8).linesIterator.map { line =>
    val c = parse(line).fold(throw _, identity).hcursor
    Order(c.get[Long]("id").fold(throw _, identity), c.get[String]("userId").fold(throw _, identity),
      c.get[Long]("amountMinor").fold(throw _, identity), c.get[String]("note").fold(throw _, identity))
  }.toVector

  val schema: Schema = Schema.Parser().parse("""{"type":"record","name":"Order","namespace":"vn.scalaeng.cost.wire",
    "fields":[{"name":"id","type":"long"},{"name":"userId","type":"string"},
    {"name":"amountMinor","type":"long"},{"name":"note","type":"string"}]}""")

  // Đóng gói của lab: count 4 byte, rồi Avro nhị phân thô; schema dùng chung ngoài payload.
  def avro(rows: Vector[Order]): Array[Byte] =
    val out = ByteArrayOutputStream()
    val header = DataOutputStream(out)
    header.writeInt(rows.size)
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = GenericDatumWriter[GenericRecord](schema)
    rows.foreach { row =>
      val record = GenericData.Record(schema)
      record.put("id", row.id)
      record.put("userId", row.userId)
      record.put("amountMinor", row.amountMinor)
      record.put("note", row.note)
      writer.write(record, encoder)
    }
    encoder.flush()
    val bytes = out.toByteArray
    header.close()
    bytes

  def readAvro(bytes: Array[Byte]): Vector[Order] =
    val in = ByteArrayInputStream(bytes)
    val header = DataInputStream(in)
    val count = header.readInt()
    require(count >= 0 && count <= bytes.length, "invalid record count")
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    val reader = GenericDatumReader[GenericRecord](schema)
    val result = Vector.fill(count) {
      val r = reader.read(null, decoder)
      Order(r.get("id").asInstanceOf[Long], r.get("userId").toString,
        r.get("amountMinor").asInstanceOf[Long], r.get("note").toString)
    }
    header.close()
    result

  /** CPU checksum modulo 2^64; tuyệt đối không dùng để tính tiền. */
  def work(seed: Long, steps: Int): Long =
    var x = seed
    var i = 0
    while i < steps do
      x = (x ^ (x >>> 13)) * 6364136223846793005L + 1442695040888963407L
      i += 1
    x

  def query(rows: Vector[Order], earlyFilter: Boolean): Long =
    rows.iterator.map { row =>
      if earlyFilter && row.id % 100 != 0 then 0L
      else
        val value = work(row.id + row.amountMinor, 512)
        if row.id % 100 == 0 then value else 0L
    }.foldLeft(0L)(_ ^ _)
