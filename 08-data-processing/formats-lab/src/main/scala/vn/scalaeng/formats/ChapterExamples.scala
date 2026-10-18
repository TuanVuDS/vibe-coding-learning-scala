package vn.scalaeng.formats

import java.nio.file.{Files, Path}
import java.time.Instant
import org.apache.avro.generic.GenericRecord

/** Small executable examples quoted verbatim in chapter 21. */
object ChapterExamples:
  def textRoundTrip(dir: Path): Unit =
    // Each pass writes one Order, including absent, empty and literal sentinel notes.
    for note <- List(None, Some(""), Some("\\N")) do
      val order = Order(1, "U-1", 1230, "USD", Instant.EPOCH, note)
      val json = dir.resolve("one.jsonl")
      TextOrders.writeJson(json, Iterator.single(order), gzip = false)
      val jsonRows = Vector.newBuilder[Order]
      TextOrders.scanJson(json, gzip = false, consume = o => { jsonRows += o; () })
      require(jsonRows.result() == Vector(order))
      val csv = dir.resolve("one.csv")
      TextOrders.writeCsv(csv, Iterator.single(order), gzip = false)
      val csvRows = Vector.newBuilder[Order]
      TextOrders.scanCsv(csv, gzip = false, consume = o => { csvRows += o; () })
      require(csvRows.result() == Vector(order))

  def readOldWithNew(dir: Path): GenericRecord =
    val path = dir.resolve("old.avro")
    Evolution.write(path, Evolution.v1, Evolution.sample(Evolution.v1))
    require(Evolution.compatible(reader = Evolution.added, writer = Evolution.v1))
    val upgraded = Evolution.read(path, readerSchema = Evolution.added)
    require(upgraded.get("source").toString == "web")
    upgraded

  def main(args: Array[String]): Unit =
    require(args.length == 1, "usage: ChapterExamples <output-directory>")
    val dir = Files.createDirectories(Path.of(args(0)))
    println(s"os=${System.getProperty("os.name")}")
    println(s"HADOOP_HOME=${Option(System.getenv("HADOOP_HOME")).getOrElse("<unset>")}")
    println(s"hadoop.home.dir=${Option(System.getProperty("hadoop.home.dir")).getOrElse("<unset>")}")
    textRoundTrip(dir)
    println("JSONL/CSV: absent, empty and literal \\N preserved")
    println(s"v1->v2 ${readOldWithNew(dir)}")
    val schemas = List("v1" -> Evolution.v1, "v2-default" -> Evolution.added,
      "v2-alias" -> Evolution.renamed, "v2-nullable" -> Evolution.nullable)
    schemas.foreach { (name, schema) =>
      Files.writeString(dir.resolve(s"$name.avsc"), schema.toString + "\n")
    }
    Demo.main(args)
