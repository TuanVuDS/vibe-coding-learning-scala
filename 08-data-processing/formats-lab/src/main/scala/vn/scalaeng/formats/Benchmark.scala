package vn.scalaeng.formats

import java.nio.file.{Files, Path}
import java.util.Locale
import scala.collection.mutable
import scala.util.Random

enum Format(val fileName: String):
  case Json extends Format("orders.jsonl")
  case JsonGzip extends Format("orders.jsonl.gz")
  case Csv extends Format("orders.csv")
  case CsvGzip extends Format("orders.csv.gz")
  case Avro extends Format("orders.avro")
  case AvroDeflate extends Format("orders.deflate.avro")
  case Parquet extends Format("orders.parquet")
  case ParquetGzip extends Format("orders.gzip.parquet")

  def write(path: Path, rows: Iterator[Order]): Unit = this match
    case Json | JsonGzip => TextOrders.writeJson(path, rows, this == JsonGzip)
    case Csv | CsvGzip => TextOrders.writeCsv(path, rows, this == CsvGzip)
    case Avro | AvroDeflate => AvroOrders.write(path, rows, this == AvroDeflate)
    case Parquet | ParquetGzip => ParquetOrders.write(path, rows, this == ParquetGzip)

  def scan(path: Path, consume: Order => Unit): Unit = this match
    case Json | JsonGzip => TextOrders.scanJson(path, this == JsonGzip, consume)
    case Csv | CsvGzip => TextOrders.scanCsv(path, this == CsvGzip, consume)
    case Avro | AvroDeflate => AvroOrders.scan(path, consume)
    case Parquet | ParquetGzip => ParquetOrders.scan(path, consume)

  def sum(path: Path): Long = this match
    case Json | JsonGzip => TextOrders.sumJson(path, this == JsonGzip)
    case Csv | CsvGzip => TextOrders.sumCsv(path, this == CsvGzip)
    case Avro | AvroDeflate => AvroOrders.sum(path)
    case Parquet | ParquetGzip => ParquetOrders.sum(path)

final case class Digest(count: Long, amount: Long, hash: Long)
object Digest:
  def of(scan: (Order => Unit) => Unit): Digest =
    var count = 0L
    var amount = 0L
    var hash = 0L
    scan { o =>
      count += 1
      amount += o.amountMinor
      hash = hash * 31L + o.hashCode().toLong // Intentional wraparound, order-sensitive checksum.
    }
    Digest(count, amount, hash)

object Benchmark:
  private def timed[A](f: => A): (A, Double) =
    val start = System.nanoTime()
    val result = f
    (result, (System.nanoTime() - start).toDouble / 1000000)

  private def number(n: Double): String = String.format(Locale.ROOT, "%.3f", Double.box(n))
  private def median(values: Seq[Double]): Double = values.sorted.apply(values.size / 2)

  def main(args: Array[String]): Unit =
    require(args.length == 3, "usage: Benchmark <output-directory> <rows> <odd-rounds>")
    val n = args(1).toInt
    val rounds = args(2).toInt
    require(n > 0 && n < Int.MaxValue && rounds > 0 && rounds % 2 == 1)
    val dir = Files.createDirectories(Path.of(args(0)))
    val expected = Digest.of(consume => Order.generated(n).foreach(consume))
    println(s"os=${System.getProperty("os.name")} ${System.getProperty("os.version")} ${System.getProperty("os.arch")}")
    println(s"java=${System.getProperty("java.runtime.version")} vm=${System.getProperty("java.vm.name")}")
    println(s"processors=${Runtime.getRuntime.availableProcessors()} maxHeapBytes=${Runtime.getRuntime.maxMemory()}")
    println(s"rows=$n measuredRounds=$rounds warmupRows=${math.min(n, 10000)} expected=$expected")
    // Small unmeasured round loads classes and codecs for every format.
    Format.values.foreach { f =>
      val path = dir.resolve(f.fileName)
      f.write(path, Order.generated(math.min(n, 10000)))
      val warm = Digest.of(consume => f.scan(path, consume))
      require(f.sum(path) == warm.amount)
    }
    val results = mutable.Map.empty[Format, Vector[(Long, Double, Double, Double)]]
    val raw = new StringBuilder("round,format,bytes,write_ms,full_read_ms,amount_read_ms\n")
    for round <- 1 to rounds do
      // Fixed seed, different ordering per round; only one format executes at a time.
      Random(2100L + round).shuffle(Format.values.toList).foreach { f =>
        val path = dir.resolve(f.fileName)
        val (_, writeMs) = timed(f.write(path, Order.generated(n)))
        val (digest, fullMs) = timed(Digest.of(consume => f.scan(path, consume)))
        require(digest == expected, s"full read mismatch: $f $digest != $expected")
        val (amount, columnMs) = timed(f.sum(path))
        require(amount == expected.amount, s"column read mismatch: $f")
        val bytes = Files.size(path)
        results.update(f, results.getOrElse(f, Vector.empty) :+ (bytes, writeMs, fullMs, columnMs))
        val line = s"$round,$f,$bytes,${number(writeMs)},${number(fullMs)},${number(columnMs)}"
        raw.append(line).append('\n')
        println(line)
      }
    Files.writeString(dir.resolve("raw.csv"), raw.toString)
    val table = new StringBuilder("| Format | Bytes (last round) | Write ms (median) | Full read ms (median) | Amount read ms (median) |\n")
      .append("|---|---:|---:|---:|---:|\n")
    Format.values.foreach { f =>
      val samples = results(f)
      table.append(s"| $f | ${samples.last._1} | ${number(median(samples.map(_._2)))} | ${number(median(samples.map(_._3)))} | ${number(median(samples.map(_._4)))} |\n")
    }
    Files.writeString(dir.resolve("summary.md"), table.toString)
    println(table.toString)

object Demo:
  def main(args: Array[String]): Unit =
    require(args.length == 1, "usage: Demo <output-directory>")
    val dir = Files.createDirectories(Path.of(args(0)))
    val rows = Order.generated(3).toVector
    Format.values.foreach { f =>
      val path = dir.resolve(f.fileName)
      f.write(path, rows.iterator)
      val actual = Vector.newBuilder[Order]
      f.scan(path, actual += _)
      require(actual.result() == rows)
      println(s"$f roundTrip=true amountMinor=${f.sum(path)}")
    }
