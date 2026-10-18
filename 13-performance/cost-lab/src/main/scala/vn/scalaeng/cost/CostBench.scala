package vn.scalaeng.cost

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Locale
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.concurrent.locks.LockSupport
import scala.jdk.CollectionConverters.*

object CostBench:
  @volatile private var sink = 0L
  @volatile private var cacheSink = Array.emptyLongArray
  private val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
  private val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
  private def gcCount: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionCount.max(0)).sum
  private def gcMs: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime.max(0)).sum
  private def allocated: Long = threads.getThreadAllocatedBytes(Thread.currentThread().threadId())
  private def fmt(n: Double): String = String.format(Locale.ROOT, "%.6f", Double.box(n))
  private def sum(xs: Array[Long]): Long = xs.foldLeft(0L)(_ ^ _)

  private final case class Sample(name: String, round: Int, events: Int, ns: Long, cpuNs: Long,
      allocatedBytes: Long, collections: Long, collectionMs: Long, checksum: Long)

  private def measure(name: String, round: Int, events: Int)(f: () => Long): Sample =
    val gc0 = gcCount
    val gcTime0 = gcMs
    val alloc0 = allocated
    val cpu0 = os.getProcessCpuTime
    val start = System.nanoTime()
    val value = f()
    val ns = System.nanoTime() - start
    val cpu = os.getProcessCpuTime - cpu0
    val bytes = allocated - alloc0
    sink = value
    Sample(name, round, events, ns, cpu, bytes, gcCount - gc0, gcMs - gcTime0, value)

  private def samples(out: Path, name: String, cases: Vector[(String, () => Long)], events: Int,
      warmup: Int = 12, rounds: Int = 21): Unit =
    // Lượt đầu trước warm-up là chẩn đoán, không gộp vào median.
    cases.foreach { case (label, f) =>
      val first = measure(label, -1, events)(f)
      println(s"FIRST $label ms=${fmt(first.ns / 1e6)} checksum=${first.checksum}")
    }
    (0 until warmup).foreach { round =>
      val ordered = cases.drop(round % cases.size) ++ cases.take(round % cases.size)
      ordered.foreach { case (_, f) => sink = f() }
    }
    val raw = (0 until rounds).toVector.flatMap { round =>
      val ordered = cases.drop(round % cases.size) ++ cases.take(round % cases.size)
      ordered.map { case (label, f) => measure(label, round, events)(f) }
    }
    val header = "case,round,events,wall_ns,process_cpu_ns,main_thread_allocated_bytes,gc_count,gc_ms,checksum\n"
    Files.writeString(out.resolve(s"$name.csv"), header + raw.map { s =>
      s"${s.name},${s.round},${s.events},${s.ns},${s.cpuNs},${s.allocatedBytes},${s.collections},${s.collectionMs},${s.checksum}"
    }.mkString("", "\n", "\n"), UTF_8)
    cases.foreach { case (label, _) =>
      val data = raw.filter(_.name == label)
      val ms = data.map(_.ns / 1e6)
      val p50 = Models.percentile(ms, .50)
      val rate = events * 1000 / p50
      val price = Models.costPerMillion(rate, 1, BigDecimal("0.12"))
      println(s"BENCH $label rounds=$rounds p50Ms=${fmt(p50)} p95Ms=${fmt(Models.percentile(ms, .95))} p99Ms=${fmt(Models.percentile(ms, .99))} eventsPerSec=${fmt(rate)} costPerMillion=${price.setScale(8, BigDecimal.RoundingMode.HALF_UP)} mainAllocMedian=${Models.percentile(data.map(_.allocatedBytes.toDouble), .5).toLong} cpuMedianMs=${fmt(Models.percentile(data.map(_.cpuNs / 1e6), .5))} gcCount=${data.map(_.collections).sum} gcMs=${data.map(_.collectionMs).sum}")
    }

  private def environment(): Unit =
    println(s"ENV java=${System.getProperty("java.version")} vendor=${System.getProperty("java.vendor")} scala=${scala.util.Properties.versionNumberString} os=${System.getProperty("os.name")} arch=${System.getProperty("os.arch")} cpu=${Runtime.getRuntime.availableProcessors()} physicalRamBytes=${os.getTotalMemorySize} heapMaxBytes=${Runtime.getRuntime.maxMemory()}")
    println(s"JVM ${ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.mkString(" ")}")
    println(s"GC ${ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getName).mkString(",")}")

  private def localBench(out: Path): Unit =
    val rows = Vector.tabulate(12000)(Orders.fixture)
    val json = Orders.json(rows)
    val avro = Orders.avro(rows)
    require(Orders.readJson(json) == rows && Orders.readAvro(avro) == rows)
    require(Orders.query(rows, false) == Orders.query(rows, true))
    println(s"PAYLOAD events=${rows.size} jsonBytes=${json.length} avroBytes=${avro.length} framing=jsonl-vs-count+raw-avro schemaOutsideAvro=true")
    samples(out, "serialization", Vector(
      "json-encode" -> (() => Orders.json(rows).length.toLong),
      "avro-encode" -> (() => Orders.avro(rows).length.toLong),
      "json-decode" -> (() => Orders.readJson(json).map(_.amountMinor).sum),
      "avro-decode" -> (() => Orders.readAvro(avro).map(_.amountMinor).sum)
    ), rows.size)
    samples(out, "query", Vector(
      "filter-late" -> (() => Orders.query(rows, false)),
      "filter-early" -> (() => Orders.query(rows, true))
    ), rows.size)
    val cached = rows.map(r => Orders.work(r.id + r.amountMinor, 512)).toArray
    val expected = sum(cached)
    val uncached = () => rows.iterator.map(r => Orders.work(r.id + r.amountMinor, 512)).foldLeft(0L)(_ ^ _)
    require(uncached() == expected)
    samples(out, "cache", Vector(
      "cache-build" -> (() => {
        val result = rows.map(r => Orders.work(r.id + r.amountMinor, 512)).toArray
        cacheSink = result
        result.length.toLong ^ result.last
      }),
      "cache-read" -> (() => sum(cached)),
      "no-cache" -> uncached
    ), rows.size)
    println(s"CACHE payloadLongBytes=${cached.length.toLong * 8} expected=$expected sinkLength=${cacheSink.length}")

  private def cpuBench(out: Path): Unit =
    val count = 60000
    val pools = Vector(1, 2, 4).map(n => n -> Executors.newFixedThreadPool(n)).toMap
    def run(workers: Int, partitions: Int, skew: Boolean): Long =
      val sizes = if skew then
        Vector(count * 8 / 10) ++ Vector.tabulate(partitions - 1) { i =>
          val rest = count - count * 8 / 10
          rest / (partitions - 1) + (if i < rest % (partitions - 1) then 1 else 0)
        }
      else Vector.tabulate(partitions)(i => count / partitions + (if i < count % partitions then 1 else 0))
      val offsets = sizes.scanLeft(0)(_ + _)
      val tasks = sizes.indices.map { index =>
        new Callable[Long]:
          def call(): Long =
            // Overhead CPU có chủ đích cho mỗi task, vẫn dùng kết quả của nó.
            val overhead = Orders.work(index.toLong, 12000)
            var checksum = 0L
            var i = offsets(index)
            while i < offsets(index + 1) do
              checksum ^= Orders.work(i.toLong, 256)
              i += 1
            checksum ^ overhead
      }
      val actual = pools(workers).invokeAll(tasks.asJava).asScala.foldLeft(0L)((a, f) => a ^ f.get())
      val overhead = sizes.indices.foldLeft(0L)((a, i) => a ^ Orders.work(i.toLong, 12000))
      actual ^ overhead
    val configs = Vector((1, 8, false), (2, 8, false), (4, 8, false), (4, 2, false), (4, 128, false), (4, 8, true))
    try
      val expected = (0 until count).foldLeft(0L)((a, i) => a ^ Orders.work(i.toLong, 256))
      configs.foreach { case (w, p, s) => require(run(w, p, s) == expected) }
      samples(out, "cpu", configs.map { case (w, p, s) =>
        s"workers$w-parts$p-skew$s" -> (() => run(w, p, s))
      }, count)
      println(s"CPU expected=$expected events=$count steps=256 fixedStepsPerTask=12000 skewHotFraction=0.8")
    finally
      pools.values.foreach(_.shutdown())
      pools.values.foreach { pool =>
        if !pool.awaitTermination(10, TimeUnit.SECONDS) then
          pool.shutdownNow()
          require(pool.awaitTermination(10, TimeUnit.SECONDS), "worker pool did not terminate")
      }

  private def batchBench(out: Path): Unit =
    val count = 2000
    val gapNs = 100000L
    val waitNs = 5000000L
    def serve(from: Int, until: Int): Long =
      var value = Orders.work(from.toLong, 12000)
      var i = from
      while i < until do
        value ^= Orders.work(i.toLong, 128)
        i += 1
      value
    val sizes = Vector(1, 16, 128)
    samples(out, "batch-service", sizes.map { size =>
      s"batch$size" -> (() => {
        var checksum = 0L
        (0 until count by size).foreach(start => checksum ^= serve(start, (start + size).min(count)))
        checksum
      })
    }, count)
    val raw = new StringBuilder("batch,round,event,scheduled_ns,completed_ns,latency_ns\n")
    val totals = new StringBuilder("batch,round,events,elapsed_ns\n")
    // Nguồn open-loop theo lịch cố định; trễ scheduler cũng được tính, không bỏ event.
    def paced(size: Int, round: Int): Vector[Double] =
      val epoch = System.nanoTime()
      val completions = new Array[Long](count)
      Models.batchRanges(count, size, gapNs, waitNs).foreach { case (from, until) =>
        val flush = epoch + (until - 1).toLong * gapNs
        var remaining = flush - System.nanoTime()
        while remaining > 0 do
          LockSupport.parkNanos(remaining)
          remaining = flush - System.nanoTime()
        sink = serve(from, until)
        val done = System.nanoTime() - epoch
        (from until until).foreach { i =>
          completions(i) = done
        }
      }
      val elapsed = System.nanoTime() - epoch
      if round >= 0 then totals.append(s"$size,$round,$count,$elapsed\n")
      completions.indices.toVector.map { i =>
        val scheduled = i.toLong * gapNs
        val latency = completions(i) - scheduled
        if round >= 0 then raw.append(s"$size,$round,$i,$scheduled,${completions(i)},$latency\n")
        latency / 1e6
      }
    (0 until 3).foreach(_ => sizes.foreach(size => paced(size, -1)))
    val data = (0 until 9).toVector.flatMap { round =>
      (sizes.drop(round % sizes.size) ++ sizes.take(round % sizes.size)).map(size => size -> paced(size, round))
    }
    Files.writeString(out.resolve("event-latency.csv"), raw.toString, UTF_8)
    Files.writeString(out.resolve("event-totals.csv"), totals.toString, UTF_8)
    sizes.foreach { size =>
      val ms = data.filter(_._1 == size).flatMap(_._2)
      println(s"LATENCY batch=$size arrivalsPerSec=${1e9 / gapNs} maxWaitMs=${waitNs / 1e6} rounds=9 events=${ms.size} p50Ms=${fmt(Models.percentile(ms, .5))} p95Ms=${fmt(Models.percentile(ms, .95))} p99Ms=${fmt(Models.percentile(ms, .99))} meanMs=${fmt(ms.sum / ms.size)} maxMs=${fmt(ms.max)}")
    }

  private def traps(out: Path): Unit =
    // Kết quả discarded có thể bị JIT loại bỏ. Có/không loại bỏ chỉ là giả thuyết khi chưa xem assembly.
    val inputs = Array.tabulate(200000)(i => i.toLong + 17)
    samples(out, "traps", Vector(
      "discarded-result" -> (() => {
        var i = 0
        while i < inputs.length do
          Orders.work(inputs(i), 32)
          i += 1
        0L
      }),
      "consumed-result" -> (() => {
        var checksum = 0L
        var i = 0
        while i < inputs.length do
          checksum ^= Orders.work(inputs(i), 32)
          i += 1
        checksum
      })
    ), inputs.length)

  def main(args: Array[String]): Unit =
    require(args.length == 2, "usage: CostBench <local|cpu|batch|traps> <output-directory>")
    val out = Path.of(args(1))
    Files.createDirectories(out)
    environment()
    println("METHOD warmup=12 rounds=21 rotated-order=true timing=wall-clock percentiles=nearest-rank pricePerNodeHour=0.12-assumed no-sockets=true")
    args(0) match
      case "local" => localBench(out)
      case "cpu" => cpuBench(out)
      case "batch" => batchBench(out)
      case "traps" => traps(out)
      case _ => throw IllegalArgumentException("unknown benchmark mode")
    println(s"SINK $sink")
