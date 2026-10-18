package vn.scalaeng.cost

object MemoryProbe:
  @volatile private var held = Array.empty[Order]
  @volatile private var sink = 0L
  private def used: Long = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
  private def afterGc(): Long =
    System.gc()
    Thread.sleep(100)
    System.gc()
    Thread.sleep(100)
    used

  // Tách frame tạo iterator khỏi frame gọi GC: local tạm có thể vẫn là GC root.
  @scala.noinline private def materialize(count: Int): Long =
    held = Array.tabulate(count)(Orders.fixture)
    held.iterator.map(_.amountMinor).sum

  @scala.noinline private def stream(count: Int): Long =
    (0 until count).iterator.map(i => Orders.fixture(i).amountMinor).sum

  def main(args: Array[String]): Unit =
    val count = 300000
    sink = Orders.fixture(0).amountMinor
    val before = afterGc()
    sink = materialize(count)
    val kept = afterGc()
    println(s"MEMORY rows=$count beforeBytes=$before heldBytes=$kept deltaBytes=${kept - before} bytesPerRow=${(kept - before).toDouble / count} sum=$sink")
    held = Array.empty[Order]
    val released = afterGc()
    sink = stream(count)
    val streamed = afterGc()
    println(s"STREAM releasedBytes=$released afterGcBytes=$streamed sum=$sink heapMaxBytes=${Runtime.getRuntime.maxMemory()}")

object ModelDemo:
  def main(args: Array[String]): Unit =
    println(s"COST ${Models.costPerMillion(50000, 2, BigDecimal("0.12"))}")
    println(s"COST_IDLE ${Models.costPerMillion(10000, 1, BigDecimal("0.12"))}")
    println(s"CONCURRENCY ${Models.concurrency(500, 20, .7)}")
    println(s"PARTITIONS ${Models.partitions(BigInt(1024).pow(3) * 100, BigInt(128) * 1024 * 1024)}")
    println(s"CACHE ${Models.cacheBreakEven(150, 90, 20)}")
    println(s"MEMORY_SLOTS ${Models.concurrentMemory(1024, 256, 256, 128)}")
    val checks = Vector[(String, () => Any)](
      "zero-throughput" -> (() => Models.costPerMillion(0, 1, BigDecimal("0.12"))),
      "empty-samples" -> (() => Models.percentile(Vector.empty, .95)),
      "zero-partition-target" -> (() => Models.partitions(100, 0)),
      "invalid-utilization" -> (() => Models.concurrency(100, 20, 0))
    )
    checks.foreach { case (label, run) =>
      try
        run()
        throw IllegalStateException("probe expected failure")
      catch case e: IllegalArgumentException => println(s"ERROR $label ${e.getClass.getName}: ${e.getMessage}")
    }
