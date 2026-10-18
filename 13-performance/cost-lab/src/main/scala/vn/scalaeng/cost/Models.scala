package vn.scalaeng.cost

object Models:
  private def nonNegative(value: Double): Boolean = value.isFinite && value >= 0

  /** Nearest-rank: p95 của ba mẫu là max; không nội suy ra mẫu chưa đo. */
  def percentile(values: Vector[Double], p: Double): Double =
    require(values.nonEmpty && values.forall(nonNegative), "samples must be finite, non-negative and non-empty")
    require(p.isFinite && p > 0 && p <= 1, "percentile must be in (0,1]")
    values.sorted.apply(math.ceil(p * values.size).toInt - 1)

  def costPerMillion(eventsPerSecond: Double, nodes: Int, pricePerNodeHour: BigDecimal): BigDecimal =
    require(eventsPerSecond.isFinite && eventsPerSecond > 0, "throughput must be positive and finite")
    require(nodes > 0 && pricePerNodeHour >= 0, "nodes must be positive and price non-negative")
    BigDecimal(1000000) / BigDecimal(eventsPerSecond) / 3600 * nodes * pricePerNodeHour

  /** Little's law dùng trung bình ở trạng thái ổn định, cùng một ranh giới hệ thống. */
  def concurrency(ratePerSecond: Double, meanResidenceMs: Double, targetUtilization: Double): Int =
    require(nonNegative(ratePerSecond) && nonNegative(meanResidenceMs), "rate and residence must be finite and non-negative")
    require(targetUtilization.isFinite && targetUtilization > 0 && targetUtilization <= 1, "utilization must be in (0,1]")
    val slots = math.ceil(ratePerSecond * meanResidenceMs / 1000 / targetUtilization)
    require(slots.isFinite && slots <= Int.MaxValue, "concurrency exceeds Int capacity")
    slots.toInt

  def partitions(bytes: BigInt, targetBytes: BigInt): BigInt =
    require(bytes >= 0 && targetBytes > 0, "bytes must be non-negative and target positive")
    ((bytes + targetBytes - 1) / targetBytes).max(BigInt(1))

  /** Gán task cho slot rảnh sớm nhất; giả định task độc lập, bỏ qua fetch/retry/GC. */
  def makespan(taskMs: Vector[Double], slots: Int, overheadMs: Double): Double =
    require(slots > 0 && nonNegative(overheadMs) && taskMs.forall(nonNegative), "invalid schedule")
    val loads = Array.fill(slots)(0.0)
    taskMs.foreach { ms =>
      val index = loads.indices.minBy(loads(_))
      loads(index) += ms + overheadMs
    }
    loads.max

  def cacheBreakEven(buildMs: Double, uncachedMs: Double, cachedReadMs: Double): Option[BigInt] =
    require(Vector(buildMs, uncachedMs, cachedReadMs).forall(nonNegative), "invalid cache timing")
    if uncachedMs <= cachedReadMs then None
    else Some((BigDecimal(buildMs) / (BigDecimal(uncachedMs) - BigDecimal(cachedReadMs))).setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt + 1)

  def transferSeconds(bytes: BigInt, effectiveBytesPerSecond: Double): Double =
    require(bytes >= 0 && effectiveBytesPerSecond.isFinite && effectiveBytesPerSecond > 0, "invalid transfer parameters")
    val seconds = bytes.toDouble / effectiveBytesPerSecond
    require(seconds.isFinite, "transfer estimate exceeds Double capacity")
    seconds

  /** Ngân sách task: heap - live cache - reserve; không phải toàn bộ RSS/container. */
  def concurrentMemory(heapBytes: BigInt, cacheBytes: BigInt, reserveBytes: BigInt, taskBytes: BigInt): BigInt =
    require(heapBytes >= 0 && cacheBytes >= 0 && reserveBytes >= 0 && taskBytes > 0, "invalid memory budget")
    ((heapBytes - cacheBytes - reserveBytes).max(BigInt(0))) / taskBytes

  /** Nguồn định kỳ hữu hạn, flush khi đủ cap, hết maxWait hoặc đã nhận EOS. */
  def batchRanges(count: Int, cap: Int, gapNs: Long, maxWaitNs: Long): Vector[(Int, Int)] =
    require(count >= 0 && cap > 0 && gapNs > 0 && maxWaitNs >= 0, "invalid batch parameters")
    val effective = (BigInt(maxWaitNs) / gapNs + 1).min(BigInt(cap)).toInt
    (0 until count by effective).toVector.map { from =>
      from -> (from.toLong + effective).min(count.toLong).toInt
    }
