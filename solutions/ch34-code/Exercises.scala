package vn.scalaeng.cost.solutions

import vn.scalaeng.cost.Models

object Exercises:
  final case class Distribution(mean: Double, p50: Double, p95: Double, p99: Double)
  def distribution(ms: Vector[Double]): Distribution =
    val median = Models.percentile(ms, .5)
    Distribution(ms.sum / ms.size, median, Models.percentile(ms, .95), Models.percentile(ms, .99))

  def requiredConsumers(rate: Double, residenceMs: Vector[Double], utilization: Double): Int =
    val summary = distribution(residenceMs)
    Models.concurrency(rate, summary.mean, utilization)

  final case class Layout(partitions: BigInt, schedulingMs: Double)
  def layout(bytes: BigInt, target: BigInt, taskTimes: Vector[Double], slots: Int, overhead: Double): Layout =
    Layout(Models.partitions(bytes, target), Models.makespan(taskTimes, slots, overhead))

  final case class WireCost(bytes: BigInt, transferSeconds: Double, charge: BigDecimal)
  def wireCost(payloadBytes: BigInt, measuredRows: Int, targetRows: BigInt,
      effectiveBytesPerSec: Double, assumedPricePerGiB: BigDecimal): WireCost =
    require(payloadBytes >= 0 && measuredRows > 0 && targetRows >= 0 && assumedPricePerGiB >= 0, "invalid wire cost")
    // Làm tròn lên tổng byte thay vì làm tròn từng event; không mất phần lẻ.
    val bytes = (payloadBytes * targetRows + measuredRows - 1) / measuredRows
    WireCost(bytes, Models.transferSeconds(bytes, effectiveBytesPerSec),
      BigDecimal(bytes) / BigDecimal(BigInt(1024).pow(3)) * assumedPricePerGiB)

  def cacheSaving(buildMs: Double, uncachedMs: Double, readMs: Double, uses: Int): Double =
    require(uses > 0, "uses must be positive")
    Models.cacheBreakEven(buildMs, uncachedMs, readMs)
    uses * uncachedMs - (buildMs + uses * readMs)

  final case class Candidate(name: String, nodes: Int, fleetRate: Double, p99Ms: Double, hourlyPrice: BigDecimal)
  def cheapest(candidates: Vector[Candidate], requiredRate: Double, maxP99Ms: Double): Option[Candidate] =
    require(requiredRate.isFinite && requiredRate > 0 && maxP99Ms.isFinite && maxP99Ms >= 0, "invalid SLO")
    candidates.foreach { c =>
      Models.costPerMillion(c.fleetRate, c.nodes, c.hourlyPrice)
      require(c.p99Ms.isFinite && c.p99Ms >= 0, "invalid latency")
    }
    candidates.filter(c => c.fleetRate >= requiredRate && c.p99Ms <= maxP99Ms)
      .sortBy(c => Models.costPerMillion(c.fleetRate, c.nodes, c.hourlyPrice)).headOption

  def feasibleSlots(heap: BigInt, cache: BigInt, reserve: BigInt, perTask: BigInt, cpuSlots: Int): BigInt =
    require(cpuSlots >= 0, "cpu slots must be non-negative")
    Models.concurrentMemory(heap, cache, reserve, perTask).min(BigInt(cpuSlots))
