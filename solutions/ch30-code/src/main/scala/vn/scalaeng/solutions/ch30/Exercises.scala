package vn.scalaeng.solutions.ch30

import vn.scalaeng.p4.{Event, Json, Receipt}
import vn.scalaeng.reliability.Policy

object Exercises:
  def frontier(records: Vector[(Long, Boolean)]): Option[Long] = Policy.frontier(records)

  /** maxAttempts gồm lần đầu: có đúng maxAttempts-1 khoảng nghỉ. */
  def backoff(maxAttempts: Int, initial: Long, cap: Long): Vector[Long] =
    require(maxAttempts >= 1 && maxAttempts <= 10 && initial >= 0 && cap >= initial, "invalid retry policy")
    Vector.tabulate(maxAttempts - 1)(n => (BigInt(initial) * BigInt(2).pow(n)).min(BigInt(cap)).toLong)

  /** Sequence nghiệp vụ theo key; cho phép seq trùng, không cam kết thứ tự Kafka toàn cục. */
  def ordered(records: Vector[(String, Long)]): Boolean =
    require(records.forall(_._2 >= 0), "negative sequence")
    records.groupMap(_._1)(_._2).values.forall(_.sliding(2).forall(v => v.size < 2 || v(0) <= v(1)))

  def admitted(records: Int, attempts: Int, oneAttemptMillis: Long, waits: Vector[Long],
      pollMillis: Long, margin: Long): Boolean =
    require(attempts >= 1 && waits.size == attempts - 1 && oneAttemptMillis >= 0 && waits.forall(_ >= 0), "invalid attempts")
    val duration = BigInt(attempts) * oneAttemptMillis + waits.map(BigInt(_)).sum
    if duration > Long.MaxValue then false
    else Policy.withinBudget(records, duration.toLong, pollMillis, margin)

  def totalLag(partitions: Vector[(Long, Option[Long])]): Option[BigInt] =
    require(partitions.forall((end, committed) => end >= 0 && committed.forall(c => c >= 0 && c <= end)), "invalid offset range")
    if partitions.isEmpty || partitions.exists(_._2.isEmpty) then None
    else Some(partitions.map((end, committed) => BigInt(end) - committed.get).sum)

  /** Redrive tạo record nguồn MỚI; provenance gốc vẫn ở log audit của operator. */
  def redrive(receipt: Receipt, correctedRaw: String): Either[String, Event] =
    if !receipt.dead then Left("only DLT may be redriven")
    else Json.event(Some(correctedRaw)).flatMap { e =>
      if receipt.input.key.contains(e.orderId) then Right(e) else Left("key must equal orderId")
    }
