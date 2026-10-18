package vn.scalaeng.solutions.ch29

import vn.scalaeng.p4.Receipt

object Exercises:
  val categories = Set("accepted", "duplicate", "invalid", "conflict", "late", "exhausted")
  def reconcile(receipts: Vector[Receipt], expected: Int): Map[String, Int] =
    require(expected >= 0 && receipts.size == expected, "input count mismatch")
    require(receipts.map(_.input.id).distinct.size == expected, "source duplicated in report")
    require(receipts.forall(r => categories(r.kind) && (r.reason.nonEmpty == Set("invalid", "conflict", "late", "exhausted")(r.kind))),
      "invalid disposition")
    receipts.groupMapReduce(_.kind)(_ => 1)(_ + _)

  /** None: có partition active chưa gửi watermark; idle set phải được chỉ định rõ. */
  def globalWatermark(partitions: Map[Int, Option[Long]], idle: Set[Int]): Option[Long] =
    require(idle.subsetOf(partitions.keySet), "unknown idle partition")
    val active = partitions.filterNot((id, _) => idle(id)).values.toVector
    if active.isEmpty || active.exists(_.isEmpty) then None else Some(active.flatten.min)

  def futureAllowed(eventTime: Long, now: Long, skew: Long): Boolean =
    require(eventTime >= 0 && now >= 0 && skew >= 0, "negative clock")
    BigInt(eventTime) <= BigInt(now) + skew

  def uniqueDead(receipts: Vector[Receipt]): Vector[Receipt] =
    require(receipts.forall(_.dead), "non-DLT receipt")
    receipts.groupBy(_.input.id).toVector.sortBy(_._1).map { (_, copies) =>
      require(copies.distinct.size == 1, "DLT identity conflict")
      copies.head
    }

  def fitsMemory(partitions: Int, queuedEach: Int, maxRecordBytes: Long, budget: BigInt): Boolean =
    require(partitions > 0 && queuedEach >= 0 && maxRecordBytes > 0 && budget >= 0, "invalid capacity")
    BigInt(partitions) * queuedEach * maxRecordBytes <= budget

  def closed(start: Long, width: Long, watermark: Long): Boolean =
    require(start >= 0 && width > 0 && watermark >= 0, "invalid window")
    BigInt(start) + width <= watermark
