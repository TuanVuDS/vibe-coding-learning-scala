package vn.scalaeng.distributed.solutions

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Exercises {
  // E1: giữ khóa, đổi giá trị; chỉ giữ partitioner khi hợp đồng phân bố còn đúng.
  def doubleValues(input: RDD[(Long, Long)]): RDD[(Long, Long)] = input.mapValues(_ * 2L)

  // E2: partitionBy trả chi phí chuẩn bị trước; bên gọi có thể persist/tái sử dụng RDD.
  def prepare(input: RDD[(Long, Long)], partitions: Int): RDD[(Long, Long)] = {
    require(partitions > 0, "partitions must be positive")
    input.partitionBy(new HashPartitioner(partitions))
  }

  // E3: cộng BigInt có tính kết hợp và không tràn Long; so với groupByKey cùng kiểu.
  def totals(input: RDD[(Long, Long)], partitions: Int): RDD[(Long, BigInt)] = {
    require(partitions > 0, "partitions must be positive")
    input.mapValues(BigInt(_)).reduceByKey(new HashPartitioner(partitions), _ + _)
  }

  // E4: audit journal append để phát hiện trùng; chưa hoàn tác dữ liệu ở sink.
  def duplicateIds(journal: Vector[String]): Set[Long] = {
    val ids = journal.map(line => line.takeWhile(_ != ',').toLong)
    ids.groupMapReduce(identity)(_ => 1)(_ + _).collect { case (id, n) if n > 1 => id }.toSet
  }

  // E5: RDD MỚI, trước job đầu; checkpoint lazy nên phải materialize trước unpersist.
  // Trên cluster, bên gọi phải đặt thư mục checkpoint bền và dùng chung.
  def materializeCheckpoint(input: RDD[Long]): RDD[Long] = {
    input.persist()
    try {
      input.checkpoint()
      input.count()
      require(input.isCheckpointed, "checkpoint not materialized")
      input
    } finally input.unpersist(blocking = true)
  }

  // E6: tính chính xác cho thanh toán; tính xấp xỉ cho dashboard.
  // count distinct / approx_count_distinct của Spark đều bỏ NULL.
  def cardinality(input: DataFrame, approximate: Boolean): DataFrame =
    if (approximate) input.agg(approx_count_distinct(col("customer_id"), 0.05).as("customers"))
    else input.agg(count_distinct(col("customer_id")).as("customers"))

  // E7: thời gian xử lý chưa gồm chờ nguồn hay độ trễ theo event time.
  def throughput(rows: Long, millis: Long): Option[Double] = {
    require(rows >= 0 && millis >= 0, "measurements must be nonnegative")
    if (millis == 0) None else Some(rows.toDouble * 1000.0 / millis)
  }
}
