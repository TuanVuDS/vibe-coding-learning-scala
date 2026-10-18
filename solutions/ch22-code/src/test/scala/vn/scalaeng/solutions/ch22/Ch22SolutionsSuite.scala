package vn.scalaeng.solutions.ch22

import org.apache.spark.sql.SparkSession
import vn.scalaeng.spark.LocalSpark

import java.nio.file.Files

class Ch22SolutionsSuite extends munit.FunSuite {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = LocalSpark.session("ch22-solutions", cores = 2, maxFailures = 3)
    spark.sparkContext.setLogLevel("OFF")
  }
  override def afterAll(): Unit = spark.stop()

  test("bài 1: mô hình Scala 2.13 — sealed trait, value class, implicit class") {
    import Model213._
    val o = Order(OrderId(1), UserId(7), OrderStatus.Pending, 450000)
    assertEquals((OrderStatus.fromDb("PAID"), o.isOpen), (Right(OrderStatus.Paid), true))
    assert(OrderStatus.fromDb("DONE").isLeft)
  }

  test("bài 2: số stage — ghi parquet, distinct().count(), join không broadcast (chiến lược đọc từ plan CUỐI)") {
    val out  = Files.createTempDirectory("ch22").resolve("x").toString
    val join = StageCounts.joinWithoutBroadcast(spark)
    val counts = (StageCounts.writeOnly(spark, out), StageCounts.distinctCount(spark), join.stages)
    println(s"[bài 2] stage: write=${counts._1}, distinct+count=${counts._2}, ${join.finalStrategy}+count=${counts._3}")
    assertEquals(counts._1, 1)
    assertEquals(join.finalStrategy, "SortMergeJoin")
    assert(counts._2 >= 2 && counts._3 >= 3, counts.toString)
  }

  test("bài 2 (review M5): broadcast vẫn tắt, nhưng một cấu hình AQE khác đổi plan cuối sang ShuffledHashJoin") {
    val r = StageCounts.joinWithoutBroadcast(spark, Map("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "134217728"))
    println(s"[bài 2] maxShuffledHashJoinLocalMapThreshold=128MB -> ${r.finalStrategy}, ${r.stages} stage")
    assertEquals(r.finalStrategy, "ShuffledHashJoin")
  }

  test("bài 3: hai nhánh trên cùng dữ liệu — không cache tính 2 lần, có cache tính 1 lần") {
    assertEquals((TwoBranches.mapCalls(spark, cache = false), TwoBranches.mapCalls(spark, cache = true)), (200000L, 100000L))
  }

  test("bài 4: tắt AQE thì số partition sau shuffle đúng bằng spark.sql.shuffle.partitions") {
    val runs = List(200, 4).map(ShufflePartitions.measure(spark, _))
    runs.foreach(r => println(s"[bài 4] shuffle.partitions=${r.shufflePartitions} -> ${r.partitionsAfter} partition, ${r.millis} ms"))
    assertEquals(runs.map(_.partitionsAfter), List(200, 4))
  }

  test("bài 5: dòng xấu làm mọi lần thử lỗi → job thất bại sau maxFailures = 3 lần") {
    val r = PoisonRecord.run(spark)
    println(s"[bài 5] $r; số lần thử = ${PoisonRecord.attempts.get()}")
    assert(r.isLeft, r.toString)
    assertEquals(PoisonRecord.attempts.get(), 3)
  }

  test("bài 6: RDD mapPartitions + reduceByKey == DataFrame groupBy") {
    val rows = (1 to 10000).map(i => (s"SKU-${i % 17}", (i % 991).toLong))
    assertEquals(RddAggregate.revenueBySku(spark, rows), RddAggregate.viaDataFrame(spark, rows))
  }

  test("bài 6 (review M4): tổng vượt Long — cả hai cách cho cùng số đúng; bản Long của DataFrame ném ARITHMETIC_OVERFLOW") {
    val rows     = Seq("A" -> Long.MaxValue, "A" -> 1L, "B" -> Long.MinValue, "B" -> -1L) ++ (1 to 20).map(i => "A" -> i.toLong)
    val expected = Map("A" -> (BigInt(Long.MaxValue) + 1 + (1 to 20).sum), "B" -> (BigInt(Long.MinValue) - 1))
    assertEquals(RddAggregate.revenueBySku(spark, rows), expected)
    assertEquals(RddAggregate.viaDataFrame(spark, rows), expected)
    val e = intercept[Exception](RddAggregate.viaDataFrameLong(spark, rows))
    println(s"[bài 6] Long + ANSI: ${e.getClass.getSimpleName}: ${e.getMessage.linesIterator.next().take(120)}")
    assert(e.getMessage.contains("ARITHMETIC_OVERFLOW"), e.getMessage)
    assertEquals(RddAggregate.revenueBySku(spark, Seq.empty), Map.empty[String, BigInt])
  }
}
