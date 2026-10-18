package vn.scalaeng.solutions.ch23

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import vn.scalaeng.sparksql.{OrderRow, SqlDemo, SqlLab}

import java.nio.file.Files

class Ch23SolutionsSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  private val ordersCsv = "../../09-spark/spark-sql-lab/data/orders.csv"

  override def beforeAll(): Unit = { spark = SqlDemo.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  test("bài 1: dòng hỏng được tách nguyên văn vào _corrupt_record") {
    val f = Files.createTempFile("ch23", ".csv")
    Files.write(f, ("order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at\n" +
      "O1,C1,2026-10-15,PAID,KB-01,2,450000,2026-10-15T01:00:00Z\n" +
      "O2,C1,2026-10-15,PAID,KB-01,hai,450000,2026-10-15T01:00:00Z\n").getBytes("UTF-8"))
    val (good, bad) = CorruptRecords.split(spark, f.toString)
    assertEquals(good.count(), 1L)
    assertEquals(bad.collect().map(_.getString(0)).toList, List("O2,C1,2026-10-15,PAID,KB-01,hai,450000,2026-10-15T01:00:00Z"))
  }

  private val goldenByCustomer = Map("C2" -> 20750000L, "C3" -> 990000L, "C9" -> 450000L) // tính tay mục 6: O1 REFUNDED, O5 giá NULL bị loại

  test("bài 2: SQL và DataFrame API cho cùng kế hoạch tối ưu và cùng kết quả — và kết quả ĐÚNG golden") {
    val latest = SqlLab.latestVersion(SqlLab.readOrders(spark, ordersCsv))
    val a = SqlVsDataFrame.viaSql(spark, latest)
    val b = SqlVsDataFrame.viaApi(latest)
    assertEquals(SqlVsDataFrame.normalizedPlan(a), SqlVsDataFrame.normalizedPlan(b))
    assertEquals(a.collect().map(r => r.getString(0) -> r.getLong(1)).toMap, goldenByCustomer)
    assertEquals(b.collect().map(r => r.getString(0) -> r.getLong(1)).toMap, goldenByCustomer)
  }

  test("bài 3: top-1 sku theo doanh thu mỗi ngày") {
    val rows = TopSkuPerDay.top(SqlLab.readOrders(spark, ordersCsv), 1).select("day", "sku", "revenue_vnd").collect()
      .map(r => (r.getDate(0).toString, r.getString(1), r.getLong(2))).toList
    assertEquals(rows, List(("2026-10-15", "KB-01", 20000000L), ("2026-10-16", "HP-01", 990000L), ("2026-10-17", "MS-01", 750000L)))
  }

  test("bài 4: UDF và hàm có sẵn cho cùng giá trị TỪNG DÒNG theo hợp đồng (space, tab, rỗng, NULL, 'i' dưới locale tr-TR)") {
    val session = spark
    import session.implicits._
    val inputs = Seq(Some(" sku-1 "), Some("\tsku\t"), Some("  "), Some(""), None, Some("i"), Some("kb-01"))
    val df     = inputs.toDF("sku").withColumn("id", monotonically_increasing_id())
    val prev   = java.util.Locale.getDefault
    java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR")) // local mode: executor cùng JVM nên UDF thấy locale này
    try {
      def values(d: org.apache.spark.sql.DataFrame) = d.orderBy("id").select("sku_norm").collect().map(r => Option(r.getString(0))).toList
      val (b, u) = (values(UdfVsBuiltin.builtin(df)), values(UdfVsBuiltin.withUdf(df)))
      assertEquals(b, List(Some("SKU-1"), Some("\tSKU\t"), Some(""), Some(""), None, Some("I"), Some("KB-01")))
      assertEquals(u, b)
    } finally java.util.Locale.setDefault(prev)
    // optimizedPlan ở đây chỉ còn `LocalRelation [sku, id, sku_norm]`: dữ liệu nằm sẵn trong driver (toDF từ Seq) nên Catalyst TÍNH LUÔN biểu thức
    // lúc tối ưu (ConvertToLocalRelation). Xem biểu thức ở analyzed plan.
    val (bp, up) = (UdfVsBuiltin.builtin(df).queryExecution.analyzed.toString, UdfVsBuiltin.withUdf(df).queryExecution.analyzed.toString)
    println(s"[bài 4] optimized (builtin): ${UdfVsBuiltin.builtin(df).queryExecution.optimizedPlan.toString.trim}")
    assert(bp.contains("upper(trim("), bp)
    assert(up.contains("UDF("), up)
  }

  test("bài 5: === không ghép NULL với NULL; <=> có") {
    assertEquals(NullSafeJoin.counts(spark), (1L, 2L))
  }

  test("bài 6: Dataset typed cùng kết quả, nhưng groupByKey/mapGroups KHÔNG tổng hợp cục bộ trước shuffle") {
    val session = spark
    import session.implicits._
    val ds        = SqlLab.latestVersion(SqlLab.readOrders(spark, ordersCsv)).as[OrderRow]
    val typedQ    = TypedVsUntyped.typed(spark, ds).toDF()
    val untypedQ  = SqlVsDataFrame.viaApi(ds.toDF())
    // Plan đọc từ CHÍNH Dataset vừa chạy, sau action — plan cuối của AQE (review chéo, m2).
    val typed     = typedQ.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
    val untyped   = untypedQ.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
    assertEquals(typed, goldenByCustomer)
    assertEquals(untyped, goldenByCustomer)
    val (typedPlan, untypedPlan) = (SqlLab.finalPlanOf(typedQ), SqlLab.finalPlanOf(untypedQ))
    // typed: mọi dòng đi qua Exchange rồi Sort (trong partition, theo khóa nhóm) rồi MapGroups; untyped: partial_sum trước Exchange (map-side combine).
    assert(typedPlan.contains("MapGroups") && typedPlan.contains("Sort [") && !typedPlan.contains("partial_sum"), typedPlan)
    assert(untypedPlan.contains("partial_sum"), untypedPlan)
  }

  test("bài 6 (review M1): tràn số — typed ném ArithmeticException như DataFrame ném ARITHMETIC_OVERFLOW, không trả số âm") {
    val session = spark
    import session.implicits._
    val big = Seq(OrderRow("X", "C1", java.time.LocalDate.parse("2026-10-15"), "PAID", "KB", 2, Some(Long.MaxValue),
      java.time.Instant.parse("2026-10-15T01:00:00Z"))).toDS()
    val typedErr = intercept[Exception](TypedVsUntyped.typed(spark, big).collect())
    assert(Iterator.iterate[Throwable](typedErr)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[ArithmeticException]), typedErr.toString)
    val sqlErr = intercept[Exception](SqlVsDataFrame.viaApi(big.toDF()).collect())
    assert(sqlErr.getMessage.contains("ARITHMETIC_OVERFLOW"), sqlErr.getMessage)
  }
}
