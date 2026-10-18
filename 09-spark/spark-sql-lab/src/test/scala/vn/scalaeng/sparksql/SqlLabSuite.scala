package vn.scalaeng.sparksql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.Instant
import scala.jdk.CollectionConverters._

class SqlLabSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  private lazy val orders    = SqlLab.readOrders(spark, "data/orders.csv")
  private lazy val customers = SqlLab.readCustomers(spark, "data/customers.csv")
  private lazy val parquet   = {
    val p = Files.createTempDirectory("ch23-test").resolve("orders.parquet").toString
    orders.write.mode("overwrite").parquet(p)
    p
  }

  override def beforeAll(): Unit = { spark = SqlDemo.session(); spark.sparkContext.setLogLevel("ERROR") }
  override def afterAll(): Unit  = spark.stop()

  /** Dựng DataFrame theo `Schemas.orders` nhưng MỌI cột nullable — giống DataFrame đọc từ file (test "nguồn file BỎ QUA nullable") —
    * để thử trường hợp biên mà file mẫu không có. (Giữ nullable = false mà đưa null vào thì Spark lỗi INTERNAL_ERROR khi ghi.)
    */
  private val fileLikeSchema = StructType(Schemas.orders.fields.map(_.copy(nullable = true)))
  private def ordersOf(rows: (String, String, String, String, String, Integer, java.lang.Long, String)*): DataFrame =
    spark.createDataFrame(rows.map { case (o, c, d, st, sku, q, p, ts) =>
      Row(o, c, Date.valueOf(d), st, sku, q, p, Timestamp.from(Instant.parse(ts)))
    }.asJava, fileLikeSchema)

  private def csvFile(content: String): String = {
    val f = Files.createTempFile("ch23", ".csv")
    Files.write(f, content.getBytes("UTF-8"))
    f.toString
  }
  private val header = "order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at\n"

  test("schema tường minh vs suy luận: inferSchema đoán unit_price_vnd là INT theo dữ liệu hiện có") {
    assertEquals(orders.schema("unit_price_vnd").dataType, LongType)
    assertEquals(SqlLab.readOrdersInferred(spark, "data/orders.csv").schema("unit_price_vnd").dataType, IntegerType)
  }

  test("nguồn file BỎ QUA nullable = false của schema: mọi cột đọc lên đều nullable") {
    assert(!Schemas.orders("order_id").nullable)
    assert(orders.schema.fields.forall(_.nullable), orders.schema.treeString)
  }

  test("FAILFAST: dòng sai kiểu làm job lỗi ngay; PERMISSIVE (mặc định) âm thầm biến thành null") {
    val bad = Files.createTempFile("bad", ".csv")
    Files.write(bad, "order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at\nX,C1,2026-10-15,PAID,KB-01,hai,1,2026-10-15T01:00:00Z\n".getBytes("UTF-8"))
    val failFast = scala.util.Try(SqlLab.readOrders(spark, bad.toString).collect())
    assert(failFast.isFailure)
    // PERMISSIVE: giá trị hỏng âm thầm thành null — job "thành công", kể cả khi schema khai báo nullable = false.
    val permissive = spark.read.option("header", "true").schema(Schemas.orders).csv(bad.toString).collect()
    assertEquals(permissive.head.getAs[Any]("quantity"), null)
  }

  test("window row_number giữ bản MỚI NHẤT: O1 là REFUNDED") {
    val latest = SqlLab.latestVersion(orders).filter(col("order_id") === "O1").select("status").collect().map(_.getString(0)).toList
    assertEquals(latest, List("REFUNDED"))
  }

  test("doanh thu theo segment: left join giữ đơn của khách không tồn tại dưới UNKNOWN") {
    val rows = SqlLab.revenueBySegment(orders, customers).collect().map(r => (r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3))).toList
    assertEquals(rows, List(("Retail", 990000L, 1L, 1L), ("UNKNOWN", 450000L, 1L, 1L), ("Wholesale", 20750000L, 2L, 2L)))
  }

  test("review M2: hai bản CÙNG updated_at khác nội dung = xung đột ở mọi thứ tự; bản giống hệt giữ một") {
    val paid     = ("X", "C1", "2026-10-15", "PAID", "KB", Integer.valueOf(1), java.lang.Long.valueOf(100L), "2026-10-15T01:00:00Z")
    val refunded = paid.copy(_4 = "REFUNDED")
    for (input <- List(Seq(paid, refunded), Seq(refunded, paid))) {
      val df = ordersOf(input: _*).repartition(2)
      assertEquals(SqlLab.latestVersion(df).count(), 0L)
      assertEquals(SqlLab.versionConflicts(df).select("order_id").collect().map(_.getString(0)).toList, List("X"))
    }
    assertEquals(SqlLab.latestVersion(ordersOf(paid, paid)).count(), 1L)
  }

  test("review M2: tổng lũy kế gắn vào đúng dòng hàng dù đổi AQE/số partition (sku phân biệt các dòng cùng đơn)") {
    val lines = Seq("C" -> 300L, "A" -> 100L, "B" -> 200L).map { case (sku, p) =>
      ("X", "C1", "2026-10-15", "PAID", sku, Integer.valueOf(1), java.lang.Long.valueOf(p), "2026-10-15T01:00:00Z")
    }
    val runs = for ((aqe, n) <- List(("true", "2"), ("false", "2"), ("false", "7"))) yield {
      spark.conf.set("spark.sql.adaptive.enabled", aqe); spark.conf.set("spark.sql.shuffle.partitions", n)
      try SqlLab.runningTotal(ordersOf(lines: _*)).select("sku", "running_total").collect().map(r => r.getString(0) -> r.getLong(1)).toList
      finally { spark.conf.unset("spark.sql.adaptive.enabled"); spark.conf.unset("spark.sql.shuffle.partitions") }
    }
    runs.foreach(r => assertEquals(r, List("A" -> 100L, "B" -> 300L, "C" -> 600L)))
  }

  test("review M3: customer_id trùng làm join NHÂN doanh thu → từ chối; một đơn nhiều sku là 1 đơn, 3 dòng hàng") {
    val dupCustomers = customers.union(customers.filter(col("customer_id") === "C2"))
    val e = intercept[IllegalArgumentException](SqlLab.revenueBySegment(orders, dupCustomers))
    assert(e.getMessage.contains("C2"), e.getMessage)
    val x = ordersOf(Seq("A" -> 100L, "B" -> 200L, "C" -> 300L).map { case (sku, p) =>
      ("X", "C1", "2026-10-15", "PAID", sku, Integer.valueOf(1), java.lang.Long.valueOf(p), "2026-10-15T01:00:00Z") }: _*)
    val r = SqlLab.revenueBySegment(x, customers).collect().head
    assertEquals((r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3)), ("Retail", 600L, 3L, 1L))
  }

  test("review M1: tích vượt Long → ANSI ném ARITHMETIC_OVERFLOW, không trả số âm") {
    val big = ordersOf(("X", "C1", "2026-10-15", "PAID", "KB", Integer.valueOf(2), java.lang.Long.valueOf(Long.MaxValue), "2026-10-15T01:00:00Z"))
    val e   = intercept[Exception](SqlLab.revenueBySegment(big, customers).collect())
    assert(e.getMessage.contains("ARITHMETIC_OVERFLOW"), e.getMessage)
  }

  test("review M6: header đổi thứ tự cột bị phát hiện (enforceSchema=false); count() không parse cột không dùng") {
    val swapped = csvFile("customer_id,order_id,day,status,sku,quantity,unit_price_vnd,updated_at\nC1,X,2026-10-15,PAID,KB-01,2,1,2026-10-15T01:00:00Z\n")
    val err = intercept[Exception](SqlLab.readOrders(spark, swapped).collect())
    println(s"[M6] header lệch: ${err.getClass.getSimpleName}: ${err.getMessage.linesIterator.next().take(160)}")
    val bad = csvFile(header + "X,C1,2026-10-15,PAID,KB-01,hai,1,2026-10-15T01:00:00Z\n")
    val df  = SqlLab.readOrders(spark, bad)
    assertEquals(df.count(), 1L)                                   // FAILFAST nhưng "xanh": count() không cần parse quantity
    assertEquals(df.select("order_id").collect().map(_.getString(0)).toList, List("X"))
    assert(scala.util.Try(df.collect()).isFailure)                 // đọc ĐỦ cột mới thấy lỗi
  }

  test("left_anti tìm đơn mồ côi; left_semi giữ đơn của khách tồn tại, không thêm cột") {
    assertEquals(SqlLab.orphanOrders(orders, customers).select("order_id").collect().map(_.getString(0)).toList, List("O4"))
    val semi = SqlLab.ordersOfKnownCustomers(orders, customers)
    assertEquals(semi.count(), 6L)
    // Không thêm cột của customers — nhưng join theo Seq("customer_id") đưa cột khóa lên ĐẦU.
    assertEquals(semi.columns.toSet, orders.columns.toSet)
    assertEquals(semi.columns.head, "customer_id")
  }

  test("review M5: sku NULL — cả bốn cách lọc trả 0 dòng (lambda không NPE); quantity NULL không vào được OrderRow") {
    val session = spark
    import session.implicits._
    val p = Files.createTempDirectory("ch23-null").resolve("o.parquet").toString
    ordersOf(("X", "C1", "2026-10-15", "PAID", null, Integer.valueOf(1), java.lang.Long.valueOf(1L), "2026-10-15T01:00:00Z")).write.parquet(p)
    assertEquals(
      List(SqlLab.withBuiltin(spark, p).collect().length, SqlLab.withUdf(spark, p).collect().length,
        SqlLab.typedColumn(spark, p).collect().length, SqlLab.typedLambda(spark, p).collect().length),
      List(0, 0, 0, 0))
    val q = ordersOf(("Y", "C1", "2026-10-15", "PAID", "KB", null, java.lang.Long.valueOf(1L), "2026-10-15T01:00:00Z"))
    val e = intercept[Exception](q.as[OrderRow].collect())
    println(s"[M5] quantity NULL -> OrderRow: ${e.getMessage.linesIterator.take(3).mkString(" | ")}")
    assert(e.getMessage.contains("NOT_NULL_ASSERT_VIOLATION"), e.getMessage)
  }

  test("tổng lũy kế bằng window frame") {
    val c2 = SqlLab.runningTotal(orders).filter(col("customer_id") === "C2").select("running_total").collect().map(_.getLong(0)).toList
    assertEquals(c2, List(20000000L, 20750000L))
  }

  test("NULL ba trị: = NULL không khớp gì; <=> và isNull khớp; != loại luôn dòng NULL") {
    assertEquals(SqlLab.nullSemantics(customers), SqlLab.NullCounts(eqNull = 0, nullSafeEqNull = 1, isNullCount = 1, notEqHanoi = 2))
  }

  private def scan(plan: String) = plan.linesIterator.find(_.contains("FileScan")).getOrElse("")

  test("hàm có sẵn được đẩy xuống Parquet; UDF và lambda của Dataset thì không") {
    assert(scan(SqlLab.planOf(SqlLab.withBuiltin(spark, parquet))).contains("PushedFilters: [IsNotNull(sku), StringStartsWith(sku,KB)]"))
    assert(scan(SqlLab.planOf(SqlLab.withUdf(spark, parquet))).contains("PushedFilters: []"))
    assert(scan(SqlLab.planOf(SqlLab.typedLambda(spark, parquet).toDF())).contains("PushedFilters: []"))
    assert(scan(SqlLab.planOf(SqlLab.typedColumn(spark, parquet).toDF())).contains("StringStartsWith(sku,KB)"))
  }

  test("column pruning: select 2 cột chỉ đọc 2 cột; Dataset[OrderRow] đọc đủ 8 cột") {
    assert(scan(SqlLab.planOf(SqlLab.withBuiltin(spark, parquet))).contains("ReadSchema: struct<order_id:string,sku:string>"))
    assert(scan(SqlLab.planOf(SqlLab.typedColumn(spark, parquet).toDF())).contains("ReadSchema: struct<order_id:string,customer_id:string,day:date"))
  }

  test("Catalyst đẩy filter xuống DƯỚI join và chỉ giữ cột cần") {
    val q = orders.join(customers, Seq("customer_id")).filter(col("segment") === "Retail" && col("quantity") > 1).select("order_id", "city")
    val plan = q.queryExecution.optimizedPlan.toString
    val joinLine   = plan.linesIterator.indexWhere(_.contains("Join Inner"))
    val filterLine = plan.linesIterator.indexWhere(_.contains("segment#") )
    assert(joinLine >= 0 && filterLine > joinLine, plan) // filter segment nằm DƯỚI join trong cây
    assert(plan.contains("Project [order_id#") && plan.contains("customer_id#"), plan)
  }

  test("Dataset typed: đọc lên case class, cột null thành None") {
    // `import spark.implicits._` cần một ĐỊNH DANH ỔN ĐỊNH (val) — `spark` ở đây là `var` nên phải gán qua val.
    val session = spark
    import session.implicits._
    val o5 = orders.as[OrderRow].filter(col("order_id") === "O5").collect().head
    assertEquals((o5.quantity, o5.unit_price_vnd), (1, None))
  }
}
