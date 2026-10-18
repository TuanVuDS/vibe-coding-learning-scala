package vn.scalaeng.solutions.ch26

import org.apache.spark.sql.SparkSession
import vn.scalaeng.sparkprod.Job

import java.io.IOException
import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.LocalDate

class Ch26SolutionsSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = { spark = Job.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  test("bài 1: phân loại lỗi qua cả chuỗi cause") {
    import Failures._
    val wrapped = new org.apache.spark.SparkException("Job aborted", new RuntimeException("task", new SocketTimeoutException("Read timed out")))
    assertEquals(classify(wrapped), Transient)
    assertEquals(classify(new IOException("đọc lỗi", new java.nio.file.NoSuchFileException("/data/x"))), Permanent)
    assertEquals(classify(new RuntimeException(new IllegalArgumentException("customers trùng customer_id"))), Data)
    assertEquals(classify(new IllegalStateException("?")), Unknown)
    // Các ca review T16 M2 — và lời giải phải phân loại GIỐNG bộ phân loại của lab cho mọi ca.
    val cases = Seq(new IllegalArgumentException("hợp đồng", new IOException("đọc")), new java.nio.file.FileAlreadyExistsException("z"),
      new IOException("parquet", new java.util.zip.ZipException("invalid header")), new IOException("không rõ"), wrapped)
    assertEquals(cases.map(classify), Seq(Data, Permanent, Permanent, Unknown, Transient))
    val withMarker = cases :+ new vn.scalaeng.sparkprod.TransientStorageException("503 SlowDown")
    assertEquals(withMarker.map(c => classify(c).toString), withMarker.map(c => vn.scalaeng.sparkprod.Failures.classify(c).toString))
  }

  test("bài 2: backoff full jitter — nằm trong [0, min(cap, base·2^(n-1))], không tràn ở lần thử 64, xác định với seed") {
    val rnd = new scala.util.Random(42)
    (1 to 64).foreach { n =>
      val d = Backoff.delayMs(n, baseMs = 100, capMs = 30000, rnd)
      assert(d >= 0 && d <= math.min(30000L, if (n <= 20) 100L << (n - 1) else 30000L), s"lần $n: $d")
    }
    assert(Backoff.delayMs(63, baseMs = Long.MaxValue / 2, capMs = Long.MaxValue, new scala.util.Random(1)) >= 0)
    assertEquals(Backoff.delayMs(5, 100, 30000, new scala.util.Random(7)), Backoff.delayMs(5, 100, 30000, new scala.util.Random(7)))
    intercept[IllegalArgumentException](Backoff.delayMs(0, 100, 30000, rnd))
    // Trần đúng ở lần thử 64 với base nhỏ, cap = Long.MaxValue: min(cap, 2^63) = Long.MaxValue; jitter 0.5 → ~2^62 (review T16 m2).
    val half = new scala.util.Random { override def nextDouble(): Double = 0.5 }
    assertEquals(Backoff.delayMs(64, baseMs = 1, capMs = Long.MaxValue, half), math.round(0.5 * Long.MaxValue.toDouble))
  }

  test("bài 3: ngày chưa có lần chạy THÀNH CÔNG (lần lỗi không tính)") {
    val runs = Files.createTempDirectory("ch26-runs")
    def m(id: String, date: String, status: String) =
      Files.write(runs.resolve(s"$id.json"), s"""{"run_id":"$id","status":"$status","exit_code":${if (status == "SUCCEEDED") 0 else 3},"config":{"run_date":"$date"}}""".getBytes(StandardCharsets.UTF_8))
    m("a", "2026-10-14", "SUCCEEDED"); m("b", "2026-10-15", "DATA_REJECTED"); m("c", "2026-10-16", "FAILED"); m("d", "2026-10-16", "SUCCEEDED")
    // Manifest được định dạng lại (khoảng trắng sau ':') vẫn được đọc.
    Files.write(runs.resolve("e.json"), "{\"status\": \"SUCCEEDED\", \"exit_code\": 0, \"config\": {\"run_date\": \"2026-10-17\"}}".getBytes(StandardCharsets.UTF_8))
    assertEquals(RunHistory.missingDates(runs, LocalDate.parse("2026-10-14"), LocalDate.parse("2026-10-17")),
      List(LocalDate.parse("2026-10-15")))
    // SUCCEEDED nhưng exit_code 1 (finalization hỏng) KHÔNG được tính là ngày đã chạy thành công.
    Files.write(runs.resolve("f.json"), "{\"status\":\"SUCCEEDED\",\"exit_code\":1,\"config\":{\"run_date\":\"2026-10-18\"}}".getBytes(StandardCharsets.UTF_8))
    assertEquals(RunHistory.missingDates(runs, LocalDate.parse("2026-10-18"), LocalDate.parse("2026-10-18")), List(LocalDate.parse("2026-10-18")))
  }

  test("bài 4: kiểm tra khối lượng theo trung vị lịch sử") {
    assertEquals(VolumeCheck.anomaly(Seq(1000, 1100, 900, 1050), today = 400, minRatio = 0.5), Some("hôm nay 400 dòng < 50% trung vị 1025.0"))
    assertEquals(VolumeCheck.anomaly(Seq(1000, 1100, 900, 1050), today = 800, minRatio = 0.5), None)
    assertEquals(VolumeCheck.anomaly(Seq(1000), today = 1, minRatio = 0.5), None) // chưa đủ lịch sử
    intercept[IllegalArgumentException](VolumeCheck.anomaly(Seq(1, 2, 3), 1, Double.NaN))
    // Lịch sử chẵn sát biên: trung vị Long.MaxValue, KHÔNG tràn thành -1 rồi im lặng (review T16 M7).
    assertEquals(VolumeCheck.anomaly(Seq.fill(4)(Long.MaxValue), today = 1, minRatio = 0.5), Some("hôm nay 1 dòng < 50% trung vị 9223372036854775807.0"))
  }

  test("bài 5: kiểm tra tương thích lúc khởi động — runtime thật đang chạy test phải qua") {
    assertEquals(RuntimeCheck.check(spark.version, scala.util.Properties.versionNumberString, System.getProperty("java.version")), Nil)
    assertEquals(RuntimeCheck.check("3.5.4", "2.12.20", "11.0.2"),
      List("Spark 3.5.4: jar build cho 4.2.x", "Scala 2.12.20: Spark 4.2 chỉ có bản Scala 2.13", "Java 11.0.2: Spark 4.2 hỗ trợ 17/21/25"))
  }

  test("bài 6: checkpoint và localCheckpoint đều cắt plan; chỉ checkpoint ghi file") {
    val dir = Files.createTempDirectory("ch26-b6")
    val reliable = CheckpointKinds.run(spark, local = false, dir)
    val local    = CheckpointKinds.run(spark, local = true, dir)
    assertEquals((reliable.planNodes, local.planNodes), (1, 1))
    assertEquals(reliable.sum, local.sum)
    assert(reliable.filesInCheckpointDir > 0 && local.filesInCheckpointDir == 0, s"$reliable / $local")
  }
}
