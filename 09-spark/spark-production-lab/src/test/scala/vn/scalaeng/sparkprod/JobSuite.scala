package vn.scalaeng.sparkprod

import org.apache.spark.sql.SparkSession

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** Test của JOB (không chỉ của hàm biến đổi): gọi đúng điểm vào `Job.run` với tham số dòng lệnh như production, rồi kiểm tra
  * mã thoát, output trên đĩa, manifest, metric và log. Đây là integration test — chạy toàn bộ pipeline trên dữ liệu thật của lab.
  */
class JobSuite extends munit.FunSuite {
  private var spark: SparkSession = _
  override def beforeAll(): Unit = { spark = Job.session(); spark.sparkContext.setLogLevel("OFF") }
  override def afterAll(): Unit  = spark.stop()

  private val input     = "../batch-pipeline-lab/data/input"
  private val customers = "../batch-pipeline-lab/data/customers.csv"
  private def out(): Path = Files.createTempDirectory("ch26-out")

  private def args(output: Path, extra: String*): Seq[String] =
    Seq(s"--input=$input", s"--customers=$customers", s"--output=$output", "--run-date=2026-10-16") ++ extra

  private def runJob(a: Seq[String], faults: Int => Unit = _ => ()): (Int, Vector[String]) = {
    val logs = ArrayBuffer.empty[String]
    val code = Job.run(spark, a, Map.empty, line => logs.synchronized { logs += line; () }, faults)
    (code, logs.toVector)
  }

  /** Manifest của các lần thực thi có `run_id` cho trước — mỗi lần một file `<run_id>--<execution_id>.json`. */
  private def manifests(output: Path, runId: String): List[String] = {
    val s = Files.list(output.resolve("_runs"))
    try s.iterator().asScala.filter(_.getFileName.toString.startsWith(s"$runId--")).toList.sortBy(_.getFileName.toString)
      .map(f => new String(Files.readAllBytes(f), StandardCharsets.UTF_8))
    finally s.close()
  }
  private def manifest(output: Path, runId: String): String = manifests(output, runId).last
  private def prom(output: Path): String =
    new String(Files.readAllBytes(output.resolve("_metrics/ch26_orders.prom")), StandardCharsets.UTF_8)

  test("cấu hình: gom MỌI lỗi một lần; biến môi trường là nguồn dự phòng; tham số dòng lệnh thắng") {
    val e = JobConfig.parse(Seq("--run-date=2026-13-01", "--max-reject-rate=NaN", "--nope=1", "lạc"), Map.empty, () => "r")
    assertEquals(e.left.toOption.get.toSet, Set(
      "tham số 'lạc' phải có dạng --key=value", "tham số lạ --nope", "thiếu --input (hoặc CH26_INPUT)",
      "thiếu --customers (hoặc CH26_CUSTOMERS)", "thiếu --output (hoặc CH26_OUTPUT)",
      "--run-date='2026-13-01': ngày dạng yyyy-MM-dd", "--max-reject-rate='NaN': số thực trong [0, 1]"))
    val env = Map("CH26_INPUT" -> "env-in", "CH26_CUSTOMERS" -> "c", "CH26_OUTPUT" -> "o", "CH26_RUN_DATE" -> "2026-10-16")
    val c = JobConfig.parse(Seq("--input=arg-in"), env, () => "r").toOption.get
    assertEquals((c.input, c.output, c.maxRejectRate, c.shufflePartitions, c.runId), ("arg-in", "o", 0.05, 4, "r"))
  }

  test("cấu hình sai → mã thoát 2, KHÔNG tạo SparkSession (điểm vào main dùng launch, review T16 M1)") {
    val logs = ArrayBuffer.empty[String]
    val code = Job.launch(Seq("--input=x"), Map.empty, l => { logs += l; () }, () => fail("không được tạo session khi cấu hình sai"),
      stopAfter = true)
    assertEquals(code, ExitCode.ConfigInvalid)
    assert(logs.exists(_.contains("\"event\":\"config_invalid\"")), logs.mkString("\n"))
  }

  test("cấu hình: tham số rỗng và tham số lặp lại là lỗi, không âm thầm dùng mặc định / bản cuối (review T16 m1)") {
    val base = Seq("--input=i", "--customers=c", "--output=o", "--run-date=2026-10-16")
    assertEquals(JobConfig.parse(base ++ Seq("--max-reject-rate=", "--run-id="), Map.empty, () => "r").left.toOption.get,
      List("--max-reject-rate= rỗng (bỏ hẳn tham số để dùng mặc định)", "--run-id= rỗng (bỏ hẳn tham số để dùng mặc định)"))
    assertEquals(JobConfig.parse(base ++ Seq("--max-attempts=BAD", "--max-attempts=2"), Map.empty, () => "r").left.toOption.get,
      List("tham số --max-attempts lặp lại"))
  }

  test("integration: chạy cả job → mã 0, output + manifest + metric Prometheus + log JSON có run_id") {
    val o = out()
    val (code, logs) = runJob(args(o, "--run-id=it-1", "--max-reject-rate=0.2"))
    assertEquals(code, ExitCode.Succeeded)
    val m = manifest(o, "it-1")
    assert(m.contains("\"status\":\"SUCCEEDED\"") && m.contains("\"paid_revenue_vnd\":21900000") && m.contains("\"attempts\":1"), m)
    assert(m.contains("\"spark\":\"4.2.0\""), m)
    assert(m.contains("\"metrics_complete\":true"), m)
    val p = prom(o)
    assert(p.contains("job_exit_code{job=\"ch26_orders\"} 0") && p.contains("job_last_run_date_epoch_day{job=\"ch26_orders\"} 20742"), p)
    val written = p.linesIterator.find(_.startsWith("spark_records_written{")).get.split(' ').last.toLong
    assert(written > 0, p)
    assert(logs.forall(l => l.startsWith("{") && l.contains("\"run_id\":\"it-1\"")), logs.mkString("\n"))
    assertEquals(logs.map(l => "\"event\":\"([a-z_]+)\"".r.findFirstMatchIn(l).get.group(1)),
      Vector("job_started", "quality_gate_passed", "pipeline_succeeded", "job_finished"))
  }

  test("quality gate theo LÔ CỦA NGÀY CHẠY: 1/6 = 16,7% > 10% → mã 3, KHÔNG ghi output; lô không tồn tại cũng mã 3") {
    val o = out()
    val (code, logs) = runJob(args(o, "--run-id=gate", "--max-reject-rate=0.1"))
    assertEquals(code, ExitCode.DataRejected)
    assert(!Files.exists(o.resolve("fact_order_lines")), "gate chặn thì không được có output")
    assert(manifest(o, "gate").contains("\"status\":\"DATA_REJECTED\""))
    assert(logs.exists(_.contains("1/6 dòng bị loại (16.7%) > ngưỡng 10.0%")), logs.mkString("\n"))
    val (missing, _) = runJob(Seq(s"--input=$input", s"--customers=$customers", s"--output=${out()}", "--run-date=2026-10-30"))
    assertEquals(missing, ExitCode.DataRejected)
  }

  test("retry: 2 lỗi I/O tạm thời rồi thành công với max-attempts=3; 3 lỗi → mã 5") {
    val transient: Int => Unit = n => if (n <= 2) throw new TransientStorageException(s"storage chập chờn (giả lập, lần $n)")
    val o = out()
    val (ok, logs) = runJob(args(o, "--run-id=retry-ok", "--max-reject-rate=0.2"), transient)
    assertEquals(ok, ExitCode.Succeeded)
    assert(manifest(o, "retry-ok").contains("\"attempts\":3"))
    assertEquals(logs.count(_.contains("attempt_failed_transient")), 2)
    val (exhausted, _) = runJob(args(out(), "--run-id=retry-no", "--max-reject-rate=0.2", "--max-attempts=2"), transient)
    assertEquals(exhausted, ExitCode.RetriesExhausted)
  }

  test("KHÔNG retry lỗi vĩnh viễn: thiếu file và vi phạm dữ liệu chỉ chạy 1 lần") {
    import Failures._
    assertEquals(classify(new java.nio.file.NoSuchFileException("x")), Permanent)
    assertEquals(classify(new IOException("bọc", new java.nio.file.AccessDeniedException("y"))), Permanent)
    assertEquals(classify(new org.apache.spark.SparkException("job lỗi", new java.net.SocketException("Connection reset"))), Transient)
    // Các ca review T16 M2: IOException KHÔNG tự động là tạm thời; lỗi dữ liệu thắng mọi loại khác trong chuỗi cause.
    assertEquals(classify(new IllegalArgumentException("hợp đồng", new IOException("đọc"))), Data)
    assertEquals(classify(new java.nio.file.FileAlreadyExistsException("z")), Permanent)
    assertEquals(classify(new IOException("đọc parquet", new java.util.zip.ZipException("invalid header"))), Permanent)
    assertEquals(classify(new IOException("không rõ")), Unknown)
    val dupCustomers = Files.createTempFile("cust", ".csv")
    Files.write(dupCustomers, "customer_id,segment,city\nC1,Retail,HN\nC1,Wholesale,HCM\n".getBytes(StandardCharsets.UTF_8))
    var attempts = 0
    val (code, _) = runJob(Seq(s"--input=$input", s"--customers=$dupCustomers", s"--output=${out()}", "--run-date=2026-10-16",
      "--max-reject-rate=0.2"), _ => attempts += 1)
    assertEquals((code, attempts), (ExitCode.DataRejected, 1))
  }

  test("chạy lại cùng ngày = cùng output (idempotent); mỗi lần THỰC THI có manifest riêng, kể cả cùng run_id (review T16 M4, M5)") {
    val o = out()
    def snapshot = spark.read.parquet(o.resolve("daily_revenue").toString).orderBy("day", "segment").collect().toList
    assertEquals(runJob(args(o, "--run-id=a", "--max-reject-rate=0.2"))._1, 0)
    val first = snapshot
    assertEquals(runJob(args(o, "--run-id=b", "--max-reject-rate=0.2"))._1, 0)
    assertEquals(snapshot, first)
    // Cùng run_id, lần sau bị gate chặn: manifest SUCCEEDED trước đó KHÔNG bị thay mất.
    assertEquals(runJob(args(o, "--run-id=a", "--max-reject-rate=0.1"))._1, ExitCode.DataRejected)
    val statusOf = "\"status\":\"([A-Z_]+)\"".replace("\"", "\"").r
    assertEquals(manifests(o, "a").map(m => statusOf.findFirstMatchIn(m).get.group(1)).toSet, Set("SUCCEEDED", "DATA_REJECTED"))
    assertEquals(manifests(o, "a").size + manifests(o, "b").size, 3)
    // Prometheus: MỘT file trạng thái hiện tại — không tích lũy series trùng theo lần chạy.
    assertEquals(Files.list(o.resolve("_metrics")).iterator().asScala.map(_.getFileName.toString).toList, List("ch26_orders.prom"))
    assert(prom(o).contains("job_exit_code{job=\"ch26_orders\"} 3"), prom(o))
  }

  test("lỗi SAU khi dữ liệu đã ghi (sink log hỏng ở pipeline_succeeded) → KHÔNG chạy lại pipeline, mã 1 (review T16 M2)") {
    var pipelineRuns = 0
    val logs = ArrayBuffer.empty[String]
    val code = Job.run(spark, args(out(), "--run-id=sink", "--max-reject-rate=0.2"), Map.empty, { line =>
      if (line.contains("\"event\":\"pipeline_succeeded\"")) throw new java.net.SocketException("log collector mất kết nối (giả lập)")
      logs += line; ()
    }, _ => pipelineRuns += 1)
    assertEquals((code, pipelineRuns), (ExitCode.Failed, 1))
    // Review vòng 2 (R2-M3): job group đã được gỡ dù sink hỏng — action sau đó không bị tính vào lần chạy này.
    assertEquals(Option(spark.sparkContext.getLocalProperty("spark.jobGroup.id")), None)
    assert(logs.exists(_.contains("\"event\":\"finalization_failed\"")), logs.mkString("\n"))
  }

  test("ghi manifest thất bại → mã 1 có log, không ném exception ra khỏi Job.run (review T16 M4)") {
    val o = out()
    Files.write(o.resolve("_runs"), "không phải thư mục".getBytes(StandardCharsets.UTF_8)) // _runs là FILE → không tạo được manifest
    val (code, logs) = runJob(args(o, "--run-id=fin", "--max-reject-rate=0.2"))
    assertEquals(code, ExitCode.Failed)
    assert(logs.exists(_.contains("\"event\":\"finalization_failed\"")), logs.mkString("\n"))
  }

  test("checkpoint cắt lineage: 60 vòng không checkpoint plan dài theo số vòng; checkpoint mỗi 10 vòng giữ plan ngắn, cùng kết quả") {
    val dir = Files.createTempDirectory("ch26-ckpt").toString
    val plain = Checkpointing.iterate(spark, 60, every = 0, dir)
    val ckpt  = Checkpointing.iterate(spark, 60, every = 10, dir)
    println(s"[checkpoint] không: $plain | mỗi 10 vòng: $ckpt")
    assertEquals(plain.result, ckpt.result)
    assert(plain.planNodes > 100 && ckpt.planNodes < 10, s"$plain vs $ckpt")
  }

  test("review T16 vòng 2 (R2-M4): ghi file metric hỏng SAU manifest cũ → manifest ghi cuối, nói thật exit_code 1 + lỗi finalization") {
    val o = out()
    Files.write(o.resolve("_metrics"), "là file, không phải thư mục".getBytes(StandardCharsets.UTF_8))
    val (code, logs) = runJob(args(o, "--run-id=prom", "--max-reject-rate=0.2"))
    assertEquals(code, ExitCode.Failed)
    val m = manifest(o, "prom")
    assert(m.contains("\"status\":\"SUCCEEDED\"") && m.contains("\"exit_code\":1") && m.contains("\"finalization_errors\":[\"metrics_file:"), m)
    assert(logs.exists(_.contains("\"event\":\"finalization_failed\"")), logs.mkString("\n"))
  }

  test("Prometheus text: escape nhãn (kể cả xuống dòng), một TYPE mỗi metric") {
    val text = JobMetrics.prometheus(Map("x_total" -> 3L), Map("job" -> "a\"b\nc"))
    assertEquals(text, "# TYPE x_total gauge\nx_total{job=\"a\\\"b\\nc\"} 3\n")
  }
}
