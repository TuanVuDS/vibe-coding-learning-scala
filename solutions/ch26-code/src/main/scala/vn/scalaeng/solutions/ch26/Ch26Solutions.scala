package vn.scalaeng.solutions.ch26

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.LocalDate
import scala.jdk.CollectionConverters._

/** Bài 1 — phân loại lỗi thành 4 nhóm, đi qua TOÀN BỘ chuỗi cause (Spark bọc lỗi gốc trong SparkException nhiều tầng).
  * Thứ tự ưu tiên khi chuỗi có nhiều loại: Data > Permanent > Transient > Unknown — giống `vn.scalaeng.sparkprod.Failures` của lab.
  * Transient là DANH SÁCH CHO PHÉP (timeout/mất kết nối mạng): "có IOException" KHÔNG đủ — ZIP/Parquet hỏng, file đã tồn tại cũng là IOException.
  */
object Failures {
  sealed trait Kind
  case object Transient extends Kind // thử lại có thể thành công
  case object Permanent extends Kind // hạ tầng/cấu hình sai: thiếu file, sai quyền, file hỏng
  case object Data      extends Kind // dữ liệu vi phạm hợp đồng
  case object Unknown   extends Kind // chưa phân loại — KHÔNG retry, cần người xem

  private def chain(e: Throwable): List[Throwable] = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toList

  def classify(e: Throwable): Kind = {
    val cs = chain(e)
    if (cs.exists(c => c.isInstanceOf[IllegalArgumentException] || c.isInstanceOf[org.apache.spark.sql.AnalysisException])) Data
    else if (cs.exists {
      case _: java.nio.file.NoSuchFileException | _: java.nio.file.AccessDeniedException | _: java.nio.file.FileAlreadyExistsException |
          _: java.io.FileNotFoundException | _: java.util.zip.ZipException | _: java.io.EOFException | _: java.net.UnknownHostException => true
      case _ => false
    }) Permanent
    else if (cs.exists {
      case _: SocketTimeoutException | _: java.net.ConnectException | _: java.net.SocketException |
          _: vn.scalaeng.sparkprod.TransientStorageException => true
      case _                                                                                    => false
    }) Transient
    else Unknown
  }
}

/** Bài 2 — backoff lũy thừa có trần và FULL JITTER: chờ ngẫu nhiên trong [0, min(cap, base·2^(n-1))]. Jitter tránh "đàn bò" — trăm job
  * cùng lỗi lúc 02:00 không cùng thử lại lúc 02:00:01. `Random` truyền vào để test xác định được.
  */
object Backoff {
  def delayMs(attempt: Int, baseMs: Long, capMs: Long, rnd: scala.util.Random): Long = {
    require(attempt >= 1 && baseMs > 0 && capMs >= baseMs, "attempt >= 1, 0 < base <= cap")
    // `base << exp` tràn Long khi base > Long.MaxValue >> exp. Đủ biết nó đã vượt cap là được: base > cap >> exp ⇒ base·2^exp > cap.
    // exp >= 63: base >= 1 nên base·2^63 > Long.MaxValue >= cap — bão hòa ở cap. (Bản cũ kẹp exp = 62 vô điều kiện nên trần bị giữ ở
    // base·2^62 khi cap lớn hơn — review T16 m2.)
    val exp     = math.min(attempt - 1, 63)
    val ceiling = if (exp >= 63 || baseMs > (capMs >> exp)) capMs else math.min(capMs, baseMs << exp)
    math.min(ceiling, math.round(rnd.nextDouble() * ceiling.toDouble))
  }
}

/** Bài 3 — "ngày nào chưa có lần chạy THÀNH CÔNG?" đọc từ manifest (các file `.json` trong thư mục `_runs`). Lần chạy lỗi không tính; nhiều lần chạy một ngày là bình thường. */
object RunHistory {
  // Chỉ đọc manifest do chính Job sinh ra; `\s*` cho phép khoảng trắng quanh ':' nếu ai đó định dạng lại file. Đây KHÔNG phải bộ đọc JSON
  // tổng quát (không xử lý escape hay cấu trúc lồng tùy ý) — công cụ thật nên dùng thư viện JSON (review T16 m3).
  private val RunDate = "\"run_date\"\\s*:\\s*\"(\\d{4}-\\d{2}-\\d{2})\"".r
  private val Status  = "\"status\"\\s*:\\s*\"([A-Z_]+)\"".r
  private val Exit    = "\"exit_code\"\\s*:\\s*(-?\\d+)".r

  def succeededDates(runsDir: Path): Set[LocalDate] =
    if (!Files.isDirectory(runsDir)) Set.empty
    else {
      val files = Files.list(runsDir)
      try files.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).flatMap { f =>
        val s = new String(Files.readAllBytes(f), StandardCharsets.UTF_8)
        // Thành công = dữ liệu SUCCEEDED VÀ job thực sự trả 0 (manifest được ghi cuối, kèm exit_code thật — review T16 vòng 2).
        for {
          st <- Status.findFirstMatchIn(s); ex <- Exit.findFirstMatchIn(s); d <- RunDate.findFirstMatchIn(s)
          if st.group(1) == "SUCCEEDED" && ex.group(1) == "0"
        } yield LocalDate.parse(d.group(1))
      }.toSet finally files.close()
    }

  def missingDates(runsDir: Path, from: LocalDate, to: LocalDate): List[LocalDate] = {
    val ok = succeededDates(runsDir)
    Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).filterNot(ok).toList
  }
}

/** Bài 4 — kiểm tra KHỐI LƯỢNG: hôm nay ít hơn hẳn trung vị N lần chạy trước → cảnh báo (nguồn gửi thiếu file, filter sai...).
  * Quy tắc "đúng" từng dòng không bắt được việc MẤT dòng; kiểm tra thống kê bắt được.
  */
object VolumeCheck {
  def anomaly(history: Seq[Long], today: Long, minRatio: Double): Option[String] = {
    require(!minRatio.isNaN && minRatio > 0 && minRatio <= 1, "minRatio trong (0, 1]")
    require(today >= 0 && history.forall(_ >= 0), "số dòng không âm")
    if (history.size < 3) None // chưa đủ lịch sử: im lặng có chủ đích, ghi rõ trong báo cáo
    else {
      // Trung vị bằng BigDecimal: cộng hai Long ở giữa có thể TRÀN (Long.MaxValue + Long.MaxValue = -2) — review T16 M7.
      val sorted = history.sorted.map(BigDecimal(_))
      val median = if (sorted.size % 2 == 1) sorted(sorted.size / 2) else (sorted(sorted.size / 2 - 1) + sorted(sorted.size / 2)) / 2
      Option.when(BigDecimal(today) < median * BigDecimal(minRatio))(
        f"hôm nay $today dòng < ${minRatio * 100}%.0f%% trung vị ${median.bigDecimal}%.1f")
    }
  }
}

/** Bài 5 — kiểm tra tương thích LÚC KHỞI ĐỘNG: jar build cho Spark 4.2 / Scala 2.13 chạy trên cụm khác phiên bản sẽ hỏng ở chỗ khó hiểu
  * (NoSuchMethodError giữa chừng). Dừng sớm với thông báo rõ ràng.
  */
object RuntimeCheck {
  def check(spark: String, scala: String, java: String): List[String] = {
    def majorMinor(v: String) = v.split('.').take(2).mkString(".")
    val javaMajor = java.split('.').head.takeWhile(_.isDigit)
    List(
      Option.when(majorMinor(spark) != "4.2")(s"Spark $spark: jar build cho 4.2.x"),
      Option.when(majorMinor(scala) != "2.13")(s"Scala $scala: Spark 4.2 chỉ có bản Scala 2.13"),
      Option.when(!Set("17", "21", "25").contains(javaMajor))(s"Java $java: Spark 4.2 hỗ trợ 17/21/25")
    ).flatten
  }
}

/** Bài 6 — checkpoint (ghi ra thư mục bền) vs localCheckpoint (lưu ở bộ nhớ/đĩa executor): cả hai cắt plan; chỉ checkpoint tạo file. */
object CheckpointKinds {
  final case class Result(planNodes: Int, filesInCheckpointDir: Long, sum: Long)

  private def countFiles(dir: Path): Long = { val s = Files.walk(dir); try s.iterator().asScala.count(Files.isRegularFile(_)).toLong finally s.close() }

  def run(spark: SparkSession, local: Boolean, dir: Path): Result = {
    spark.sparkContext.setCheckpointDir(dir.toString)
    val before = countFiles(dir)
    var df = spark.range(0, 1000, 1, 4).toDF("v")
    (1 to 20).foreach(_ => df = df.withColumn("v", col("v") + 1))
    df = if (local) df.localCheckpoint(eager = true) else df.checkpoint(eager = true)
    Result(df.queryExecution.analyzed.collect { case p => p }.size, countFiles(dir) - before, df.agg(sum("v")).first().getLong(0))
  }
}
