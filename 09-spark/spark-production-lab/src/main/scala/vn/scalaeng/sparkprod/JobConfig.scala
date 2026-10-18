package vn.scalaeng.sparkprod

import java.time.LocalDate
import scala.util.Try

/** Cấu hình của MỘT lần chạy. Không có giá trị mặc định cho đường dẫn: thiếu là lỗi khởi động, không âm thầm đọc/ghi chỗ khác. */
final case class JobConfig(
    input: String,
    customers: String,
    output: String,
    runDate: LocalDate,
    maxRejectRate: Double,
    shufflePartitions: Int,
    maxAttempts: Int,
    runId: String
)

object JobConfig {
  val Keys: Seq[String] = Seq("input", "customers", "output", "run-date", "max-reject-rate", "shuffle-partitions", "max-attempts", "run-id")

  /** Nguồn cấu hình theo thứ tự ưu tiên: tham số dòng lệnh `--key=value` > biến môi trường `CH26_KEY` > mặc định (chỉ cho tham số an toàn).
    * Gom MỌI lỗi một lần (như Validated, Chương 20) — người vận hành sửa một lượt, không phải chạy lại năm lần.
    */
  def parse(args: Seq[String], env: Map[String, String], newRunId: () => String): Either[List[String], JobConfig] = {
    val (pairs, malformed) = args.partition(a => a.startsWith("--") && a.contains("="))
    val kvs      = pairs.map { a => val (k, v) = a.drop(2).span(_ != '='); k -> v.drop(1) }
    val fromArgs = kvs.toMap
    val unknown  = fromArgs.keySet -- Keys
    // `--k=` có mặt nhưng rỗng KHÔNG phải "vắng mặt": người gõ có ý định đặt giá trị — báo lỗi thay vì âm thầm dùng mặc định.
    // `--k` lặp lại: không đoán bản nào đúng (review T16 m1). Biến môi trường rỗng vẫn coi như không đặt (thói quen phổ biến của shell/CI).
    val duplicated = kvs.groupBy(_._1).collect { case (k, vs) if vs.size > 1 => k }.toList.sorted
    val emptyArgs  = kvs.collect { case (k, v) if v.trim.isEmpty && Keys.contains(k) => k }.distinct.sorted
    def get(k: String): Option[String] =
      fromArgs.get(k).orElse(env.get("CH26_" + k.toUpperCase.replace('-', '_'))).map(_.trim).filter(_.nonEmpty)
    def required(k: String): Either[String, String] = get(k).toRight(s"thiếu --$k (hoặc CH26_${k.toUpperCase.replace('-', '_')})")
    def parsed[A](k: String, default: Option[A])(f: String => Option[A], rule: String): Either[String, A] =
      get(k) match {
        case None    => default.toRight(s"thiếu --$k")
        case Some(v) => f(v).toRight(s"--$k='$v': $rule")
      }

    val input     = required("input")
    val customers = required("customers")
    val output    = required("output")
    val runDate   = parsed[LocalDate]("run-date", None)(v => Try(LocalDate.parse(v)).toOption, "ngày dạng yyyy-MM-dd")
    val rate      = parsed[Double]("max-reject-rate", Some(0.05))(v => v.toDoubleOption.filter(r => !r.isNaN && r >= 0 && r <= 1), "số thực trong [0, 1]")
    val shuffle   = parsed[Int]("shuffle-partitions", Some(4))(v => v.toIntOption.filter(n => n >= 1 && n <= 10000), "số nguyên 1..10000")
    val attempts  = parsed[Int]("max-attempts", Some(3))(v => v.toIntOption.filter(n => n >= 1 && n <= 10), "số nguyên 1..10")
    val runId     = parsed[String]("run-id", Some(newRunId()))(v => Option(v).filter(_.matches("[A-Za-z0-9_.-]{1,64}")), "1..64 ký tự [A-Za-z0-9_.-]")

    val errors = malformed.map(a => s"tham số '$a' phải có dạng --key=value").toList ++
      unknown.toList.sorted.map(k => s"tham số lạ --$k") ++
      duplicated.map(k => s"tham số --$k lặp lại") ++
      emptyArgs.map(k => s"--$k= rỗng (bỏ hẳn tham số để dùng mặc định)") ++
      List(input, customers, output, runDate, rate, shuffle, attempts, runId).collect { case Left(e) => e }
        .filterNot(e => emptyArgs.exists(k => e.startsWith(s"thiếu --$k"))) // đã báo "rỗng" ở trên, không báo thêm "thiếu"
    if (errors.nonEmpty) Left(errors)
    else Right(JobConfig(input.toOption.get, customers.toOption.get, output.toOption.get, runDate.toOption.get, rate.toOption.get,
      shuffle.toOption.get, attempts.toOption.get, runId.toOption.get))
  }
}
