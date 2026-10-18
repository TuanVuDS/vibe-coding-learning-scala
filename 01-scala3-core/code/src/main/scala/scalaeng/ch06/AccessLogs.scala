package scalaeng.ch06

import scala.collection.mutable

/** Chương 6 — Collections: phân tích access log của một backend service.
  *
  * Định dạng một dòng log (đơn giản hóa):
  *   <timestampMs> <userId|-> <method> <path> <status> <latencyMs>
  */
final case class AccessLog(
    timestampMs: Long,
    userId: String,
    method: String,
    path: String,
    status: Int,
    latencyMs: Long
):
  def isError: Boolean = status >= 500
  def isAnonymous: Boolean = userId == "-"

/** Thống kê tính trong MỘT lượt duyệt bằng foldLeft. */
final case class LatencyStats(count: Int, total: Long, max: Long):
  def average: Double = if count == 0 then 0.0 else total.toDouble / count
  def add(latency: Long): LatencyStats = LatencyStats(count + 1, total + latency, math.max(max, latency))

object LatencyStats:
  val Empty: LatencyStats = LatencyStats(0, 0L, 0L)

object AccessLogs:

  private def isNumber(s: String): Boolean = s.nonEmpty && s.forall(_.isDigit)

  /** collect = filter + map trong một bước, dùng partial function với pattern. */
  def parseAll(lines: List[String]): List[AccessLog] =
    lines
      .map(_.trim.split("\\s+"))
      .collect:
        case Array(ts, user, method, path, status, latency)
            if isNumber(ts) && isNumber(status) && isNumber(latency) =>
          AccessLog(ts.toLong, user, method, path, status.toInt, latency.toLong)

  /** Số dòng không parse được — để giám sát chất lượng dữ liệu. */
  def countInvalid(lines: List[String]): Int = lines.size - parseAll(lines).size

  /** partition: tách làm hai List theo điều kiện, một lượt duyệt. */
  def splitErrors(logs: List[AccessLog]): (List[AccessLog], List[AccessLog]) =
    logs.partition(_.isError)

  def errorRate(logs: List[AccessLog]): Double =
    if logs.isEmpty then 0.0 else logs.count(_.isError).toDouble / logs.size

  /** groupMapReduce: đếm theo key mà không tạo List trung gian. */
  def requestsByPath(logs: List[AccessLog]): Map[String, Int] =
    logs.groupMapReduce(_.path)(_ => 1)(_ + _)

  /** Sắp xếp ổn định: count giảm dần, cùng count thì path tăng dần. */
  def topPaths(logs: List[AccessLog], n: Int): List[(String, Int)] =
    requestsByPath(logs).toList.sortBy((path, count) => (-count, path)).take(n)

  /** Set: loại trùng. */
  def uniqueUsers(logs: List[AccessLog]): Set[String] =
    logs.filterNot(_.isAnonymous).map(_.userId).toSet

  /** foldLeft: gộp toàn bộ thành một giá trị tích lũy. */
  def latencyStats(logs: List[AccessLog]): LatencyStats =
    logs.foldLeft(LatencyStats.Empty)((acc, log) => acc.add(log.latencyMs))

  /** Percentile theo phương pháp nearest-rank; dùng Vector vì cần truy cập theo chỉ số. */
  def latencyPercentile(logs: List[AccessLog], p: Int): Long =
    require(p > 0 && p <= 100, s"p ngoài (0, 100]: $p")
    require(logs.nonEmpty, "không có dữ liệu")
    val sorted: Vector[Long] = logs.map(_.latencyMs).toVector.sorted
    val rank = math.ceil(p / 100.0 * sorted.size).toInt
    sorted(rank - 1)

  /** groupBy + maxBy: request chậm nhất của mỗi user. */
  def slowestPerUser(logs: List[AccessLog]): Map[String, AccessLog] =
    logs.filterNot(_.isAnonymous).groupBy(_.userId).view.mapValues(_.maxBy(_.latencyMs)).toMap

  /** flatMap: một log sinh ra 0..n phần tử (các segment của path). */
  def pathSegments(logs: List[AccessLog]): List[String] =
    logs.flatMap(_.path.split("/").filter(_.nonEmpty)).distinct.sorted

  /** flatten: gộp nhiều lô dữ liệu (ví dụ nhiều file) thành một List. */
  def mergeBatches(batches: List[List[AccessLog]]): List[AccessLog] =
    batches.flatten.sortBy(_.timestampMs)

  /** sliding: phát hiện chuỗi lỗi liên tiếp (cửa sổ trượt). */
  def hasErrorBurst(logs: List[AccessLog], window: Int, threshold: Int): Boolean =
    logs.sortBy(_.timestampMs).sliding(window).exists(_.count(_.isError) >= threshold)

  /** Builder mutable cục bộ: chấp nhận được vì không lộ ra ngoài hàm. */
  def errorPathsImperative(logs: List[AccessLog]): List[String] =
    val buffer = mutable.ListBuffer.empty[String]
    for log <- logs if log.isError do buffer += log.path
    buffer.toList

val sampleLines: List[String] = List(
  "1700000000000 u1 GET /api/products 200 35",
  "1700000000100 u2 GET /api/products/42 200 48",
  "1700000000200 -  GET /api/products 200 30",
  "1700000000300 u1 POST /api/orders 201 120",
  "1700000000400 u3 POST /api/orders 500 900",
  "dòng log hỏng không đúng định dạng",
  "1700000000500 u2 GET /api/orders/7 200 60",
  "1700000000600 u3 POST /api/orders 503 1500",
  "1700000000700 u1 GET /api/products abc 40"
)

@main def ch06Demo(): Unit =
  import AccessLogs.*
  val logs = parseAll(sampleLines)
  println(s"Parse được ${logs.size}/${sampleLines.size} dòng, lỗi định dạng: ${countInvalid(sampleLines)}")
  val (errors, ok) = splitErrors(logs)
  println(s"Lỗi 5xx: ${errors.size}, thành công: ${ok.size}")
  println(f"Error rate: ${errorRate(logs) * 100}%.1f%%")
  println(s"Top paths: ${topPaths(logs, 2)}")
  println(s"Users: ${uniqueUsers(logs).toList.sorted}")
  val stats = latencyStats(logs)
  println(f"Latency: count=${stats.count}, avg=${stats.average}%.1f ms, max=${stats.max} ms")
  println(s"p50=${latencyPercentile(logs, 50)} ms, p95=${latencyPercentile(logs, 95)} ms")
  println(s"Chậm nhất theo user: ${slowestPerUser(logs).toList.sortBy(_._1).map((u, l) => s"$u->${l.latencyMs}")}")
  println(s"Segments: ${pathSegments(logs)}")
  println(s"Có burst lỗi (2 lỗi trong 3 request)? ${hasErrorBurst(logs, 3, 2)}")
  println(s"Error paths: ${errorPathsImperative(logs)}")

  // Lazy evaluation: view chỉ tính phần tử cần thiết
  var evaluated = 0
  val firstSlow = logs.view.map { l => evaluated += 1; l }.find(_.latencyMs > 100)
  println(s"find trên view: ${firstSlow.map(_.path)}, số phần tử đã map: $evaluated / ${logs.size}")
