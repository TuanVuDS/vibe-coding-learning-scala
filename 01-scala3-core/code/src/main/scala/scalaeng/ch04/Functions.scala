package scalaeng.ch04

import scala.annotation.tailrec

/** Chương 4 — Functions và Methods.
  *
  * Ví dụ xoay quanh việc làm sạch dữ liệu người dùng (backend) và
  * tổng hợp click events (data engineering).
  */

/** Một bản ghi click. `case class` sẽ học kỹ ở Chương 5; ở đây chỉ cần biết
  * nó là một "record" bất biến có các field có tên.
  */
final case class ClickEvent(userId: String, page: String, durationMs: Long)

object TextCleaning:
  // Function values: hàm là giá trị, có kiểu String => String
  val trim: String => String           = _.trim
  val lower: String => String          = _.toLowerCase
  val collapseSpaces: String => String = _.replaceAll("\\s+", " ")

  // Function composition: andThen chạy từ trái sang phải
  val normalizeName: String => String  = trim.andThen(collapseSpaces)
  val normalizeEmail: String => String = trim.andThen(lower)

  // compose chạy từ phải sang trái: lower(trim(x))
  val normalizeEmailCompose: String => String = lower.compose(trim)

  /** Gộp một danh sách bước làm sạch thành một hàm duy nhất. */
  def pipeline(steps: List[String => String]): String => String =
    steps.foldLeft(identity[String])(_ andThen _)

object HigherOrder:
  /** Higher-order function: nhận một hàm làm tham số. */
  def applyTwice(f: Int => Int, x: Int): Int = f(f(x))

  /** Higher-order function: trả về một hàm. */
  def multiplier(factor: Int): Int => Int = x => x * factor

  /** Multiple parameter lists (currying). Cố định `rate`, nhận `price` sau. */
  def discount(rate: BigDecimal)(price: BigDecimal): BigDecimal =
    price - price * rate

  /** Default và named parameters. */
  def paginate(total: Int, pageSize: Int = 20, page: Int = 1): String =
    val pages = if total == 0 then 0 else (total + pageSize - 1) / pageSize
    s"page $page/$pages (size=$pageSize, total=$total)"

  /** By-name parameter (`=> A`): `block` chỉ được đánh giá khi được gọi.
    * `clock` được truyền vào để test có thể thay bằng đồng hồ giả.
    */
  def timed[A](clock: () => Long)(block: => A): (A, Long) =
    val start  = clock()
    val result = block
    val end    = clock()
    (result, end - start)

object Recursion:
  /** Đệ quy thường: mỗi lần gọi chiếm một stack frame. */
  def factorial(n: Int): BigInt =
    if n <= 1 then BigInt(1) else n * factorial(n - 1)

  /** Đệ quy đuôi: `@tailrec` yêu cầu compiler chuyển thành vòng lặp.
    * Nếu hàm không thật sự tail-recursive, compiler báo lỗi.
    */
  def totalDuration(events: List[ClickEvent]): Long =
    @tailrec
    def loop(remaining: List[ClickEvent], acc: Long): Long = remaining match
      case Nil          => acc
      case head :: tail => loop(tail, acc + head.durationMs)
    loop(events, 0L)

  /** Phiên bản không tail-recursive — sẽ StackOverflowError với list rất dài. */
  def totalDurationNaive(events: List[ClickEvent]): Long = events match
    case Nil          => 0L
    case head :: tail => head.durationMs + totalDurationNaive(tail)

object ClickAnalytics:
  /** Kết hợp các higher-order function của collection để tổng hợp dữ liệu. */
  def longClicks(events: List[ClickEvent], minMs: Long): List[ClickEvent] =
    events.filter(_.durationMs >= minMs)

  def durationByPage(events: List[ClickEvent]): Map[String, Long] =
    events.groupMapReduce(_.page)(_.durationMs)(_ + _)

@main def ch04Demo(): Unit =
  import TextCleaning.*, HigherOrder.*, Recursion.*, ClickAnalytics.*

  println(s"[${normalizeName("   Trần    Thị   Bình  ")}]")
  println(normalizeEmail("  An.Nguyen@Example.COM "))
  println(s"applyTwice(+3, 10) = ${applyTwice(_ + 3, 10)}")
  val triple = multiplier(3)
  println(s"triple(7) = ${triple(7)}")

  val tenPercentOff: BigDecimal => BigDecimal = discount(BigDecimal("0.1"))
  println(s"Giảm 10% của 250000 = ${tenPercentOff(BigDecimal(250_000))}")
  println(paginate(total = 95))
  println(paginate(total = 95, page = 3, pageSize = 50))

  val events = List(
    ClickEvent("u1", "/home", 1200),
    ClickEvent("u2", "/product/42", 5400),
    ClickEvent("u1", "/product/42", 3100),
    ClickEvent("u3", "/cart", 800)
  )
  println(s"Tổng thời gian = ${totalDuration(events)} ms")
  println(s"Click dài (>= 3000ms) = ${longClicks(events, 3000).map(_.userId)}")
  println(s"Theo trang = ${durationByPage(events).toList.sortBy(_._1)}")
  println(s"20! = ${factorial(20)}")

  val bigList = List.fill(200_000)(ClickEvent("u", "/p", 1))
  println(s"Tail-recursive trên 200000 phần tử = ${totalDuration(bigList)}")
