package scalaeng.ch03

/** Chương 3 — Variables, Types và Expressions.
  *
  * Ví dụ thực tế: tính toán một đơn hàng đơn giản của một cửa hàng online.
  * Tiền dùng `BigDecimal` (không dùng `Double`) để tránh sai số dấu phẩy động.
  */
object OrderBasics:

  /** Ngưỡng miễn phí vận chuyển (giá trị minh họa, không phải chính sách thật). */
  val FreeShippingThreshold: BigDecimal = BigDecimal(500_000)
  val StandardShippingFee: BigDecimal   = BigDecimal(30_000)

  /** `if` trong Scala là một expression: nó trả về giá trị. */
  def shippingFee(subtotal: BigDecimal): BigDecimal =
    if subtotal >= FreeShippingThreshold then BigDecimal(0)
    else StandardShippingFee

  def lineTotal(unitPrice: BigDecimal, quantity: Int): BigDecimal =
    unitPrice * quantity

  /** `match` cũng là expression. Guard (`if ...`) giúp phân loại theo khoảng. */
  def quantityCategory(quantity: Int): String = quantity match
    case q if q <= 0  => "invalid"
    case 1            => "single"
    case q if q < 10  => "small"
    case _            => "bulk"

  /** Đọc số lượng từ chuỗi do người dùng nhập.
    * `toIntOption` trả về `Option[Int]`; chuỗi lỗi sẽ thành 0.
    * (Option sẽ được học kỹ ở Chương 7.)
    */
  def parseQuantity(raw: String): Int =
    raw.trim.toIntOption.getOrElse(0)

  /** Định dạng số có dấu phân cách hàng nghìn.
    * Phải chỉ định Locale: với Locale mặc định là vi_VN, dấu phân cách sẽ là "."
    * thay vì "," và output sẽ khác nhau giữa các máy.
    * `setScale(0, HALF_UP)` làm tròn trước khi in để không âm thầm cắt phần lẻ.
    */
  def formatVnd(amount: BigDecimal): String =
    val rounded = amount.setScale(0, BigDecimal.RoundingMode.HALF_UP).toLong
    "%,d VND".formatLocal(java.util.Locale.US, rounded)

  /** Kiểu `Nothing`: method này không bao giờ trả về giá trị bình thường. */
  def invalidInput(message: String): Nothing =
    throw IllegalArgumentException(message)

  /** Tính tổng tiền một giỏ hàng: List các cặp (đơn giá, số lượng). */
  def subtotal(items: List[(BigDecimal, Int)]): BigDecimal =
    items.map((price, qty) => lineTotal(price, qty)).sum

  /** Cùng phép tính nhưng dùng `var` và vòng lặp — để so sánh phong cách. */
  def subtotalImperative(items: List[(BigDecimal, Int)]): BigDecimal =
    var total = BigDecimal(0)
    for (price, qty) <- items do
      total = total + lineTotal(price, qty)
    total

  def orderTotal(items: List[(BigDecimal, Int)]): BigDecimal =
    val sub = subtotal(items)
    sub + shippingFee(sub)

@main def ch03Demo(): Unit =
  import OrderBasics.*

  val customerName = "Nguyễn Văn An"           // String, kiểu được suy luận
  val items: List[(BigDecimal, Int)] = List(    // kiểu khai báo tường minh
    (BigDecimal(120_000), 2),                    // áo thun x2
    (BigDecimal(85_000), parseQuantity(" 3 ")) // tất x3, số lượng từ input
  )

  val sub   = subtotal(items)
  val total = orderTotal(items)

  println(s"Khách hàng: $customerName")
  println(s"Số dòng hàng: ${items.size}")
  println(s"Tạm tính: ${formatVnd(sub)}")
  println(s"Phí ship: ${formatVnd(shippingFee(sub))}")
  println(s"Tổng cộng: ${formatVnd(total)}")
  println(s"Phân loại số lượng 3: ${quantityCategory(3)}")
  println(s"Phân loại số lượng 25: ${quantityCategory(25)}")
  println(s"Input lỗi 'abc' -> ${parseQuantity("abc")}")
  println(s"Imperative == functional: ${subtotalImperative(items) == sub}")
