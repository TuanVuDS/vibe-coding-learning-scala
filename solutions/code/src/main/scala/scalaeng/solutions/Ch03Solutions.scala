package scalaeng.solutions

import scala.math.BigDecimal.RoundingMode

/** Lời giải bài tập Chương 3. Hãy tự làm trước khi đọc! */
object Ch03Solutions:

  // Bài 1
  def describeProduct(name: String, price: BigDecimal, quantity: Int): String =
    s"$name x$quantity = ${price * quantity}"

  // Bài 2: số điện thoại di động VN dạng nội địa: đúng 10 chữ số, bắt đầu bằng 0
  def isValidPhone(raw: String): Boolean =
    val digits = raw.trim
    digits.length == 10 && digits.startsWith("0") && digits.forall(_.isDigit)

  // Bài 3: làm tròn tới đồng theo HALF_UP
  def vatAmount(subtotal: BigDecimal, rate: BigDecimal): BigDecimal =
    (subtotal * rate).setScale(0, RoundingMode.HALF_UP)

  // Bài 4: phân loại HTTP status code
  def classifyStatus(status: Int): String = status match
    case s if s >= 100 && s < 200 => "informational"
    case s if s >= 200 && s < 300 => "success"
    case s if s >= 300 && s < 400 => "redirection"
    case s if s >= 400 && s < 500 => "client-error"
    case s if s >= 500 && s < 600 => "server-error"
    case _                        => "unknown"

  // Bài 5: parse một dòng CSV đơn giản "SKU, đơn giá, số lượng"
  def parseOrderLine(line: String): (String, BigDecimal, Int) =
    val parts = line.split(",").map(_.trim)
    if parts.length != 3 then
      throw IllegalArgumentException(s"Cần 3 cột, nhận ${parts.length}: '$line'")
    val sku   = parts(0)
    val price = parts(1).toDoubleOption match
      case Some(_) => BigDecimal(parts(1))
      case None    => throw IllegalArgumentException(s"Đơn giá không hợp lệ: '${parts(1)}'")
    val qty   = parts(2).toIntOption.getOrElse(
      throw IllegalArgumentException(s"Số lượng không hợp lệ: '${parts(2)}'")
    )
    if sku.isEmpty then throw IllegalArgumentException("SKU rỗng")
    if price < 0 || qty <= 0 then throw IllegalArgumentException(s"Giá trị âm/0: '$line'")
    (sku, price, qty)
