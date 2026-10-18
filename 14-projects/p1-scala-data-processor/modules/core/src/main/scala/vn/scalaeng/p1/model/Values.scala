package vn.scalaeng.p1.model

/** Tiền VND (đơn vị đồng). Chấp nhận "120000", "120.000", "120,000", có thể kèm "VND"/"đ".
  * Dấu . hoặc , CHỈ được dùng làm phân cách hàng nghìn (nhóm 3 chữ số) — VND không có phần lẻ.
  */
opaque type Vnd = Long
object Vnd:
  val Zero: Vnd = 0L
  private val Grouped = """[1-9][0-9]{0,2}([.,][0-9]{3})+""".r
  private val Plain   = """[1-9][0-9]*""".r
  def parse(raw: String): Either[String, Vnd] =
    val s = raw.trim.toLowerCase.stripSuffix("vnd").stripSuffix("đ").trim
    val digits = s match
      case Grouped(_*) if s.contains('.') && s.contains(',') => None   // trộn hai loại phân cách
      case Grouped(_*) => Some(s.filter(_.isDigit))
      case Plain()     => Some(s)
      case _           => None
    digits.flatMap(_.toLongOption).filter(_ <= 10_000_000_000L).toRight(s"'$raw' không phải số tiền VND hợp lệ")
  def unsafe(v: Long): Vnd = v
  extension (a: Vnd)
    def dong: Long = a
    def plus(b: Vnd): Vnd = Math.addExact(a, b)
    def times(q: Quantity): Vnd = Math.multiplyExact(a, Quantity.toInt(q).toLong)

opaque type Quantity = Int
object Quantity:
  val Max = 1000
  def parse(raw: String): Either[String, Quantity] =
    val s = raw.trim
    if !s.forall(_.isDigit) || s.isEmpty then Left(s"'$raw' không phải số nguyên dương")
    else s.toIntOption.filter(q => q >= 1 && q <= Max).toRight(s"'$raw' ngoài khoảng 1..$Max")
  def toInt(q: Quantity): Int = q
  extension (q: Quantity) def count: Int = q

enum OrderStatus:
  case Paid, Cancelled, Refunded
object OrderStatus:
  def parse(raw: String): Either[String, OrderStatus] =
    OrderStatus.values.find(_.toString.equalsIgnoreCase(raw.trim)).toRight(s"'$raw' không thuộc PAID|CANCELLED|REFUNDED")
