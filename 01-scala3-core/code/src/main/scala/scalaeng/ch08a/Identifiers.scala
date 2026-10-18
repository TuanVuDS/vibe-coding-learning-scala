package scalaeng.ch08a

/** Chương 8A — Opaque types: kiểu riêng biệt lúc biên dịch, KHÔNG tốn chi phí lúc chạy.
  * Bên ngoài object này, UserId KHÔNG phải String — không thể nhầm lẫn với OrderId.
  */
object Identifiers:
  opaque type UserId = String
  opaque type OrderId = String

  object UserId:
    /** Smart constructor: cách duy nhất để tạo UserId từ bên ngoài. */
    def parse(raw: String): Either[String, UserId] =
      val s = raw.trim
      if s.matches("U-[0-9]{1,10}") then Right(s) else Left(s"UserId không hợp lệ: '$raw'")
    /** Dùng cho dữ liệu đã chắc chắn hợp lệ (ví dụ đọc từ DB). */
    def unsafe(raw: String): UserId = raw

  object OrderId:
    def parse(raw: String): Either[String, OrderId] =
      val s = raw.trim
      if s.startsWith("ORD-") && s.length > 4 then Right(s) else Left(s"OrderId không hợp lệ: '$raw'")

  extension (id: UserId) def value: String = id
  extension (id: OrderId) def orderValue: String = id

object Money:
  /** Tiền VND lưu dưới dạng Long (đơn vị: đồng). Lúc chạy chỉ là một Long — không cấp phát đối tượng. */
  opaque type Vnd = Long

  object Vnd:
    val Zero: Vnd = 0L
    def apply(dong: Long): Either[String, Vnd] =
      if dong >= 0 then Right(dong) else Left(s"Số tiền âm: $dong")
    def unsafe(dong: Long): Vnd = dong

  /** Extension methods: thêm phép toán cho Vnd mà vẫn giữ nó là kiểu riêng. */
  extension (a: Vnd)
    def +(b: Vnd): Vnd = a + b
    def *(qty: Int): Vnd = a * qty
    def dong: Long = a
    def >=(b: Vnd): Boolean = a >= b
    def format: String = "%,d VND".formatLocal(java.util.Locale.US, a)

/** Extension method cho kiểu có sẵn (String) — không cần wrapper hay kế thừa. */
object StringSyntax:
  extension (s: String)
    def normalizeEmail: String = s.trim.toLowerCase
    def isBlankOrEmpty: Boolean = s.trim.isEmpty
