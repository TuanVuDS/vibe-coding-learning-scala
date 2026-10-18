package scalaeng.ch03

/** Chương 3 — tham quan các kiểu cơ bản và đặc điểm của chúng. */
object TypesTour:

  val anInt: Int         = 42
  val aLong: Long        = 3_000_000_000L   // vượt giới hạn Int nên cần Long
  val aDouble: Double    = 0.1 + 0.2        // sai số dấu phẩy động!
  val aDecimal: BigDecimal = BigDecimal("0.1") + BigDecimal("0.2")
  val aBoolean: Boolean  = anInt > 10
  val aChar: Char        = 'S'
  val aString: String    = "Scala"
  val aUnit: Unit        = ()               // giá trị duy nhất của kiểu Unit

  /** Int bị tràn số (overflow) mà không báo lỗi. */
  val overflow: Int = Int.MaxValue + 1

  /** Mọi kiểu đều là con của Any; vì vậy List này có kiểu List[Any]. */
  val mixed: List[Any] = List(anInt, aString, aBoolean, aUnit)

  def describe(value: Any): String = value match
    case i: Int     => s"Int($i)"
    case s: String  => s"String($s)"
    case b: Boolean => s"Boolean($b)"
    case ()         => "Unit"
    case other      => s"Khác(${other.getClass.getSimpleName})"

@main def ch03Types(): Unit =
  import TypesTour.*
  println(s"0.1 + 0.2 (Double)     = $aDouble")
  println(s"0.1 + 0.2 (BigDecimal) = $aDecimal")
  println(s"Int.MaxValue + 1       = $overflow")
  println(s"Long lớn               = $aLong")
  println(mixed.map(describe).mkString(", "))
