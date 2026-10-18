package jvmlab

/** Thí nghiệm 3: opaque type thực sự "không tốn chi phí" — xem chữ ký bytecode bằng reflection. */
object Prices:
  opaque type Vnd = Long
  object Vnd:
    def apply(d: Long): Vnd = d
  extension (a: Vnd) def plus(b: Vnd): Vnd = a + b

final case class MoneyBox(amount: Long)

object Api:
  def totalOpaque(a: Prices.Vnd, b: Prices.Vnd): Prices.Vnd = Prices.plus(a)(b)
  def totalBoxed(a: MoneyBox, b: MoneyBox): MoneyBox = MoneyBox(a.amount + b.amount)

@main def opaqueErasure(): Unit =
  val cls = Class.forName("jvmlab.Api$")
  for m <- cls.getDeclaredMethods.sortBy(_.getName) if m.getName.startsWith("total") do
    println(s"${m.getName}(${m.getParameterTypes.map(_.getName).mkString(", ")}): ${m.getReturnType.getName}")
