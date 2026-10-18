package scalaeng.solutions

/** Lời giải bài tập Chương 8A. */
object Ch08aSolutions:

  // Bài 1
  final case class Result[A](data: A, warnings: List[String]):
    def map[B](f: A => B): Result[B] = Result(f(data), warnings)
    def addWarning(w: String): Result[A] = copy(warnings = warnings :+ w)

  // Bài 2
  object EmailType:
    opaque type Email = String
    object Email:
      def parse(raw: String): Either[String, Email] =
        val s = raw.trim.toLowerCase
        s.split("@", -1) match
          case Array(local, domain) if local.nonEmpty && domain.contains('.') && !domain.startsWith(".") && !domain.endsWith(".") =>
            Right(s)
          case _ => Left(s"Email không hợp lệ: '$raw'")
    extension (e: Email)
      def domain: String = e.substring(e.indexOf('@') + 1)
      def asString: String = e

  // Bài 3
  object PercentType:
    opaque type Percent = Int
    object Percent:
      def parse(p: Int): Either[String, Percent] =
        if p >= 0 && p <= 100 then Right(p) else Left(s"Percent ngoài [0, 100]: $p")
    extension (p: Percent)
      /** Làm tròn xuống; dùng Math.multiplyExact để báo lỗi thay vì tràn số âm thầm. */
      def applyTo(amount: Long): Long = Math.multiplyExact(amount, p.toLong) / 100

  // Bài 4
  trait HasUpdatedAt:
    def updatedAt: Long

  def maxByField[A <: HasUpdatedAt](rows: List[A]): Option[A] = rows.maxByOption(_.updatedAt)

  // Bài 5
  sealed trait Event
  final case class OrderPlaced(id: String) extends Event
  final case class UserSignedUp(id: String) extends Event

  trait Pipeline[+A]:
    def run(): List[A]
  trait Sink[-A]:
    def write(a: A): String

  def connect[A](p: Pipeline[A], s: Sink[A]): List[String] = p.run().map(s.write)
