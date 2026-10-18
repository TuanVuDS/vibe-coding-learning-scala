package scalaeng.ch08b

// BẢN SAO của 01-scala3-core/code/src/main/scala/scalaeng/ch08b/Monoids.scala
// để project lời giải tự chạy độc lập. Khi sửa bài học, hãy đồng bộ file này.

/** Monoid: một phép gộp kết hợp (associative) + phần tử trung hòa.
  * Đây là nền tảng của aggregation phân tán: gộp từng phần rồi gộp các kết quả.
  * (Thư viện Cats có sẵn Monoid/Semigroup; ở đây tự viết để hiểu cơ chế.)
  */
trait Monoid[A]:
  def empty: A
  def combine(x: A, y: A): A

object Monoid:
  def apply[A](using m: Monoid[A]): Monoid[A] = m

  given Monoid[Int] with
    def empty = 0
    def combine(x: Int, y: Int) = x + y

  given Monoid[Long] with
    def empty = 0L
    def combine(x: Long, y: Long) = x + y

  /** Gộp Map theo key: cần Monoid cho value. */
  given [K, V](using mv: Monoid[V]): Monoid[Map[K, V]] with
    def empty = Map.empty
    def combine(x: Map[K, V], y: Map[K, V]) =
      y.foldLeft(x) { case (acc, (k, v)) => acc.updated(k, acc.get(k).fold(v)(mv.combine(_, v))) }

extension [A](xs: List[A])(using m: Monoid[A])
  def combineAll: A = xs.foldLeft(m.empty)(m.combine)

/** Thống kê latency có thể gộp từ nhiều lô (nhiều file, nhiều partition). */
final case class Stats(count: Long, sum: Long, max: Long):
  def avg: Double = if count == 0 then 0.0 else sum.toDouble / count

object Stats:
  def of(latency: Long): Stats = Stats(1, latency, latency)
  given Monoid[Stats] with
    def empty = Stats(0, 0, Long.MinValue)
    def combine(x: Stats, y: Stats) = Stats(x.count + y.count, x.sum + y.sum, math.max(x.max, y.max))
