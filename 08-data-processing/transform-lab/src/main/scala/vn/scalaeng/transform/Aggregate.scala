package vn.scalaeng.transform

import cats.Monoid
import cats.kernel.CommutativeMonoid
import cats.syntax.all.*

/** Thống kê có thể GỘP: đếm, tổng, min, max. Là một Monoid — có phần tử trung hòa (`empty`) và phép gộp KẾT HỢP
  * (`combine(combine(a, b), c) == combine(a, combine(b, c))`) — và còn GIAO HOÁN (`a |+| b == b |+| a`).
  * Kết hợp: tính được theo khối CÓ thứ tự. Giao hoán thêm: tính được theo partition KHÔNG có thứ tự (Spark).
  *
  * `sum` là `BigInt`: với `Long` + `Math.addExact`, `(MaxValue |+| 1) |+| -1` ném lỗi tràn nhưng `MaxValue |+| (1 |+| -1)` thì không —
  * cách đặt ngoặc đổi cả việc thành công hay thất bại, tức KHÔNG còn là Monoid (review chéo, M3). BigInt làm phép cộng toàn phần.
  */
final case class Stats(count: Long, sum: BigInt, min: Long, max: Long):
  /** Trung bình tính từ (sum, count) — ĐÚNG khi gộp nhiều phần. */
  def mean: Option[BigDecimal] = Option.when(count > 0)(BigDecimal(sum) / count)

object Stats:
  def of(v: Long): Stats = Stats(1, v, v, v)

  given CommutativeMonoid[Stats] with
    def empty: Stats = Stats(0, BigInt(0), Long.MaxValue, Long.MinValue)
    def combine(a: Stats, b: Stats): Stats =
      Stats(a.count + b.count, a.sum + b.sum, a.min.min(b.min), a.max.max(b.max)) // count: Long đủ cho 2^63 dòng

object Aggregate:
  /** Tổng hợp TỔNG QUÁT: nhóm theo khóa K, đổi mỗi phần tử thành V, gộp bằng Monoid[V]. Một hàm cho mọi loại tổng hợp. */
  def by[A, K, V: Monoid](xs: Iterable[A])(key: A => K)(value: A => V): Map[K, V] =
    xs.foldLeft(Map.empty[K, V]) { (acc, a) =>
      val k = key(a)
      acc.updated(k, acc.getOrElse(k, Monoid[V].empty) |+| value(a))
    }

  /** Gộp hai bảng tổng hợp từng phần (dùng sau khi tính trên nhiều khối/partition). */
  def merge[K, V: Monoid](a: Map[K, V], b: Map[K, V]): Map[K, V] =
    b.foldLeft(a) { case (acc, (k, v)) => acc.updated(k, acc.getOrElse(k, Monoid[V].empty) |+| v) }

  /** CỐ Ý SAI: "trung bình của các trung bình". Không phải phép gộp hợp lệ — kết quả phụ thuộc cách chia dữ liệu. */
  def averageOfAverages(parts: List[List[Long]]): BigDecimal =
    val means = parts.filter(_.nonEmpty).map(p => BigDecimal(p.sum) / p.size)
    means.sum / means.size
