package vn.scalaeng.transform

import cats.Monoid
import cats.kernel.CommutativeMonoid
import fs2.{Pure, Stream}

/** CÙNG một phép tổng hợp chạy theo 3 MÔ HÌNH THỰC THI. Logic (key, value, Monoid) không đổi; thứ thay đổi là
  * dữ liệu nằm ở đâu và được duyệt thế nào. Đây là mô phỏng TRONG MỘT JVM để thấy ý tưởng — KHÔNG phải Spark,
  * không có mạng, không chịu lỗi máy, không có optimizer.
  */
object Execution:
  /** 1. Trong bộ nhớ: toàn bộ dữ liệu đã nằm trong một `List`. Đơn giản nhất; giới hạn bởi RAM của một máy. */
  def inMemory[A, K, V: Monoid](xs: List[A])(key: A => K)(value: A => V): Map[K, V] =
    Aggregate.by(xs)(key)(value)

  /** 2. Theo luồng: dữ liệu đến từng KHỐI (chunk); chỉ giữ bảng tổng hợp đang tích lũy. Bộ nhớ ~ số khóa, không ~ số dòng. */
  def streaming[A, K, V: Monoid](xs: Stream[Pure, A], chunkSize: Int)(key: A => K)(value: A => V): Map[K, V] =
    xs.chunkN(chunkSize).fold(Map.empty[K, V]) { (acc, chunk) =>
      Aggregate.merge(acc, Aggregate.by(chunk.toList)(key)(value)) // tổng hợp khối rồi gộp vào kết quả
    }.toList.headOption.getOrElse(Map.empty)

  /** 3. Chia partition (mô phỏng ý tưởng phân tán):
    *   (a) dữ liệu bị chia NGẪU NHIÊN thành n phần (như các file/block trên nhiều máy);
    *   (b) mỗi phần tổng hợp RIÊNG (map-side combine);
    *   (c) "shuffle": gửi từng kết quả từng phần tới partition phụ trách khóa đó (hash(key) % n);
    *   (d) mỗi partition gộp các kết quả nó nhận; ghép lại thành kết quả cuối.
    * Thứ tự và cách chia ở (a), (c) là tùy ý, nên phép gộp phải KẾT HỢP và GIAO HOÁN → đòi `CommutativeMonoid`.
    * Bản đầu chỉ đòi `Monoid`: nối chuỗi (Monoid, không giao hoán) cho "abcdef" ở bộ nhớ nhưng "adbcfe"/"befadc" khi chia 3 partition
    * với seed khác nhau (review chéo, M2). Giờ compiler từ chối luôn trường hợp đó.
    */
  def partitioned[A, K, V: CommutativeMonoid](xs: List[A], n: Int, seed: Long)(key: A => K)(value: A => V): Map[K, V] =
    require(n > 0, "n phải > 0")
    val rnd    = scala.util.Random(seed)
    val splits = xs.groupBy(_ => rnd.nextInt(n)).values.toList                       // (a)
    val local  = splits.map(part => Aggregate.by(part)(key)(value))                  // (b)
    val routed = local.flatMap(_.toList).groupBy((k, _) => Math.floorMod(k.hashCode, n)) // (c)
    routed.values.map(_.foldLeft(Map.empty[K, V])((acc, kv) => Aggregate.merge(acc, Map(kv)))) // (d)
      .foldLeft(Map.empty[K, V])(Aggregate.merge)
