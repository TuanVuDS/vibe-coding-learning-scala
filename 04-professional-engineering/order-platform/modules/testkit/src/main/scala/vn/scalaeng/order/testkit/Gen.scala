package vn.scalaeng.order.testkit

import scala.util.Random

/** Bộ sinh dữ liệu ngẫu nhiên TỐI GIẢN để hiểu cơ chế property-based testing.
  * Thư viện thật (ScalaCheck) có thêm: shrinking, phân bố thông minh, tích hợp test runner.
  */
final case class Gen[A](sample: Random => A):
  def map[B](f: A => B): Gen[B] = Gen(r => f(sample(r)))
  def flatMap[B](f: A => Gen[B]): Gen[B] = Gen(r => f(sample(r)).sample(r))

object Gen:
  def const[A](a: A): Gen[A] = Gen(_ => a)
  def choose(lo: Int, hi: Int): Gen[Int] = Gen(r => lo + r.nextInt(hi - lo + 1))
  def oneOf[A](first: A, rest: A*): Gen[A] =
    val all = (first +: rest).toVector
    Gen(r => all(r.nextInt(all.size)))
  /** Chọn generator theo trọng số: ví dụ 70% dòng hợp lệ, 30% dòng lỗi. */
  def frequency[A](weighted: (Int, Gen[A])*): Gen[A] =
    val total = weighted.map(_._1).sum
    Gen { r =>
      val pick = r.nextInt(total)
      val (_, g) = weighted.foldLeft((0, Option.empty[Gen[A]])) { case ((acc, found), (w, g)) =>
        if found.isEmpty && pick < acc + w then (acc + w, Some(g)) else (acc + w, found)
      }
      g.get.sample(r)
    }
  def listOfN[A](n: Int, g: Gen[A]): Gen[List[A]] = Gen(r => List.fill(n)(g.sample(r)))
  def listOf[A](maxSize: Int, g: Gen[A]): Gen[List[A]] = choose(0, maxSize).flatMap(listOfN(_, g))
  def stringOf(maxLen: Int, chars: Gen[Char]): Gen[String] = listOf(maxLen, chars).map(_.mkString)
