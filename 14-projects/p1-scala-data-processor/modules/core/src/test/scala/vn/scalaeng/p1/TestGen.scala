package vn.scalaeng.p1

import scala.util.{Random, Try}

/** Gen/forAll tối giản như Chương 10 (seed cố định để tái hiện). */
final case class Gen[A](sample: Random => A):
  def map[B](f: A => B): Gen[B] = Gen(r => f(sample(r)))
  def flatMap[B](f: A => Gen[B]): Gen[B] = Gen(r => f(sample(r)).sample(r))

object Gen:
  def choose(lo: Int, hi: Int): Gen[Int] = Gen(r => lo + r.nextInt(hi - lo + 1))
  def oneOf[A](first: A, rest: A*): Gen[A] = { val v = (first +: rest).toVector; Gen(r => v(r.nextInt(v.size))) }
  def listOfN[A](n: Int, g: Gen[A]): Gen[List[A]] = Gen(r => List.fill(n)(g.sample(r)))
  def listOf[A](max: Int, g: Gen[A]): Gen[List[A]] = choose(0, max).flatMap(listOfN(_, g))
  def stringOf(max: Int, c: Gen[Char]): Gen[String] = listOf(max, c).map(_.mkString)
  def shuffle[A](xs: List[A]): Gen[List[A]] = Gen(r => r.shuffle(xs))

object Prop:
  def forAll[A](gen: Gen[A], runs: Int = 200, seed: Long = 42L)(p: A => Boolean): Option[String] =
    val rnd = Random(seed)
    Iterator.range(0, runs).map { i =>
      val a = gen.sample(rnd)
      if Try(p(a)).getOrElse(false) then None else Some(s"seed=$seed lần=$i input=$a")
    }.collectFirst { case Some(f) => f }
