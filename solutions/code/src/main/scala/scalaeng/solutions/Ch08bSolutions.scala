package scalaeng.solutions

import scalaeng.ch08b.Monoid
import scalaeng.ch08b.combineAll

/** Lời giải bài tập Chương 8B. Dùng lại `Monoid` của bài học (bản sao trong solutions/code/src/main/scala/scalaeng/ch08b/). */
object Ch08bSolutions:

  // Bài 1
  trait Show[A]:
    def show(a: A): String

  object Show:
    given Show[Int] with
      def show(a: Int) = a.toString
    given Show[String] with
      def show(a: String) = "\"" + a + "\""
    given Show[Boolean] with
      def show(a: Boolean) = if a then "có" else "không"
    given [A](using s: Show[A]): Show[List[A]] with
      def show(xs: List[A]) = xs.map(s.show).mkString("[", ", ", "]")

  def display[A: Show](a: A): String = summon[Show[A]].show(a)

  // Bài 2: instance trong companion -> không cần import
  final case class Version(major: Int, minor: Int, patch: Int)
  object Version:
    given Ordering[Version] = Ordering.by(v => (v.major, v.minor, v.patch))

  // Bài 3
  given optionMonoid[A](using m: Monoid[A]): Monoid[Option[A]] with
    def empty: Option[A] = None
    def combine(x: Option[A], y: Option[A]): Option[A] = (x, y) match
      case (Some(a), Some(b)) => Some(m.combine(a, b))
      case (Some(_), None)    => x
      case (None, _)          => y

  given tupleMonoid[A, B](using ma: Monoid[A], mb: Monoid[B]): Monoid[(A, B)] with
    def empty: (A, B) = (ma.empty, mb.empty)
    def combine(x: (A, B), y: (A, B)): (A, B) = (ma.combine(x._1, y._1), mb.combine(x._2, y._2))

  // Bài 4
  trait Validator[A]:
    def validate(a: A): List[String]

  final case class Signup(name: String, age: Int)
  object Signup:
    given Validator[Signup] with
      def validate(s: Signup): List[String] =
        List(
          Option.when(s.name.trim.isEmpty)("name rỗng"),
          Option.when(s.age < 13)(s"tuổi ${s.age} < 13")
        ).flatten

  def validateAll[A: Validator](xs: List[A]): Map[Int, List[String]] =
    xs.zipWithIndex.flatMap { (a, i) =>
      val errs = summon[Validator[A]].validate(a)
      if errs.isEmpty then None else Some(i -> errs)
    }.toMap

  // Bài 5: gộp cục bộ từng lô (như mỗi partition), rồi gộp các Map (như sau shuffle)
  def aggregateByKey[K, V](batches: List[List[(K, V)]])(using m: Monoid[V]): Map[K, V] =
    val partials: List[Map[K, V]] = batches.map { batch =>
      batch.groupMapReduce(_._1)(_._2)(m.combine)
    }
    partials.combineAll
