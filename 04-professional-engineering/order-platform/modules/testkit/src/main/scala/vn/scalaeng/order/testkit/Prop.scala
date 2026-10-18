package vn.scalaeng.order.testkit

import scala.util.{Random, Try}

/** Kiểm tra một thuộc tính trên nhiều input ngẫu nhiên với seed CỐ ĐỊNH -> lỗi tái hiện được. */
object Prop:
  final case class Failure(seed: Long, iteration: Int, input: String, reason: String)

  def forAll[A](gen: Gen[A], runs: Int = 200, seed: Long = 42L)(property: A => Boolean): Option[Failure] =
    val rnd = Random(seed)
    Iterator.range(0, runs).map { i =>
      val input = gen.sample(rnd)
      Try(property(input)).toEither match
        case Right(true)  => None
        case Right(false) => Some(Failure(seed, i, input.toString, "property trả về false"))
        case Left(e)      => Some(Failure(seed, i, input.toString, s"exception: $e"))
    }.collectFirst { case Some(f) => f }
