package vn.scalaeng.order.domain

import Quantity.value

/** Tiền VND, đơn vị đồng. */
opaque type Vnd = Long
object Vnd:
  val Zero: Vnd = 0L
  def parse(raw: String): Either[String, Vnd] =
    raw.trim.toLongOption match
      case Some(v) if v > 0 => Right(v)
      case Some(v)          => Left(s"đơn giá phải > 0: $v")
      case None             => Left(s"đơn giá không phải số nguyên: '$raw'")
  def unsafe(v: Long): Vnd = v
  extension (a: Vnd)
    def +(b: Vnd): Vnd = Math.addExact(a, b)
    def *(q: Quantity): Vnd = Math.multiplyExact(a, q.value.toLong)
    def dong: Long = a

opaque type Quantity = Int
object Quantity:
  def parse(raw: String): Either[String, Quantity] =
    raw.trim.toIntOption match
      case Some(q) if q > 0 => Right(q)
      case Some(q)          => Left(s"số lượng phải > 0: $q")
      case None             => Left(s"số lượng không phải số nguyên: '$raw'")
  extension (q: Quantity) def value: Int = q
