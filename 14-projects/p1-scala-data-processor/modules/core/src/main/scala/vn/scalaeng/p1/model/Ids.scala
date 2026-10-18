package vn.scalaeng.p1.model

opaque type OrderId = String
object OrderId:
  def parse(raw: String): Either[String, OrderId] =
    val s = raw.trim.toUpperCase
    if s.matches("ORD-[0-9]{1,10}") then Right(s) else Left(s"'$raw' không đúng dạng ORD-<số>")
  extension (x: OrderId) def value: String = x

opaque type UserId = String
object UserId:
  def parse(raw: String): Either[String, UserId] =
    val s = raw.trim.toUpperCase
    if s.matches("U-[0-9]{1,10}") then Right(s) else Left(s"'$raw' không đúng dạng U-<số>")
  extension (x: UserId) def value: String = x

opaque type Sku = String
object Sku:
  def parse(raw: String): Either[String, Sku] =
    val s = raw.trim.toUpperCase
    if s.matches("[A-Z0-9][A-Z0-9-]{0,31}") then Right(s) else Left(s"'$raw' không phải SKU hợp lệ")
  extension (x: Sku) def value: String = x
