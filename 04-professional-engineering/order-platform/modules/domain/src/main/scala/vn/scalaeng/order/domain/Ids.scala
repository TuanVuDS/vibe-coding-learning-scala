package vn.scalaeng.order.domain

opaque type OrderId = String
object OrderId:
  def parse(raw: String): Either[String, OrderId] =
    val s = raw.trim
    if s.matches("ORD-[0-9]+") then Right(s) else Left(s"order_id không hợp lệ: '$raw'")
  extension (id: OrderId) def value: String = id

opaque type Sku = String
object Sku:
  def parse(raw: String): Either[String, Sku] =
    val s = raw.trim
    if s.nonEmpty && s.length <= 32 then Right(s) else Left(s"sku không hợp lệ: '$raw'")
  extension (sku: Sku) def value: String = sku
