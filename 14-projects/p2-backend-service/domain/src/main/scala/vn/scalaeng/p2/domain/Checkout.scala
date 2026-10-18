package vn.scalaeng.p2.domain

final case class Product(id: Long, sku: String, name: String, price: Long, stock: Int, active: Boolean)
final case class ProductDraft(sku: String, name: String, price: Long, stock: Int)
final case class Line(productId: Long, quantity: Int)
final case class PricedLine(productId: Long, quantity: Int, price: Long)
final case class Account(user: User, hash: String, role: String)
final case class Principal(id: UserId, admin: Boolean)

object Checkout:
  def merge(lines: List[Line]): Either[DomainError, Map[Long, Int]] =
    if lines.isEmpty || lines.size > 100 || lines.exists(l => l.productId <= 0 || l.quantity < 1 || l.quantity > 1000) then
      Left(DomainError.Invalid(List("đơn cần 1..100 dòng; id dương; quantity 1..1000")))
    else
      val merged = lines.groupMapReduce(_.productId)(_.quantity.toLong)(_ + _)
      if merged.values.exists(_ > 1000) then Left(DomainError.Invalid(List("tổng quantity mỗi sản phẩm tối đa 1000")))
      else Right(merged.view.mapValues(_.toInt).toMap)

  def price(wanted: Map[Long, Int], products: List[Product]): Either[DomainError, (Money, List[PricedLine])] =
    val found = products.filter(_.active).map(p => p.id -> p).toMap
    if wanted.keys.exists(id => !found.contains(id)) then Left(DomainError.MissingProduct)
    else if wanted.exists((id, q) => found(id).stock < q) then Left(DomainError.OutOfStock)
    else
      val items = wanted.toList.sortBy(_._1).map((id, q) => PricedLine(id, q, found(id).price))
      scala.util.Try(items.foldLeft(0L)((n, l) => Math.addExact(n, Math.multiplyExact(l.price, l.quantity.toLong))))
        .toEither.left.map(_ => DomainError.Invalid(List("tổng tiền vượt Long")))
        .flatMap(n => Money.fromMinor(n).left.map(e => DomainError.Invalid(List(e))).map(_ -> items))

  def product(d: ProductDraft): Either[DomainError, ProductDraft] =
    val v = d.copy(sku = d.sku.trim.toUpperCase(java.util.Locale.ROOT), name = d.name.trim)
    if !v.sku.matches("[A-Z0-9-]{1,32}") || v.name.isEmpty || v.name.length > 100 || v.price <= 0 || v.stock < 0 then
      Left(DomainError.Invalid(List("sku 1..32 chữ/số/dấu -; name 1..100; price > 0; stock >= 0")))
    else Right(v)
