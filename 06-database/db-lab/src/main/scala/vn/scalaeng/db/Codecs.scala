package vn.scalaeng.db

import org.typelevel.doobie.{Meta, Read}
import org.typelevel.doobie.postgres.implicits.*

/** Ánh xạ kiểu Scala <-> kiểu cột SQL. Tách khỏi Model để domain không phụ thuộc Doobie. */
object Codecs:
  given Meta[UserId]    = Meta[Long].timap(UserId(_))(_.value)
  given Meta[ProductId] = Meta[Long].timap(ProductId(_))(_.value)
  given Meta[OrderId]   = Meta[Long].timap(OrderId(_))(_.value)

  /** `tiemap`: chuỗi trong DB không khớp enum -> lỗi khi đọc, thay vì một giá trị sai lặng lẽ. */
  given Meta[OrderStatus] = Meta[String].tiemap(OrderStatus.fromDb)(_.dbName)

  given Read[User]    = Read.derived
  given Read[Product] = Read.derived
  given Read[Order]   = Read.derived
