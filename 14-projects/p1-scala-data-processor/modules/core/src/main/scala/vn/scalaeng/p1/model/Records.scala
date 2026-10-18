package vn.scalaeng.p1.model

import java.time.{Instant, LocalDate}

final case class Order(
    orderId: OrderId,
    userId: UserId,
    sku: Sku,
    unitPrice: Vnd,
    quantity: Quantity,
    status: OrderStatus,
    orderDate: LocalDate,
    updatedAt: Instant,
    lineNo: Int
):
  def total: Vnd = unitPrice.times(quantity)

final case class User(userId: UserId, name: String, email: Option[Email], phone: Option[Phone], city: City)

/** Một vấn đề của một trường; nhiều vấn đề của cùng bản ghi được thu thập đầy đủ. */
final case class Issue(field: String, message: String):
  def render: String = s"$field: $message"

enum Source:
  case Orders, Users

/** Bản ghi bị loại: nguồn, vị trí, dữ liệu gốc và TẤT CẢ lý do. */
final case class Rejected(source: Source, position: Int, raw: String, issues: List[Issue])
