package vn.scalaeng.order.domain

import java.time.LocalDate

final case class OrderLine(orderId: OrderId, sku: Sku, unitPrice: Vnd, quantity: Quantity, orderDate: LocalDate):
  def total: Vnd = unitPrice * quantity

/** Lỗi ở mức một dòng dữ liệu, kèm số dòng trong file gốc (tính cả header). */
final case class RowError(lineNo: Int, reason: String, raw: String)
