package vn.scalaeng.quality

import java.nio.charset.StandardCharsets
import java.nio.file.{Files as JFiles, Path}
import scala.jdk.CollectionConverters.*

/** Đọc input theo lô `ingest_date=YYYY-MM-DD/orders.csv` và ghi output CSV ổn định (sắp xếp, định dạng cố định) — điều kiện để so golden. */
object DataFiles:
  /** CSV đơn giản (không có dấu phẩy trong giá trị) — đủ cho lab; dữ liệu thật dùng thư viện CSV (Chương 19).
    *   - BOM UTF-8 (U+FEFF) đầu file bị bỏ: `trim` không bỏ nó, và header `﻿order_id` làm MỌI dòng mất cột đầu (review T17 m2).
    *   - Dòng THIẾU cột: cột thiếu là chuỗi rỗng — để validation báo đúng trường nào bắt buộc mà trống.
    *   - Dòng THỪA cột: giá trị thừa KHÔNG được bỏ âm thầm; chúng nằm ở khóa `Contract.ExtraColumns` để contract và pipeline từ chối dòng đó
    *     (review T17 M3 — bản đầu dùng `zipAll` rồi lọc tên cột rỗng, xóa mất dữ liệu sai trước khi ai kịp thấy).
    */
  def records(file: Path): Vector[Map[String, String]] =
    val lines = JFiles.readAllLines(file, StandardCharsets.UTF_8).asScala.toVector.filter(_.trim.nonEmpty)
    lines.headOption.fold(Vector.empty) { h =>
      val cols = h.stripPrefix("﻿").split(",", -1).map(_.trim).toVector
      lines.tail.map { l =>
        val values = l.split(",", -1).toVector
        val row    = cols.zipAll(values.take(cols.size), "", "").toMap
        if values.size > cols.size then row + (Contract.ExtraColumns -> values.drop(cols.size).mkString(",")) else row
      }
    }

  def batches(root: Path): Vector[(String, Map[String, String])] =
    val dirs = JFiles.list(root)
    try dirs.iterator().asScala.toVector.filter(_.getFileName.toString.startsWith("ingest_date=")).sortBy(_.getFileName.toString)
      .flatMap(d => records(d.resolve("orders.csv")).map(r => d.getFileName.toString.stripPrefix("ingest_date=") -> r))
    finally dirs.close()

  def toRaw(batch: String, r: Map[String, String]): RawOrder =
    def g(k: String) = r.getOrElse(k, "")
    RawOrder(batch, g("order_id"), g("customer_id"), g("day"), g("status"), g("sku"), g("quantity"), g("unit_price_vnd"), g("updated_at"),
      r.get(Contract.ExtraColumns))

  def customers(file: Path): Vector[(String, String)] =
    records(file).map(r => r.getOrElse("customer_id", "").trim.toUpperCase(java.util.Locale.ROOT) -> r.getOrElse("segment", "").trim)

object Csv:
  def daily(rows: Seq[DailyRow]): String =
    ("day,segment,revenue_vnd,order_lines,orders" +: rows.map(r => s"${r.day},${r.segment},${r.revenueVnd},${r.orderLines},${r.orders}")).mkString("", "\n", "\n")
  def rejected(rows: Seq[Rejected]): String =
    ("batch,order_id,sku,errors" +: rows.map(r => s"${r.raw.batch},${r.raw.orderId},${r.raw.sku.trim},${r.errors.mkString(" | ")}")).mkString("", "\n", "\n")
  def conflicts(rows: Seq[Order]): String =
    ("order_id,sku,status,updated_at" +: rows.map(o => s"${o.orderId},${o.sku},${o.status},${o.updatedAt}")).mkString("", "\n", "\n")
