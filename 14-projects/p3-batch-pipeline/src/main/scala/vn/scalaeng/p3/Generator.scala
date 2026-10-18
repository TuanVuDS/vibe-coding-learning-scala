package vn.scalaeng.p3

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

/** Sinh dữ liệu LỚN có seed (cùng seed → cùng file, byte-for-byte) với lỗi CÓ CHỦ ĐÍCH, để chạy pipeline ở quy mô khác dữ liệu mẫu.
  * Mỗi lô `ingest_date = d` chứa đơn/click của ngày d, cộng một ít bản ghi ĐẾN MUỘN của ngày d-1 và cập nhật hoàn tiền cho đơn ngày d-1.
  * Lỗi tiêm: ~1% dòng hỏng, ~0,5% gửi lại y hệt; khách C00001 là "khách nóng" (~20% đơn) để quan sát skew (Chương 25).
  *
  *   scala-cli run . --main-class vn.scalaeng.p3.Generator -- <thư mục ra> <số ngày> <đơn mỗi ngày> <click mỗi ngày> <seed>
  */
object Generator {
  final case class Summary(days: Int, orderLines: Long, clickLines: Long, customers: Int)

  def generate(out: Path, start: LocalDate, days: Int, ordersPerDay: Int, clicksPerDay: Int, seed: Long, customers: Int = 2000): Summary = {
    require(days > 0 && ordersPerDay > 0 && clicksPerDay >= 0 && customers > 1, "tham số phải dương")
    // Sinh vào thư mục đã có lô cũ sẽ để lại lô ngoài Summary mà pipeline vẫn đọc (review P3 m1): chỉ ghi vào thư mục MỚI hoặc RỖNG.
    require(!Files.exists(out) || { val s = Files.list(out); try !s.iterator().hasNext finally s.close() },
      s"thư mục ra $out không rỗng — chọn thư mục mới (generator không xóa dữ liệu của bạn)")
    val rnd = new Random(seed)
    val skus = Vector("KB-01", "MS-01", "HP-01", "MN-27", "CB-02", "SSD-1T")
    val price = Map("KB-01" -> 450000L, "MS-01" -> 250000L, "HP-01" -> 990000L, "MN-27" -> 4500000L, "CB-02" -> 90000L, "SSD-1T" -> 1800000L)
    val campaigns = Vector("SALE10", "NEWBIE", "FLASH", "RETARGET")
    def cust(): String = if (rnd.nextDouble() < 0.2) "C00001" else f"C${1 + rnd.nextInt(customers)}%05d"
    def ts(d: LocalDate): String = Instant.ofEpochSecond(d.atStartOfDay(ZoneOffset.UTC).toEpochSecond + rnd.nextInt(86400)).toString

    Files.createDirectories(out)
    val segs = Vector("Retail", "Wholesale")
    val custLines = "customer_id,segment,city" +: (1 to customers).map(i => f"C$i%05d,${segs(if (i % 7 == 0) 1 else 0)},HN")
    Files.write(out.resolve("customers.csv"), custLines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))

    var orderLines = 0L; var clickLines = 0L; var orderSeq = 0L
    var previousDayPaid = Vector.empty[String] // đơn PAID ngày trước — một phần sẽ bị hoàn tiền ở lô sau
    (0 until days).foreach { i =>
      val d = start.plusDays(i.toLong)
      val ob = new StringBuilder(Orders.Columns.mkString(",") + "\n")
      val today = Vector.newBuilder[String]
      def order(day: LocalDate, updated: String): String = {
        orderSeq += 1
        val sku = skus(rnd.nextInt(skus.size))
        val line = s"O$orderSeq,${cust()},$day,PAID,$sku,${1 + rnd.nextInt(5)},${price(sku)},$updated"
        if (day == d) today += line
        line
      }
      (1 to ordersPerDay).foreach { _ =>
        val r = rnd.nextDouble()
        val line =
          if (r < 0.01) { orderSeq += 1; s"O$orderSeq,${cust()},$d,PAID,KB-01,${if (rnd.nextBoolean()) "0" else "abc"},450000,${ts(d)}" } // dòng hỏng, ID riêng
          else if (r < 0.03 && i > 0) order(d.minusDays(1), ts(d))                                                            // đến muộn
          else order(d, ts(d))
        ob ++= line ++= "\n"
        if (rnd.nextDouble() < 0.005) ob ++= line ++= "\n" // gửi lại y hệt
      }
      // Hoàn tiền ~2% đơn PAID của ngày trước, cập nhật đến trong lô hôm nay.
      previousDayPaid.filter(_ => rnd.nextDouble() < 0.02).foreach { l =>
        val f = l.split(',')
        ob ++= Seq(f(0), f(1), f(2), "REFUNDED", f(4), f(5), f(6), Instant.parse(f(7)).plusSeconds(86400).toString).mkString(",") ++= "\n"
      }
      previousDayPaid = today.result()
      writeBatch(out, "orders", d, "orders.csv", ob.toString)
      orderLines += ob.count(_ == '\n') - 1

      val cb = new StringBuilder
      (1 to clicksPerDay).foreach { k =>
        val r = rnd.nextDouble()
        val day = if (r < 0.02 && i > 0) d.minusDays(1) else d
        val line =
          if (r > 0.995) s"""{"event_id":"K$d-$k","customer_id":"""      // JSON hỏng
          else s"""{"event_id":"K$d-$k","customer_id":"${cust()}","campaign":"${campaigns(rnd.nextInt(campaigns.size))}","ts":"${ts(day)}"}"""
        cb ++= line ++= "\n"
        if (rnd.nextDouble() < 0.005) cb ++= line ++= "\n"
      }
      writeBatch(out, "clicks", d, "clicks.jsonl", cb.toString)
      clickLines += cb.count(_ == '\n')
    }
    Summary(days, orderLines, clickLines, customers)
  }

  private def writeBatch(out: Path, source: String, d: LocalDate, file: String, content: String): Unit = {
    val dir = Files.createDirectories(out.resolve(source).resolve(s"ingest_date=$d"))
    Files.write(dir.resolve(file), content.getBytes(StandardCharsets.UTF_8)); ()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 5, "usage: <out> <days> <ordersPerDay> <clicksPerDay> <seed>")
    println(generate(Paths.get(args(0)), LocalDate.parse("2026-10-15"), args(1).toInt, args(2).toInt, args(3).toInt, args(4).toLong))
  }
}
