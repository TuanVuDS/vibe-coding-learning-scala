package vn.scalaeng.platform

import java.time.LocalDate

/** Tích hợp batch + streaming. "Speed layer" kiểu Lambda của lab: cộng doanh thu PAID NGAY khi event đến, CỐ Ý bỏ dedup/cập nhật/hoàn tiền —
  * nhanh nhưng là con số TẠM. Một speed layer thật có thể xử lý dedup và cập nhật; cái giá cố hữu của Lambda là DUY TRÌ HAI ĐƯỜNG LOGIC và phải
  * ĐỐI SOÁT chúng. Batch tính lại từ raw là con số đúng. Lab KHÔNG triển khai Kappa (không có stream processor giữ state, offset, replay qua
  * cùng processor); nó chỉ minh họa ý tưởng Kappa hướng tới: cùng MỘT logic (`StagingOrders.compute` + `DailyRevenue.compute`) dùng lại khi
  * phát lại (replay) raw.
  */
object SpeedLayer:
  def runningPaid(rawLines: Seq[String]): Map[LocalDate, BigInt] =
    rawLines.flatMap(RawOrders.parse).filter(_.status == "PAID").groupMapReduce(_.day)(o => BigInt(o.quantity) * o.priceVnd)(_ + _)

  /** Tổng doanh thu batch của một ngày, đọc từ partition serving đã tính. */
  def batchPaid(p: Platform, day: LocalDate): BigInt =
    Files2.lines(p.partitionFile(DailyRevenue.layer, DailyRevenue.name, day)).drop(1).map(_.split(",", -1)(1)).map(BigInt(_)).sum

  def reconcile(speed: Map[LocalDate, BigInt], batch: Map[LocalDate, BigInt]): List[String] =
    (speed.keySet ++ batch.keySet).toList.sorted.flatMap { d =>
      val (s, b) = (speed.getOrElse(d, BigInt(0)), batch.getOrElse(d, BigInt(0)))
      Option.when(s != b)(s"$d: speed=$s batch=$b lệch=${s - b}")
    }
