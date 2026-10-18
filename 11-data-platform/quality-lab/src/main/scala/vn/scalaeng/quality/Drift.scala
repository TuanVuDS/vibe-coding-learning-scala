package vn.scalaeng.quality

/** Phát hiện THAY ĐỔI NGOÀI MONG ĐỢI giữa hai lần chạy (bản cũ và bản mới của code, hoặc hôm qua và hôm nay):
  * so HỒ SƠ (profile) của output thay vì từng dòng. Golden dataset bắt thay đổi trên dữ liệu MẪU; drift bắt thay đổi trên dữ liệu THẬT,
  * nơi không có đáp án tính tay.
  */
final case class Profile(rows: Int, revenue: BigInt, unknownShare: BigDecimal, segmentShare: Map[String, BigDecimal], days: Int)

object Profile:
  private def share(part: BigInt, total: BigInt): BigDecimal =
    if total == 0 then BigDecimal(0) else (BigDecimal(part) / BigDecimal(total)).setScale(4, BigDecimal.RoundingMode.HALF_EVEN)

  def of(daily: Seq[DailyRow]): Profile =
    val total = daily.map(_.revenueVnd).sum
    val bySeg = daily.groupBy(_.segment).view.mapValues(rs => share(rs.map(_.revenueVnd).sum, total)).toMap
    Profile(daily.size, total, bySeg.getOrElse("UNKNOWN", BigDecimal(0)), bySeg, daily.map(_.day).distinct.size)

final case class Tolerance(maxRevenueChange: BigDecimal, maxShareChange: BigDecimal, maxUnknownShare: BigDecimal)

object Drift:
  /** Danh sách thay đổi vượt ngưỡng. Ngưỡng là quyết định nghiệp vụ; đặt quá chặt sinh cảnh báo giả, quá lỏng thì vô dụng. */
  def compare(base: Profile, now: Profile, t: Tolerance): List[String] =
    val revChange =
      if base.revenue == 0 then BigDecimal(if now.revenue == 0 then 0 else 1)
      else ((BigDecimal(now.revenue) - BigDecimal(base.revenue)).abs / BigDecimal(base.revenue)).setScale(4, BigDecimal.RoundingMode.HALF_EVEN)
    val segs = (base.segmentShare.keySet ++ now.segmentShare.keySet).toList.sorted
    List(
      Option.when(revChange > t.maxRevenueChange)(s"doanh thu đổi ${revChange * 100}% (${base.revenue} → ${now.revenue}) > ${t.maxRevenueChange * 100}%"),
      Option.when(now.unknownShare > t.maxUnknownShare)(s"tỷ trọng UNKNOWN ${now.unknownShare * 100}% > ${t.maxUnknownShare * 100}%"),
      Option.when(now.days < base.days)(s"số ngày giảm ${base.days} → ${now.days}")
    ).flatten ++ segs.flatMap { s =>
      val (a, b) = (base.segmentShare.getOrElse(s, BigDecimal(0)), now.segmentShare.getOrElse(s, BigDecimal(0)))
      Option.when((a - b).abs > t.maxShareChange)(s"tỷ trọng $s đổi ${a * 100}% → ${b * 100}%")
    }
