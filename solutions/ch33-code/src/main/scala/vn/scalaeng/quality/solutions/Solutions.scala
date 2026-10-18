package vn.scalaeng.quality.solutions

import vn.scalaeng.quality.*

import java.time.LocalDate

/** Lời giải Chương 33. Mỗi bài là hàm thuần trên kiểu của quality-lab. */
object Solutions:

  // Bài 1 — check theo TỶ LỆ: vài dòng UNKNOWN là bình thường (khách mới chưa đồng bộ), nhiều mới là sự cố.
  // `failed` = 1 nếu vượt ngưỡng (một vi phạm cấp tập), mẫu ghi tỷ lệ thật để người trực đọc được ngay.
  def maxShare[A](name: String, sev: Severity, maxRatio: BigDecimal)(bad: A => Boolean): Check[A] =
    Check(name, sev, xs => {
      val n     = xs.count(bad)
      val ratio = if xs.isEmpty then BigDecimal(0) else BigDecimal(n) / xs.size
      val over  = ratio > maxRatio
      CheckResult(name, sev, xs.size, if over then 1 else 0,
        if over then List(s"$n/${xs.size} = ${(ratio * 100).setScale(2, BigDecimal.RoundingMode.HALF_EVEN)}% > ${maxRatio * 100}%") else Nil)
    })

  // Bài 2 — accepted values: tập giá trị hợp lệ của cột phân loại. Bắt lỗi chính tả/đổi tên ở dimension ("retail" vs "Retail").
  def acceptedValues[A](name: String, sev: Severity, allowed: Set[String])(field: A => String): Check[A] =
    Check.row[A](name, sev)(a => !allowed(field(a)))(field)

  // Bài 3 — sinh bản ghi HỢP LỆ từ chính contract: contract là nguồn sự thật cho cả validator lẫn dữ liệu test.
  // Hàm tất định theo `i` để test tái hiện được; property test có thể thay `i` bằng Gen.
  def sampleRecord(c: Contract, i: Int): Map[String, String] =
    c.fields.map { f =>
      val v = f.`type` match
        case "string"    => s"${f.name}-$i"
        case "int"       => (i % 1000).toString
        case "long"      => (i.toLong * 1_000_000L).toString
        case "date"      => LocalDate.parse("2026-01-01").plusDays((i % 365).toLong).toString
        case "timestamp" => s"2026-10-15T${"%02d".format(i % 24)}:00:00Z"
        case "enum"      => f.values.fold("")(vs => vs(i % vs.size))
        case other       => sys.error(s"kiểu chưa hỗ trợ: $other")
      f.name -> v
    }.toMap

  // Bài 4 — đối soát (reconciliation) chạy TRONG pipeline production, không chỉ trong test: mỗi dòng input vào đúng một ngăn.
  def reconciliation(inputRows: Int): Check[Output] =
    Check("đối soát: input = rejected + trùng + thay thế + xung đột + fact", Severity.Error, outs => {
      val bad = outs.filter(o => o.rejected.size + o.exactDuplicates + o.superseded + o.conflicts.size + o.facts.size != inputRows)
      CheckResult("đối soát", Severity.Error, outs.size, bad.size, bad.take(1).map(o =>
        s"input=$inputRows, ngăn=${o.rejected.size}+${o.exactDuplicates}+${o.superseded}+${o.conflicts.size}+${o.facts.size}").toList)
    })

  // Bài 5 — drift trên TỶ LỆ BỊ LOẠI: nguồn đổi định dạng thường biểu hiện bằng rejected tăng vọt, trước khi doanh thu kịp lệch.
  def rejectRate(o: Output, inputRows: Int): BigDecimal =
    if inputRows == 0 then BigDecimal(0) else (BigDecimal(o.rejected.size) / inputRows).setScale(4, BigDecimal.RoundingMode.HALF_EVEN)

  def rejectDrift(baseRate: BigDecimal, nowRate: BigDecimal, maxIncrease: BigDecimal): Option[String] =
    Option.when(nowRate - baseRate > maxIncrease)(s"tỷ lệ bị loại tăng ${baseRate * 100}% → ${nowRate * 100}% (ngưỡng +${maxIncrease * 100}%)")

  // Bài 6 — (xem test) Incremental.step là idempotent theo lô: áp lại cùng lô không đổi trạng thái.

  // Bài 7 — backfill theo ngày: tính lại các ngày `days` từ TOÀN BỘ input rồi THAY THẾ đúng các ngày đó trong bảng đang phục vụ
  // (overwrite theo partition, Chương 24) — không append, không động vào ngày khác.
  def backfill(serving: Vector[DailyRow], recomputed: Vector[DailyRow], days: Set[LocalDate]): Vector[DailyRow] =
    (serving.filterNot(r => days(r.day)) ++ recomputed.filter(r => days(r.day))).sortBy(r => (r.day, r.segment))
