package vn.scalaeng.solutions.ch19

import io.circe.Json
import java.time.{Duration, Instant, LocalDate}
import munit.CatsEffectSuite
import vn.scalaeng.ingest.*

class Ch19SolutionsSuite extends CatsEffectSuite:
  private def ev(id: String, ts: String, user: String = "U-1", kind: EventType = EventType.Impression, country: String = "VN") =
    val t = Instant.parse(ts)
    AdEvent(id, kind, t, t.atZone(Normalize.Vietnam).toLocalDate, user, "CMP-1", None, "Android", country, None)

  test("bài 1: định dạng SDK cũ hiểu là giờ Việt Nam; định dạng có múi giờ vẫn ưu tiên"):
    assertEquals(LegacyTimestamp.parse(Some(Json.fromString("2026-10-15 08:00:00"))), Right(Instant.parse("2026-10-15T01:00:00Z")))
    assertEquals(LegacyTimestamp.parse(Some(Json.fromString("2026-10-15T08:00:00Z"))), Right(Instant.parse("2026-10-15T08:00:00Z")))
    assert(LegacyTimestamp.parse(Some(Json.fromString("15/10/2026"))).isLeft)

  test("bài 2: impression theo quốc gia và ngày VN"):
    val events = List(ev("1", "2026-10-15T01:00:00Z"), ev("2", "2026-10-15T18:00:00Z"), ev("3", "2026-10-15T02:00:00Z", country = "TH"),
      ev("4", "2026-10-15T03:00:00Z", kind = EventType.Click))
    assertEquals(ByCountry.impressions(events), List(
      ("TH", LocalDate.parse("2026-10-15"), 1L),
      ("VN", LocalDate.parse("2026-10-15"), 1L),
      ("VN", LocalDate.parse("2026-10-16"), 1L)
    ))

  test("bài 3: gửi lại với event_id MỚI và lệch vài ms vẫn bị nhận ra là trùng; sự kiện ở giây khác thì không"):
    val events = List(ev("a", "2026-10-15T01:00:00.100Z"), ev("b", "2026-10-15T01:00:00.900Z"), ev("c", "2026-10-15T01:00:01.000Z"))
    assertEquals(ContentKey.dedup(events).map(_.eventId), List("a", "c"))

  test("bài 4: cửa sổ 10 phút — bộ nhớ giới hạn; bản trùng đến quá muộn lọt qua (đánh đổi đã biết)"):
    val events = (0 until 120).toList.map(i => ev(s"e$i", Instant.parse("2026-10-15T00:00:00Z").plusSeconds(i * 60L).toString)) ++
      List(ev("e119", "2026-10-15T01:59:30Z"), ev("e0", "2026-10-15T01:59:40Z"))
    val (kept, maxSize) = WindowedDedup.run(Duration.ofMinutes(10))(events)
    assertEquals(kept.size, 121)              // e119 (trùng gần) bị loại; e0 (trùng sau 2 giờ) lọt qua
    assert(maxSize <= 11, s"maxSize=$maxSize") // chỉ nhớ ~10 phút gần nhất, không phải 120 id

  test("bài 5: khoảng nghỉ > 30 phút mở phiên mới"):
    val events = List(ev("1", "2026-10-15T01:00:00Z"), ev("2", "2026-10-15T01:20:00Z"), ev("3", "2026-10-15T02:00:00Z"),
      ev("4", "2026-10-15T01:05:00Z", user = "U-2"))
    assertEquals(Sessions.of(events).map(s => (s.userId, s.events, s.duration.toMinutes)),
      List(("U-1", 2, 20L), ("U-1", 1, 0L), ("U-2", 1, 0L)))

  test("bài 6: doanh thu theo ngày từ CSV theo luồng; dòng hỏng được đếm, không làm dừng"):
    val csv = """order_id,order_date,amount_vnd,status
                |O1,2026-10-15,450000,PAID
                |O2,2026-10-15,250000,PAID
                |O3,2026-10-15,990000,CANCELLED
                |O4,2026-10-16,abc,PAID
                |O5,2026-10-16,100000,PAID
                |O6,16/10/2026,100000,PAID
                |""".stripMargin
    RevenueByDay.fromString(csv).map { (revenue, bad) =>
      assertEquals(revenue, Map(LocalDate.parse("2026-10-15") -> BigInt(700000), LocalDate.parse("2026-10-16") -> BigInt(100000)))
      assertEquals(bad, 2L)
    }

  test("bài 6 (review M4, M5): dòng thiếu cột không làm dừng file; tổng vượt Long không tràn; số âm là dòng hỏng"):
    val csv = """order_id,order_date,amount_vnd,status
                |O1,2026-10-15,9223372036854775807,PAID
                |O2,2026-10-15,2
                |O3,2026-10-15,1,PAID
                |O4,2026-10-15,-5,PAID
                |""".stripMargin
    RevenueByDay.fromString(csv).map { (revenue, bad) =>
      assertEquals(revenue, Map(LocalDate.parse("2026-10-15") -> (BigInt(Long.MaxValue) + 1)))
      assertEquals(bad, 2L)
    }

  test("bài 1 (review M7): ngày không tồn tại bị TỪ CHỐI, không tự sửa"):
    List("2026-02-30 08:00:00", "2026-04-31 08:00:00", "2025-02-29 08:00:00").foreach { s =>
      assert(LegacyTimestamp.parse(Some(Json.fromString(s))).isLeft, s)
    }
    assertEquals(LegacyTimestamp.parse(Some(Json.fromString("2028-02-29 08:00:00"))), Right(Instant.parse("2028-02-29T01:00:00Z")))
