package vn.scalaeng.ingest

import io.circe.Json
import java.time.{Instant, LocalDate}
import munit.FunSuite

class NormalizeSuite extends FunSuite:
  private val now = Instant.parse("2026-10-17T00:00:00Z")

  test("ts: ISO 'Z', ISO có offset và epoch millis đều về cùng Instant"):
    val expected = Instant.parse("2026-10-15T01:00:00Z")
    assertEquals(Normalize.timestamp(Some(Json.fromString("2026-10-15T01:00:00Z"))), Right(expected))
    assertEquals(Normalize.timestamp(Some(Json.fromString("2026-10-15T08:00:00+07:00"))), Right(expected))
    assertEquals(Normalize.timestamp(Some(Json.fromLong(expected.toEpochMilli))), Right(expected))
    assertEquals(Normalize.timestamp(Some(Json.fromString("15/10/2026"))), Left("ts không đọc được: \"15/10/2026\""))
    assertEquals(Normalize.timestamp(None), Left("thiếu ts"))

  test("chuẩn hóa mã: trim + chữ hoa; các cách viết 'Việt Nam' về VN"):
    assertEquals(Normalize.campaignId(Some(" cmp-7 ")), Right("CMP-7"))
    assertEquals(Normalize.userId(Some("u-42")), Right("U-42"))
    assertEquals(List("vn", "Viet Nam", "VIETNAM", "VNM").map(c => Normalize.country(Some(c))), List.fill(4)(Right("VN")))
    assertEquals(Normalize.country(Some("Vietnamm")), Left("geo.country không hợp lệ: VIETNAMM"))

  test("báo ĐỦ mọi lỗi của một dòng, không dừng ở lỗi đầu"):
    val line = """{"event_id":"x2","type":"view","ts":"2026-10-15T05:00:00Z","campaign_id":"CMP-1","geo":{"country":"Vietnamm"}}"""
    assertEquals(Normalize.event(10, line, now).left.map(_.reasons),
      Left(List("type không hỗ trợ: view", "thiếu user.id", "geo.country không hợp lệ: VIETNAMM")))

  test("ts ở tương lai (quá 5 phút lệch đồng hồ) hoặc trước 2020 bị loại; lệch 4 phút được chấp nhận"):
    def at(ts: String) = s"""{"event_id":"e","type":"impression","ts":"$ts","user":{"id":"U-1"},"campaign_id":"CMP-1","geo":{"country":"VN"}}"""
    assert(Normalize.event(1, at("2026-10-17T00:04:00Z"), now).isRight)
    assertEquals(Normalize.event(1, at("2026-10-17T00:06:00Z"), now).left.map(_.reasons), Left(List("ts ở tương lai: 2026-10-17T00:06:00Z")))
    assertEquals(Normalize.event(1, at("2019-12-31T23:59:59Z"), now).left.map(_.reasons), Left(List("ts quá cũ: 2019-12-31T23:59:59Z")))

  test("trường tùy chọn thiếu hoặc null không làm hỏng dòng: os -> 'unknown', city -> None"):
    val line = """{"event_id":"i5","type":"impression","ts":"2026-10-15T09:00:00Z","user":{"id":"U-6"},"campaign_id":"CMP-2","device":{"os":null},"geo":{"country":"VN","city":null}}"""
    val e = Normalize.event(1, line, now).toOption.get
    assertEquals((e.os, e.city), ("unknown", None))

  test("ngày báo cáo theo GIỜ VIỆT NAM: 17:30Z ngày 15 là ngày 16 ở Việt Nam"):
    val line = """{"event_id":"i4","type":"impression","ts":"2026-10-15T17:30:00Z","user":{"id":"U-1"},"campaign_id":"CMP-1","geo":{"country":"VN"}}"""
    assertEquals(Normalize.event(1, line, now).map(_.day), Right(LocalDate.parse("2026-10-16")))

  test("click phải có impression_id; JSON hỏng có lý do rõ"):
    val click = """{"event_id":"c5","type":"click","ts":"2026-10-15T06:40:00Z","user":{"id":"U-2"},"campaign_id":"CMP-1","geo":{"country":"VN"}}"""
    assertEquals(Normalize.event(1, click, now).left.map(_.reasons), Left(List("click thiếu impression_id")))
    assertEquals(Normalize.event(1, """{"event_id":""", now).left.map(_.reasons), Left(List("JSON không hợp lệ: exhausted input")))

  // ---- Regression từ review chéo (Codex) ----
  test("M2: nhiều trường SAI KIỂU trong một dòng đều được báo, không dừng ở trường đầu"):
    val line = """{"event_id":12,"type":"view","campaign_id":34,"geo":{"country":"BAD"}}"""
    assertEquals(Normalize.event(1, line, now).left.map(_.reasons), Left(List(
      "event_id phải là chuỗi, nhận Number", "type không hỗ trợ: view", "thiếu ts", "thiếu user.id",
      "campaign_id phải là chuỗi, nhận Number", "geo.country không hợp lệ: BAD")))

  test("M2: object lồng nhau sai cấu trúc, trường tùy chọn sai kiểu -> báo lỗi, không âm thầm coi là thiếu"):
    val line = """{"event_id":"e","type":"impression","ts":"2026-10-15T01:00:00Z","user":"U-1","campaign_id":"CMP-1","device":{"os":7},"geo":{"country":"VN","city":5}}"""
    assertEquals(Normalize.event(1, line, now).left.map(_.reasons), Left(List(
      "user phải là object, nhận String", "device.os phải là chuỗi, nhận Number", "geo.city phải là chuỗi, nhận Number")))

  test("M3: chỉ nhận mã ISO 3166-1 thật"):
    assertEquals(List("vn", "TH", "in").map(c => Normalize.country(Some(c))), List(Right("VN"), Right("TH"), Right("IN")))
    assertEquals(Normalize.country(Some("ZZ")), Left("geo.country không hợp lệ: ZZ"))
    assert(Normalize.country(Some("ÅÅ")).isLeft)

  test("M6: kết quả không đổi khi locale mặc định của máy là Thổ Nhĩ Kỳ"):
    val line  = """{"event_id":"e","type":"IMPRESSION","ts":"2026-10-15T01:00:00Z","user":{"id":"u-1"},"campaign_id":"cmp-1","geo":{"country":"in"}}"""
    val saved = java.util.Locale.getDefault
    try
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"))
      assertEquals(Normalize.event(1, line, now).map(e => (e.eventType, e.country, e.userId)), Right((EventType.Impression, "IN", "U-1")))
    finally java.util.Locale.setDefault(saved)
