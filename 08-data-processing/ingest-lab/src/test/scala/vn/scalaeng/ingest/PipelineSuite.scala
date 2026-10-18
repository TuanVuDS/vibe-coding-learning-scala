package vn.scalaeng.ingest

import cats.effect.IO
import fs2.io.file.{Files, Path}
import java.time.Instant
import munit.CatsEffectSuite
import scala.util.Random

class PipelineSuite extends CatsEffectSuite:
  private val now       = Instant.parse("2026-10-17T00:00:00Z")
  private val campaigns = Map("CMP-1" -> Campaign("CMP-1", "A", "x"), "CMP-2" -> Campaign("CMP-2", "B", "y"))

  private def imp(id: String, cmp: String, user: String = "U-1", ts: String = "2026-10-15T01:00:00Z") =
    s"""{"event_id":"$id","type":"impression","ts":"$ts","user":{"id":"$user"},"campaign_id":"$cmp","geo":{"country":"VN"}}"""
  private def click(id: String, impId: String, cmp: String, user: String = "U-1") =
    s"""{"event_id":"$id","type":"click","ts":"2026-10-15T02:00:00Z","user":{"id":"$user"},"campaign_id":"$cmp","impression_id":"$impId","geo":{"country":"VN"}}"""

  private def report(lines: List[String]) =
    val (s, rejected) = Pipeline.runAll(lines, campaigns, now)
    (Aggregate.report(s, campaigns), rejected)

  test("trùng event_id: chỉ đếm một lần nhưng CÓ ghi nhận số bản trùng"):
    val (s, _) = Pipeline.runAll(List(imp("i1", "CMP-1"), imp("i1", "CMP-1"), imp("i1", "CMP-1")), campaigns, now)
    assertEquals((Aggregate.report(s, campaigns).map(_.impressions), s.duplicates), (List(1L), 2L))

  test("click đến TRƯỚC impression vẫn được đếm khi impression tới"):
    val (rows, rejected) = report(List(click("c1", "i1", "CMP-1"), imp("i1", "CMP-1")))
    assertEquals((rows.map(r => (r.impressions, r.clicks)), rejected), (List((1L, 1L)), Nil))

  test("click sai campaign bị loại DÙ đến trước hay sau impression"):
    val after  = report(List(imp("i1", "CMP-1"), click("c1", "i1", "CMP-2")))
    val before = report(List(click("c1", "i1", "CMP-2"), imp("i1", "CMP-1")))
    for (rows, rejected) <- List(after, before) do
      assertEquals(rows.map(_.clicks), List(0L))
      assertEquals(rejected.flatMap(_.reasons), List("click thuộc CMP-1 nhưng ghi CMP-2"))

  test("click mồ côi được báo kèm số dòng gốc"):
    val (_, rejected) = report(List(imp("i1", "CMP-1"), click("c9", "i404", "CMP-1")))
    assertEquals(rejected.map(r => (r.line, r.reasons)), List((2L, List("click mồ côi: không có impression i404"))))

  test("CTR làm tròn HALF_UP 2 chữ số; 0 impression -> 0"):
    val lines = imp("i1", "CMP-1") :: imp("i2", "CMP-1") :: imp("i3", "CMP-1") :: click("c1", "i1", "CMP-1") :: Nil
    assertEquals(report(lines)._1.map(_.ctrPercent), List(BigDecimal("33.33")))

  // ---- Thuộc tính: dữ liệu thật đến nhiều lần và không theo thứ tự ----
  private val sample: IO[List[String]] = Files[IO].readUtf8Lines(Path("data/events.jsonl")).compile.toList

  test("THUỘC TÍNH: gửi lại mọi dòng (at-least-once) không đổi báo cáo"):
    sample.map { lines =>
      assertEquals(report(lines.flatMap(l => List(l, l)))._1, report(lines)._1)
    }

  test("M1: cùng event_id, KHÁC nội dung = xung đột: bị loại có lý do; cùng nội dung = gửi lại: đếm duplicate"):
    val a = imp("i1", "CMP-1", ts = "2026-10-15T01:00:00Z")
    val b = imp("i1", "CMP-1", ts = "2026-10-16T01:00:00Z")    // khác NGÀY -> khác ô báo cáo
    val retry = imp("i1", "CMP-1", ts = "2026-10-15T01:00:05Z") // cùng ngày, lệch vài giây -> gửi lại
    val (s1, r1) = Pipeline.runAll(List(a, b), campaigns, now)
    val (s2, r2) = Pipeline.runAll(List(a, retry), campaigns, now)
    assertEquals((s1.conflicts, s1.duplicates, r1.size), (1L, 0L, 1))
    assert(r1.head.reasons.head.startsWith("event_id i1 trùng nhưng nội dung khác"), r1.head.reasons.toString)
    assertEquals((s2.conflicts, s2.duplicates, r2), (0L, 1L, Nil))
    // Với xung đột, bản được tính phụ thuộc thứ tự — ghi nhận (không giấu): đảo thứ tự đổi NGÀY trong báo cáo.
    val forward = Aggregate.report(s1, campaigns).map(_.day.toString)
    val reverse = Aggregate.report(Pipeline.runAll(List(b, a), campaigns, now)._1, campaigns).map(_.day.toString)
    assertEquals((forward, reverse), (List("2026-10-15"), List("2026-10-16")))

  test("THUỘC TÍNH: đảo thứ tự dòng (20 hoán vị cố định) không đổi báo cáo — với dữ liệu KHÔNG có xung đột id"):
    sample.map { lines =>
      assertEquals(Pipeline.runAll(lines, campaigns, now)._1.conflicts, 0L) // tiền điều kiện của thuộc tính
      val expected = report(lines)._1
      (1 to 20).foreach { seed =>
        assertEquals(report(Random(seed).shuffle(lines))._1, expected, s"seed=$seed")
      }
    }

  test("GOLDEN: chạy chương trình đọc file thật, report.csv khớp kết quả tính TAY"):
    Files[IO].tempDirectory.use { out =>
      for
        summary  <- Ingest.run(Path("data/events.jsonl"), Path("data/campaigns.csv"), out, now)
        actual   <- Files[IO].readUtf8(out / "report.csv").compile.string
        expected <- Files[IO].readUtf8(Path("expected/report.csv")).compile.string
        rejected <- Files[IO].readUtf8Lines(out / "rejected.jsonl").filter(_.nonEmpty).compile.count
      yield
        assertEquals(actual.replace("\r\n", "\n"), expected.replace("\r\n", "\n"))
        assertEquals(summary, Summary(lines = 17, accepted = 8, rejected = 7, duplicates = 2, orphanClicks = 1, reportRows = 3))
        assertEquals(rejected, 7L)
    }
