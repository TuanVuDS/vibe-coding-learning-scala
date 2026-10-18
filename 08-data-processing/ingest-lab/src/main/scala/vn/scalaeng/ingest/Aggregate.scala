package vn.scalaeng.ingest

import java.time.LocalDate
import scala.math.BigDecimal.RoundingMode

/** Trạng thái tích lũy khi đi qua luồng sự kiện. Bất biến (immutable): mỗi bước trả trạng thái MỚI.
  *
  * Bộ nhớ dùng KHÔNG phụ thuộc số dòng file mà phụ thuộc:
  *  - `seen`: số event_id khác nhau (để dedup) — đây là phần lớn nhất; Chương 27–30 bàn cách giới hạn bằng cửa sổ thời gian;
  *  - `impressions`: id impression đã thấy (để kiểm tra click hợp lệ);
  *  - `pendingClicks`: click đến TRƯỚC impression của nó (dữ liệu đến không theo thứ tự);
  *  - `cells`: số (campaign, ngày) — nhỏ.
  */
final case class State(
    seen: Map[String, Fingerprint],                 // event_id -> "dấu vân tay" của lần đầu thấy
    impressions: Map[String, String],               // impressionId -> campaignId
    pendingClicks: Map[String, List[Pending]],      // impressionId -> click đang chờ impression
    cells: Map[(String, LocalDate), Cell],
    duplicates: Long,
    conflicts: Long
)

/** Những gì phải GIỐNG NHAU để hai bản ghi cùng event_id được coi là MỘT sự kiện gửi lại (retry).
  * Không gồm `ts` chính xác (SDK có thể gán lại giờ khi gửi lại) nhưng gồm NGÀY — trường quyết định ô báo cáo.
  */
final case class Fingerprint(eventType: EventType, userId: String, campaignId: String, impressionId: Option[String], day: LocalDate)

object Fingerprint:
  def of(e: AdEvent): Fingerprint = Fingerprint(e.eventType, e.userId, e.campaignId, e.impressionId, e.day)

/** Click đang chờ: giữ cả số dòng và dòng gốc để nếu thành "mồ côi" vẫn truy được về file nguồn. */
final case class Pending(event: AdEvent, line: Long, raw: String)

final case class Cell(impressions: Long, clicks: Long, users: Set[String]):
  def add(e: AdEvent): Cell = e.eventType match
    case EventType.Impression => copy(impressions = impressions + 1, users = users + e.userId)
    case EventType.Click      => copy(clicks = clicks + 1, users = users + e.userId)

object State:
  val empty: State = State(Map.empty, Map.empty, Map.empty, Map.empty, 0, 0)

object Aggregate:
  private def count(s: State, e: AdEvent): State =
    val key = (e.campaignId, e.day)
    s.copy(cells = s.cells.updated(key, s.cells.getOrElse(key, Cell(0, 0, Set.empty)).add(e)))

  private def mismatch(impCampaign: String, click: AdEvent, line: Long, raw: String): Rejected =
    Rejected(line, List(s"click thuộc $impCampaign nhưng ghi ${click.campaignId}"), raw)

  /** Một bước: dedup theo event_id → xử lý impression/click. Trả trạng thái mới và các dòng bị loại (thường 0 hoặc 1;
    * nhiều hơn khi một impression đến muộn "nhả" ra các click chờ mà click đó ghi sai campaign).
    *
    * Kiểm tra campaign phải giống nhau DÙ click đến trước hay sau impression. Bản đầu chỉ kiểm tra khi click đến sau,
    * nên click sai campaign đến SỚM vẫn được đếm — test "thứ tự đến không đổi báo cáo" phát hiện ra.
    */
  def step(s: State, e: AdEvent, line: Long, raw: String): (State, List[Rejected]) =
    s.seen.get(e.eventId) match
      case Some(fp) if fp == Fingerprint.of(e) => (s.copy(duplicates = s.duplicates + 1), Nil) // gửi lại: bỏ qua, CÓ ĐẾM
      case Some(fp) =>
        // CÙNG id, KHÁC nội dung: không phải gửi lại mà là XUNG ĐỘT id (bug SDK, id bị tái sử dụng). Không âm thầm chọn một bản;
        // bản đầu đã được tính, bản này bị loại kèm lý do. Kết quả vì vậy PHỤ THUỘC thứ tự đến — đó là lý do phải báo ra (review chéo, M1).
        (s.copy(conflicts = s.conflicts + 1), List(Rejected(line, List(s"event_id ${e.eventId} trùng nhưng nội dung khác bản đã nhận ($fp)"), raw)))
      case None => accept(s.copy(seen = s.seen.updated(e.eventId, Fingerprint.of(e))), e, line, raw)

  private def accept(s1: State, e: AdEvent, line: Long, raw: String): (State, List[Rejected]) =
      e.eventType match
        case EventType.Impression =>
          val (ok, bad) = s1.pendingClicks.getOrElse(e.eventId, Nil).partition(_.event.campaignId == e.campaignId)
          val s2 = s1.copy(impressions = s1.impressions.updated(e.eventId, e.campaignId), pendingClicks = s1.pendingClicks - e.eventId)
          ((e :: ok.map(_.event)).foldLeft(s2)(count), bad.map(p => mismatch(e.campaignId, p.event, p.line, p.raw)))
        case EventType.Click =>
          val impId = e.impressionId.getOrElse("")
          s1.impressions.get(impId) match
            case Some(c) if c != e.campaignId => (s1, List(mismatch(c, e, line, raw)))
            case Some(_)                      => (count(s1, e), Nil)
            case None => (s1.copy(pendingClicks = s1.pendingClicks.updated(impId, Pending(e, line, raw) :: s1.pendingClicks.getOrElse(impId, Nil))), Nil)

  /** Cuối luồng: click vẫn chờ = click "mồ côi" (impression không bao giờ tới) → loại. */
  def orphans(s: State): List[Rejected] =
    s.pendingClicks.values.flatten.toList.sortBy(_.line).map { p =>
      Rejected(p.line, List(s"click mồ côi: không có impression ${p.event.impressionId.getOrElse("")}"), p.raw)
    }

  /** CTR = clicks / impressions, tính bằng BigDecimal, làm tròn 2 chữ số phần trăm. Không có impression → 0. */
  def report(s: State, campaigns: Map[String, Campaign]): List[ReportRow] =
    s.cells.toList.map { case ((cid, day), c) =>
      val ctr = if c.impressions == 0 then BigDecimal(0)
                else (BigDecimal(c.clicks) * 100 / c.impressions).setScale(2, RoundingMode.HALF_UP)
      ReportRow(cid, campaigns.get(cid).map(_.name).getOrElse("(không rõ)"), day, c.impressions, c.clicks, c.users.size.toLong, ctr)
    }.sortBy(r => (r.campaignId, r.day.toEpochDay))
