# Lời giải — Chương 19: Scala cho Data Processing

> Code: [`ch19-code/`](ch19-code/) — dùng lại mã nguồn `08-data-processing/ingest-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch19-code && scala-cli test .` → **8/8 test pass** (17/10/2026; 2 test regression từ review chéo).

## Bài 1 — Định dạng thời gian của SDK cũ

```scala
def parse(raw: Option[Json]): Either[String, Instant] =
  Normalize.timestamp(raw).orElse {
    raw.flatMap(_.asString)
      .flatMap(s => Try(LocalDateTime.parse(s.trim, Legacy).atZone(Normalize.Vietnam).toInstant).toOption)
      .toRight(...)
  }
```

- Thử các định dạng **có múi giờ** trước (`Normalize.timestamp`), định dạng mơ hồ **sau cùng**: `"2026-10-15T08:00:00Z"` phải được hiểu là UTC như đã ghi,
  không bị "đoán" sang giờ Việt Nam.
- Formatter: `ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT)`. Mặc định (`SMART`) **âm thầm sửa** `2026-02-30` thành
  `2026-02-28` — bản đầu của lời giải mắc lỗi này (review chéo, M7). `uuuu` thay `yyyy` vì STRICT với `yyyy` (năm theo era) đòi thêm trường era. Test: 30/02, 31/04,
  29/02 năm không nhuận bị từ chối; 29/02/2028 được nhận.
- `LocalDateTime` (không múi giờ) + `atZone(Asia/Ho_Chi_Minh)` → `Instant`. Quyết định "giờ không ghi múi giờ là giờ Việt Nam" lấy từ **tài liệu SDK** —
  nếu không có tài liệu, đó là giả định phải ghi rõ và hỏi lại nguồn dữ liệu.
- Test: `"2026-10-15 08:00:00"` → `2026-10-15T01:00:00Z`.

## Bài 2 — Impression theo quốc gia và ngày

```scala
events.filter(_.eventType == EventType.Impression)
  .groupMapReduce(e => (e.country, e.day))(_ => 1L)(_ + _)
```

- `groupMapReduce(key)(map)(reduce)` đếm trong **một** lượt, không tạo các danh sách con như `groupBy(...).map(_.size)`.
- `e.day` đã là ngày giờ VN (tính ở `Normalize`), nên event lúc 18:00Z ngày 15 rơi vào ngày 16 — test có trường hợp này.
- Sắp xếp kết quả để output xác định (Map không có thứ tự).

## Bài 3 — Dedup theo nội dung

```scala
val material = List(e.eventType.toString, e.userId, e.campaignId, e.impressionId.getOrElse(""), e.ts.getEpochSecond.toString).mkString("|")
SHA-256(material)
```

- Chọn trường **xác định "cùng một sự kiện"**: loại, user, campaign, impression được bấm, và thời gian **cắt về giây** (lần gửi lại có thể lệch vài ms).
- Dấu phân cách `|` tránh va chạm kiểu `("ab","c")` và `("a","bc")` cùng ra `"abc"`.
- **Đánh đổi:** hai sự kiện thật khác nhau nhưng trùng mọi trường trong cùng một giây bị gộp nhầm (người dùng xem cùng quảng cáo hai lần trong một giây —
  hiếm, nhưng có). Cắt về phút thì gộp nhầm nhiều hơn; giữ mili giây thì không nhận ra lần gửi lại. Không có lựa chọn "đúng tuyệt đối"; đo trên dữ liệu thật.
- Cách tốt hơn khi có thể: yêu cầu SDK sinh `event_id` **một lần** và giữ nguyên khi gửi lại (idempotency key).

## Bài 4 — Dedup với bộ nhớ giới hạn

```scala
def check(e: AdEvent): (WindowedDedup, Boolean) =
  val newest2 = if e.ts.isAfter(newest) then e.ts else newest
  val kept    = seen.filter((_, t) => !t.isBefore(newest2.minus(window)))   // quên id cũ hơn cửa sổ
  if kept.contains(e.eventId) then (copy(seen = kept, newest = newest2), true)
  else (copy(seen = kept.updated(e.eventId, e.ts), newest = newest2), false)
```

- Test: 120 sự kiện cách nhau 1 phút + hai bản trùng. Số id phải nhớ cùng lúc **≤ 11** (cửa sổ 10 phút) thay vì 120.
- **Không phải trần bộ nhớ cố định:** trạng thái là O(số id **trong cửa sổ**) — phụ thuộc tốc độ sự kiện. 10.000 sự kiện cùng một giây (burst) thì phải nhớ 10.000 id
  (review chéo, m1). Muốn trần cứng phải đặt giới hạn số id và quyết định làm gì khi vượt (loại bản cũ nhất, ghi ra kho ngoài). Hàm `run` của lời giải còn giữ mọi sự kiện
  được giữ lại trong `Vector` — đó là output của bài tập, không phải trạng thái dedup.
- `e119` gửi lại sau 30 giây → bị loại (trùng). `e0` gửi lại sau **2 giờ** → **lọt qua**: đây là đánh đổi có chủ ý, test khẳng định nó để không ai "sửa nhầm".
- "Thời gian" ở đây là thời gian **sự kiện** lớn nhất đã thấy, không phải đồng hồ máy — chạy lại một file cũ cho cùng kết quả.
- `filter` toàn bộ Map mỗi bước là O(n) — đủ cho bài tập; bản thật dùng cấu trúc sắp theo thời gian (hàng đợi) để chỉ bỏ phần đầu. Chương 27–30 (watermark)
  làm việc này trong hệ thống streaming.

## Bài 5 — Sessionization

```scala
es.sortBy(_.ts).foldLeft(List.empty[Session]) {
  case (current :: done, e) if !e.ts.isAfter(current.end.plus(gap)) => current.copy(end = e.ts, events = current.events + 1) :: done
  case (acc, e)                                                       => Session(user, e.ts, e.ts, 1) :: acc
}.reverse
```

- Gom theo user, **sắp theo thời gian** (dữ liệu đến không theo thứ tự!), rồi `foldLeft`: sự kiện cách `end` của phiên hiện tại không quá 30 phút → kéo dài phiên;
  ngược lại → mở phiên mới.
- Danh sách tích lũy ngược (thêm vào đầu là O(1)) rồi `reverse` một lần cuối.
- Test: U-1 có 01:00, 01:20 (cùng phiên, 20 phút) và 02:00 (cách 40 phút → phiên mới).
- Giới hạn: cần **mọi** sự kiện của một user trong bộ nhớ để sắp xếp. Với luồng vô hạn, phiên phải được "đóng" khi quá `gap` không có sự kiện mới —
  đó là *session window* trong streaming (Chương 27).

## Bài 6 — Doanh thu theo ngày từ CSV theo luồng

```scala
text.through(lowlevel.rows[IO, String]())
  .through(lowlevel.headers[IO, String])
  .map(row => summon[CsvRowDecoder[OrderRow, String]](row))
  .fold((Map.empty[LocalDate, Long], 0L)) { ... }
```

- `lenient.attemptDecodeUsingHeaders[OrderRow]()` trả `Either` cho **mỗi dòng**, kể cả dòng thiếu/thừa cột — dòng hỏng thành `Left` và được đếm.
  Bản đầu dùng `lowlevel.rows` + `lowlevel.headers` + tự giải mã: dòng thiếu cột ném `HeaderSizeError` ngay ở `headers` và **dừng cả luồng** (review chéo, M4).
  Lỗi cú pháp CSV mức thấp (dấu ngoặc kép không đóng) vẫn dừng luồng: không thể biết dòng kết thúc ở đâu.
- Tổng doanh thu là `BigInt`: hai đơn hợp lệ `9223372036854775807` + `1` cộng bằng `Long` thành **âm** (review chéo, M5). Số tiền ≤ 0 là dòng hỏng.
- `fold` chỉ giữ `Map[ngày, tổng]` + bộ đếm lỗi: bộ nhớ ~ số ngày, không ~ số dòng.
- Đơn `CANCELLED` không tính doanh thu; ngày sai định dạng (`16/10/2026`) cũng là dòng hỏng.
- Lỗi thật gặp khi viết: `row.as[OrderRow]` → `No implicit CellDecoder found` (bài học mục 10.2): `as` giải mã **một ô**, không phải cả dòng.
- Test: 6 dòng, 2 hỏng (`amount = abc`, ngày sai định dạng) → `15-10 = 700.000`, `16-10 = 100.000`; test regression: dòng thiếu cột + số âm đếm là 2 dòng hỏng,
  tổng `Long.MaxValue + 1` đúng bằng BigInt.
