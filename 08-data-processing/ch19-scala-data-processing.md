# Chương 19 — Scala cho Data Processing

> **Phiên bản:** Scala 3.9.0 · JDK 21 · fs2-io 3.13.0 · fs2-data-csv 1.14.1 (+ generic) · circe-parser 0.14.17 · munit 1.2.4 · munit-cats-effect 2.2.0
> **Code:** [`ingest-lab/`](ingest-lab/) — pipeline sự kiện quảng cáo (impression/click, JSON Lines lồng nhau) + bảng campaign CSV → báo cáo CTR theo ngày
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **20/20 test pass** (`scala-cli test .` và `sbt test`), lời giải 8/8.
> Review chéo của Codex tìm ra 7 lỗi lớn (decoder dừng ở lỗi kiểu đầu tiên, xung đột `event_id`, mã quốc gia giả, phụ thuộc locale, CSV thiếu cột
> làm dừng file, tràn số, ngày không tồn tại bị "sửa") — đã sửa, mỗi lỗi có regression test (mục 10.6–10.10).
> Kết quả trên dữ liệu mẫu khớp **báo cáo tính tay trước khi chạy**. Thí nghiệm bộ nhớ 1 triệu dòng chạy thật trên máy biên soạn (12 CPU logic).

---

## Vị trí trong lộ trình

Đây là chương mở đầu nhánh **Data Engineering**. Project 1 (sau Chương 11) đã tự viết parser CSV/JSON để luyện Scala core. Chương này làm cùng loại việc
**như trong công việc thật**: dùng thư viện (circe, fs2-data-csv), đọc file **theo luồng** để bộ nhớ không phụ thuộc kích thước file, xử lý dữ liệu
lồng nhau, đến không theo thứ tự và bị gửi lặp. (Đọc theo luồng giữ *đầu vào* nhỏ; bộ nhớ thật sự còn phụ thuộc *trạng thái* — mục 3.5.) Chương 20 đi sâu vào biến đổi dữ liệu theo phong cách functional (validation tích lũy, data quality rule);
Chương 21 bàn về định dạng (Avro, Parquet) và schema evolution; Chương 22+ chuyển sang Spark khi một máy không đủ.

## 1. Mục tiêu học tập

1. Đọc CSV và JSON Lines bằng thư viện; decode cấu trúc **lồng nhau** và trường **có thể thiếu hoặc null**.
2. Tách hai bước: **decode lỏng** (dạng thô) → **kiểm tra chặt + chuẩn hóa** (dạng sạch), báo **mọi** lỗi của một dòng.
3. Chuẩn hóa dữ liệu thực tế: mã viết lẫn hoa thường, nhiều cách viết tên quốc gia, hai định dạng thời gian, múi giờ khi tính "ngày".
4. Xử lý dữ liệu **trùng** (gửi lại), **đến không theo thứ tự**, **mồ côi** (tham chiếu tới thứ không tồn tại) — không âm thầm bỏ dòng nào.
5. Thiết kế transformation thành **hàm nhỏ, thuần**; giữ I/O ở lớp vỏ mỏng.
6. Đọc file **theo luồng** với fs2, và hiểu vì sao "đọc theo luồng" **chưa đủ** để bộ nhớ ổn định (thí nghiệm 1 triệu dòng).
7. Kiểm thử dữ liệu: golden dataset tính tay, test thuộc tính (gửi lặp, đảo thứ tự), và *mutation test* để kiểm tra chính bộ test.

## 2. Kiến thức tiên quyết

- Chương 6 (collections: `groupMapReduce`, `foldLeft`, `partition`), Chương 7 (`Either`, ADT), Chương 8A (case class, enum).
- Chương 16B (`IO`, `Ref`) — lớp vỏ dùng `IO`; phần lõi không cần.
- Project 1 (đã quen với validation, dedup, golden test).

## 3. Lý thuyết

### 3.1. Bài toán: sự kiện quảng cáo

Mỗi lần một quảng cáo **hiển thị** (impression) hoặc được **bấm** (click), SDK trên điện thoại gửi một dòng JSON lên server. Server ghi nối tiếp vào file
**JSON Lines** (`.jsonl`: mỗi dòng một object JSON). Cuối ngày cần báo cáo: mỗi chiến dịch (campaign), mỗi ngày — bao nhiêu impression, bao nhiêu click,
bao nhiêu người dùng khác nhau, CTR (click-through rate = click / impression) là bao nhiêu.

Dữ liệu thật **không sạch**:

| Hiện tượng | Ví dụ trong `data/events.jsonl` | Vì sao xảy ra |
|---|---|---|
| Cấu trúc lồng nhau | `"user":{"id":"U-1"}`, `"geo":{"country":"VN","city":"Hà Nội"}` | SDK gom trường theo nhóm |
| Thời gian hai định dạng | `"2026-10-15T01:00:00Z"` và `1792029600000` (epoch millis) | Hai phiên bản SDK |
| Mã viết lẫn lộn | `"cmp-1"`, `" CMP-1 "`, `"Viet Nam"`, `"vn"` | Nhập tay, SDK khác nhau |
| Trường thiếu / `null` | không có `device`, `"os":null`, `"city":null` | SDK cũ, quyền truy cập vị trí |
| Bản ghi trùng | dòng 4 giống hệt dòng 1; hai dòng `c6` | Mạng chập chờn → SDK **gửi lại** (at-least-once) |
| Đến không theo thứ tự | click `c2` (dòng 5) đến trước impression `i3` (dòng 6) | Nhiều thiết bị, hàng đợi gửi lại |
| Mồ côi | click trỏ tới `i999` không bao giờ tới | Impression bị mất |
| Mâu thuẫn | click ghi `CMP-2` nhưng impression của nó thuộc `CMP-1` | Bug của SDK |
| Hỏng hẳn | `{"event_id":"x1","type":` | Ghi file bị cắt ngang |

**Nguyên tắc:** không bao giờ âm thầm bỏ một dòng. Mỗi dòng hoặc được tính vào báo cáo, hoặc nằm trong `rejected.jsonl` kèm **số dòng gốc** và **mọi lý do**,
hoặc được đếm là bản trùng.

### 3.2. Decode lỏng, kiểm tra chặt

**Vấn đề.** Nếu decode thẳng JSON vào `case class AdEvent(...)` với kiểu chặt, thư viện báo lỗi đầu tiên nó gặp, bằng ngôn ngữ của thư viện
("DecodingFailure at .user.id"), và ta mất thông tin về các trường khác.

**Cách làm.** Hai bước:

```scala
// Bước 1 — dạng THÔ: mọi trường là Option; lỗi KIỂU được ghi lại theo từng trường, không làm decoder thất bại
final case class RawEvent(eventId: Option[String], eventType: Option[String], ts: Option[Json],
    userId: Option[String], campaignId: Option[String], impressionId: Option[String],
    os: Option[String], country: Option[String], city: Option[String],
    typeErrors: List[(String, String)] = Nil)

given Decoder[RawEvent] = Decoder.instance { c =>
  def str(path: String, cursor: Option[ACursor], field: String): Option[String] =
    cursor.flatMap(_.downField(field).focus) match
      case None                => None                                  // thiếu
      case Some(j) if j.isNull => None                                  // null
      case Some(j) => j.asString match
        case some @ Some(_) => some
        case None           => errors(path) = s"$path phải là chuỗi, nhận ${j.name}"; None   // SAI KIỂU: ghi lỗi
  ...
}

// Bước 2 — kiểm tra từng trường, GOM mọi lỗi, rồi mới dựng dạng SẠCH
def validate(line: Long, text: String, r: RawEvent, now: Instant): Either[Rejected, AdEvent]
```

- Thiếu và `null` đều thành `None`. Ở dữ liệu này hai trường hợp có cùng ý nghĩa; nếu với bạn chúng khác nhau (ví dụ PATCH: không gửi = giữ nguyên,
  `null` = xóa), phải xử lý riêng.
- **Sai kiểu** (`"event_id": 12`, `"user": "U-1"`) **không** làm decoder thất bại: lỗi được ghi vào `typeErrors`, và `validate` báo nó **thay cho** "thiếu ...".
  Bản đầu của lab dùng `c.get[Option[String]](...)` trong một for-comprehension: trường sai kiểu **đầu tiên** làm cả dòng dừng với thông báo của thư viện
  (`DecodingFailure at .event_id: Got value '12' with wrong type`), các lỗi khác không được báo; còn `.orElse(Right(None))` ở trường lồng nhau lại **nuốt** lỗi kiểu
  (`"os": 7` biến thành "không có OS"). Review chéo tìm ra cả hai (mục 10.6).
- `ts: Option[Json]`: chưa quyết định kiểu ở bước 1, vì trường này có hai dạng.

Kết quả: dòng `{"event_id":12,"type":"view","campaign_id":34,"geo":{"country":"BAD"}}` bị loại với **sáu** lý do một lúc (lỗi kiểu, lỗi giá trị, trường thiếu);
dòng 10 của dữ liệu mẫu bị loại với **ba** lý do — `type không hỗ trợ: view`, `thiếu user.id`, `geo.country không hợp lệ: VIETNAMM`.
Người sửa SDK nhìn một lần là thấy hết.

**Lựa chọn thay thế:** decode chặt + để thư viện báo lỗi — chấp nhận được cho API nội bộ nhỏ. `Validated`/`ValidatedNel` của Cats gom lỗi gọn hơn tuple
`Either` — Chương 20 dùng. **Lỗi thiết kế thường gặp:** `Option.get` khắp nơi "vì chắc chắn có"; bắt mọi exception rồi bỏ dòng mà không ghi lý do.

### 3.3. Chuẩn hóa: mỗi trường một hàm nhỏ

```scala
def campaignId(raw: Option[String]): Either[String, String]   // " cmp-7 " → "CMP-7"; sai định dạng → lỗi
def country(raw: Option[String]): Either[String, String]       // "vn" | "Viet Nam" | "VNM" → "VN"; phải là mã ISO THẬT
def timestamp(raw: Option[Json]): Either[String, Instant]      // ISO có múi giờ HOẶC epoch millis
```

Mỗi hàm **thuần**, nhận dữ liệu và trả kết quả — test được bằng một dòng (`NormalizeSuite`). Hàm `validate` chỉ còn việc ghép.

**Hai cái bẫy của "chuẩn hóa chuỗi":**
- **Locale.** `s.toUpperCase()` không tham số dùng locale **của máy**. Với locale Thổ Nhĩ Kỳ, `"in"` thành `"İN"` (chữ I có chấm) và `"IMPRESSION"` hạ thành
  `"ımpressıon"` — cùng file, khác máy, khác kết quả. Mã, định danh, giá trị enum luôn dùng `toUpperCase(Locale.ROOT)` / `toLowerCase(Locale.ROOT)`; test
  `M6` đặt locale mặc định là `tr-TR` để khóa điều này.
- **"Trông giống" không phải "hợp lệ".** Kiểm tra "hai chữ cái" nhận cả `ZZ` và `ÅÅ`. Lab tra `Locale.getISOCountries` (danh sách mã ISO 3166-1 alpha-2 của JDK).

**Thời gian và ngày.** `Instant` là một điểm trên trục thời gian, không có múi giờ. "Ngày" thì **có**: impression lúc `2026-10-15T17:30:00Z` là
00:30 ngày **16** ở Việt Nam. Báo cáo cho khách hàng Việt Nam phải tính ngày theo `Asia/Ho_Chi_Minh`:

```scala
day = t.atZone(Normalize.Vietnam).toLocalDate
```

Test `ngày báo cáo theo GIỜ VIỆT NAM` khóa đúng trường hợp biên này. Tính theo UTC thì một phần ba ngày làm việc ở Việt Nam bị dồn sang ngày hôm trước.

**Không đọc đồng hồ hệ thống trong hàm thuần.** Kiểm tra "ts ở tương lai" cần "bây giờ" — `validate` nhận `now` làm tham số. Test truyền một thời điểm cố định,
nên chạy hôm nay hay năm sau vẫn cho cùng kết quả.

**Trường tùy chọn.** Thiếu `os` → `"unknown"`: báo cáo theo campaign không cần OS, loại dòng vì thiếu OS là mất dữ liệu vô ích. Thiếu `city` → `None`:
giữ nguyên "không biết", không bịa giá trị. Quyết định "bắt buộc / mặc định / để trống" là quyết định **nghiệp vụ** — ghi rõ trong code.

### 3.4. Trùng, không theo thứ tự, mồ côi

Phần tổng hợp (`Aggregate.step`) là một hàm thuần `(State, AdEvent) => (State, List[Rejected])`, gọi lần lượt cho từng sự kiện:

1. **Dedup theo `event_id` — phân biệt gửi lại và xung đột.** Mỗi id được nhớ cùng một **dấu vân tay** (loại, user, campaign, impression, **ngày**):
   - cùng id, cùng dấu vân tay → **gửi lại** (retry): bỏ qua và **đếm** (`duplicates`). Đếm để biết hệ thống gửi lặp nhiều hay ít.
   - cùng id, **khác** dấu vân tay → **xung đột id** (bug SDK, id bị tái sử dụng): bản sau bị **loại** với lý do `event_id ... trùng nhưng nội dung khác`, đếm
     `conflicts`. Bản đầu vẫn được tính — nên với dữ liệu có xung đột, kết quả **phụ thuộc thứ tự đến**; điều đó được báo ra thay vì giấu.
   Bản đầu của lab coi mọi id trùng là gửi lại: hai impression cùng `i1` ở hai **ngày** khác nhau cho báo cáo khác nhau tùy thứ tự, không ai biết (mục 10.7).
2. **Impression:** ghi nhận id → campaign; "nhả" các click **đã đến trước** đang chờ impression này.
3. **Click:** impression đã có → kiểm tra cùng campaign rồi đếm; chưa có → cho vào `pendingClicks` chờ.
4. **Cuối luồng:** click vẫn chờ = **mồ côi** → `rejected.jsonl` kèm số dòng gốc.

**Kiểm tra phải giống nhau dù dữ liệu đến theo thứ tự nào.** Bản đầu của lab chỉ kiểm tra "click và impression cùng campaign" khi click đến **sau**;
click sai campaign đến **trước** vẫn được đếm. Test thuộc tính "đảo thứ tự dòng không đổi báo cáo" (mục 3.6) bắt được lỗi này.

### 3.5. Đọc theo luồng — và vì sao chưa đủ

```scala
Files[IO].readUtf8Lines(events)        // fs2: đọc từng khối, tách dòng — KHÔNG nạp cả file
  .zipWithIndex                        // số dòng gốc
  .filter((text, _) => text.trim.nonEmpty)
  .evalMap { (raw, idx) => state.modify(s => Pipeline.decide(campaigns, now)(s, raw, idx + 1) ...) }
  .flatMap(Stream.emits)               // các dòng bị loại
  .map(r => rejectedJson(r) + "\n")
  .through(text.utf8.encode)
  .through(Files[IO].writeAll(outDir / "rejected.jsonl"))   // ghi DẦN, cũng không giữ trong RAM
```

**Thí nghiệm** (máy biên soạn, 12 CPU logic, JDK 21): file 1.000.000 sự kiện (164,8 MiB) do `GenerateEvents` sinh, so sánh `NaiveIngest`
(`Files.readAllLines` rồi xử lý cả `List`) với `IngestMain` (theo luồng), cùng giới hạn heap:

| Heap (`-Xmx`) | Đọc cả file vào `List` | Đọc theo luồng |
|---|---|---|
| 128 MiB | `OutOfMemoryError` | **`OutOfMemoryError`** |
| 256 MiB | `OutOfMemoryError` | xong, 24,6 giây |
| 1 GiB | xong; heap dùng ~915 MiB | xong, 24,3 giây |

Dòng 128 MiB là bài học chính: **đọc theo luồng cũng hết bộ nhớ**. Nguyên nhân không phải file mà là **trạng thái**: để dedup, `State.seen` nhớ **mọi**
`event_id` đã thấy — 1 triệu chuỗi. Đọc theo luồng chỉ bảo đảm *dữ liệu đầu vào* không nằm hết trong RAM; bộ nhớ thật sự = **kích thước trạng thái
tích lũy**. Cách giới hạn trạng thái:

- Dedup trong **cửa sổ thời gian** (chỉ nhớ id của 10 phút gần nhất) — lời giải bài 4; đổi lại, bản trùng đến quá muộn sẽ lọt qua.
- Lưu trạng thái ra ngoài (RocksDB, bảng database) — Chương 27–30.
- Khi dữ liệu vượt một máy: Spark (Chương 22) — trạng thái được chia theo khóa ra nhiều máy.

**Không được khẳng định** rằng code xử lý `List` trong Scala có đặc tính hiệu năng giống Spark DataFrame: `List` nằm trong bộ nhớ **một** JVM, không có
query optimizer, không tự song song hóa. Chương 20 và 23 so sánh chi tiết.

> Về thời gian: 24 giây cho 1 triệu dòng (~24 µs/dòng) là chưa tối ưu — mỗi dòng một `Ref.modify`, một lần parse JSON, cấu trúc bất biến cấp phát nhiều.
> Chương này ưu tiên tính đúng và dễ đọc; Chương 32 và 34 bàn cách đo và tối ưu.

### 3.6. Kiểm thử dữ liệu

| Loại test | Ở lab | Bắt được gì |
|---|---|---|
| Unit test hàm chuẩn hóa | `NormalizeSuite` (7) | Từng quy tắc: định dạng thời gian, mã quốc gia, ngày theo giờ VN |
| Unit test tổng hợp | `PipelineSuite` (5 đầu) | Dedup, click đến trước, sai campaign, mồ côi, làm tròn CTR |
| **Golden** | `GOLDEN: ...` | Cả pipeline thật (đọc file → ghi file) so với `expected/report.csv` **tính tay trước khi chạy** |
| **Thuộc tính** | `THUỘC TÍNH: gửi lại mọi dòng`, `... đảo thứ tự` | Lỗi phụ thuộc thứ tự/lặp — loại lỗi mà ví dụ cụ thể dễ bỏ sót |

**Golden tính tay** (mục 6): viết kết quả mong đợi **trước** khi chạy chương trình. Nếu chạy trước rồi chép output làm "mong đợi", golden test chỉ chứng minh
chương trình... giống chính nó.

**Test thuộc tính** không so với một kết quả cụ thể mà kiểm tra một **quan hệ**: "nhân đôi mọi dòng → báo cáo không đổi" (dedup đúng), "đảo thứ tự →
báo cáo không đổi" (xử lý đến-sớm đúng). Lab dùng 20 hoán vị với seed cố định để test **xác định** (chạy lại cho cùng kết quả).

Thuộc tính có **tiền điều kiện**: "đảo thứ tự không đổi báo cáo" chỉ đúng khi dữ liệu **không có xung đột id** — test khẳng định `conflicts == 0` trước khi kiểm tra,
và một test riêng chứng minh với xung đột thì thứ tự **có** đổi kết quả. 20 hoán vị của một mẫu 18 dòng cũng không phải chứng minh cho mọi dữ liệu; nó bắt được loại
lỗi đã biết (mutation test bên dưới).

**Mutation test — kiểm tra chính bộ test.** Một bộ test toàn màu xanh chưa chứng minh nó bắt được lỗi. Thử **cố ý đưa lỗi vào** và xem test có đỏ không:
khi đổi kiểm tra campaign của click đến sớm thành `partition(_ => true)` (bỏ kiểm tra), **2 test thất bại** — test riêng cho trường hợp đó và test đảo thứ tự.
Nếu không test nào đỏ, bộ test có lỗ hổng.

**Kiểm tra cả dữ liệu thử.** Bản đầu của `GenerateEvents` tính campaign của click bằng `i % 3` riêng cho từng dòng, nên click lệch campaign với impression
của nó: 40% dòng bị loại ở lần chạy 1 triệu dòng. Pipeline đúng — **bộ sinh dữ liệu** sai. Con số bất thường (400.666 dòng bị loại) là dấu hiệu; đừng bỏ qua.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| JSON Lines (`.jsonl`) | Mỗi dòng một JSON — đọc/ghi từng dòng, nối thêm dễ, hỏng một dòng không hỏng cả file |
| Impression / Click / CTR | Lượt hiển thị / lượt bấm / tỷ lệ bấm = click ÷ impression |
| Raw / staging / clean | Dữ liệu thô / đang xử lý / đã kiểm tra |
| Normalization | Chuẩn hóa về một dạng (mã chữ hoa, ISO country code, Instant) |
| Deduplication | Loại bản trùng |
| At-least-once delivery | Mỗi sự kiện được giao **ít nhất** một lần — có thể nhiều lần |
| Out-of-order events | Sự kiện đến không theo thứ tự thời gian |
| Orphan record | Bản ghi tham chiếu tới thứ không tồn tại |
| Streaming (fs2 `Stream`) | Xử lý từng phần tử/khối khi đọc, không nạp toàn bộ |
| State | Thông tin tích lũy qua các phần tử (đã thấy id nào, đếm bao nhiêu) |
| Golden dataset | Bộ dữ liệu + kết quả mong đợi cố định để so sánh |
| Property-based test | Test kiểm tra một quan hệ luôn đúng trên nhiều đầu vào |
| Mutation testing | Cố ý cài lỗi để kiểm tra bộ test có phát hiện không |
| Sessionization | Gom các hoạt động của một người thành "phiên" theo khoảng nghỉ |

## 5. Ví dụ đơn giản

Decode một dòng JSON lồng nhau bằng circe, trường có thể thiếu:

```scala
import io.circe.parser

val line = """{"event_id":"i1","user":{"id":"u-1"},"geo":{"country":"vn"}}"""
val result = parser.decode[RawEvent](line)
// Right(RawEvent(Some(i1),None,None,Some(u-1),None,None,None,Some(vn),None))
// "type", "ts", "campaign_id" thiếu → None; chưa có lỗi nào ở bước này
```

Bước kiểm tra sau đó sẽ báo `thiếu type`, `thiếu ts`, `thiếu campaign_id` — cả ba cùng lúc.

## 6. Ví dụ thực tế — báo cáo tính tay trước khi chạy

`data/events.jsonl` có 18 dòng (1 dòng trống). Trước khi chạy chương trình, ta tính tay từng dòng:

| Dòng | Sự kiện | Kết luận |
|---|---|---|
| 1 | impression `i1`, CMP-1, U-1, 01:00Z | ✅ CMP-1 / 15-10 |
| 2 | impression `i2`, ts epoch = 02:00Z, `" CMP-1 "`, `"Viet Nam"` | ✅ CMP-1 / 15-10 (sau chuẩn hóa) |
| 3 | click `c1` → `i1`, `08:05+07:00` | ✅ CMP-1 / 15-10 |
| 4 | giống hệt dòng 1 | trùng |
| 5 | click `c2` → `i3` (chưa có) | chờ |
| 6 | impression `i3`, CMP-2 | ✅ + nhả `c2` ✅ CMP-2 / 15-10 |
| 7 | impression `i4`, 17:30Z = 00:30 ngày 16 giờ VN | ✅ CMP-1 / **16-10** |
| 8 | click `c3` → `i999` | chờ → cuối luồng: **mồ côi** |
| 9 | JSON bị cắt | ❌ JSON không hợp lệ |
| 10 | type `view`, thiếu user, country `Vietnamm` | ❌ 3 lý do |
| 11 | ts 2026-12-01 (tương lai) | ❌ |
| 12 | CMP-9 không có trong `campaigns.csv` | ❌ |
| 13 | click `c4` → `i2` (CMP-1) nhưng ghi CMP-2 | ❌ mâu thuẫn |
| 14 | (trống) | bỏ qua, không tính dòng |
| 15 | click thiếu `impression_id` | ❌ |
| 16 | click `c6` → `i2` | ✅ CMP-1 / 15-10 |
| 17 | `c6` lần nữa (ts khác 5 giây) | trùng (theo `event_id`) |
| 18 | impression `i5`, `os: null`, `city: null` | ✅ CMP-2 / 15-10 |

Kết quả tính tay → `expected/report.csv`:

| campaign | ngày | impressions | clicks | users | CTR |
|---|---|---|---|---|---|
| CMP-1 | 15-10 | 2 (i1, i2) | 2 (c1, c6) | 2 (U-1, U-2) | 100.00 |
| CMP-1 | 16-10 | 1 (i4) | 0 | 1 | 0.00 |
| CMP-2 | 15-10 | 2 (i3, i5) | 1 (c2) | 2 (U-3, U-6) | 50.00 |

Tổng: 17 dòng không trống = 8 được tính + 6 bị loại khi đọc + 1 mồ côi + 2 trùng.

## 7. Code đầy đủ

```
08-data-processing/ingest-lab/
├── project.scala / build.sbt / project/build.properties
├── data/events.jsonl, data/campaigns.csv      ← dữ liệu mẫu bẩn có chủ ý
├── expected/report.csv                        ← golden, tính TAY
├── src/main/scala/vn/scalaeng/ingest/
│   ├── Model.scala      ← RawEvent (+ decoder lồng nhau), AdEvent, Campaign, Rejected, ReportRow
│   ├── Normalize.scala  ← hàm thuần: parse, timestamp, campaignId, userId, country, validate
│   ├── Aggregate.scala  ← State, step (dedup, đến sớm, mâu thuẫn), orphans, report (CTR)
│   ├── Ingest.scala     ← Pipeline.decide/runAll (thuần) + Ingest.run (fs2: đọc/ghi theo luồng, CSV)
│   └── Mains.scala      ← IngestMain, GenerateEvents (sinh dữ liệu lớn), NaiveIngest (cố ý sai, cho thí nghiệm)
└── src/test/scala/vn/scalaeng/ingest/
    ├── NormalizeSuite.scala   ← 11 test (4 regression từ review chéo)
    └── PipelineSuite.scala    ← 9 test: tổng hợp, xung đột id, thuộc tính, golden
```

```bash
cd 08-data-processing/ingest-lab
scala-cli test .                                   # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.ingest.IngestMain -- data/events.jsonl data/campaigns.csv out

# Thí nghiệm bộ nhớ (mục 3.5)
scala-cli run . --main-class vn.scalaeng.ingest.GenerateEvents -- big.jsonl 1000000
scala-cli run . --main-class vn.scalaeng.ingest.NaiveIngest --java-opt -Xmx256m -- big.jsonl
scala-cli run . --main-class vn.scalaeng.ingest.IngestMain  --java-opt -Xmx256m -- big.jsonl data/campaigns.csv out-big
```

## 8. Giải thích code

- **`Model.scala`** — `RawEvent` và decoder viết tay bằng `Decoder.instance` + `downField`: rõ ràng hơn derive tự động khi JSON lồng nhau còn case class phẳng.
  `@CsvName("campaign_id")` đặt tên cột CSV kiểu snake_case cho field camelCase.
- **`Normalize.validate`** — gom 6 kết quả `Either` vào một tuple, `collect { case Left(e) => e }` lấy mọi lỗi; chỉ khi không có lỗi mới dựng `AdEvent`.
  Ràng buộc **liên trường** (click phải có `impression_id`) kiểm tra sau các trường đơn.
- **`Aggregate.step`** — `State` bất biến; mỗi bước trả `State` mới. Khi impression đến, `partition` các click đang chờ thành *cùng campaign* (đếm) và
  *khác campaign* (loại).
- **`Aggregate.report`** — CTR bằng `BigDecimal(clicks) * 100 / impressions` rồi `setScale(2, HALF_UP)`; không dùng `Double` cho số sẽ hiển thị cho khách hàng.
  Sắp xếp theo `(campaign, ngày)` để output **xác định**.
- **`Pipeline.decide`** — quyết định cho MỘT dòng, thuần; dùng chung cho `Ingest.run` (đọc file) và test (`runAll`, không cần file) — không có hai bản logic.
- **`Ingest.run`** — `Ref.modify` giữ trạng thái giữa các phần tử của stream; `evalMap` + `flatMap(Stream.emits)` biến "0..n dòng bị loại mỗi bước"
  thành một luồng ghi ra `rejected.jsonl`. Click mồ côi được nối thêm (`Flags.Append`) sau khi luồng kết thúc.
- **fs2-data-csv** — `decodeUsingHeaders[Campaign]()` đọc CSV theo header; `encodeUsingFirstHeaders[ReportRow]()` ghi CSV với header từ tên field.
  `CellEncoder[BigDecimal]` dùng `toPlainString` để không ra dạng `1E+2`.

## 9. Output mong đợi (output thật)

```
$ scala-cli run . --main-class vn.scalaeng.ingest.IngestMain -- data/events.jsonl data/campaigns.csv out
Summary(17,8,7,2,1,3) trong 943 ms
```

`Summary(lines, accepted, rejected, duplicates, orphanClicks, reportRows)`. `out/report.csv` — **khớp từng byte** với `expected/report.csv` tính tay:

```
campaign_id,campaign_name,day,impressions,clicks,unique_users,ctr_percent
CMP-1,Khuyến mãi tháng 10,2026-10-15,2,2,2,100.00
CMP-1,Khuyến mãi tháng 10,2026-10-16,1,0,1,0.00
CMP-2,Ra mắt sản phẩm,2026-10-15,2,1,2,50.00
```

`out/rejected.jsonl` (trường `raw` rút gọn):

```
{"line":9,"reasons":["JSON không hợp lệ: exhausted input"],"raw":"{\"event_id\":\"x1\",\"type\":"}
{"line":10,"reasons":["type không hỗ trợ: view","thiếu user.id","geo.country không hợp lệ: VIETNAMM"],"raw":"..."}
{"line":11,"reasons":["ts ở tương lai: 2026-12-01T00:00:00Z"],"raw":"..."}
{"line":12,"reasons":["campaign không tồn tại: CMP-9"],"raw":"..."}
{"line":13,"reasons":["click thuộc CMP-1 nhưng ghi CMP-2"],"raw":"..."}
{"line":15,"reasons":["click thiếu impression_id"],"raw":"..."}
{"line":8,"reasons":["click mồ côi: không có impression i999"],"raw":"..."}
```

**1 triệu dòng** (`-Xmx256m`) và báo cáo:

```
Summary(1000000,999000,1000,0,0,2) trong 24613 ms
campaign_id,campaign_name,day,impressions,clicks,unique_users,ctr_percent
CMP-1,Khuyến mãi tháng 10,2026-10-15,449000,50000,9980,11.14
CMP-2,Ra mắt sản phẩm,2026-10-15,450000,50000,10000,11.11
```

1.000 dòng bị loại là các dòng `GenerateEvents` cố ý làm hỏng (mỗi 1.000 dòng một dòng). `NaiveIngest -Xmx1g`:
`đọc 1000000 dòng, hợp lệ 999000; heap dùng ~915 MiB`.

**Test:** `NormalizeSuite 11/11`, `PipelineSuite 9/9`; `sbt test`: `Passed: Total 20, Failed 0`.

## 10. Lỗi thường gặp (thật)

**1. "Recursion limit exceeded" khi nối `Stream.emits` với pipe của fs2-data.** Viết `Stream.emits(rows).through(encodeUsingFirstHeaders[ReportRow]())`:

```
[error] .\src/main/scala/vn/scalaeng/ingest/Ingest.scala:67:78
[error] Recursion limit exceeded.
[error] Maybe there is an illegal cyclic reference?
[error] A recurring operation is (inner to outer):
[error]   subtype F <:< [x] =>> fs2.Pure[x]
```

`Stream.emits` tạo `Stream[Pure, A]` (luồng không có effect); pipe của fs2-data có tham số effect `F[_]` cần suy ra, và compiler rơi vào vòng suy luận.
Sửa: chỉ rõ effect — `Stream.emits(rows).covary[IO].through(...)`. Thông báo lỗi trông đáng sợ nhưng nguyên nhân chỉ là thiếu một chú thích kiểu.

**2. `row.as[T]` không giải mã cả dòng.** Lời giải bài 6 lúc đầu viết `.map(row => row.as[OrderRow])`:

```
[error] No implicit CellDecoder found for type vn.scalaeng.solutions.ch19.OrderRow.
[error] You can define one using CellDecoder.instance, by calling map on another CellDecoder or by using generic derivation for coproducts and unary products.
```

`CsvRow.as[T]` của fs2-data giải mã **một ô** (cần `CellDecoder`), không phải cả dòng. Thông báo gợi ý derive `CellDecoder` — **gợi ý sai hướng** cho
trường hợp này. Sửa: `summon[CsvRowDecoder[OrderRow, String]](row)` (hoặc dùng `decodeUsingHeaders[OrderRow]()` trên cả luồng).

**3. `OutOfMemoryError` dù đã đọc theo luồng** — mục 3.5: trạng thái tích lũy (tập id để dedup) mới là thứ chiếm bộ nhớ.

**4. Ngày lệch một ngày** — tính `LocalDate` theo UTC (hoặc theo múi giờ của máy chạy job!) thay vì múi giờ của nghiệp vụ. `ZoneId.systemDefault()` trong
code xử lý dữ liệu là dấu hiệu nguy hiểm: cùng một job chạy ở hai máy khác múi giờ cho hai báo cáo khác nhau.

**5. Số liệu bất thường mà không có lỗi** — 40% dòng bị loại vì bộ sinh dữ liệu sai (mục 3.6). Luôn xem tỷ lệ `rejected / lines` và lý do nhiều nhất.

**Các lỗi 10.6–10.10 do review chéo của Codex tìm ra bằng chương trình thăm dò (probe) — mọi test của bài lúc đó đều xanh.**

**6. Decoder dừng ở lỗi kiểu đầu tiên.** Bản đầu, dòng `{"event_id":12,"type":"view","campaign_id":34,"geo":{"country":"BAD"}}` chỉ cho **một** lý do:
`JSON không hợp lệ: DecodingFailure at .event_id: Got value '12' with wrong type, expecting string`. Sửa: đọc từng trường, ghi lỗi kiểu (mục 3.2) — giờ là 6 lý do.

**7. Cùng `event_id`, khác nội dung, bị coi là gửi lại** — báo cáo phụ thuộc thứ tự (mục 3.4). Sửa: dấu vân tay + loại xung đột.

**8. `ZZ` được nhận là mã quốc gia; `"in"` thành `"İN"` trên máy locale Thổ Nhĩ Kỳ** (mục 3.3). Sửa: `Locale.getISOCountries` + `Locale.ROOT`.

**9. Lời giải bài 6: dòng thiếu cột làm dừng cả file** — `HeaderSizeError: Headers have size 4 but row has size 3`; và hai đơn hợp lệ `9223372036854775807` + `1`
cho tổng **âm** `-9223372036854775808`. Sửa: `lenient.attemptDecodeUsingHeaders` + tổng `BigInt`.

**10. Lời giải bài 1: `2026-02-30` được "sửa" thành `2026-02-28`.** `DateTimeFormatter.ofPattern` mặc định `ResolverStyle.SMART`. Sửa: `uuuu` + `STRICT`.

## 11. Cách debug

- **Một dòng bị loại không rõ vì sao:** `rejected.jsonl` có `line` và `raw` — chép `raw` vào test `Normalize.event(1, raw, now)`.
- **Số liệu báo cáo lệch:** tính tay trên một mẫu nhỏ (như mục 6), rồi chạy `Pipeline.runAll` trên đúng mẫu đó trong REPL/test.
- **Nghi lỗi do thứ tự:** chạy test đảo thứ tự với nhiều seed hơn; in seed khi test thất bại (lab truyền `s"seed=$seed"` vào assertion).
- **Hết bộ nhớ:** đo kích thước trạng thái (`state.seen.size`, `pendingClicks.size`) theo số dòng; xem heap bằng JFR (Chương 11).
- **Chậm:** đo từng bước (parse JSON, validate, `Ref.modify`) trên 100.000 dòng trước khi đoán.

## 12. Bài tập

1. **(Cơ bản)** SDK cũ gửi `ts` dạng `"2026-10-15 08:00:00"` không có múi giờ, tài liệu SDK nói là giờ Việt Nam. Hỗ trợ định dạng này như phương án cuối.
2. **(Cơ bản)** Báo cáo số impression theo **(quốc gia, ngày VN)** từ danh sách `AdEvent` đã sạch.
3. **(Trung bình)** Một SDK sinh `event_id` **mới** mỗi lần gửi lại. Dedup theo **nội dung**; nêu đánh đổi.
4. **(Trung bình)** Dedup với **bộ nhớ giới hạn**: chỉ nhớ id trong cửa sổ 10 phút theo thời gian sự kiện. Đo số id lớn nhất phải nhớ; nêu trường hợp lọt.
5. **(Nâng cao)** **Sessionization**: các hoạt động của một user cách nhau quá 30 phút thuộc hai phiên. Trả số sự kiện và thời lượng mỗi phiên.
6. **(Nâng cao)** Đọc CSV đơn hàng `order_id,order_date,amount_vnd,status` **theo luồng**, tính doanh thu theo ngày cho đơn `PAID`; dòng hỏng được **đếm**,
   không làm dừng cả file.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch19-solutions.md`](../solutions/ch19-solutions.md)
- Code + test (6 test): [`solutions/ch19-code/`](../solutions/ch19-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao tách "decode lỏng" và "kiểm tra chặt" thành hai bước?
2. `c.get[Option[String]]("x")` trả gì khi trường vắng mặt? Khi `"x": null`? Khi nào cần phân biệt hai trường hợp?
3. Impression lúc `2026-10-15T17:30:00Z` thuộc ngày nào trong báo cáo cho Việt Nam? Vì sao không dùng `ZoneId.systemDefault()`?
4. Đọc file theo luồng có bảo đảm bộ nhớ ổn định không? Thí nghiệm 128 MiB nói gì?
5. Vì sao phải **đếm** bản trùng chứ không chỉ bỏ qua?
6. Golden test khác test thuộc tính thế nào? Vì sao phải tính golden **trước** khi chạy?
7. Mutation test kiểm tra điều gì?
8. Vì sao không được nói xử lý `List` trong Scala "giống Spark nhưng nhỏ hơn"?

<details><summary>Gợi ý đáp án</summary>

1. Để báo **mọi** lỗi của một dòng bằng ngôn ngữ nghiệp vụ, và để kiểu dữ liệu sạch chỉ tồn tại khi đã hợp lệ.
2. `Right(None)` cho cả hai. Cần phân biệt khi "không gửi" và "gửi null" mang nghĩa khác (ví dụ cập nhật một phần).
3. Ngày 16-10 (00:30 giờ VN). `systemDefault` phụ thuộc máy chạy — cùng dữ liệu, khác máy, khác báo cáo.
4. Không. Theo luồng chỉ giữ *đầu vào* nhỏ; *trạng thái* (tập id để dedup) vẫn tăng theo dữ liệu → OOM ở 128 MiB.
5. Số bản trùng là chỉ số sức khỏe của hệ thống gửi; tăng đột biến là sự cố cần điều tra.
6. Golden so với một kết quả cụ thể; thuộc tính kiểm tra quan hệ (lặp/đảo thứ tự không đổi kết quả). Golden chép từ output thì chỉ chứng minh chương trình giống chính nó.
7. Bộ test có thật sự phát hiện lỗi hay không — cố ý cài lỗi, test phải đỏ.
8. `List` nằm trong bộ nhớ một JVM, không có optimizer, không song song hóa, không chịu lỗi máy — đặc tính hiệu năng và vận hành khác hẳn.
</details>

## 15. Mini project

**"User activity log"**: file JSON Lines các hoạt động `{"user":{"id"},"action":"view|add_to_cart|purchase","sku","ts"}` từ web và app (hai định dạng
thời gian, trùng, đến không theo thứ tự). Xây pipeline theo khuôn của lab:
- Dạng thô → dạng sạch, `rejected.jsonl` có số dòng và mọi lý do.
- Báo cáo theo ngày VN: số user hoạt động, tỷ lệ chuyển đổi view → purchase theo SKU.
- Sessionization (lời giải bài 5) và thời lượng phiên trung vị.
- Golden tính tay cho 20 dòng; test thuộc tính gửi lặp + đảo thứ tự; một mutation test.
- Thí nghiệm bộ nhớ với 1 triệu dòng: heap nhỏ nhất chạy được là bao nhiêu? Trạng thái nào chiếm nhiều nhất?

## 16. Nguồn tài liệu chính thức

- fs2 (Stream, Files, text): https://fs2.io/
- fs2-data CSV: https://fs2-data.gnieh.org/documentation/csv/
- circe — decoding, cursors: https://circe.github.io/circe/
- JSON Lines: https://jsonlines.org/
- java.time (`Instant`, `ZoneId`, `ZonedDateTime`): https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/package-summary.html
- ISO 3166-1 (mã quốc gia): https://www.iso.org/iso-3166-country-codes.html
- Maven Central (tra phiên bản): https://repo1.maven.org/maven2/

## 17. Checklist tự đánh giá

- [ ] Decode được JSON lồng nhau, trường thiếu/null, bằng `Decoder.instance` + `downField`.
- [ ] Viết được validation báo mọi lỗi của một dòng, có số dòng gốc.
- [ ] Chuẩn hóa thời gian nhiều định dạng và tính ngày theo đúng múi giờ nghiệp vụ.
- [ ] Xử lý được bản trùng, sự kiện đến sớm, bản ghi mồ côi — không bỏ dòng nào âm thầm.
- [ ] Đọc/ghi file theo luồng với fs2; giải thích vì sao vẫn có thể hết bộ nhớ.
- [ ] Viết golden test tính tay, test thuộc tính, và thử một mutation test.
- [ ] Chạy được 15 test của lab và thí nghiệm 1 triệu dòng trên máy mình.
