# Project 1 — Scala Data Processor

> **Phiên bản:** Scala 3.9.0 · JDK 21 · sbt 1.12.11 · munit 1.2.4 · **không dependency ngoài nào khác**
> **Kiến thức sử dụng:** Chương 1–11 (không cần framework)
> **Trạng thái kiểm chứng:**
> - Toàn bộ mã nguồn **đã biên dịch** bằng `scalac 3.9.0` với `-Werror -Wunused:all -Wvalue-discard`, **không cảnh báo**.
> - Chương trình **đã chạy thật** trên dữ liệu mẫu; `report.csv` và `summary.json` khớp **kết quả tính tay trước khi chạy** (mục 7).
> - **35/35 test pass** bằng `sbt test` (sbt 1.12.11, munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).

---

## 1. Mục tiêu

Nắm chắc Scala core và Functional Programming bằng một chương trình xử lý dữ liệu hoàn chỉnh:

- Đọc **CSV** và **JSON**, validate với **tích lũy lỗi**, chuẩn hóa dữ liệu bẩn.
- Dedup theo khóa nghiệp vụ với quy tắc rõ ràng; join hai nguồn; aggregate.
- Tách **core thuần** (không `var`, không I/O) khỏi **shell** (file, log, exit code).
- Output **xác định** (chạy lại cho file giống hệt từng byte) và ghi file **nguyên tử**.
- Kiểm thử ở 4 tầng: unit, property-based, golden, integration.

## 2. Kiến thức tiên quyết

| Chương | Dùng ở đâu |
|---|---|
| 5 OOP | `enum OrderStatus`, `Source`; case class `Order`, `User` |
| 6 Collections | `groupBy`, `partitionMap`, `groupMapReduce`, `foldLeft` |
| 7 FP | `Either`, ADT lỗi, Functional Core / Imperative Shell |
| 8A Types | Opaque type cho id, tiền, email, số điện thoại, thành phố |
| 9 Project Structure | 2 module sbt, exit code, structured logging |
| 10 Testing | Gen/Prop tự viết, golden, `FunFixture` |
| 11 JVM | Đệ quy đuôi (không tràn stack với dòng 50.000 trường) |

## 3. Đặc tả

### 3.1. `orders.csv`

Header bắt buộc (không phân biệt hoa thường, cho phép BOM UTF-8):
`order_id,user_id,sku,unit_price,quantity,status,order_date,updated_at`

| Trường | Quy tắc | Chuẩn hóa |
|---|---|---|
| `order_id` | `ORD-<1..10 chữ số>` | chữ hoa |
| `user_id` | `U-<1..10 chữ số>` | chữ hoa |
| `sku` | `[A-Z0-9][A-Z0-9-]{0,31}` (không dấu phẩy, không khoảng trắng) | chữ hoa |
| `unit_price` | VND > 0, tối đa 10 tỷ; chấp nhận `120000`, `1.250.000`, `1,250,000`, hậu tố `VND`/`đ`; **cấm** trộn `.` và `,`, cấm phần lẻ | → `Long` (đồng) |
| `quantity` | số nguyên 1..1000, chỉ chữ số (`+3` bị từ chối) | |
| `status` | `PAID` / `CANCELLED` / `REFUNDED` | không phân biệt hoa thường |
| `order_date` | `yyyy-MM-dd` | |
| `updated_at` | ISO-8601 UTC, ví dụ `2026-10-01T08:30:00Z` | |
| **Liên trường** | ngày của `updated_at` **theo giờ Việt Nam** ≥ `order_date` | |

Dòng trống bị bỏ qua nhưng **số dòng luôn tính theo file gốc** (header là dòng 1).

### 3.2. `users.json`

Một **mảng** JSON các object:

| Trường | Bắt buộc | Quy tắc |
|---|---|---|
| `user_id` | có | như trên |
| `name` | có | chuỗi không rỗng; gộp khoảng trắng thừa, chuẩn hóa Unicode NFC |
| `email` | không | thiếu / `null` / `""` → không có; nếu có thì phải hợp lệ; chuẩn hóa chữ thường |
| `phone` | không | như email; chấp nhận `+84`, `84`, khoảng trắng, `.`, `-`, `()`; kết quả là **số di động 10 chữ số** bắt đầu bằng `0` |
| `city` | có | ánh xạ alias (`TP HCM`, `hcm`, `Sài Gòn` → `Hồ Chí Minh`; `ha noi`, `HN` → `Hà Nội`; ...), tên khác giữ nguyên sau chuẩn hóa |

### 3.3. Quy tắc xử lý

1. **Lỗi cấp file** (header sai, JSON hỏng, gốc JSON không phải mảng, file không phải UTF-8) → dừng, exit code 2.
2. **Lỗi cấp bản ghi** → bản ghi vào `rejected.csv` với **mọi** lý do; các bản ghi khác vẫn được xử lý.
3. **Dedup orders theo `order_id`:** giữ bản có `updated_at` **mới nhất**; nếu bằng nhau giữ bản **xuất hiện trước** và phân biệt "trùng hoàn toàn" với "xung đột nội dung".
4. **Dedup users theo `user_id`:** giữ phần tử đầu tiên.
5. **Join:** order có `user_id` không tồn tại (hoặc user bị loại) → thành phố `UNKNOWN`, đếm `orphan_orders`, log cảnh báo — **không** loại order.
6. **Aggregate:** chỉ đơn `PAID`, nhóm theo (`order_date`, thành phố): số đơn, số món, doanh thu.
7. **Ngưỡng chất lượng:** tỷ lệ dòng order bị loại > `--max-reject-ratio` (mặc định 0.1) → exit code 2, **nhưng vẫn ghi output** để điều tra.
8. **Bất biến bắt buộc:** `order_rows = orders_valid + orders_rejected + orders_superseded` và `user_items = users_valid + users_rejected`.

### 3.4. Output (thư mục `--out`)

| File | Nội dung |
|---|---|
| `report.csv` | `date,city,paid_orders,items,revenue_vnd`, sắp theo ngày rồi thành phố |
| `rejected.csv` | `source,position,issues,raw` — nguồn, vị trí, mọi lý do (nối bằng ` \| `), dữ liệu gốc |
| `summary.json` | Thống kê + `balanced` |

Output **không** chứa thời điểm chạy → chạy lại cho file giống hệt. Thời điểm nằm trong log (stderr).

### 3.5. Exit code

| Code | Ý nghĩa |
|---|---|
| 0 | Thành công |
| 1 | Tham số dòng lệnh sai |
| 2 | Lỗi dữ liệu cấp file, hoặc vượt ngưỡng tỷ lệ lỗi |
| 3 | Lỗi I/O (thiếu file, không ghi được) |

## 4. Thiết kế

```
modules/core  (THUẦN — không var, không I/O)                     modules/app (shell)
┌───────────────────────────────────────────────────────────┐    ┌──────────────────────────┐
│ csv/Csv          parseLine: máy trạng thái + đệ quy đuôi   │    │ Args.parse (thuần)        │
│ json/Json        ADT + recursive descent parser + render   │◄───│ Main.run: đọc file,       │
│ model/*          opaque type + smart constructor           │    │  gọi Processor, ghi file  │
│ parse/*          OrderParser, UserParser (tích lũy lỗi)    │    │  nguyên tử, log JSON,     │
│ pipeline/        Dedup, Processor (join, aggregate, stats) │    │  exit code                │
│ report/Reports   CSV/JSON output (không timestamp)         │    └──────────────────────────┘
└───────────────────────────────────────────────────────────┘
```

**Luồng dữ liệu:**

```
orders.csv ─► header? ─► parseRow (8 trường, tích lũy lỗi, liên trường) ─┬─► rejected
                                                                         └─► Dedup.latestByOrderId ─┬─► superseded ─► rejected
users.json ─► JSON? mảng? ─► parseUser ─┬─► rejected                                                │
                                        └─► Dedup.firstByUserId ─► userById ─► join ◄──────────────┘ kept
                                                                                │
                                                     filter PAID ─► groupBy (date, city) ─► report.csv
```

### Các quyết định thiết kế và trade-off

| Quyết định | Lý do | Cái giá / phương án khác |
|---|---|---|
| Tự viết JSON parser | P1 chỉ dùng standard library; hiểu parser hoạt động | Thực tế dùng circe / jsoniter-scala (Ch19, Ch21): nhanh hơn, đầy đủ hơn, có codec tự sinh |
| CSV parser nghiêm ngặt (`a, "b"` là lỗi) | RFC 4180; tránh đoán ý | File từ một số hệ thống có khoảng trắng trước dấu nháy sẽ bị loại — có thể nới lỏng nếu nghiệp vụ yêu cầu |
| Tích lũy lỗi bằng tuple + `productIterator` | Không cần thư viện; báo **mọi** lỗi | `productIterator` mất kiểu tĩnh; Cats `Validated` + `mapN` an toàn kiểu hơn (Ch16B) |
| Dedup giữ `updated_at` mới nhất | Mô phỏng CDC: bản mới nhất là sự thật | Nếu đồng hồ nguồn sai lệch, "mới nhất" có thể sai — cần sequence number từ nguồn (Ch28) |
| Order mồ côi giữ lại với `UNKNOWN` | Không làm mất doanh thu; vấn đề được giám sát | Có hệ thống chọn loại order (referential integrity chặt) — quyết định nghiệp vụ |
| Ngày nghiệp vụ theo giờ VN | 18:30 UTC ngày 01/10 là 01:30 ngày 02/10 ở VN | Hệ thống đa quốc gia cần timezone theo cửa hàng/khách hàng |
| Tiền dùng `Long` (đồng) | VND không có phần lẻ; nhanh; `addExact` chống tràn | Ngoại tệ/tỷ giá cần `BigDecimal` |
| Sắp xếp thành phố theo mã Unicode | Xác định, đơn giản | Không đúng thứ tự từ điển tiếng Việt (`Đ` xếp sau `U`) — xem bài mở rộng 3 |
| Đọc toàn bộ file vào bộ nhớ | Đơn giản; đủ cho file vài chục MB | File nhiều GB cần đọc dạng stream (Ch19) hoặc Spark (Ch22) |

## 5. Build và chạy

```bash
cd 14-projects/p1-scala-data-processor
sbt test                                   # 35 test
CI=true sbt test                           # như CI: cảnh báo là lỗi

sbt "app/run --orders data/orders.csv --users data/users.json --out out"
echo $?                                    # 2: dữ liệu mẫu cố ý có 6/19 dòng lỗi (31,6% > 10%)

sbt "app/run --orders data/orders.csv --users data/users.json --out out --max-reject-ratio 0.5"
echo $?                                    # 0
```

## 6. Output mong đợi (output thật)

Log (stderr):

```
{"ts":"...","level":"INFO","msg":"finished","order_rows":19,"orders_valid":10,"orders_rejected":6,"orders_superseded":3,"users_valid":4,"users_rejected":4,"orphan_orders":2,"balanced":true}
{"ts":"...","level":"WARN","msg":"orders reference unknown users","count":2}
{"ts":"...","level":"WARN","msg":"order reject ratio above threshold","ratio":0.316,"max":0.1}
```

`out/report.csv`:

```
date,city,paid_orders,items,revenue_vnd
2026-10-01,Hà Nội,1,1,1250000
2026-10-01,Hồ Chí Minh,1,3,360000
2026-10-02,Cần Thơ,1,4,396000
2026-10-02,UNKNOWN,1,2,170000
2026-10-02,Đà Nẵng,2,2,470000
2026-10-03,Hà Nội,1,1,120000
2026-10-03,UNKNOWN,1,1,120000
```

`out/summary.json`:

```json
{"order_rows":19,"orders_valid":10,"orders_rejected":6,"orders_superseded":3,"user_items":8,"users_valid":4,"users_rejected":4,"orphan_orders":2,"paid":8,"cancelled":1,"refunded":1,"paid_revenue_vnd":2886000,"balanced":true}
```

`out/rejected.csv`: 13 dòng — xem [`expected/rejected.csv`](expected/rejected.csv) (đã được rà soát từng dòng).

## 7. Kiểm tra tay (làm TRƯỚC khi chạy chương trình)

Đây là cách tạo golden dataset đáng tin: tính kết quả bằng tay từ đặc tả, rồi để chương trình xác nhận.

**Orders** (số dòng theo file; dòng 19 trống bị bỏ qua → 19 dòng dữ liệu):

| Dòng | Nội dung chính | Kết quả |
|---|---|---|
| 2 | ORD-1 q=2 lúc 02:00Z | bị dòng 4 thay (mới hơn) |
| 3 | ORD-2 giá `"1.250.000"` | **giữ** — 1.250.000đ, Hà Nội, 01/10 |
| 4 | ORD-1 q=3 lúc 05:00Z | **giữ** — 360.000đ, HCM, 01/10 |
| 5 | bản sao y hệt dòng 3 | trùng hoàn toàn với dòng 3 |
| 6 | ORD-3 q=1 | **giữ** — 350.000đ, Đà Nẵng, 02/10 |
| 7 | ORD-3 q=2, cùng `updated_at` | xung đột với dòng 6, giữ dòng 6 |
| 8 | giá `abc`, q=0, `SHIPPED` | loại — **3 lỗi** |
| 9 | 4 cột | loại |
| 10 | nháy kép không đóng | loại |
| 11 | ORD-7 CANCELLED | giữ, không tính doanh thu |
| 12 | ORD-8 REFUNDED | giữ, không tính doanh thu |
| 13 | ORD-9 user U-99 không tồn tại | **giữ** — 170.000đ, UNKNOWN, 02/10 |
| 14 | `updated_at` 10:00Z ngày 02 (17:00 VN) < `order_date` 03 | loại — liên trường |
| 15 | `updated_at` 18:30Z ngày 01 = **01:30 ngày 02 ở VN**, `order_date` 02 | **giữ** — 120.000đ, Đà Nẵng, 02/10 |
| 16 | giá `"99,000 VND"`, q=4, status `paid` | **giữ** — 396.000đ, Cần Thơ, 02/10 |
| 17 | giá `"1.000,000"` (trộn phân cách) | loại |
| 18 | sku `"GIAY,DEN"` (CSV đúng, sku sai) | loại |
| 20 | `ord-20,u-2,...,Paid` | **giữ** sau chuẩn hóa — 120.000đ, Hà Nội, 03/10 |
| 21 | user U-4 (user bị loại) | **giữ** — 120.000đ, UNKNOWN, 03/10 |

→ 19 = 10 giữ + 6 loại + 3 bị thay ✓. PAID: 8; doanh thu: 1.250.000 + 360.000 + 350.000 + 170.000 + 120.000 + 396.000 + 120.000 + 120.000 = **2.886.000** ✓.

**Users:** #1 U-1 (HCM), #2 U-2 (`ha noi` → Hà Nội), #3 U-3 (`Da Nang` → Đà Nẵng), #8 U-8 (`  Cần   Thơ ` → Cần Thơ) hợp lệ;
#4 (3 lỗi), #5 (trùng U-1), #6 (không phải object), #7 (`name` là số) bị loại → 8 = 4 + 4 ✓.

## 8. Kiểm thử

| Suite | Số test | Loại | Nội dung chính |
|---|---|---|---|
| `CsvJsonSuite` | 7 | unit + property | RFC 4180, dòng 50.000 trường (đệ quy đuôi), JSON lồng/escape/vị trí lỗi/giới hạn độ sâu, **round-trip** CSV và JSON |
| `ValuesSuite` | 7 | unit | Mọi quy tắc của tiền, số lượng, email, điện thoại, thành phố (cả Unicode NFD), id, status |
| `ParserSuite` | 7 | unit | Tích lũy **8 lỗi** một lúc, lỗi cấu trúc, liên trường theo giờ VN (biên 16:59Z / 18:30Z), trường tùy chọn |
| `ProcessorSuite` | 7 | unit + property | Header/BOM, dedup + lý do, chỉ PAID, mồ côi; **bất biến cân bằng**, **bảo toàn doanh thu**, **không phụ thuộc thứ tự dòng**, xác định |
| `GoldenSuite` | 3 | golden | 3 file output so với `expected/` |
| `MainSuite` | 4 | integration | Tham số, exit code 2/3, **chạy lại giống hệt từng byte**, không để lại file `.tmp` |

Kiểm tra lần đầu trên máy bạn: `sbt projects` (3 project) → `CI=true sbt test` (35 test) → chạy 2 lệnh ở mục 5 và so với mục 6.

## 9. Lỗi thường gặp (gặp thật khi làm project này)

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Alphanumeric method plus is not declared infix; it should not be used as infix operator` (fail với `-Werror`) | Từ Scala 3.4, gọi method tên chữ cái theo kiểu `a plus b` cần method khai báo `infix` | Viết `a.plus(b)`, hoặc khai báo `infix def plus` |
| Số điện thoại bàn `(028) 3822-1234` bị loại | Quy tắc chỉ nhận di động 10 số | Quyết định nghiệp vụ — bài mở rộng 2 |
| User bị trùng ghi `raw` chỉ có `U-1` | Bản đầu chỉ giữ `User` đã parse, mất JSON gốc | Mang theo JSON gốc từ `parseAll` — người vận hành cần dữ liệu gốc đầy đủ |
| Golden test không tìm thấy `data/` | sbt fork test → thư mục làm việc là module | Test tự dò `.` rồi `../..`, hoặc `-Dp1.root=...` |
| Doanh thu ngày lệch một ngày | Lấy ngày từ `updated_at` theo UTC | Dùng `order_date`; khi suy ngày từ thời điểm, luôn chỉ rõ timezone |
| `"Hà Nội"` nhập ở hai nơi không bằng nhau | Một nơi NFC, một nơi NFD | `Normalizer.normalize(_, NFC)` trước khi so sánh |

## 10. Bài tập mở rộng

1. **(Cơ bản)** Thêm `--date-from` / `--date-to` lọc báo cáo theo `order_date`. Parse trong `Args`, lọc trong core, test biên (ngày đầu, ngày cuối).
2. **(Cơ bản)** Chấp nhận số điện thoại cố định 11 số bắt đầu `02` (ví dụ `(028) 3822-1234`). Cập nhật `Phone`, test, và cân nhắc: có nên đổi tên kiểu thành `PhoneNumber` + enum `Kind`?
3. **(Trung bình)** Sắp xếp thành phố theo **thứ tự từ điển tiếng Việt** bằng `java.text.Collator.getInstance(Locale.forLanguageTag("vi"))`. Cập nhật golden file và giải thích diff.
4. **(Trung bình)** Thêm báo cáo `top_skus.csv`: 5 SKU doanh thu cao nhất mỗi ngày (tie theo SKU). Viết property: tổng doanh thu top-N ≤ doanh thu ngày.
5. **(Nâng cao)** Đổi `UserParser` để báo **trường thừa** (key không có trong đặc tả) như một cảnh báo không làm loại bản ghi — thiết kế kiểu dữ liệu cho "cảnh báo" khác "lỗi" và đưa vào `summary.json`.
6. **(Nâng cao)** Xử lý file lớn: thay `Files.readAllLines` bằng đọc từng dòng (`Files.lines` + `Iterator`) cho orders, giữ core thuần. Đo bộ nhớ với file 1 triệu dòng (sinh bằng `Gen`) trước và sau (Chương 11: `-Xlog:gc`, `-Xmx`). Ghi lại điều gì vẫn phải nằm trong bộ nhớ (gợi ý: dedup cần nhớ order_id).

Lời giải của bài mở rộng là mở — tiêu chí đánh giá ở mục 11.

## 11. Checklist tự đánh giá

- [ ] `CI=true sbt test` xanh trên máy tôi; tôi chạy được 2 kịch bản ở mục 5 và output khớp mục 6.
- [ ] Tôi giải thích được từng dòng trong `rejected.csv` mà không nhìn code.
- [ ] Module `core` không có `var`, không có I/O (`grep -rn "var \|Files\.\|System\." modules/core/src/main` không ra kết quả).
- [ ] Mỗi quy tắc ở mục 3.1–3.2 có ít nhất một test cho trường hợp đúng và một test cho trường hợp sai.
- [ ] Tôi chạy lại hai lần và output giống hệt; tôi biết vì sao output không chứa timestamp.
- [ ] Tôi giải thích được 3 trade-off ở mục 4 và khi nào nên chọn phương án khác.
- [ ] Bài mở rộng của tôi có test và cập nhật golden file kèm giải thích diff.

## 12. Nguồn tham khảo

- RFC 4180 (CSV): https://www.rfc-editor.org/rfc/rfc4180
- RFC 8259 (JSON): https://www.rfc-editor.org/rfc/rfc8259
- `java.time` (Instant, ZoneId): https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/package-summary.html
- `java.text.Normalizer`: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/text/Normalizer.html
- Scala 3 — infix methods: https://docs.scala-lang.org/scala3/reference/changed-features/operators.html
