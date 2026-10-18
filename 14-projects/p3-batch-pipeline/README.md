# Project 3 — Batch Data Pipeline

> **Phiên bản:** **Scala 2.13.18** (Spark 4.2 chỉ có bản `_2.13` — Chương 26 mục 3.8) · JDK 21 · Apache Spark **4.2.0** (`Provided`) · sbt 1.12.11 + sbt-assembly 2.3.1 ·
> hadoop-bare-naked-local-fs 0.1.0 (chỉ Windows) · munit 1.2.4
> **Trạng thái kiểm chứng (17/10/2026, sau review chéo):** **19/19 test pass** (`scala-cli test .` và `sbt test`); `sbt assembly` → jar 125.243 byte; **`spark-submit`
> thật** trên WSL2 Ubuntu 24.04 + Temurin 21.0.12.1 với dữ liệu sinh **305.299 dòng đơn + 904.521 click** (3 lô). Mọi output trong README là output thật.

Đọc từ file (thay cho object storage — xem mục 4.6), validate schema, biến đổi, join, tổng hợp, ghi Parquet theo ngày, kiểm tra chất lượng, **chạy lại an toàn**.
Đây là bài tổng hợp của Chương 22–26: pipeline Chương 24 + vận hành Chương 26 + hai nguồn dữ liệu + publish an toàn.

## 1. Mục tiêu

- Xây pipeline batch Spark **hai nguồn**: đơn hàng (CSV) và click (JSON Lines), theo lô `ingest_date`.
- Đầu ra cho người đọc: `fact_order_lines`, `daily_revenue` (ngày × segment), `campaign_daily` (ngày × chiến dịch: click, khách, khách chuyển đổi, doanh thu được hỗ trợ).
- Mọi dòng input vào **đúng một ngăn**, đối soát được; dữ liệu xấu không vào báo cáo, không biến mất.
- **Rerun safety:** chạy lại một ngày không sinh dòng trùng; lô mới sửa đúng partition ngày cũ; partition không còn dữ liệu bị gỡ.
- **Write–audit–publish:** người đọc không bao giờ thấy output chưa kiểm tra.
- Giải thích được **query plan** của từng join.

## 2. Tiên quyết

Chương 24 (pipeline 10 bước, ba chế độ ghi, bài 4–5: write–audit–publish, partition mồ côi), Chương 26 (cấu hình, log JSON, mã thoát, manifest, fat jar,
spark-submit, Windows/winutils), Chương 25 (join strategy, đọc plan cuối của AQE), Chương 23 (dedup có xung đột, khóa dimension duy nhất), Chương 19 (click events).

## 3. Đặc tả

**Input** (`--orders`, `--clicks`, `--customers`):

```
<orders>/ingest_date=2026-10-15/orders.csv       order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at
<clicks>/ingest_date=2026-10-15/clicks.jsonl     {"event_id":"K1","customer_id":"C1","campaign":"SALE10","ts":"2026-10-15T00:30:00Z"}
<customers>.csv                                  customer_id,segment,city
```

**Quy tắc nghiệp vụ**

| Chủ đề | Quy tắc |
|---|---|
| Đơn — hợp lệ | như Chương 24 (8 quy tắc, gom mọi lỗi, giá trị trường sau parser + lý do vào `rejected_orders`); `day` đúng cú pháp `yyyy-MM-dd`, `updated_at` ISO-8601 **có múi giờ** (`Z`/`±HH:MM`) |
| Đơn — dedup | trùng tuyệt đối bỏ; mỗi dòng hàng `(order_id, sku)` giữ bản `updated_at` mới nhất; nhiều bản mới nhất khác nội dung → `order_conflicts` |
| Click — hợp lệ | `event_id`, `customer_id`, `campaign` bắt buộc; `ts` ISO-8601 **có múi giờ**; JSON hỏng bị loại kèm lý do (`rejected_clicks`) |
| Click — dedup | cùng `event_id` cùng nội dung → giữ bản đến sớm nhất; khác nội dung → xung đột, không tính, **lưu** vào `click_conflicts` để truy vết |
| Ngày của click | ngày **UTC** của `ts` — không phải ngày của lô (click K6 đến ở lô 16 nhưng thuộc ngày 15). Job **tự đặt** `spark.sql.session.timeZone=UTC` ở đầu mỗi lần chạy, không tin session của người gọi |
| Ngữ nghĩa một lần chạy | output = **snapshot** của mọi lô ≤ `run-date`: partition đang phục vụ mà không còn trong kết quả bị gỡ (kể cả khi cả một ngày bị rút khỏi input); giảm `run-date` = quay về snapshot cũ hơn; bảng rỗng là kết quả hợp lệ |
| Chuẩn hóa | `trim` + `upper` cho mã (`c1` → `C1`, `" sale10 "` → `SALE10`) |
| Khách chuyển đổi | khách có click chiến dịch X **và** có dòng hàng PAID trong **cùng ngày** |
| Doanh thu được hỗ trợ | doanh thu PAID của khách chuyển đổi trong ngày, tính cho **mỗi** chiến dịch họ click (assisted — cộng các chiến dịch có thể vượt doanh thu thật) |
| Tiền | DECIMAL(38,0), ANSI bật |

**Vận hành**

| Tình huống | Hành vi | Mã thoát |
|---|---|---|
| Thành công | publish 3 bảng + 4 bảng lỗi/xung đột + manifest | 0 |
| Cấu hình sai (thiếu, sai, **rỗng** hoặc **lặp** tham số) | gom mọi lỗi, **không tạo SparkSession** | 2 |
| Lô của ngày chạy thiếu, hoặc tỷ lệ dòng bị loại > `--max-reject-rate` (mặc định 5%), ở **bất kỳ** nguồn nào | không ghi gì | 3 |
| `customers` trùng khóa | không ghi gì | 3 |
| Audit staging thất bại | xóa staging, bản đang phục vụ giữ nguyên | 4 |
| Publish hỏng giữa chừng, rollback được | bản đang phục vụ về đúng bản trước; nhật ký `_publish/<execution_id>.journal` | 1 |
| Publish hỏng, rollback **không** được | GIỮ `_staging/`, `_trash/` và nhật ký của lần thực thi đó; log `publish_partial` — phục hồi theo mục 9 | 1 |
| Ghi manifest thất bại (dữ liệu có thể đã publish) | log `finalization_failed` | 1 |
| Lỗi bất ngờ | | 1 |

## 4. Thiết kế và trade-off

### 4.1. Luồng

```
orders/ ─ read (RAW String) ─ validate ─┬─ rejected_orders
                                         └─ dedup ─┬─ order_conflicts
customers.csv ─ khóa duy nhất ───────── broadcast join ─ fact_order_lines ─┬─ daily_revenue
clicks/ ─ read (RAW String) ─ validate ─┬─ rejected_clicks                 │
                                         └─ dedup ─ clicks ───── join (customer_id, day) ─ campaign_daily
   gate theo lô (cả hai nguồn) ─► _staging/<execution_id>/ ─► audit (so nội dung) ─► publish có nhật ký + rollback ─► _runs/<run_id>--<execution_id>.json
```

### 4.2. Tính lại toàn bộ, publish theo partition

Mỗi lần chạy đọc **mọi lô tới `run-date`** và tính lại toàn bộ (lô 16 chứa hoàn tiền và đơn trễ của ngày 15). Rồi:

1. **Write** mọi bảng vào `_staging/<execution_id>/` (`errorifexists` — không ghi đè gì). `execution_id` do job sinh, **duy nhất** mỗi lần thực thi; `run_id` do
   người vận hành đặt (chạy lại cùng `run_id` không đụng staging/thùng rác của lần trước).
2. **Audit** đọc lại staging **với schema tường minh** (bảng rỗng không có file để suy schema) và so **nội dung**: fact staging bằng fact đã tính (multiset,
   `exceptAll` hai chiều); `daily_revenue` và `campaign_daily` bằng bảng **tính lại từ fact đọc lại** (+ click đã dedup); grain duy nhất; miền giá trị của
   `campaign_daily` (`users ≥ 1`, `0 ≤ converted_users ≤ users`, `clicks ≥ users`, tỷ lệ trong [0, 1]); bốn bảng cách ly đủ số dòng; hai phương trình đối soát.
3. **Publish** theo một **kế hoạch di chuyển**: `day=X` cũ → `_trash/<execution_id>/`, staging → chỗ cũ (mỗi bước một `ATOMIC_MOVE`); mọi partition đang phục vụ
   mà không còn trong kết quả → thùng rác (snapshot). Trước khi đụng thư mục phục vụ: **preflight** (staging đủ, thùng rác chưa tồn tại). Mỗi bước ghi
   (kèm `DSYNC`) vào `_publish/<execution_id>.journal` **hai** dòng: `INTENT` **trước** khi move (chết giữa chừng vẫn biết bước nào có thể đã xảy ra) và `DONE`
   sau khi move. Danh sách bước đã làm trong bộ nhớ được cập nhật **ngay** sau move, trước khi ghi `DONE` — nên ghi nhật ký hỏng cũng **không** làm mất rollback
   (review vòng 2: bản trước để lỗi ghi `DONE` thoát ra ngoài, job tưởng chưa làm gì và xóa staging cần để phục hồi). Lỗi ở bước k → **đảo ngược** các bước đã
   move theo thứ tự ngược; trong rollback, nhật ký chỉ là best-effort. Thành công → xóa thùng rác của lần đó (nhật ký giữ lại làm bằng chứng).

**Vì sao phải có nhật ký + rollback (review chéo tìm ra).** Bản đầu publish tuần tự rồi `finally` xóa staging. Reviewer tạo xung đột ở đích của bảng thứ hai:
fact đã là bản ngày 16, daily vẫn bản ngày 15 (`dailyTotal=21600000` thay vì 21.900.000), staging bị xóa nên không còn gì để phục hồi, và không có dấu vết bước
nào đã chạy. Giờ test chèn lỗi ở bước thứ 3 cho nhật ký thật `DONE, DONE, FAILED, UNDONE, UNDONE, ROLLED_BACK` và hai bảng về đúng bản trước. Vẫn **không phải
transaction**: trong lúc publish/rollback người đọc có thể thấy trạng thái trộn.

| Lựa chọn | Ưu | Nhược |
|---|---|---|
| **Staging + publish theo partition** (P3) | không ai thấy dữ liệu chưa audit; gỡ partition mồ côi; lỗi giữa chừng tự rollback | cả lần publish không nguyên tử: trong lúc publish (và nếu rollback hỏng, tới khi phục hồi bằng tay) có thể thấy ngày 15 mới + 16 cũ |
| Dynamic overwrite (Ch24) | đơn giản, một lệnh | ghi thẳng vào chỗ người đọc; để lại partition mồ côi |
| Table format (Delta/Iceberg) | commit nguyên tử, time travel, MERGE | thêm phụ thuộc, vận hành catalog (Chương 31) |

**Tính lại toàn bộ** đúng và đơn giản nhưng chi phí tăng theo lịch sử. Khi lịch sử dài: tính lại **cửa sổ N ngày** gần nhất (đơn đến muộn/hoàn tiền quá N ngày
xử lý riêng) — khi đó phạm vi gỡ partition phải là **cửa sổ đó** (khai báo tường minh), không phải "các ngày còn dữ liệu hợp lệ": bản đầu của P3 dùng định nghĩa
sau và không gỡ được một ngày bị rút **trọn** khỏi input (review chéo; test `review P3 M3`).

### 4.3. Hai join và plan của chúng

| Join | Kích thước | Plan cuối thật | Vì sao |
|---|---|---|---|
| fact × customers | lớn × nhỏ | `BroadcastHashJoin` | hint `broadcast()` — không shuffle bảng đơn |
| click (theo khách-ngày-chiến dịch) × doanh thu theo khách-ngày | lớn × lớn | dữ liệu mẫu nhỏ: `BroadcastHashJoin`; tắt broadcast tự động (giả lập lớn): `SortMergeJoin` | AQE chọn theo kích thước **thật** — đây là plan **quan sát được** với cấu hình mặc định, không phải luật (AQE có thể chọn `ShuffledHashJoin`) |

Output thật của test plan (tắt `autoBroadcastJoinThreshold` để giả lập bảng lớn):

```
[plan] join click × doanh thu khi tắt broadcast: +- *(15) SortMergeJoin [customer_id#735, day#738], [customer_id#630, day#631], LeftOuter
                                               | +- *(12) BroadcastHashJoin [customer_id#630], [customer_id#671], LeftOuter, BuildRight, false, false
```

Dòng thứ hai: join với `customers` **vẫn** broadcast dù đã tắt ngưỡng tự động — **hint thắng ngưỡng**. Vì cả plan luôn chứa một `BroadcastHashJoin` (của join khách),
test tìm **đúng** join click × doanh thu theo khóa `[customer_id, day]` rồi mới kiểm tra loại join. Doanh thu được tổng hợp theo `(customer_id, day)` **trước** join:
mỗi khách-ngày một dòng, join không nhân số click.

### 4.4. Gate theo lô cho cả hai nguồn

Gate xét lô `ingest_date = run-date` của **từng** nguồn: một nguồn tốt không che nguồn kia. Ngưỡng là cấu hình (NaN/ngoài [0,1] bị từ chối).

### 4.5. Bộ sinh dữ liệu

`Generator` (seed cố định → cùng từng byte trên thư mục **mới/rỗng** — ghi vào thư mục đã có lô cũ bị từ chối, vì lô cũ ngoài `Summary` sẽ lẫn vào input): mỗi lô có ~1% dòng hỏng, ~0,5% gửi lại y hệt, ~2% đơn đến muộn của ngày trước, hoàn tiền ~2% đơn PAID ngày trước; khách
`C00001` chiếm ~20% đơn (khách nóng — xem skew ở Chương 25).

### 4.6. Vì sao file thay vì object storage (MinIO/S3)

Đọc S3 từ Spark cần `hadoop-aws` khớp **đúng** phiên bản Hadoop trong bản phân phối Spark và bộ AWS SDK v2 (hàng trăm MB), cộng một MinIO server — chưa kiểm chứng
trên máy này nên không đưa vào đường chính. Code chỉ dùng đường dẫn: đổi `--orders=s3a://bucket/orders` là đủ **nếu** classpath có `hadoop-aws`. Lưu ý: `Publish` dùng
`Files.move` (hệ thống file cục bộ) — trên S3 **không có** rename nguyên tử, phải đổi chiến lược publish sang table format (mục 4.2). Bài tập mở rộng 4.

## 5. Cách chạy

```bash
cd 14-projects/p3-batch-pipeline
scala-cli test .                                         # hoặc: sbt test   (18 test)

# Dữ liệu mẫu (tính tay được) — chạy trực tiếp bằng Scala CLI:
scala-cli run . --main-class vn.scalaeng.p3.Job -- --orders=data/sample/orders --clicks=data/sample/clicks \
  --customers=data/sample/customers.csv --output=/tmp/p3 --run-date=2026-10-16 --max-reject-rate=0.3

# Dữ liệu lớn:
scala-cli run . --main-class vn.scalaeng.p3.Generator -- /tmp/p3-data 3 100000 300000 2026

# Triển khai: fat jar + spark-submit (Linux/macOS/WSL2 — trên Windows spark-submit cần winutils, Chương 26 mục 3.7)
sbt assembly                                             # target/scala-2.13/p3-batch-pipeline.jar
SPARK_HOME=... IN=/tmp/p3-data OUT=/tmp/p3-out deploy/submit.sh --run-date=2026-10-17 --max-reject-rate=0.02
```

## 6. Output thực tế

**Dữ liệu lớn, `spark-submit` trên WSL2** (`local[4]`, driver 2 GB, 12 CPU logic, input trên ext4 của WSL; mỗi lệnh một lần, không phải benchmark; chạy lại
17/10/2026 với jar sau review vòng 2, trên cùng dữ liệu đã sinh `Summary(3,305299,904521,2000)` — 3 lô, 305.299 dòng đơn, 904.521 click, 2.000 khách):

```
{"run_id":"big-2026-10-15","event":"quality_gate_passed","source":"orders","rows":100494,"rejected":975}
{"run_id":"big-2026-10-15","event":"quality_gate_passed","source":"clicks","rows":301566,"rejected":1471}
{"run_id":"big-2026-10-15","event":"published","table":"fact_order_lines","replaced":[],"added":["day=2026-10-15"],"removed":[]}
{"run_id":"big-2026-10-15","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":57076,"execution_id":"2026-10-17T083346322027924Z-8ef22932"}
{"run_id":"big-2026-10-16","event":"published","table":"fact_order_lines","replaced":["day=2026-10-15"],"added":["day=2026-10-16"],"removed":[]}
{"run_id":"big-2026-10-16","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":53771,"execution_id":"2026-10-17T083450445052554Z-b96c412f"}
{"run_id":"big-2026-10-17","event":"published","table":"fact_order_lines","replaced":["day=2026-10-15","day=2026-10-16"],"added":["day=2026-10-17"],"removed":[]}
{"run_id":"big-2026-10-17","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":49410,"execution_id":"2026-10-17T083552215057379Z-e29da4d0"}
{"run_id":"big-2026-10-17","event":"published","table":"fact_order_lines","replaced":["day=2026-10-15","day=2026-10-16","day=2026-10-17"],"added":[],"removed":[]}
{"run_id":"big-2026-10-17","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":45166,"execution_id":"2026-10-17T083646860777140Z-93e238f3"}
== số file mỗi partition (fact_order_lines):
day=2026-10-15: 1 file, 1.7M
day=2026-10-16: 1 file, 1.7M
day=2026-10-17: 1 file, 1.6M
== _staging: 0 mục, _trash: 0 mục, _publish: 4 nhật ký
== nhật ký publish lần cuối (đếm theo loại):
      1 COMMITTED
     26 DONE
     26 INTENT
```

(Rút gọn `ts`/`level`; chỉ giữ `quality_gate_passed` của lần đầu và `published` của `fact_order_lines` — mỗi lần chạy có gate cho cả hai nguồn và
`published` cho cả ba bảng.) Lần chạy thứ tư dùng lại `run_id` `big-2026-10-17`: `execution_id` mới, không đụng gì của lần thứ ba.
Thời gian mỗi lần tăng so với bản trước review (28–37 s → 45–57 s ở lượt này; một lượt đo, máy dùng chung): audit giờ so **nội dung** (`exceptAll` và tính lại hai bảng tổng hợp) — một shuffle trên mỗi
bảng. Đó là giá của "audit xanh nghĩa là dữ liệu đúng"; bảng rất lớn thì so tổng kiểm theo partition (Chương 24 mục 3.8). 26 bước publish = 3 bảng × 3 ngày
× 2 bước (cũ → thùng rác, mới → chỗ cũ) + 4 bảng không partition × 2 bước; mỗi bước có một dòng `INTENT` trước và một dòng `DONE` sau. Manifest lần chạy cuối — **đối soát khớp tuyệt đối**:

```
"counts":{"clicksRejected":4545,"clicksConflict":0,"ordersInput":305299,"ordersExactDup":1497,"clicksKept":895483,"factLines":297050,"ordersRejected":2964,
          "ordersSuperseded":3788,"clicksInput":904521,"paidRevenueVnd":1189620280000,"ordersConflict":0,"clicksDup":4493}
```

305.299 = 2.964 + 1.497 + 3.788 + 0 + 297.050; 904.521 = 4.545 + 4.493 + 0 + 895.483.

**Dữ liệu mẫu** — `campaign_daily` (test golden):

| Ngày | Chiến dịch | Click | Khách | Chuyển đổi | Doanh thu hỗ trợ | Tỷ lệ |
|---|---|---|---|---|---|---|
| 15/10 | NEWBIE | 2 | 2 | 1 | 700.000 | 0,5000 |
| 15/10 | SALE10 | 3 | 2 | 2 | 20.700.000 | 1,0000 |
| 16/10 | SALE10 | 2 | 2 | 1 | 750.000 | 0,5000 |

Tổng doanh thu hỗ trợ 22.150.000 > doanh thu thật 21.900.000: khách C1 (700.000) click cả NEWBIE và SALE10 ngày 15.

## 7. Kiểm tra tay (làm TRƯỚC khi chạy)

Đơn: như Chương 24 mục 6 — sau hai lô, `daily_revenue` = Retail 15/10 **700.000**, UNKNOWN **450.000**, Wholesale 15/10 **20.000.000**, Wholesale 16/10 **750.000**.

Click (11 dòng): loại 3 (K5 thiếu campaign; dòng JSON cắt cụt; K9 `ts = not-a-time`), K3 gửi lại 1 → còn 7. Ngày 15: SALE10 = K1, K2 (`c1`/`" sale10 "` sau chuẩn hóa),
K3 → 3 click, khách {C1, C2}, cả hai có đơn PAID ngày 15 → 2 chuyển đổi, 700.000 + 20.000.000. NEWBIE = K4 (C3) + **K6** (C1, đến ở lô 16 nhưng `ts` ngày 15) →
1 chuyển đổi (C1; C3 không có đơn PAID hợp lệ). Ngày 16: SALE10 = K7 (C2, có O6 750.000), K8 (C9, đơn O4 là ngày 15) → 1 chuyển đổi.

## 8. Kiểm thử

`PipelineSuite` — 18 test, munit, Spark local (timeout mỗi test 300 s — test tích hợp chạy job nhiều lần):

| Test | Kiểm tra |
|---|---|
| config | gom mọi lỗi, tham số bắt buộc; rỗng/lặp là lỗi |
| click: chuẩn hóa + lý do | `c1 → C1`, `" sale10 " → SALE10`; ba dòng bị loại đúng lý do |
| click: dedup + ngày theo `ts` | K3 trùng, K6 lô 16 → ngày 15, xung đột event_id |
| campaign_daily golden | bảng mục 6; assisted > doanh thu thật |
| plan | join click × doanh thu (khóa `[customer_id, day]`): Broadcast; SortMerge khi tắt broadcast tự động — đọc plan **cuối** của AQE |
| e2e | ngày 15 → 16 (golden, log `published`, manifest), chạy lại y hệt, staging và thùng rác được dọn, đủ 7 bảng |
| audit | staging bị sửa → mã 4, bảng phục vụ giữ nguyên |
| partition mồ côi | O6 thành xung đột → `day=2026-10-16` bị gỡ khỏi fact/daily; campaign ngày 16 vẫn còn |
| gate | 1/4 click bị loại (25%) > 20% → mã 3, không có output; lô thiếu; customers trùng |
| generator | cùng seed cùng byte |
| dữ liệu ngẫu nhiên | 3 ngày sinh với seed 42 qua audit, chạy lại y hệt — regression trên **một** fixture, không phải chứng minh cho mọi input |
| review M1 | 4 kiểu sửa staging giữ nguyên tổng (giá, segment, campaign, xóa bản ghi lỗi) → mã 4, không publish |
| review M2 | lỗi ở bước publish thứ 3 → rollback, bảng về bản trước, nhật ký đúng thứ tự; chạy lại cùng `run_id` thành công |
| review M3 | rút trọn lô ngày 15 → partition ngày 15 bị gỡ ở cả ba bảng |
| review M4 | chỉ REFUNDED / mọi đơn bị loại / mọi click xung đột → mã 0 |
| review M5 | `2026`, timestamp thiếu múi giờ bị loại; session đặt `Asia/Ho_Chi_Minh` vẫn ra ngày UTC |
| review M6 | cấu hình sai không tạo session; cùng `run_id` hai lần → hai manifest |
| generator | từ chối thư mục ra không rỗng |

## 9. Lỗi thường gặp (gặp thật khi làm project này)

1. **`/*` trong Scaladoc** — viết mẫu đường dẫn `orders/ingest_date=YYYY-MM-DD/*.csv` trong `/** ... */`: `error: unclosed comment` (Scala cho comment lồng nhau; đúng lỗi
   ghi ở Chương 26 mục 10 — gặp lại ở chính project này).
2. **Chuỗi `"\n"` bị biến thành xuống dòng thật** khi sinh code bằng script → `error: unclosed string literal`. Kiểm tra file sinh ra trước khi biên dịch.
3. **Click lô 16 gán nhầm ngày 16** nếu lấy ngày theo `ingest_date` — K6 phải thuộc ngày 15 (test riêng).
4. **Join click với từng dòng đơn** (không tổng hợp theo khách-ngày trước) nhân số click theo số dòng hàng.
5. **Cộng `assisted_revenue_vnd` của mọi chiến dịch rồi gọi là doanh thu** — vượt doanh thu thật (mục 6).
6. **Audit "xanh" nhưng dữ liệu sai** (review chéo): bản đầu chỉ so tổng — đổi mọi giá về 1 và doanh thu dòng về 0 mà giữ khóa thì fact vẫn qua; đổi segment
   thành WRONG mà giữ tổng thì daily vẫn qua; thay bảng `rejected_orders` bằng bảng rỗng thì vẫn publish (`code=0 factRevenue=[0] dailyRevenue=[21900000]`).
   Kiểm tra tổng có **điểm mù chung**; phải so nội dung (Chương 33).
7. **`[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Parquet. It must be specified manually. SQLSTATE: 42KD9`** — audit đọc một bảng staging rỗng (chỉ có đơn
   REFUNDED → `daily_revenue` không có dòng nào) mà không kèm schema. Bảng rỗng là hợp lệ; đọc lại bằng `spark.read.schema(...)`.
8. **Windows: `Unable to establish loopback connection`** khi suite chạy nhiều truy vấn — `Job.session()` đặt `spark.sql.artifact.isolation.enabled=false`
   (Chương 24 mục 10).

**Runbook — publish hỏng mà rollback cũng hỏng** (log `publish_partial`, mã 1):

1. **Dừng** mọi lần chạy khác trên cùng `--output` (hai job cùng publish sẽ làm rối thêm).
2. Đọc `_publish/<execution_id>.journal`: `INTENT` là bước sắp làm, `DONE` là bước đã làm, `UNDONE` là bước đã đảo ngược được; dòng `ROLLBACK_FAILED` cho biết
   lỗi. Nhật ký có thể **thiếu** dòng (ghi hỏng trong lúc rollback là best-effort): `INTENT` không có `DONE` nghĩa là move **có thể** đã xảy ra — kiểm tra trên đĩa
   xem dữ liệu đang ở `<nguồn>` hay `<đích>`, đừng suy ra từ nhật ký.
3. Với mỗi bước mà dữ liệu đang nằm ở `<đích>` và chưa có `UNDONE`, **theo thứ tự ngược**: chuyển `<đích>` về `<nguồn>` (cột 3 → cột 2). Dữ liệu cần thiết còn nguyên trong
   `_staging/<execution_id>/` và `_trash/<execution_id>/` — job không xóa chúng trong trường hợp này.
4. Khi thư mục phục vụ đã về bản trước: xóa hai thư mục trên, sửa nguyên nhân (quyền, dung lượng), chạy lại với cùng `run_id` (job sẽ sinh `execution_id` mới).

## 10. Bài tập mở rộng

1. Giữ thùng rác của N lần thực thi thành công gần nhất (để quay lại bản trước nhanh) thay vì xóa ngay; viết lệnh **phục hồi tự động** đọc nhật ký `_publish/`
   theo runbook mục 9.
2. Thay "tính lại toàn bộ" bằng **cửa sổ tính lại 7 ngày**: dữ liệu cũ hơn đến muộn đi vào bảng `late_arrivals` để xử lý riêng; phạm vi gỡ partition = đúng cửa
   sổ đó (tham số tường minh), có test rút trọn một ngày trong và ngoài cửa sổ.
3. Mô hình ghi nhận **last-click**: mỗi đơn PAID gán cho click gần nhất **trước** nó trong ngày (cần **thời điểm đặt hàng**, không dùng `updated_at`). Đơn không có
   click phù hợp (ví dụ C9: đơn ngày 15, click ngày 16) vào nhóm `UNATTRIBUTED`; chốt cách xử lý hai click cùng thời điểm. Chứng minh: doanh thu ghi nhận +
   `UNATTRIBUTED` = doanh thu thật.
4. Đọc/ghi **S3/MinIO**: kiểm chứng tổ hợp `hadoop-aws` + Hadoop của Spark 4.2.0; thay `Publish` bằng chiến lược hợp với object storage (Delta Lake hoặc con trỏ phiên bản).
5. Gộp metric `SparkListener` của Chương 26 vào manifest; đặt cảnh báo khối lượng (Chương 26 bài 4) cho cả hai nguồn.

## 11. Checklist tự đánh giá

- [ ] Giải thích được vì sao mỗi dòng input vào đúng một ngăn, và đối soát trên dữ liệu 1,2 triệu dòng khớp.
- [ ] Chạy lại một ngày không tạo dòng trùng; lô mới sửa đúng partition cũ; partition mồ côi bị gỡ.
- [ ] Người đọc không thấy dữ liệu chưa audit; biết giới hạn của publish theo partition.
- [ ] Đọc plan cuối và giải thích hai join.
- [ ] Phân biệt doanh thu thật và doanh thu được hỗ trợ.
- [ ] Build fat jar, chạy spark-submit; biết vì sao Windows cần WSL/winutils.

## 12. Nguồn tham khảo

- Spark 4.2.0: https://spark.apache.org/docs/4.2.0/ — JSON/CSV data sources, `partitionBy`, AQE, join hints, `spark-submit`
- Hadoop S3A (cho bài mở rộng 4): https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
- Delta Lake / Apache Iceberg (publish nguyên tử trên object storage): https://docs.delta.io/ , https://iceberg.apache.org/docs/latest/
- Chương 24, 25, 26 của giáo trình.
