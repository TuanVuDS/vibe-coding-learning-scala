# Project 5 — Production-Oriented Data Platform

> **Phiên bản:** Scala **3.9.0** · JDK 21 · sbt 1.12.11 (+ sbt-assembly 2.3.1) · circe 0.14.17 · kafka-clients 4.4.0 · Cats Effect 3.7.1 · Doobie 1.0.0-RC13 ·
> PostgreSQL JDBC 42.7.14 · http4s 0.23.38 · munit 1.2.4 / munit-cats-effect 2.2.0 · test: embedded-kafka 4.4.0 (KRaft), embedded-postgres 2.2.2 (PostgreSQL 18.6) ·
> **Dagster 1.13.26** (Python 3.12.10 — orchestration, công cụ riêng)
> **Trạng thái kiểm chứng bản sửa Vòng 4 (Codex, 17/10/2026):** Scala CLI **20 pass, 4 skipped**, 0 failed, 0 compiler warning với Kafka KRaft thật;
> 4 test PostgreSQL không bật do fixture không khởi động được trong sandbox ở review Vòng 2. Mặc định không bật dịch vụ: **18 pass, 6 skipped**.
> Lead từng chạy 17/17 test, sbt/assembly và toàn luồng Dagster trước các vòng sửa Codex (output lịch sử ở mục 6); bản hiện tại cần lead review lại và kiểm chứng DB/Dagster/sbt.
> Docker/MinIO/Spark cluster **chưa** kiểm chứng (mục 4.5).

Bài tổng hợp của toàn giáo trình: nguồn dữ liệu → ingestion (Kafka hoặc file) → xử lý → kiểm tra chất lượng → lưu trữ → API phục vụ → giám sát, có orchestrator
điều phối. Trọng tâm **không** phải viết mọi thứ bằng Scala, mà là đặt **mỗi việc vào đúng công cụ** và giữ hợp đồng rõ giữa chúng.

## 1. Mục tiêu

- Ghép các mảnh đã học thành một nền tảng chạy được: contract ở cửa ngõ (Ch33), lớp raw/staging/serving + lineage (Ch31), dedup/phiên bản/xung đột (Ch24),
  at-least-once + idempotency (Ch30), write–audit–publish (Ch24, P3), API + PostgreSQL (Ch14–15, P2), orchestration (Ch31).
- Chứng minh bằng test: **backfill 3 ngày cho kết quả giống hệt chạy hằng ngày**; chết giữa chừng không làm sai số; dữ liệu xấu không lọt vào bảng phục vụ.
- Viết **runbook** cho 3 sự cố: consumer lag tăng, job batch hỏng giữa chừng, schema thay đổi.

## 2. Tiên quyết

Chương 31 (bắt buộc), 33, 30, 24, 15, 14; Project 2 (stack backend), Project 4 (Kafka). JDK 21, sbt hoặc Scala CLI. Test PostgreSQL/Kafka dùng bản **nhúng**,
không cần Docker; cần quyền chạy tiến trình con và mở cổng loopback. Dagster cần Python 3.10–3.14 (`pip install dagster==1.13.26`).

## 3. Đặc tả

```
 producer ─► Kafka topic orders.events ─► KafkaLanding ─┐
 bên gửi  ─► file .jsonl thả vào       ─► FileLanding ──┤ contract ─ vi phạm ─► quarantine/
                                                        ▼
                                              raw/orders/ingest_date=D/*.jsonl   (append-only)
                                                        │  staging_orders@day   (validate, dedup event_id, phiên bản, xung đột) → part.jsonl
                                                        │  daily_revenue@day    (× dimension/customers.csv)
                                                        ▼
                                              quality gate ─ chặn ─► (không nạp, mã 4)
                                                        ▼
                                     PostgreSQL: daily_revenue, serving_loads, VIEW monthly_revenue
                                                        ▼
                                     API http4s: /api/v1/revenue/daily, /monthly, /health/ready, /metrics
             Dagster: raw_orders → staging_orders → daily_revenue → serving_daily_revenue (mỗi ngày một partition)
                      raw_orders → raw_reconcile (không phân vùng: đối soát cấp raw)
```

**Sự kiện** (JSON Lines, một hợp đồng cho mọi đường vào): `event_id, order_id, customer_id, day, status, sku, quantity, unit_price_vnd, updated_at`.

| Quy tắc | Ở đâu |
|---|---|
| Đúng bộ 9 trường, đúng kiểu JSON (contract) | cửa ngõ — sai thì **cả file** (thả vào) / **record** (Kafka) vào `quarantine/` kèm lý do |
| `day` = `yyyy-MM-dd` (MỘT quy tắc `Order.dayOf` cho chọn partition, validate, đối soát và tham số API; trim khoảng trắng, từ chối năm mở rộng/ngày không tồn tại), `updated_at` ISO-8601 **có múi giờ**, `quantity` 1..1000, giá > 0, status ∈ PAID/REFUNDED/CANCELLED | staging — sai thì vào `rejected.jsonl` của partition; API sai ngày trả 400 |
| Cùng `event_id` **và cùng nội dung** (đã chuẩn hóa) = sự kiện gửi lại → giữ một; cùng `event_id` mà **khác** nội dung trên **toàn raw**, kể cả khác ngày/lô → cách ly cả nhóm tại mọi ngày liên quan | staging (`duplicate_events`, `event_conflicts`); collision xét trước validate nghiệp vụ; dòng ngày sai vẫn ở `unassigned` và tham gia chỉ mục ID |
| Mỗi `(order_id, sku)` giữ bản `updated_at` mới nhất; nhiều bản mới nhất khác nội dung → xung đột, không tính | staging |
| Partition theo **ngày nghiệp vụ** `day`, không theo ngày nhận | staging, daily |
| Đối soát mỗi ngày: `rows = rejected + event_conflicts + duplicate_events + superseded + conflicts + kept`; tổng PAID của staging = tổng daily (độc lập với phép join segment) | quality gate |
| Mỗi dòng raw thuộc **đúng một** ngày hoặc `unassigned` kèm lý do; `raw_rows = assigned + unassigned` | đối soát cấp raw — lệnh `reconcile`, asset Dagster `raw_reconcile`, file `_quality/raw_reconcile.json` |
| Staging lưu **JSON Lines** (codec circe có escape), không ghép chuỗi CSV | staging — `order_id` có dấu phẩy/ngoặc kép/xuống dòng đi qua nguyên vẹn |

**Mã thoát của job** (orchestrator dựa vào): 0 thành công · 1 lỗi bất ngờ · 2 sai tham số · 3 file bị cách ly · 4 quality gate chặn.

## 4. Thiết kế và trade-off

### 4.1. Trách nhiệm từng thành phần — khi nào Scala, khi nào SQL, khi nào công cụ khác

| Việc | Công cụ trong P5 | Vì sao công cụ này |
|---|---|---|
| Lập lịch, thứ tự, partition, lịch sử chạy, backfill qua giao diện | **Dagster** (`orchestration/defs.py`) | orchestration không phải processing; đổi lịch không sửa code tính toán |
| Kiểm contract, parse, validate, dedup theo phiên bản, xung đột | **Scala** (`core`) | quy tắc phức tạp, cần kiểu mạnh, test được từng nhánh, tái sử dụng |
| Ghi bền + commit offset theo thứ tự đúng | **Scala** (`ingest`) + Kafka | thứ tự "ghi rồi mới commit" là trách nhiệm của consumer tự viết trong P5 |
| Tổng theo tháng trên bảng đã sạch | **SQL** — `VIEW monthly_revenue` trong PostgreSQL | ngắn, ai cũng đọc được, optimizer làm tốt; viết bằng Scala chỉ thêm code |
| Lưu bảng phục vụ, truy vấn theo khoảng, transaction khi nạp | **PostgreSQL** | việc của database: index, khóa, transaction, quyền |
| Lưu raw/staging | filesystem (lake) | rẻ, append-only; production: object storage (mục 4.5) |
| API cho người đọc | **Scala** (http4s) | hợp đồng HTTP, validate tham số, readiness — như P2 |
| Batch cực lớn (hàng tỷ dòng) | **Spark** (dùng Scala 2.13 — Project 3) | P5 dùng Scala thuần vì dữ liệu nhỏ; cùng mô hình asset áp nguyên cho job Spark |

Không yêu cầu Scala làm mọi việc: `daily_revenue` ở đây là một phép join + group đơn giản — trong hệ thống có warehouse, nó sẽ là **SQL/dbt**. P5 giữ nó trong
Scala để lineage, quality gate và test nằm cùng một chỗ cho người học; đó là lựa chọn sư phạm, không phải khuyến nghị.

### 4.2. Ingestion: hai đường, một hợp đồng

- **File thả vào:** tên file trong raw = hash nội dung → thả lại cùng file là no-op. Sai contract → cả file vào quarantine kèm `<file>.reason.txt`, lệnh
  trả mã 3 (bên gửi sửa và gửi lại cả file).
- **Kafka — quarantine theo record:** mỗi record vi phạm thành một dòng **phong bì JSON** `{"topic","partition","offset","reason","value"}` (giá trị gốc
  nguyên văn). Đường Kafka không có `.reason.txt` và không trả mã 3 — lý do nằm trong phong bì (runbook 3b).
- **Kafka:** mỗi lần poll ghi các record hợp lệ thành **một** file (fsync + rename) rồi **mới** commit offset. Chết giữa hai bước → đọc lại các offset đó →
  raw có **bản sao** (at-least-once). Không sửa ở landing; staging bỏ sự kiện trùng theo `event_id` — test `chết SAU khi ghi file, TRƯỚC commit` cho doanh thu
  giống hệt khi không có sự cố. Lý do không cố "exactly-once" ở landing: tên file theo offset chỉ tất định khi ranh giới lô giống nhau, và lô phụ thuộc
  `max.poll.records` + thời điểm — test cố ý đổi `max.poll.records` để thấy điều đó.

### 4.3. Xử lý và lineage

Giống Chương 31: asset tất định, tính trên **snapshot** input (đọc mỗi file một lần — hash và phép tính dùng cùng byte), ghi nguyên tử, lineage có hash và
ID lần tính upstream; chỉ tính partition thiếu/cũ. `staging_orders` v3 dựng chỉ mục ID trên toàn snapshot raw trước khi chọn ngày. Mỗi ngày lưu thêm dependency
`event-index / event_ids@day`: các nội dung chuẩn hóa, khử trùng, sắp xếp của mọi ID chạm ngày đó. Lô mới khác ngày có cùng ID nhưng khác nội dung làm đổi
fingerprint, khiến **ngày cũ cũng stale** và gate chặn publish đến khi rebuild. Lô không liên quan giữ ngày cũ fresh. Mỗi ngày chỉ đếm các dòng của chính ngày
đó trong `event_conflicts`; dòng ngày sai vẫn được đếm ở `unassigned`, tránh đếm lặp giữa các ngày. Sự kiện hợp lệ được chuẩn hóa trước so sánh; sự kiện sai
nghiệp vụ giữ nội dung đã decode để nhận ra collision với bản hợp lệ. Cách này quét toàn raw và giữ chỉ mục trong RAM, phù hợp dữ liệu nhỏ; production cần
chỉ mục ID bền và snapshot theo phiên bản để giảm chi phí quét.

Với input asset, orchestrator tra **toàn lịch sử** theo asset/ngày và hash của byte snapshot, không chỉ so bản latest. Nếu có nhiều lần tính cùng byte,
chọn lần khớp hash gần nhất trong lịch sử đã đọc: đây là định danh phiên bản nội dung, không cam kết tìm đúng writer vật lý khi nhiều writer tạo byte giống
nhau. Thiếu file hoặc không tìm được bản lịch sử khớp → fail rõ trước khi ghi downstream; không âm thầm ghi `materializationId=None`. P5 chưa khóa toàn
root hay bảo đảm giao dịch nhiều writer; producer phải publish raw/dimension hoàn chỉnh, và status/gate phát hiện input đã đổi để chạy lại.
`daily_revenue` tăng lên v2 để các partition cũ phải dựng lại refs, kể cả khi byte doanh thu không đổi sau sửa provenance.

Lô 16 chứa hoàn tiền + đơn trễ của **ngày 15** → ngày 15 cũ và
được nạp lại; lô 17 chạm ngày 16 → ngày 16 nạp lại. Dagster **không** tự biết điều này (nó partition theo ngày chạy) — job Scala biết nhờ lineage và trả lời qua
lệnh `p5.jar stale --from --to`; `run-platform.sh` dùng nó để chạy lại đúng các ngày cũ (mục 6). Bài mở rộng 1 biến bước này thành Dagster sensor.
`defs.py` hiện chỉ khai báo asset, `deps` và partition (theo giờ Việt Nam — `timezone="Asia/Ho_Chi_Minh"`); **chưa** cấu hình schedule, sensor hay retry
policy — mô tả năng lực của Dagster ở đây không có nghĩa P5 đã triển khai chúng.

### 4.4. Nạp serving: một ngày, một transaction

`ServingDb.load`: `pg_advisory_xact_lock(day)` (hai lần nạp cùng ngày chạy tuần tự) → so `output_hash` với lần nạp trước (giống → `unchanged`) → `DELETE` ngày
đó → `INSERT` các dòng mới → ghi `serving_loads`. Người đọc thấy ngày đó ở bản cũ hoặc mới, không nửa vời. Ngày rỗng (mọi đơn bị hoàn tiền) vẫn có dòng trong
`serving_loads` — "đã nạp, không có doanh thu" khác "chưa nạp". Ràng buộc `CHECK` trong bảng là lớp kiểm tra cuối (doanh thu không âm, orders ≤ order_lines).

### 4.5. Những gì P5 cố ý không làm (và lý do)

| Thiết kế gốc (`00-roadmap/06-projects-overview.md`) | P5 | Lý do |
|---|---|---|
| Parquet trên MinIO | file JSONL/CSV trên filesystem | MinIO cần Docker (daemon chưa chạy trên máy kiểm chứng) và `hadoop-aws` khớp phiên bản — chưa kiểm chứng; mô hình lớp không đổi |
| Spark 2.13 cho batch hằng ngày | Scala thuần | dữ liệu nhỏ; Spark đã có ở P3 — thay `compute` bằng job Spark không đổi orchestration/lineage |
| Docker Compose | PostgreSQL/Kafka **nhúng** cho test và demo | chạy được không cần Docker; production dùng dịch vụ thật |
| Backend P2 làm nguồn sự kiện | fixture JSONL + producer trong test | P2 không phát sự kiện ra Kafka; bài mở rộng 3 |

## 5. Cách chạy

```bash
cd 14-projects/p5-data-platform
sbt test                                             # unit (core); test DB/Kafka tự bỏ qua
RUN_DB_TESTS=1 RUN_KAFKA_IT=1 sbt test               # + PostgreSQL 18.6 nhúng + Kafka KRaft nhúng
scala-cli test project.scala core ingest serving app --server=false # tương đương bằng Scala CLI (liệt kê module, không kéo orchestration/)
sbt app/assembly                                     # app/target/scala-3.9.0/p5.jar

# Từng bước bằng tay (Dagster gọi đúng các lệnh này):
java -jar p5.jar land-file   --root=<lake> --file=data/drop/orders-2026-10-15.jsonl --ingest-date=2026-10-15
java -jar p5.jar materialize --root=<lake> --asset=staging_orders --day=2026-10-15
java -jar p5.jar materialize --root=<lake> --asset=daily_revenue  --day=2026-10-15
java -jar p5.jar publish     --root=<lake> --day=2026-10-15 --jdbc-url=jdbc:postgresql://localhost:5432/postgres?user=postgres
java -jar p5.jar stale       --root=<lake> --from=2026-10-15 --to=2026-10-17   # ngày cũ theo lineage
java -jar p5.jar reconcile   --root=<lake>                                     # đối soát cấp raw → _quality/raw_reconcile.json
java -jar p5.jar serve       --jdbc-url=... --port=8085

# Toàn luồng qua Dagster (PostgreSQL nhúng làm server cho demo):
SCALA_CLI=scala-cli.bat DAGSTER=<venv>/Scripts/dagster WORK=<thư mục đã có> orchestration/run-platform.sh
#   tạo thư mục con mới trong WORK (không xóa gì), giữ log; mã thoát 1 nếu BẤT KỲ bước Dagster/API nào hỏng
```

`<lake>` cần `dimension/customers.csv` (copy từ `data/dimension/`).

## 6. Output thực tế

Output lịch sử của lead, **trước các vòng sửa Codex**, từ `orchestration/run-platform.sh` (Windows 11 + Git Bash; PostgreSQL 18.6 nhúng làm server;
Dagster 1.13.26 gọi `p5.jar` cho từng asset). Chưa tái kiểm toàn luồng trên bản stop-file Vòng 3 và bản dữ liệu/provenance Vòng 4:

```
PostgreSQL: READY jdbc:postgresql://localhost:55432/postgres?user=postgres
partition 2026-10-15: exit=0 4 asset được tính
partition 2026-10-16: exit=0 4 asset được tính
partition 2026-10-17: exit=0 4 asset được tính
== Sau 3 lần chạy 'hằng ngày' (mỗi ngày chỉ partition của ngày đó):
== GET /api/v1/revenue/daily?from=2026-10-15&to=2026-10-17
[{"day":"2026-10-15","segment":"Retail","revenue_vnd":1150000,"order_lines":2,"orders":1},{"day":"2026-10-15","segment":"UNKNOWN","revenue_vnd":450000,"order_lines":1,"orders":1},{"day":"2026-10-15","segment":"Wholesale","revenue_vnd":20000000,"order_lines":1,"orders":1},{"day":"2026-10-16","segment":"Wholesale","revenue_vnd":750000,"order_lines":1,"orders":1},{"day":"2026-10-17","segment":"Retail","revenue_vnd":1890000,"order_lines":2,"orders":2}]
== Ngày cũ theo lineage (job Scala biết, Dagster không): 2026-10-15 2026-10-16
chạy lại 2026-10-15: exit=0
chạy lại 2026-10-16: exit=0
== Ngày cũ còn lại: []
== Sau khi chạy lại ngày cũ:
== GET /api/v1/revenue/daily?from=2026-10-15&to=2026-10-17
[{"day":"2026-10-15","segment":"Retail","revenue_vnd":700000,"order_lines":2,"orders":2},{"day":"2026-10-15","segment":"UNKNOWN","revenue_vnd":450000,"order_lines":1,"orders":1},{"day":"2026-10-15","segment":"Wholesale","revenue_vnd":20000000,"order_lines":1,"orders":1},{"day":"2026-10-17","segment":"Retail","revenue_vnd":1890000,"order_lines":2,"orders":2}]
== GET /api/v1/revenue/monthly
[{"month":"2026-10","segment":"Retail","revenue_vnd":2590000,"orders":4},{"month":"2026-10","segment":"UNKNOWN","revenue_vnd":450000,"orders":1},{"month":"2026-10","segment":"Wholesale","revenue_vnd":20000000,"orders":1}]
== GET /metrics
# TYPE p5_serving_days_loaded gauge
p5_serving_days_loaded 3
# TYPE p5_serving_last_day_epoch_day gauge
p5_serving_last_day_epoch_day 20743
# TYPE p5_serving_last_load_age_seconds gauge
p5_serving_last_load_age_seconds 6
```

**Đọc kỹ — đây là bài học quan trọng nhất của project.** Ba lần chạy "hằng ngày" (mỗi ngày chỉ materialize partition của **ngày đó**) để lại serving **sai**:
ngày 15 Retail 1.150.000 (O1/KB-01 vẫn PAID) và ngày 16 Wholesale 750.000 (O6 chưa hoàn tiền). Lô 16 mang hoàn tiền của ngày 15, lô 17 mang hoàn tiền của
ngày 16 — nhưng lịch của Dagster chỉ biết "hôm nay là ngày nào". Lệnh `p5.jar stale` hỏi **lineage** và trả đúng `2026-10-15 2026-10-16`; chạy lại hai ngày đó
cho kết quả khớp golden và khớp test "backfill 3 ngày = chạy hằng ngày". Trong production: một schedule chạy ngày hôm nay **cộng** một bước chạy lại mọi ngày
`stale` (bài mở rộng 1 biến nó thành Dagster sensor). `p5_serving_last_day_epoch_day 20743` = 17/10/2026.

Output test ban đầu của lead (`RUN_DB_TESTS=1 RUN_KAFKA_IT=1`, khi chỉ có 11 test); số test hiện tại ở mục 7:

```
Test run vn.scalaeng.p5.CoreSuite finished: 0 failed, 0 ignored, 5 total
Test run vn.scalaeng.p5.KafkaLandingSuite finished: 0 failed, 0 ignored, 2 total
Test run vn.scalaeng.p5.PlatformSuite finished: 0 failed, 0 ignored, 4 total
[info] Passed: Total 5 / Passed: Total 2 / Passed: Total 4        (sbt test, ba module)
```

## 7. Kiểm thử

| Suite | Test | Kiểm tra |
|---|---|---|
| `CoreSuite` (luôn chạy) | 15 | contract; landing no-op; golden 3 ngày; đối soát theo ngày/cấp raw; gate; dữ liệu muộn; các regression M1/M2/M3/N2. Thêm **R2-M1**: khác ngày cùng file (hai thứ tự), hai lô lần lượt và đảo thứ tự, collision với bản sai nghiệp vụ/ngày; **R2-M2**: rematerialize xen giữa snapshot/refs (có nhiều bản cùng byte), thiếu lịch sử khớp hash hoặc thiếu file → fail trước output |
| `KafkaLandingSuite` (`RUN_KAFKA_IT=1`) | 2 | Kafka → raw + quarantine (phong bì JSON có lý do) + lag 0; **chết sau khi ghi, trước commit** → raw có bản sao nhưng doanh thu không đổi |
| `PlatformSuite` | 7 | 4 test (`RUN_DB_TESTS=1`): load idempotent, backfill = chạy hằng ngày, gate chặn, API/metrics qua socket HTTP thật. 3 test luôn chạy: N1 thiếu/lặp/biên 366 ngày; **R2-N1** gọi route với `%2B02026-10-15`, ngày không tồn tại/sai độ rộng → 400 trước DB; M4 CLI `reconcile` công bố unassigned |

Bản Vòng 4 có **24 test**. Codex chạy `scala-cli.bat test project.scala core ingest serving app --server=false` (17/10/2026): mặc định **18 pass, 6 skipped**;
bật `RUN_KAFKA_IT=1`, không bật DB: **20 pass, 4 skipped**, exit 0, **0 compiler warning**. Log UTF-8:
[p5-kafka-utf8.log](../../.cache/t26/p5-kafka-utf8.log). Summary munit vẫn ghi Platform `0 ignored, 7 total`; bốn dòng `skipped` không được tính pass.
DB không bật vì review Vòng 2 đã gặp fixture PostgreSQL timeout 10000 ms → `ConnectException: Connection refused: getsockopt` trong sandbox; không xác nhận
lại load/API có DB hay sbt/Dagster trên bản sửa này. Hai hint nâng dependency không phải compiler warning; giữ pin chuẩn repo.

Golden (tính tay trước khi chạy, cùng dữ liệu với Ch31): ngày 15 Retail 700.000 / UNKNOWN 450.000 / Wholesale 20.000.000; ngày 16 rỗng; ngày 17 Retail 1.890.000;
tháng 10: Retail 2.590.000 (4 đơn), UNKNOWN 450.000, Wholesale 20.000.000. Đối soát ngày 15: 8 dòng = 1 bị loại (O3 quantity 0) + 0 xung đột event_id
+ 1 sự kiện gửi lại (E1503) + 1 bị thay (O1/KB-01 PAID → REFUNDED) + 0 xung đột + 5 giữ lại. Đối soát cấp raw: 15 dòng = 14 có ngày (8 + 4 + 2) + 1 `unassigned`
(E1506, `day='2026-13-01'`).

## 8. Lỗi thường gặp (gặp thật khi làm project này)

1. **Hiệu ứng chạy ngoài `IO`:** bản đầu của `Main.publish` gọi `Quality.check` (đọc file) trực tiếp, nên chương trình `IO(tính staging) *> publish(...)` đọc đĩa
   ngay lúc **dựng**, trước khi staging được tính: `java.nio.file.NoSuchFileException: ...\staging\staging_orders\day=2026-10-15\part.csv`. Mọi đọc/ghi file
   trong code Cats Effect phải nằm trong `IO.blocking` (Chương 16B).
2. **`#$name` không được nội suy trong `sql"..."` của Doobie 1.0.0-RC13:** SQL gửi đi còn nguyên `#` → `org.postgresql.util.PSQLException: ERROR: syntax error at or
   near "#"`. Tên database/bảng là định danh, không phải tham số `?`: dùng `Fragment.const` — và **chỉ** với chuỗi do chương trình tự đặt, không bao giờ với input
   người dùng.
3. **Scala CLI che lỗi ranh giới module:** `scala-cli test` biên dịch mọi module chung nên test của `app` dùng được `http4s-ember-client` dù module `app` không khai
   báo; `sbt test` báo `value client is not a member of org.http4s.ember`. Chạy **cả hai** trước khi coi là xong.
4. **Kỳ vọng sai về dữ liệu muộn:** test đầu viết "lần chạy ngày 16 không đụng ngày 15" — sai: lô 16 mang hoàn tiền O1 và đơn trễ O8 của ngày 15, nên ngày 15
   **phải** được nạp lại (`2026-10-15 loaded` lần thứ hai). Khi test đỏ, đọc lại dữ liệu trước khi sửa code.
5. **Dagster không có partition của ngày hôm nay** khi thiếu `end_offset=1` (Chương 31 mục 10).
6. **Lịch "chạy ngày hôm nay" để lại ngày cũ sai** — không có lỗi nào, chỉ có số sai (mục 6). Orchestrator phải hỏi lineage (`p5.jar stale`) chứ không chỉ lịch.
7. **Git Bash mở rộng `*`** khi truyền `--select "*"` sang `dagster.exe`: `Error: Got unexpected extra arguments (build.sbt core data ...)`. Liệt kê asset tường minh.

**Review chéo (Codex) tìm ra — không có exception nào, chỉ có số sai hoặc báo cáo im lặng:**

8. **CSV tự ghép chuỗi làm mất doanh thu (M1).** Staging nội suy `order_id` thẳng vào CSV: sự kiện hợp lệ `order_id="O,comma"` thành 8 cột, `DailyRevenue` âm thầm
   bỏ dòng không đủ 7 cột (`daily=Vector() gate=List()`), và quality gate dùng lại **chính phép tính đó** nên không thấy. Sửa: staging lưu JSON Lines qua codec
   circe; dòng không giải mã được là lỗi; gate đối soát tổng PAID của staging với tổng daily **độc lập** với phép join.
9. **`.head` quyết định doanh thu (M2).** Dedup `groupBy(event_id).map(_.head)` trước bước xung đột: hai sự kiện cùng `event_id` khác giá 100/900 cho daily
   100 hay 900 tùy **thứ tự dòng trong file**. Sửa: chỉ coi là gửi lại khi nội dung chuẩn hóa giống hệt; khác nội dung → cách ly cả nhóm (`event_conflicts`).
10. **Ba quy tắc ngày (M3).** Chọn partition so chuỗi, validate dùng regex + parse, đối soát chỉ dùng `LocalDate.parse`: `day="+02026-10-15"` được Java parse
    nên không vào `unassigned`, nhưng cũng không vào ngày 15 — rơi khỏi **mọi** đối soát. Sửa: một hàm `Order.dayOf` dùng ở cả ba nơi; test tổng raw = có ngày + unassigned.
11. **Đối soát chỉ có trong test (M4).** `Reconcile.unassigned` không có caller ngoài test; luồng vận hành không công bố dòng ngày sai. Sửa: lệnh `reconcile`
    (ghi `_quality/raw_reconcile.json`, mã 1 nếu không cân), asset Dagster `raw_reconcile` gắn metadata + cảnh báo. Chính sách: **không** chặn publish các ngày
    hợp lệ (chúng không bị ảnh hưởng), nhưng số lượng và vị trí phải nhìn thấy được.
12. **Script demo `rm -rf "$WORK"` và luôn trả 0 (M5, M6).** `${WORK:?}` chỉ chặn chuỗi rỗng; mọi lệnh Dagster trả 23 mà script vẫn exit 0. Sửa: `WORK` phải
    tồn tại, mỗi lần chạy `mktemp -d` thư mục con, không xóa gì; mỗi bước được kiểm mã, readiness bằng `curl -f`, `trap` giữ mã thoát (mục 6: probe Dagster giả trả 23).
13. **API: thiếu tham số → 404, "366 ngày" nhận 367 (N1).** Matcher query của http4s không khớp thì route rơi xuống 404; `DAYS.between(...) <= 366` với
    `BETWEEN` bao cả hai đầu là 367 ngày. Sửa: bắt route trước rồi validate (thiếu/lặp/sai → 400 có lý do), giới hạn tính cả hai đầu.
14. **Cleanup treo trên Windows + Git Bash.** Đóng đầu ghi FIFO không chuyển EOF qua `scala-cli.bat` đến JVM, nên chờ `wait` không có hạn có thể treo;
    `kill` riêng launcher còn có thể để `postgres.exe` con giữ cổng 55432. Bản hiện tại dùng file `stop` trong thư mục `p5-run.*` mới tạo cho mỗi lần chạy.
    `LocalPostgres` ghi PID + thời điểm khởi động cho JVM và PostgreSQL (`jvm.pid/started`, `pg.pid/started`), poll file mỗi 200 ms, gọi `pg.close()` trong
    `finally`, rồi chỉ ghi `pg.stopped` khi cổng đã đóng và
    `postmaster.pid` đã mất. `pg.ready` là marker riêng cho readiness. STDIN đóng không ảnh hưởng vòng đời server.

    Cleanup tạo `stop`, chờ launcher tối đa 30 giây; khi thiếu xác nhận dừng hoặc cổng còn bận, gọi `pg_ctl -D <run-dir>/pg-data -m immediate -t 10 -w stop`
    với giới hạn ngoài 15 giây. Nếu server hoặc JVM/launcher vẫn sống, chỉ dừng tiến trình thuộc lần chạy: Windows kiểm PID, thời điểm khởi động và dòng lệnh
    chứa run-dir trước `taskkill /T`; PostgreSQL phải có dòng lệnh chứa đúng `pg-data`. Nhánh dự phòng dùng cả `postmaster.pid` nếu start hỏng trước marker.
    Unix kiểm dòng lệnh rồi gửi signal. Mọi lần chờ tiến trình/cổng đều có hạn. Không kill theo tên `postgres.exe` hoặc chỉ theo cổng.
    Cổng đã bận trước khi start thì script trả 1, không đụng server đang chạy. Exit 0 chỉ khi mọi bước và cleanup thành công; một bước hỏng, cleanup timeout
    hoặc thiếu marker dừng đều trả 1. Giữ toàn bộ log và dữ liệu, kể cả `postgres-stop.log` khi cần fallback.

    `pg-data`, binaries PostgreSQL và marker đều nằm trong thư mục con do `mktemp -d` tạo; script không xóa dữ liệu. Thư mục làm việc của zonky cũng đặt
    trong run-dir, `setCleanDataDirectory(false)` giữ cluster để xem lỗi. Trên Windows đặt `SCALA_CLI=scala-cli.bat`; đường dẫn đưa sang JVM/Python được
    chuẩn hóa bằng `cygpath`. Cần `timeout` của Git Bash cho nhánh dự phòng. **Chưa kiểm chứng runtime Vòng 3:** compile và kiểm cú pháp đạt; lần chạy trong
    sandbox dừng ở `AccessDeniedException` khi JVM resolve run-dir, trước khi PostgreSQL khởi động. Lead cần chạy thật cả luồng thành công, lỗi Dagster và
    dừng dự phòng; xem handoff P5. Không coi marker giả lập là PostgreSQL/Dagster thật.

15. **Xung đột ID khác ngày lọt qua gate (R2-M1).** Lọc ngày trước group ID cho phép cùng ID ở ngày 15 giá 100 và ngày 16 giá 900 đều đóng góp doanh thu;
    lô sau chỉ chứa ngày 16 còn để ngày 15 fresh. Sửa: chỉ mục toàn raw trước khi chọn ngày và fingerprint ID trong dependency mỗi ngày. Hai ngày đều cách ly
    bản của mình, kể cả lô đến đảo thứ tự; rebuild rồi gate mới pass. Đối soát mỗi ngày vẫn cân.
16. **Snapshot đúng byte nhưng thiếu ID upstream (R2-M2).** Upstream rematerialize giữa lúc đọc file và dựng refs: chỉ tra latest thấy hash khác rồi ghi None,
    dù bản cũ còn trong lineage. Sửa: tra lịch sử khớp hash, chọn bản khớp gần nhất nếu cùng byte; không có bản khớp thì fail rõ trước khi ghi output.
17. **API nhận năm mở rộng (R2-N1).** `LocalDate.parse` nhận `+02026-10-15` dù API hứa `yyyy-MM-dd`. API dùng chung `Order.dayOf`; route test truyền dấu `+`
    đã URL-encode (`%2B`) để xác nhận 400 do quy tắc ngày, không do giải mã query.

## 9. Runbook vận hành

### Sự cố 1 — Consumer lag tăng (dữ liệu realtime "đứng")

**Dấu hiệu:** `KafkaLanding.lag` tăng liên tục ở một hoặc nhiều partition; `p5_serving_last_load_age_seconds` tăng; số file mới trong `raw/orders/ingest_date=<hôm nay>`
không tăng.

1. Consumer còn sống không? (log/heartbeat). Chết lặp lại → đọc lỗi cuối: thường là ghi raw thất bại (đĩa đầy, quyền) — **đừng** commit offset bằng tay để "bỏ qua".
2. Sống nhưng chậm: so tốc độ vào (end offset tăng/phút) với tốc độ landing; xem `quarantine/` có tăng vọt không (producer đổi schema → sự cố 3).
3. Cần đuổi kịp: tăng `max.poll.records` hoặc số partition/consumer (một consumer mỗi partition). An toàn vì landing at-least-once + dedup theo `event_id`.
4. Sau khi đuổi kịp: chạy lại các ngày bị ảnh hưởng — lineage tự đánh dấu cũ; xác nhận `/metrics` và đối soát.

### Sự cố 2 — Job batch hỏng giữa chừng

**Dấu hiệu:** Dagster báo run thất bại ở một asset; mã thoát của `p5.jar` cho biết loại lỗi.

1. Đọc mã thoát: **3** (file bị cách ly) → sự cố 3; **4** (quality gate chặn) → đọc danh sách vi phạm, KHÔNG nạp bằng tay; **1** → lỗi hạ tầng.
2. Không cần dọn dẹp: mỗi partition ghi nguyên tử, serving nạp trong một transaction — bước hỏng không để lại trạng thái nửa vời. Lineage chỉ có dòng cho bước đã xong.
3. Sửa nguyên nhân rồi **chạy lại cùng partition** trong Dagster: bước đã xong sẽ ra cùng kết quả (idempotent), `publish` trả `unchanged` nếu không có gì đổi.
4. Nhiều ngày bị ảnh hưởng: backfill khoảng ngày; kiểm `serving_loads` và đối soát từng ngày.

### Sự cố 3 — Producer đổi schema

**Dấu hiệu:** file/record vào `quarantine/orders/` ("bộ trường sai: thừa channel", "sai kiểu ..."); số dòng raw của ngày giảm. Đường **file**: lý do ở
`<file>.reason.txt`, `land-file` trả mã 3. Đường **Kafka**: lý do nằm trong từng phong bì JSON (`reason`), consumer vẫn commit (record đã được giữ ở quarantine).

1. Đọc lý do; xác định thay đổi có **tương thích** theo hướng của bạn không (Chương 33: FORWARD — reader cũ đọc dữ liệu mới).
2. Không tương thích: báo producer (owner trong contract), giữ quarantine làm bằng chứng; **không** sửa file trong raw.
3. Tương thích và được chấp nhận: nâng phiên bản contract + code (`OrderEvent`, `StagingOrders.codeVersion`), deploy, rồi:
   - **3a — đường file:** thả lại các file trong quarantine (`land-file`) → lineage làm cũ đúng các ngày liên quan → backfill.
   - **3b — đường Kafka:** file quarantine **không** thả lại trực tiếp được (mỗi dòng là phong bì, không phải sự kiện). Trích `value` của từng dòng (ví dụ
     `jq -r .value`), sửa nếu cần, rồi gửi lại vào topic (producer) hoặc ghi thành một file `.jsonl` mới và `land-file` — staging dedup theo `event_id`
     nên gửi lại an toàn. Giữ file quarantine gốc làm bằng chứng.
4. Thêm một test contract cho phiên bản mới trước khi deploy.

## 10. Bài tập mở rộng

1. Biến `p5.jar stale` thành một Dagster **sensor**: định kỳ hỏi danh sách ngày cũ và tạo run cho từng ngày (thay vì vòng lặp trong script).
2. Thay lake filesystem bằng object storage (MinIO/S3) và Parquet; giải quyết rename không nguyên tử (table format hoặc con trỏ phiên bản).
3. Cho Project 2 phát sự kiện đơn hàng ra Kafka (outbox pattern trong cùng transaction với đơn) và nối vào P5.
4. Thay `DailyRevenue.compute` bằng SQL (dbt hoặc view) và chứng minh output giống hệt bằng golden + kappa replay.
5. Cảnh báo: rule Prometheus cho `p5_serving_last_load_age_seconds > 2 giờ` và cho tỷ lệ quarantine.

## 11. Checklist tự đánh giá

- [ ] Tôi giải thích được vì sao mỗi việc trong bảng 4.1 nằm ở công cụ đó.
- [ ] Chạy lại bất kỳ bước nào cũng an toàn; tôi chỉ ra được cơ chế idempotent của từng bước (hash file, lineage, transaction + output_hash).
- [ ] Tôi chứng minh được bằng test: backfill = chạy hằng ngày; chết giữa landing và commit không làm sai số.
- [ ] Dữ liệu xấu có chỗ của nó (quarantine, rejected, conflicts, unassigned) và được đếm.
- [ ] Tôi xử lý được 3 sự cố theo runbook mà không sửa dữ liệu bằng tay.

## 12. Nguồn tham khảo

- Chương 14–16B, 24, 29–31, 33 và Project 2–4 của giáo trình.
- Dagster: https://docs.dagster.io/ · PyPI: https://pypi.org/project/dagster/
- Kafka consumer (commit, `max.poll.records`): https://kafka.apache.org/documentation/#consumerconfigs
- PostgreSQL advisory lock: https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
- Doobie: https://typelevel.org/doobie/ · http4s: https://http4s.org/v0.23/
