# Danh sách project thực hành

Tất cả project dùng **cùng một domain**: một sàn thương mại điện tử nhỏ có người dùng, sản phẩm,
đơn hàng, và các sự kiện click/impression. Nhờ vậy dữ liệu và khái niệm được tái sử dụng xuyên suốt,
và Project 5 có thể ghép các project trước lại với nhau.

## Mini project theo chương (Giai đoạn 1)

| Chương | Mini project | Kỹ năng |
|---|---|---|
| Ch3 | Máy tính hóa đơn dòng lệnh | expression, BigDecimal, match |
| Ch4 | Bộ làm sạch tên/email người dùng | function composition |
| Ch5 | Domain model đơn hàng | case class, enum, trait |
| Ch6 | Thống kê log truy cập | groupBy, fold, sort |
| Ch7 | Parser CSV an toàn | Either, ADT lỗi |
| Ch8 | Kiểu an toàn cho tiền và ID | opaque type, type class |

## Project 1 — Scala Data Processor (sau Ch10)

**Mục tiêu:** nắm chắc Scala core và FP.

- **Input:** `orders.csv` (có dòng lỗi, thiếu field, trùng `order_id`) và `users.json`.
- **Xử lý:** parse → validate (tích lũy lỗi) → chuẩn hóa (email, số điện thoại, tiền tệ)
  → dedup theo `order_id` (giữ bản ghi có `updated_at` mới nhất) → join user → aggregate doanh thu theo ngày/thành phố.
- **Output:** `report.csv`, `rejected.csv` (kèm lý do lỗi), log thống kê.
- **Kỹ thuật:** Scala 3, collections, Either/Validated, case class, opaque type, munit + ScalaCheck.
- **Tiêu chí hoàn thành:** chạy lại 2 lần cho cùng kết quả; test bao phủ mọi rule validate; không có `var` trong core logic.

## Project 2 — Backend Service (sau Ch18)

**Mục tiêu:** áp dụng Scala vào Backend Engineering.

- **Chức năng:** đăng ký/đăng nhập (JWT), CRUD sản phẩm, tạo/xem đơn hàng, phân trang/lọc/sắp xếp.
- **Kỹ thuật:** Scala 3.9, Cats Effect, http4s Ember, Tapir (OpenAPI), circe, Doobie, PostgreSQL,
  Flyway, HikariCP, Testcontainers, Docker Compose.
- **Kiến trúc:** module `domain` (thuần, không phụ thuộc thư viện HTTP/DB), `service`, `persistence`, `http`, `app`.
- **Tiêu chí hoàn thành:** integration test chạy với PostgreSQL thật; graceful shutdown;
  `/health` và `/metrics`; tài liệu OpenAPI sinh tự động.

## Project 3 — Batch Data Pipeline (sau Ch26)

**Mục tiêu:** pipeline Spark có thể vận hành.

- **Input:** file CSV (đơn hàng) + JSON Lines (click) theo lô `ingest_date`, 3 ngày, sinh có seed. *Đã đổi so với kế hoạch MinIO/S3:* tổ hợp `hadoop-aws` +
  AWS SDK chưa kiểm chứng; S3 không có rename nguyên tử nên publish phải đổi chiến lược — để thành bài mở rộng (README P3 mục 4.6).
- **Pipeline:** read → kiểm tra schema → validate → chuẩn hóa → tách bản ghi lỗi → dedup → join → aggregate
  → ghi Parquet partition theo ngày → data quality check.
- **Kỹ thuật:** **Scala 2.13 + Spark 4.2.0** (build riêng), fat jar + spark-submit (đã chạy thật trên WSL2).
- **Tiêu chí hoàn thành:** rerun một ngày không sinh dữ liệu trùng (overwrite theo partition);
  giải thích được query plan của bước join; có test cho từng transformation bằng DataFrame nhỏ.

## Project 4 — Kafka Event Processing (sau Ch30)

**Mục tiêu:** ứng dụng xử lý event bằng Scala.

- **Luồng:** producer sinh click/order event → topic `events.raw` → consumer validate schema
  → transform → ghi PostgreSQL (upsert idempotent) → event lỗi sang `events.dlq`.
- **Kỹ thuật:** Scala 3.9, kafka-clients/fs2-kafka, Avro hoặc JSON Schema, Docker Compose (Kafka KRaft).
- **Tiêu chí hoàn thành:** chứng minh bằng test rằng event trùng không làm sai kết quả;
  restart consumer giữa chừng không mất dữ liệu (at-least-once + idempotent sink); có metric consumer lag.

## Project 5 — Production-Oriented Data Platform (sau Ch34)

```
Source (backend P2 + event producer)
   → Ingestion (Kafka topic / file drop)
   → Processing (consumer Scala 3 cho realtime, Spark 2.13 cho batch hằng ngày)
   → Data Quality Checks
   → Storage (Parquet trên MinIO + PostgreSQL serving tables)
   → Serving (API http4s đọc bảng tổng hợp)
   → Monitoring (metrics + log + cảnh báo đơn giản)
   ↑ Orchestration: Dagster (Python) lập lịch job batch và backfill
```

**Trách nhiệm từng thành phần (nội dung bắt buộc trong README của project):**

| Việc | Công cụ | Vì sao không dùng Scala cho việc này / vì sao dùng |
|---|---|---|
| Aggregate đơn giản trên bảng đã sạch | SQL (trong Spark SQL hoặc PostgreSQL) | SQL ngắn gọn, ai cũng đọc được, optimizer làm tốt |
| Validation phức tạp, parse format lạ, business rule | Scala | Kiểu mạnh, test được, tái sử dụng |
| Lập lịch, retry cấp job, backfill | Dagster | Đây là orchestration, không phải processing |
| Lưu trữ | MinIO/PostgreSQL | Hệ thống lưu trữ chuyên dụng |

**Tiêu chí hoàn thành:** backfill 3 ngày cho kết quả giống chạy hằng ngày; tài liệu vận hành
(runbook) cho 3 sự cố: consumer lag tăng, job batch fail giữa chừng, schema thay đổi.
