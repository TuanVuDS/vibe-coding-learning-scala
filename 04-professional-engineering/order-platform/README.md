# order-platform

Batch job xử lý file đơn hàng CSV (Chương 9–10 của giáo trình). Scala 3.9.0, sbt 1.12.11, JDK 21.

## Module

| Module | Vai trò | Phụ thuộc |
|---|---|---|
| `testkit` | Gen/Prop cho property-based testing (chỉ dùng khi test) | — |
| `domain` | Opaque type và kiểu dữ liệu | — |
| `core` | Parse CSV, validate, dedup, doanh thu theo ngày (thuần) | `domain` |
| `app` | Cấu hình, đọc/ghi file, log JSON, exit code | `core` |

## Lệnh

```bash
sbt test                 # toàn bộ test
CI=true sbt test         # như CI: cảnh báo thành lỗi
sbt scalafmtAll          # định dạng code
sbt app/run              # chạy với APP_ENV=dev và sample-data/orders.csv
```

## Biến môi trường

| Biến | Mặc định | Ghi chú |
|---|---|---|
| `APP_ENV` | `dev` | `dev` / `staging` / `prod` |
| `INPUT_PATH` | `sample-data/orders.csv` (chỉ dev) | Bắt buộc ngoài dev |
| `OUTPUT_DIR` | `out` | |
| `MAX_REJECT_RATIO` | `0.05` | Trong [0, 1] |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

## Exit code

| Code | Ý nghĩa | Gợi ý cho orchestrator |
|---|---|---|
| 0 | Thành công | Chạy bước tiếp theo |
| 1 | Cấu hình sai | Không retry; sửa cấu hình |
| 2 | Dữ liệu vượt ngưỡng lỗi hoặc file sai định dạng | Không retry; người kiểm tra dữ liệu |
| 3 | Lỗi I/O | Có thể retry có giới hạn |

## Trạng thái kiểm chứng

Mã nguồn đã biên dịch bằng `scalac 3.9.0` với `-Werror` và chạy thật; `sbt test` (sbt 1.12.11): 25/25 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Plugin scalafmt và GitHub Actions chưa chạy — hãy báo lại nếu gặp lỗi.
