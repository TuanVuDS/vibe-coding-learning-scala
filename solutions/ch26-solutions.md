# Lời giải — Chương 26: Spark Production Engineering

> Code: [`ch26-code/`](ch26-code/) — Scala 2.13.18 + Spark 4.2.0, dùng lại source của `09-spark/spark-production-lab` và `batch-pipeline-lab`.
> Chạy: `cd solutions/ch26-code && scala-cli test .` → **6/6 test pass** (17/10/2026, Windows 11 + JDK 21).

## Bài 1 — Phân loại lỗi

| Đầu vào (test) | Kết quả |
|---|---|
| `SparkException("Job aborted", RuntimeException("task", SocketTimeoutException))` | `Transient` |
| `IOException("đọc lỗi", NoSuchFileException("/data/x"))` | `Permanent` — dù lớp ngoài là `IOException` |
| `RuntimeException(IllegalArgumentException("customers trùng customer_id"))` | `Data` |
| `IllegalStateException` | `Unknown` — không retry, cần người xem |

- Thứ tự ưu tiên khi chuỗi cause có nhiều loại: **Data > Permanent > Transient > Unknown**. Đi qua toàn bộ `getCause` — Spark bọc lỗi gốc nhiều tầng.
- Transient là **danh sách cho phép** (timeout, mất kết nối, `TransientStorageException` của lab), không phải "mọi `IOException`": ZIP/Parquet hỏng
  (`ZipException`), file đã tồn tại, thiếu file cũng là `IOException` nhưng retry vô ích. `IOException("không rõ")` → `Unknown`.
- Lời giải viết lại bộ phân loại **độc lập** để luyện tập (không gọi `vn.scalaeng.sparkprod.Failures`), và test ép nó cho **cùng kết quả** với bộ phân loại của
  lab trên mọi ca kiểm thử — hai bản lệch nhau là lỗi.
- `Unknown` mặc định **không** retry: retry mù một lỗi chưa hiểu che mất nó và tốn tài nguyên.

## Bài 2 — Backoff có trần + full jitter

```scala
val exp     = math.min(attempt - 1, 63)
val ceiling = if (exp >= 63 || baseMs > (capMs >> exp)) capMs else math.min(capMs, baseMs << exp)
math.min(ceiling, math.round(rnd.nextDouble() * ceiling.toDouble))
```

- `base << exp` tràn `Long` khi `base > Long.MaxValue >> exp`. Không cần tính đúng giá trị khổng lồ — chỉ cần biết nó **vượt trần**: `base > cap >> exp` ⇒
  `base·2^exp > cap`. Với `exp >= 63`, `base >= 1` nên `base·2^63 > Long.MaxValue >= cap` — bão hòa ở `cap`. (Bản đầu kẹp `exp = 62` vô điều kiện nên trần
  bị giữ ở `base·2^62` khi `cap` lớn hơn.) Test chạy lần thử 1..64, `base = Long.MaxValue/2`, và lần thử 64 với `cap = Long.MaxValue`.
- **Full jitter** (chờ ngẫu nhiên trong `[0, trần]`) thay vì chờ đúng `trần`: trăm job cùng lỗi lúc 02:00 không cùng thử lại một lúc.
- `Random` truyền vào → test xác định (cùng seed cùng kết quả).

## Bài 3 — Ngày còn thiếu lần chạy thành công

Manifest: `a` 14/10 SUCCEEDED, `b` 15/10 DATA_REJECTED, `c` 16/10 FAILED, `d` 16/10 SUCCEEDED → thiếu **15/10 và 17/10**.

- Ngày 16 có một lần lỗi **và** một lần thành công: tính là đủ — chạy lại sau lỗi là bình thường.
- "Thành công" = `status` SUCCEEDED **và** `exit_code` 0. Manifest được ghi **cuối cùng**, kèm mã thoát thật: dữ liệu đã publish nhưng bước kết thúc (đóng
  metrics, ghi file metrics) hỏng thì manifest ghi SUCCEEDED + `exit_code: 1` + `finalization_errors` — ngày đó **chưa** được tính là xong (test ngày 18/10).
- Đọc manifest bằng regex cho gọn bài tập; production dùng thư viện JSON hoặc một bảng "run log" trong DB/catalog.
- Đây là nền của cảnh báo SLA: "đến 07:00 chưa có SUCCEEDED cho hôm qua".

## Bài 4 — Kiểm tra khối lượng

- Lịch sử `[1000, 1100, 900, 1050]` → trung vị 1025; hôm nay 400 < 50% → `hôm nay 400 dòng < 50% trung vị 1025.0`; 800 → không cảnh báo.
- Trung vị tính bằng `BigDecimal`: cộng hai `Long` ở giữa có thể tràn (`Long.MaxValue + Long.MaxValue = -2`) và im lặng bỏ qua cảnh báo.
- **Trung vị** thay vì trung bình: một ngày đột biến (khuyến mãi) không kéo ngưỡng.
- Dưới 3 lần chạy: không cảnh báo — **im lặng có chủ đích**, ghi rõ; còn hơn cảnh báo giả mỗi lần triển khai mới.
- Quy tắc từng dòng (Chương 24) bắt dòng **sai**; kiểm tra thống kê bắt dòng **mất** (nguồn gửi thiếu một file).

## Bài 5 — Kiểm tra tương thích lúc khởi động

- Runtime thật của test (Spark 4.2.0, Scala 2.13.18, Java 21.0.x) → `Nil`.
- `("3.5.4", "2.12.20", "11.0.2")` → ba lỗi rõ ràng, một cho mỗi thành phần.
- So **major.minor**: Spark 4.2.x tương thích nhị phân trong cùng nhánh; 4.1 hay 4.3 thì không bảo đảm. Java so major.
- Không có bước này, jar build cho 4.2 chạy trên cụm 3.5 hỏng giữa chừng với `NoSuchMethodError` — sau khi đã ghi một phần output.

## Bài 6 — checkpoint vs localCheckpoint

| | Node plan | File mới trong checkpointDir | Tổng |
|---|---|---|---|
| `checkpoint(eager = true)` | 1 | > 0 | bằng nhau |
| `localCheckpoint(eager = true)` | 1 | **0** | bằng nhau |

- Cả hai cắt plan về 1 node (`LogicalRDD`). Khác nhau ở **độ bền**: checkpoint ghi ra thư mục bền (HDFS/S3 trên cluster); localCheckpoint lưu ở executor — mất
  executor là mất, Spark không tính lại được vì lineage đã bị cắt.
- Dùng localCheckpoint khi chỉ cần cắt plan cho nhanh và chấp nhận chạy lại cả job nếu mất executor.
