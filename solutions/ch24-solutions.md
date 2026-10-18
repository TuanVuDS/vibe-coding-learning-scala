# Lời giải — Chương 24: Batch Data Pipeline

> Code: [`ch24-code/`](ch24-code/) — Scala 2.13.18 + Spark 4.2.0, dùng lại `09-spark/batch-pipeline-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch24-code && scala-cli test .` → **6/6 test pass** (17/10/2026, Windows 11 + JDK 21).

## Bài 1 — Quy tắc liên trường

```scala
val violated = col("day").isNotNull && col("updated_at").isNotNull && to_date(col("updated_at")) < col("day")
v.withColumn("errors", when(violated, concat(col("errors"), array(lit(Reason)))).otherwise(col("errors")))
```

- Quy tắc trên **một** trường (`day` là ngày, `updated_at` là timestamp) không bắt được "cập nhật trước khi đặt hàng" — cần quy tắc trên **hai** trường.
- Chỉ áp khi cả hai trường parse được: dòng `Z` có `day = 2026-13-01` chỉ nhận lỗi `day`, không thêm lỗi "phái sinh" làm nhiễu báo cáo (test so đúng mảng lỗi
  của ba dòng X, Y, Z).
- `to_date(updated_at)` theo **múi giờ phiên** (`spark.sql.session.timeZone = UTC` trong lab). Đơn đặt 23:00 giờ Việt Nam là ngày khác trong UTC — quy tắc so ngày
  phải chốt múi giờ, giống bài học Chương 19.

## Bài 2 — Báo cáo chất lượng theo lô

```scala
Pipeline.rejected(validated).select(col("ingest_date"), explode(col("errors")).as("reason"))
  .groupBy("ingest_date", "reason").agg(count(lit(1)).as("rows"))
```

- `explode` biến mảng lý do thành nhiều dòng: một dòng 6 lỗi (O5) đóng góp 6 lần. Lô 15/10: **2** dòng bị loại nhưng **7** lần vi phạm — báo cáo phải nói rõ
  đang đếm cái nào.
- Lưu báo cáo theo lô mỗi lần chạy → thấy xu hướng (nguồn nào tệ đi), cơ sở cho data contract với bên gửi (Chương 33).

## Bài 3 — Quality gate theo lô

| Lô | Dòng | Bị loại | Tỷ lệ | Ngưỡng 20% | Ngưỡng 30% |
|---|---|---|---|---|---|
| 2026-10-15 | 7 | 2 | 28,6% | **chặn** | qua |
| 2026-10-16 | 6 | 1 | 16,7% | qua | qua |

- Gate tính **theo lô**: tỷ lệ chung 3/13 = 23% che mất việc một lô cụ thể tệ.
- Trả `Either`: `Left(các lô vi phạm)` — người gọi buộc phải xử lý; chạy gate **trước** khi ghi để không có output dở dang.
- Ngưỡng `NaN` → `require` thất bại ngay (`rate > NaN` luôn false → gate không bao giờ chặn — lỗi đã bị review chéo bắt ở Chương 20).

## Bài 4 — Write–audit–publish

```
df ──ghi──► daily.staging ──audit (đọc lại)──► ok? ──► đổi tên daily → daily.old, staging → daily, xóa daily.old
                                              └── lỗi ──► xóa staging, daily giữ nguyên
```

- Test: publish 10 dòng, rồi publish bản 3 dòng với audit "phải đủ 10" → `Left(List("thiếu dòng"))`, thư mục phục vụ vẫn đọc ra **10**, không còn `daily.staging`.
- `Files.move(..., ATOMIC_MOVE)` nguyên tử trên **cùng** hệ thống file cục bộ. Giữa hai lần `move` có một khoảnh khắc `daily` không tồn tại — người đọc lúc đó nhận
  lỗi "không có thư mục" (chứ không đọc dữ liệu dở dang). Object storage (S3, GCS) **không** có rename nguyên tử; ở đó dùng table format có transaction log
  (Delta Lake, Iceberg) hoặc một con trỏ "phiên bản hiện tại" — Chương 31.

## Bài 5 — Partition mồ côi của dynamic overwrite

Tái hiện (test): ghi ngày 15 và 16; tính lại chỉ còn ngày 15 → ghi dynamic → thư mục `day=2026-10-16` **vẫn còn** với dữ liệu cũ.

```scala
val expected = df.select("day").distinct().collect().map(_.getDate(0).toString).toSet
Pipeline.writeByDay(df, dir)
val stale = existingDays.filter(scope.contains) -- expected   // chỉ trong PHẠM VI lần tính lại
stale.foreach(deleteDirectory)
```

- `scope`: các ngày mà input của lần chạy này **bao phủ**. Không có `scope`, hàm sẽ xóa cả các ngày cũ mà lần chạy này không hề đọc — một lỗi còn tệ hơn.
- Khi nào xảy ra thật: mọi dòng của một ngày thành xung đột; sửa bug làm `day` của các đơn đổi sang ngày khác; nguồn rút lại (retract) cả một ngày.

## Bài 6 — Compaction

```scala
spark.read.parquet(partitionDir).repartition(n).write.parquet(staging)   // đọc THẲNG thư mục partition: không có cột day
delete(partitionDir); move(staging, partitionDir)
```

- Test: 8 file → 2 file; `exceptAll` hai chiều = 0 và đủ 1.000 dòng → **multiset** không đổi (so `count` thôi không đủ: có thể mất một dòng và trùng một dòng).
- Đọc thẳng thư mục `day=...`: cột `day` không có trong file mà nằm ở tên thư mục, nên ghi lại không thêm cột thừa.
- Compaction là job riêng, chạy khi không ai đang ghi partition đó (hoặc qua table format có `OPTIMIZE`). Đổi lại: thêm một lượt đọc/ghi toàn partition.
