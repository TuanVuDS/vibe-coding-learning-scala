# Chương 24 — Batch Data Pipeline

> **Phiên bản:** **Scala 2.13.18** · JDK 21 · Apache Spark **4.2.0** (`spark-sql_2.13`) · hadoop-bare-naked-local-fs 0.1.0 (chỉ cần trên Windows) · munit 1.2.4
> **Code:** [`batch-pipeline-lab/`](batch-pipeline-lab/) — pipeline 10 bước (read → schema → validate → chuẩn hóa → bản ghi lỗi → dedup → join → aggregate →
> write → kiểm tra) trên hai lô đơn hàng; thí nghiệm small files, ba chế độ ghi, partition pruning
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **18/18 test pass** (`scala-cli test .` và `sbt test`; 4 test regression từ review chéo), lời giải 6/6, Spark 4.2.0
> local mode trên Windows 11 + JDK 21. Mọi bảng, kế hoạch, số file và thông báo lỗi trong bài là output thật; số liệu doanh thu khớp **tính tay** (mục 6).

---

## Vị trí trong lộ trình

Chương 19–20 xây pipeline bằng Scala thuần trên một máy; Chương 22–23 dạy mô hình thực thi và API của Spark. Chương này **ghép** tất cả thành một pipeline
batch chạy được, chạy lại được, và **tự kiểm tra** được. Chương 25 tối ưu hiệu năng (skew, join, partition) trên nền này; Chương 26 đưa nó lên production
(cấu hình, giám sát, triển khai); **Project 3** mở rộng nó thành một ứng dụng hoàn chỉnh.

## 1. Mục tiêu học tập

1. Thiết kế pipeline thành các **bước có hợp đồng rõ ràng**: mỗi dòng input đi vào đúng một ngăn (fact, bị loại, trùng, bị thay thế, xung đột).
2. Đọc dữ liệu thô an toàn: lớp RAW toàn String, kiểm tra header, `try_cast`, `_corrupt_record`, **partition discovery**.
3. Tách **bản ghi lỗi** kèm lý do có cấu trúc; dedup hai tầng (trùng tuyệt đối, phiên bản); join với dimension đã kiểm tra.
4. Chọn **file layout**: định dạng, cột partition, số file mỗi partition; nhận ra và sửa **small files problem**.
5. Hiểu ba chế độ ghi (append, overwrite tĩnh, overwrite động) dưới góc **idempotency** và **rerun safety**.
6. Đọc plan để xác nhận **partition pruning**, broadcast join; biết khi nào skew và shuffle cần tối ưu (Chương 25).
7. Viết bước **kiểm tra kết quả** đọc lại output và đối soát số dòng/tổng tiền.

## 2. Kiến thức tiên quyết

Chương 23 (schema tường minh, `enforceSchema`, window dedup có phát hiện xung đột, kiểm tra khóa dimension, NULL), Chương 22 (partition, shuffle, cache, AQE),
Chương 21 (Parquet, schema), Chương 20 (validation gom lỗi, quality gate, `BigInt` cho tổng), Chương 19 (bảng rejected, dedup, event trễ).

## 3. Lý thuyết

### 3.1. Pipeline là chuỗi bước có hợp đồng

**Vấn đề.** Một job "đọc CSV → groupBy → ghi" chạy được trong ngày đầu. Ngày thứ hai có dòng hỏng, file gửi lại hai lần, một đơn được hoàn tiền sau một ngày,
và ai đó chạy lại job sau sự cố. Câu hỏi "doanh thu ngày 15 là bao nhiêu?" bắt đầu có **nhiều đáp án**.

**Cách hoạt động.** Lab chia pipeline thành 10 bước, mỗi bước là một hàm `DataFrame => DataFrame` (hoặc tách thành nhiều nhánh), test được riêng:

```
data/input/ingest_date=2026-10-15/orders.csv ┐
data/input/ingest_date=2026-10-16/orders.csv ┴─ 1 read (RAW, String) ─ 2 schema ─ 3 validate + 4 chuẩn hóa ─┬─ 5 rejected/ (giá trị trường + lý do)
                                                                                                           └─ accepted ─ 6 dedup ─┬─ trùng tuyệt đối (đếm)
                                                                                                                                  ├─ bị thay thế (đếm)
                                                                                                                                  ├─ conflicts/ (cách ly)
                                                    data/customers.csv ─ kiểm tra khóa duy nhất ─ 7 join (broadcast) ─┘ latest
                                                                                         ─ 8 aggregate ─ 9 write (Parquet, partition theo day) ─ 10 đọc lại + đối soát
```

Hợp đồng đối soát (bước 10) — kiểm tra trên **mọi** lần chạy, không chỉ trong test:

```
input    = rejected + accepted
accepted = trùng tuyệt đối + bị thay thế + xung đột + fact
```

Không dòng nào "biến mất" mà không có tên ngăn. Đây là thứ khác biệt giữa một job và một **pipeline**. Hai phương trình này so các **số trong bộ nhớ**; bước 10
còn **đọc lại** mọi bảng đã ghi và so **nội dung** (mục 3.8) — vì số khớp không chứng minh dữ liệu trên đĩa đúng.

**Ngữ nghĩa một lần chạy** (chốt sau review chéo): output = **snapshot** của toàn bộ input có `ingest_date <= upTo`. Chạy lại cùng `upTo` → cùng output;
tăng `upTo` → dữ liệu trễ sửa đúng ngày cũ; **giảm** `upTo` → quay về snapshot cũ hơn, kể cả **xóa** những ngày không còn dữ liệu. Bảng rỗng (mọi dòng bị loại,
không có đơn PAID) là kết quả hợp lệ, không phải lỗi.

**Lựa chọn thay thế.** Một câu SQL dài làm hết: ngắn, nhưng không test từng bước, không có ngăn cho bản ghi lỗi. Công cụ data quality chuyên dụng (Great Expectations,
Deequ): mạnh, thêm phụ thuộc — Chương 33. **Lỗi thiết kế thường gặp:** chỉ đếm output, không đếm phần bị bỏ; filter "sạch" dữ liệu bằng `na.drop()` không ghi lại.

### 3.2. Read và schema: lớp RAW toàn String

**Vấn đề → vì sao.** `FAILFAST` dừng cả job vì một dòng; `PERMISSIVE` với schema có kiểu biến giá trị hỏng thành `null` **không lý do**; và dưới ANSI (mặc định
ở Spark 4), `cast` thường **ném lỗi** (output thật):

```
org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" because it is malformed.
Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
```

**Cách hoạt động.** Đọc **mọi cột là String** (cộng cột `_corrupt_record` giữ nguyên văn dòng sai số cột), rồi tự ép kiểu bằng `try_cast` — hỏng thì NULL,
và bước validate biết **vì sao** nó NULL. **Nhưng `try_cast` rộng hơn hợp đồng:** nó nhận `"2026"` thành ngày 2026-01-01, và nhận timestamp **không có múi giờ**
rồi hiểu theo `spark.sql.session.timeZone` — cùng chuỗi `2026-10-15T00:00:00` cho hai instant lệch 7 giờ giữa phiên UTC và phiên Asia/Ho_Chi_Minh (reviewer đo
thật). Vì `updated_at` quyết định phiên bản nào thắng khi dedup, lab kiểm tra **cú pháp trước** bằng regex (`yyyy-MM-dd`; timestamp ISO-8601 **bắt buộc** có
`Z` hoặc `±HH:MM`), rồi mới `try_cast` để bắt ngày không tồn tại như 2026-02-30. Khi có offset, instant không phụ thuộc múi giờ phiên: `…T01:00:00Z` và
`…T08:00:00+07:00` là **trùng tuyệt đối** (test `review T14 M4`). Kiểu đích phải viết **tường minh** `TIMESTAMP_LTZ` (instant): `AS TIMESTAMP` chỉ là
alias theo cấu hình `spark.sql.timestampType` — phiên đặt `TIMESTAMP_NTZ` thì offset bị **bỏ qua**, hai cách viết của cùng một instant thành hai giá trị khác nhau
và dedup chọn sai phiên bản (review vòng 2 tái hiện; test regression chạy pipeline dưới phiên NTZ):

```scala
spark.read
  .option("header", "true")
  .option("enforceSchema", "false")          // so TÊN cột trong header — đổi thứ tự cột là lỗi, không tráo giá trị (Chương 23)
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .schema(rawSchema)                         // 8 cột String + _corrupt_record
  .csv("data/input")
  .filter(col("ingest_date") <= lit(java.sql.Date.valueOf(upTo)))
```

**Partition discovery.** Thư mục tên `ingest_date=2026-10-15` biến thành **cột** `ingest_date` kiểu **DATE** (Spark tự suy kiểu từ tên thư mục). Lọc trên cột đó
là **partition pruning**: Spark chỉ mở các thư mục cần. Plan thật khi `upTo = 2026-10-15`:

```
FileScan csv [order_id#13813,...,_corrupt_record#13821,ingest_date#13822] ... PartitionFilters: [isnotnull(ingest_date#13822), (ingest_date#13822 <= 2026-10-15)], ...
```

Bước 2 kiểm tra thêm: đủ 8 cột + `ingest_date`, và `ingest_date` phải là DATE — kiểu này do Spark **suy** từ tên thư mục, nên một thư mục đặt tên sai định dạng có thể đổi kiểu suy ra (Chưa kiểm chứng trên lab: kiểm tra kiểu là phòng thủ rẻ). Header sai thứ tự
→ job dừng với `CSV header does not conform to the schema` (test bước 2). **Khi không dùng RAW String:** nguồn có schema mạnh (Parquet/Avro có kiểm soát) —
đọc thẳng kiểu thật.

### 3.3. Validate, chuẩn hóa, bản ghi lỗi

Validate và chuẩn hóa nằm trong **một** `select`: `trim`, `upper` (hàm có sẵn — không UDF, Chương 23), `try_cast` từng cột. Mỗi quy tắc là một `when(vi phạm, lý do)`;
gom vào mảng rồi bỏ NULL — **mọi** lỗi của một dòng, như `ValidatedNec` ở Chương 20:

```scala
def rule(failed: Column, reason: String): Column = when(failed, lit(reason))
typed.withColumn("errors", filter(array(
  rule(col("_corrupt_record").isNotNull, "dòng sai số cột"),
  rule(col("quantity").isNull || col("quantity") < 1 || col("quantity") > 1000, "quantity: số nguyên 1..1000"),
  ...
), _.isNotNull))
```

Dòng có `errors` không rỗng ghi ra `rejected/` (partition theo `ingest_date`) với **giá trị các trường trước chuẩn hóa** (`raw` là struct 8 cột String) — người
sửa nguồn thấy họ đã gửi giá trị gì. Chú ý: đó là giá trị **sau CSV parser**, không phải từng byte gốc — dấu nháy, dấu phân cách, xuống dòng `\r\n` đã mất; dòng
trống bị parser bỏ qua và **không** được đếm vào `input` (input đếm *record mà parser trả ra*). Cần truy vết đúng byte thì giữ thêm tên file + vị trí record.
Output thật: `O5` nhận **6** lý do một lúc (mục 9).

**Lỗi thường gặp.** Lọc trên **chỉ** cột `_corrupt_record` mà không cache → Spark từ chối (lỗi thật, mục 10). Ở lab không gặp vì quy tắc tham chiếu nhiều cột.

### 3.4. Dedup hai tầng, event trễ, xung đột

1. **Trùng tuyệt đối** — cùng mọi trường nghiệp vụ và `updated_at` (file gửi lại, retry ở nguồn): `row_number` trên **mọi** trường, giữ bản 1. `ingest_date`
   **không** thuộc so sánh: cùng nội dung đến ở hai lô vẫn là một (test riêng).
2. **Phiên bản** — mỗi dòng hàng `(order_id, sku)` giữ bản `updated_at` lớn nhất; nhiều bản mới nhất **khác** nội dung → **xung đột**, cách ly vào `conflicts/`,
   không đoán (bài học review chéo Chương 23).

**Event trễ và cập nhật muộn.** Lô 16/10 chứa `O1 REFUNDED` (cập nhật cho đơn **ngày 15**) và `O8` (đơn ngày 15 đến muộn). Vì vậy pipeline không thể chỉ xử lý
"lô mới": mỗi lần chạy **tính lại từ mọi lô tới `upTo`** rồi ghi mỗi bảng thành snapshot (mục 3.7). Đánh đổi: tốn tính lại (với lịch sử dài, giới hạn "cửa sổ
tính lại" N ngày hoặc dùng table format có MERGE — Chương 26/31), đổi lại **đúng** và đơn giản.

### 3.5. Join, aggregate

- Kiểm tra `customer_id` **duy nhất** trước join (trùng → nhân doanh thu; `require` dừng job). Bảng khách nhỏ → `broadcast`: plan thật dùng
  **BroadcastHashJoin**, không shuffle bảng đơn hàng (Chương 25 đo chi tiết). Left join, khách không tồn tại → `UNKNOWN`.
- Tiền tính bằng **DECIMAL(38,0)**: khoảng giá trị tới 38 chữ số (≈ 1,08 × 10¹⁹ lần `Long.MaxValue`: (10³⁸ − 1) / (2⁶³ − 1)), ANSI vẫn bật (test: 2 × 1000 × `Long.MaxValue` ra số đúng).
  Vượt 38 chữ số thì ANSI **ném lỗi** chứ không quay vòng — DECIMAL không phải "không bao giờ tràn", chỉ là miền rộng hơn nhiều và tràn thì lỗi to (output thật,
  `sum` của 38 chữ số 9 và 1):
  `SparkArithmeticException: [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] 100000000000000000000000000000000000000 cannot be represented as Decimal(38, 0).`
- Grain đầu ra: một dòng = `(day, segment)`; `order_lines` (dòng hàng) và `orders` (`countDistinct` đơn) là hai cột khác nhau.

### 3.6. Write: định dạng, partition, small files

**Định dạng.** Parquet: cột, nén, có schema, đọc ít cột (Chương 21). CSV/JSON chỉ cho trao đổi với người/hệ ngoài.

**Partition theo cột.** `partitionBy("day")` → `fact_order_lines/day=2026-10-15/part-....parquet`. Người đọc lọc theo ngày chỉ mở một thư mục (plan thật):

```
FileScan parquet [...,day#13811] ... PartitionFilters: [isnotnull(day#13811), (day#13811 = 2026-10-16)], PushedFilters: [], ...
```

Chọn cột partition: **hay được lọc**, **ít giá trị** (ngày: vài trăm/năm). Partition theo `order_id` tạo hàng triệu thư mục — tệ hơn không partition.
Chú ý: cột partition **không nằm trong file**, nó nằm ở tên thư mục; đọc lại, Spark đưa nó xuống **cuối** schema (bảng mục 9 có `day` ở cột cuối).

**Small files problem.** Mỗi task ghi **một file cho mỗi partition nó gặp**. Dữ liệu 3 ngày rải trên 8 task (output thật):

| Cách ghi | Số file mỗi ngày |
|---|---|
| `partitionBy("day")` thẳng từ 8 task | **8, 8, 8** |
| `repartition(col("day"))` rồi `partitionBy("day")` | **1, 1, 1** |

Hàng nghìn file nhỏ làm chậm **người đọc** (mỗi file một lượt mở, metadata, footer Parquet) và nặng cho NameNode/object storage. `repartition(col("day"))` gom mỗi ngày
về một task — trả một shuffle (con số "1 file mỗi ngày" giả định `maxRecordsPerFile` = 0, mặc định: không giới hạn). **Khi không áp dụng:** một ngày rất
lớn thành một task khổng lồ (skew theo ngày). `maxRecordsPerFile` **không** chữa việc này — reviewer đo thật: 100 dòng cùng ngày, `maxRecordsPerFile=10` →
10 file nhưng vẫn **một** task ghi. Nó chỉ giới hạn kích thước file. Chia công việc của ngày lớn cần `repartition(n, day, khóa phụ)` (đổi lại nhiều file hơn
cho ngày đó), hoặc compaction định kỳ (lời giải bài 6).

### 3.7. Idempotency và rerun safety: ba chế độ ghi

**Idempotent**: chạy N lần với cùng input → cùng output như chạy 1 lần. Pipeline sẽ bị chạy lại (lỗi giữa chừng, sửa bug, backfill) — nên phải idempotent.

Thí nghiệm thật: ghi dữ liệu 3 ngày (10.000 dòng), chạy lại y hệt, rồi chạy lại **chỉ** ngày 16:

| Chế độ | Sau lần 1 | Sau chạy lại | Thư mục còn lại sau chạy lại riêng ngày 16 |
|---|---|---|---|
| `append` | 10.000 | **20.000** — nhân đôi | 15, 16, 17 (ngày 16 giờ có 3 bản) |
| `overwrite` tĩnh (mặc định) | 10.000 | 10.000 | **chỉ 16** — xóa mất 15 và 17 |
| `overwrite` + `partitionOverwriteMode=dynamic` | 10.000 | 10.000 | 15, 16, 17 — chỉ thay ngày 16 |

**Giới hạn của dynamic overwrite.** Nó chỉ thay partition **có** trong dữ liệu mới. Nếu tính lại làm một ngày **biến mất**, partition cũ **vẫn nằm đó** với số
liệu cũ. Review chéo tái hiện ba cách điều này xảy ra thật trên phiên bản đầu của lab: (1) lô sau thêm bản CANCELLED **cùng** `updated_at` → cả hai thành xung
đột, ngày đó không còn fact nhưng fact/doanh thu cũ vẫn được phục vụ; (2) hoàn tiền muộn → fact còn (REFUNDED) nhưng daily ngày đó không còn dòng PAID, partition
daily cũ còn nguyên; (3) chạy `upTo=16` rồi `upTo=15` cùng output → báo cáo 21.600.000 nhưng đọc cả bảng ra **22.350.000** (ngày 16 cũ còn đó). Tệ hơn,
bước 10 lúc đó chỉ so các ngày *có trong dữ liệu mới* nên vẫn báo OK.

**Lab dùng `writeSnapshot`:** ghi động, **rồi xóa** các partition không còn trong kết quả (partition mồ côi) — cho cả fact, daily và rejected:

```scala
def writeSnapshot(spark: SparkSession, df: DataFrame, path: String, part: String): Set[String] = {
  val expected = df.select(col(part).cast("string")).distinct().collect().map(_.getString(0)).toSet
  writePartitioned(df, path, part)                                   // overwrite + partitionOverwriteMode=dynamic
  val orphans = partitionsOnDisk(spark, path, part) -- expected       // Hadoop FileSystem API: local, HDFS, S3A
  orphans.foreach(v => fs.delete(new HPath(path, s"$part=$v"), true))
  orphans
}
```

Vì sao không dùng overwrite **tĩnh** (xóa cả bảng rồi ghi)? Overwrite tĩnh có một khoảng thời gian bảng **trống**, và job chết giữa chừng là mất cả bảng. Cách
trên giữ mỗi partition ở bản cũ hoặc bản mới; partition mồ côi chỉ bị xóa **sau** khi ghi xong. `RunReport.removedPartitions` liệt kê chúng để người vận hành
thấy. Kết quả thật: chạy lần 2 và lần 3 (y hệt) cho cùng bảng và cùng số file (test `rerun safety`); ba ca trên có regression test (test `review T14 M2`).

Vẫn **không nguyên tử** giữa các partition và giữa các bảng: người đọc giữa lúc ghi có thể thấy ngày 15 mới + ngày 16 cũ → mẫu **write–audit–publish** (lời giải
bài 4, Project 3) hoặc table format có transaction (Delta Lake, Apache Iceberg — Chương 31). Và vì mỗi lần chạy tính lại **toàn bộ** lịch sử, cách này hợp với
bảng vừa phải; bảng lớn tính lại theo **phạm vi ngày bị chạm**, và phạm vi đó phải tính từ cả trạng thái cũ lẫn input mới (Chương 31).

### 3.8. Kiểm tra kết quả, plan, shuffle, skew

Bước 10 **đọc lại từ đĩa** (không tin dữ liệu trong bộ nhớ) và so **nội dung**, trên **toàn bộ** bảng (output là snapshot):

1. Fact đọc lại **bằng** fact đã tính — so multiset bằng `exceptAll` hai chiều, không chỉ số dòng; không trùng `(order_id, sku)`.
2. Daily đọc lại **bằng** daily **tính lại từ fact đọc lại**; grain `(day, segment)` duy nhất; tổng doanh thu khớp.
3. `rejected` và `conflicts` đọc lại đủ số dòng — bản ghi cách ly cũng là output.

Bảng đọc lại bằng **schema tường minh**: bảng rỗng không có file Parquet nào để Spark suy schema (`UNABLE_TO_INFER_SCHEMA`, mục 10), nhưng rỗng là kết quả hợp lệ.

Phiên bản đầu chỉ so số dòng + tổng; reviewer sửa **mọi** giá trị trong output mà giữ nguyên số dòng và tổng (giá = 1; segment = WRONG; xóa cả thư mục
`rejected/`) và bước 10 vẫn báo OK. Giờ cả ba bị bắt (test `review T14 M3`). Bài học: **kiểm tra tổng có điểm mù chung** — thêm nhiều kiểm tra tổng nữa
không chữa được, phải so nội dung (hoặc tổng kiểm theo từng partition: count + sum + hash, khi bảng quá lớn để `exceptAll` — một shuffle trên cả bảng).
Test cố ý `append` thêm một bản `O2` → báo "thiếu 0, thừa 1" và khóa trùng. Kiểm tra **sau** khi ghi nghĩa là output sai đã nằm trên đĩa; muốn chặn trước khi
người đọc thấy → ghi vào staging, kiểm tra, rồi publish (lời giải bài 4, Project 3).

**Shuffle trong pipeline:** window dedup (theo khóa dòng hàng), `groupBy`, `repartition(day)` trước khi ghi. Dữ liệu nhỏ → `spark.sql.shuffle.partitions = 4`
thay vì 200 (Chương 22). **Skew:** nếu một ngày hoặc một khách chiếm phần lớn dữ liệu, task đó quyết định thời gian job — đo và xử lý ở Chương 25 (AQE skew join,
salting). **Đọc plan** cho từng quyết định: `PartitionFilters` (pruning có xảy ra?), `BroadcastHashJoin` (broadcast có được dùng?), số `Exchange`.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Batch pipeline | Xử lý dữ liệu theo lô, định kỳ |
| Raw / staging / serving layer | Lớp dữ liệu thô / trung gian / phục vụ người đọc |
| Partition discovery | Spark suy cột từ tên thư mục `cột=giá trị` |
| Partition pruning, `PartitionFilters` | Chỉ đọc thư mục partition thỏa điều kiện |
| `try_cast` | Ép kiểu, hỏng thì NULL thay vì lỗi |
| Rejected / quarantine | Bản ghi bị loại / bị cách ly chờ xử lý |
| Late event, backfill | Sự kiện đến muộn / chạy lại cho dữ liệu quá khứ |
| Idempotency, rerun safety | Chạy nhiều lần = chạy một lần / chạy lại an toàn |
| Dynamic partition overwrite | Ghi đè chỉ các partition có trong dữ liệu mới |
| Small files problem, compaction | Quá nhiều file nhỏ / gộp file nhỏ thành file lớn |
| Reconciliation | Đối soát số dòng/tổng giữa các bước |
| Write–audit–publish | Ghi vào staging, kiểm tra, rồi mới công bố |

## 5. Ví dụ đơn giản

```scala
val raw       = Pipeline.readRaw(spark, "data/input", LocalDate.parse("2026-10-16"))
val validated = Pipeline.validate(raw)
Pipeline.rejected(validated).select(col("raw.order_id"), col("errors")).show(truncate = false)
Pipeline.accepted(validated).groupBy("ingest_date").count().show()
```

Bốn dòng đã cho thấy nguyên tắc chính: dòng hỏng không biến mất, nó sang một bảng khác **kèm lý do**.

## 6. Ví dụ thực tế — tính tay hai lô đơn hàng

**Lô 2026-10-15** (7 dòng): `O1` KB (sku `" kb-01 "`, khách `c1` — cần chuẩn hóa) 2 × 450.000; `O1` MS 1 × 250.000; `O2` 50 × 400.000 (`paid` chữ thường) **hai lần**
y hệt; `O3` quantity 0 ❌; `O4` khách C9 không tồn tại 450.000; `O5` hỏng 6 trường ❌.
**Lô 2026-10-16** (6 dòng): `O1` KB **REFUNDED** (cập nhật đơn ngày 15); `O6` 3 × 250.000; `O7` PAID và `O7` CANCELLED **cùng giờ** (xung đột); `O8` đơn **ngày 15**
đến muộn 450.000; `O9` quantity `abc` ❌.

Sau cả hai lô: input 13 = rejected 3 (O3, O5, O9) + accepted 10. Accepted 10 = trùng tuyệt đối 1 (O2) + bị thay thế 1 (O1 KB PAID) + xung đột 2 (O7) + fact 6.
Fact PAID: O1 MS (Retail), O2 (Wholesale), O4 (UNKNOWN), O6 (Wholesale, 16/10), O8 (Retail). O1 KB đã REFUNDED → không tính.

| Ngày | Segment | Doanh thu | Dòng hàng | Đơn |
|---|---|---|---|---|
| 15/10 | Retail | 250.000 + 450.000 = **700.000** | 2 | 2 (O1, O8) |
| 15/10 | UNKNOWN | **450.000** | 1 | 1 |
| 15/10 | Wholesale | **20.000.000** | 1 | 1 |
| 16/10 | Wholesale | **750.000** | 1 | 1 |

Tổng 21.900.000. Chỉ với lô 15: Retail = 900.000 + 250.000 = **1.150.000**, tổng 21.600.000. Lô 16 làm **giảm** doanh thu ngày 15 (hoàn tiền) và **tăng** nó (đơn
trễ) — chỉ đúng nếu partition ngày 15 được tính lại. Output thật ở mục 9 khớp từng số.

## 7. Code đầy đủ

```
09-spark/batch-pipeline-lab/                    ← BUILD RIÊNG, Scala 2.13.18
├── project.scala / build.sbt / project/build.properties
├── data/input/ingest_date=2026-10-15/orders.csv, data/input/ingest_date=2026-10-16/orders.csv, data/customers.csv
├── src/main/scala/vn/scalaeng/batch/
│   ├── Pipeline.scala      ← 10 bước, RunReport, run, verify
│   ├── Experiments.scala   ← small files, ba chế độ ghi, partition pruning, chiến lược join
│   └── PipelineDemo.scala  ← session + chạy 3 lần + in mọi thí nghiệm
└── src/test/scala/vn/scalaeng/batch/PipelineSuite.scala   ← 18 test (4 test regression từ review chéo)
```

```bash
cd 09-spark/batch-pipeline-lab
scala-cli test .                       # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.batch.PipelineDemo
```

## 8. Giải thích code

- **`rawSchema`** — 8 cột String + `_corrupt_record`; không khai báo `ingest_date` (partition discovery thêm nó).
- **`validate`** — `struct(Columns.map(col): _*).as("raw")` giữ giá trị trường trước chuẩn hóa; `when(trim(col("day")).rlike(DayPattern), expr("try_cast(...)"))`
  — kiểm tra cú pháp rồi mới ép kiểu; `filter(array(...), _.isNotNull)` là higher-order function của Spark SQL (chạy trong engine, không phải lambda Scala).
- **`dedup`** — `Deduped(latest, conflicts, exactDuplicates, superseded)`: bốn ngăn, mỗi ngăn đếm được; `collect_set(when(is_top, struct(payload)))` đếm số
  nội dung khác nhau **chỉ** trong các bản mới nhất.
- **`enrich`** — kiểm tra khóa trùng bằng một action riêng (`collect` tối đa 3 khóa để báo lỗi), rồi `broadcast(...)`.
- **`writePartitioned` / `writeSnapshot`** — `repartition(col(part))` + `partitionOverwriteMode=dynamic` + `partitionBy(part)`, rồi xóa partition mồ côi bằng
  Hadoop `FileSystem` (cùng API cho local, HDFS, S3A — không dùng `java.nio.file`, vốn chỉ hiểu đĩa cục bộ).
- **`run`** — `cache()` `validated` và `facts` vì mỗi cái được nhiều nhánh đọc (ghi + đếm); `unpersist` trong `finally`.
- **`verify`** — đọc lại mọi bảng với schema tường minh (`readBack`; thư mục mất → coi là bảng rỗng để báo "thiếu N dòng" thay vì ném lỗi); `exceptAll`
  hai chiều cho fact; daily so với `dailyRevenue(fact đọc lại)`; đếm lại rejected/conflicts. Trả `List[String]` — rỗng là đạt.

## 9. Output mong đợi (output thật)

`PipelineDemo` (rút gọn phần bảng lặp lại):

```
== Lần 1: chỉ lô 2026-10-15
   input=7 rejected=2 accepted=5 trùng=1 thay-thế=0 xung-đột=0 fact=4 doanh-thu-PAID=21600000
   kiểm tra: OK
+---------+-----------+-----------+------+----------+
|segment  |revenue_vnd|order_lines|orders|day       |
+---------+-----------+-----------+------+----------+
|Retail   |1150000    |2          |1     |2026-10-15|
|UNKNOWN  |450000     |1          |1     |2026-10-15|
|Wholesale|20000000   |1          |1     |2026-10-15|
+---------+-----------+-----------+------+----------+

== Lần 2: lô 15 + 16 (hoàn tiền O1 và đơn trễ O8 sửa partition ngày 15)
   input=13 rejected=3 accepted=10 trùng=1 thay-thế=1 xung-đột=2 fact=6 doanh-thu-PAID=21900000
   kiểm tra: OK
+---------+-----------+-----------+------+----------+
|segment  |revenue_vnd|order_lines|orders|day       |
+---------+-----------+-----------+------+----------+
|Retail   |700000     |2          |2     |2026-10-15|
|UNKNOWN  |450000     |1          |1     |2026-10-15|
|Wholesale|20000000   |1          |1     |2026-10-15|
|Wholesale|750000     |1          |1     |2026-10-16|
+---------+-----------+-----------+------+----------+

== Lần 3: chạy LẠI y hệt lần 2
   input=13 rejected=3 accepted=10 trùng=1 thay-thế=1 xung-đột=2 fact=6 doanh-thu-PAID=21900000
   kiểm tra: OK
   (bảng giống hệt lần 2)

== Dòng bị loại (giá trị trường + lý do)
|2026-10-15 |O3      |[quantity: số nguyên 1..1000]
|2026-10-15 |O5      |[customer_id: bắt buộc, day: phải là ngày yyyy-MM-dd, status: phải là một trong PAID/REFUNDED/CANCELLED, sku: bắt buộc,
                       unit_price_vnd: số nguyên dương, updated_at: phải là timestamp ISO-8601 có múi giờ (Z hoặc ±HH:MM)]
|2026-10-16 |O9      |[quantity: số nguyên 1..1000]

== Xung đột phiên bản (cách ly, không tính doanh thu)
|O7      |HP-01|CANCELLED|2026-10-16 07:00:00|
|O7      |HP-01|PAID     |2026-10-16 07:00:00|

== File layout của fact_order_lines
   day=2026-10-15: 1 file
   day=2026-10-16: 1 file
== Lần 4: GIẢM upTo về 2026-10-15 trên cùng output (quay về snapshot lần 1)
   input=7 rejected=2 accepted=5 trùng=1 thay-thế=0 xung-đột=0 fact=4 doanh-thu-PAID=21600000
   kiểm tra: OK
   (bảng giống hệt lần 1: Retail 1150000, UNKNOWN 450000, Wholesale 20000000 — chỉ ngày 2026-10-15)
   partition đã xóa: daily_revenue/day=2026-10-16, fact_order_lines/day=2026-10-16, rejected/ingest_date=2026-10-16
== Chiến lược join: BroadcastHashJoin
== Small files: 8 task ghi thẳng      -> List((day=2026-10-15,8), (day=2026-10-16,8), (day=2026-10-17,8))
== Small files: repartition(day) trước -> List((day=2026-10-15,1), (day=2026-10-16,1), (day=2026-10-17,1))
== Ghi chế độ append: ModeResult(10000,20000,Set(day=2026-10-15, day=2026-10-16, day=2026-10-17))
== Ghi chế độ static: ModeResult(10000,10000,Set(day=2026-10-16))
== Ghi chế độ dynamic: ModeResult(10000,10000,Set(day=2026-10-15, day=2026-10-16, day=2026-10-17))
```

Plan pruning ở mục 3.2 và 3.6. Số `#13811` và đường dẫn thư mục tạm thay đổi mỗi lần chạy. Lần 4 là ngữ nghĩa snapshot (mục 3.1): trước khi sửa theo review chéo, lần chạy này báo 21.600.000 nhưng bảng vẫn còn ngày 16 cũ. **Test:** `PipelineSuite 18/18`; `sbt test`: `Passed: Total 18, Failed 0`.

## 10. Lỗi thường gặp (thật)

Các thông báo dưới đây là output thật, tái hiện có chủ đích trên lab này (Spark 4.2.0):

**1. `cast` thường trên dữ liệu bẩn dưới ANSI:**

```
org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" because it is malformed.
Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
```

Một dòng `O9` làm hỏng cả job. Dùng `try_cast` **và** ghi lý do (mục 3.3) — đừng tắt ANSI: nó cũng tắt kiểm tra tràn số.

**2. Truy vấn chỉ cột `_corrupt_record`:**

```
org.apache.spark.sql.AnalysisException: [UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN] The feature is not supported: Queries from raw JSON/CSV/XML files
are disallowed when the referenced columns only include the internal corrupt record column
```

Spark cần parse cả dòng mới biết nó hỏng; với column pruning, query chỉ cần cột đó sẽ không parse gì. `cache()` dữ liệu trước, hoặc tham chiếu thêm cột khác.

**3. Ghi lần hai vào cùng đường dẫn với chế độ mặc định:**

```
org.apache.spark.sql.AnalysisException: [PATH_ALREADY_EXISTS] Path file:/C:/Users/.../x already exists. Set mode as "overwrite" to overwrite the existing path. SQLSTATE: 42K04
```

Mặc định `errorifexists` an toàn — nhưng đừng "sửa" bằng `append` (nhân đôi khi chạy lại) hay `overwrite` tĩnh (xóa partition khác): mục 3.7.

**4. `partitionBy` cột không có trong dữ liệu:**

```
org.apache.spark.sql.AnalysisException: Partition column `day` not found in schema struct<id:bigint>.
```

**5. Đọc lại một bảng rỗng không kèm schema** (phiên bản đầu của bước 10, review T14 M1; tái hiện lại khi viết regression):

```
org.apache.spark.sql.AnalysisException: [UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Parquet. It must be specified manually. SQLSTATE: 42KD9
```

Ghi một DataFrame rỗng có `partitionBy` không tạo file Parquet nào — chỉ `_SUCCESS`. Bảng rỗng là kết quả **hợp lệ** (mọi dòng bị loại, chỉ có đơn REFUNDED,
`upTo` trước mọi lô), nên `spark.read.schema(schemaĐãBiết).parquet(path)` — đừng biến "không có dữ liệu" thành job thất bại.

**6. Đọc một bảng mà thư mục đã bị xóa** (khi viết test M3):

```
org.apache.spark.sql.AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/C:/Users/VuTT/AppData/Local/Temp/ch24-tamper310521714050189021019/rejected. SQLSTATE: 42K03
```

Bước kiểm tra không được **ném** lỗi đúng lúc nó cần **báo** lỗi: `readBack` kiểm tra thư mục tồn tại, thiếu thì đọc như bảng rỗng → verify báo
`rejected đọc lại 0 dòng != đã tính 3`.

**7. Kiểm tra báo OK nhưng output sai** — không có exception nào, đó mới là lỗi nguy hiểm nhất: partition mồ côi sau dynamic overwrite, và verify chỉ so số
dòng + tổng (mục 3.7, 3.8). Chỉ test **tấn công** (sửa output có chủ đích rồi xem kiểm tra có đỏ không) mới tìm ra loại lỗi này.

**8. Header lệch cột:** `CSV header does not conform to the schema` (test bước 2) — xem mục 3.2. **9. Append khi chạy lại** — nhân đôi dòng (mục 3.7), bước 10
bắt được (test `đối soát phát hiện output bị ghi thêm`). **10. Small files** — mục 3.6.

**11. Windows: cạn cổng loopback khi chạy nhiều truy vấn** (gặp thật khi chạy suite 18 test bằng `sbt test`):

```
org.apache.spark.executor.RemoteClassLoaderError: org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage5
...
Caused by: java.io.IOException: Unable to establish loopback connection
	at java.base/sun.nio.ch.PipeImpl$Initializer.run(PipeImpl.java:103)
	...
	at org.apache.spark.executor.ExecutorClassLoader.getClassFileInputStreamFromSparkRPC(ExecutorClassLoader.scala:131)
```

Nguyên nhân (đọc mã nguồn Spark 4.2.0 `ArtifactManager` + `Executor.addReplClassLoaderIfNeeded`, rồi đo): Spark 4 bật mặc định
`spark.sql.artifact.isolation.enabled=true` — cô lập artifact theo session, phục vụ Spark Connect. Mỗi truy vấn SQL chạy kèm class URI của session, nên executor
dùng `ExecutorClassLoader`: class không thấy ở classpath (Janino thử cả những tên như `expressions.Object` khi biên dịch codegen) được hỏi lại driver qua RPC, mỗi
lần mở một `Pipe` — trên Windows là **một cặp socket loopback**. Số đo thật trên máy này, cùng 60 truy vấn `groupBy`: mặc định **+1.650 socket TIME_WAIT**
(~27 socket mỗi truy vấn); `spark.sql.artifact.isolation.enabled=false` → **không tăng**, và nhanh hơn ~12% (27,1 s → 23,9 s). Socket TIME_WAIT tồn tại vài
phút, nên một suite dài (hoặc nhiều JVM Spark chạy cùng lúc) làm cạn dải cổng động (16.384 cổng) và task bắt đầu lỗi ngẫu nhiên.

Lab tắt cô lập trong `PipelineDemo.session()`: job batch chỉ có **một** session, không cần cách ly artifact giữa các session. Đây là cấu hình được đánh dấu
`internal` trong Spark 4.2 — có thể đổi ở phiên bản sau; đừng tắt nó trên cụm dùng chung cho Spark Connect nhiều người dùng. Kiểm tra nhanh trên Windows:
`netstat -an | find /c "TIME_WAIT"` trước và sau một lần chạy.

## 11. Cách debug

- **Số dòng không khớp:** in `RunReport` — ngăn nào lệch? Đếm `rejected` theo lý do (lời giải bài 2).
- **Một ngày sai:** đọc riêng partition đó (`filter(col("day") === ...)` — có pruning), so với tính tay trên các dòng input của ngày đó **ở mọi lô**.
- **Chạy lại cho kết quả khác:** kiểm tra chế độ ghi; liệt kê thư mục partition và số file (`Experiments.filesPerPartition`); đọc `RunReport.removedPartitions`
  — partition nào vừa bị xóa vì không còn dữ liệu.
- **Chậm:** `explain("formatted")` — có `PartitionFilters`? có `BroadcastHashJoin`? bao nhiêu `Exchange`? Số file đầu vào? Rồi sang Chương 25.

## 12. Bài tập

1. **(Cơ bản)** Thêm quy tắc **liên trường**: `updated_at` không được trước ngày đặt hàng `day`. Không cộng lỗi "phái sinh" cho dòng đã sai kiểu.
2. **(Cơ bản)** Báo cáo chất lượng theo lô: số lần vi phạm theo từng lý do, mỗi `ingest_date`.
3. **(Trung bình)** Quality gate theo lô: dừng (không ghi gì) nếu tỷ lệ dòng bị loại của một lô vượt ngưỡng; ngưỡng NaN/ngoài [0, 1] là lỗi cấu hình.
4. **(Trung bình)** Write–audit–publish: ghi vào staging, kiểm tra, đổi tên sang thư mục phục vụ; kiểm tra thất bại thì bản cũ giữ nguyên.
5. **(Nâng cao)** Tái hiện **partition mồ côi** của dynamic overwrite **thuần** (`writePartitioned`, không dọn), rồi viết hàm ghi + dọn partition mồ côi **chỉ trong
   phạm vi** lần tính lại (các ngày input chạm tới) — phiên bản cho tính lại tăng dần; lab chính tính lại toàn bộ nên dùng `writeSnapshot` dọn cả bảng.
6. **(Nâng cao)** Compaction: gộp một partition 8 file nhỏ thành 2 file, chứng minh nội dung (multiset) không đổi.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch24-solutions.md`](../solutions/ch24-solutions.md)
- Code + test (6 test): [`solutions/ch24-code/`](../solutions/ch24-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao lớp RAW đọc mọi cột là String?
2. `try_cast` khác `cast` thế nào dưới ANSI?
3. Partition discovery là gì? Làm sao biết pruning đã xảy ra?
4. Vì sao pipeline tính lại từ mọi lô thay vì chỉ lô mới?
5. Trùng tuyệt đối khác "bị thay thế" khác "xung đột" thế nào?
6. So sánh `append`, overwrite tĩnh, overwrite động khi chạy lại.
7. Small files problem là gì? Hai cách xử lý và cái giá của mỗi cách?
8. Bước kiểm tra kết quả đối soát những gì? Vì sao phải đọc lại từ đĩa?

<details><summary>Gợi ý đáp án</summary>

1. Để lỗi kiểu thành lý do có cấu trúc thay vì NULL âm thầm hoặc dừng cả job.
2. `cast` ném `CAST_INVALID_INPUT`; `try_cast` trả NULL.
3. Thư mục `cột=giá trị` thành cột; plan có `PartitionFilters` không rỗng.
4. Lô mới có thể cập nhật/bổ sung dữ liệu của ngày cũ (hoàn tiền, event trễ).
5. Cùng mọi trường / cùng dòng hàng nhưng cũ hơn / nhiều bản mới nhất khác nội dung.
6. Nhân đôi / xóa partition khác / chỉ thay partition có trong dữ liệu mới (nhưng để lại partition mồ côi).
7. Quá nhiều file nhỏ làm chậm người đọc; `repartition` theo cột partition (thêm shuffle, có thể skew) hoặc compaction định kỳ (thêm job).
8. input = rejected + accepted; accepted = các ngăn dedup + fact; không trùng khóa; tổng tiền. Đĩa là thứ người đọc thấy.
</details>

## 15. Mini project

Thêm **lô thứ ba** `ingest_date=2026-10-17` với: một dòng hoàn tiền cho `O2`, một bản sửa giải quyết xung đột `O7` (`updated_at` muộn hơn), một file có header đổi
thứ tự cột. Dự đoán (tính tay) `RunReport` và bảng doanh thu, rồi chạy. Quyết định: lô có header sai nên dừng cả pipeline hay chỉ bỏ lô đó? Viết test cho quyết định
của bạn. **Project 3** mở rộng pipeline này thành ứng dụng có cấu hình, manifest lần chạy và write–audit–publish.

## 16. Nguồn tài liệu chính thức

- CSV data source (header, `enforceSchema`, `mode`, `columnNameOfCorruptRecord`): https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html
- Partition discovery: https://spark.apache.org/docs/4.2.0/sql-data-sources-parquet.html#partition-discovery
- `partitionOverwriteMode`, ANSI: https://spark.apache.org/docs/4.2.0/configuration.html , https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html
- `try_cast` và higher-order functions (`filter`): https://spark.apache.org/docs/4.2.0/api/sql/index.html
- `DataFrameWriter` (mode, partitionBy): https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/DataFrameWriter.html

## 17. Checklist tự đánh giá

- [ ] Vẽ được 10 bước và hợp đồng đối soát; mọi dòng input vào đúng một ngăn.
- [ ] Đọc dữ liệu thô bằng RAW String + `try_cast` + lý do lỗi; kiểm tra header.
- [ ] Dedup hai tầng và xử lý event trễ bằng tính lại partition.
- [ ] Chọn cột partition, tránh small files, đọc `PartitionFilters` trong plan.
- [ ] Giải thích ba chế độ ghi; biết giới hạn của dynamic overwrite.
- [ ] Viết bước kiểm tra đọc lại output.
- [ ] Chạy được 18 test của lab và 6 test lời giải.
