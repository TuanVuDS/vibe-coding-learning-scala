# Chương 23 — Spark SQL và DataFrames

> **Phiên bản:** **Scala 2.13.18** · JDK 21 · Apache Spark **4.2.0** · munit 1.2.4
> **Code:** [`spark-sql-lab/`](spark-sql-lab/) — schema tường minh vs suy luận, DataFrame vs Dataset, join (inner/left/semi/anti), window (bản mới nhất, tổng lũy kế),
> NULL ba trị, UDF vs hàm có sẵn, Catalyst (đẩy điều kiện, cắt cột)
> **Trạng thái kiểm chứng:** **18/18 test pass** (`scala-cli test .` và `sbt test`), lời giải 7/7, Spark 4.2.0 local mode trên Windows 11 + JDK 21 (17/10/2026).
> Mọi schema, bảng kết quả, kế hoạch thực thi và thông báo lỗi trong bài là output thật. Đã qua review chéo (Codex): 7 MAJOR + 6 MINOR đã sửa,
> mỗi lỗi có test hồi quy (mục 10).

---

## Vị trí trong lộ trình

Chương 22 dạy mô hình thực thi (partition, shuffle, stage, lineage). Chương này dạy **API bạn dùng hằng ngày**: DataFrame và Spark SQL, và **Catalyst** — bộ tối ưu
biến truy vấn của bạn thành kế hoạch hiệu quả. Hiểu Catalyst là hiểu vì sao cùng một kết quả có thể nhanh gấp nhiều lần tùy cách viết. Chương 24 dùng tất cả để xây
pipeline batch 10 bước.

## 1. Mục tiêu học tập

1. Đọc dữ liệu với **schema tường minh**; giải thích rủi ro của `inferSchema`; chọn chế độ `FAILFAST`/`PERMISSIVE`.
2. Phân biệt **DataFrame** (`Dataset[Row]`), **Dataset[T]** typed và **Spark SQL**; biết đánh đổi của từng loại.
3. Dùng column expression, `select`, `filter`, `groupBy`/`agg`, các loại **join** (inner, left, left_semi, left_anti).
4. Dùng **window function**: bản mới nhất theo khóa, tổng lũy kế, top-N mỗi nhóm.
5. Hiểu **logic ba trị** của NULL: `=`, `<=>`, `isNull`, và cái bẫy của `!=`.
6. Biết vì sao **UDF** và lambda của Dataset là hộp đen với Catalyst; đọc `PushedFilters` và `ReadSchema` trong plan.
7. Đọc kế hoạch logic đã tối ưu: đẩy filter xuống dưới join, cắt cột.

## 2. Kiến thức tiên quyết

Chương 22 (Spark local, plan, `Exchange`, Scala 2.13), Chương 21 (Parquet, schema, nullable), Chương 15 (SQL: join, group by, window cơ bản), Chương 20 (validation, Monoid).

## 3. Lý thuyết

### 3.1. Schema: tường minh hay để Spark đoán

```scala
// RÚT GỌN — schema đầy đủ 8 cột, ĐÚNG THỨ TỰ trong file, ở `Schemas.orders` (SqlLab.scala)
val orders: StructType = StructType(Seq(
  StructField("order_id", StringType, nullable = false),
  StructField("customer_id", StringType, nullable = false),
  StructField("day", DateType, nullable = false),
  // status, sku, quantity (IntegerType), unit_price_vnd (LongType, nullable), updated_at (TimestampType)
))
spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "FAILFAST").schema(orders).csv(path)
```

**`enforceSchema = false` — kiểm tra header.** Mặc định (`true`) Spark áp schema **theo vị trí** và bỏ qua tên trong header. Review chéo thử file đổi chỗ hai cột đầu
(`customer_id,order_id,...`): đọc **thành công**, `order_id = C1`, `customer_id = X` — tráo giá trị âm thầm; cả hai là String nên FAILFAST không cứu. Với
`enforceSchema = false` (output thật):

```
Caused by: org.apache.spark.SparkIllegalArgumentException: CSV header does not conform to the schema.
```

`inferSchema = true` khiến Spark **đọc dữ liệu để đoán kiểu**. Trên `data/orders.csv`, Spark đoán `unit_price_vnd` là **integer** vì các giá trị hiện có vừa `Int`:

```
 |-- unit_price_vnd: integer (nullable = true)      ← đoán
 |-- unit_price_vnd: long (nullable = true)         ← schema tường minh
```

Ngày mai có một đơn 3 tỷ đồng → file mới được đoán là `long` → hai ngày dữ liệu **khác schema**, ghi Parquet ra hai kiểu, job đọc lại có thể lỗi. Đoán kiểu còn tốn một lượt
đọc dữ liệu. **Quy tắc:** pipeline production luôn dùng schema tường minh; `inferSchema` chỉ để khám phá dữ liệu.

**Bẫy: nguồn file bỏ qua `nullable = false`.** Schema khai báo `order_id` không null, nhưng DataFrame đọc lên có **mọi** cột `nullable = true`
(test `nguồn file BỎ QUA nullable = false`). Spark không tin file tuân thủ ràng buộc, nên "không null" phải được **kiểm tra** (quality rule — Chương 20), không thể
chỉ khai báo.

**Chế độ đọc:**

| `mode` | Dòng không khớp schema | Dùng khi |
|---|---|---|
| `PERMISSIVE` (mặc định) | Trường hỏng thành `null`, job "thành công" | Có cột `_corrupt_record` để giữ dòng hỏng (lời giải bài 1) |
| `DROPMALFORMED` | Bỏ dòng, **không báo** | Hầu như không nên — mất dữ liệu âm thầm |
| `FAILFAST` | Job lỗi khi **parse** trường hỏng | Dữ liệu bẩn là sự cố cần dừng |

Test: dòng có `quantity = "hai"` → `FAILFAST` lỗi khi `collect()`; `PERMISSIVE` cho `quantity = null` (các trường parse được vẫn giữ giá trị) và job chạy tiếp —
một con số sai sẽ đi vào báo cáo nếu không ai kiểm tra.

**FAILFAST không phải quality gate cho cả file.** CSV có **column pruning**: Spark chỉ parse cột mà query cần. Cùng file `quantity = "hai"`, cùng reader FAILFAST
(output thật của test `review M6`): `count()` → **1**, `select("order_id").collect()` → `[X]` — đều "xanh"; chỉ `collect()` đủ cột mới ném
`MALFORMED_RECORD_IN_PARSING ... NumberFormatException: For input string: "hai"`. Muốn kiểm tra toàn file, phải có một bước **thật sự đọc mọi trường bắt buộc**
(và tách lỗi parse khỏi quality rule như Chương 20).

### 3.2. DataFrame, Dataset, SQL

| | DataFrame (`Dataset[Row]`) | `Dataset[OrderRow]` typed | Spark SQL |
|---|---|---|---|
| Kiểm tra tên cột | Lúc chạy | **Lúc biên dịch** (`_.sku`) | Lúc chạy |
| Catalyst tối ưu biểu thức | Có | Chỉ với `Column`; **lambda là hộp đen** | Có |
| Chi phí | Thấp | Có thể tốn (de)serialize đối tượng | Thấp |
| Hợp với | Hầu hết pipeline | Logic nghiệp vụ phức tạp, cần kiểu | Người quen SQL, truy vấn ad-hoc |

DataFrame và SQL cho **cùng** kế hoạch (lời giải bài 2 so `optimizedPlan` của hai cách: giống hệt). Chọn theo người đọc code, không theo hiệu năng.

`Dataset[OrderRow]` cần `Encoder` — có sẵn cho case class qua `import spark.implicits._`. Trường khai báo `Option` nhận NULL thành `None`
(`unit_price_vnd: Option[Long]` → O5 cho `None`). Encoder hỗ trợ cả `java.time.LocalDate` và `Instant`.

**"An toàn kiểu" không phải "an toàn null".** Encoder **không** tự biến cột nullable thành `Option`:
- `quantity: Int` không nhận NULL. Dòng quantity NULL → `.as[OrderRow].collect()` ném (output thật):
  `[NOT_NULL_ASSERT_VIOLATION] NULL value appeared in non-nullable field: - field (class: "int", name: "quantity") - root class: "vn.scalaeng.sparksql.OrderRow"`.
- `sku: String` thì **nhận** `null` của JVM. Bản đầu lambda `_.sku.startsWith("KB")` ném `NullPointerException` với sku NULL, trong khi hàm có sẵn, UDF và
  Dataset + Column trả 0 dòng (review chéo). Sửa: `o.sku != null && ...`.
Dữ liệu phải qua quality gate **trước** `.as[T]`, hoặc khai báo `Option` cho trường được phép thiếu.

### 3.3. Join

```scala
orders.join(customers.select("customer_id", "segment"), Seq("customer_id"), "left")   // giữ mọi đơn
orders.join(customers, Seq("customer_id"), "left_anti")                               // đơn KHÔNG có khách: mồ côi
orders.join(customers, Seq("customer_id"), "left_semi")                               // đơn CÓ khách, không thêm cột
```

**Inner join làm mất dữ liệu âm thầm.** Đơn `O4` của khách `C9` không tồn tại: inner join bỏ nó, doanh thu 450.000 "biến mất". Lab dùng **left join** và gom vào
`UNKNOWN` — số liệu không mất, và có người nhìn thấy để sửa nguồn. `left_anti` liệt kê đúng các bản ghi mồ côi (kết quả: `O4`).

**Left join không bảo vệ chiều ngược lại: khóa trùng NHÂN dữ liệu.** Review chéo nhân đôi dòng C2 trong `customers`: Wholesale **41.500.000** thay vì 20.750.000.
Left join giữ dòng không khớp, nhưng mỗi dòng khớp **hai** lần thì đơn bị đếm hai lần. Trước join với bảng dimension, kiểm tra khóa **duy nhất**
(`SqlLab.duplicateCustomerIds`); `revenueBySegment` từ chối chạy nếu có trùng (`require` → `IllegalArgumentException` nêu tên khóa). Không `dropDuplicates` để
che — hai dòng C2 khác segment là lỗi dữ liệu cần người quyết định. Đối soát số dòng/tổng tiền trước và sau join là thói quen tốt.

`join(..., Seq("customer_id"), ...)` (khóa cùng tên) chỉ giữ **một** cột `customer_id` và đưa nó lên **đầu** danh sách cột — test semi join phát hiện điều này.
`join(..., left("k") === right("k"))` giữ cả hai cột khóa.

### 3.4. Window function

Window tính trên một "cửa sổ" các dòng liên quan **mà không gộp** chúng lại (khác `groupBy`):

```scala
// Bản MỚI NHẤT theo dòng hàng — giữ các bản có updated_at lớn nhất, đếm số nội dung khác nhau trong đó
val byLine = Window.partitionBy("order_id", "sku")
orders
  .withColumn("max_updated_at", max("updated_at").over(byLine))
  .filter(col("updated_at") === col("max_updated_at"))
  .withColumn("variants", size(collect_set(struct(/* mọi cột không phải khóa */)).over(byLine)))
// variants = 1 → latestVersion (dropDuplicates các bản giống hệt); variants > 1 → versionConflicts

// Tổng lũy kế theo khách — orderBy phải PHÂN BIỆT được mọi dòng hàng
val w2 = Window.partitionBy("customer_id").orderBy("day", "order_id", "sku").rowsBetween(Window.unboundedPreceding, Window.currentRow)
paidLines.withColumn("revenue_vnd", col("quantity").cast(LongType) * col("unit_price_vnd"))
  .withColumn("running_total", sum("revenue_vnd").over(w2))   // paidLines: latestVersion + status PAID + giá không null
```

- `O1` có hai phiên bản (PAID lúc 01:00, REFUNDED lúc 09:00 hôm sau) → bản mới nhất **REFUNDED** → không tính doanh thu. `dropDuplicates("order_id")` giữ một bản
  **bất kỳ** — có thể là PAID — và kết quả thay đổi giữa các lần chạy.
- **Hòa thời gian.** Bản đầu dùng `row_number() over (orderBy updated_at desc)`. Review chéo: hai bản cùng `updated_at`, một PAID một REFUNDED → input
  `[PAID, REFUNDED]` cho PAID, `[REFUNDED, PAID]` cho REFUNDED. `row_number` chỉ **xác định** khi thứ tự sắp xếp phân biệt được mọi bản. Lab không đoán: bản mới
  nhất mâu thuẫn là **xung đột**, loại khỏi doanh thu và trả ở `versionConflicts`. (Nguồn có số thứ tự/offset duy nhất thì dùng nó làm tie-breaker — một quyết
  định nghiệp vụ, không phải thứ tự chữ cái của status.)
- **Hòa trong tổng lũy kế.** Bản đầu `orderBy("day", "order_id")`: các sku của cùng một đơn hòa nhau. Đơn X có A/B/C = 100/200/300: tổng cuối luôn 600, nhưng dòng
  100 nhận running total **100** (AQE bật, 2 partition) hay **400** (AQE tắt, 7 partition). Thêm `sku` vào orderBy và output; test chạy 3 cấu hình.
- `rowsBetween(unboundedPreceding, currentRow)`: khung từ đầu partition tới dòng hiện tại. Thiếu khung mà có `orderBy`, Spark dùng khung mặc định `RANGE` — dòng
  cùng giá trị sắp xếp được cộng chung; ghi khung tường minh để khỏi bất ngờ.
- `row_number` (1,2,3), `rank` (1,1,3), `dense_rank` (1,1,2): khác nhau khi **hòa** — top-N mỗi nhóm thường dùng `dense_rank` (lời giải bài 3).
- Window có `partitionBy` cần dữ liệu **gom theo khóa** → thường có shuffle theo khóa đó (nếu dữ liệu đã được chia sẵn theo khóa, ví dụ chỉ có 1 partition,
  thì không cần — Chương 22 mục 3.3); window **không** `partitionBy` dồn mọi dòng về một partition — tránh trên dữ liệu lớn.

### 3.5. NULL: logic ba trị

So sánh với NULL cho **NULL**, không phải `true`/`false`; `filter` chỉ giữ dòng có điều kiện **true**. Trên `customers` (C3 có `city` NULL), số đo thật:

| Điều kiện | Số dòng |
|---|---|
| `city = NULL` (`=== lit(null)`) | **0** — không bao giờ đúng |
| `city <=> NULL` (null-safe) | **1** |
| `city.isNull` | **1** |
| `city != "Hà Nội"` (4 khách: Hà Nội, TP.HCM, NULL, Huế) | **2** — dòng NULL **bị loại** |

Dòng cuối là bẫy thật: "mọi khách không ở Hà Nội" mà thiếu khách chưa có thành phố. Viết đúng: `col("city") =!= "Hà Nội" || col("city").isNull`.
Join cũng vậy: `===` không bao giờ ghép NULL với NULL, `<=>` thì có (lời giải bài 5: 1 vs 2 cặp).

Công cụ: `coalesce(col, lit(...))` (giá trị đầu tiên không null), `na.fill`, `na.drop` — dùng có chủ đích, ghi rõ lý do.

### 3.6. Catalyst: vì sao cách viết đổi tốc độ

Catalyst biến truy vấn qua 4 bước: **parsed → analyzed → optimized (logic) → physical**. Kế hoạch logic đã tối ưu của
`orders.join(customers).filter(segment = Retail && quantity > 1).select(order_id, city)`:

```
Project [order_id#0, city#10]
+- Join Inner, (customer_id#1 = customer_id#8)
   :- Project [order_id#0, customer_id#1]
   :  +- Filter ((isnotnull(quantity#5) AND (quantity#5 > 1)) AND isnotnull(customer_id#1))
   :     +- Relation [order_id#0,customer_id#1,day#2,status#3,sku#4,quantity#5,unit_price_vnd#6L,updated_at#7] csv
   +- Project [customer_id#8, city#10]
      +- Filter ((isnotnull(segment#9) AND (segment#9 = Retail)) AND isnotnull(customer_id#8))
         +- Relation [customer_id#8,segment#9,city#10,signup_date#11] csv
```

Ta viết filter **sau** join; Catalyst:
1. **Đẩy filter xuống** (predicate pushdown) từng phía **trước** join — join ít dòng hơn.
2. **Cắt cột** (column pruning): chỉ giữ `order_id, customer_id` và `customer_id, city`.
3. Tự thêm `isnotnull(customer_id)` vì inner join không bao giờ khớp khóa null.

Với **Parquet**, việc đẩy còn đi xa hơn — xuống tận bộ đọc file (đọc ít row group, ít cột). Dòng `FileScan` thật:

```
-- filter bằng hàm CÓ SẴN: col("sku").startsWith("KB")
FileScan parquet [order_id#181,sku#185] ... PushedFilters: [IsNotNull(sku), StringStartsWith(sku,KB)], ReadSchema: struct<order_id:string,sku:string>

-- filter bằng UDF
FileScan parquet [order_id#190,sku#194] ... DataFilters: [UDF(sku#194)], ... PushedFilters: [], ReadSchema: struct<order_id:string,sku:string>

-- Dataset[OrderRow] + lambda: _.sku.startsWith("KB")
FileScan parquet [order_id#204,customer_id#205,day#206,status#207,sku#208,quantity#209,unit_price_vnd#210L,updated_at#211] ... PushedFilters: [], ReadSchema: struct<order_id:string,customer_id:string,day:date,...
```

- **UDF** là hộp đen: Catalyst không biết `UDF(sku)` nghĩa là gì → `PushedFilters: []` → đọc hết rồi mới lọc.
- **Lambda của Dataset** cũng hộp đen, **thêm** việc phải dựng đối tượng `OrderRow` nên đọc **đủ 8 cột** (không cắt cột).
- Dataset + `Column` (`.filter(col("sku").startsWith("KB"))`) thì được đẩy — nhưng vẫn đọc đủ cột vì kết quả là `OrderRow`.

Một lần đo trên máy tác giả (`UdfBench` của lời giải bài 4: 2 triệu dòng, cache, 1 lượt khởi động + 3 lượt): hàm có sẵn 1.233–1.650 ms, UDF 1.738–2.548 ms — UDF
phải chuyển dữ liệu nội bộ của Spark sang kiểu Scala và ngược lại cho mỗi dòng. Đây là **quan sát** (phụ thuộc JIT, tải máy, dữ liệu), không phải tỷ lệ cố định;
hãy đo trên workload của bạn. Và trước khi so tốc độ, hai cách phải **cùng ý nghĩa**: lời giải bài 4 chỉ ra `String.trim` của Java bỏ cả tab còn `trim` của Spark
chỉ bỏ dấu cách, `toUpperCase()` không chỉ locale biến "i" thành "İ" dưới locale tr-TR.
**Quy tắc:** dùng hàm có sẵn trong `org.apache.spark.sql.functions` trước; chỉ viết UDF khi không có cách nào khác, và biết cái giá.

**Một hộp đen khác — `groupByKey/mapGroups`** (lời giải bài 6): kế hoạch thật là `Filter(lambda) → AppendColumns → Exchange → Sort → MapGroups` — **mọi dòng** đi qua
shuffle rồi bị sắp xếp; trong khi `groupBy().agg(sum)` có `partial_sum` trước `Exchange` (tổng hợp cục bộ như Chương 20). Cùng kết quả, chi phí khác hẳn trên dữ liệu lớn.

### 3.7. Scala collections, DataFrame, Dataset — khi nào dùng gì

| Câu hỏi | Scala collections (Ch19–20) | Spark DataFrame/SQL | Spark Dataset typed |
|---|---|---|---|
| Dữ liệu vừa RAM một máy? | ✔ đơn giản nhất | Thừa, chi phí khởi động vài giây | Thừa |
| Dữ liệu lớn hơn một máy | ✘ | ✔ | ✔ (chậm hơn DataFrame ở chỗ lambda) |
| Cần optimizer, đọc ít cột Parquet | ✘ | ✔ | Một phần |
| Kiểm tra kiểu lúc biên dịch | ✔ | ✘ (tên cột là chuỗi) | ✔ |
| Logic nghiệp vụ phức tạp, đệ quy | ✔ | Khó | ✔ |

Không có nghĩa "Spark = List chạy trên nhiều máy": khác bộ nhớ, thứ tự, khả năng tối ưu, chi phí khởi động và chi phí shuffle (Chương 20 mục 3.5).

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Schema, `StructType`/`StructField` | Mô tả cột: tên, kiểu, nullable |
| Schema inference | Spark đọc dữ liệu để đoán kiểu |
| `FAILFAST` / `PERMISSIVE` / `DROPMALFORMED` | Chế độ xử lý dòng hỏng khi đọc |
| DataFrame / Dataset / Encoder | `Dataset[Row]` / dataset typed / bộ chuyển đối tượng ↔ dạng nội bộ của Spark |
| Column expression | Biểu thức trên cột (`col("x") * 2`) — Catalyst hiểu được |
| Left semi / left anti join | Giữ dòng trái có / không có cặp bên phải, không thêm cột |
| Window function, frame | Hàm trên cửa sổ dòng liên quan / khung dòng của cửa sổ |
| Three-valued logic, null-safe equality (`<=>`) | Logic true/false/NULL / so sánh coi NULL bằng NULL |
| UDF (user-defined function) | Hàm tự viết — hộp đen với optimizer |
| Catalyst, logical/physical plan | Bộ tối ưu của Spark SQL / kế hoạch logic, vật lý |
| Predicate pushdown, column pruning | Đẩy điều kiện về sát nguồn / chỉ đọc cột cần |
| `PushedFilters`, `ReadSchema` | Điều kiện và cột được chuyển xuống bộ đọc file |

## 5. Ví dụ đơn giản

```scala
val orders = SqlLab.readOrders(spark, "data/orders.csv")
orders.createOrReplaceTempView("orders")
spark.sql("SELECT status, COUNT(*) AS n FROM orders GROUP BY status").show()
orders.groupBy("status").count().show()          // cùng truy vấn, DataFrame API
```

Hai dòng cuối cho cùng kết quả và cùng kế hoạch tối ưu (lời giải bài 2 chứng minh bằng code).

## 6. Ví dụ thực tế — doanh thu theo phân khúc

Dữ liệu (`data/`): 7 dòng đơn hàng, 4 khách. Tính tay:
- `O1`: bản mới nhất là REFUNDED → loại. `O5`: giá NULL → loại (không tính doanh thu sai).
- PAID còn: `O2` (C2 Wholesale, 50 × 400.000 = 20.000.000), `O3` (C3 Retail, 990.000), `O4` (C9 — không tồn tại, 450.000), `O6` (C2 Wholesale, 3 × 250.000 = 750.000).

Kết quả thật (`revenueBySegment`):

```
+---------+-----------+-----------+------+
|  segment|revenue_vnd|order_lines|orders|
+---------+-----------+-----------+------+
|   Retail|     990000|          1|     1|
|  UNKNOWN|     450000|          1|     1|
|Wholesale|   20750000|          2|     2|
+---------+-----------+-----------+------+
```

Khớp tính tay. Inner join sẽ cho tổng 21.740.000 thay vì 22.190.000 — thiếu 450.000 mà không có dấu hiệu nào.

## 7. Code đầy đủ

```
09-spark/spark-sql-lab/                   ← BUILD RIÊNG, Scala 2.13.18
├── project.scala / build.sbt / project/build.properties
├── data/orders.csv, data/customers.csv
├── src/main/resources/log4j2.properties
├── src/main/scala/vn/scalaeng/sparksql/
│   ├── SqlLab.scala   ← OrderRow, Schemas, readOrders, latestVersion, revenueBySegment, joins, runningTotal, nullSemantics, UDF/lambda/Column
│   └── SqlDemo.scala  ← session (timezone UTC), demo đầy đủ
└── src/test/scala/vn/scalaeng/sparksql/SqlLabSuite.scala   ← 18 test (6 hồi quy từ review)
```

```bash
cd 09-spark/spark-sql-lab
scala-cli test .                       # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.sparksql.SqlDemo
```

## 8. Giải thích code

- **`SqlDemo.session`** — `spark.sql.session.timeZone = UTC`: timestamp hiển thị và cắt ngày theo UTC thay vì múi giờ của máy chạy (cùng lý do như Chương 19).
- **`latestVersion` / `versionConflicts`** — `max(updated_at)` theo dòng hàng + `size(collect_set(struct(...)))` đếm số nội dung khác nhau của bản mới nhất
  (mục 3.4). Dòng có `updated_at` NULL không bao giờ là "mới nhất" — kiểm tra NULL thuộc quality gate trước bước này.
- **`revenueBySegment`** — `col("quantity").cast(LongType) * col("unit_price_vnd")`: phép nhân làm trên `Long` (không phải `Int`). Cast **không** chống tràn: tích/tổng
  vượt `Long` thì ANSI (mặc định bật ở Spark 4) ném `[ARITHMETIC_OVERFLOW] overflow. Use 'try_multiply' ...` — lỗi to thay vì số âm (test `review M1`). Miền dữ liệu
  (quantity ≥ 1, giá > 0) làm mọi số hạng không âm, nên tràn xảy ra khi và chỉ khi tổng thật vượt Long. Cần tổng lớn hơn → `cast("decimal(38,0)")` như Chương 22.
  `coalesce(col("segment"), lit("UNKNOWN"))` gom dòng không khớp join; `duplicateCustomerIds` chạy **trước** (một action riêng).
  Grain: `order_lines` = số dòng hàng, `orders` = `countDistinct("order_id")` — một đơn 3 sku là 1 đơn, 3 dòng hàng.
- **`nullSemantics`** — `lit(null).cast(StringType)`: NULL có kiểu để so sánh với cột chuỗi.
- **`isKeyboard = udf(...)`** — UDF kiểm tra `sku != null` bên trong: UDF nhận NULL như giá trị Scala `null`, phải tự xử lý.
- **`typedLambda` / `typedColumn`** — cùng ý nghĩa (kể cả với sku NULL, sau khi sửa), khác cách viết filter; test so `PushedFilters` và collect thật trên dữ liệu NULL.
- **`planOf` / `finalPlanOf`** — với AQE, plan trước action là `isFinalPlan=false`; sau action, đọc trên **cùng** Dataset. `toString` sau khi chạy in cả Final lẫn
  Initial Plan, nên `finalPlanOf` lấy riêng `AdaptiveSparkPlanExec.executedPlan`.
- **Test** — `val session = spark; import session.implicits._`: import cần định danh ổn định (mục 10).

## 9. Output mong đợi (output thật)

Schema (mục 3.1), bảng doanh thu (mục 6), plan (mục 3.6). Thêm:

```
== Bản mới nhất theo (order_id, sku) — window max(updated_at) + phát hiện xung đột
+--------+--------+-------------------+
|order_id|status  |updated_at         |
+--------+--------+-------------------+
|O1      |REFUNDED|2026-10-16 09:00:00|
|O2      |PAID    |2026-10-15 02:00:00|
|O3      |PAID    |2026-10-16 03:00:00|
|O4      |PAID    |2026-10-16 04:00:00|
|O5      |PAID    |2026-10-16 05:00:00|
|O6      |PAID    |2026-10-17 06:00:00|
+--------+--------+-------------------+

== Đơn mồ côi (left_anti)
+--------+-----------+
|order_id|customer_id|
+--------+-----------+
|      O4|         C9|
+--------+-----------+

== Tổng lũy kế theo khách (window frame)
+-----------+----------+--------+-----+-----------+-------------+
|customer_id|       day|order_id|  sku|revenue_vnd|running_total|
+-----------+----------+--------+-----+-----------+-------------+
|         C2|2026-10-15|      O2|KB-01|   20000000|     20000000|
|         C2|2026-10-17|      O6|MS-01|     750000|     20750000|
|         C3|2026-10-16|      O3|HP-01|     990000|       990000|
|         C9|2026-10-16|      O4|KB-01|     450000|       450000|
+-----------+----------+--------+-----+-----------+-------------+

== NULL: NullCounts(0,1,1,2)
```

**Test:** `SqlLabSuite 18/18`; `sbt test`: `Passed: Total 18, Failed 0`.

## 10. Lỗi thường gặp (thật)

**1. `import spark.implicits._` trên một `var`:**

```
[error] stable identifier required, but SqlLabSuite.this.spark.implicits found.
[error]     import spark.implicits._
[error] Unable to find encoder for type vn.scalaeng.sparksql.OrderRow. An implicit Encoder[vn.scalaeng.sparksql.OrderRow] is needed to store
  vn.scalaeng.sparksql.OrderRow instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._
```

Lỗi thứ hai là **hệ quả** của lỗi thứ nhất. `import x.y._` trong Scala cần `x` là định danh ổn định (`val`/`object`). Sửa: `val session = spark; import session.implicits._`.

**2. Thiếu `--add-opens` khi `collect()` DataFrame có cột `date`:**

```
org.apache.spark.SparkRuntimeException: [EXPRESSION_DECODING_FAILED] Failed to decode a row to a value of the expressions: createexternalrow(...
Caused by: java.lang.IllegalAccessException: symbolic reference class is not accessible: class sun.util.calendar.ZoneInfo,
  from interface org.apache.spark.sql.catalyst.util.SparkDateTimeUtils (unnamed module @11e1e137)
```

Lỗi thật gặp khi viết lab: thông báo chính ("decode a row") không gợi ý nguyên nhân; phải đọc **`Caused by`** cuối cùng. Thiếu `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED`
— một trong các cờ `spark-submit` tự thêm. Review chéo tách từng cột trên JVM chỉ thiếu cờ này: `select(updated_at).collect()` (timestamp) **chạy được**,
`select(day).collect()` (date → `java.sql.Date`, qua `DateTimeUtils.toJavaDate`) **lỗi**, `as[OrderRow]` (LocalDate/Instant) chạy được. Bản đầu của mục này ghi
"cột timestamp" — suy từ một `Row` chứa cả hai kiểu, cũng là một chẩn đoán vội. Bài học giữ nguyên: thu nhỏ đến **một** cột trước khi kết luận. Lab hiện khai báo đủ (Chương 22 mục 3.8). Bài học debug: lần đầu tác giả **đoán sai** nguyên nhân (nghĩ do `nullable = false`),
sửa test theo giả thuyết đó và test vẫn đỏ — chỉ khi đọc `Caused by` mới tìm ra đúng.

**3. `!=` loại mất dòng NULL** — mục 3.5. **4. Inner join làm mất bản ghi mồ côi** — mục 3.3/6. **5. `inferSchema` đổi kiểu theo dữ liệu** — mục 3.1.

**6. `spark.range` trả `Dataset[java.lang.Long]`** (Chương 22, lời giải bài 3): `Encoder[scala.Long]` không khớp — dùng `x.longValue` sau `import spark.implicits._`.

Các lỗi dưới đây do **review chéo** (Codex) tìm ra trên bản đầu bằng probe chạy thật; mỗi lỗi giờ có test hồi quy:

**7. Dedup không xác định khi hòa `updated_at`; tổng lũy kế hòa giữa các sku** — mục 3.4.
**8. Left join nhân doanh thu khi dimension trùng khóa; gọi số dòng hàng là "orders"** — mục 3.3, 8.
**9. Tin rằng `cast(LongType)` chống tràn; lambda Dataset tràn lặng lẽ** — mục 8 và lời giải bài 6 (typed trả `-2` cho `2 × Long.MaxValue`, DataFrame ném lỗi).
**10. Lambda typed ném NPE với sku NULL; `Int` không nhận NULL** — mục 3.2.
**11. Header đổi thứ tự được đọc thành công; `count()` xanh với dữ liệu hỏng** — mục 3.1.
**12. Lời giải tính cả bản PAID cũ đã bị REFUNDED** (bài 2, 6): hai cách so nhau "bằng nhau" nhưng cùng sai — luôn có **golden** tính tay, không chỉ so hai implementation.

## 11. Cách debug

- `df.printSchema()` / `df.schema.treeString` — kiểu và nullable thật sau khi đọc.
- `df.explain("extended")` — cả 4 giai đoạn của Catalyst; `df.explain("formatted")` — dễ đọc hơn cho plan dài.
- Tìm trong `FileScan`: `PushedFilters` (đẩy được gì), `ReadSchema` (đọc cột nào), `PartitionFilters` (Chương 24).
- Kết quả join lạ: đếm `left_anti` hai chiều để thấy dòng không khớp; kiểm tra khóa có NULL, khoảng trắng, hoa/thường.
- Exception dài: đọc **`Caused by` cuối cùng** trước.

## 12. Bài tập

1. **(Cơ bản)** Đọc CSV ở chế độ `PERMISSIVE` với cột `_corrupt_record`; tách dòng tốt và dòng hỏng (giữ nguyên văn dòng hỏng).
2. **(Cơ bản)** Viết doanh thu theo khách bằng **Spark SQL** và bằng **DataFrame API**; chứng minh hai kế hoạch tối ưu giống nhau.
3. **(Trung bình)** Top-N sku theo doanh thu **mỗi ngày** bằng window.
4. **(Trung bình)** Chuẩn hóa sku (`trim` + `upper`) bằng UDF và bằng hàm có sẵn: chốt **hợp đồng** trước (khoảng trắng nào, locale nào, NULL), so **từng giá trị**, so plan, rồi mới đo thời gian trên 2 triệu dòng.
5. **(Nâng cao)** Join trên khóa có thể NULL: đếm số cặp với `===` và `<=>`; giải thích.
6. **(Nâng cao)** Doanh thu theo khách bằng `Dataset[OrderRow]` + `groupByKey`/`mapGroups`; so kết quả và **kế hoạch vật lý** với `groupBy().agg()`.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch23-solutions.md`](../solutions/ch23-solutions.md)
- Code + test (7 test + `UdfBench`): [`solutions/ch23-code/`](../solutions/ch23-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao không dùng `inferSchema` trong pipeline production?
2. Khai báo `nullable = false` khi đọc CSV có bảo đảm không có null không?
3. `PERMISSIVE` và `FAILFAST` khác nhau thế nào? Khi nào dùng cái nào?
4. Vì sao lab dùng left join thay vì inner join cho doanh thu?
5. `row_number` khác `dropDuplicates` thế nào khi dedup?
6. `city != "Hà Nội"` trả gì cho dòng có city NULL?
7. Vì sao UDF chậm hơn và không được đẩy xuống Parquet?
8. `groupByKey/mapGroups` khác `groupBy().agg()` ở bước nào của kế hoạch?

<details><summary>Gợi ý đáp án</summary>

1. Kiểu thay đổi theo dữ liệu từng ngày (schema drift) và tốn thêm một lượt đọc.
2. Không — nguồn file coi mọi cột nullable; phải kiểm tra bằng quality rule.
3. PERMISSIVE: trường hỏng thành null, job chạy tiếp (dùng kèm `_corrupt_record`). FAILFAST: lỗi ngay. Dùng FAILFAST khi dữ liệu bẩn là sự cố.
4. Inner join bỏ đơn của khách không tồn tại — doanh thu mất âm thầm. (Và left join vẫn cần kiểm tra khóa dimension duy nhất, kẻo nhân doanh thu.)
5. `row_number` theo thứ tự tường minh — xác định chỉ khi thứ tự phân biệt được mọi bản (hòa → xung đột); `dropDuplicates` giữ bản bất kỳ.
6. NULL → không được `filter` giữ.
7. Catalyst không hiểu bên trong UDF — không tối ưu, không đẩy điều kiện; mỗi dòng phải chuyển đổi sang kiểu Scala để gọi hàm.
8. `groupByKey/mapGroups` không có tổng hợp cục bộ: mọi dòng qua `Exchange` rồi `Sort`; `agg` có `partial_*` trước `Exchange`.
</details>

## 15. Mini project

**"Báo cáo CTR bằng Spark"**: đọc `events.jsonl` của Chương 19 bằng schema tường minh (cấu trúc lồng nhau: `StructType` trong `StructType`), chuẩn hóa bằng hàm có
sẵn (không UDF), dedup bằng window, left join với campaign, báo cáo CTR theo campaign × ngày (giờ VN: `from_utc_timestamp`). So kết quả với golden của Chương 19;
kiểm tra plan có `PushedFilters` khi đọc từ Parquet.

## 16. Nguồn tài liệu chính thức

- Spark SQL, DataFrames and Datasets Guide 4.2: https://spark.apache.org/docs/4.2.0/sql-programming-guide.html
- Data sources — CSV (mode, columnNameOfCorruptRecord): https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html
- Window functions: https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-window.html
- NULL semantics: https://spark.apache.org/docs/4.2.0/sql-ref-null-semantics.html
- Scala UDF: https://spark.apache.org/docs/4.2.0/sql-ref-functions-udf-scalar.html
- `functions` API: https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/functions$.html
- Performance tuning (AQE, join hints): https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html

## 17. Checklist tự đánh giá

- [ ] Đọc dữ liệu bằng schema tường minh; giải thích bẫy `inferSchema` và `nullable = false`.
- [ ] Chọn đúng chế độ đọc; tách dòng hỏng bằng `_corrupt_record`.
- [ ] Viết join đúng loại; tìm bản ghi mồ côi bằng `left_anti`.
- [ ] Dedup bản mới nhất, tổng lũy kế, top-N bằng window.
- [ ] Viết điều kiện đúng với NULL.
- [ ] Đọc `PushedFilters`/`ReadSchema`; giải thích vì sao UDF và lambda là hộp đen.
- [ ] Chạy được 18 test của lab và 7 test lời giải.
