# Lời giải — Chương 23: Spark SQL và DataFrames

> Code: [`ch23-code/`](ch23-code/) — Scala 2.13.18 + Spark 4.2.0, dùng lại `09-spark/spark-sql-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch23-code && scala-cli test .` → **7/7 test pass** (17/10/2026). Đo thời gian: `scala-cli run . --main-class vn.scalaeng.solutions.ch23.UdfBench`.
> Đã sửa sau review chéo (Codex): bài 2 và 6 dedup trước khi lọc PAID + so golden; bài 4 chốt hợp đồng chuẩn hóa; bài 6 tràn số.

## Bài 1 — `_corrupt_record`

```scala
val schemaWithCorrupt = Schemas.orders.add(StructField("_corrupt_record", StringType, nullable = true))
spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").schema(schemaWithCorrupt).csv(path).cache()
```

- Cột `_corrupt_record` phải có trong schema (kiểu `String`); dòng không khớp schema được lưu **nguyên văn** ở đó; trường parse **hỏng** thành null, các trường parse được **vẫn giữ** giá trị (`order_id = O2`, `customer_id = C1`...).
- `.cache()`: Spark từ chối truy vấn mà chỉ tham chiếu cột corrupt record trên dữ liệu CSV/JSON chưa cache (nó cần parse lại toàn bộ dòng); cache một lần rồi lọc hai nhánh.
- Test: 2 dòng, dòng `quantity = "hai"` xuất hiện nguyên văn trong nhánh hỏng → ghi ra bảng rejected như Chương 19.

## Bài 2 — SQL và DataFrame cho cùng kế hoạch

`latest.createOrReplaceTempView("orders")` + `spark.sql("SELECT customer_id, SUM(...) ... GROUP BY customer_id")` và
`latest.filter(...).groupBy("customer_id").agg(sum(...))`, với `latest = SqlLab.latestVersion(orders)`: test so `queryExecution.optimizedPlan` (bỏ các số `#123`
định danh cột) — **giống hệt** — và so **cả hai** với golden tính tay: C2 = 20.750.000, C3 = 990.000, C9 = 450.000.

**Lỗi của bản đầu (review chéo):** đọc thẳng 7 dòng lịch sử rồi lọc PAID → tính cả bản PAID cũ của O1 (đã REFUNDED): C1 = 900.000. SQL và API "bằng nhau" — vì
cùng sai. So hai implementation với nhau không thay được golden. **Dedup trước, lọc trạng thái sau.**
Chọn SQL hay API theo người đọc code: SQL dễ cho người quen SQL; API dễ ghép, dễ tái sử dụng hàm, compiler bắt lỗi cú pháp Scala (nhưng không bắt tên cột).

## Bài 3 — Top-N mỗi ngày

```scala
val w = Window.partitionBy("day").orderBy(col("revenue_vnd").desc)
revenue.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= n)
```

Kết quả top-1: 15-10 KB-01 20.000.000 (O2; O1 đã REFUNDED), 16-10 HP-01 990.000 (O3 > O4 450.000; O5 giá null bị loại), 17-10 MS-01 750.000 — khớp tính tay.
`dense_rank` cho các sku **hòa** cùng hạng (giữ cả hai); `row_number` sẽ chọn ngẫu nhiên một trong hai.

## Bài 4 — UDF vs hàm có sẵn

**Chốt hợp đồng trước khi so.** Bản đầu: UDF `s.trim.toUpperCase` vs `upper(trim(col))`, test chỉ so **số** giá trị khác nhau (1.000 = 1.000). Review chéo:

| Đầu vào | Spark `upper(trim(...))` | UDF bản đầu |
|---|---|---|
| `"\tsku\t"` | `"\tSKU\t"` (Spark `trim` chỉ bỏ dấu cách) | `"SKU"` (Java `trim` bỏ cả tab, ký tự điều khiển) |
| `"i"`, JVM locale tr-TR | `"I"` | `"İ"` (U+0130 — `toUpperCase()` theo locale JVM) |

Hợp đồng chọn: bỏ dấu cách `' '` hai đầu, viết hoa không phụ thuộc locale, NULL giữ NULL. UDF cài đúng hợp đồng (`stripSpaces` + `toUpperCase(Locale.ROOT)`); test đặt
locale tr-TR và so **từng dòng** của 7 đầu vào (dấu cách, tab, chuỗi toàn dấu cách, rỗng, NULL, "i", "kb-01").

Bẫy thật khi viết test: dữ liệu tạo bằng `Seq(...).toDF` nằm trong driver, nên `optimizedPlan` chỉ còn `LocalRelation [sku, id, sku_norm]` — Catalyst **tính luôn**
biểu thức lúc tối ưu. Xem biểu thức (`upper(trim(...))` vs `UDF(...)`) ở `analyzed` plan.

**Đo** (`UdfBench`, tách khỏi unit test — benchmark trong test dễ timeout khi máy bận và dạy nhầm rằng tỷ lệ là hợp đồng): 2 triệu dòng, cache, 1 lượt khởi động:

```
lần 1: builtin 1650 ms, UDF 2548 ms
lần 2: builtin 1551 ms, UDF 2390 ms
lần 3: builtin 1233 ms, UDF 1738 ms
```

UDF chậm hơn ở lần đo này vì mỗi dòng phải chuyển `UTF8String` nội bộ sang `String` của Java/Scala và ngược lại; UDF cũng không được đẩy xuống nguồn (mục 3.6).
Con số và cả tỷ lệ phụ thuộc máy, JIT, tải; đo workload của bạn.

## Bài 5 — Join trên khóa NULL

`left: (A,1), (NULL,2)`, `right: (A,10), (NULL,20)`: `===` → **1** cặp (A); `<=>` → **2** cặp (A và NULL–NULL).
Thường bạn **muốn** `===` (NULL không phải giá trị khóa thật). Dùng `<=>` khi NULL mang nghĩa "không xác định" và cần ghép hai bảng cùng quy ước — và ghi rõ lý do.

## Bài 6 — Dataset typed: `groupByKey/mapGroups`

Cùng kết quả với `groupBy().agg(sum)` **và** đúng golden (đầu vào là `latestVersion(orders)`, như bài 2). Kế hoạch vật lý thật (phần trên của plan; bên dưới
`Filter` còn các bước Window của `latestVersion`). Test đọc plan **cuối** của chính Dataset vừa `collect()` (`SqlLab.finalPlanOf`):

```
SerializeFromObject
+- MapGroups <lambda>
   +- Sort [value ASC NULLS FIRST]
      +- Exchange hashpartitioning(value, 200)
         +- AppendColumns <lambda>               ← tính khóa bằng lambda
            +- Filter invoke(<lambda>.apply(newInstance(OrderRow)))
               +- FileScan csv [order_id, customer_id, ..., updated_at]   ← đọc đủ cột
```

- **Không có `partial_sum`**: mọi dòng đi qua shuffle; với `agg` thì mỗi partition tổng hợp trước (Chương 20 — map-side combine).
- Thêm `Sort` trước `MapGroups` — sắp **trong từng partition** theo khóa nhóm để gom các dòng cùng khóa, không phải sắp toàn cục kết quả.
- Filter là lambda → không đẩy xuống nguồn; cần dựng `OrderRow` → đọc đủ cột.
- **Tràn số (review chéo):** lambda chạy số học **Scala** — ANSI chỉ áp cho biểu thức SQL. Bản đầu `quantity.toLong * price` + `.sum` trả **-2** cho
  `2 × Long.MaxValue`, trong khi DataFrame ném `ARITHMETIC_OVERFLOW`. Giờ dùng `Math.multiplyExact`/`addExact`: cả hai cùng chính sách "tràn = lỗi" (test bài 6 M1).
- Muốn giữ kiểu mà vẫn có tổng hợp cục bộ: `groupByKey(...).reduceGroups(...)` hoặc `Aggregator` typed; hoặc dùng DataFrame cho bước tổng hợp, chỉ dùng Dataset cho logic.
