# Review T11 — Chương 23: Spark SQL và DataFrames (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 7 MAJOR, 6 MINOR, không có BLOCKER.** Các mục dưới đây cần tác giả sửa và lead kiểm chứng lại trước khi đánh dấu hoàn thành.

Đã đọc handoff, toàn bộ chương, source/test/config/resources/data của lab, lời giải và code/test lời giải; đối chiếu template và PHẦN 11–14 của đề bài. Chương **đủ 17 mục, không có mục rỗng**, tiếng Việt có dấu, ví dụ đơn hàng dễ nối tiếp Ch22; phần NULL ba trị, schema inference, đọc plan và hướng dẫn tìm `Caused by` hữu ích cho người mới. Tuy nhiên, các bảo đảm về tính đúng của doanh thu, tính xác định và tính tương đương giữa API vượt quá điều code thực hiện. Không có BLOCKER về build/dependency đã phát hiện.

## Kiểm chứng và phạm vi ghi

Chỉ tạo báo cáo này và dữ liệu trong `.cache/review-t11/`; không sửa code/bài/lời giải, file dùng chung hay Ch25. Các thay đổi Ch22/Ch25/backend xuất hiện trong working tree trong lúc review thuộc công việc đồng thời của agent khác.

Môi trường probe xác nhận: **Spark 4.2.0, Scala 2.13.18, Java 21.0.12.1, ANSI=true**, Windows, local[2]. `.cache/` được git ignore. Đặt:

```powershell
$env:COURSIER_CACHE = 'C:/Users/VuTT/zero_to_hero_scala/.cache/coursier'
$env:SPARK_LOCAL_DIRS = 'C:\Users\VuTT\zero_to_hero_scala\.cache\review-t11\local'
```

Mọi lệnh Scala CLI đều dùng `--server=false`, `--workspace` riêng trong `.cache/review-t11/`, cùng:

```text
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/warehouse
```

Các lượt chạy:

1. Từ `09-spark/spark-sql-lab`: `scala-cli.bat test . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/lab-build` + hai java-opt trên → **exit 0, 12 pass / 0 fail / 0 ignored**. Output thật:

   ```text
   vn.scalaeng.sparksql.SqlLabSuite: finished 23.663s
   Test run vn.scalaeng.sparksql.SqlLabSuite finished: 0 failed, 0 ignored, 12 total 23.666s
   ```

2. Từ `solutions/ch23-code`: lệnh test tương tự, workspace `sol-build` → lượt đầu **exit 1, 5 pass / 1 fail / 0 ignored**:

   ```text
   [bài 4] builtin vs UDF (ms, 3 lần): (2491,3695), (2594,4359), (6976,7119)
   java.util.concurrent.TimeoutException: test timed out after 30 seconds
   Test run vn.scalaeng.solutions.ch23.Ch23SolutionsSuite finished: 1 failed, 0 ignored, 6 total 63.709s
   ```

   Lượt đầu có phần giao thời với compile/demo/probe khác của reviewer; không dùng số đo này để kết luận tỷ lệ hiệu năng. Chạy lại sau khi các JVM khác của reviewer kết thúc, giữ nguyên code/cấu hình/lệnh, log `sol-test-isolated.log` → **exit 0, 6 pass / 0 fail / 0 ignored**:

   ```text
   [bài 4] builtin vs UDF (ms, 3 lần): (1967,2512), (2227,3567), (2366,3586)
   vn.scalaeng.solutions.ch23.Ch23SolutionsSuite: finished 56.988s
   Test run vn.scalaeng.solutions.ch23.Ch23SolutionsSuite finished: 0 failed, 0 ignored, 6 total 56.998s
   ```

   Không khẳng định toàn máy rảnh vì agent khác đang làm việc; xác nhận không còn test/demo/probe JVM khác của chính reviewer trong lượt chạy lại này.

3. Từ lab: `scala-cli.bat run . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/demo-build --main-class vn.scalaeng.sparksql.SqlDemo` + hai java-opt → **exit 0**. Schema tường minh/suy luận, bản mới nhất, bảng Retail=990000 / UNKNOWN=450000 / Wholesale=20750000, orphan O4, running totals và `NullCounts(0,1,1,2)` **khớp bài**. Logical plan mục 3.6 khớp cấu trúc và các filter/project; FileScan builtin/UDF/lambda/Column khớp `PushedFilters` và `ReadSchema`. ID biểu thức/đường dẫn tạm không phải hợp đồng đầu ra.
4. Probe độc lập trong `.cache/review-t11/probe/`, dùng lại source main của lab và lời giải qua directives, chạy `scala-cli.bat run .cache/review-t11/probe --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/probe-build --main-class Probe` + hai java-opt. `SPARK_LOCAL_DIRS` riêng là `.cache/review-t11/local-probe`. **Exit 0**; từng trường hợp lỗi mong đợi được bắt và in chuỗi nguyên nhân. Một lượt compile ban đầu của **probe reviewer** có dư dấu `)`, đã sửa và chạy lại; không phải lỗi source tác giả.

5. Chạy thêm cùng lệnh probe nhưng `--main-class Probe2`: kiểm tra grain, thay số partition, final plan typed/untyped và ép điều kiện thích hợp để AQE đổi join. Cả `probe2.log` và `probe2-round2.log` **exit 0**, xác nhận `beforeSortMerge=true afterBroadcast=true` với automatic broadcast ban đầu=-1, adaptive threshold=10485760 và adaptive nonEmptyPartitionRatioForBroadcastJoin=0.0. Initial/final MapGroups vẫn giữ đặc điểm không partial_sum. Codepoint normalize với locale tr-TR: builtin **73** (`I`), UDF **304** (`İ`). Round2 xác nhận runningTotal đổi theo AQE/số partition (M2).
6. `.cache/review-t11/no-opens/` dùng cùng dependency/source main, chỉ bỏ cờ `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED`. Chạy `scala-cli.bat run .cache/review-t11/no-opens --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t11/no-opens-build --main-class NoOpens` + hai java-opt; SPARK_LOCAL_DIRS=`.cache/review-t11/local-no-opens`. Lượt `no-opens.log` chỉ collect timestamp **exit 0**; lượt `no-opens2.log` bắt từng exception, **exit 0**, xác nhận module chưa mở (`OPEN calendar=false`), Date/full Row lỗi nhưng timestamp-only và typed LocalDate/Instant thành công; chi tiết ở m6.

Log: `.cache/review-t11/lab-test.log`, `sol-test.log`, `sol-test-isolated.log`, `demo.log`, `probe.log`, `probe2.log`, `probe2-round2.log`, `no-opens.log`, `no-opens2.log`. PowerShell gom stderr thành `NativeCommandError` cả cho hint/WARN; kết luận dựa vào exit code và summary thực. Thiếu winutils/native-hadoop không chặn đọc/ghi Parquet. Không thấy compiler warning của lab/lời giải; probe2 lượt đầu có một unused import của **reviewer**, đã bỏ trong source probe. Một số chữ tiếng Việt bị encoding của tiến trình/log thay bằng `?`; số liệu, plan và error class vẫn đọc được. Không chạy sbt trong sandbox theo AGENTS.md; lead chạy lại sbt cuối.

## MAJOR

### M1 — Ép sang Long không bảo đảm hết overflow; Dataset và SQL không tương đương khi tràn số

Vị trí: `09-spark/ch23-spark-sql-dataframes.md:263`; `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:70`, `:85`; `solutions/ch23-code/src/main/scala/vn/scalaeng/solutions/ch23/Ch23Solutions.scala:79`; `solutions/ch23-solutions.md:51`.

Probe gọi trực tiếp implementation với ANSI mặc định bật:

```text
quantity=2, price=Long.MaxValue:
  TypedVsUntyped.typed -> [C1,-2]
  SqlVsDataFrame.viaApi -> [ARITHMETIC_OVERFLOW], try_multiply, SQLSTATE: 22003
  SqlLab.revenueBySegment -> cùng lỗi overflow
hai dòng cùng khách, amount=Long.MaxValue và 1:
  typed -> [C1,-9223372036854775808]
  SQL/API -> [ARITHMETIC_OVERFLOW], try_add, SQLSTATE: 22003
```

Phép nhân đã có một toán hạng `Long` nên Spark cũng đã nâng kiểu; cast không tạo kiểu số vô hạn. `.toLong * ...` và `.sum` trong lambda chạy theo số học Scala, wrap-around âm mà ANSI không bảo vệ. Test chỉ so một bộ số nhỏ, không chứng minh khẳng định “cùng kết quả”.

Đề xuất: nêu miền giá trị/giới hạn tổng và policy lỗi; dùng Decimal với precision phù hợp ở SQL và phép tính checked/BigInt có kiểm tra biên tương ứng ở typed. Nếu giữ Long để dạy cơ bản, sửa khẳng định thành chỉ an toàn trong miền đã kiểm tra, phát hiện overflow nhất quán và thêm test cả nhân/tổng vượt biên. Không tắt ANSI để hai bên cùng trả số sai. [Spark 4.2 ANSI Compliance](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html) xác nhận ANSI mặc định bật và kiểm tra overflow của SQL.

### M2 — Dedup chưa xác định khi hòa timestamp; runningTotal cũng thiếu thứ tự đủ để phân biệt dòng hàng

Vị trí: `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:55`, `:58`, `:82`, `:87`; `09-spark/ch23-spark-sql-dataframes.md:105`, `:109`, `:262`, `:377`.

Hai bản cùng `(order_id=X, sku=KB, updated_at=2026-10-15T01:00:00Z)`, một PAID, một REFUNDED:

```text
input [PAID, REFUNDED] -> latestVersion: PAID
input [REFUNDED, PAID] -> latestVersion: REFUNDED
```

Doanh thu có thể thay đổi chỉ vì thứ tự đọc, trái mô tả “tường minh và xác định”. `row_number` chỉ có tính xác định khi tiêu chí sắp xếp phân biệt đủ các bản mâu thuẫn. Ngoài ra, khóa dòng nghiệp vụ là `(order_id, sku)` nhưng running window chỉ orderBy `(day, order_id)`; một đơn có nhiều SKU tạo các dòng hòa thứ tự. Hàm còn bỏ `sku` khỏi output nên khó truy nguyên dòng nào nhận tổng nào.

Probe round2 với cùng một đơn X có SKU A/B/C, tiền 100/200/300, cùng ngày/khách, **đã tái hiện** tổng gắn vào dòng khác nhau khi thay cấu hình:

```text
AQE=true, shuffle.partitions=2:  (revenue,running) = (100,100), (200,300), (300,600)
AQE=false, shuffle.partitions=2: (revenue,running) = (100,100), (300,400), (200,600)
AQE=false, shuffle.partitions=7: (revenue,running) = (300,300), (100,400), (200,600)
```

Tổng cuối luôn 600 nhưng running total của riêng dòng 100 đổi 100→400; đây là khác biệt hành vi quan sát được, không chỉ thiếu cam kết trên lý thuyết.

Đề xuất: định nghĩa tie policy có ý nghĩa nghiệp vụ (version/source sequence duy nhất; hoặc từ chối/quarantine hai payload khác nhau cùng version), xử lý timestamp/key thiếu. Đối với running total, hoặc cộng các SKU thành một đơn trước window, hoặc thêm `sku`/line_id vào cả orderBy và output. Thêm test hòa timestamp, nhiều SKU, thay thứ tự/partition; không dùng status alphabet như quy tắc nghiệp vụ ngầm.

### M3 — Left join vẫn có thể nhân doanh thu; chưa chốt grain của cột orders

Vị trí: `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:68`, `:70`; `09-spark/ch23-spark-sql-dataframes.md:93`, `:224`.

Nhân đôi đúng dòng C2 trong customers, giữ nguyên orders, probe thu được:

```text
[Retail,990000,1], [UNKNOWN,450000,1], [Wholesale,41500000,4]
```

Wholesale đúng trước đó là 20750000 và 2. Left join bảo toàn dòng không khớp nhưng **không bảo đảm quan hệ nhiều-một**; bảng dimension có hai dòng cùng customer_id sẽ nhân mỗi fact. Bài đang dạy tránh mất doanh thu nhưng chưa dạy kiểm tra cardinality đối xứng này.

Ngoài ra `latestVersion` có grain `(order_id, sku)` còn `count(lit(1)).as("orders")` đếm dòng hàng. Probe một order X có ba SKU A/B/C, tiền 100/200/300, trả `[Retail,600,3]`: **một đơn bị đếm thành ba orders**. Cần chốt định nghĩa trước khi gọi cột là orders.

Đề xuất: kiểm tra uniqueness/non-null của customer_id trước join, quy tắc chọn dimension version hoặc báo lỗi nếu vi phạm; không tùy tiện dropDuplicates để che xung đột segment. Thêm đối soát số dòng/tổng tiền trước–sau join. Đổi nhãn thành order_lines hoặc dùng countDistinct(order_id) theo định nghĩa đơn hàng, với test một order có nhiều SKU và dimension trùng cùng/khác segment.

### M4 — Bài 4: UDF và builtin khác semantics; test đếm distinct không chứng minh cùng kết quả

Vị trí: `solutions/ch23-code/src/main/scala/vn/scalaeng/solutions/ch23/Ch23Solutions.scala:51`, `:53`; `solutions/ch23-code/src/test/scala/vn/scalaeng/solutions/ch23/Ch23SolutionsSuite.scala:44`, `:45`; `solutions/ch23-solutions.md:41`.

Probe:

```text
input "\tsku\t": Spark upper(trim(...)) -> "\tSKU\t"; UDF -> "SKU"
input "i", JVM default locale tr-TR: builtin -> "I" (U+0049); UDF -> "İ" (U+0130)
```

Java `String.trim` loại ký tự điều khiển ASCII như tab; Spark trim mặc định chỉ loại space. `String.toUpperCase` không chỉ định locale dùng locale của JVM, nên còn có thể khác giữa executor. Cả hai vẫn có thể trả cùng **số lượng** distinct dù giá trị khác hoàn toàn; test hiện không so cặp input/output. Bài tập yêu cầu so cả plan nhưng test/lời giải bài 4 mới đo thời gian và cardinality, chưa thể hiện plan cho phép chuẩn hóa này.

Đề xuất: chốt hợp đồng normalize (space hay toàn bộ whitespace, Unicode/collation/locale), viết hai implementation cùng hợp đồng, locale tường minh; so toàn bộ giá trị bằng phép so an toàn với null và giữ duplicates, bổ sung tab/chuỗi rỗng/null/Unicode. In/giải thích plan của chính hai query normalization; không suy `PushedFilters` của startsWith sang upper(trim). [Spark built-in functions](https://spark.apache.org/docs/4.2.0/sql-ref-functions-builtin.html) mô tả trim mặc định loại space.

### M5 — Lambda typed không an toàn với NULL như Column/UDF; kiểu Int không nhận được quantity null

Vị trí: `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:17`, `:116`; `09-spark/ch23-spark-sql-dataframes.md:57`, `:82`, `:267`; `solutions/ch23-code/src/main/scala/vn/scalaeng/solutions/ch23/Ch23Solutions.scala:77`.

Parquet cùng schema, sku=null:

```text
withBuiltin.collect.length = 0
withUdf.collect.length = 0
typedColumn.collect.length = 0
typedLambda.collect -> NullPointerException: Cannot invoke "String.startsWith(String)"
```

Vì vậy “cùng ý nghĩa, khác cách viết filter” không đúng trên miền dữ liệu mà schema đọc lên cho phép. Dataset case class không tự biến mọi cột nullable thành Option. Với quantity=null, API trả `[C1,null]`, còn typed báo:

```text
[NOT_NULL_ASSERT_VIOLATION] NULL value appeared in non-nullable field:
- field (class: "int", name: "quantity")
- root class: "vn.scalaeng.sparksql.OrderRow"
```

Đề xuất: cho lambda xử lý null sku như UDF, xác định quality gate trước `.as[OrderRow]`, hoặc dùng Option cho trường thực sự được phép thiếu. Giải thích rõ Int non-nullable và String có thể chứa JVM null; thêm test đọc dữ liệu thiếu giá trị, collect thật thay vì chỉ count/đọc plan. Nếu so typed/untyped, áp dụng cùng chính sách dữ liệu không hợp lệ trước hai nhánh. Quy tắc sum bỏ NULL/all-null trả NULL của API được mô tả ở [Spark NULL semantics](https://spark.apache.org/docs/4.2.0/sql-ref-null-semantics.html).

### M6 — Reader áp schema theo vị trí nên header đổi thứ tự vẫn đọc sai âm thầm; FAILFAST không phải quality gate toàn dòng

Vị trí: `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:44`, `:46`, `:52`; `09-spark/ch23-spark-sql-dataframes.md:55`, `:67`, `:69`.

Đổi hai cột đầu file thành `customer_id,order_id,...`, dòng bắt đầu `C1,X,...`: `readOrders(...).collect()` **thành công** nhưng trả `order_id=C1, customer_id=X`. Hai cột đều String nên FAILFAST không cứu được. Mặc định `enforceSchema=true` bỏ qua tên header và áp schema theo vị trí.

Với file quantity=`hai`, cùng reader:

```text
count() -> 1
select("order_id").collect() -> [X]
collect() đủ cột -> MALFORMED_RECORD_IN_PARSING / NumberFormatException
```

Spark lazy và CSV column pruning có thể không parse cột sai mà query không dùng. “Dòng không khớp schema làm job lỗi NGAY”/“dữ liệu phải sạch” cần giới hạn vào parse những trường được yêu cầu; count xanh không xác nhận dữ liệu sạch.

Đề xuất: đặt `enforceSchema=false` cho reader có header để phát hiện header lệch; test đảo thứ tự/header sai tên. Giải thích pruning và action; nếu có bước xác thực toàn file thì phải thực sự parse/kiểm tra đủ các trường bắt buộc, tách parse errors khỏi quality rules, và kiểm chứng cách materialize được chọn. [Spark CSV options](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html) khuyến nghị tắt enforceSchema và ghi rõ corrupt detection phụ thuộc tập cột được đọc.

### M7 — Lời giải bài 2 và 6 tính lại bản PAID đã bị REFUNDED; hai cách cùng sai nghiệp vụ vẫn pass test

Vị trí: `solutions/ch23-code/src/main/scala/vn/scalaeng/solutions/ch23/Ch23Solutions.scala:26`, `:32`, `:77`; `solutions/ch23-code/src/test/scala/vn/scalaeng/solutions/ch23/Ch23SolutionsSuite.scala:31`, `:58`, `:61`; `09-spark/ch23-spark-sql-dataframes.md:223`.

Lời giải đọc thẳng 7 dòng lịch sử rồi filter PAID, không gọi latestVersion. Trên chính data của bài:

```text
viaApi(raw orders) -> C1=900000, C2=20750000, C3=990000, C9=450000
viaApi(latestVersion(orders)) -> C2=20750000, C3=990000, C9=450000
```

C1=900000 là bản PAID cũ của O1; bài mục 6 đã khẳng định O1 REFUNDED phải loại. TopSkuPerDay lại xử lý đúng bằng latestVersion. Test equality SQL/API và typed/untyped chỉ chứng minh hai implementation cùng tính raw history, không kiểm tra golden nghiệp vụ.

Đề xuất: truyền tập latest đã kiểm chứng vào cả SQL/API/typed hoặc chuẩn hóa trong một hàm chung; phải dedup **trước** filter PAID. Thêm expected totals cụ thể và regression refund/version lặp. Nếu chủ đích chỉ tổng hợp các dòng PAID lịch sử để cô lập thí nghiệm optimizer, phải đổi tên/mô tả bài tập, nói rõ khác doanh thu snapshot và không dùng nó như báo cáo doanh thu cùng domain.

## MINOR

### m1 — PERMISSIVE không làm mọi cột khác thành null

Vị trí: `solutions/ch23-solutions.md:13`.

“Các cột khác null” không khớp probe và parser: dòng quantity=`hai` vẫn có `order_id=X`, `customer_id=C1`, day/status/sku/price/timestamp hợp lệ; chỉ quantity=null, `_corrupt_record` giữ nguyên dòng. Sửa thành **trường parse hỏng thành null; các trường parse được có thể được giữ**. Cache trước khi tách corrupt-only query đã chạy đúng, kể cả collect nhánh bad trước; không ghi phần đó thành lỗi. Nguồn: [Spark CSV PERMISSIVE](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html).

### m2 — Phân biệt kế hoạch quan sát với bảo đảm chung; helper hiện lấy initial plan dưới AQE

Vị trí: `09-spark/ch23-spark-sql-dataframes.md:118`, `:170`, `:177`, `:380`; `09-spark/spark-sql-lab/src/main/scala/vn/scalaeng/sparksql/SqlLab.scala:105`; `solutions/ch23-code/src/test/scala/vn/scalaeng/solutions/ch23/Ch23SolutionsSuite.scala:62`.

`latestVersion(orders.coalesce(1))` có Window/Sort nhưng **không Exchange**. Window yêu cầu clustered distribution, không phải lúc nào partitionBy cũng tạo shuffle mới. `planOf` gọi executedPlan trước action trả `AdaptiveSparkPlan isFinalPlan=false`; probe cùng query sau collect chuyển true và có `AQEShuffleRead coalesced`. Test bài 6 collect một Dataset rồi dựng Dataset mới để lấy plan, nên chưa kiểm tra final plan của query đã chạy.

Đề xuất: ghi “với nguồn/cấu hình lab này” ở kết luận về Exchange/Sort và nêu điều kiện distribution đã được đáp ứng; Sort ở MapGroups là sort **trong partition theo khóa nhóm**, không global order của toàn output. Khi minh họa AQE, giữ cùng một biến Dataset, in trước/sau collect, phân biệt initial/final. Giới hạn phát biểu UDF/builtin theo predicate mà nguồn hỗ trợ; không phải mọi builtin đều pushdown xuống Parquet. [WindowExecBase 4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowExecBase.scala) và [Spark AQE](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html) là nguồn đối chiếu. Phần mapGroups không partial aggregation nhìn chung đúng theo [KeyValueGroupedDataset API](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/KeyValueGroupedDataset.html).

### m3 — Benchmark nằm trong unit test mặc định 30 giây; tỷ lệ thời gian không phải điều cần ghi nhớ

Vị trí: `solutions/ch23-code/src/test/scala/vn/scalaeng/solutions/ch23/Ch23SolutionsSuite.scala:40`, `:46`; `solutions/ch23-solutions.md:35`, `:42`.

Một test chạy 2 triệu dòng, nhiều lần countDistinct và 6 lần đo đã timeout ở lượt đầu. `.sortBy(_._1)` còn đổi thứ tự các cặp thời gian nhưng bảng gọi là “lần đo 1/2/3”. Tỷ lệ cũng phụ thuộc JIT, cache, dữ liệu, partition, tải máy; câu “tỷ lệ mới là điều cần nhớ” dễ dạy thành một bảo đảm hiệu năng.

Đề xuất: tách test correctness nhỏ với assertion giá trị thật; benchmark là entry point/nhóm riêng có warm-up và timeout phù hợp, in đúng thứ tự hoặc gọi là mẫu đã sắp xếp. Không assert tỷ lệ nhanh/chậm cố định; ghi rõ số đo của máy tác giả là một quan sát, người học cần đo workload của mình. Lượt chạy lại tách các JVM khác của reviewer **6/6 pass**, nên đây là rủi ro độ ổn định dưới tải, không phải kết luận test luôn đỏ.

### m4 — Handoff chưa đủ bằng chứng và mục bàn giao theo AGENTS.md

Vị trí: `.team/handoffs/T11-ch23-spark-sql.md:3`, `:4`.

Handoff chỉ có ba bullet: chưa có lệnh chính xác/output tóm tắt test thật, dependency/version + URL chính thức/ngày tra cho từng dependency, đề xuất PROGRESS/glossary/compatibility-matrix, và giới hạn chưa kiểm chứng. Bài/code có một phần thông tin nhưng handoff vẫn chưa đáp ứng mục 2.3 của AGENTS.md. Bổ sung các mục này; nếu không có đề xuất hay phần chưa chạy, ghi rõ thay vì bỏ trống.

### m5 — Snippet window và schema chưa đủ rõ để người mới chép chạy

Vị trí: `09-spark/ch23-spark-sql-dataframes.md:35`, `:110`.

Schema rút gọn đặt `...` sau các field và bỏ cột giữa, trong khi reader CSV áp theo vị trí; cần gắn nhãn pseudocode/rút gọn và dẫn Schemas.orders đầy đủ. Dòng `.withColumn(...)` đứng riêng sau `val w2 = ...` thiếu DataFrame receiver; thêm ví dụ `paidOrders.withColumn(...)` với cách tạo paidOrders/revenue_vnd hoặc dẫn rõ runningTotal. Các giải thích Window/Join nên dùng những phản ví dụ ở M2/M3 để hoàn thiện phần ưu/nhược và lỗi thiết kế theo template.

### m6 — Đã tìm đúng cờ JVM nhưng vẫn gán nhầm lỗi decode cho timestamp

Vị trí: `09-spark/ch23-spark-sql-dataframes.md:322`, `:330`; `09-spark/spark-sql-lab/project.scala:14`; `solutions/ch23-code/project.scala:12`; `.team/handoffs/T11-ch23-spark-sql.md:4`.

Đã tái hiện trên JVM thiếu **riêng** cờ mở sun.util.calendar, xác nhận package chưa mở. Kết quả tách từng kiểu:

```text
select(updated_at).collect()   -> thành công (TimestampType -> java.sql.Timestamp)
select(day).collect()          -> EXPRESSION_DECODING_FAILED
  createexternalrow(static_invoke(DateTimeUtils.toJavaDate(input[0, date, true])), ...)
  Caused by IllegalAccessException: ... sun.util.calendar.ZoneInfo ... SparkDateTimeUtils
collect() toàn bộ Row          -> cùng lỗi do cột day
as[OrderRow].collect()         -> thành công (LocalDate/Instant)
```

Nguồn [SparkDateTimeUtils 4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala#L252) cho thấy MethodHandle tới ZoneInfo được dùng trong `toJavaDate`. Chỉnh tiêu đề/giải thích thành trường hợp decode **DateType sang java.sql.Date** trong môi trường này, giữ root cause và cách thêm cờ vốn đúng. Không suy từ Row chứa cả date/timestamp rằng timestamp là cột gây lỗi; ghi rõ các đường decode khác có thể không đi qua code đó. Đây là lỗi diễn giải nguyên nhân, không phải transcript exception giả hay code build hiện tại bị thiếu cờ.

## Nguồn và giới hạn

Ngày tra: 17/10/2026. [Spark 4.2.0 overview](https://spark.apache.org/docs/4.2.0/) xác nhận Scala 2.13 và JDK 21; dependency 4.2.0/munit 1.2.4 đã được resolve và chạy thật. [GlobalMentor README](https://github.com/globalmentor/hadoop-bare-naked-local-fs) khớp artifact 0.1.0 và BareLocalFileSystem. [JavaModuleOptions 4.2.0](https://github.com/apache/spark/blob/v4.2.0/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java) có đủ 15 cờ add-opens đang khai báo, gồm sun.util.calendar; không phải toàn bộ JVM options của spark-submit chỉ có 15 cờ này.

Probe input rỗng: latestVersion/revenueBySegment/runningTotal/typed aggregate/top đều trả 0 dòng, không phát hiện lỗi. Null-safe join 1 vs 2 và top-1 sample đã kiểm tra qua suite; top-N dùng dense_rank giữ các mức doanh thu, có thể ra hơn N SKU khi hòa (probe N=2, doanh thu 100/100/90 ra 3 dòng), cần gọi rõ policy chấp nhận ties. Hai lượt đảo input nhiều-SKU ở probe đầu và đổi shuffle partitions=1/2/3/7 với AQE bật cho cùng kết quả; sau đó thí nghiệm AQE disabled đã tái hiện đổi thứ tự/running total như M2, nên không dựa vào một lần chạy ổn định để khẳng định tính xác định.

Tiêu chí review lại: sửa M1–M7 ở bài/code/lời giải tương ứng, bổ sung regression cho miền dữ liệu/overflow/ties/cardinality/normalization/null/header/refund, chạy lại suite cùng các test mới; lead kiểm chứng sbt và cập nhật handoff/trạng thái. Các output sample hiện tại đã khớp, không cần thay các con số đúng chỉ để che edge cases.

Chưa chạy cluster nhiều máy, Docker/database, hay sbt; không suy số đo local thành bảo đảm production. Mini project CTR mới là đề bài, chưa có implementation/golden Spark để kiểm chứng. Transcript lỗi compiler ở mục 10.1 chưa được tái hiện độc lập trong lượt này. Những lỗi probe cố ý kích hoạt không được tính là test fail của tác giả.
