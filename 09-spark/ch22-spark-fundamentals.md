# Chương 22 — Spark Fundamentals

> **Phiên bản:** **Scala 2.13.18** · JDK 21 · Apache Spark **4.2.0** (`spark-sql_2.13`) · hadoop-bare-naked-local-fs 0.1.0 (chỉ cần trên Windows) · munit 1.2.4
> **Code:** [`spark-fundamentals-lab/`](spark-fundamentals-lab/) — 6 thí nghiệm có số đo: lazy evaluation, cache, narrow/wide, stage, partition, fault tolerance
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **12/12 test pass** (`scala-cli test .` và `sbt test`) với Spark 4.2.0 chạy
> local mode trên **Windows 11** + JDK 21. Mọi kế hoạch thực thi, số đếm và thông báo lỗi trong bài là output thật.
> Đã qua review chéo (Codex): 5 MAJOR + 3 MINOR đã sửa, mỗi lỗi có test hồi quy hoặc phản ví dụ chạy thật (mục 10).

---

## Vị trí trong lộ trình

Chương 19–21 xử lý dữ liệu trên **một JVM**; Chương 20 kết thúc bằng câu hỏi: khi dữ liệu không vừa một máy thì sao? Apache Spark trả lời bằng cách chia dữ liệu thành
**partition**, gửi **task** tới nhiều **executor**, và ghi nhớ cách tính (**lineage**) để tính lại phần bị mất. Chương này dạy mô hình thực thi đó. Chương 23 đi vào
DataFrame/SQL; Chương 24–26 xây pipeline batch có thể vận hành; Project 3 ghép lại.

## 0. Từ Scala 3 về Scala 2.13 — vì sao và những gì thay đổi

**Spark 4.x không có bản build cho Scala 3.** Trên Maven Central (17/10/2026) chỉ có `spark-sql_2.13`; khai báo Spark trong project Scala 3 thì không tải được:

```
[error] .\A.scala:2:15
[error] Error downloading org.apache.spark:spark-sql_3:4.2.0
[error]   not found: C:\Users\VuTT\AppData\Local\ScalaCli\cache\local-repo\1.9.1\org.apache.spark\spark-sql_3\4.2.0\ivys\ivy.xml
```

Tài liệu Spark yêu cầu ứng dụng Scala dùng **cùng phiên bản Scala** mà Spark được biên dịch. Vì vậy (xem `00-roadmap/04-compatibility-matrix.md`):

- Nhánh Spark là **build riêng** với `scalaVersion := "2.13.18"`. Không trộn artifact `_3` và `_2.13` trên một classpath.
- Ranh giới giữa thế giới Scala 3 (backend, xử lý dữ liệu Chương 19–21, Kafka) và Spark là **dữ liệu**: file Parquet/Avro/JSON, bảng database, Kafka topic —
  không chia sẻ class qua classpath. Chương 21 đã chuẩn bị đúng điều này.

Những gì bạn đã quen ở Scala 3 nhưng **viết khác** ở Scala 2.13:

| Scala 3 | Scala 2.13 | Ghi chú |
|---|---|---|
| Thụt lề (`if x then`, `def f =` xuống dòng) | Ngoặc nhọn `{ }`, `if (x) ... else ...` | Scala 2.13 không hiểu cú pháp thụt lề |
| `@main def run() = ...` | `object App { def main(args: Array[String]): Unit = ... }` | |
| `given` / `using` | `implicit val` / `implicit` tham số | `import spark.implicits._` là implicit kiểu Scala 2 |
| `enum Status: case Paid, Refunded` | `sealed trait Status; case object Paid extends Status ...` | |
| `opaque type UserId = Long` | `final case class UserId(value: Long) extends AnyVal` | value class — có thể bị boxing |
| `A \| B` (union type) | không có | dùng `Either` hoặc sealed trait |
| `extension (s: String) def ...` | `implicit class RichString(s: String) { def ... }` | |
| `import x.*` | `import x._` | Scala 2.13 vẫn hiểu `_` |
| `derives Codec` | macro/thư viện riêng | Spark dùng `Encoder` (mục Chương 23) |

Viết cú pháp Scala 3 trong project Scala 2.13 cho lỗi ngay ở bộ phân tích cú pháp:

```
[error] .\C.scala:2:1
[error] expected class or object definition
[error] enum Status:
[error] ^
```

Tư duy (hàm thuần, bất biến, `Option`/`Either`, ADT, Monoid ở Chương 20) **giữ nguyên** — chỉ cú pháp đổi.

## 1. Mục tiêu học tập

1. Giải thích kiến trúc Spark: **driver**, **executor**, **cluster manager**; job → **stage** → **task**.
2. Phân biệt **transformation** (lazy) và **action**; chứng minh lazy evaluation bằng số đếm.
3. Hiểu **partition** và **shuffle**; phân biệt phụ thuộc **narrow** và **wide**; đọc toán tử `Exchange` trong kế hoạch vật lý.
4. Giải thích vì sao shuffle tách **stage**; dự đoán số stage của một job.
5. Dùng `repartition` vs `coalesce` đúng chỗ; biết `spark.sql.shuffle.partitions` và AQE.
6. Giải thích **fault tolerance** bằng lineage; quan sát một task thất bại được chạy lại.
7. Dùng **cache/persist** có chủ đích và đo hiệu quả.
8. Chạy Spark local trên Windows/Linux/macOS, trong sbt và Scala CLI.

## 2. Kiến thức tiên quyết

- Chương 20 (biến đổi thuần, Monoid, ba mô hình thực thi — phần "chia partition + shuffle" là mô phỏng của chính điều Spark làm).
- Chương 11 (JVM, heap, GC) — executor là JVM.
- Chương 21 (Parquet/Avro) — định dạng Spark đọc/ghi nhiều nhất.

## 3. Lý thuyết

### 3.1. Kiến trúc: driver, executor, cluster manager

```
                    ┌──────────── Driver (chương trình của bạn) ────────────┐
                    │ SparkSession → xây kế hoạch → chia job thành stage/task │
                    └───────────────┬───────────────────────┬──────────────┘
                                    │ gửi task               │ nhận kết quả nhỏ
              ┌─────────────────────▼──┐          ┌──────────▼─────────────┐
              │ Executor 1 (JVM)       │  shuffle │ Executor 2 (JVM)       │
              │ task task task         │◄────────►│ task task              │
              │ bộ nhớ cache           │          │ bộ nhớ cache           │
              └────────────────────────┘          └────────────────────────┘
                 Cluster manager (YARN / Kubernetes / Standalone) cấp tài nguyên cho executor
```

- **Driver**: chạy `main` của bạn, giữ `SparkSession`, biến code DataFrame thành kế hoạch, chia việc, theo dõi tiến độ. **Một** driver cho mỗi ứng dụng.
- **Executor**: JVM chạy **task** trên dữ liệu; mỗi executor có nhiều core (mỗi core chạy một task cùng lúc) và bộ nhớ để cache.
- **Cluster manager**: cấp máy/container cho executor. Ở **local mode** (`master("local[2]")`) driver và executor nằm trong **một** JVM, mỗi "core" là một thread —
  không có executor ở máy khác (nên không có mạng *giữa các máy*), dù Spark vẫn mở dịch vụ mạng cục bộ (driver có host/port thật). Mô hình job/stage/task/
  shuffle/lineage **giống** cluster; thứ khác là chi phí mạng, lỗi máy và tài nguyên.

Lab dùng `local[2,2]`: 2 thread chạy task, mỗi task được thử tối đa **2** lần. Mặc định `local[n]` chỉ thử **1** lần (task lỗi = job lỗi) — thí nghiệm fault tolerance
cần tham số thứ hai.

### 3.2. Transformation lazy, action thực thi

**Transformation** (`map`, `filter`, `select`, `groupBy`, `join`...) chỉ **mô tả** dữ liệu mới từ dữ liệu cũ. **Action** (`count`, `collect`, `show`, `write`...) mới bắt
Spark thực sự chạy.

```scala
val calls   = spark.sparkContext.longAccumulator("map calls")
val doubled = spark.range(0, n).map { x => calls.add(1); x * 2 }   // transformation: CHƯA chạy
val before  = calls.value                                           // 0
val rows    = doubled.filter($"value" % 3 === 0).count()           // action: chạy cả chuỗi
```

Số đo thật với n = 1.000.000: sau transformation `map` chạy **0** lần; sau action **1.000.000** lần.

**Vì sao lazy?** Khi chưa chạy, Spark nhìn được **cả chuỗi** và tối ưu: gộp `map` + `filter` vào một lượt duyệt, đẩy `filter` xuống sát nguồn, chỉ đọc cột cần
thiết (Chương 23). Một hệ thống chạy ngay từng bước (như `List` của Scala) không làm được điều này.

**Lỗi thiết kế thường gặp:** đo thời gian một transformation (đo được ~0 ms vì chưa chạy gì); đặt side effect (ghi database, gửi HTTP) trong `map` — chạy lúc nào,
bao nhiêu lần là không xác định (task có thể chạy lại, mục 3.6). `LongAccumulator` trong transformation chỉ dùng để **minh họa**: Spark có thể cộng trùng khi task chạy lại;
chỉ accumulator cập nhật trong **action** mới được bảo đảm cộng đúng một lần.

### 3.3. Partition, shuffle, narrow và wide

Dữ liệu của một DataFrame được chia thành **partition**; mỗi partition được một **task** xử lý. `spark.range(0, 100000, 1, numPartitions = 4)` → 4 partition → 4 task.

- **Narrow dependency**: mỗi partition **cha** đóng góp vào **tối đa một** partition con — dữ liệu không bị **phân tán lại** (`filter`, `map`, `select`, và cả
  `coalesce`). Một partition con *có thể* có nhiều cha: `coalesce(1)` gộp 4 partition vào 1 mà vẫn narrow (đo thật ở dưới).
- **Wide (shuffle) dependency**: bản ghi của **một** partition cha bị chia ra **nhiều** partition con theo khóa (hash/range). Phải **shuffle**: mỗi task ghi dữ liệu
  theo partition đích ra đĩa, task bên kia đọc (qua mạng trên cluster). Shuffle là phần **đắt nhất** của hầu hết job Spark.

`groupBy`, `join`, `distinct`, `repartition`, `orderBy` **thường** sinh shuffle — nhưng "thường" không phải "luôn". Planner chỉ chèn toán tử shuffle khi cách dữ liệu
đang được chia **chưa** đáp ứng yêu cầu của bước sau. Phản ví dụ chạy thật (`Experiments.dependencyFacts`):

```
[4] phản ví dụ: coalesce(1) narrow=true, partition con 0 có 4 cha; 1 partition: groupBy có shuffle=false, count = 1 stage; broadcast join: BroadcastExchange=true, shuffle=false
```

- `coalesce(1)`: 4 cha → 1 con, vẫn `NarrowDependency`.
- Dữ liệu chỉ có **1** partition: `groupBy` không cần shuffle (mọi khóa đã cùng chỗ), `count()` chỉ 1 stage.
- **Broadcast join** có `BroadcastExchange` — gửi **nguyên** bảng nhỏ tới mọi executor — nhưng **không** shuffle bảng lớn. Tìm chữ "Exchange" trong plan sẽ nhầm;
  shuffle là `Exchange hashpartitioning(...)`, `Exchange rangepartitioning(...)`, `Exchange RoundRobinPartitioning(...)`, `Exchange SinglePartition`.
  Lab dùng `Experiments.hasShuffleExchange` để phân biệt.

Kế hoạch vật lý thật (`df.queryExecution.executedPlan`):

```
-- chỉ filter/select (narrow):
*(1) Project [(id#122L * 10) AS x#126L]
+- *(1) Filter ((id#122L % 2) = 0)
   +- *(1) Range (0, 1000, step=1, splits=2)

-- groupBy (wide):
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[_groupingexpression#132L], functions=[count(1)], output=[k#128L, n#129L])
   +- Exchange hashpartitioning(_groupingexpression#132L, 200), ENSURE_REQUIREMENTS, [plan_id=313]
      +- HashAggregate(keys=[_groupingexpression#132L], functions=[partial_count(1)], output=[_groupingexpression#132L, count#134L])
         +- Project [(id#127L % 10) AS _groupingexpression#132L]
            +- Range (0, 1000, step=1, splits=2)
```

Đọc **từ dưới lên**:
- Narrow: `Range → Filter → Project`, tất cả trong cùng `*(1)` — **whole-stage codegen**: Spark sinh một đoạn mã Java duy nhất cho cả ba bước, chạy một lượt.
- Wide: `partial_count` (đếm **cục bộ** trong từng partition) → **`Exchange hashpartitioning(..., 200)`** (shuffle theo khóa vào 200 partition) → `count` (gộp các
  phần). Đây chính là "map-side combine" của Chương 20 — và nó đúng vì `count` là Monoid.

### 3.4. Job, stage, task — shuffle là ranh giới stage

Mỗi **action** sinh một (hoặc vài) **job**. Spark cắt job thành **stage** tại mỗi shuffle: các bước narrow liên tiếp gộp vào một stage; shuffle bắt đầu stage mới.
Mỗi stage có số **task** bằng số partition.

Số stage thật của **các query dưới đây, với cấu hình lab** (4 partition, AQE bật), đếm bằng `SparkListener` (`Experiments.stagesOf`):

| Job | Stage |
|---|---|
| `filter(...).collect()` | **1** |
| `filter(...).count()` | **2** |
| `groupBy(...).count().collect()` | **2** |

Dòng giữa là điều nhiều người không ngờ: **`count()` ở đây có shuffle**. `Dataset.count()` thực chất là một phép tổng hợp — đếm cục bộ từng partition, rồi
shuffle về **một** partition để cộng. Nhưng với dữ liệu chỉ 1 partition, cùng `count()` chỉ còn **1** stage (phản ví dụ mục 3.3). Muốn biết job có shuffle hay
không, **đọc kế hoạch**, đừng đoán theo tên hàm.

**Đo đúng cũng là kỹ năng.** Listener nhận sự kiện **bất đồng bộ** qua hàng đợi. Bản đầu của `stagesOf` "chờ 500 ms rồi đọc"; review chéo chèn một listener
chậm đứng trước trên cùng hàng đợi → action đã xong mà số đếm vẫn là **0**, và nó còn đếm cả stage của job khác chạy cùng lúc. Bản hiện tại gắn **job group**
riêng cho action (chỉ đếm stage của group đó), rồi chạy một job "rào chắn" nhỏ: sự kiện trên một hàng đợi được xử lý **theo thứ tự**, nên khi nhận JobEnd của
rào chắn thì mọi sự kiện trước đó đã xử lý xong. Chờ có hạn; hết hạn thì **ném lỗi** chứ không trả con số dở dang. Tăng `sleep` không phải cách sửa.

### 3.5. repartition, coalesce, shuffle partitions và AQE

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle? | **Có** (`Exchange` trong plan) | **Không** — chỉ gộp các partition có sẵn |
| Tăng số partition? | Được | Không (chỉ giảm) |
| Phân bố | `repartition(n)`: round-robin — số **dòng** tương đối đều (không bảo đảm đều về byte hay chi phí). `repartition(n, cols)`: **hash theo khóa** — đều hay không tùy phân bố khóa | Có thể lệch |
| Dùng khi | Cần nhiều song song hơn, hoặc gom theo cột (`repartition($"day")`) | Giảm số file đầu ra mà không trả giá shuffle |

Số đo thật: ban đầu 4 partition; `repartition(8)` → 8 (Exchange = true); `coalesce(2)` → 2 (Exchange = false).

**Hash không chữa được khóa nóng.** Mọi bản ghi **cùng khóa** về **cùng** partition. 1.000 dòng, một khóa chiếm 90%, `repartition(8, $"k")` (output thật):

```
[5] repartition(8, k) với khóa nóng 90%: số dòng mỗi partition = 9, 11, 14, 12, 15, 16, 12, 911
```

Một task gánh gần hết việc, tăng `n` cũng không giúp. Đó là **data skew** — Chương 25 đo và xử lý (AQE skew join, salting).

**`spark.sql.shuffle.partitions`** (mặc định **200**): số partition **mặc định** cho shuffle của join/aggregation khi không có chỉ định khác. Không phải mọi
shuffle: `repartition(8)` dùng 8; tổng hợp toàn cục (`count()`) dồn về `SinglePartition` = 1; và AQE có thể đổi con số sau khi chạy. Với dữ liệu nhỏ, 200 task gần như rỗng là lãng phí. **AQE**
(Adaptive Query Execution, bật mặc định) đo kích thước thật sau shuffle và **gộp** các partition nhỏ: groupBy 100.000 dòng thành 10 khóa → plan ghi 200, **sau khi
chạy** chỉ còn **1** partition. Plan ở mục 3.3 ghi `isFinalPlan=false` vì AQE còn điều chỉnh lúc chạy. Chương 25 bàn chi tiết.

### 3.6. Fault tolerance bằng lineage

Spark không sao lưu từng partition. Nó ghi **lineage** — chuỗi transformation tạo ra partition:

```
(4) MapPartitionsRDD[95] at map at Experiments.scala:94 []
 |  ParallelCollectionRDD[94] at parallelize at Experiments.scala:94 []
```

Khi một task thất bại (executor chết, máy mất), Spark **chạy lại task đó**: đọc lại nguồn và áp lại các transformation cho **đúng partition đó**. Thí nghiệm: task đầu tiên
của partition 0 cố ý ném exception; với `local[2,2]`:

```
ERROR Executor: Exception in task 0.0 in stage 28.0 (TID 44)
[6] fault tolerance: tổng = 1001000; số lần task thất bại rồi được chạy lại = 1
```

Log `ERROR` là thật, nhưng **job vẫn thành công** và tổng đúng (`2 + 4 + ... + 2000 = 1.001.000`): `task 0.0` (lần thử 0) lỗi, lần thử 1 thành công.

**Hệ quả thiết kế:** vì task **có thể chạy nhiều lần**, transformation phải **thuần** và **xác định**. `map` gửi email sẽ gửi hai lần; `map` dùng `Random` không seed sẽ cho
kết quả khác ở lần chạy lại. Đây là lý do mọi chương trước nhấn mạnh hàm thuần.

**Giới hạn:** lineage dài (hàng trăm bước, vòng lặp) làm tính lại tốn kém → dùng checkpoint (ghi ra lưu trữ bền). Lỗi *dữ liệu* (một dòng hỏng làm mọi lần thử đều
lỗi) không được cứu bằng chạy lại — sau `maxFailures` lần, job thất bại.

### 3.7. Cache và persist

Không cache, **mỗi action tính lại từ đầu** theo lineage. Số đo thật, 1.000.000 dòng, hai action liên tiếp trên cùng Dataset:

| | Số lần `map` chạy |
|---|---|
| Không cache | **2.000.000** (tính 2 lần) |
| `cache()` | **1.000.000** (action thứ hai đọc từ bộ nhớ) |

Bảng trên là **quan sát của thí nghiệm này**, không phải bảo đảm chung: dữ liệu đã cache vẫn có thể bị **đẩy ra** (evict) khi thiếu bộ nhớ, hoặc mất cùng
executor — khi đó Spark lặng lẽ **tính lại** theo lineage. Cache là tối ưu, không phải lưu trữ bền (muốn bền: ghi file hoặc checkpoint).

`Dataset.cache()` = `persist(StorageLevel.MEMORY_AND_DISK)`: không đủ RAM thì ghi phần còn lại ra **đĩa** cục bộ (khác `RDD.cache()` = `MEMORY_ONLY`).
`persist(level)` chọn mức khác (`MEMORY_ONLY`, `DISK_ONLY`, có/không serialize...).

**Khi nào cache:** một Dataset được dùng cho **nhiều** action/nhánh (ví dụ vừa ghi kết quả vừa tính báo cáo chất lượng), và **tính lại tốn hơn** đọc lại từ
RAM/đĩa — kể cả khi dữ liệu lớn hơn RAM, nếu tính lại rất đắt (join lớn), `MEMORY_AND_DISK` vẫn có thể đáng. Quyết định bằng **đo**, không theo cảm giác.
**Khi nào không:** chỉ dùng một lần (cache chỉ tốn bộ nhớ, còn thêm chi phí ghi); dữ liệu rẻ để đọc lại từ nguồn (Parquet có pruning). **Luôn `unpersist()`**
khi xong — tốt nhất trong `finally`, để dọn cả khi action lỗi. `cache()` cũng **lazy** — chỉ lưu khi action đầu tiên chạy.

### 3.8. Chạy Spark trên máy học tập

- **Linux/macOS:** chạy thẳng.
- **Windows:** Spark tính toán bình thường, nhưng **ghi file** cần `winutils.exe` của Hadoop:

  ```
  WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: ... HADOOP_HOME and hadoop.home.dir are unset.
  Exception in thread "main" java.lang.RuntimeException: java.io.FileNotFoundException: ... HADOOP_HOME and hadoop.home.dir are unset.
  ```

  Lab dùng thư viện **thuần Java** `com.globalmentor:hadoop-bare-naked-local-fs` thay hệ thống file local của Hadoop:
  `.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")` — ghi/đọc Parquet thành công, dòng `WARN Shell` vẫn in nhưng vô hại.
  Cách khác: WSL2 (chạy như Linux) hoặc Docker. Tránh tải `winutils.exe` từ nguồn không chính thức.
- **JDK 17+ và `--add-opens`:** `spark-submit` tự thêm 15 cờ `--add-opens` (cùng vài cờ khác) — danh sách thật nằm trong lớp
  `org.apache.spark.launcher.JavaModuleOptions` của spark-launcher 4.2.0. Khi chạy Spark **trong** sbt, Scala CLI hay test, bạn không có các cờ đó và phải tự khai báo.
  Thiếu cờ không phải lúc nào cũng lỗi ngay: 6 thí nghiệm của chương này chạy được cả khi bỏ hết. Nhưng ở Chương 23, `collect()` một cột **timestamp** thất bại:

  ```
  org.apache.spark.SparkRuntimeException: [EXPRESSION_DECODING_FAILED] Failed to decode a row to a value of the expressions: ...
  Caused by: java.lang.IllegalAccessException: symbolic reference class is not accessible: class sun.util.calendar.ZoneInfo,
    from interface org.apache.spark.sql.catalyst.util.SparkDateTimeUtils (unnamed module @11e1e137)
  ```

  — thiếu `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED`. Lỗi kiểu này xuất hiện **muộn**, ở một tính năng bất kỳ, với thông báo không nhắc gì tới
  `--add-opens`. Vì vậy mọi lab Spark của giáo trình khai báo **đủ 15 cờ** giống `spark-submit` (xem `project.scala`, `build.sbt`), để môi trường test giống môi trường
  chạy thật.
- **sbt:** Spark phải chạy trong JVM **riêng** (`fork := true`) để nhận `javaOptions`; `Test / parallelExecution := false` vì mỗi JVM chỉ có **một SparkContext**
  hoạt động (nhiều `SparkSession` qua `newSession()` thì được — chúng dùng chung context, cấu hình SQL riêng). Chạy các suite tuần tự với một session dùng chung
  tránh việc suite này đổi cấu hình/cache ảnh hưởng suite kia.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Driver / executor / cluster manager | Điều phối / nơi chạy task / nơi cấp tài nguyên |
| Job / stage / task | Việc do một action sinh ra / đoạn giữa hai shuffle / việc trên một partition |
| Partition | Một phần dữ liệu, đơn vị song song |
| Transformation / action | Mô tả dữ liệu mới (lazy) / bắt Spark chạy |
| Lazy evaluation | Trì hoãn thực thi đến khi cần kết quả |
| Narrow / wide dependency | Mỗi partition cha góp vào tối đa một con (không phân tán lại) / bản ghi của một cha bị chia ra nhiều con theo khóa (shuffle) |
| Shuffle / `Exchange` | Trao đổi dữ liệu theo khóa giữa các partition / toán tử trong plan — `Exchange hashpartitioning` là shuffle, `BroadcastExchange` thì không |
| Data skew | Một vài khóa chiếm phần lớn dữ liệu → một task gánh gần hết việc |
| DAG | Đồ thị có hướng không chu trình của các bước tính |
| Lineage | Chuỗi transformation tạo ra một dataset — dùng để tính lại |
| Whole-stage codegen (`*(1)`) | Sinh một đoạn mã cho nhiều toán tử liên tiếp |
| AQE (Adaptive Query Execution) | Điều chỉnh kế hoạch lúc chạy theo kích thước thật |
| Accumulator | Biến chỉ-cộng, gom từ executor về driver |
| Cache / persist / unpersist | Giữ kết quả trong bộ nhớ (và/hoặc đĩa) / giải phóng |
| Local mode | Driver + executor trong một JVM |

## 5. Ví dụ đơn giản

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Hello {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("hello").getOrCreate()
    import spark.implicits._                       // implicit kiểu Scala 2: Encoder, cú pháp $"cột"
    val df = Seq(("CMP-1", 450000L), ("CMP-1", 250000L), ("CMP-2", 990000L)).toDF("campaign", "amount")
    df.groupBy($"campaign").agg(sum($"amount").as("total")).show()
    spark.stop()
  }
}
```

Output (thật — đoạn code trên, chạy bằng `scala-cli run .` với `//> using scala 2.13.18` và `//> using dep org.apache.spark::spark-sql:4.2.0`):

```
+--------+------+
|campaign| total|
+--------+------+
|   CMP-1|700000|
|   CMP-2|990000|
+--------+------+
```

`show()` là action — trước nó, `groupBy`/`agg` chưa chạy gì. Thứ tự dòng của `groupBy` **không được bảo đảm**; cần thứ tự thì `orderBy`.

## 6. Ví dụ thực tế — nhìn lại Chương 20 bằng con mắt Spark

Chương 20 mô phỏng "chia partition + shuffle" trong một JVM. Spark làm đúng các bước đó, ở quy mô cụm máy:

| Chương 20 (`Execution.partitioned`) | Spark (`groupBy(...).agg(...)`) |
|---|---|
| (a) chia dữ liệu ngẫu nhiên thành n phần | dữ liệu sẵn ở nhiều partition (file, block) |
| (b) mỗi phần tổng hợp riêng | `HashAggregate(partial_count)` trong từng task |
| (c) gửi theo `hash(key) % n` | `Exchange hashpartitioning(key, 200)` |
| (d) gộp các phần | `HashAggregate(count)` sau shuffle |
| đúng vì phép gộp là Monoid | đúng vì `sum`, `count`, `min`, `max` là Monoid; `avg` được Spark tính bằng `(sum, count)` |

Những gì Spark làm thêm: chạy thật trên nhiều máy, tối ưu kế hoạch, ghi shuffle ra đĩa, chạy lại task lỗi, gộp partition bằng AQE.

## 7. Code đầy đủ

```
09-spark/spark-fundamentals-lab/            ← BUILD RIÊNG, Scala 2.13.18
├── project.scala / build.sbt / project/build.properties
├── src/main/resources/log4j2.properties    ← giảm log Spark xuống WARN
├── src/main/scala/vn/scalaeng/spark/
│   ├── LocalSpark.scala        ← SparkSession local[cores,maxFailures] + cấu hình Windows
│   ├── Experiments.scala       ← 6 thí nghiệm + phản ví dụ, mỗi cái trả về số đo
│   └── FundamentalsDemo.scala  ← chạy và in tất cả
└── src/test/scala/vn/scalaeng/spark/FundamentalsSuite.scala   ← 12 test (4 hồi quy từ review)
```

```bash
cd 09-spark/spark-fundamentals-lab
scala-cli test .                                            # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.spark.FundamentalsDemo
```

Lần đầu tải Spark và phụ thuộc (~vài trăm MB).

## 8. Giải thích code

- **`LocalSpark.session`** — `master(s"local[$cores,$maxFailures]")`; `spark.ui.enabled=false` (không mở web UI trong test); cấu hình `fs.file.impl` cho Windows.
- **`lazyEvaluation`** — đọc `calls.value` **trước** action để chứng minh chưa chạy; `spark.range(...).map` cần `Encoder[Long]` → `import spark.implicits._`.
- **`cacheEffect`** — hai action trên cùng Dataset, đếm lần `map` chạy; `unpersist(blocking = true)` dọn cache trước khi đo lần sau.
- **`physicalPlan`** — `df.queryExecution.executedPlan.toString`: chuỗi kế hoạch vật lý (giống `df.explain()` nhưng trả về để test so sánh).
- **`stagesOf`** — job group riêng + `SparkListener` (`onJobStart` ghi các stage của group, `onStageCompleted` đếm, `onJobEnd` của job rào chắn mở
  `CountDownLatch`); `await` có hạn, hết hạn ném `TimeoutException` (mục 3.4). Production dùng metric của Spark (Chương 26).
- **`hasShuffleExchange`** — dòng plan có `Exchange ` nhưng không phải `BroadcastExchange`.
- **`dependencyFacts`**, **`hashPartitionSizes`** — phản ví dụ mục 3.3 và 3.5; `spark_partition_id()` cho biết dòng nằm ở partition nào.
- **`faultTolerance`** — `TaskContext.get()` cho biết partition và **lần thử** (`attemptNumber`) của task đang chạy; chỉ lần thử 0 của partition 0 ném lỗi.
  `FaultCounter` là biến của JVM — chỉ đúng ở local mode (trên cluster, task chạy ở JVM khác; dùng accumulator hoặc log).
- **`FundamentalsSuite`** — một `SparkSession` cho cả suite (`beforeAll`/`afterAll`): tạo session mất vài giây.

## 9. Output mong đợi (output thật)

```
Spark 4.2.0 · Scala 2.13.18 · master=local[2,2]
[1] lazy: sau transformation map đã chạy 0 lần; sau action 1000000 lần; kết quả 333334 dòng
[2] cache: 2 action KHÔNG cache -> map chạy 2000000 lần; CÓ cache -> 1000000 lần
[3] kế hoạch vật lý — chỉ filter/select (narrow):        (xem mục 3.3)
[3] kế hoạch vật lý — groupBy (wide):                     (xem mục 3.3)
[4] stage hoàn thành: filter+collect = 1; filter+count = 2; groupBy+collect = 2
[4] phản ví dụ: coalesce(1) narrow=true, partition con 0 có 4 cha; 1 partition: groupBy có shuffle=false, count = 1 stage; broadcast join: BroadcastExchange=true, shuffle=false
[5] partition: ban đầu 4; repartition(8) -> 8 (Exchange=true); coalesce(2) -> 2 (Exchange=false)
[5] repartition(8, k) với khóa nóng 90%: số dòng mỗi partition = 9, 11, 14, 12, 15, 16, 12, 911
[5] spark.sql.shuffle.partitions = 200; AQE = true; số partition sau groupBy (sau khi chạy) = 1
ERROR Executor: Exception in task 0.0 in stage 39.0 (TID 62)
[6] fault tolerance: tổng = 1001000; số lần task thất bại rồi được chạy lại = 1
[6] lineage:
(4) MapPartitionsRDD[119] at map at Experiments.scala:174 []
 |  ParallelCollectionRDD[118] at parallelize at Experiments.scala:174 []
```

Số `#122L`, `plan_id`, `stage 39.0`, `TID 62`, RDD id thay đổi giữa các lần chạy. **Test:** `FundamentalsSuite 12/12`; `sbt test`: `Passed: Total 12, Failed 0`.

## 10. Lỗi thường gặp (thật)

1. **Khai báo Spark trong project Scala 3** → `Error downloading org.apache.spark:spark-sql_3:4.2.0 ... not found` (mục 0). Spark là build Scala 2.13 riêng.
2. **Cú pháp Scala 3 trong project 2.13** → `expected class or object definition` tại `enum Status:` (mục 0).
3. **Ghi file trên Windows không có winutils** → `HADOOP_HOME and hadoop.home.dir are unset` (mục 3.8).
4. **"`count()` thì làm gì có shuffle"** — có, khi dữ liệu nhiều partition: 2 stage (mục 3.4). Và ngược lại, "groupBy luôn shuffle" cũng sai (1 partition,
   dữ liệu đã chia sẵn theo khóa). Đọc plan thay vì đoán.
5. **Đo thời gian transformation** → ~0 ms vì lazy; đo quanh **action**.
6. **Side effect trong transformation** → chạy lại khi task lỗi hoặc khi không cache (mục 3.6, 3.7): ghi trùng, gửi trùng.
7. **`local[2]` và thí nghiệm lỗi** → không thử lại (maxFailures = 1), job thất bại ngay. Dùng `local[2,2]` để quan sát chạy lại.

Các lỗi dưới đây do **review chéo** (Codex) tìm ra trên bản đầu, bằng probe chạy thật; mỗi lỗi giờ có test hồi quy:

8. **Định nghĩa narrow/wide theo "một-một / nhiều-một"** — `coalesce(1)` là nhiều-cha→một-con nhưng narrow. Định nghĩa đúng nằm ở chỗ dữ liệu có bị
   **phân tán lại** theo khóa không (mục 3.3).
9. **Tìm chữ "Exchange" để phát hiện shuffle** — trúng cả `BroadcastExchange` (không phải shuffle). Dùng `hasShuffleExchange`.
10. **"repartition chia đều"** — chỉ round-robin `repartition(n)` đều theo số dòng; `repartition(n, cols)` dồn khóa nóng vào một partition (mục 3.5).
11. **Đo bằng listener rồi `sleep`** — listener chậm khác trên cùng hàng đợi làm số đếm = 0; job khác chạy cùng lúc bị đếm lẫn (mục 3.4).
12. **"Tắt broadcast là buộc sort-merge join"** và **RDD/DataFrame "cho cùng kết quả"** khi tổng vượt `Long` — xem lời giải bài 2 và bài 6.

## 11. Cách debug

- **`df.explain()`** / `df.explain("formatted")`: kế hoạch vật lý; tìm `Exchange` (shuffle), `BroadcastHashJoin`, `*(n)` (codegen).
- **Spark UI** (`http://localhost:4040` khi chạy, lab tắt trong test): job, stage, task, thời gian, dữ liệu shuffle — công cụ debug chính ở Chương 25.
- **`rdd.getNumPartitions`**, **`rdd.toDebugString`**: số partition, lineage.
- **Log:** sửa `log4j2.properties` (`rootLogger.level = info`) để thấy Spark làm gì; dòng `ERROR Executor: Exception in task X.Y` cho biết task X, lần thử Y.
- **Tái hiện nhỏ:** cùng logic trên `Seq(...).toDF` 10 dòng trong test — nhanh hơn nhiều so với chạy trên dữ liệu thật.

## 12. Bài tập

1. **(Cơ bản)** Viết lại `Order` và `OrderStatus` của Chương 15 bằng cú pháp Scala 2.13 (sealed trait, case object, value class cho id); biên dịch bằng Scala 2.13.
2. **(Cơ bản)** Dự đoán **số stage** của: (a) `select` + `write.parquet`; (b) `distinct().count()`; (c) `join` hai DataFrame không broadcast. Kiểm chứng bằng `stagesOf`.
3. **(Trung bình)** Chứng minh `cache` có lợi khi một Dataset được dùng cho **hai nhánh** (đếm theo campaign và đếm theo ngày); đo số lần `map` chạy có/không cache.
4. **(Trung bình)** Đo ảnh hưởng của `spark.sql.shuffle.partitions` (200 vs 4) khi **tắt AQE** trên groupBy 1 triệu dòng: số partition sau shuffle và thời gian.
5. **(Nâng cao)** Thí nghiệm "lỗi dữ liệu không cứu được bằng chạy lại": task **luôn** lỗi ở một dòng xấu → job thất bại sau `maxFailures` lần; in số lần thử.
6. **(Nâng cao)** Viết `Execution.partitioned` của Chương 20 bằng **RDD API** (`mapPartitions` + `reduceByKey`) và so kết quả với `groupBy` của DataFrame.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch22-solutions.md`](../solutions/ch22-solutions.md)
- Code + test: [`solutions/ch22-code/`](../solutions/ch22-code/)

## 14. Câu hỏi kiểm tra

1. Driver và executor làm gì? Ở local mode chúng nằm ở đâu?
2. Vì sao Spark lazy? Lợi ích cụ thể?
3. Narrow và wide khác nhau thế nào? Tìm shuffle trong plan bằng toán tử nào?
4. Vì sao `count()` trong lab có 2 stage? Khi nào nó chỉ có 1?
5. `repartition` và `coalesce` khác nhau? Muốn giảm số file đầu ra không tốn shuffle thì dùng gì?
6. AQE làm gì với 200 partition shuffle khi dữ liệu nhỏ?
7. Lineage giúp chịu lỗi thế nào? Vì sao transformation phải thuần?
8. Khi nào không nên cache?

<details><summary>Gợi ý đáp án</summary>

1. Driver: chạy main, lập kế hoạch, chia task. Executor: chạy task, cache. Local mode: cùng một JVM, core = thread.
2. Để thấy cả chuỗi và tối ưu (gộp bước, đẩy filter xuống nguồn, chỉ đọc cột cần).
3. Narrow: mỗi partition cha góp vào tối đa một partition con, không phân tán lại (kể cả `coalesce` nhiều-cha→một-con). Wide: bản ghi của một cha bị chia ra nhiều con theo khóa → shuffle. Tìm `Exchange hashpartitioning/rangepartitioning/SinglePartition/RoundRobin` — **không** tính `BroadcastExchange`.
4. `count` là tổng hợp: đếm cục bộ rồi shuffle về một partition để cộng — khi dữ liệu có nhiều partition. Dữ liệu 1 partition thì chỉ 1 stage.
5. `repartition` luôn shuffle, tăng/giảm được; `coalesce` chỉ gộp, không shuffle. Dùng `coalesce`.
6. Đo kích thước thật sau shuffle và gộp partition nhỏ (lab: 200 → 1).
7. Task lỗi được tính lại từ nguồn theo lineage; vì có thể chạy nhiều lần, side effect sẽ lặp và hàm không xác định sẽ cho kết quả khác.
8. Dataset chỉ dùng một lần; lớn hơn bộ nhớ; quên unpersist.
</details>

## 15. Mini project

**"Giải phẫu một job"**: lấy pipeline báo cáo CTR của Chương 19, viết lại bằng Spark (đọc `events.jsonl` bằng `spark.read.json`, lọc, groupBy campaign × ngày).
- Vẽ DAG: stage, task mỗi stage, nơi shuffle — dự đoán **trước**, rồi kiểm chứng bằng `explain()` và `stagesOf`.
- So báo cáo với golden của Chương 19 (cùng dữ liệu → cùng số).
- Thêm một task lỗi giả lập và chứng minh báo cáo không đổi.
- Đo có/không cache khi cùng dữ liệu sạch được dùng cho hai báo cáo.

## 16. Nguồn tài liệu chính thức

- Spark 4.2 documentation: https://spark.apache.org/docs/4.2.0/ (lưu ý: link `latest` có thể trỏ sang bản khác)
- RDD Programming Guide (transformation, action, lineage, persistence, accumulator): https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html
- Cluster Mode Overview (driver, executor, cluster manager): https://spark.apache.org/docs/4.2.0/cluster-overview.html
- Spark SQL — Adaptive Query Execution: https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html
- Spark configuration (`spark.task.maxFailures`, `spark.sql.shuffle.partitions`): https://spark.apache.org/docs/4.2.0/configuration.html
- Hadoop on Windows: https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
- hadoop-bare-naked-local-fs: https://github.com/globalmentor/hadoop-bare-naked-local-fs
- Scala 2 → 3 (đọc ngược để biết Scala 2.13 khác gì): https://docs.scala-lang.org/scala3/guides/migration/compatibility-intro.html

## 17. Checklist tự đánh giá

- [ ] Giải thích vì sao nhánh Spark dùng Scala 2.13 và ranh giới giữa hai thế giới là dữ liệu.
- [ ] Viết được code Spark bằng cú pháp Scala 2.13.
- [ ] Vẽ được driver/executor/task cho một job; dự đoán số stage.
- [ ] Chứng minh lazy evaluation và hiệu quả cache bằng số đếm.
- [ ] Đọc được `Exchange`, `HashAggregate(partial_...)`, `*(n)` trong plan.
- [ ] Phân biệt repartition/coalesce; giải thích shuffle partitions và AQE.
- [ ] Giải thích fault tolerance bằng lineage và hệ quả với side effect.
- [ ] Chạy được 8 test của lab trên máy mình (kể cả Windows).
