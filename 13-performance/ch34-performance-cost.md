# Chương 34 — Performance và Cost: đo để chọn sizing

> Phiên bản: Scala **3.9.0** · JDK **21** (JVM thực tế ghi ở mục 9) · circe-parser **0.14.17**, Avro **1.12.2**, slf4j-nop **2.0.17**, munit **1.2.4** · Trạng thái: **Đã biên dịch & chạy bằng Scala CLI, 22/22 test pass**, mục 9 có transcript; review vòng 1 yêu cầu sửa cách trình bày; Claude đã chạy sbt 15/15, bản sửa vòng 2 có handoff riêng.

## 1. Mục tiêu học tập

- Biến câu hỏi “thêm máy có nhanh hơn không?” thành giả thuyết có metric, ngân sách và điều kiện bác bỏ.
- Phân biệt throughput (tốc độ hoàn tất), service time (thời gian phục vụ) và event latency (thời gian từ lịch đến tới hoàn tất).
- Đọc wall-clock, process CPU, allocation, GC và phân vị trước khi chọn CPU/memory/parallelism.
- Tính node-hour, chi phí trên một triệu event, cache break-even và concurrency bằng Little's law.
- Chọn partition, serialization, query và batch size theo kết quả đúng, SLO và tổng chi phí; ghi rõ giới hạn suy luận local → cluster.

## 2. Kiến thức tiên quyết

**Vị trí trong lộ trình.** [Ch11 — JVM](../04-professional-engineering/ch11-jvm-fundamentals.md) → [Ch25 — Spark Performance](../09-spark/ch25-spark-performance.md) → [Ch32 — xử lý phân tán](../11-data-platform/ch32-advanced-distributed-processing.md) → **Ch34 — đo và chọn sizing/cost** → [P5 — nền tảng end-to-end](../14-projects/p5-data-platform/). Chương này chuyển metric thành quyết định tài nguyên; P5 áp dụng vào hệ thống tổng hợp.

Đọc [Ch11](../04-professional-engineering/ch11-jvm-fundamentals.md) về heap/JIT/GC, [Ch19](../08-data-processing/ch19-scala-data-processing.md) về xử lý file bằng iterator, [Ch21](../08-data-processing/ch21-data-formats.md) về định dạng và [Ch25](../09-spark/ch25-spark-performance.md) về Spark plan/metrics. [Ch32](../11-data-platform/ch32-advanced-distributed-processing.md) giải thích partitioner, shuffle, locality và retry. Ch34 dùng những bằng chứng đó để ra quyết định; phần triển khai join/salting/checkpoint nằm ở các chương trên.

[Ch29](../10-streaming/ch29-streaming-pipeline.md), [Ch30](../10-streaming/ch30-streaming-reliability.md) và [P4](../14-projects/p4-kafka-event-processing/) đặt câu hỏi về độ bền: một đường ghi nhanh nhưng mất receipt hoặc commit sai chưa phải tối ưu hợp lệ. Lab chương này chỉ đo xử lý trong JVM, không thay đường ghi bền bằng checksum rồi gọi đó là cùng một hệ thống.

## 3. Lý thuyết

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền. Các timing trích dưới đây giữ snapshot cũ; mô hình dùng input giả định được ghi riêng.

### 3.1. Bắt đầu từ câu hỏi và ranh giới phép đo

**Vấn đề.** “Latency 2 ms” chưa đủ để sizing. Cần biết đó là parse, chờ batch, ghi đĩa hay ack broker. Chốt input và hợp đồng kết quả trước khi đo. Ví dụ: nguồn 10.000 event/s, cap nào giữ p99 ≤6 ms? SLO (service-level objective, mục tiêu mức dịch vụ) 6 ms là **giả định bài học**.

**Mô hình.** Đo elapsed wall-clock bằng `System.nanoTime`. Lab giữ một lượt FIRST, chạy 12 warm-up rồi lấy 21 mẫu. Warm-up giúp nạp class/JIT; số lượt này chưa bảo đảm JVM ổn định. Phân vị nearest-rank lấy mẫu thứ `ceil(p×n)` sau sắp xếp. Với 21 mẫu, p99 chính là max.

Code cốt lõi trong [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala): luân phiên thứ tự case ở cả warm-up lẫn các round đo. Các biến thuộc hàm `samples`; đây là trích đoạn, không phải file chạy độc lập.

```scala
(0 until warmup).foreach { round =>
  val ordered = cases.drop(round % cases.size) ++ cases.take(round % cases.size)
  ordered.foreach { case (_, f) => sink = f() }
}
val raw = (0 until rounds).toVector.flatMap { round =>
  val ordered = cases.drop(round % cases.size) ++ cases.take(round % cases.size)
  ordered.map { case (label, f) => measure(label, round, events)(f) }
}
```

Hàm phân vị trong [Models.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala):

```scala
def percentile(values: Vector[Double], p: Double): Double =
  require(values.nonEmpty && values.forall(nonNegative), "samples must be finite, non-negative and non-empty")
  require(p.isFinite && p > 0 && p <= 1, "percentile must be in (0,1]")
  values.sorted.apply(math.ceil(p * values.size).toInt - 1)
```

Trích output thật từ [batch.txt](cost-lab/verification/batch.txt), cùng case trước/sau warm-up:

```text
FIRST batch1 ms=353.831000 checksum=6908601787964946812
BENCH batch1 rounds=21 p50Ms=41.698200 p95Ms=44.938400 p99Ms=45.235400 eventsPerSec=47963.701071 costPerMillion=0.00069497 mainAllocMedian=30112 cpuMedianMs=46.875000 gcCount=0 gcMs=0
```

**Lựa chọn và giới hạn.** Dùng lab wall-clock để học quy trình và tìm chênh lệch lớn; giữ raw, không chọn lượt đẹp nhất. Khi cần kết luận khác biệt nhỏ, chuyển sang JMH (Java Microbenchmark Harness), công cụ chuyên cho microbenchmark JVM có fork, warm-up và tiêu thụ kết quả. Xem [mẫu dead code](https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_08_DeadCode.java) và [forking](https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_12_Forking.java). **JMH chưa chạy trong T20**. Load test end-to-end cần thêm nguồn, queue và sink. Không biến timing thành assertion CI.

### 3.2. Resource sizing: CPU, concurrency và parallelism khác nhau

**Vấn đề.** Parallelism là số phần việc đang chạy. Concurrency gồm cả việc đang chờ trong hệ thống. Thêm thread không tạo thêm core; phải biết workload đang dùng CPU hay chờ I/O.

**Mô hình và phép đo.** Nếu CPU service demand là `D` giây CPU/event, số core khởi điểm là `ceil(λ×D/u)`. Little's law dùng trung bình cùng ranh giới, ở trạng thái ổn định: `L=λW`. Hàm `Models.concurrency` chia thêm utilization `u` để có dự phòng; đây là giả định sizing, không phải bảo đảm tail SLO. Nếu W gồm queue, L cũng gồm việc chờ và không trực tiếp là số connection đang bận.

Trong [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala), `os` là `com.sun.management.OperatingSystemMXBean`; đoạn `measure` giữ checksum qua field `@volatile sink`. Bộ đếm CPU bao toàn JVM, còn mốc wall chỉ bao `f()`:

```scala
val cpu0 = os.getProcessCpuTime
val start = System.nanoTime()
val value = f()
val ns = System.nanoTime() - start
val cpu = os.getProcessCpuTime - cpu0
val bytes = allocated - alloc0
sink = value
```

```scala
def concurrency(ratePerSecond: Double, meanResidenceMs: Double, targetUtilization: Double): Int =
  require(nonNegative(ratePerSecond) && nonNegative(meanResidenceMs), "rate and residence must be finite and non-negative")
  require(targetUtilization.isFinite && targetUtilization > 0 && targetUtilization <= 1, "utilization must be in (0,1]")
  val slots = math.ceil(ratePerSecond * meanResidenceMs / 1000 / targetUtilization)
  require(slots.isFinite && slots <= Int.MaxValue, "concurrency exceeds Int capacity")
  slots.toInt
```

Output thật: [cpu.txt](cost-lab/verification/cpu.txt) đo 60.000 input; [model-demo.txt](cost-lab/verification/model-demo.txt) tính với λ=500/s, mean W=20 ms, u=0,7 **giả định**:

```text
BENCH workers4-parts8-skewfalse rounds=21 p50Ms=7.056100 p95Ms=7.497700 p99Ms=7.867600 eventsPerSec=8503280.849194 costPerMillion=0.00000392 mainAllocMedian=2840 cpuMedianMs=0.000000 gcCount=0 gcMs=0
CONCURRENCY 15
```

`CPU_ms/wall_ms` xấp xỉ số core trung bình bận của tiến trình. Trên máy này, CPU delta có bước quan sát 15,625 ms; mẫu ngắn bằng 0 không có nghĩa không dùng CPU (mục 10.2). Bài tập `requiredConsumers` lấy mean của các residence sample rồi gọi `Models.concurrency`; xem [Exercises.scala](../solutions/ch34-code/Exercises.scala). Allocation chỉ đo thread gọi, không gồm worker. Case CPU có 12.000 bước overhead/task ở worker **và** 12.000 bước/task tính lại ở driver; cả hai nằm trong timing.

**Lựa chọn và lỗi thiết kế.** CPU-bound: giảm việc/event hoặc tăng core vừa đủ; nhiều worker có thể tranh memory bandwidth. I/O-bound: pool có trần hoặc async/fiber/virtual thread giúp chờ rẻ hơn nhưng không tăng quota DB. Dùng JFR/thread dump khi chưa rõ bottleneck. Không đưa p95 thay mean vào Little's law, coi vCPU là physical core hay ngoại suy scale-out tuyến tính.

### 3.3. Memory: ngân sách live state trước khi tăng heap

**Vấn đề.** Cache, state và queue phải sống bao lâu? Allocation là byte được cấp phát; live heap là đối tượng còn sống. RSS (resident set size, bộ nhớ tiến trình đang cư trú) còn gồm stack, metaspace và native/direct buffer.

**Mô hình.** `taskSlots ≤ floor((heap−liveCache−reserve)/peakBytesPerTask)`. Dự toán phần ngoài heap riêng. Không dùng allocation/event làm retained/event hoặc đưa toàn bộ RAM cho `-Xmx`.

[Probes.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala) đọc heap sau hai yêu cầu GC, rồi so khi giữ 300.000 Order với khi bỏ tham chiếu. `materialize` trả về trước GC để tránh local tạm còn giữ mảng trên frame:

```scala
private def afterGc(): Long =
  System.gc()
  Thread.sleep(100)
  System.gc()
  Thread.sleep(100)
  used
```

```scala
val before = afterGc()
sink = materialize(count)
val kept = afterGc()
println(s"MEMORY rows=$count beforeBytes=$before heldBytes=$kept deltaBytes=${kept - before} bytesPerRow=${(kept - before).toDouble / count} sum=$sink")
held = Array.empty[Order]
val released = afterGc()
```

Output thật của JVM riêng `-Xmx256m`, từ [memory.txt](cost-lab/verification/memory.txt):

```text
MEMORY rows=300000 beforeBytes=3778424 heldBytes=80312088 deltaBytes=76533664 bytesPerRow=255.11221333333333 sum=1529850000
STREAM releasedBytes=3920168 afterGcBytes=3922312 sum=1529850000 heapMaxBytes=268435456
```

**Lựa chọn và giới hạn.** Iterator, projection và queue có trần giảm live set; vẫn có allocation và GC. Tăng heap khi đã biết state bắt buộc phải giữ; spill đổi RAM lấy I/O. Probe dùng `System.gc()` nên chỉ ước lượng heap sau GC, **chưa đo peak streaming/RSS**. Dùng GC log, heap dump hoặc JFR khi cần sâu hơn. Với concurrency thật, phải đo lại peak/task trước khi chốt slot; memory có thể buộc tăng node count và cost.

### 3.4. Partition sizing và data skew: phải tính cả task cuối

**Vấn đề.** Quá ít task bỏ trống slot; quá nhiều task tốn overhead. Hot key (khóa có quá nhiều record) tạo task kéo đuôi dù còn core rảnh.

**Mô hình.** `ceil(inputBytes/targetBytes)` chỉ là partition count khởi điểm. Với task độc lập, cân bằng: `T≈work/slots+tasks×overhead/slots`. Task nóng tạo cận dưới `T≥max(taskTime)`. Hàm `makespan` trong [Models.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala) gán task tiếp theo cho slot rảnh sớm nhất:

```scala
def makespan(taskMs: Vector[Double], slots: Int, overheadMs: Double): Double =
  require(slots > 0 && nonNegative(overheadMs) && taskMs.forall(nonNegative), "invalid schedule")
  val loads = Array.fill(slots)(0.0)
  taskMs.foreach { ms =>
    val index = loads.indices.minBy(loads(_))
    loads(index) += ms + overheadMs
  }
  loads.max
```

Output thật từ [cpu.txt](cost-lab/verification/cpu.txt), cùng 4 worker/8 task; chỉ đổi sang 80% input ở task đầu:

```text
BENCH workers4-parts8-skewfalse rounds=21 p50Ms=7.056100 p95Ms=7.497700 p99Ms=7.867600 eventsPerSec=8503280.849194 costPerMillion=0.00000392 mainAllocMedian=2840 cpuMedianMs=0.000000 gcCount=0 gcMs=0
BENCH workers4-parts8-skewtrue rounds=21 p50Ms=20.093500 p95Ms=22.753800 p99Ms=23.260100 eventsPerSec=2986040.261776 costPerMillion=0.00001116 mainAllocMedian=2920 cpuMedianMs=15.625000 gcCount=0 gcMs=0
```

**Lựa chọn và giới hạn.** Tăng partition có thể giảm memory/task nhưng tăng scheduler/file overhead. Coalesce/AQE giảm task nhỏ; pre-aggregate hoặc salting có thể giảm skew nhưng thêm byte và phải giữ đúng join semantics. Lab chỉ có CPU task trong một JVM, chưa đo remote fetch/retry/shuffle. [Ch25 mục 9](../09-spark/ch25-spark-performance.md#9-output-mong-đợi) có Spark task count/AQE thật; [Ch32](../11-data-platform/ch32-advanced-distributed-processing.md) có TaskMetrics theo stage. Không gộp các lượt đó vào phân vị Ch34. Không cố định 128 MiB cho mọi nguồn hoặc tăng partition để “chữa” hot key vẫn ở cùng một task.

### 3.5. Serialization và network transfer: CPU đổi lấy bao nhiêu byte?

**Vấn đề.** Format gọn có bù được CPU và chi phí quản lý schema không? Phải round-trip đúng Unicode, duplicates và `Long amountMinor` trước khi so tốc độ.

**Mô hình.** `transferSeconds=bytes/effectiveBytesPerSecond`; `egressCost=GiB×assumedPricePerGiB`. Không chồng lấp: latency cộng encode, transfer và decode. Khi pipeline ổn định, bước chậm nhất chặn throughput. Bandwidth và giá ở mục 9.6 là **giả định**, chưa đo mạng.

Đo byte cùng 12.000 Order trong [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala); Avro raw có count 4 byte và schema ngoài gói, khác Avro container/Schema Registry wire format:

```scala
val rows = Vector.tabulate(12000)(Orders.fixture)
val json = Orders.json(rows)
val avro = Orders.avro(rows)
require(Orders.readJson(json) == rows && Orders.readAvro(avro) == rows)
require(Orders.query(rows, false) == Orders.query(rows, true))
println(s"PAYLOAD events=${rows.size} jsonBytes=${json.length} avroBytes=${avro.length} framing=jsonl-vs-count+raw-avro schemaOutsideAvro=true")
```

```scala
def transferSeconds(bytes: BigInt, effectiveBytesPerSecond: Double): Double =
  require(bytes >= 0 && effectiveBytesPerSecond.isFinite && effectiveBytesPerSecond > 0, "invalid transfer parameters")
  val seconds = bytes.toDouble / effectiveBytesPerSecond
  require(seconds.isFinite, "transfer estimate exceeds Double capacity")
  seconds
```

Output thật từ [local.txt](cost-lab/verification/local.txt); CPU encode được đo trong cùng harness mục 3.2:

```text
PAYLOAD events=12000 jsonBytes=1402760 avroBytes=845226 framing=jsonl-vs-count+raw-avro schemaOutsideAvro=true
BENCH json-encode rounds=21 p50Ms=18.584900 p95Ms=27.596300 p99Ms=30.419500 eventsPerSec=645685.475843 costPerMillion=0.00005162 mainAllocMedian=34548224 cpuMedianMs=15.625000 gcCount=10 gcMs=14
BENCH avro-encode rounds=21 p50Ms=4.464100 p95Ms=7.528600 p99Ms=8.447900 eventsPerSec=2688111.825452 costPerMillion=0.00001240 mainAllocMedian=7435376 cpuMedianMs=0.000000 gcCount=0 gcMs=0
```

**Lựa chọn và lỗi thiết kế.** JSON dễ đọc; Avro giảm tên field lặp nhưng cần quản lý schema evolution. Nén đổi CPU lấy byte; columnar hợp query ít cột (Ch21). Dùng khi byte/CPU thực sự là bottleneck. Không so format nén với format không nén rồi quy hết lợi ích cho encoding. Cộng header, TLS, retry, fan-out và replication khi đo end-to-end. Local shuffle read không là byte qua NIC; lab này không mở socket.

### 3.6. Query optimization: bỏ việc thừa trước khi mua máy

**Vấn đề.** Chỉ 1% Order được chọn thì có cần checksum 512 bước trên mọi row? Filter sớm tránh việc thừa nếu phép tính thuần và predicate xác định.

**Mô hình.** Bỏ fraction `f` công việc đắt làm giảm CPU/event. Tính lại cost/million từ rate mới, rồi cộng scan, validation và sink còn bắt buộc. Selectivity có thể đổi theo dữ liệu.

Hàm đầy đủ trong [Orders.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Orders.scala); `work` chỉ là checksum CPU, không có I/O:

```scala
def query(rows: Vector[Order], earlyFilter: Boolean): Long =
  rows.iterator.map { row =>
    if earlyFilter && row.id % 100 != 0 then 0L
    else
      val value = work(row.id + row.amountMinor, 512)
      if row.id % 100 == 0 then value else 0L
  }.foldLeft(0L)(_ ^ _)
```

Output thật từ [local.txt](cost-lab/verification/local.txt); lab/test đối chiếu cùng XOR -94954666026242113:

```text
BENCH filter-late rounds=21 p50Ms=11.807400 p95Ms=12.731500 p99Ms=12.949200 eventsPerSec=1016311.804462 costPerMillion=0.00003280 mainAllocMedian=291024 cpuMedianMs=15.625000 gcCount=0 gcMs=0
BENCH filter-early rounds=21 p50Ms=0.495400 p95Ms=0.709500 p99Ms=0.955700 eventsPerSec=24222850.222043 costPerMillion=0.00000138 mainAllocMedian=291024 cpuMedianMs=0.000000 gcCount=0 gcMs=0
```

**Lựa chọn và giới hạn.** Builtin/predicate pushdown, projection, index hoặc pre-aggregation có thể giảm scan/CPU, đổi lại storage và maintenance. Đo final plan, input bytes và shuffle khi dùng DE thật; code trên chưa chứng minh database index hay Spark pushdown. Ch25 có `PushedFilters` thật, nhưng một lượt builtin 417 ms/UDF 214 ms chưa đủ xếp hạng tốc độ. Không đẩy filter qua outer join làm đổi null semantics, làm mất duplicates hoặc bỏ side effect mà hợp đồng yêu cầu. Giữ cách đơn giản khi query chưa phải hot spot.

### 3.7. Caching: cần bao nhiêu lần dùng mới hoàn vốn?

**Vấn đề.** Kết quả có được dùng đủ nhiều trước eviction/invalidation không? Cache giữ kết quả để tái sử dụng, tốn RAM và cần quản lý dữ liệu cũ.

**Mô hình.** B là build **chưa đọc**, U là tính lại, R là đọc cache. Cache lợi khi `B+kR<kU`, tức `k>B/(U−R)` với U>R. Lấy floor rồi cộng 1 để không chọn trường hợp bằng nhau. Hàm trong [Models.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala):

```scala
def cacheBreakEven(buildMs: Double, uncachedMs: Double, cachedReadMs: Double): Option[BigInt] =
  require(Vector(buildMs, uncachedMs, cachedReadMs).forall(nonNegative), "invalid cache timing")
  if uncachedMs <= cachedReadMs then None
  else Some((BigDecimal(buildMs) / (BigDecimal(uncachedMs) - BigDecimal(cachedReadMs))).setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt + 1)
```

Build trong [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala) giữ mảng qua field `@volatile cacheSink`, giúp kết quả vẫn quan sát được:

```scala
val result = rows.map(r => Orders.work(r.id + r.amountMinor, 512)).toArray
cacheSink = result
result.length.toLong ^ result.last
```

Output thật từ [local.txt](cost-lab/verification/local.txt). Test/lab còn đối chiếu XOR của U và R:

```text
BENCH cache-build rounds=21 p50Ms=11.880400 p95Ms=12.997900 p99Ms=13.230800 eventsPerSec=1010067.001111 costPerMillion=0.00003300 mainAllocMedian=439936 cpuMedianMs=15.625000 gcCount=1 gcMs=1
BENCH cache-read rounds=21 p50Ms=0.119900 p95Ms=0.222300 p99Ms=0.250500 eventsPerSec=100083402.835696 costPerMillion=3.3E-7 mainAllocMedian=576024 cpuMedianMs=0.000000 gcCount=0 gcMs=0
BENCH no-cache rounds=21 p50Ms=12.139000 p95Ms=12.620900 p99Ms=13.321800 eventsPerSec=988549.303897 costPerMillion=0.00003372 mainAllocMedian=576128 cpuMedianMs=15.625000 gcCount=0 gcMs=0
```

**Lựa chọn và giới hạn.** Dùng cache khi kết quả đắt, dùng lại nhiều và đủ RAM. Recompute đơn giản, tránh stale; persist đổi RAM lấy I/O/storage. Theo median này, k=1 nhưng chỉ lợi 0,1387 ms, **chưa đủ bằng chứng chọn cache**; mục 9.4 tính chi tiết. Cần đo cả build và nhiều read trong một lượt. Ch25 cho phản ví dụ: materialize +hai consumer tổng 2249 ms, không cache 1191 ms; đây là snapshot Ch25, chưa chạy lại trong T20. Không chỉ đo cache-hit, giữ cache vô hạn hay gọi cache là checkpoint. TTL, miss và node-hour tăng vì RAM có thể đảo quyết định.

### 3.8. Batch size: throughput tăng nhưng event còn phải chờ

**Vấn đề.** Batch lớn khấu hao overhead nhưng event phải chờ đầy lô. Service throughput không đo cùng ranh giới với event latency.

**Mô hình.** Nguồn đều λ có mean fill wait xấp xỉ `(batch−1)/(2λ)` trước timeout; burst làm giả định này sai. Lab so cap 1/16/128, lịch 100 µs/event (10.000/s), timeout 5 ms. Cap 128 chỉ gom tối đa 51 event/lô; EOS flush sau event cuối. SLO p99 ≤6 ms là giả định.

Trong [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala), `paced` chờ lịch cố định; thread thức muộn vẫn giữ mốc arrival cũ. Sau khi đã ghi completion, code tính latency như sau:

```scala
completions.indices.toVector.map { i =>
  val scheduled = i.toLong * gapNs
  val latency = completions(i) - scheduled
  if round >= 0 then raw.append(s"$size,$round,$i,$scheduled,${completions(i)},$latency\n")
  latency / 1e6
}
```

Output thật từ [batch.txt](cost-lab/verification/batch.txt), 3 warm-up +9 round ×2000 event/cap:

```text
LATENCY batch=1 arrivalsPerSec=10000.0 maxWaitMs=5.0 rounds=9 events=18000 p50Ms=6.363600 p95Ms=12.087600 p99Ms=13.180500 meanMs=6.374900 maxMs=15.111600
LATENCY batch=16 arrivalsPerSec=10000.0 maxWaitMs=5.0 rounds=9 events=18000 p50Ms=8.270400 p95Ms=15.435600 p99Ms=16.713200 meanMs=8.325756 maxMs=22.970500
LATENCY batch=128 arrivalsPerSec=10000.0 maxWaitMs=5.0 rounds=9 events=18000 p50Ms=10.043700 p95Ms=17.437900 p99Ms=19.051000 meanMs=10.061335 maxMs=22.588200
```

**Lựa chọn và giới hạn.** Cap nhỏ giảm fill wait; cap lớn giảm overhead/event nhưng tăng RAM/replay. Timeout chỉ giới hạn fill wait. Cả ba cap chưa đạt SLO, nên chưa chọn candidate. Đây là nguồn theo lịch trong cùng JVM; completion/range/array vẫn có overhead harness, chưa có broker/ack. Không reset arrival khi consumer rảnh (coordinated omission, bỏ sót thời gian chờ). `parkNanos` có thể thức muộn; chưa tách scheduler khỏi tải nền. Không lấy service capacity khi input sẵn có để tính hóa đơn service đang chờ nguồn.

### 3.9. Cost trade-offs: lọc theo SLO trước, rồi mới chọn rẻ

**Vấn đề.** Cấu hình phải đúng dữ liệu, đủ throughput và đạt p99 trước khi so chi phí. Không có candidate hợp lệ thì trả `None` (bài tập 6).

**Mô hình.** Compute cost là `nodes×elapsedHours×pricePerNodeHour`. Trong cost/million, rate là **tổng fleet**, không nhân node vào rate lần nữa. Tất cả giá là **giả định học tập**, không phải báo giá cloud. Hàm trong [Models.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala) và lời gọi ModelDemo:

```scala
def costPerMillion(eventsPerSecond: Double, nodes: Int, pricePerNodeHour: BigDecimal): BigDecimal =
  require(eventsPerSecond.isFinite && eventsPerSecond > 0, "throughput must be positive and finite")
  require(nodes > 0 && pricePerNodeHour >= 0, "nodes must be positive and price non-negative")
  BigDecimal(1000000) / BigDecimal(eventsPerSecond) / 3600 * nodes * pricePerNodeHour
```

Output thật từ [model-demo.txt](cost-lab/verification/model-demo.txt): giá 0,12 đơn vị tiền/node-hour; fleet hai node xử lý 50.000/s so với một node nhận 10.000/s. Cả hai là input **giả định**, không phải benchmark fleet:

```text
COST 0.001333333333333333333333333333333333
COST_IDLE 0.003333333333333333333333333333333334
```

**Lựa chọn và giới hạn.** Service luôn bật dùng arrival thực và toàn thời gian cấp máy, gồm idle. Batch job cộng startup, sink, retry và quy tắc làm tròn billing. Thêm storage, network và recovery theo ranh giới. Scale-up đơn giản nhưng có trần; scale-out thêm replication/shuffle. Autoscaling giảm idle nhưng cần thời gian khởi động phù hợp. Thử đổi rate, giá, cache reuse và skew để xem ngưỡng đổi quyết định.

[Review P4/T15](../.team/reviews/T15-ch29-30-p4-review.md) đo snapshot 500/1000/2000 event: 5614/9844/29385 ms. Đây là bằng chứng trước sửa, chưa chạy lại trong Ch34; checksum local không thay đường ghi bền đó. Cần kiểm bottleneck/ownership trước khi tăng consumer; P5 nối tiếp quyết định kiến trúc. Không gọi tiền giả định là USD hoặc coi laptop là node cloud. Mục 9 tổng hợp quyết định và điều kiện phải đo lại.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong chương |
|---|---|
| Resource sizing | Chọn ngân sách CPU, memory, slot và số node theo workload/SLO |
| Throughput / capacity | Tốc độ hoàn tất quan sát / mức tối đa theo điều kiện đo |
| Service time / residence time | Thời gian phục vụ / thời gian ở trong ranh giới đã chọn, có thể gồm chờ |
| Tail latency, p95/p99 | Phân vị ở đuôi; định nghĩa nearest-rank trong lab |
| Warm-up / JIT / DCE | Chạy làm nóng / biên dịch tức thời / loại bỏ tính toán không được dùng |
| Allocation / live heap / RSS | Byte cấp phát / đối tượng còn sống trong heap / memory cư trú toàn tiến trình |
| Parallelism / concurrency | Công việc chạy đồng thời / công việc đang ở trong hệ thống |
| Skew / straggler | Lệch phân bố công việc / task chậm kéo đuôi |
| Effective bandwidth | Tốc độ byte thực dùng sau các overhead và tranh chấp |
| Materialization / break-even | Thực sự tạo kết quả cache / điểm tiết kiệm vượt chi phí tạo |
| Little's law | Quan hệ trung bình L=λW với điều kiện và ranh giới nhất quán |
| Headroom / utilization | Dự phòng / tỷ lệ sử dụng theo mô hình |
| Open-loop / coordinated omission | Nguồn theo lịch độc lập / bỏ sót latency khi nguồn đợi consumer rảnh |
| Node-hour / cost per million | Số node nhân giờ cấp phát / chi phí trên một triệu event |

## 5. Ví dụ đơn giản

Giả sử một fleet hai node **đã đo được** tổng 50.000 event/s. Chỉ riêng ví dụ sau dùng các con số **giả định**, không phải benchmark fleet của chương. Tính chi phí với 0,12 đơn vị tiền/node-hour:

```scala
Models.costPerMillion(50000, 2, BigDecimal("0.12"))
Models.concurrency(500, 20, .7) // 500/s × mean 20 ms, dự phòng theo u=0,7
Models.partitions(BigInt(1024).pow(3) * 100, BigInt(128) * 1024 * 1024)
```

[ModelDemo](cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala) gọi đúng các hàm này và in output thật ở mục 9.1. Giải thích từng phép tính: một triệu event cần 20 giây fleet; hai node dùng 40 node-second; chia 3600 đổi sang node-hour. Little's law cho trung bình 10 công việc trong ranh giới; chia 0,7 rồi ceil thành 15 slot theo policy dự phòng. 100 GiB /128 MiB =800 partition đầu; đây chưa là lựa chọn production.

## 6. Ví dụ thực tế (BE và DE)

**DE — chọn pipeline tổng hợp đơn hàng.** Chốt bốn trường của fixture, tổng amount và duplicates; đo encode/decode, filter placement, cache reuse. Với mục tiêu giả định một triệu event/ngày, job chạy một lần khác service mở cả ngày. Tính chi phí cả startup và cache build nếu đưa vào job; khi dữ liệu vượt heap, so iterator với live state trong memory probe. Nếu chuyển sang Spark, bổ sung TaskMetrics/plan và phép đo trên cluster, không dùng rate một JVM làm rate/node cố định.

**BE — chọn batch cho enrichment.** Mục tiêu giả định là nguồn đều 10.000/s và p99 ≤6 ms. Bắt đầu từ bảng service throughput để loại cấu hình thiếu headroom; dùng event latency để loại cấu hình chờ quá lâu; so CPU và cost sau đó. Nếu sink cần transaction/ack thì bổ sung đường đó vào benchmark. Một cap thắng trong checksum có thể thua khi DB lock/commit hoặc hot tenant chi phối.

Decision record (bản ghi quyết định) nên có các trường sau:

| Trường | Nội dung bắt buộc |
|---|---|
| Hợp đồng | Snapshot, schema, tiền/timezone/duplicate policy, durability và SLO |
| Baseline và giả thuyết | Một thay đổi, metric dự kiến thay đổi và kết quả phải giữ |
| Bằng chứng | CPU/RAM/JDK, input, warm-up, samples, phân vị và link raw |
| Mô hình | Đơn vị, giá giả định, startup/idle/recovery/network, điều kiện bị bỏ qua |
| Quyết định | Chọn/giữ baseline/chưa đủ dữ liệu; nêu điều kiện khiến phải đo lại |

## 7. Code đầy đủ

- [project.scala](cost-lab/project.scala), [build.sbt](cost-lab/build.sbt), [build.properties](cost-lab/project/build.properties): cùng pin Scala/dependency; sbt 1.12.11 để lead kiểm chứng.
- [Models.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala): công thức, validation, BigInt cho byte/count.
- [Orders.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Orders.scala): fixture, hai codec, checksum và query rewrite.
- [CostBench.scala](cost-lab/src/main/scala/vn/scalaeng/cost/CostBench.scala): sample, CPU task, cache, batch và bẫy đo.
- [Probes.scala](cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala): mô hình đơn giản, lỗi runtime và live heap.
- [CostSuite.scala](cost-lab/src/test/scala/vn/scalaeng/cost/CostSuite.scala): test logic, round-trip và batching; không assert timing.
- [verify.py](cost-lab/verify.py), [verify_sources.py](cost-lab/verify_sources.py): chạy tuần tự và kiểm nguồn phiên bản; [analyze.py](cost-lab/analyze.py) tính lại bảng từ raw, kiểm số round/checksum và event không bị mất.

Từ **gốc repository**, chạy trên Windows:

```powershell
$env:PYTHONIOENCODING = 'utf-8'
python 13-performance/cost-lab/verify_sources.py
python 13-performance/cost-lab/verify.py all
python 13-performance/cost-lab/analyze.py
```

Có thể chọn `test`, `demo`, `local`, `cpu`, `batch`, `traps`, `memory` để chỉ chạy một nhóm. Verifier đặt COURSIER_CACHE tại `.cache/coursier`; TEMP/TMP/java.io.tmpdir và build workspace tại `.cache/t20`, đã được `.gitignore` bỏ qua. Mọi lệnh Scala CLI dùng `--server=false`, compiler `-deprecation -feature -Wunused:all -Werror`; cờ suppress chỉ tắt gợi ý dependency mới, không tắt compiler warning. Cần tải dependency qua mạng ở lần đầu; unit test/benchmark không cần DB/Docker/Kafka hoặc socket của ứng dụng. Mỗi benchmark mode chạy JVM riêng, các case bên trong mode dùng chung JVM. Script đợi JVM xong mới bắt đầu lệnh kế tiếp.

Nếu chạy thủ công:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:TEMP = "$PWD/.cache/t20/tmp"
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
scala-cli.bat test 13-performance/cost-lab --server=false
scala-cli.bat test solutions/ch34-code --server=false
scala-cli.bat run 13-performance/cost-lab --server=false --main-class vn.scalaeng.cost.ModelDemo
```

Trong sandbox hiện tại, sbt launcher bị chặn BootServerSocket; lệnh `sbt.bat test` trong `cost-lab` dành cho Claude chạy ở môi trường phù hợp. Chưa gắn trạng thái hoàn tất review/sbt khi chưa có kết quả đó.

## 8. Giải thích code

`measure` đọc bộ đếm trước/sau, chỉ bao `f()` trong mốc wall-clock, rồi publish checksum vào volatile field. GC count/time là delta quanh phép đo, có thể bao một ít instrumentation đọc metric. `process_cpu_ns` là CPU toàn JVM; `main_thread_allocated_bytes` có cả allocation của thao tác lấy mốc CPU và body, không là tổng allocation của pool. Unit test dùng dữ liệu biên thay vì kỳ vọng máy chạy trong một số ms cụ thể.

Phân vị nearest-rank sắp xếp n mẫu và lấy chỉ số `ceil(p×n)−1`. `p99` với n=21 là mẫu lớn nhất; không nội suy tạo ra một mẫu chưa đo. Với event, mẫu là từng event, gộp cùng một cap qua chín round; các event trong cùng batch/round có tương quan, không coi 18.000 event là 18.000 run độc lập. Mỗi CSV giữ round để đọc cả biến thiên giữa các run.

CPU experiment chia các khoảng input liên tiếp thành task; `invokeAll` đợi xong và `.get()` lấy exception/kết quả. Overhead/task có kết quả được XOR rồi loại bằng tính lại ở driver để checksum nghiệp vụ cuối không đổi khi đổi partition. Batch harness cũng tiêu thụ checksum, nhưng checksum đó **gồm overhead theo biên batch**, nên khác nhau giữa cap; nó không phải tổng tiền cần bằng nhau. `batchRanges` được test bao phủ mỗi event đúng một lần; phân tích CSV còn kiểm đủ 2.000 event/round và lịch arrival. Pool thuộc benchmark và được shutdown/awaitTermination trong `finally`; không để thread sống sau thí nghiệm. Không chạy Spark/Kafka/network benchmark song song với nhóm này.

`Orders.avro` tạo count header rồi ghi record binary; reader dùng schema ngoài payload để dựng lại từng `Order`. Đây là codec minh họa dành cho input fixture đáng tin, không là parser endpoint untrusted đã harden: chưa có schema ID, framing limit toàn gói hay quy tắc chống trailing bytes. JSON decoder tương tự chỉ lấy các field cần, không validate schema đóng. Không dùng lab này thay contract validation ở Ch21/P4.

`Models` từ chối input âm/NaN/vô hạn/chia 0 bằng `require`. Byte/count dùng BigInt; giá dùng BigDecimal, không tính tiền bằng Double. Thời gian/rate đo dùng Double và giữ độ chính xác đủ cho mô hình ước lượng. `concurrency` kiểm tràn Int; checksum Long overflow modulo có chủ đích, khác số học tiền. Các lời giải chỉ tái sử dụng Models, không kéo dependency codec hay network vào test mô hình.

## 9. Output mong đợi

Đã biên dịch và chạy bằng Scala CLI ngày **17/10/2026**, **22 test pass / 0 fail / 0 ignored**, compiler **0 warning** với `-Werror`. Giữ snapshot benchmark đã có và memory probe sau sửa frame. Vòng 2 chạy lại hai suite và ModelDemo; transcript test dưới đây là lượt mới. Mọi số bên dưới lấy từ transcript/CSV trong [verification](cost-lab/verification/); [analysis.md](cost-lab/verification/analysis.md) được tính lại bằng `analyze.py`. Không chạy lại để chọn lượt nhanh hơn. Thời gian sẽ khác khi người học chạy lại; test chỉ kiểm logic.

Máy đo: **Intel Core i7-9750H CPU @2,60 GHz**, Windows 11 amd64, **12 logical processor** JVM thấy, **34.188.873.728 byte RAM vật lý** (~31,84 GiB). JVM là **Eclipse Adoptium 21.0.12.1**, Scala **3.9.0**, G1; benchmark `-Xms128m -Xmx512m`, memory probe `-Xmx256m`. CPU model lấy từ Windows registry trong [sources.json](cost-lab/verification/sources.json); RAM/JVM lấy từ `ENV`/`JVM` trong từng transcript. Không cô lập được mọi tải nền của máy dùng chung; đây là snapshot local, không phải báo cáo capacity một máy cloud.

### 9.1. Test và mô hình đơn giản

Trích output thật từ [lab-test.txt](cost-lab/verification/lab-test.txt), [solutions-test.txt](cost-lab/verification/solutions-test.txt):

```text
Test run vn.scalaeng.cost.CostSuite finished: 0 failed, 0 ignored, 15 total 0.527s
Test run vn.scalaeng.cost.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 0.18s
```

[model-demo.txt](cost-lab/verification/model-demo.txt) dùng input **giả định** mục 5 và 3.9; đây là output tính toán, không là benchmark fleet:

```text
COST 0.001333333333333333333333333333333333
COST_IDLE 0.003333333333333333333333333333333334
CONCURRENCY 15
PARTITIONS 800
CACHE Some(3)
MEMORY_SLOTS 4
```

### 9.2. Codec và query: giảm công việc đã đo được

Mỗi case xử lý **12.000 Order**, 12 warm-up và 21 mẫu đo. Phân vị bên dưới là **thời gian một lượt xử lý cả tập**, không là latency/event. Raw: [serialization.csv](cost-lab/verification/serialization.csv), [query.csv](cost-lab/verification/query.csv); transcript [local.txt](cost-lab/verification/local.txt).

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Case | p50 / p95 / p99 (ms/lượt) | Event/s từ p50 | Main alloc median (byte/lượt) | Process CPU median (ms) |
|---|---:|---:|---:|---:|
| JSON encode | 18,5849 / 27,5963 / 30,4195 | 645.685 | 34.548.224 | 15,625 |
| Avro encode | 4,4641 / 7,5286 / 8,4479 | 2.688.112 | 7.435.376 | 0 |
| JSON decode | 17,4150 / 23,7360 / 36,9068 | 689.061 | 31.510.656 | 15,625 |
| Avro decode | 6,1722 / 9,5136 / 11,8080 | 1.944.201 | 9.515.392 | 0 |
| Filter late | 11,8074 / 12,7315 / 12,9492 | 1.016.312 | 291.024 | 15,625 |
| Filter early | 0,4954 / 0,7095 / 0,9557 | 24.222.850 | 291.024 | 0 |

CPU median 0 **không có nghĩa không dùng CPU**: nhiều sample ngắn hơn độ phân giải bộ đếm Windows. Across 21 sample, JSON encode có 10 collection/14 ms GC, JSON decode 6/8 ms; hai case Avro không có collection trong vùng đo. Delta GC/CPU có thể gồm cả JIT hoặc thread nền JVM, không chỉ codec. Không lấy allocation làm live heap.

**Quyết định.** Với fixture/schema hiện tại, Avro giảm cả wall-clock và byte; filter early tránh checksum trên 99% row và giữ cùng XOR **-94954666026242113**. Nếu đây là hot path có cùng hợp đồng, thử filter trước rồi đo end-to-end; xem cost/million ở mục 9.8. Không chọn codec chỉ vì bảng này nếu consumer cần JSON hoặc chưa có schema evolution. Selectivity khác, data lớn vượt cache CPU, sink chậm hoặc chi phí quản lý schema có thể đổi kết luận. Rate cực cao của filter early chỉ đo vòng checksum sau lọc, không phải capacity pipeline bền.

### 9.3. Worker, partition và skew

[cpu.txt](cost-lab/verification/cpu.txt), [cpu.csv](cost-lab/verification/cpu.csv): **60.000 event/lượt**, 256 bước/event; checksum mọi cấu hình **-4204707283274803175**. Overhead có phần chạy worker và phần chạy driver như mục 3.2. CPU/wall bên dưới dùng **tổng CPU/tổng wall của 21 sample**, không chia hai median.

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Worker / task / phân bố | p50 / p95 / p99 (ms) | Event/s từ p50 | CPU/wall xấp xỉ core bận | Cost/triệu event (*) |
|---|---:|---:|---:|---:|
| 1 / 8 / đều | 24,7001 / 27,3475 / 27,8695 | 2.429.140 | 1,190 | 0,00001372 |
| 2 / 8 / đều | 12,8123 / 14,0799 / 14,7240 | 4.683.000 | 2,210 | 0,00000712 |
| 4 / 8 / đều | 7,0561 / 7,4977 / 7,8676 | 8.503.281 | 4,054 | 0,00000392 |
| 4 / 2 / đều | 12,3937 / 13,8361 / 14,4498 | 4.841.169 | 2,043 | 0,00000689 |
| 4 / 128 / đều | 10,0775 / 11,3540 / 12,0314 | 5.953.858 | 3,613 | 0,00000560 |
| 4 / 8 / 80% ở task đầu | 20,0935 / 22,7538 / 23,2601 | 2.986.040 | 1,432 | 0,00001116 |

(*) **Giá giả định 0,12 đơn vị tiền/node-hour, cùng một node cho mọi hàng**; giữ giá khi đổi worker trên cùng máy. Cách tính chỉ gồm phần elapsed xử lý đã nóng. Tăng worker trên một máy không đồng nghĩa mua thêm node. Nếu đổi sang máy có giá gấp đôi, phải tính lại bằng giá đó.

**Quyết định.** Chọn 4 worker/8 task làm ứng viên cho workload CPU này: 2 task không sử dụng hết 4 worker; 128 task thêm cả overhead worker lẫn phần tuần tự driver; skew làm task cuối chi phối. Không mua thêm node trước khi kiểm task distribution. Không dùng tỷ lệ tăng tốc này cho cluster: chưa có remote shuffle, network, spill, retry, contention nhiều tenant hoặc overhead scheduler Spark. Bằng chứng Spark ở Ch25/Ch32 mới giúp kiểm các yếu tố đó.

Ví dụ nối CPU với sizing: 4 worker/8 task đều có tổng process CPU **0,59375 s** cho **1.260.000 event** qua 21 sample (`cpu.csv`), D≈**4,7123×10⁻⁷ CPU-s/event**. Với rate mục tiêu **giả định 3.000.000/s**, u=**0,7 giả định**, mô hình `ceil(λD/u)` cho **3 core** khởi điểm. Đây chưa là benchmark 3 worker hay cam kết 3 core đạt SLO: CPU gồm thread nền JVM, độ phân giải thô và workload checksum nhân tạo. Cần đo cấu hình đó, peak memory và sink trước khi chốt node count; bảng thực tế mới đo 1/2/4 worker.

### 9.4. Cache: kết quả thật nằm sát điểm hòa vốn

Raw [cache.csv](cost-lab/verification/cache.csv), transcript [local.txt](cost-lab/verification/local.txt); 12.000 input/lượt:

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Đường xử lý | p50 / p95 / p99 (ms) | Main alloc median (byte) |
|---|---:|---:|
| B — cache build, chưa đọc | 11,8804 / 12,9979 / 13,2308 | 439.936 |
| R — đọc cache đã có | 0,1199 / 0,2223 / 0,2505 | 576.024 |
| U — tính lại | 12,1390 / 12,6209 / 13,3218 | 576.128 |

Từ median: `B/(U−R) = 11,8804/12,0191 ≈ 0,9885`, công thức trả k=1. Nhưng một consumer chỉ lợi **0,1387 ms theo phép cộng median**, nhỏ hơn biến thiên giữa các sample; **chưa đủ bằng chứng chọn cache cho một lần đọc**. Hai consumer: dự toán `B+2R=12,1202 ms` so với `2U=24,2780 ms`, lợi 12,1578 ms; đây là **mô hình cộng các median**, không phải phép đo pipeline hai consumer.

**Quyết định.** Thử cache khi có nhiều consumer và RAM đủ, đo lại cả build + các read trong cùng một lượt. Payload Long của mảng **96.000 byte** chỉ là một phần live state. R dùng generic fold nên vẫn allocation/boxing; read cache không đồng nghĩa zero allocation. Với giá mục 9.3, dự toán hai consumer/triệu input là **0,00003367** có cache so với **0,00006744** không cache, chưa tính giá RAM/dữ liệu gốc/invalidation. Cache miss, TTL ngắn hoặc tăng node count vì giữ state có thể làm recompute rẻ hơn. Số đọc có cache ở Ch25 đã cho một phản ví dụ sau hai consumer, xem mục 3.7.

### 9.5. Memory và GC: từ live state đến giới hạn slot

Output thật của [memory.txt](cost-lab/verification/memory.txt), JVM riêng `-Xmx256m`:

```text
MEMORY rows=300000 beforeBytes=3778424 heldBytes=80312088 deltaBytes=76533664 bytesPerRow=255.11221333333333 sum=1529850000
STREAM releasedBytes=3920168 afterGcBytes=3922312 sum=1529850000 heapMaxBytes=268435456
```

Tăng live heap khoảng **76,53 MB thập phân** khi giữ 300.000 object, ~255,11 byte/row trong fixture/JVM này. Sau giải phóng và xử lý iterator, heap sau GC gần baseline; sum **1.529.850.000** khớp. Không suy peak streaming chỉ 3,92 MB: đây là **sau GC**, không phải đỉnh. Một đoạn nguyên văn từ [memory-gc.log](cost-lab/verification/memory-gc.log):

```text
[1.058s][info][gc             ] GC(7) Pause Full (System.gc()) 76M->76M(130M) 33.144ms
[1.187s][info][gc             ] GC(8) Pause Full (System.gc()) 77M->3M(128M) 5.532ms
```

**Mô hình và quyết định.** Giả sử peak/task được **đo riêng** là 128 MiB, heap 1 GiB, live cache 256 MiB và reserve 256 MiB: còn 512 MiB → tối đa 4 task. Đây là **input giả định của bài tập**, không là peak đo được từ probe. Với fixture hiện tại, tránh giữ input khi chỉ cần sum; nếu cần giữ state thì dự toán từ live data rồi đo peak với 4 task trước khi tăng heap/node. Bước tăng giá compute vì memory phải được đưa vào cost/million; probe này chưa so giá các loại node. Cấu trúc object, độ dài Unicode, GC, compressed oops và concurrency đổi thì byte/row cũng đổi.

### 9.6. Network: đo byte thật, dùng bandwidth/giá giả định

Output payload thật: `PAYLOAD events=12000 jsonBytes=1402760 avroBytes=845226`. Cùng dữ liệu, JSONL có newline cuối; Avro raw có count 4 byte và schema ngoài gói. Avro nhỏ hơn khoảng **39,75%** theo `(JSON−Avro)/JSON` trong fixture này.

Giả định **effective bandwidth 50.000.000 byte/s**, **egress 0,05 đơn vị tiền/GiB**, truyền một chiều một lần, không nén/TLS/header/retry/replication. Tính bằng bài tập 4 hoặc [analyze.py](cost-lab/analyze.py); các cột sau payload là **ngoại suy/mô hình**, không phải số đo mạng:

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Codec | Byte/event trung bình | Byte/triệu event, ceil tổng | Transfer giây/triệu | Egress cost/triệu |
|---|---:|---:|---:|---:|
| JSONL | 116,8967 | 116.896.667 | 2,337933 | 0,00544343 |
| Avro raw + count | 70,4355 | 70.435.500 | 1,408710 | 0,00327991 |

Ngoại suy giữ nguyên byte/event trung bình của snapshot 12.000 row, **không phải** encode một triệu row rồi đo byte. Độ dài ID/note, varint, số header theo batch và phân phối dữ liệu thay đổi thì cần đo lại payload, không giữ nguyên tỷ lệ này.

**Quyết định.** Trong mô hình giá này, tiết kiệm egress lớn hơn chênh lệch compute codec ở mục 9.8, nên đáng thử Avro nếu contract/consumer cho phép. Nếu cần độ trễ một event, cộng encode + transfer + decode khi chúng không chồng lấp; nếu pipeline theo batch, bottleneck throughput khác tổng latency. Chưa mở socket, chưa đo effective bandwidth/NIC/TLS hay egress thực tế. Trong cùng vùng miễn phí egress hoặc mạng nhanh và sink chậm, lợi tiền có thể không còn; schema ID/framing thực tế và Avro container cũng đổi byte.

### 9.7. Batch: capacity tốt chưa có nghĩa đạt tail SLO

Raw [batch-service.csv](cost-lab/verification/batch-service.csv), [event-latency.csv](cost-lab/verification/event-latency.csv), [event-totals.csv](cost-lab/verification/event-totals.csv); output [batch.txt](cost-lab/verification/batch.txt). Service đo khi **2.000 event đã có sẵn**; mỗi cap 21 run sau warm-up:

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Cap | Service p50 / p95 / p99 (ms/2.000 event) | Capacity event/s từ p50 | Cost/triệu ở capacity (*) |
|---|---:|---:|---:|
| 1 | 41,6982 / 44,9384 / 45,2354 | 47.964 | 0,00069497 |
| 16 | 3,0011 / 3,1491 / 3,5665 | 666.422 | 0,00005002 |
| 128 | 0,7450 / 0,8974 / 0,9169 | 2.684.564 | 0,00001242 |

(*) Cùng giá/node giả định mục 9.3, chỉ tính service processing. Bảng dưới đo lịch arrival cố định **10.000 event/s**, timeout **5 ms**, 3 warm-up +9 round, **18.000 latency event/cap**; cap 128 thực tế flush tối đa 51 event/lô vì timeout. Không áp capacity batch đầy 128 cho lô 51.

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Cap | Event p50 / p95 / p99 (ms) | Mean / max (ms) | p99 từng round: min–max (ms) | Đạt p99 ≤6 ms giả định? |
|---|---:|---:|---:|---|
| 1 | 6,3636 / 12,0876 / 13,1805 | 6,3749 / 15,1116 | 12,2978–13,4721 | Không |
| 16 | 8,2704 / 15,4356 / 16,7132 | 8,3258 / 22,9705 | 15,7720–20,9944 | Không |
| 128 | 10,0437 / 17,4379 / 19,0510 | 10,0613 / 22,5882 | 17,9729–20,8882 | Không |

**Quyết định.** Chưa chọn được cấu hình đạt SLO; hàm chọn ứng viên phải trả `None`. `parkNanos` thức muộn và tải nền là những giả thuyết cần kiểm tiếp, không thể quy toàn bộ tail cho fill wait hay checksum. Bước sau là đo scheduler/harness riêng và load test trên môi trường triển khai, gồm sink/ack; không tự nới SLO để tuyên bố cap 128 thắng. Thêm worker không chữa được nguồn thức muộn nếu bottleneck nằm trong harness này.

Một node luôn bật, nguồn đều λ=10.000/s, giá **0,12/node-hour giả định**: cost/triệu ở trạng thái ổn định là `0,12 × 1.000.000/(3600 × 10.000) = 0,00333333`, gồm cả thời gian chờ, **không** dùng capacity 2.684.564/s để tính hóa đơn. Trong cửa sổ hữu hạn, `event-totals.csv` cho completion/s lần lượt khoảng **9.835,98 / 9.598,32 / 9.701,05** vì phải hoàn tất đuôi sau arrival cuối; không coi đây là steady-state capacity. Startup, sink và làm tròn billing vẫn chưa có trong mô hình.

### 9.8. Bảng cost per 1 triệu event và quyết định cuối

Giá **giả định 0,12 đơn vị tiền/node-hour**, một node, rate lấy median service đã nóng ở mục 9.2; đây là chi phí của **từng bước** ở capacity, không phải giá pipeline production:

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Bước / phương án | Cost/triệu event | Điều kiện quyết định |
|---|---:|---|
| JSON encode | 0,00005162 | Giữ khi contract/debug/consumer cần JSON |
| Avro encode | 0,00001240 | Thử khi quản trị được schema/framing |
| JSON decode | 0,00004838 | Cùng snapshot, bốn field của lab |
| Avro decode | 0,00001715 | Cần kiểm cả đường consumer thực |
| Query filter late | 0,00003280 | Baseline chạy checksum mọi row |
| Query filter early | 0,00000138 | Giữ pure semantics và selectivity tương tự |

Chi phí encode+decode **cộng theo hai bước service** là ~0,00010000 với JSON và ~0,00002955 với Avro, chưa tính truyền/queue/overlap/startup. Với giá egress giả định mục 9.6, cộng thêm 0,00544343 hoặc 0,00327991 tương ứng. Không cộng các cost của những phép đo khác input count hay ranh giới (CPU worker, cache read, P4 snapshot) thành một hóa đơn.

Kết quả sizing là một quyết định có điều kiện: thử filter sớm và Avro cho fixture này; 4 worker/8 task cho checksum CPU cân bằng; cache cần xác nhận lại với nhiều consumer; iterator giảm live input sau xử lý; batch **chưa có ứng viên đạt SLO**. Mọi lựa chọn vẫn cần đo end-to-end khi thêm durability, input thật, peak memory, startup và tài nguyên cluster. Không đủ số đo thì giữ quyết định mở, không bịa một node count production.

## 10. Lỗi thường gặp

### 10.1. Lỗi input mô hình — thông báo runtime thật

[ModelDemo](cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala) cố tình truyền input sai; [model-demo.txt](cost-lab/verification/model-demo.txt) lưu nguyên thông báo:

```text
ERROR zero-throughput java.lang.IllegalArgumentException: requirement failed: throughput must be positive and finite
ERROR empty-samples java.lang.IllegalArgumentException: requirement failed: samples must be finite, non-negative and non-empty
ERROR zero-partition-target java.lang.IllegalArgumentException: requirement failed: bytes must be non-negative and target positive
ERROR invalid-utilization java.lang.IllegalArgumentException: requirement failed: utilization must be in (0,1]
```

Zero throughput không có cost/million hữu hạn; thiếu latency sample không được thay bằng 0; target partition 0 sai đơn vị/ngân sách; utilization phải thuộc (0,1]. Sửa dữ liệu hoặc ghi chưa có phép đo thay vì bắt lỗi rồi trả một giá tiền đẹp. Test còn từ chối NaN/vô hạn và count vượt Int capacity. Các thông báo compiler khác chưa phát sinh trong lượt cuối nên không dựng ví dụ lỗi giả.

### 10.2. Lỗi đo đạc thường gặp — ví dụ đã chạy thật

**Bỏ kết quả nên có thể bị dead-code elimination.** [traps.txt](cost-lab/verification/traps.txt), [traps.csv](cost-lab/verification/traps.csv) chạy 200.000 input, 32 bước checksum/input; một case bỏ toàn bộ kết quả và trả 0, case kia XOR kết quả vào sink volatile:

**Bối cảnh đo:** i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ nào chạy đồng thời. p99 có thể chịu nhiễu nền.

| Case | FIRST (ms) | Sau warm-up p50 / p95 / p99 (ms) |
|---|---:|---:|
| discarded-result | 337,6024 | 0,0063 / 0,0104 / 0,0104 |
| consumed-result | 14,7916 | 8,4872 / 9,6210 / 9,6826 |

Không dùng 0,0063 ms để báo “xử lý 31 tỷ event/s”. Kết quả bỏ đi không còn chứng minh checksum nghiệp vụ đã được tính. Chênh lệch phù hợp với giả thuyết JIT loại công việc, nhưng **chưa xem assembly để kết luận chính xác phần nào bị loại**. Dùng input biến đổi, tiêu thụ kết quả và JMH Blackhole/return khi chuyển sang microbenchmark nghiêm túc; sink không bảo đảm mọi phép tối ưu đều bị chặn.

**Không warm-up, tính lần đầu gồm cả runtime setup.** Trong [batch.txt](cost-lab/verification/batch.txt), batch1 `FIRST=353,8310 ms`, median sau warm-up **41,6982 ms**. Đây là khoảng FIRST của hàm đo, **không phải** process startup từ lúc gọi Scala CLI; chưa tách được bao nhiêu là class loading, JIT, scheduler hay tải nền. Codec và CPU còn có lượt validate trước FIRST nên FIRST ở đó cũng không hoàn toàn cold. Nếu job của bạn chỉ chạy một lần, chi phí khởi động là thật và phải đo riêng; đừng xóa nó để làm hóa đơn rẻ. Nếu service sống lâu, đo sau warm-up nhưng giữ raw để xem còn drift. Không lấy số FIRST của hai case nối tiếp làm hai JVM cold độc lập.

**So mean khi phân phối lệch.** Event cap128 có mean **10,0613 ms** nhưng p99 **19,0510 ms**, max **22,5882 ms**; lấy mean để xét tail SLO sẽ bỏ qua phần đuôi. Với 21 run, p99 bằng max; với 100 sample mà 99 sample=1 ms, một sample=1000 ms, nearest-rank p99 vẫn **1 ms** dù mean=10,99 ms. Đây là input **tổng hợp của test bài 1**, không là số đo event thực; giữ max/histogram và sample count để tránh đọc nhầm cả percentile.

**Bỏ static reference nhưng local tạm còn sống trên frame đo.** Lượt probe trước sửa trong [memory-before-frame-fix.txt](cost-lab/verification/memory-before-frame-fix.txt) có `heldBytes=80293168`, `releasedBytes=80402088`: xóa `held` chưa làm heap giảm như kỳ vọng. Tách materialize/sum sang hàm `@noinline` trả Long, gọi GC sau khi frame đó trả về; lượt mới ở mục 9.5 xuống `releasedBytes=3920168`. Đây là bằng chứng thay đổi harness ảnh hưởng phép đo, **chưa có heap dump chỉ đích danh GC root**, nên nguyên nhân local/iterator còn sống là giả thuyết phù hợp chứ không là kết luận profiler. Không gọi số retained cũ là “streaming giữ 80 MB” hay khẳng định memory leak của production từ một probe.

**Lẫn metric và đơn giá.** Process CPU median Avro encode bằng 0 do bộ đếm thô (phân tích ngay dưới), không phải Avro miễn phí CPU; capacity batch128 không phải arrival dùng tính hóa đơn; mean residence không phải p99. `analysis.md` vẫn tính số rate cơ học cho trap để đối chiếu raw, nhưng đánh dấu cost discarded-result **không hợp lệ** vì không có ý nghĩa nghiệp vụ. Trước khi đổi sizing, quay lại ranh giới và đơn vị ở mục 3.

**Bộ đếm CPU có bước quan sát 15,625 ms.** Lab dùng `com.sun.management.OperatingSystemMXBean.getProcessCpuTime()`, khác `System.nanoTime()` đo wall-clock. [JDK 21 API](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html#getProcessCpuTime()) trả đơn vị ns nhưng không bảo đảm độ chính xác ns; không hỗ trợ thì trả -1. Trên Windows 11/Adoptium 21.0.12.1 của snapshot này, các delta dương trong sáu CSV đều là bội của **15.625.000 ns =15,625 ms**; đây là bước quan sát, không phải hằng số cho mọi Windows/JDK. [analysis.md](cost-lab/verification/analysis.md#bộ-đếm-cpu) lưu số mẫu 0, delta dương nhỏ nhất và ước chung lớn nhất, tính lại bằng `analyze.py`.

Vì vậy, CPU=0 ở một sample Avro encode ~4,46 ms không đủ để suy ra CPU demand/event. Cần gom nhiều lượt thành vùng đo CPU dài hơn hoặc dùng profiler, rồi đối chiếu thread nền/JIT. Tổng CPU/tổng wall ở mục 9.3 giảm ảnh hưởng lượng tử hóa, vẫn không cô lập CPU của riêng nghiệp vụ. **Chưa đo độ phân giải API trên máy khác.**

## 11. Cách debug

1. Xác nhận contract trước khi xem tốc độ: cùng input, schema, duplicates, tổng tiền, ranh giới durability và SLO. Nếu invariant khác, kết quả timing chưa so được.
2. Đọc `ENV`, `JVM`, input count và `FIRST`; so raw theo round xem JIT/GC/tải nền có làm drift không. Nếu chưa ổn, tăng thời gian/warm-up và dùng nhiều fork JMH trước khi kết luận khác biệt nhỏ.
3. CPU cao: tìm hot method bằng JFR. Wall dài mà CPU thấp: thread dump, lock/pool/I/O, scheduler delay; không tăng thread theo cảm giác.
4. Memory: phân biệt allocation rate với live heap; đọc heap sau GC và pause. Với Spark, so spill và peak execution memory theo task/stage; không gộp task GC time rồi gọi là pause toàn JVM.
5. Network: lấy remote bytes/fetch wait/retry và số byte nghiệp vụ. Mạng thật chưa đo trong lab; dùng mô hình bandwidth giả định chỉ để thiết kế thí nghiệm tiếp.
6. Giữ input/giá không đổi khi so cấu hình; bổ sung startup, idle, cache build, sink và recovery vào chi phí nếu ranh giới yêu cầu. Đổi một yếu tố, chạy tuần tự và giữ raw của cả baseline lẫn phương án.

## 12. Bài tập (cơ bản → nâng cao)

1. **Phân phối latency.** Viết `distribution` trả mean/p50/p95/p99 nearest-rank. Test 99 mẫu 1 ms +1 mẫu 1000 ms; giải thích vì sao p99 vẫn 1 ms và cần giữ max/histogram. Từ chối input rỗng/không hợp lệ.
2. **Little's law.** Viết `requiredConsumers(rate, residenceMs, utilization)` dùng mean, ceil và headroom. Test mean 20 ms, rate 100/s, u=0,5 →4. Ghi rõ khi nào residence gồm queue làm kết quả không còn là số consumer bận.
3. **Partition và skew.** Viết `layout` tính partition đầu từ byte và mô phỏng task list trên slot. So `[10,10,10,10]` với `[37,1,1,1]`, hai slot, overhead 1 ms. Test empty/huge byte; giải thích vì sao tăng partition count không tự chữa hot key.
4. **Byte và network cost.** Viết `wireCost` từ tổng payload byte đo, số row đo, số row mục tiêu, effective bytes/s và giá/GiB giả định. Làm tròn **tổng** byte lên một lần. Test 10 byte/3 row →1 triệu row, zero input và sai đơn vị/chia 0. Nêu các overhead chưa bao gồm.
5. **Cache break-even.** Viết `cacheSaving(B,U,R,k)` tính `kU−(B+kR)`. B=150/U=90/R=20: hai consumer lỗ 10 ms, ba consumer lợi 60 ms. Thêm U≤R và materialization có thể làm tăng node count; không assert thời gian runtime.
6. **Chọn cấu hình theo SLO rồi theo chi phí.** Viết `cheapest` từ candidate có node count, fleet throughput, p99, giá/node-hour. Loại cấu hình thiếu rate hoặc vượt p99; trả None khi tất cả đều bị loại. Test candidate rẻ nhưng trễ, candidate rẻ nhưng chậm, input NaN và fleet nhiều node.
7. **Kết hợp giới hạn CPU/memory.** Viết `feasibleSlots` là min của CPU slot và ngân sách memory sau cache/reserve. Test không còn memory, CPU thấp hơn memory, BigInt vượt Long; nêu phần ngoài heap cần dự toán riêng.

## 13. Đáp án và lời giải

Xem [ch34-solutions.md](../solutions/ch34-solutions.md), [Exercises.scala](../solutions/ch34-code/Exercises.scala) và [ExercisesSuite.test.scala](../solutions/ch34-code/ExercisesSuite.test.scala). Cả bảy bài có lời giải và test munit độc lập với timing; chạy `python 13-performance/cost-lab/verify.py test` từ gốc repo để kiểm lab và lời giải tuần tự.

## 14. Câu hỏi kiểm tra

1. Khi throughput service là 1 triệu/s nhưng nguồn chỉ 10.000/s, dùng con số nào để tính chi phí một service luôn bật? → Dùng rate thực và toàn thời gian cấp phát; capacity chỉ là cận lạc quan.
2. Vì sao CPU time lớn hơn wall time không phải lỗi? → Nhiều thread đang chạy trên nhiều core; CPU cộng dồn theo thread.
3. Vì sao allocation 1 GiB không đồng nghĩa cần heap ≥1 GiB? → Đối tượng ngắn hạn được GC; heap cần chứa live set và phần dự phòng.
4. p99 của 21 run có đủ để cam kết SLO production không? → Không; gần max của ít mẫu và khác đơn vị event latency.
5. Cache read nhanh gấp 100 lần có bảo đảm tiết kiệm tiền không? → Còn materialization, hit ratio, TTL và RAM/node-hour.
6. Đo local shuffle read có định cỡ NIC cluster được không? → Không; cần remote fetch/byte và bandwidth thực trên cluster.
7. Vì sao đúng số hàng chưa đủ so hai query? → Có thể sai duplicates, amount/null/outer join hoặc durability; phải đối soát contract.
8. Timeout batch 5 ms có giới hạn latency tổng 5 ms không? → Không; scheduler delay, queue và service/commit còn cộng thêm.

## 15. Mini project — Sizing decision record cho order enrichment

Chọn một snapshot từ fixture hoặc dữ liệu của bạn; ghi schema, seed, row count, byte và checksum/tổng tiền. Đặt **giả định riêng** cho arrival rate, p99 SLO, giá compute/network và thời gian cấp máy. Đo hai phương án chỉ khác một yếu tố (filter placement, codec hoặc batch cap), lưu warm-up, nhiều round và raw. Báo cả startup nếu job chạy một lần; báo idle nếu service luôn bật. Không hạ yêu cầu durability để thắng timing.

Nộp bảng: cấu hình → dữ liệu đúng? → rate/p99 → CPU/live heap/GC → cost/million → đạt SLO? → quyết định. Thử tăng giá RAM, tăng hot-key fraction và giảm số lần dùng cache; ghi ngưỡng khiến đổi quyết định. Test logic chọn cấu hình bằng dữ liệu tổng hợp, không assert thứ hạng runtime. Nếu thiếu máy/cluster thì mô hình network, executor loss, autoscaling và remote bandwidth phải ghi **Chưa kiểm chứng**.

Tiêu chí nghiệm thu: tái lập được từ một lệnh, không chạy benchmark chồng nhau, đúng dữ liệu, không bỏ tail, mọi đơn giá gắn nhãn giả định, mọi kết luận có điều kiện bác bỏ. Lab là bộ khối tham khảo cho bài này, không phải một hệ thống load test production đã hoàn thiện.

## 16. Nguồn tài liệu chính thức

Kiểm tra ngày **17/10/2026 (UTC+7)**. [sources.json](cost-lab/verification/sources.json) lưu URL, HTTP status, SHA-256 và CPU registry; POM được giữ trong cùng thư mục. Tài liệu Avro dùng spec **1.12.0** cho quy tắc binary encoding; artifact runtime là **1.12.2** được kiểm tra riêng qua POM. Không suy phiên bản runtime từ URL spec.

- [Scala compiler 3.9.0](https://repo.maven.apache.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom), [munit_3 1.2.4](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom), [circe-parser_3 0.14.17](https://repo.maven.apache.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom): artifact `_3` và build/test thật kiểm khả năng dùng Scala 3.
- [Avro 1.12.2](https://repo.maven.apache.org/maven2/org/apache/avro/avro/1.12.2/avro-1.12.2.pom), [Avro specification](https://avro.apache.org/docs/1.12.0/specification/), [slf4j-nop 2.0.17](https://repo.maven.apache.org/maven2/org/slf4j/slf4j-nop/2.0.17/slf4j-nop-2.0.17.pom), [sbt 1.12.11](https://repo.maven.apache.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom).
- [OpenJDK JMH dead code](https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_08_DeadCode.java), [JMH forking](https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_12_Forking.java): cách dùng kết quả và tách profile khi benchmark.
- [OperatingSystemMXBean JDK 21](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html#getProcessCpuTime()) (kiểm tra 17/10/2026): đơn vị và giới hạn độ chính xác của CPU time.
- [JDK 21 java command](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html), [MemoryMXBean](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/MemoryMXBean.html): heap, GC log và giới hạn quan sát memory.
- [MIT Urban Operations Research — queueing questions and model assumptions](https://web.mit.edu/urban_or_book/www/book/chapter4/4.1.html): điều kiện steady state và giới hạn mô hình queue; suy luận sizing trong chương là mô hình cần xác nhận bằng đo.
- [Spark SQL performance 4.2.0](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html), [Spark monitoring 4.2.0](https://spark.apache.org/docs/4.2.0/monitoring.html): final plan, task/GC/shuffle metrics để nối bằng chứng Ch25/Ch32 với quyết định.

## 17. Checklist tự đánh giá

- [ ] Tôi ghi rõ ranh giới latency/CPU/memory và giữ raw, không chỉ mean.
- [ ] Tôi phân biệt event p99, run p99, throughput capacity và arrival thực.
- [ ] Tôi tính cả startup, cache build, idle, memory, network và recovery theo phạm vi quyết định.
- [ ] Tôi không dùng p95 thay mean trong Little's law hoặc coi headroom là bảo đảm tail SLO.
- [ ] Tôi giữ contract dữ liệu/durability khi tối ưu query/codec/batch.
- [ ] Tôi chỉ ngoại suy local sang cluster như giả thuyết và ghi phần chưa đo.
- [ ] Tôi chạy bảy lời giải, hiểu case None khi không có cấu hình đạt SLO.
- [ ] Tôi có thể giải thích khi nào lựa chọn của mình hết đúng và cần đo lại.
