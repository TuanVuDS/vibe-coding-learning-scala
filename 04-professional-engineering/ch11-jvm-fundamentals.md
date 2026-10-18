# Chương 11 — JVM Fundamentals cho Scala Engineer

> **Phiên bản:** Scala 3.9.0 · OpenJDK 21.0.10 · Không dependency ngoài
> **Code:** [`jvm-lab/`](jvm-lab/) — 8 thí nghiệm, chạy bằng [`run-experiments.sh`](jvm-lab/run-experiments.sh)
> **Trạng thái kiểm chứng:** Mọi thí nghiệm **đã chạy thật**. Số liệu trong bài là của **máy biên soạn: 1 CPU, ~4 GB RAM, Linux, OpenJDK 21.0.10** —
> trên máy bạn số liệu sẽ khác (nhất là thời gian), nhưng **xu hướng** và **lý do** thì như nhau.
> Môi trường biên soạn chỉ có JRE (không có `javac`, `javap`, `jcmd`, `jfr`), nên bài dùng reflection, ThreadMXBean và API `jdk.jfr` để làm những việc tương đương — kèm lệnh công cụ chuẩn để bạn dùng trên JDK đầy đủ.

---

## Phạm vi

Chương này **chỉ** dạy phần JVM cần cho backend và data processing: đọc được cấu hình JVM đang chạy, hiểu bộ nhớ đi đâu, GC ảnh hưởng gì, vì sao container làm JVM
hành xử khác, và cách thu thập bằng chứng (GC log, thread dump, JFR) khi có sự cố. Tinh chỉnh GC chuyên sâu và JIT là mức [CS].

## 1. Mục tiêu học tập

- Mô tả đường đi từ `.scala` → bytecode → class loading → thông dịch/JIT.
- Phân biệt heap và stack; dự đoán `OutOfMemoryError` và `StackOverflowError`.
- Giải thích **boxing** và ước lượng chi phí bộ nhớ của `Array[Int]`, `Vector[Int]`, `List[Int]`.
- Đọc GC log; hiểu vì sao JVM chọn Serial GC hay G1; biết các cờ JVM quan trọng.
- Phát hiện deadlock và lấy thread dump; đọc kết quả profiling của JFR.
- Áp dụng quy trình **đo trước, sửa sau** cho vấn đề CPU và bộ nhớ.

## 2. Kiến thức tiên quyết

- Chương 1 (JVM tổng quan), Chương 6 (collections, chi phí), Chương 8A (opaque type), Chương 9 (cấu trúc project).

## 3. Lý thuyết

### 3.1. Bytecode, class loading, JIT

1. `scalac` sinh `.class` (bytecode). Mỗi `object`, `case class`, lambda... có thể thành một hoặc nhiều class.
2. **Class loading:** JVM nạp class **lười** — lần đầu dùng mới nạp. Chương trình `jvmInfo` nhỏ đã nạp **~1.200 class** (output thật: `Số class đã nạp: 1207`), phần lớn của JDK.
   Ứng dụng backend thật nạp hàng chục nghìn class → đó là một phần lý do JVM khởi động chậm.
3. **Thông dịch rồi JIT:** code chạy bằng trình thông dịch trước; đoạn nào chạy nhiều ("nóng") được JIT biên dịch sang mã máy tối ưu. Hệ quả:
   - Lần chạy đầu chậm hơn các lần sau → **benchmark phải warm-up**.
   - Service chạy lâu đạt hiệu năng cao; CLI/job vài giây không kịp hưởng lợi.

**Kiểm chứng: opaque type thật sự không tốn chi phí** (thí nghiệm 3, output thật qua reflection — tương đương `javap`):

```
totalBoxed(jvmlab.MoneyBox, jvmlab.MoneyBox): jvmlab.MoneyBox
totalOpaque(long, long): long
```

`Vnd` là `long` nguyên thủy trong bytecode; case class wrapper là một đối tượng trên heap. Khẳng định ở Chương 8A được xác nhận ở mức bytecode.
Trên JDK đầy đủ: `javap -p -cp target/scala-3.9.0/classes 'jvmlab.Api$'`.

### 3.2. Heap và stack

| | Stack | Heap |
|---|---|---|
| Chứa | Khung gọi hàm: biến cục bộ nguyên thủy, tham chiếu | Mọi đối tượng (`case class`, `List`, `String`, mảng) |
| Mỗi thread | Một stack riêng | Chung cho mọi thread |
| Kích thước | `-Xss` (mặc định thường 1 MB trên Linux x64) | `-Xms` / `-Xmx` |
| Dọn dẹp | Tự động khi hàm trả về | Garbage Collector |
| Lỗi khi đầy | `StackOverflowError` | `OutOfMemoryError: Java heap space` |

**Thí nghiệm 6 — độ sâu đệ quy phụ thuộc `-Xss`** (output thật):

```
-Xss256k  ->  Độ sâu đệ quy đạt được: ~2989
-Xss4m    ->  Độ sâu đệ quy đạt được: ~84909
```

Liên hệ Chương 4: đệ quy không tail-recursive với 200.000 phần tử tràn stack mặc định. Tăng `-Xss` chỉ là che giấu — sửa thuật toán (`@tailrec`, `foldLeft`).

**Thí nghiệm 5 — OOM khi giữ tham chiếu** (`-Xmx64m`, output thật):

```
OutOfMemoryError sau ~60 MB: Java heap space
```

Trong DE, nguyên nhân OOM thường gặp nhất: **đọc cả file lớn vào `List`** (`Files.readAllLines`, `.toList` trên iterator), `groupBy` trên dữ liệu lớn, cache không giới hạn.
`order-platform` (Chương 9) đọc toàn bộ file — chấp nhận được với file vài chục MB; với file nhiều GB cần xử lý dạng stream (Chương 19).

### 3.3. Boxing

Generic trên JVM chỉ làm việc với **đối tượng**. `List[Int]` thực chất chứa con trỏ tới các đối tượng `java.lang.Integer` (16 byte mỗi đối tượng trên JVM 64-bit
với compressed oops), cộng với node của danh sách liên kết. `Array[Int]` là mảng `int[]` nguyên thủy, 4 byte mỗi phần tử, nằm liền nhau.

**Thí nghiệm 2 — bộ nhớ cho 5 triệu số nguyên** (output thật, đo xấp xỉ bằng `Runtime` sau `System.gc()`):

```
Array[Int]  : ~19 MB      ≈ 5M × 4 byte
Vector[Int] : ~99 MB      ≈ 5M × (4 byte tham chiếu + 16 byte Integer)
List[Int]   : ~191 MB     ≈ 5M × (24 byte node :: + 16 byte Integer)
```

**Thời gian tính tổng** (20 lần sau warm-up, 1 CPU; con số tuyệt đối không quan trọng, tỷ lệ mới quan trọng):

```
Array[Int] while-loop            1.47 ms/lần
Array[Int].sum                  36.48 ms/lần   <- dùng foldLeft(0L)(_ + _)
Vector[Int].foldLeft            34.18 ms/lần
List[Int].foldLeft              38.58 ms/lần
```

**Bài học quan trọng nhất của thí nghiệm:** `Array` dùng `foldLeft` chậm ngang `List`, chậm hơn vòng `while` ~25 lần. Container không phải thủ phạm duy nhất —
**lambda generic `(Long, Int) => Long` bị boxing** ở mỗi lần gọi. Vòng `while` làm việc trực tiếp với `int`/`long` nguyên thủy.

Khi nào quan tâm? **Chỉ khi đo được** đây là điểm nóng: vòng lặp số học trên hàng chục triệu phần tử, xử lý dữ liệu trong bộ nhớ quy mô lớn. Trong 95% code backend,
ưu tiên code dễ đọc. (Trong Spark, dữ liệu nằm trong định dạng nhị phân Tungsten chứ không phải đối tượng Scala — một trong những lý do DataFrame nhanh hơn RDD đối tượng; Chương 23.)

Lưu ý thêm: JVM cache sẵn `Integer` từ -128 đến 127, nên đo với giá trị nhỏ sẽ đánh giá thấp chi phí boxing — thí nghiệm dùng giá trị từ 1.000 trở lên.

### 3.4. Garbage collection

**Giả thuyết thế hệ (generational hypothesis):** hầu hết đối tượng chết trẻ. JVM chia heap thành vùng trẻ (young) và vùng già (old); GC vùng trẻ thường xuyên và rẻ.

**Thí nghiệm 4 — `-Xlog:gc`**, tạo 30 lô × 200.000 `Event` sống ngắn (output thật):

```
[0.005s][info][gc] Using Serial
[0.386s][info][gc] GC(0) Pause Young (Allocation Failure) 18M->9M(61M) 12.960ms
[0.466s][info][gc] GC(1) Pause Young (Allocation Failure) 26M->17M(61M) 22.713ms
...                                                     (55 lần pause tổng cộng)
```

Đọc một dòng: `GC(1)` lần thứ 2 · `Pause Young` dừng ứng dụng để dọn vùng trẻ · `Allocation Failure` vì hết chỗ cấp phát ·
`26M->17M(61M)` heap đã dùng trước→sau (tổng heap hiện tại) · `22.713ms` thời gian dừng.

**JVM ergonomics — cái bẫy của container.** Máy biên soạn có **1 CPU**, và JVM tự chọn **Serial GC** (một luồng, dừng toàn bộ):

```
CPU khả dụng: 1
GC: Copy, MarkSweepCompact            <- Serial GC
```

Giả lập 4 CPU bằng `-XX:ActiveProcessorCount=4`:

```
CPU khả dụng: 4
GC: G1 Young Generation, G1 Concurrent GC, G1 Old Generation   <- G1
```

JVM chỉ chọn G1 (GC mặc định từ JDK 9) khi nhận diện là "server-class machine" — đủ CPU và bộ nhớ; ngược lại dùng Serial. Container Kubernetes giới hạn `cpu: 1`
→ JVM thấy 1 CPU → Serial GC → pause dài hơn với heap lớn. Ngoài ra, `-Xmx` mặc định chỉ là **1/4 bộ nhớ** mà JVM thấy (bộ nhớ của container).

**Cấu hình khuyến nghị cho container** (đặt rõ ràng thay vì phó mặc ergonomics):

```bash
-XX:MaxRAMPercentage=75.0     # heap tối đa = 75% giới hạn bộ nhớ container
-XX:+UseG1GC                  # hoặc chấp nhận Serial cho job nhỏ 1 CPU — nhưng hãy CHỌN có chủ đích
-XX:+ExitOnOutOfMemoryError   # OOM thì thoát để orchestrator khởi động lại, không chạy "nửa sống"
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/dumps
-Xlog:gc*:file=/logs/gc.log:time,uptime:filecount=5,filesize=10m
```

| GC | Đặc điểm | Hợp với |
|---|---|---|
| Serial | Một luồng, dừng toàn bộ | Container nhỏ, heap < vài trăm MB, job ngắn |
| Parallel | Nhiều luồng, tối ưu throughput | Job batch không quan tâm độ trễ |
| **G1** (mặc định) | Cân bằng throughput/độ trễ, heap lớn | Hầu hết backend service |
| ZGC | Pause rất ngắn (mili-giây) với heap rất lớn | Service nhạy độ trễ, heap hàng chục GB |

Quyết định đổi GC phải dựa trên **GC log/JFR**, không dựa trên bài viết trên mạng.

### 3.5. Threading

- Mỗi `java.lang.Thread` (platform thread) gắn với một thread của hệ điều hành, có stack riêng (`-Xss`). Hàng nghìn thread → hàng GB bộ nhớ stack ảo và chi phí chuyển ngữ cảnh.
- JDK 21 có **virtual thread** (rẻ, hàng triệu). Cats Effect dùng **fiber** với ý tưởng tương tự ở mức thư viện. Chương 16 đi sâu.
- `synchronized` khóa trên một đối tượng. Hai thread lấy hai khóa theo **thứ tự ngược nhau** → deadlock.

**Thí nghiệm 7 — phát hiện deadlock bằng ThreadMXBean** (output thật):

```
Phát hiện deadlock giữa 2 thread:
  "transfer-A-to-B" BLOCKED chờ java.lang.Object@1e643faf đang bị giữ bởi "transfer-B-to-A"
  "transfer-B-to-A" BLOCKED chờ java.lang.Object@5594a1b5 đang bị giữ bởi "transfer-A-to-B"
```

Trên JDK đầy đủ: `jcmd <pid> Thread.print` in thread dump cùng thông tin deadlock tương tự. Khi service "treo" mà CPU thấp, **thread dump là việc đầu tiên cần làm**.
Đặt **tên thread có ý nghĩa** (như `transfer-A-to-B`) — thread dump với tên `Thread-37` gần như vô dụng.

### 3.6. Profiling với Java Flight Recorder

JFR có sẵn trong JDK, chi phí thấp (thường được mô tả là dưới vài phần trăm với cấu hình mặc định), dùng được ở production.

**Thí nghiệm 8** — workload tách chuỗi, JFR lấy mẫu CPU mỗi 10 ms (output thật):

```
acc=55579617; số lần GC: 49; số mẫu CPU: 42
Top 5 method đang chạy khi lấy mẫu (hot spots):
    20  java.lang.String.split
     8  java.util.ArrayList.grow
     2  java.lang.invoke.DirectMethodHandle.allocateInstance
     1  scala.collection.mutable.ArraySeq$ofInt.length
     1  jdk.internal.misc.Unsafe.allocateUninitializedArray
```

Gần một nửa thời gian CPU nằm trong `String.split` (dùng regex nội bộ). Nếu đây là job parse CSV thật, đó là chỗ đầu tiên đáng tối ưu — **không phải** chỗ ta đoán.
Trên JDK đầy đủ:

```bash
java -XX:StartFlightRecording=duration=60s,filename=rec.jfr -jar app.jar
jfr print --events jdk.ExecutionSample rec.jfr | head
jfr summary rec.jfr
# hoặc mở rec.jfr bằng JDK Mission Control (JMC)
```

### 3.7. Quy trình xử lý vấn đề hiệu năng

| Triệu chứng | Thu thập | Đọc gì |
|---|---|---|
| CPU cao | JFR (ExecutionSample) | Method nóng nhất |
| Chậm, CPU thấp | Thread dump (2–3 lần cách nhau vài giây) | Thread BLOCKED/WAITING ở đâu; deadlock; chờ I/O, chờ pool |
| Bộ nhớ tăng dần, GC liên tục | GC log, heap dump (`HeapDumpOnOutOfMemoryError`) | Tỷ lệ heap sau GC có tăng dần không (rò rỉ); đối tượng nào chiếm nhiều nhất |
| Pause dài | GC log | Thời gian pause, loại GC, kích thước heap |
| Khởi động chậm | `-Xlog:class+load`, đo thời gian | Số class nạp, khởi tạo tài nguyên |

Nguyên tắc: **đo → giả thuyết → thay đổi một thứ → đo lại**. Không tối ưu dựa trên cảm giác.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Bytecode / class loading | Mã trung gian / nạp class vào JVM |
| JIT, warm-up | Biên dịch tức thời / giai đoạn chạy trước để JIT tối ưu |
| Heap / stack | Vùng nhớ đối tượng / vùng nhớ khung gọi hàm |
| Boxing / unboxing | Bọc giá trị nguyên thủy thành đối tượng / ngược lại |
| Compressed oops | Con trỏ đối tượng nén 32-bit trên JVM 64-bit |
| Generational GC, young/old generation | GC theo thế hệ, vùng trẻ/già |
| Stop-the-world pause | Dừng toàn bộ ứng dụng để GC |
| Ergonomics | JVM tự chọn cấu hình theo phần cứng |
| Heap dump / thread dump | Ảnh chụp bộ nhớ / trạng thái mọi thread |
| Java Flight Recorder (JFR) | Bộ ghi sự kiện, profiler tích hợp |
| Hot spot | Đoạn code chiếm nhiều thời gian CPU nhất |
| Memory leak | Đối tượng không còn dùng nhưng vẫn bị tham chiếu |

## 5. Ví dụ đơn giản

```scala
@main def mem(): Unit =
  val rt = Runtime.getRuntime
  println(s"CPU: ${rt.availableProcessors()}, max heap: ${rt.maxMemory() / 1024 / 1024} MB")
```

```bash
scala-cli run . --java-opt -Xmx256m     # max heap xấp xỉ 256 MB (thường hơi nhỏ hơn, do cách tính vùng survivor)
```

Output thật với `-Xmx512m`: `Heap tối đa (-Xmx): 494 MB` — `maxMemory()` không tính một phần vùng trẻ, nên nhỏ hơn giá trị cờ.

## 6. Ví dụ thực tế

**Kịch bản DE:** job `order-platform` chạy trong Kubernetes Pod `cpu: 1, memory: 1Gi` bị OOMKilled khi file đầu vào tăng từ 50 MB lên 800 MB.
Phân tích: heap mặc định = 1/4 × 1 GiB ≈ 256 MB; `Files.readAllLines` + `List[String]` + `List[OrderLine]` nhân dung lượng file lên nhiều lần; Serial GC do 1 CPU.
Hướng xử lý theo thứ tự: (1) đặt `-XX:MaxRAMPercentage=75` và log GC để có bằng chứng; (2) đổi đọc file sang xử lý dạng stream (Chương 19); (3) chỉ khi cần mới tăng giới hạn Pod.

**Kịch bản BE:** API thỉnh thoảng không phản hồi, CPU ~0%. Thread dump cho thấy toàn bộ thread của pool chờ connection database → pool cạn vì một request giữ connection
trong khi gọi HTTP ra ngoài. Đây không phải vấn đề JVM nhưng thread dump là công cụ tìm ra (Chương 15–16).

## 7. Code đầy đủ

| File | Thí nghiệm | Cờ JVM gợi ý |
|---|---|---|
| [`JvmInfo.scala`](jvm-lab/src/JvmInfo.scala) | 1. Cấu hình JVM đang chạy | `-Xmx512m` |
| [`BoxingLab.scala`](jvm-lab/src/BoxingLab.scala) | 2. Boxing: bộ nhớ và thời gian | `-Xmx2g` |
| [`OpaqueErasure.scala`](jvm-lab/src/OpaqueErasure.scala) | 3. Chữ ký bytecode của opaque type | — |
| [`GcLab.scala`](jvm-lab/src/GcLab.scala) | 4. GC log | `-Xlog:gc`, thử thêm `-XX:+UseG1GC` |
| [`FailureLab.scala`](jvm-lab/src/FailureLab.scala) | 5. OOM, 6. Stack depth | `-Xmx64m`; `-Xss256k` vs `-Xss4m` |
| [`DeadlockLab.scala`](jvm-lab/src/DeadlockLab.scala) | 7. Deadlock, thread dump | — |
| [`JfrLab.scala`](jvm-lab/src/JfrLab.scala) | 8. JFR profiling qua API | — |

Trích thí nghiệm deadlock:

```scala
@main def deadlockLab(): Unit =
  val lockA = Object(); val lockB = Object()
  def worker(name: String, first: Object, second: Object) =
    val t = Thread(() =>
      first.synchronized {
        Thread.sleep(100)
        second.synchronized { println(s"$name xong") }
      }, name)
    t.setDaemon(true); t.start(); t
  worker("transfer-A-to-B", lockA, lockB)
  worker("transfer-B-to-A", lockB, lockA)
  Thread.sleep(500)
  val mx = ManagementFactory.getThreadMXBean
  Option(mx.findDeadlockedThreads()) match
    case None => println("Không có deadlock")
    case Some(ids) =>
      for info <- mx.getThreadInfo(ids, true, true) do
        println(s"  \"${info.getThreadName}\" ${info.getThreadState} chờ ${info.getLockName} đang bị giữ bởi \"${info.getLockOwnerName}\"")
```

## 8. Giải thích code

- `BoxingTools.usedMb` gọi `System.gc()` rồi ngủ 200 ms trước khi đo — `System.gc()` chỉ là **gợi ý**; số đo là xấp xỉ. Đo bộ nhớ chính xác cần heap dump hoặc công cụ như JOL.
- `time` chạy warm-up trước khi đo và cộng `hashCode` kết quả vào `sink` để JIT không loại bỏ phép tính "vô dụng" (dead code elimination). Đây là micro-benchmark minh họa;
  đo nghiêm túc dùng **JMH** (qua plugin `sbt-jmh`) vốn xử lý warm-up, fork JVM, dead code elimination đúng cách.
- `Vector.tabulate(n)(i => i + 1_000)` tránh cache `Integer` (-128..127).
- `OpaqueErasure` dùng `Class.forName("jvmlab.Api$")` — `object Api` biên dịch thành class `Api$` (singleton) cộng class `Api` chứa static forwarder.
- `stackLab` chạy đệ quy trong **thread mới** để cờ `-Xss` chắc chắn áp dụng.
- `deadlockLab` đặt thread là daemon để JVM thoát được dù hai thread kẹt mãi.
- `jfrLab` dùng `jdk.jfr.Recording` để bật đúng hai loại sự kiện, `RecordingFile.readAllEvents` để đọc lại — cùng dữ liệu mà JMC hiển thị.
- Đổi tên `object BoxingLab` thành `BoxingTools`: trùng tên chỉ khác hoa/thường với `@main def boxingLab` khiến compiler cảnh báo (output thật):
  ```
  Generated class jvmlab.BoxingLab differs only in case from jvmlab.boxingLab.
    Such classes will overwrite one another on case-insensitive filesystems.
  ```

## 9. Output mong đợi

Toàn bộ output thật đã được trích ở mục 3. Tóm tắt (máy 1 CPU, ~4 GB RAM):

```
== jvmInfo (-Xmx512m)
Java: 21.0.10 (OpenJDK 64-Bit Server VM)
CPU khả dụng: 1
Heap tối đa (-Xmx): 494 MB
GC: Copy, MarkSweepCompact
JVM args: -Dstdout.encoding=UTF-8 -Xmx512m
Số class đã nạp: 1207
```

Trên máy nhiều CPU, dòng GC sẽ là `G1 Young Generation, G1 Concurrent GC, G1 Old Generation`. Thời gian ở thí nghiệm 2, 4, 8 sẽ khác; tỷ lệ bộ nhớ ở thí nghiệm 2 gần như giống.

## 10. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `OutOfMemoryError: Java heap space` | Giữ quá nhiều dữ liệu; heap mặc định nhỏ trong container | Heap dump để xem ai chiếm; xử lý dạng stream; đặt `MaxRAMPercentage` |
| Pod bị **OOMKilled** nhưng không có `OutOfMemoryError` | Tổng bộ nhớ tiến trình (heap + metaspace + stack thread + direct buffer) vượt giới hạn container | Để chừa 20–25% ngoài heap; không đặt `-Xmx` bằng giới hạn container |
| `StackOverflowError` | Đệ quy sâu | Sửa thuật toán; chỉ tăng `-Xss` khi biết rõ lý do |
| Benchmark cho kết quả "quá tốt" hoặc dao động lớn | Không warm-up, JIT loại bỏ code, đo một lần | JMH |
| Service chậm dần, GC ngày càng thường xuyên | Rò rỉ bộ nhớ (cache không giới hạn, listener không gỡ) | So sánh heap sau GC theo thời gian; heap dump |
| Hiệu năng khác nhau giữa máy dev (8 CPU) và production (1 CPU) | Ergonomics chọn GC và heap khác | In cấu hình JVM khi khởi động (thí nghiệm 1); đặt cờ tường minh |
| `UnsupportedClassVersionError` | Chạy bytecode bằng JDK cũ hơn | Thống nhất JDK 21 |

## 11. Cách debug

1. **Luôn log cấu hình JVM khi khởi động** (CPU, max heap, GC, JVM args) — như thí nghiệm 1. Nửa số sự cố hiệu năng bắt đầu từ "à, production chạy với cấu hình khác".
2. Bật GC log **sẵn** ở production (chi phí rất thấp, xoay vòng file).
3. Bật `-XX:+HeapDumpOnOutOfMemoryError` sẵn — OOM thường không tái hiện được lần hai.
4. Treo/chậm: 3 thread dump cách nhau 5 giây (`jcmd <pid> Thread.print`), so sánh.
5. CPU cao: JFR 60 giây (`jcmd <pid> JFR.start duration=60s filename=/tmp/rec.jfr`).
6. Một thay đổi mỗi lần, đo lại bằng cùng phương pháp.

## 12. Bài tập

1. **(Cơ bản)** Chạy `jvmInfo` với `-Xmx256m`, `-XX:ActiveProcessorCount=2`, `-XX:+UseSerialGC`. Ghi lại output và giải thích từng thay đổi.
2. **(Cơ bản)** Chạy `stackLab` với `-Xss512k`, `-Xss1m`, `-Xss2m`. Độ sâu có tỷ lệ thuận với `-Xss` không? Ước lượng kích thước một stack frame của hàm `depth`.
3. **(Trung bình)** Sửa `BoxingLab` để đo thêm `arr.sum` (dùng `Numeric[Int]`) và một vòng `while` trên `Vector[Int]`. So sánh và giải thích thứ tự nhanh/chậm.
4. **(Trung bình)** Chạy `gcLab` với `-Xmx32m` và `-Xmx512m`, đếm số dòng `Pause` trong GC log. Giải thích vì sao heap lớn hơn → ít pause hơn, và cái giá phải trả là gì.
5. **(Nâng cao)** Sửa `deadlockLab` để **không còn deadlock** theo hai cách: (a) cả hai thread lấy khóa theo cùng thứ tự; (b) dùng `ReentrantLock.tryLock` với timeout.
   Thảo luận: trong backend thật, vấn đề tương tự xuất hiện ở đâu (gợi ý: transaction database cập nhật hai dòng theo thứ tự khác nhau).

## 13. Đáp án và lời giải

Xem [`solutions/ch09-ch11-solutions.md`](../solutions/ch09-ch11-solutions.md).

## 14. Câu hỏi kiểm tra

1. Vì sao phải warm-up trước khi đo hiệu năng trên JVM?
2. `List[Int]` 5 triệu phần tử tốn bao nhiêu bộ nhớ so với `Array[Int]`? Vì sao?
3. Vì sao `Array.foldLeft` có thể chậm hơn vòng `while` nhiều lần?
4. Vì sao cùng một ứng dụng dùng GC khác nhau trên laptop và trong container 1 CPU?
5. Pod bị OOMKilled nhưng log không có `OutOfMemoryError` — giải thích.
6. Service treo, CPU thấp: bạn thu thập gì đầu tiên? Vì sao?
7. Opaque type có tránh được boxing khi đưa vào `List[Vnd]` không?

## 15. Mini project — Hồ sơ hiệu năng của order-platform

Sinh file `orders` 2 triệu dòng (viết generator bằng Scala, seed cố định). Chạy `order-platform` với:
- `-Xmx256m` → có OOM không? Bật `HeapDumpOnOutOfMemoryError`.
- `-Xmx2g -Xlog:gc` → bao nhiêu lần GC, tổng thời gian pause?
- JFR 60 giây → 5 method nóng nhất là gì? (Dự đoán trước khi chạy, so sánh với kết quả.)
Viết báo cáo 1 trang: số liệu, giả thuyết, một thay đổi đề xuất và kết quả đo sau thay đổi.

## 16. Nguồn tài liệu chính thức

- JVM Specification (Java SE 21): https://docs.oracle.com/javase/specs/jvms/se21/html/
- Java 21 — The `java` command (cờ JVM, `-Xlog`, `-XX`): https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html
- HotSpot GC Tuning Guide (Java 21): https://docs.oracle.com/en/java/javase/21/gctuning/
- JDK Flight Recorder API (`jdk.jfr`): https://docs.oracle.com/en/java/javase/21/docs/api/jdk.jfr/module-summary.html
- `jcmd`: https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html
- JMH: https://github.com/openjdk/jmh · sbt-jmh: https://github.com/sbt/sbt-jmh
- Scala 3 Book — Scala and the JVM (tương tác Java): https://docs.scala-lang.org/scala3/book/interacting-with-java.html

## 17. Checklist tự đánh giá

- [ ] Tôi đọc được cấu hình JVM đang chạy và biết vì sao nó như vậy.
- [ ] Tôi phân biệt heap/stack, OOM/StackOverflow và nguyên nhân thường gặp.
- [ ] Tôi ước lượng được chi phí boxing và biết khi nào nó đáng quan tâm.
- [ ] Tôi đọc được một dòng GC log.
- [ ] Tôi đặt được cờ JVM hợp lý cho container.
- [ ] Tôi lấy và đọc được thread dump, phát hiện deadlock.
- [ ] Tôi dùng JFR tìm hot spot và tuân thủ "đo trước, sửa sau".
