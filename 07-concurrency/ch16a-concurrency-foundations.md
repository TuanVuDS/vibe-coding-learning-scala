# Chương 16A — Concurrency nền tảng: Thread, Future, ExecutionContext

> **Phiên bản:** Scala 3.9.0 · JDK 21 · munit 1.2.4 · chỉ standard library + `java.util.concurrent`
> **Code:** [`concurrency-lab/`](concurrency-lab/) — 9 thí nghiệm + bộ công cụ `Toolkit` (delay, timeout, backoff, retry)
> **Trạng thái kiểm chứng:** Mọi thí nghiệm **đã chạy thật**; 9/9 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> **Môi trường đo: máy 1 CPU**, Linux, OpenJDK 21.0.10. Điều này **ảnh hưởng trực tiếp** tới kết quả của thí nghiệm 1, 3, 4 — và chính là một phần bài học (mục 3.2, 3.5).

---

## Vị trí trong lộ trình

Theo [curriculum](../00-roadmap/01-curriculum.md), chương này học **trước** Chương 13 (chọn framework) và 16B (Cats Effect).
Mục tiêu: hiểu concurrency bằng công cụ của **ngôn ngữ và standard library**, gặp tận tay các giới hạn của `Future` — để khi học Cats Effect, bạn biết nó giải quyết vấn đề gì.

| Công cụ | Thuộc về |
|---|---|
| `Thread`, `synchronized`, `@volatile` | Ngôn ngữ / JVM |
| `java.util.concurrent` (`Executors`, `AtomicLong`, `ConcurrentHashMap`...) | JDK |
| `Future`, `Promise`, `ExecutionContext`, `blocking`, `Await` | Scala standard library |
| `IO`, `Resource`, `Fiber`, `Ref` | Thư viện ngoài (Cats Effect — Chương 16B) |

## 1. Mục tiêu học tập

- Phân biệt **concurrency** và **parallelism**; biết khi nào thêm thread làm chương trình nhanh hơn, khi nào không.
- Nhận diện và sửa **race condition** bằng `synchronized`, atomic, và trạng thái bất biến + CAS.
- Dùng `Future` đúng cách: tuần tự vs song song, lỗi lan truyền, phục hồi.
- Chọn và cấu hình **ExecutionContext**; hiểu `blocking`, thread starvation.
- Cài đặt **timeout** và **retry có backoff**; hiểu vì sao `Future` **không hủy được**.
- Quản lý tài nguyên an toàn với `Using`.

## 2. Kiến thức tiên quyết

- Chương 7 (`Either`, `Try`), Chương 11 (thread, stack, deadlock), Chương 12 (server xử lý request đồng thời).

## 3. Lý thuyết

### 3.1. Concurrency và parallelism

| | Concurrency (đồng thời) | Parallelism (song song) |
|---|---|---|
| Định nghĩa | Nhiều việc **tiến triển xen kẽ** trong cùng khoảng thời gian | Nhiều việc **chạy cùng một thời điểm** trên nhiều CPU |
| Cần nhiều CPU? | Không | Có |
| Lợi ích chính | Không để thread ngồi chờ I/O (DB, HTTP, file) | Tăng tốc tính toán CPU |
| Ví dụ BE | Server phục vụ 1.000 request đang chờ database | Nén ảnh, mã hóa |
| Ví dụ DE | Gọi 10 API nguồn dữ liệu cùng lúc | Spark chia dữ liệu cho nhiều executor |

**Thí nghiệm 4 (`cpuLab`)** đếm số nguyên tố tới 2 triệu, tuần tự và chia 4 `Future` trên pool 4 thread. Output thật trên máy **1 CPU**:

```
CPU khả dụng: 1
tuần tự : 148933 số nguyên tố, 1156ms
4 Future: 148933 số nguyên tố, 1094ms  (tăng tốc x1.06)
```

Không tăng tốc: 4 thread chia nhau **một** CPU. Trên máy 4 CPU, tăng tốc kỳ vọng gần x4 (trừ chi phí điều phối). Bài học: **parallelism bị giới hạn bởi số CPU thật**
— và container thường giới hạn CPU (`cpu: 1` trong Kubernetes). Luôn đo trên môi trường giống production.

### 3.2. Race condition

`count += 1` không phải một bước mà là ba: **đọc** → **cộng** → **ghi**. Hai thread đọc cùng giá trị 5, cùng ghi 6 → mất một lần tăng (*lost update*).

**Thí nghiệm 1 (`raceLab`)** — output thật, máy 1 CPU:

```
CPU: 1, kỳ vọng: 8000000
var không đồng bộ : 8,000,000  (mất 0)  94ms
synchronized      : 8,000,000  181ms
AtomicLong        : 8,000,000  163ms
var + cửa sổ race : 20,001 / 80,000  (mất 59,999)
```

Đọc kỹ dòng đầu: **`var` không đồng bộ vẫn ra đúng 8 triệu**. Trên 1 CPU, các thread hiếm khi bị chuyển đổi đúng giữa bước đọc và bước ghi (một phép tăng chỉ mất vài nano giây).
Chạy với `-XX:ActiveProcessorCount=4` cũng **không đổi** kết quả — cờ đó chỉ "đánh lừa" JVM về số CPU, không tạo thêm CPU thật.

Dòng cuối mở rộng "cửa sổ race" bằng `Thread.yield()` giữa đọc và ghi — mô phỏng hệ điều hành tạm dừng thread đúng lúc tệ nhất: **mất ~75% số lần cập nhật**, lặp lại ổn định qua 3 lần chạy.

> **Bài học quan trọng nhất của chương:** code có race condition có thể **pass mọi test trên máy dev** (ít CPU, tải thấp) và **sai trên production** (nhiều CPU, tải cao, GC pause, thread bị tạm dừng).
> Không thể chứng minh code đồng thời đúng bằng cách chạy thử. Phải **thiết kế để đúng**: không có trạng thái mutable chia sẻ, hoặc mọi truy cập đều được đồng bộ.

Các cách sửa:

| Cách | Cơ chế | Khi nào dùng |
|---|---|---|
| `synchronized` | Khóa: một thread vào vùng găng tại một thời điểm | Cập nhật nhiều biến cùng lúc; đơn giản |
| `AtomicLong`, `AtomicReference` | Lệnh CPU nguyên tử (CAS), không khóa | Một biến; bộ đếm |
| Trạng thái bất biến + `AtomicReference` + vòng CAS | Tính trạng thái mới, CAS, thất bại thì làm lại | Trạng thái phức tạp (`ProductStore`, Chương 12) |
| Không chia sẻ | Mỗi thread dữ liệu riêng, gộp kết quả cuối | Tính toán song song (Monoid, Chương 8B) |
| Database / hàng đợi | Đẩy việc đồng bộ cho hệ thống chuyên dụng | Nhiều tiến trình/máy (Chương 15, 28) |

### 3.3. Future: giá trị sẽ có trong tương lai

```scala
given ExecutionContext = ExecutionContext.global
val f: Future[Int] = Future { tínhToánLâu() }   // bắt đầu chạy NGAY trên thread khác
f.map(_ * 2)                                      // biến đổi khi xong — không chặn
f.onComplete(r => ...)                            // callback
Await.result(f, 5.seconds)                        // CHẶN thread hiện tại — chỉ dùng ở biên (main, test)
```

**Future là eager (chạy ngay khi tạo).** Thí nghiệm 2 (`eagerLab`), output thật:

```
1. trước khi tạo Future
2. thân Future đang chạy
3. sau khi tạo, CHƯA gọi map hay Await
Số email đã gửi: 2
```

Hệ quả:
- `val a = sendEmail(); val b = sendEmail()` là **hai** lần gửi. `Future` không phải "mô tả một phép tính" có thể tái sử dụng — không thỏa referential transparency (Chương 7).
- Thứ tự **khai báo** quyết định tuần tự hay song song (mục 3.4) — dễ nhầm khi refactor.
- Đây là lý do chính để dùng `IO` của Cats Effect: `IO` là **mô tả**, chỉ chạy khi được yêu cầu (Chương 16B).

### 3.4. Tuần tự hay song song? Và cái bẫy của pool

**Thí nghiệm 3 (`ioLab`)**: ba lời gọi "I/O" mỗi cái chờ 300 ms. Output thật trên máy 1 CPU:

```
global, for-comprehension (tuần tự)             : 1055ms
global, tạo trước rồi gộp (song song?)            : 903ms
global + blocking {}                              : 307ms
pool I/O riêng 8 thread                           : 336ms
global, chờ KHÔNG chặn thread (scheduler)          : 312ms
```

1. **for-comprehension là tuần tự:** `b` chỉ được tạo sau khi `a` xong → ~900 ms + chi phí khởi động.
2. **Tạo trước rồi gộp lẽ ra song song — nhưng vẫn ~900 ms!** `ExecutionContext.global` có số thread **bằng số CPU** (ở đây 1). `Thread.sleep` (giống JDBC, HTTP client chặn) **chiếm** thread duy nhất → ba việc vẫn chạy lần lượt.
3. **`blocking { ... }`** báo cho `global` rằng đoạn này sẽ chặn → nó tạo thêm thread tạm → ~300 ms.
4. **Pool riêng cho I/O** (8 thread) → ~300 ms. Đây là cách phổ biến: tách pool CPU (cỡ số lõi) khỏi pool I/O chặn (lớn hơn, có giới hạn).
5. **Chờ không chặn thread** (`Toolkit.delay` dùng scheduler) → ~300 ms mà không tốn thread nào trong lúc chờ. Thư viện I/O bất đồng bộ thật (non-blocking) hoạt động theo kiểu này.

| Loại việc | ExecutionContext phù hợp |
|---|---|
| Tính toán CPU ngắn | `global` / pool cỡ số CPU |
| I/O **chặn** (JDBC, `Thread.sleep`, file) | Pool riêng có giới hạn, hoặc `blocking {}` trên `global` |
| I/O **không chặn** | Pool nhỏ là đủ |

### 3.5. Thread starvation — "deadlock" của thread pool

**Thí nghiệm 8 (`starvationLab`)**: pool 1 thread; Future ngoài **chờ chặn** (`Await`) Future trong — nhưng Future trong cần chính thread đó để chạy. Output thật:

```
Kết quả: Failure(java.util.concurrent.TimeoutException: Future timed out after [1 second])
```

Không có `timeout`, chương trình **treo vĩnh viễn**. Trong backend thật: pool 10 thread, 10 request cùng chờ một tác vụ khác trên cùng pool → service đứng im, CPU 0%
(kịch bản BE trong Chương 11). Quy tắc: **không `Await` bên trong `Future`**; kết hợp bằng `map`/`flatMap`.

### 3.6. Lỗi lan truyền và phục hồi

Thí nghiệm 5 (`errorLab`), output thật:

```
map trên Future lỗi:               Failure(java.lang.IllegalArgumentException: không có sku X)
recover:                           Success(0)
recoverWith sang nguồn khác:       Success(100)
traverse, một phần tử lỗi:         Failure(java.lang.IllegalArgumentException: không có sku X)
exception trong map:               Failure(java.lang.ArithmeticException: / by zero)
transform thành Try:               Success(Left(không có sku X))
```

- Lỗi **đi qua** `map`/`flatMap` như `Either` (Chương 7); exception ném trong `map` cũng thành `Failure`.
- `recover` (giá trị thay thế), `recoverWith` (Future thay thế — fallback sang nguồn khác).
- `Future.traverse`/`sequence` thất bại nếu **một** phần tử thất bại — nhưng **các phần tử khác vẫn đang chạy** (không bị hủy).
- `Future[A]` chỉ có kiểu lỗi `Throwable`. Muốn lỗi nghiệp vụ có kiểu: `Future[Either[MyError, A]]` — cồng kềnh; Cats Effect và ZIO xử lý gọn hơn.

### 3.7. Timeout và cancellation

```scala
def withTimeout[A](f: Future[A], d: FiniteDuration)(using ExecutionContext): Future[A] =
  val p = Promise[A]()
  val task = scheduler.schedule(() => p.tryFailure(TimeoutException(s"quá $d")), ...)
  f.onComplete { r => task.cancel(false); p.tryComplete(r) }
  p.future
```

`Promise` là "Future có thể hoàn thành bằng tay"; `tryComplete` đảm bảo chỉ bên **đến trước** thắng. Thí nghiệm 6 (`timeoutLab`), output thật:

```
Kết quả có timeout: Failure(java.util.concurrent.TimeoutException: quá 200 milliseconds)
Ngay sau timeout, side effect = 0
0.5 giây sau,      side effect = 1  <- công việc VẪN chạy và VẪN ghi DB
```

**Future không có cancellation.** Timeout chỉ khiến **người chờ** thôi chờ; công việc vẫn chạy, vẫn chiếm thread, vẫn ghi DB.
Trong backend: client đã nhận 504 và **retry**, trong khi lần đầu vẫn ghi thành công → dữ liệu trùng. Hệ quả thiết kế:
- Thao tác ghi phải **idempotent** (Chương 15, 17, 30).
- Cần cơ chế hủy thật (interruption) — Cats Effect có cancellation tích hợp: timeout **hủy** fiber và chạy finalizer (Chương 16B).

### 3.8. Retry với backoff

```scala
Toolkit.backoffDelays(100.millis, 2.0, 1.second, retries = 4)   // 100, 200, 400, 800 ms
Toolkit.retry(delays, _.isInstanceOf[java.io.IOException])(action)
```

Thí nghiệm 7 (`retryLab`): lỗi tạm thời 2 lần, thành công lần 3 sau ~404 ms (100 + 200 + thời gian chạy). Nguyên tắc:
- **Chỉ retry lỗi tạm thời** (timeout mạng, 503). Không retry lỗi nghiệp vụ (400, 422) — test `retry: KHÔNG retry lỗi không thuộc loại retryable`.
- **Backoff lũy thừa có trần** để không dội bão request vào hệ thống đang quá tải; thực tế thêm **jitter** (ngẫu nhiên hóa) để nhiều client không retry cùng lúc.
- **Giới hạn số lần**; hết lượt thì trả lỗi cuối cùng (có test).
- Retry thao tác ghi → phải idempotent (mục 3.7).
- `backoffDelays` là hàm **thuần** → test lịch chờ không cần chờ thật.

### 3.9. Quản lý tài nguyên

Thí nghiệm 9 (`resourceLab`), output thật:

```
mở connection; mở statement; đóng statement; đóng connection;
Failure(query lỗi)
```

`scala.util.Using.Manager` đóng tài nguyên **theo thứ tự ngược** lúc mở, **kể cả khi lỗi**, và trả `Try`. Giới hạn: `Using` làm việc với code **đồng bộ**.
Với `Future`, tài nguyên phải đóng khi Future hoàn thành (kể cả khi lỗi, kể cả khi bị "timeout") — rất dễ rò rỉ. `Resource` của Cats Effect giải quyết bài toán này (Chương 16B).

### 3.10. Deadlock

Đã thí nghiệm ở Chương 11 (`deadlockLab`) và lời giải (lấy khóa theo thứ tự toàn cục; `tryLock` có timeout). Thread starvation (mục 3.5) là "họ hàng" của deadlock ở cấp thread pool.

### 3.11. Vì sao cần effect system — tổng kết các giới hạn của Future

| Vấn đề gặp trong chương | Future | Cats Effect `IO` (Chương 16B) |
|---|---|---|
| Chạy ngay khi tạo, không tái sử dụng được | Eager | Lazy — là mô tả, chạy khi được yêu cầu |
| Không hủy được khi timeout | ✗ | Cancellation tích hợp, chạy finalizer |
| Phải truyền `ExecutionContext` khắp nơi | `using ExecutionContext` | Runtime quản lý; `IO.blocking` cho việc chặn |
| Tài nguyên với code bất đồng bộ | Tự xử lý, dễ rò | `Resource` |
| Timeout, retry, race, chạy song song có giới hạn | Tự viết (như `Toolkit`) | Có sẵn, đã được kiểm thử |
| Hàng triệu tác vụ đồng thời | Mỗi tác vụ chặn = một thread | Fiber nhẹ |

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Concurrency / parallelism | Đồng thời / song song |
| Race condition, lost update | Điều kiện tranh chấp, mất cập nhật |
| Critical section | Vùng găng — đoạn chỉ một thread được vào |
| Atomic, CAS | Nguyên tử; compare-and-set |
| Thread pool, ExecutionContext | Nhóm thread tái sử dụng; nơi Future chạy |
| Blocking / non-blocking I/O | I/O chặn thread / không chặn |
| Thread starvation | Thread pool cạn vì các thread chờ nhau |
| Eager / lazy | Chạy ngay / chạy khi cần |
| Promise | Đối tượng hoàn thành Future bằng tay |
| Timeout, cancellation | Hết thời gian chờ, hủy tác vụ |
| Exponential backoff, jitter | Chờ tăng theo lũy thừa, thêm ngẫu nhiên |
| Idempotent | Thực hiện nhiều lần như một lần |

## 5. Ví dụ đơn giản

```scala
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

@main def simple(): Unit =
  given ExecutionContext = ExecutionContext.global
  val price: Future[Long] = Future(120_000L)
  val qty: Future[Int]    = Future(3)
  val total = for p <- price; q <- qty yield p * q     // price, qty đã chạy song song vì tạo TRƯỚC
  println(Await.result(total, 1.second))                 // 360000 — Await chỉ ở biên chương trình
```

## 6. Ví dụ thực tế

- **BE:** trang chi tiết sản phẩm gọi 3 service (giá, tồn kho, đánh giá). Gọi song song có timeout từng lời gọi; service đánh giá lỗi thì `recover` thành danh sách rỗng (trang vẫn hiển thị); service giá lỗi thì trả lỗi (không bán với giá sai).
- **DE:** job thu thập dữ liệu từ 20 API đối tác: giới hạn đồng thời (không quá 4 kết nối mỗi đối tác), retry lỗi 503 với backoff, không retry 400, ghi kết quả idempotent theo khóa (đối tác, ngày).

Cả hai kịch bản đều dùng đủ: song song, timeout, retry có chọn lọc, fallback, idempotency — bài tập 2–4 xây các mảnh còn thiếu.

## 7. Code đầy đủ

- [`Toolkit.scala`](concurrency-lab/src/main/scala/vn/scalaeng/conc/Toolkit.scala) — `named`, `fixedPool`, `delay`, `withTimeout`, `backoffDelays`, `retry`.
- [`Labs.scala`](concurrency-lab/src/main/scala/vn/scalaeng/conc/Labs.scala) — 9 thí nghiệm `@main`.
- [`ToolkitSuite.scala`](concurrency-lab/src/test/scala/vn/scalaeng/conc/ToolkitSuite.scala) — 9 test.

Chạy từng thí nghiệm:

```bash
cd 07-concurrency/concurrency-lab
scala-cli run . --main-class vn.scalaeng.conc.raceLab
scala-cli run . --main-class vn.scalaeng.conc.ioLab
# ... eagerLab, cpuLab, errorLab, timeoutLab, retryLab, starvationLab, resourceLab
scala-cli test .
```

## 8. Giải thích code

- `named(prefix)` đặt tên thread (`io-1`, `cpu-3`, `scheduler-1`) — thread dump và log đọc được (Chương 11). Thread daemon để không giữ JVM sống khi `main` kết thúc.
- `delay` dùng `ScheduledExecutorService`: đăng ký "hoàn thành Promise sau d ms" rồi **trả thread về ngay**. 50 lần chờ 200 ms song song xong trong ~200 ms (test).
- `withTimeout` hủy tác vụ hẹn giờ khi Future gốc xong trước (`task.cancel(false)`) để không tích tụ tác vụ rác trong scheduler.
- `retry` là đệ quy qua `recoverWith`: mỗi lần thất bại lấy độ trễ đầu danh sách, chờ **không chặn** (`delay`), gọi lại với phần còn lại.
- `raceLab` dùng `var` được closure bắt lại → trình biên dịch Scala biến nó thành đối tượng trên heap (`LongRef`) dùng chung giữa các thread — đúng hình thái của "biến chia sẻ".
- `cpuLab` chạy warm-up trước khi đo để JIT biên dịch hàm (Chương 11).

## 9. Output mong đợi

Đã trích ở mục 3 (tất cả là output thật trên máy 1 CPU). Trên máy nhiều CPU, hãy chạy lại và ghi lại: `raceLab` dòng đầu có thể **mất cập nhật thật**;
`cpuLab` có tăng tốc rõ rệt; `ioLab` dòng 2 có thể giảm xuống (vì `global` có nhiều thread hơn). Các kết luận ở mục 3 không đổi.

## 10. Lỗi thường gặp

| Lỗi | Triệu chứng | Cách đúng |
|---|---|---|
| Biến mutable chia sẻ không đồng bộ | Sai số ngẫu nhiên dưới tải; test máy dev vẫn pass | Bất biến + CAS / atomic / khóa |
| `Await` trong code nghiệp vụ hoặc trong Future | Thread starvation, treo | `map`/`flatMap`; `Await` chỉ ở `main`/test |
| I/O chặn trên `global` | Throughput thấp, độ trễ cao bất thường | Pool I/O riêng hoặc `blocking {}` |
| Viết for-comprehension mà tưởng song song | Chậm gấp N lần | Tạo Future trước, hoặc `Future.sequence`/`traverse` |
| Tưởng timeout hủy công việc | Ghi trùng dữ liệu khi client retry | Thao tác idempotent; effect system có cancellation |
| Retry mọi lỗi, không giới hạn, không backoff | Bão request khi hệ thống phụ thuộc gặp sự cố | Chỉ lỗi tạm thời, giới hạn lần, backoff + jitter |
| Pool không đặt tên, không giới hạn | Không đọc được thread dump; tràn thread | `named(...)`, `newFixedThreadPool(n)` |
| Đo tăng tốc song song trên laptop rồi áp cho container 1 CPU | Kỳ vọng sai về hiệu năng | Đo trên cấu hình giống production |

## 11. Cách debug

- **Thread dump** khi service treo: `jcmd <pid> Thread.print` (JDK đầy đủ) — tìm nhiều thread cùng tên pool đang `WAITING` trong `Await`/`park` → starvation; `BLOCKED` vòng tròn → deadlock (Chương 11).
- **Lỗi không tái hiện được:** thêm `Thread.yield()`/`sleep` nhỏ giữa đọc và ghi để mở rộng cửa sổ race (như `raceLab`); chạy test nhiều lần; chạy trên máy nhiều lõi.
- **In tên thread** trong log (`Thread.currentThread.getName`) để biết đoạn code chạy trên pool nào.
- **Đo trước khi kết luận** về song song: in số CPU khả dụng cùng kết quả đo.

## 12. Bài tập

1. **(Cơ bản)** Viết `BankAccount` với `deposit`/`withdraw` (không cho số dư âm) an toàn đa luồng bằng `AtomicReference[Long]` + vòng CAS. Viết test 8 thread rút tiền đồng thời: tổng tiền rút thành công không vượt số dư ban đầu.
2. **(Cơ bản)** Viết `firstSuccessful[A](sources: List[() => Future[A]]): Future[A]` — thử lần lượt từng nguồn, trả kết quả của nguồn thành công đầu tiên; tất cả thất bại thì trả lỗi của nguồn cuối.
3. **(Trung bình)** Viết `traverseLimited[A, B](xs: List[A], maxConcurrent: Int)(f: A => Future[B]): Future[List[B]]` — chạy song song nhưng **không quá `maxConcurrent`** tác vụ cùng lúc, giữ thứ tự kết quả. Test bằng cách đếm số tác vụ đang chạy tối đa.
4. **(Trung bình)** Viết **circuit breaker** dạng máy trạng thái thuần: `Closed` (đếm lỗi liên tiếp), `Open(until)` (từ chối ngay), `HalfOpen` (cho thử một lần). Đồng hồ là tham số. Test chuyển trạng thái không cần chờ thật.
5. **(Nâng cao)** Sửa `starvationLab` theo hai cách: (a) không dùng `Await` (dùng `flatMap`); (b) giữ `Await` nhưng dùng pool khác cho Future trong. Giải thích vì sao (a) là cách đúng và (b) chỉ là giải pháp tạm.

## 13. Đáp án và lời giải

Xem [`solutions/ch12-ch16a-solutions.md`](../solutions/ch12-ch16a-solutions.md).

## 14. Câu hỏi kiểm tra

1. Vì sao thí nghiệm `cpuLab` không tăng tốc trên máy biên soạn? Bạn dự đoán gì trên máy 8 CPU?
2. Vì sao `var` không đồng bộ vẫn ra đúng 8 triệu trong `raceLab`? Kết luận "code này an toàn" có đúng không?
3. Ba cách sửa race condition và khi nào dùng mỗi cách?
4. Future "eager" gây ra hệ quả gì khi refactor?
5. Vì sao `Future.sequence` của ba lời gọi `Thread.sleep` trên `global` vẫn mất ~900 ms trên máy 1 CPU?
6. Thread starvation khác deadlock thế nào? Làm sao phòng tránh?
7. Timeout với Future có hủy công việc không? Hệ quả với thao tác ghi?
8. Lỗi nào nên retry, lỗi nào không? Vì sao cần backoff và jitter?

## 15. Mini project

Viết **trình gọi đối tác** cho kịch bản DE ở mục 6: danh sách 20 "đối tác" giả lập (mỗi đối tác là hàm trả `Future` với độ trễ và tỷ lệ lỗi ngẫu nhiên có seed cố định).
Yêu cầu: tối đa 4 lời gọi đồng thời (`traverseLimited`), timeout 500 ms mỗi lời gọi, retry lỗi tạm thời với backoff (tối đa 3 lần), đối tác lỗi 5 lần liên tiếp thì circuit breaker mở.
In báo cáo: thành công/thất bại theo đối tác, số lần retry, tổng thời gian. Mọi thành phần có test; phần chạy thật chỉ ở `@main`.

## 16. Nguồn tài liệu chính thức

- Scala — Futures and Promises: https://docs.scala-lang.org/overviews/core/futures.html
- Scala API `scala.concurrent`: https://www.scala-lang.org/api/current/scala/concurrent.html
- Scala API `scala.util.Using`: https://www.scala-lang.org/api/current/scala/util/Using$.html
- JDK `java.util.concurrent`: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/package-summary.html
- Java Language Specification — Threads and Locks (Memory Model): https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html

## 17. Checklist tự đánh giá

- [ ] Tôi giải thích được concurrency vs parallelism và đã thấy giới hạn số CPU bằng thí nghiệm.
- [ ] Tôi không tin "test pass" là bằng chứng code đồng thời đúng.
- [ ] Tôi sửa được race condition bằng ít nhất 3 cách và biết khi nào dùng cách nào.
- [ ] Tôi biết Future là eager và chọn được tuần tự/song song có chủ đích.
- [ ] Tôi chọn đúng ExecutionContext cho việc CPU, I/O chặn, I/O không chặn.
- [ ] Tôi không dùng `Await` ngoài biên chương trình.
- [ ] Tôi cài được timeout, retry có backoff, và hiểu vì sao cần idempotency.
- [ ] Tôi liệt kê được các giới hạn của Future mà Cats Effect giải quyết.
