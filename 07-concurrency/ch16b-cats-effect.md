# Chương 16B — Effect system với Cats Effect 3

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Cats Effect / testkit 3.7.1 · munit 1.2.4 · munit-cats-effect 2.2.0.
> **Code:** [effects-lab/](effects-lab/) và [lời giải](../solutions/effects-code/).
> **Trạng thái kiểm chứng:** 22/22 (sbt + Scala CLI) + 12/12 lời giải, munit thật; đã review chéo ngày 16/10/2026. sbt do Claude chạy ngoài sandbox; vòng sửa ngày 17/10/2026 chạy lại Scala CLI với `-Werror`, chờ lead kiểm tra các sửa đổi.

## Vị trí trong lộ trình

Hãy học chương này **sau [Ch16A](ch16a-concurrency-foundations.md), trước [Ch13](../05-backend/ch13-choosing-a-framework.md)**. Ch16A giúp bạn quan sát giới hạn của `Future`; Ch16B giới thiệu công cụ để quản lý vòng đời tác vụ. Ch13–15 dùng `IO`, `Resource` và `Ref` để xây backend, nên cần hiểu những kiểu này trước khi đọc code framework. Số chương thể hiện nhóm chủ đề; thứ tự học đi theo các phụ thuộc đó.

## 1. Mục tiêu học tập

Chương này giúp bạn mô tả tác vụ bằng IO, ghép tuần tự/đồng thời, hủy có dọn dẹp, quản lý tài nguyên, chia sẻ trạng thái an toàn và test bằng thời gian ảo.

**Ranh giới:** `for`, hàm, generic type, pattern matching là tính năng ngôn ngữ; `Future` thuộc standard library. `IO`, runtime (bộ thực thi), `Resource`, `Fiber`, `Ref`, `Deferred`, `Semaphore`, `TestControl` là **thư viện ngoài Cats Effect**. `parTraverse` / `parMapN` là abstraction/syntax của Cats kết hợp instance cho IO.

## 2. Kiến thức tiên quyết

Đọc [Ch16A](ch16a-concurrency-foundations.md), mục 3.11, và [Toolkit](concurrency-lab/src/main/scala/vn/scalaeng/conc/Toolkit.scala). Cần hiểu flatMap, Either, for-comprehension, race condition (điều kiện tranh chấp) và starvation (cạn thread thực thi).

Lab dùng dịch vụ và pool **giả trong bộ nhớ**; không cần HTTP, database, Docker hoặc mạng lúc test sau khi tải đủ dependency. Kết quả không chứng minh tốc độ JDBC hay HTTP thật.

## 3. Lý thuyết

### 3.1. Đối chiếu với Future bằng thí nghiệm

`Comparisons` tái hiện ý tưởng Ch16A mà không sửa lab cũ. Mọi dòng trong bảng đều có test munit thật; output ở mục 9. Đồng bộ bằng Promise/Deferred thay vì dựa vào phỏng đoán tốc độ scheduler.

| Giới hạn | Thí nghiệm thực hiện | Cách IO giải quyết |
|---|---|---|
| Eager | Future tăng counter trước khi ghép kết quả; IO chưa tăng | Suspend bằng delay; chạy mô tả hai lần tăng hai lần |
| Không có cancellation | Future tiếp tục ghi sau khi người chờ timeout | `start/cancel/join`, kiểm tra `Canceled` và `onCancel` |
| Timeout không dừng việc | Mở cổng ghi sau timeout: Future vẫn ghi | IO đang chờ Deferred bị hủy; mở cổng sau đó không còn ghi |
| Retry tự viết | Đợi lại Future lỗi: thân chỉ chạy một lần; factory chạy ba lần | Policy IO cũng tự viết, nhưng replay mô tả và sleep hủy được |
| Tài nguyên async | Using đóng trước Future đọc | Resource giữ tới khi IO hoàn tất, đóng ngược cả lúc lỗi/hủy |
| Cạn pool do chặn | Future ngoài Await Future trong trên pool một thread bị timeout | join treo fiber; blocking tách lời gọi chặn khỏi compute |

Future **tái sử dụng được kết quả**; đọc lại nó không chạy lại thân. IO chưa memoize thì tái chạy mô tả. Không nên nói Future “không thể dùng lại” dưới mọi nghĩa.

### 3.2. IO lazy và referential transparency

Khi tạo một `Future` gửi email, bạn có thể đã khởi động việc gửi trước khi kịp đặt timeout. `IO[A]` giải quyết vấn đề này bằng cách giữ **mô tả phép tính** trả về `A`, có thể thất bại hoặc bị hủy. `IO.delay(body)` chưa chạy `body`; `IO(body)` là cách viết ngắn. Runtime chỉ thực hiện body khi diễn giải đến bước đó.

Đoạn sau trích phần thân `Comparisons.eager` trong [Comparisons.scala](effects-lab/src/main/scala/vn/scalaeng/effects/Comparisons.scala). Hai bộ đếm AtomicInteger đã được tạo trước đoạn này; Future cũng đã chạy xong để phép so sánh không phụ thuộc tốc độ scheduler.

```scala
description = IO.delay(ioCount.incrementAndGet()) // Chỉ đặt tên mô tả, chưa tăng.
before <- IO.delay((futureCount.get(), ioCount.get())) // Chụp trạng thái trước.
_ <- description // Ghép lần chạy thứ nhất vào chuỗi IO.
_ <- description // Ghép lại cùng mô tả: body được chạy thêm một lần.
after <- IO.delay((futureCount.get(), ioCount.get())) // Đọc lại sau hai lần chạy.
```

`IO.pure(value)` nhận giá trị **đã được tính**. Vì vậy, `IO.pure(sendEmail())` gọi `sendEmail()` trước khi Cats Effect quản lý việc gửi. Khi cần trì hoãn cả bước dựng một IO khác, bạn dùng `IO.defer`. Không nên đặt side effect bên ngoài IO rồi mong `pure` làm nó trở thành lazy.

Referential transparency (tính trong suốt tham chiếu) ở đây cho phép bạn thay tên mô tả bằng biểu thức tạo nó mà không chạy side effect sớm. Nó không biến thao tác gửi email thành hàm thuần. Nếu bạn ghép `action *> action`, chương trình vẫn thực hiện action hai lần. Nếu muốn lưu lại một kết quả để dùng nhiều lần, bạn phải chủ động chọn memoization và quyết định phạm vi sống của cache.

Future vẫn phù hợp khi bạn tích hợp API trả Future hoặc muốn chia sẻ một kết quả đang chạy. IO đòi hỏi một runtime, nhưng giúp ghép timeout, retry, tài nguyên và cancellation trong cùng mô tả. Với service có nhiều bước cần dọn dẹp, khả năng ghép vòng đời thường đáng giá hơn chi phí học thêm abstraction này.

### 3.3. IOApp, flatMap và for-comprehension

Sau khi dựng mô tả, chương trình cần một điểm vào để chạy nó. `IOApp.Simple` cung cấp runtime và yêu cầu bạn định nghĩa `run: IO[Unit]`. Các hàm nghiệp vụ trả IO cho người gọi ghép tiếp; chúng không tự gọi `unsafeRunSync()` ở mỗi bước.

Đây là `SimpleExample` trong [EffectsDemo.scala](effects-lab/src/main/scala/vn/scalaeng/effects/EffectsDemo.scala), với import `cats.effect.{IO, IOApp, Ref}`:

```scala
object SimpleExample extends IOApp.Simple: // IOApp sở hữu runtime của executable.
  val run: IO[Unit] = for
    counter <- Ref.of[IO, Int](0) // Cấp trạng thái khi chương trình chạy.
    action = counter.updateAndGet(_ + 1) // Dấu = chỉ đặt tên mô tả.
    first <- action // Dấu <- ghép việc chạy và lấy kết quả.
    second <- action // Bước này chỉ chạy sau first.
    _ <- IO.println(s"first=$first second=$second") // Ghép cả việc in vào IO.
  yield ()
```

`a.flatMap(x => b(x))` chỉ chạy `b(x)` sau khi `a` thành công. Scala dịch for-comprehension thành các lời gọi `flatMap` và `map`; cú pháp `for` **không tự tạo concurrency**. Toán tử `*>` cũng ghép tuần tự, nhưng bỏ kết quả bên trái. Bạn dùng hai cách này khi bước sau phụ thuộc bước trước hoặc nghiệp vụ yêu cầu thứ tự.

Khi nhúng IO vào một hệ thống khác, bạn có thể cần một runtime tự cấu hình. Khi đó, bạn cũng phải chịu trách nhiệm về vòng đời runtime. Với executable trong lab, IOApp là lựa chọn dễ quản lý hơn. Khi chuyển từ API Future, `IO.fromFuture(IO(factory()))` trì hoãn việc gọi factory; `IO.pure(future)` không trì hoãn Future đã tạo, và Future bên dưới vẫn không tự có cancellation.

### 3.4. delay, blocking, interruptible và sleep

Bọc một lời gọi JDBC trong IO chưa đủ để bảo vệ compute thread. Runtime cần biết tác vụ nào là lời gọi chặn để chuyển nó sang blocking pool. Compute pool dành cho việc chạy các bước của fiber; blocking pool nhận công việc phải giữ thread trong lúc chờ.

| Công cụ | Hoạt động / khi dùng | Đánh đổi và lỗi tránh |
|---|---|---|
| `IO.delay(body)` | Trì hoãn code đồng bộ ngắn tại nơi fiber đang chạy | Không tự chuyển pool; tránh Await, sleep, JDBC chậm |
| `IO.blocking(body)` | Chạy lời gọi chặn trên blocking pool rồi quay lại | Bảo vệ compute; cancellation thường đợi body, không tự interrupt |
| `IO.interruptible(body)` | Chạy trên blocking pool, thử `Thread.interrupt` khi hủy | Chỉ dùng khi API hỗ trợ interruption; không ép được driver phớt lờ interrupt |
| `IO.sleep(d)` | Treo fiber, đăng ký timer và nhường thread | Phù hợp timer/backoff; `Thread.sleep` vẫn chiếm thread |

Trong `Comparisons.starvation`, hai CountDownLatch đã được cấp trước đoạn trích sau. Latch `began` xác nhận lời gọi chặn đã bắt đầu; latch `gate` chỉ mở sau khi compute chạy.

```scala
blocked <- IO.blocking { began.countDown(); gate.await(); 7 }.start // Chờ trên blocking pool.
_ <- IO.blocking(began.await()) // Đồng bộ thí nghiệm bằng latch.
compute <- IO.delay(42).guarantee(IO.delay(gate.countDown())) // Compute vẫn chạy, mở gate.
answer <- blocked.joinWithNever // Đợi kết quả mà không Await một thread.
child <- IO.pure(42).start // Tạo thêm một fiber tính giá trị ngắn.
joined <- child.joinWithNever // Chỉ dùng vì con này không bị hủy trong thí nghiệm.
```

Thí nghiệm `Comparisons.interruptible` dùng `Thread.sleep` và xác nhận lời gọi thực sự nhận `InterruptedException` khi bị hủy. Kết quả này chỉ áp dụng cho API có hỗ trợ interruption; nó không chứng minh mọi driver Java đều dừng theo cách đó.

Một client bất đồng bộ tích hợp Cats Effect có thể tránh dành thread cho từng request. Nếu buộc dùng API chặn, bạn vẫn cần giới hạn số lời gọi vì blocking pool có thể tăng nhiều thread. Bạn không nên đưa tính toán CPU dài sang blocking pool để mong có thêm CPU, hoặc chờ runtime tự sửa starvation do chọn sai primitive.

### 3.5. Lỗi và cancellation là hai trạng thái khác nhau

Một đơn hàng thiếu giá, một lỗi mạng và một request bị client hủy cần những cách xử lý khác nhau. `IO.raiseError[A](e)` mô tả thất bại; exception ném trong `IO.delay` cũng đi vào kênh lỗi. `attempt` chuyển lỗi thành `Either[Throwable, A]`, còn `handleErrorWith` cho phép chọn một IO thay thế.

Đoạn này trích test cuối trong [EffectsSuite.scala](effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala), nằm bên trong `CatsEffectSuite`:

```scala
val failure = IO.raiseError[Int](IllegalArgumentException("unknown sku X")) // Chưa ném ra ngoài IO.
for
  error <- failure.attempt // Lỗi trở thành Left; for vẫn tiếp tục.
  fallback <- failure.handleErrorWith(_ => IO.pure(10)) // Chạy lại mô tả với cách phục hồi khác.
yield
  assertEquals(error.left.map(_.getMessage), Left("unknown sku X")) // Giữ thông tin lỗi.
  assertEquals(fallback, 10) // Giá trị minh họa, không phải policy bán hàng.
  intercept[ArithmeticException](IO.pure(1 / 0)) // pure tính đối số ngay.
```

Cancellation không phải một `Left` thông thường để `attempt` phục hồi. Khi muốn dọn dẹp riêng lúc hủy, bạn ghép `onCancel`; khi cần dọn cả lúc thành công, lỗi và hủy, bạn dùng `guarantee`. Mục 3.7 minh họa việc chờ finalizer kết thúc trước khi quan sát kết quả hủy.

`IO[A]` không có tham số kiểu dành riêng cho lỗi nghiệp vụ. Bạn có thể dùng `Either` cho validation thuần hoặc `IO[Either[OrderError, A]]` khi vừa có tương tác ngoài vừa muốn mô tả domain error trong type. Cách lồng kiểu này dài hơn, nhưng giúp người gọi thấy rõ các trường hợp nghiệp vụ. Trong Checkout, khuyến mãi timeout có thể dùng fallback; giá bắt buộc bị lỗi phải giữ nguyên lỗi. Bắt mọi Throwable rồi trả giá 0 sẽ làm một đơn hàng sai trông như hợp lệ.

### 3.6. Resource giữ đúng phạm vi async

`Using.resource(c)(_ => Future(query()))` có thể trả Future rồi đóng `c` trước khi query dùng nó. `Using` vẫn phù hợp cho code đồng bộ. Với công việc bất đồng bộ, `Resource.make(acquire)(release)` mô tả việc mở và đóng, còn `resource.use(c => work(c))` giữ tài nguyên cho đến khi **IO do work trả về** kết thúc.

Hàm `Comparisons.nestedResources` ghi lại thứ tự mở/đóng trong Ref. Dòng `result <- ...` được trích từ hàm `resources`; biến `log` đã được cấp trước đó.

```scala
def nestedResources(log: Ref[IO, Vector[String]]): Resource[IO, Unit] =
  def named(name: String): Resource[IO, Unit] =
    Resource.make(log.update(_ :+ s"open $name"))(_ => log.update(_ :+ s"close $name")) // acquire / release.
  named("connection") *> named("statement") // Mở connection trước, đóng statement trước.

result <- nestedResources(log).use(_ => IO.cede *> IO.raiseError[Unit](IllegalStateException("query failed"))).attempt
```

`use` ở đây kết thúc bằng lỗi sau một bước nhường lịch `IO.cede`. Resource vẫn đóng statement rồi connection trước khi `attempt` nhận lỗi. Lab có test riêng cho cancellation, nên kết luận đóng đúng scope không chỉ dựa vào đường chạy thành công.

Với `Resource.make` mặc định, acquire và release được che cancellation, còn use có thể bị hủy. Việc bảo vệ acquire không có nghĩa bạn nên để acquire chờ vô hạn. Finalizer cũng có thể chậm hoặc thất bại; Resource không chống được việc tiến trình bị kill và không đảo ngược giao dịch đã commit.

Bạn có thể dùng bracket khi chỉ quản lý một tài nguyên đơn giản. Resource thuận tiện hơn khi cần ghép nhiều connection, file hoặc client có phạm vi lồng nhau. API `allocated` trả cả tài nguyên lẫn finalizer cho người gọi, nên đòi hỏi bạn tự giữ và chạy finalizer đúng lúc. Một lỗi thường gặp là gọi `start` trong `use` rồi trả khỏi `use` ngay: tài nguyên đã đóng trong khi fiber con vẫn chạy. Pool giả trong lab yêu cầu mọi truy cập kết thúc trong scope; nó không phát hiện việc cố giữ tham chiếu sau scope.

### 3.7. Fiber và cancellation có hợp tác

Khi cần chạy một tác vụ con, bạn dùng `action.start` để nhận một `Fiber`. Fiber là tác vụ nhẹ do runtime điều phối. `fiber.join` treo fiber đang gọi để chờ con, thay vì giữ một thread bằng `Await`, và trả một `Outcome` mô tả cách con kết thúc.

Đoạn trích trong `Comparisons.cancellation` dùng hai Deferred làm cổng, một Ref đếm số lần ghi và một Ref đánh dấu dọn dẹp. Những giá trị này đã được cấp trước đoạn dưới.

```scala
fiber <- (started.complete(()).void *> gate.get *> writes.update(_ + 1))
  .onCancel(canceled.set(true)).start // Con báo đã bắt đầu rồi đợi gate; hủy sẽ đánh dấu cleanup.
_ <- started.get // Chỉ tiếp tục khi con đã bắt đầu.
_ <- fiber.cancel // Yêu cầu hủy và đợi onCancel kết thúc.
outcome <- fiber.join // Quan sát cách con kết thúc.
_ <- gate.complete(()) // Mở cổng sau hủy để chứng minh con không ghi nữa.
n <- writes.get
cleaned <- canceled.get
```

`Outcome.Succeeded(resultIO)` chứa IO để lấy kết quả thành công. `Outcome.Errored(error)` chứa lỗi, còn `Outcome.Canceled()` cho biết tác vụ đã bị hủy. Hàm đầy đủ pattern match `Outcome` để kiểm tra trạng thái cuối cùng. `joinWithNever` thuận tiện khi bạn đã loại trừ trường hợp hủy; nếu con bị hủy, hàm đó không trả kết quả. Vì vậy, bạn nên dùng `join` khi đang khảo sát cancellation.

`fiber.cancel` đợi finalizer, nhưng không thể cưỡng bức dừng mọi instruction trong một body đồng bộ. Sleep và việc chờ Deferred có thể bị hủy tại những điểm runtime hỗ trợ. Một `start` riêng lẻ cũng không bảo đảm con tự bị hủy khi cha kết thúc. Người gọi phải quản lý handle bằng join/cancel, hoặc dùng scope như `action.background.use(...)`.

Khi một đoạn chuyển trạng thái ngắn cần tránh hủy giữa chừng, bạn có thể dùng `IO.uncancelable { poll => ... }`. `poll` khôi phục khả năng hủy theo scope tương ứng; nó không phá một lớp che hủy bên ngoài. Test trong lab đặt phần chờ vào `poll` rồi xác nhận finalizer chạy khi hủy. Với acquire/use/release thông thường, bạn nên bắt đầu bằng Resource vì cấu trúc đó đã diễn đạt vòng đời cần thiết.

Nếu tác vụ có thể chạy tuần tự, bạn không cần thêm fiber handle. Các combinator song song cũng tự quản lý những fiber nội bộ. Fiber nhẹ nhưng vẫn tốn bộ nhớ và không tạo thêm CPU. Bạn nên tránh bỏ handle, để tác vụ fire-and-forget giữ connection, hoặc che cancellation cho toàn request khiến shutdown phải đợi mãi.

### 3.8. parMapN, parTraverse và timeout

Giá, tồn kho và khuyến mãi là ba nguồn độc lập, nên việc chờ tuần tự làm tăng thời gian báo giá không cần thiết. `parMapN` chạy các IO đồng thời rồi ghép kết quả. Đoạn sau trích thân `Checkout.quote` trong [Effects.scala](effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala); file import `cats.syntax.all.*` để có syntax ghép song song.

```scala
def borrowed[A](action: IO[A]): IO[A] = pool.connection.use(_ => action) // Giữ connection trong action.
requests.update(_ + 1) *> // Đếm request bắt đầu, kể cả request sau đó lỗi.
  (
    borrowed(services.price).timeout(budget), // Giá bắt buộc: timeout giữ lỗi.
    borrowed(services.stock).timeoutTo(budget, IO.pure(0)), // Chưa rõ kho thì không bán.
    borrowed(services.promotion).timeoutTo(budget, IO.pure(0L)) // Chưa xác nhận thì không giảm.
  ).parMapN(Quote.apply) // Ghép ba giá trị thành một Quote khi cả ba thành công.
```

Với danh sách, `items.parTraverse(f)` chạy các action đồng thời và giữ thứ tự **kết quả theo input**. Nó không hứa thứ tự side effect. Nếu một nhánh lỗi, combinator yêu cầu hủy những nhánh còn lại và đợi chúng dọn dẹp. Test dùng handshake để chắc chắn cả ba service đã bắt đầu trước khi cho giá thất bại; nhờ đó test thật sự kiểm tra việc hủy sibling.

`action.timeout(d)` yêu cầu hủy khi hết hạn và thông thường trả TimeoutException. `timeoutTo(d, fallback)` chuyển sang IO thay thế. Cả hai phải phối hợp với cancellation và cleanup, nên không cam kết trả đúng tại thời điểm `d`. Một body blocking không ngắt được, một vùng uncancelable hoặc kết quả vừa hoàn tất trong lúc hủy có thể ảnh hưởng thời điểm và kết quả quan sát. Timeout cũng không thu hồi được email đã gửi hay transaction đã commit.

Bạn chọn tuần tự khi bước sau cần dữ liệu trước hoặc khi muốn giảm tải. Bạn chọn concurrency khi các nguồn độc lập và hệ thống chịu được tải đó. Client timeout vẫn cần thiết để điều khiển socket hoặc driver ở tầng dưới. Semaphore có thể giới hạn số action đang làm việc, nhưng `parTraverse` vẫn tạo nhiều fiber chờ; dữ liệu lớn cần streaming hoặc worker queue. Trong Checkout, timeout bao ngoài `borrowed`, nên budget tính cả lúc đợi permit. Bạn không nên coi timeout là bằng chứng thao tác ghi chưa xảy ra rồi retry vô điều kiện.

### 3.9. Ref, Deferred, Semaphore

Một counter dùng chung, một tín hiệu bắt đầu và một pool có số kết nối hữu hạn là ba vấn đề khác nhau. Cats Effect cung cấp primitive riêng cho từng nhu cầu để chương trình không phải tự trộn khóa, biến mutable và cơ chế chờ.

| Primitive | Hoạt động / khi dùng | Thay thế, đánh đổi, lỗi |
|---|---|---|
| `Ref[IO, A]` | `update`/`modify` cập nhật nguyên tử một state immutable | Có vai trò tương tự AtomicReference trong IO; chỉ bảo vệ state trong tiến trình |
| `Deferred[IO, A]` | `get` treo fiber; lần `complete` đầu tiên thắng và trả true | Là tín hiệu một lần như Promise; không reset được, nhiều reader cùng thấy kết quả |
| `Semaphore[IO]` | `permit.use` chờ lượt và trả lượt khi kết thúc | Giới hạn công việc đồng thời; không tự tạo hay quản lý socket |

Test Ref trong `EffectsSuite` ghép 1.000 cập nhật đồng thời. Hàm `_ + 1` thuần, nên Ref có thể thử lại cập nhật bằng CAS (compare-and-set) mà không lặp side effect ngoài ý muốn.

```scala
for
  count <- Ref.of[IO, Int](0) // Cấp một Ref dùng chung cho toàn bộ cập nhật.
  _ <- List.fill(1000)(count.update(_ + 1)).parSequence_ // Mỗi update là nguyên tử.
  n <- count.get // Đọc sau khi toàn bộ action kết thúc.
yield assertEquals(n, 1000) // Nằm trong CatsEffectSuite của lab.
```

Nếu bạn tách `get` rồi `set`, hai fiber vẫn có thể đọc cùng một giá trị cũ và ghi đè cập nhật của nhau. Bạn cũng không được sửa collection mutable ở bên ngoài Ref rồi mong snapshot được bảo vệ. Khi nhiều trường cần nhất quán, hãy gom chúng vào một case class trong **một** Ref; nhiều Ref riêng không tự tạo thành transaction.

Test Deferred minh họa một tín hiệu có nhiều người đọc. `joinWithNever` ở đây hợp lệ vì các reader không bị hủy và chắc chắn nhận giá trị sau `complete`.

```scala
signal <- Deferred[IO, Int] // Ban đầu chưa có giá trị.
readers <- List.fill(3)(signal.get).parSequence.start // Ba reader cùng chờ, không giữ thread.
first <- signal.complete(42) // Lần đầu trả true và đánh thức reader.
second <- signal.complete(99) // Lần sau trả false, không thay 42 bằng 99.
result <- readers.joinWithNever // Kết quả của cả ba reader đều là 42.
```

Với Semaphore, cách ghép trong lời giải [Exercises.boundedTraverse](../solutions/effects-code/src/main/scala/vn/scalaeng/effectsolutions/Exercises.scala) là:

```scala
def boundedTraverse[A, B](items: List[A], limit: Int)(action: A => IO[B]): IO[List[B]] =
  IO.raiseWhen(limit <= 0)(IllegalArgumentException("limit must be positive")) *> // Kiểm tra cấu hình.
    Semaphore[IO](limit.toLong).flatMap { semaphore => // Một semaphore cho cả danh sách.
      items.parTraverse(a => semaphore.permit.use(_ => action(a))) // Trả permit cả khi lỗi/hủy.
    }
```

FakePool kết hợp Resource để giữ scope, Semaphore để giới hạn lượt mượn, và Ref để lưu ID/log. Connection đồng thời không có thứ tự đóng toàn cục; điều cần kiểm tra là mỗi connection đều được đóng và pool đóng cuối. Nếu cần chuyển dữ liệu từ producer sang consumer, Queue phù hợp hơn Semaphore. Bạn cần tránh giữ một permit rồi chờ fiber con cũng cần permit ấy, vì các fiber vẫn có thể deadlock dù không chặn thread.

### 3.10. Retry với backoff vẫn cần policy ứng dụng

Lỗi mạng tạm thời có thể đáng thử lại, nhưng SKU sai sẽ không tự đúng sau một lần chờ. Retry thao tác ghi còn có thể tạo trùng đơn hàng. Cats Effect cung cấp mô tả có thể chạy lại, cơ chế bắt lỗi và sleep có thể hủy; **ứng dụng vẫn phải quyết định policy retry**. Bảng “retry có sẵn” ở Ch16A không có nghĩa IO tự biết lỗi nghiệp vụ nào đáng thử lại.

Đây là helper `Retry.apply` trong Effects.scala. Nó nhận danh sách thời gian chờ và hàm phân loại lỗi; `Retry.backoff(base, cap, retries)` tạo lịch nhân đôi có trần cho danh sách đó.

```scala
def apply[A](action: IO[A], delays: List[FiniteDuration])(
    retryable: Throwable => Boolean
): IO[A] =
  IO.defer(action).handleErrorWith { error => // Chạy mô tả; chỉ xét retry khi lỗi.
    delays match
      case delay :: rest if retryable(error) => // Còn lượt và đúng loại lỗi mới thử lại.
        IO.sleep(delay) *> apply(action, rest)(retryable) // Sleep có thể bị hủy.
      case _ => IO.raiseError(error) // Hết lượt hoặc không retry: giữ lỗi hiện tại.
  }
```

Nếu `delays` có hai phần tử, helper chạy tối đa ba lần: lần đầu và hai lần thử lại. `IO.defer` hoãn bước dựng vòng chạy. Tuy nhiên, nếu action chỉ đợi một Future đã tạo trước đó, chạy lại IO vẫn chỉ đợi lại Future ấy. Bạn phải đưa factory tạo công việc mới vào IO khi cần mỗi lần retry là một lần thực thi mới.

Helper nhỏ dễ đọc, nhưng bạn phải tự test số lượt, lỗi cuối, lịch chờ và cancellation. Một thư viện policy có thể bổ sung jitter và metrics khi yêu cầu phức tạp hơn. Bạn nên retry việc đọc hoặc việc ghi có idempotency key, đồng thời phân loại lỗi tạm thời rõ ràng. Lab chưa thêm jitter hay deadline tổng; khi đưa vào production, team cần chọn nguồn random, ngân sách retry và cách quan sát tải. Retry validation hoặc retry không giới hạn có thể che lỗi cấu hình và gây quá tải.

### 3.11. CatsEffectSuite và TestControl

Nếu test ngủ thật ở mỗi bước backoff, bộ test vừa chậm vừa phụ thuộc scheduler. `munit.CatsEffectSuite` nhận test trả IO và chạy IO ấy cho bạn. Assertion nằm trong IO phải được ghép bằng `for` hoặc `*>`; việc tạo rồi bỏ một mô tả assertion không làm assertion chạy.

Test sau trích từ `EffectsSuite`; các import ở đầu file là `cats.effect.{Deferred, IO, Ref}`, `cats.effect.testkit.TestControl`, `munit.CatsEffectSuite`, `cats.syntax.all.*` và `scala.concurrent.duration.*`. Đoạn dưới giữ nguyên test giá timeout, chỉ thêm chú thích và lớp bao để thấy cấu trúc suite.

```scala
class EffectsSuite extends CatsEffectSuite:
  test("timeout giá trả lỗi thay vì giá giả") {
    TestControl.executeEmbed(for // Dùng runtime có clock ảo cho chương trình bên trong.
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      count <- Ref.of[IO, Long](0L)
      result <- FakePool.resource(3, log).use(pool => Checkout.quote(
        Services(IO.never[Long], IO.pure(3), IO.pure(0L)), pool, count, 1.second)).attempt
    yield assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException])))
  }
```

`TestControl.executeEmbed(program)` chạy chương trình bằng clock và scheduler của test runtime. Test timeout không phải chờ một giây thực. Test retry riêng đo `IO.monotonic` và xác nhận tổng chờ 100 + 200 = 300 ms **ảo**. Artifact testkit_3 3.7.1 và method executeEmbed đã được kiểm tra theo source đúng tag, rồi compile và chạy thật ngày **16/10/2026**.

Bạn nên dùng thời gian ảo cho sleep, backoff và timeout. Với interruption và lời gọi blocking, bạn cần runtime thật như các thí nghiệm đối chiếu của lab. TestControl không giả lập network, JDBC, `Thread.sleep` hay mọi cách các thread CPU xen kẽ nhau. Vì vậy, một test thời gian ảo pass không chứng minh chương trình không còn race; bạn cũng không nên thay các phép đồng bộ rõ ràng bằng assertion đòi chạy nhanh hơn một ngưỡng rất nhỏ.

### 3.12. So sánh khái niệm với ZIO 2

Ngày **16/10/2026**, metadata/POM xác nhận **ZIO 2.1.27 có zio_3**; chỉ kiểm chứng artifact, **chưa chạy ZIO với Scala 3.9.0**. Không thêm ZIO vào lab.

| Khía cạnh | Cats Effect | ZIO 2 |
|---|---|---|
| Kiểu chính | IO[A], lỗi Throwable | ZIO[R,E,A]: môi trường, lỗi, kết quả |
| Thực thi | IOApp, Fiber, cancellation | ZIOAppDefault, fiber, interruption |
| Tài nguyên | Resource | Scope, acquire/release |
| Retry | Ghép IO / thư viện policy | Schedule |

Cats Effect kết nối với stack Typelevel như http4s và Doobie. ZIO thể hiện môi trường và lỗi trong kiểu, đồng thời cung cấp một hệ abstraction riêng. Bạn nên chọn theo thư viện cần tích hợp và kinh nghiệm của team; bạn không cần học cả hai cùng lúc. Việc trộn runtime chỉ để đổi tên primitive làm tăng trách nhiệm quản lý vòng đời mà chưa giải quyết thêm nhu cầu nghiệp vụ. Bảng này so sánh khái niệm, không đo hiệu năng hay khẳng định hai API tương đương hoàn toàn; nguồn phiên bản và kiểu nằm ở mục 16.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Effect / effect system | Hiệu ứng / cách mô tả và ghép tương tác ngoài hàm thuần |
| Lazy / eager | Trì hoãn thân / khởi động khi tạo |
| Referential transparency | Thay tên bằng biểu thức mô tả không chạy side effect sớm |
| Runtime / fiber | Bộ thực thi / tác vụ nhẹ do runtime điều phối |
| Cooperative cancellation | Hủy được quan sát tại điểm runtime/API hỗ trợ |
| Finalizer / masking / poll | Dọn dẹp / che hủy / khôi phục khả năng hủy theo scope |
| Acquire / release / scope | Mở / đóng / phạm vi sống tài nguyên |
| Fallback / backoff / jitter | Phương án thay thế / lịch chờ tăng / lệch ngẫu nhiên |
| Virtual time | Clock do test runtime điều khiển |
| `IO[A]` | Mô tả tác vụ trả A, có thể lỗi hoặc bị hủy; chưa chạy khi chỉ tạo giá trị |
| `IOApp` | Điểm vào ứng dụng, cấp và quản lý runtime để chạy IO |
| `Resource` | Mô tả acquire/release và giữ tài nguyên trong scope use |
| `Fiber` | Tác vụ nhẹ có handle để chờ hoặc hủy, do runtime điều phối |
| `Outcome` | Trạng thái kết thúc: Succeeded, Errored hoặc Canceled |
| `Ref` | Ô trạng thái dùng cập nhật nguyên tử; hàm cập nhật phải thuần |
| `Deferred` | Tín hiệu có giá trị một lần, cho phép nhiều fiber chờ |
| `Semaphore` | Bộ đếm permit để giới hạn số tác vụ trong một scope đồng thời |
| `parMapN` / `parTraverse` | Syntax Cats để chạy các IO độc lập đồng thời rồi ghép kết quả |
| `TestControl` | Test runtime điều khiển clock/scheduler cho IO, dùng kiểm tra thời gian ảo |

## 5. Ví dụ đơn giản

Đây là `SimpleExample` trong [EffectsDemo.scala](effects-lab/src/main/scala/vn/scalaeng/effects/EffectsDemo.scala), đã chạy với version ở đầu bài:

```scala
package vn.scalaeng.effects
import cats.effect.{IO, IOApp, Ref}

object SimpleExample extends IOApp.Simple:
  val run: IO[Unit] = for
    counter <- Ref.of[IO, Int](0)
    action = counter.updateAndGet(_ + 1)
    first <- action
    second <- action
    _ <- IO.println(s"first=$first second=$second")
  yield ()
```

Mỗi lần action chạy cập nhật Ref một lần. Hai `<-` tuần tự làm second thấy cập nhật của first. Lệnh chạy ở mục 7, output thật ở mục 9.

## 6. Ví dụ thực tế (BE và DE)

Checkout báo giá gọi đồng thời ba service. Giá bắt buộc; stock timeout -> 0 để không bán khi không biết kho; promotion timeout -> 0L để không giảm khi chưa xác nhận. Mỗi nhánh mượn connection giả trong Resource. Demo có pool 3 permit, giá 120.000 đồng, stock 3; promotion không trả lời và bị hủy sau budget 100 ms. Mọi connection đóng trước pool.

Counter đếm **request bắt đầu**, kể cả lỗi, không phải đơn hàng thành công. Test giá lỗi xác nhận hai sibling bị hủy, không xuất Quote giả. Đây là policy minh họa: sản phẩm thật có thể cần trạng thái “chưa xác nhận khuyến mãi” để khách quyết định. Pool thật dùng driver, không chia sẻ một JDBC connection cho ba nhánh chỉ vì cùng pool.

Với DE, batch đọc nhiều đối tác có cùng cấu trúc: timeout từng nguồn, Semaphore giới hạn tải, kết quả giữ thứ tự input. Exercises.batch là xử lý trong bộ nhớ, không phải Spark, không phân tán/exactly-once; retry ghi vẫn cần idempotency key/giao dịch ở đích.

## 7. Code đầy đủ

- [project.scala](effects-lab/project.scala): pin Scala/JDK/dependency, bật `-deprecation -feature -Wunused:all -Werror`.
- [build.sbt](effects-lab/build.sbt), [build.properties](effects-lab/project/build.properties): build sbt 1.12.11 tương đương; **Claude đã chạy 22/22 test pass** ngoài sandbox ngày 16/10/2026, theo [review T01](../.team/reviews/T01-ch16b-cats-effect-review.md).
- [Effects.scala](effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala): Retry, pool, model, Checkout.
- [Comparisons.scala](effects-lab/src/main/scala/vn/scalaeng/effects/Comparisons.scala): đối chiếu Future/IO.
- [EffectsDemo.scala](effects-lab/src/main/scala/vn/scalaeng/effects/EffectsDemo.scala): ba entry point.
- [EffectsSuite.scala](effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala): 22 test thật.
- [verification/](effects-lab/verification/): log, scripts và bằng chứng version.

Từ repo root, vào lab rồi chạy test bằng Scala CLI hoặc sbt. Trên Windows, dùng tên executable `.bat` như bên dưới:

```powershell
cd 07-concurrency/effects-lab
scala-cli.bat test . --server=false
sbt.bat test
scala-cli.bat run . --server=false --main-class vn.scalaeng.effects.SimpleExample
scala-cli.bat run . --server=false --main-class vn.scalaeng.effects.EffectsDemo
scala-cli.bat run . --server=false --main-class vn.scalaeng.effects.ErrorDemo
```

Để chạy lời giải, mở terminal tại repo root:

```powershell
scala-cli.bat test solutions/effects-code --server=false
```

> Nếu môi trường không ghi được cache Coursier mặc định, bạn có thể đặt `$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'` **khi đang ở effects-lab** trước lệnh test/run. Đây là cách xử lý quyền ghi của môi trường; người học có cache mặc định hoạt động không cần đặt biến này. `--server=false` chạy không qua Bloop và được dùng trong kiểm chứng của task.

Scala CLI 1.9.1 có thể hiện **hint** munit/munit-cats-effect có bản mới; đây không phải cảnh báo compiler. Giữ version theo quy ước/POM, không tắt cảnh báo để test pass.

## 8. Giải thích code

`Checkout.quote` bắt đầu bằng `requests.update(_ + 1)`, nên counter ghi nhận request đã bắt đầu kể cả khi giá lỗi sau đó. Hàm cục bộ `borrowed` giữ connection bằng `pool.connection.use(_ => action)`. Ba biểu thức dịch vụ được đặt trong một tuple rồi ghép bằng `parMapN`, vì chúng không cần kết quả của nhau để khởi động.

Mỗi nhánh có timeout riêng đặt **bên ngoài** `borrowed`. Vì vậy, budget tính cả thời gian chờ permit, thay vì chỉ tính từ lúc đã có connection. Khi pool nhỏ, một nhánh có thể timeout trước khi action dịch vụ bắt đầu. Nếu giá timeout, Checkout trả lỗi; nếu kho hoặc khuyến mãi timeout, nó dùng giá trị fallback đã chọn theo nghiệp vụ. Timeout vẫn phải chờ cleanup hoàn tất.

`FakePool.connection` ghép Resource của permit với Resource của connection. Khi query lỗi hoặc bị hủy, Resource đóng connection trước rồi trả permit. `Ref` lưu ID và log bằng các giá trị immutable. Các connection độc lập có thể đóng theo thứ tự khác nhau, nên test kiểm tra đủ số mở/đóng, thứ tự tài nguyên lồng nhau và sự kiện pool đóng cuối, thay vì áp đặt một thứ tự toàn cục không có trong hợp đồng API.

`Comparisons.timeout` cho Future chờ một Promise. Sau khi người chờ nhận timeout, harness mới mở cổng và đợi Future gốc kết thúc để xác nhận nó vẫn ghi. Phía IO dùng Deferred với giao thức tương tự, nhưng timeout đã hủy và đợi cleanup trước khi mở cổng nên không còn thao tác ghi. Kết luận dựa vào những điểm đồng bộ này, không dựa vào việc ngủ thêm rồi đoán scheduler đã làm gì.

Harness đặt executor trong Resource và shutdown khi kết thúc. `Await` chỉ có mặt trong phần tái hiện giới hạn Future; Checkout không dùng nó. Các test retry kiểm tra giới hạn số lượt, trần backoff và hủy trong thời gian chờ. Test ba dịch vụ dùng handshake để xác nhận chúng đã bắt đầu đồng thời. TestControl kiểm tra lịch thời gian ảo, còn runtime thật kiểm tra interruption và blocking pool.

## 9. Output mong đợi

Output chạy thật lần đầu **16/10/2026**, chạy lại trong vòng sửa **17/10/2026**, Windows/JDK 21/Scala 3.9.0; log lần chạy lại nằm trong verification/. Chỉ bỏ dòng trống do lớp xuất console; đây không phải benchmark.

`SimpleExample`:

```text
first=1 second=2
```

`EffectsDemo`:

```text
eager: Future before=1 after=1; IO before=0 after=2
cancel: canceled=true writes=0 finalizer=true
timeout: Future error=true before=0 after=1; IO error=true writes=0 finalizer=true
retry: Future cached=1 fresh=3; IO=3
resource: Future closedBeforeRead=true
open connection -> open statement -> close statement -> close connection
java.lang.IllegalStateException: query failed
starvation: Future timedOut=true; blocking+compute=49; fiber join=42
interruptible: interrupted=true
quote: price=120000 stock=3 discount=0 payable=120000 requests=1
pool: opened=3 closed=3 last=close pool
```

Future vẫn ghi sau timeout; IO hủy và dọn xong, không ghi. `blocking+compute=49` là tổng kết quả 42+7, không phải 49 ms. cached=1 nghĩa là đợi lại Future lỗi không chạy lại thân.

Test: lab **22 pass**, lời giải **12 pass**, cả hai **0 fail, 0 ignored, 0 cảnh báo compiler**, bật `-Werror`. Thời lượng thay đổi; test không đòi tốc độ dưới vài mili giây.

## 10. Lỗi thường gặp (compiler và runtime thật)

### 10.1. Lỗi runtime

Ba dòng đầu do ErrorDemo; dòng cuối do query lỗi trong EffectsDemo. Dùng attempt in exception.toString nên demo thoát 0, không phải service crash.

| Thông báo thật | Nguyên nhân / cách xử lý |
|---|---|
| `java.lang.IllegalArgumentException: unknown sku X` | raiseError báo SKU sai; giữ lỗi, không retry/bán giá 0 |
| `java.util.concurrent.TimeoutException: 20 milliseconds` | never.timeout; kiểm tra budget/khả năng hủy, chỉ fallback theo nghiệp vụ |
| `java.lang.IllegalArgumentException: pool size must be positive` | Pool size=0; validate cấu hình |
| `java.lang.IllegalStateException: query failed` | Use lỗi; kiểm tra release ngược và giữ nguyên lỗi |

Một số lỗi thiết kế không tự phát sinh exception: pure(sideEffect()), bỏ assertion IO, get rồi set Ref, bỏ fiber handle. Using/Future giả chỉ báo closedBeforeRead=true, không có JDBC thật để trích lỗi driver. Không gán thông báo tưởng tượng cho các trường hợp này.

### 10.2. Thiếu syntax cho parTraverse

Các ví dụ lỗi dưới đây được tạo trong `effects-lab/.cache/compiler-examples/`, không đặt vào `src/` của lab. Script [compile_errors.py](effects-lab/verification/compile_errors.py) sao chép `project.scala` để giữ đúng Scala 3.9.0, JDK 21, Cats Effect 3.7.1 và cờ `-Werror`. Script biên dịch bản đúng trước, rồi sửa bản sao để gây lỗi. Các khối chẩn đoán trích nguyên dòng từ compiler, chỉ bỏ ANSI và dòng trống; log liên kết giữ đầy đủ đường dẫn, vị trí lỗi, lệnh và exit code.

Khi quên `import cats.syntax.all.*`, syntax cho danh sách không được đưa vào scope:

```scala
package vn.scalaeng.effects
import cats.effect.IO
object MissingSyntax:
  val quotes: IO[List[Long]] = List(120000L, 20000L).parTraverse(n => IO.pure(n))
```

Lệnh chạy trong thư mục bản sao là `scala-cli.bat compile project.scala Example.scala --server=false`, exit **1**. [Log thiếu syntax](effects-lab/verification/compiler-missing-syntax.log) có mã E008 và chẩn đoán:

```text
  |value parTraverse is not a member of List[Long], but could be made available as an extension method.
  |
  |One of the following imports might fix the problem:
  |
  |  import cats.implicits.catsSyntaxParallelTraverse1
  |  import cats.syntax.all.catsSyntaxParallelTraverse1
  |  import cats.syntax.parallel.catsSyntaxParallelTraverse1
  |
1 error found
```

Bạn thêm `import cats.syntax.all.*` ở đầu file để có syntax Cats dùng trong lab, hoặc chọn import hẹp `cats.syntax.parallel.*` nếu chỉ cần nhóm chức năng song song. Bản đúng có import `cats.syntax.all.*` đã compile exit 0 trong [log baseline](effects-lab/verification/compiler-missing-syntax-baseline.log). Lỗi này xảy ra ở bước tìm extension method, trước khi có thể quan sát hành vi song song lúc chạy.

### 10.3. unsafeRunSync thiếu IORuntime

Đoạn sau gọi một phương thức unsafe mà chưa có runtime trong implicit scope:

```scala
package vn.scalaeng.effects
import cats.effect.IO
object MissingRuntime:
  def run(): Unit = IO.println("order accepted").unsafeRunSync()
```

Cùng lệnh compile phía trên trả exit **1**. [Log thiếu runtime](effects-lab/verification/compiler-missing-runtime.log) ghi mã E172 và phần giải thích sau:

```text
  |Could not find an implicit IORuntime.
  |
  |Instead of calling unsafe methods directly, consider using cats.effect.IOApp, which
  |runs your IO. If integrating with non-functional code or experimenting in a REPL / Worksheet,
  |add the following import:
  |
  |import cats.effect.unsafe.implicits.global
```

Ở executable của chương, bạn nên để `IOApp.Simple` chạy `run` như mục 3.3. Khi thử nghiệm trong REPL hoặc thật sự cần tích hợp code không dùng IO, import `cats.effect.unsafe.implicits.global` cung cấp runtime mặc định; [bản baseline](effects-lab/verification/compiler-missing-runtime-baseline.log) có import này đã compile exit 0. Ví dụ này chỉ được biên dịch, không gọi `run()`; nó không khuyến khích rải unsafeRunSync trong service hoặc test.

### 10.4. Bỏ rơi IO và chọn đúng cờ cảnh báo

Đây là lỗi dễ bỏ sót vì `action` chỉ trả một mô tả, không tự thực hiện việc in:

```scala
package vn.scalaeng.effects
import cats.effect.IO
object DiscardedIO:
  def action: IO[Unit] = IO.println("order accepted")
  def build(): IO[Unit] =
    action
    IO.unit
```

`build()` trả về `IO.unit`; IO do dòng `action` tạo ra bị bỏ đi. Với **chính source này**, `-Wvalue-discard` không cảnh báo và compile exit **0**, như [log đối chứng](effects-lab/verification/compiler-discarded-io-value-discard.log). Cờ đó không bắt được mọi biểu thức non-Unit nằm giữa một block.

Khi thêm `--scalac-option -Wnonunit-statement` vào lệnh compile, Scala 3.9.0 phát hiện biểu thức non-Unit không được dùng. Vì project.scala vẫn bật `-Werror`, cảnh báo E176 làm compile exit **1**. [Log nonunit-statement](effects-lab/verification/compiler-discarded-io-nonunit-statement.log) ghi:

```text
6 |    action
  |    ^^^^^^
  |unused value of type (vn.scalaeng.effects.DiscardedIO.action : => cats.effect.IO[Unit])
No warnings can be incurred under -Werror
1 warning found
1 error found
```

Bạn sửa phần thân thành `action *> IO.unit` hoặc ghép `_ <- action` trong for-comprehension. Đổi thành `action.void` rồi tiếp tục bỏ kết quả IO vẫn không làm action chạy. [Bản đúng](effects-lab/verification/compiler-discarded-io-baseline.log) ghép `action *> IO.unit` đã compile exit **0**. Cờ `-Wnonunit-statement` chỉ được thêm vào thí nghiệm này; lab giữ bộ cờ chuẩn ghi ở mục 7.

Từ repo root, bạn tái lập toàn bộ ba cặp bản đúng/bản lỗi và phép so sánh hai cờ bằng:

```powershell
python 07-concurrency/effects-lab/verification/compile_errors.py
```

## 11. Cách debug

1. IO chưa chạy hay đang chờ? Kiểm tra đã ghép vào run/test chưa, có bỏ một mô tả assertion không.
2. Vẽ ai giữ permit và chờ Deferred/Fiber; join không chặn thread nhưng vẫn deadlock nếu con chờ cha.
3. Log request ID ở acquire/use/release; timeout chậm có thể do finalizer hoặc body không interrupt được.
4. Với race: một Ref chứa state immutable, update thuần; dùng Deferred tạo interleaving thay sleep tùy ý.
5. Với retry: ghi attempt, loại lỗi, tổng budget; phân biệt chạy factory mới với đợi Future cũ.

`python 07-concurrency/effects-lab/verification/run_checks.py` lưu output/exit code thật, đặt cache trong lab. `verify_versions.py` cần mạng và chạy riêng, không gọi Maven trong unit test. Khi báo lỗi mới, giữ lệnh, version và output.

## 12. Bài tập (cơ bản → nâng cao)

1. **Replay:** repeat(action,times) trả List kết quả. Hai lần tăng Ref cho hai giá trị kế tiếp; zero không chạy, số âm lỗi. Giải thích cùng IO trong List.fill vẫn chạy nhiều lần.
2. **Fallback:** promotion(action) chỉ đổi TimeoutException thành 0L; giữ lỗi khác. Test thành công, timeout, validation.
3. **Resource:** session(log) mở connection rồi statement. Test đóng ngược lúc use lỗi/hủy; Deferred xác nhận use đã bắt đầu trước khi cancel.
4. **Giới hạn:** boundedTraverse(items,limit)(action) bằng Semaphore, giữ thứ tự input. Test peak≤limit, empty, limit≤0, nhánh lỗi hủy sibling; giải thích giới hạn bộ nhớ khi nhiều fiber chờ.
5. **Retry đọc:** base 100 ms, nhân đôi, trần 1 giây; retries+1 lần chạy, chỉ IOException. TestControl đo thành công lần ba sau 300 ms ảo, validation một lần, giữ lỗi cuối khi hết lượt.
6. **Batch:** dùng Checkout/pool, giới hạn request đồng thời, một Ref counter, giữ thứ tự kết quả. Test bốn request promotion timeout, pool đóng cuối và giá lỗi làm batch fail.

Lời giải đủ sáu bài có test ở mục 13. Mở rộng tự chọn: jitter có nguồn random truyền vào, deadline tổng quanh retry; không retry ghi trước khi xác định idempotency key.

## 13. Đáp án và lời giải

Đọc [ch16b-solutions.md](../solutions/ch16b-solutions.md) sau khi tự làm. [Exercises.scala](../solutions/effects-code/src/main/scala/vn/scalaeng/effectsolutions/Exercises.scala) chứa sáu lời giải; [ExercisesSuite.scala](../solutions/effects-code/src/test/scala/vn/scalaeng/effectsolutions/ExercisesSuite.scala) có 12 test. Build tái dùng Effects.scala qua `using file`, không kéo test lab sang; lệnh chạy ở mục 7.

## 14. Câu hỏi kiểm tra

1. IO.delay(send()) dùng hai lần khác Future lưu trong val thế nào? IO.pure(send()) khác ở đâu?
2. For-comprehension có tạo concurrency không? Khi nào cần tuần tự?
3. Ba Outcome của join là gì? Vì sao không dùng joinWithNever để khảo sát hủy?
4. Timeout 100 ms có thể trả sau 100 ms vì lý do nào? attempt có bắt cancellation không?
5. Ref.get rồi set vì sao race? Vì sao modify phải nhận hàm thuần?
6. Timeout ngoài pool.use tính cả gì? Thu nhỏ pool ảnh hưởng fallback thế nào?
7. IO có tự có retry policy/jitter không? TestControl chứng minh điều gì và không thay test nào?
8. Artifact ZIO `_3` chứng minh gì, chưa chứng minh gì về Scala 3.9.0?

Đáp án ở file lời giải.

## 15. Mini project

Xây “Order quote batch”: List Services theo SKU, tối đa 2 request đồng thời, pool 3 permit, timeout từng service, một counter; trả List Quote đúng thứ tự input, tổng request và log tài nguyên.

Nghiệm thu: giá bắt buộc; stock timeout không bán; promotion timeout không giảm; connection đóng trước pool; giá lỗi làm batch fail/hủy sibling; input rỗng kết thúc sạch. Giải thích permit waiting nằm trong budget. Dùng TestControl cho lịch chờ và runtime thật cho interruption.

Exercises.batch/test bài 6 là bản tối thiểu; chưa kiểm chứng throughput, giao dịch hoặc chịu kill tiến trình.

## 16. Nguồn tài liệu chính thức

Đọc [cổng tài liệu Cats Effect 3](https://typelevel.org/cats-effect/docs/) và các trang dành cho người học: [IO](https://typelevel.org/cats-effect/docs/datatypes/io), [Resource](https://typelevel.org/cats-effect/docs/std/resource), [Concepts: concurrency và cancellation](https://typelevel.org/cats-effect/docs/concepts), [Testing với TestControl](https://typelevel.org/cats-effect/docs/core/test-runtime). Các trang IO, Resource, Concepts và Test Runtime đã được mở kiểm tra ngày **16/10/2026**. Phiên bản dependency của lab vẫn lấy theo POM bên dưới; ví dụ cấu hình cũ trên một trang hướng dẫn có thể chưa dùng cùng phiên bản.

Kiểm chứng **16/10/2026** qua Maven metadata/POM; response và SHA-256 ở [versions.json](effects-lab/verification/versions.json).

- [Metadata cats-effect_3](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect_3/maven-metadata.xml): stable 3.7.x mới nhất **3.7.1**, đã compile/run với Scala 3.9.0.
- [POM munit-cats-effect_3 2.2.0](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom): munit **1.2.4**, CE 3.7.0; lab chạy patch 3.7.1. POM 2.2.1 dùng munit 1.3.6 nên không chọn. [CatsEffectSuite](https://typelevel.org/munit-cats-effect/) hướng dẫn test trả IO.
- [TestControl source 3.7.1](https://github.com/typelevel/cats-effect/blob/v3.7.1/testkit/shared/src/main/scala/cats/effect/testkit/TestControl.scala): executeEmbed; đã resolve artifact testkit_3 và chạy test thật.
- [Resource](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/std/resource.md), [MonadCancel](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/typeclasses/monadcancel.md), [thread model](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/thread-model.md): release/masking/blocking. Thread model có lịch sử CE2; đọc phần CE3 cho API chương này.
- [Metadata zio_3](https://repo.maven.apache.org/maven2/dev/zio/zio_3/maven-metadata.xml), [POM 2.1.27](https://repo.maven.apache.org/maven2/dev/zio/zio_3/2.1.27/zio_3-2.1.27.pom), [ZIO source](https://github.com/zio/zio/blob/v2.1.27/core/shared/src/main/scala/zio/ZIO.scala): artifact Scala 3 và kiểu ZIO[R,E,A], chưa chạy ZIO trong task.

## 17. Checklist tự đánh giá

- [ ] Phân biệt Scala, standard library, Cats và Cats Effect.
- [ ] Dự đoán lúc chạy với delay/pure/Future và khi replay IO.
- [ ] Chọn đúng blocking primitive; không Await trong Checkout.
- [ ] Quản lý fiber con, hiểu Outcome và cancellation có hợp tác.
- [ ] Giải thích timeout chờ cleanup; Resource đóng lúc lỗi/hủy.
- [ ] Update Ref nguyên tử, Deferred handshake, trả permit đúng scope.
- [ ] Chọn fallback/retry theo nghiệp vụ, cân nhắc idempotency.
- [ ] Chạy 22 test lab, 12 test lời giải và demo trên máy mình.
- [ ] Phân biệt test dịch vụ giả với bằng chứng production.
