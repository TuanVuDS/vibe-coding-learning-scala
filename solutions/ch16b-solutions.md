> Scala 3.9.0 · JDK 21 · Cats Effect/testkit 3.7.1 · munit 1.2.4 · munit-cats-effect 2.2.0. Code và test chạy thật ngày 16/10/2026, chạy lại 12/12 test Scala CLI ngày 17/10/2026. Claude đã review chéo T01; vòng sửa này chờ lead kiểm tra các sửa đổi.

# Lời giải Ch16B — Cats Effect

Đề bài ở [chương 16B](../07-concurrency/ch16b-cats-effect.md). Code hoàn chỉnh: [Exercises.scala](effects-code/src/main/scala/vn/scalaeng/effectsolutions/Exercises.scala); test: [ExercisesSuite.scala](effects-code/src/test/scala/vn/scalaeng/effectsolutions/ExercisesSuite.scala). Không chỉ có pseudo-code: sáu bài dùng chung một build Scala CLI, **12 test munit thật**.

Chạy từ repo root:

```powershell
scala-cli.bat test solutions/effects-code --server=false
```

> Nếu môi trường không ghi được cache mặc định, từ repo root bạn có thể đặt `$env:COURSIER_CACHE = Join-Path $PWD '07-concurrency/effects-lab/.cache/coursier'` trước lệnh test. Khi cache mặc định hoạt động, bạn không cần cấu hình này.

[project.scala](effects-code/project.scala) pin mọi version, bật `-deprecation -feature -Wunused:all -Werror`, và dùng `using file` trỏ tới **Effects.scala của lab** để tái dùng model, Retry, FakePool và Checkout. Không kéo Comparisons, demo hoặc test lab vào build lời giải; không cần publish artifact. Đây là build một module, không có sbt riêng. Unit test không dùng mạng/Docker sau khi tải dependency.

## Bài 1 — Replay mô tả

`repeat` validate số lần trong IO rồi tạo `List.fill(times)(action).sequence`. Mỗi phần tử là cùng mô tả nhưng sequence diễn giải từng phần tử, vì vậy hai lần tăng Ref trả `List(1,2)`. Zero không thực hiện action; số âm trả IllegalArgumentException. Không lấy kết quả action trước rồi nhân bản giá trị, vì đó là semantics khác.

Test “bài 1” kiểm tra cả zero, hai lần và số âm. Ref được cấp mới trong mỗi lần chạy test, không dùng state mutable cấp suite để tránh các test ảnh hưởng nhau.

## Bài 2 — Fallback đúng loại lỗi

`promotion` pattern match lỗi trong `handleErrorWith`: TimeoutException -> `IO.pure(0L)`; lỗi khác -> `IO.raiseError(error)`. Giá trị thành công giữ nguyên. Nếu dùng catch-all trả 0, lỗi dữ liệu cấu hình khuyến mãi sẽ biến mất và hệ thống trông như đang hoạt động bình thường.

Test kiểm tra đủ thành công 20.000, timeout về 0 và lỗi “bad discount” còn nguyên. Helper không bắt cancellation; IO bị hủy không tự trở thành giá trị fallback.

## Bài 3 — Scope tài nguyên lồng nhau

`session` dùng hai Resource.make ghép bằng `*>`: connection trước statement, release theo thứ tự ngược. Hai test dùng lỗi IOException và cancellation thật. Test hủy chờ `Deferred` được complete từ phần use, nên không thể hủy trước lúc acquire xong rồi nhầm tưởng đã kiểm tra release.

Không tự gọi close ở cả nhánh lỗi lẫn nhánh thành công. Resource quản lý release một lần cho mỗi lần acquire thành công. Không gọi start bên trong use rồi trả về trước khi fiber con hoàn tất; như vậy connection đã đóng mà con còn chạy.

## Bài 4 — Giới hạn action bằng Semaphore

`boundedTraverse` từ chối limit≤0, tạo semaphore rồi `items.parTraverse(a => semaphore.permit.use(_ => action(a)))`. Permit được giữ chỉ trong scope action và trả khi action thành công, lỗi hoặc hủy. parTraverse giữ thứ tự kết quả theo input, dù các action có thời gian khác nhau.

Ba test kiểm tra: peak đúng 2 với runtime ảo, kết quả đúng thứ tự; một action lỗi hủy sibling đã bắt đầu và chạy onCancel; input rỗng/limit sai. Không chỉ kiểm tra List kết quả mà bỏ qua cleanup. Cách này vẫn tạo nhiều fiber chờ; input rất lớn cần streaming/worker queue, không chỉ tăng semaphore limit.

## Bài 5 — Retry đọc có giới hạn

`retryRead` tái dùng Retry của lab với base 100 ms, cap 1 giây và predicate IOException. Bao bước dựng policy trong IO.defer để lỗi cấu hình cũng đi theo IO. `retries=2` là tối đa **3 lần chạy**, không phải 2.

TestControl kiểm tra hai lỗi tạm thời rồi thành công lần ba, tổng thời gian ảo 100+200 ms. Test thứ hai dùng validation để xác nhận chỉ chạy một lần, sau đó kiểm tra helper giữ lỗi của attempt cuối khi hết lượt. Test lab còn kiểm tra hủy trong backoff ngăn attempt kế tiếp.

Jitter và deadline tổng không nằm trong đề bắt buộc. Khi mở rộng, truyền nguồn random kiểm soát được trong test; không dùng Thread.sleep để backoff, không coi retry ghi là an toàn khi chưa có idempotency key.

## Bài 6 — Batch báo giá / mini project

`batch` cấp log Ref và request Ref, mở một FakePool cho cả batch, rồi gọi boundedTraverse trên List Services. Mỗi Checkout.quote tăng request counter khi bắt đầu, mượn connection từng nguồn, chạy đồng thời với timeout/fallback theo chính sách chương. Sau use mới đọc log nên sự kiện close pool đã có trong kết quả.

Ba test kiểm tra bốn request có promotion timeout, giá lỗi giữ nguyên lỗi, và batch rỗng. Với batch thành công: quotes đúng thứ tự, counter=4, số connection đóng bằng số mở và pool đóng cuối. Không yêu cầu tổng connection mở là 12 trong mọi lịch chạy: deadline bao gồm thời gian chờ permit, nên một nhánh có thể timeout trước khi mượn được connection. Test song song ba nguồn bắt buộc bằng handshake và test hủy sibling nằm ở EffectsSuite của lab.

Các giới hạn: mô phỏng hoàn toàn trong bộ nhớ; counter không phải metric thành công; không có JDBC/HTTP, retry ghi, giao dịch hoặc exactly-once. API thật nên biểu diễn trạng thái khuyến mãi/tồn kho chưa xác nhận thay vì âm thầm suy diễn theo mọi nghiệp vụ.

## Đáp án câu hỏi kiểm tra

1. delay suspend body, IO chưa memoize chạy hai lần; Future val chia sẻ một lần chạy/kết quả. pure tính đối số ngay, trước khi runtime quản lý.
2. For-comprehension ghép tuần tự bằng flatMap/map. Chọn tuần tự khi bước sau cần kết quả trước hoặc có quy tắc thứ tự; parMapN/parTraverse cho việc độc lập.
3. Succeeded mang IO chứa kết quả, Errored mang lỗi, Canceled biểu diễn hủy. joinWithNever sẽ không trả khi con bị hủy; dùng join và match Outcome để quan sát trạng thái này.
4. Timeout phải đợi finalizer/body không ngắt được hoặc masking kết thúc; không phải deadline cứng. attempt chỉ chuyển lỗi thành Either, không phục hồi cancellation.
5. Get/set tách hai bước nên có lost update. CAS có thể tính lại hàm modify, vì vậy email trong hàm đó có thể gửi nhiều lần ngoài dự kiến.
6. Budget bao cả chờ permit, thực thi và chờ hủy/cleanup. Pool nhỏ tăng xếp hàng, có thể dẫn đến fallback dù service riêng lẻ nhanh.
7. Policy/jitter do ứng dụng/thư viện riêng quyết định. TestControl xác nhận lịch sleep/backoff/timeout trên clock ảo, không thay test Thread.interrupt, blocking pool hoặc driver thật.
8. zio_3 chứng minh có artifact publish Scala 3; không chứng minh đã compile/run ZIO với Scala 3.9.0 trên môi trường này. Không có lab ZIO trong task.

## Kiểm chứng và nguồn phiên bản

Log test thật: [solutions-test.log](../07-concurrency/effects-lab/verification/solutions-test.log). Kết quả **12 pass, 0 failed, 0 ignored**, không có cảnh báo compiler với `-Werror`. Scala CLI có thể in hint dependency mới; version munit được giữ đúng quy ước repo.

Ngày **16/10/2026**, [POM munit-cats-effect 2.2.0](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom) xác nhận munit 1.2.4 và CE 3.7.0; build này chạy CE patch 3.7.1. [Metadata CE _3](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect_3/maven-metadata.xml) xác nhận stable 3.7.1; bằng chứng source TestControl và POM chi tiết nằm trong [versions.json](../07-concurrency/effects-lab/verification/versions.json) và mục 16 bài học.
