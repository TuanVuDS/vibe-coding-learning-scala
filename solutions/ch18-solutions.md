# Lời giải Chương 18 — Backend Architecture

Scala 3.9.0 · JDK 21 · Cats Effect/testkit 3.7.1 · munit 1.2.4 · munit-cats-effect 2.2.0.
Code tái sử dụng domain/service của P2: [Exercises.scala](ch18-code/src/main/scala/vn/scalaeng/solutions/ch18/Exercises.scala),
[test](ch18-code/src/test/scala/vn/scalaeng/solutions/ch18/ExercisesSuite.scala).

Từ root repo:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
scala-cli.bat test solutions/ch18-code --server=false
```

## Bài 1 — Policy sở hữu thuần

`allowed(principal, owner)` cho phép `principal.admin || principal.id == owner`; ngược lại trả DomainError.Forbidden.
Đây là policy không gắn HTTP status; DTO adapter mới đổi Forbidden thành 403.
Test thử owner, admin khác owner và khách khác owner. Dùng cùng rule trong CLI/worker giúp tránh endpoint mới quên kiểm tra.
Không đưa principal từ JSON request: nó phải xuất phát từ security logic đã xác minh.

## Bài 2 — Cache key không giao thoa tenant

`CatalogKey(tenant, id, currency)` chứa mọi đầu vào ảnh hưởng kết quả; chuẩn hóa currency bằng Locale.ROOT,
không chuẩn hóa tenant thành lower khi hệ thống chưa quy định tenant không phân biệt hoa thường.
Test hai tenant/hai currency khác key, currency có khoảng trắng cùng key, input sai bị chặn.
Cache vẫn không thay thế authorization: caller phải được phép tenant trước khi tra key; key đủ trường chỉ ngăn collision dữ liệu.

## Bài 3 — Retry có điều kiện cho READ

Đi qua cause chain để tìm SQLException SQLSTATE 40001 (serialization failure) hay 40P01 (deadlock).
Dùng tập seen để không lặp vô hạn khi Throwable có cause cycle. Không retry 23505 (unique), input lỗi hoặc mọi SQLException chung chung.
Test có exception bọc, hai SQLSTATE được phép, unique và IllegalArgumentException không được phép.
Chỉ dùng classifier này khi chạy lại **toàn bộ read transaction** mới. Nó không cho phép tự retry POST/commit không chắc chắn.

## Bài 4 — Deadline toàn cục

Đặt `.timeout(budget)` bên ngoài `retryRead`, không đặt timeout đầy đủ trong từng attempt.
TestControl dùng thời gian ảo: lần đọc đầu mất 1 giây rồi lỗi, backoff 100 ms, lần hai bị cancel ở mốc 1500 ms;
chỉ hai attempt được bắt đầu. Nếu mỗi retry được thêm 1500 ms, ngân sách tổng không còn ý nghĩa.
JDBC blocking có thể trì hoãn cancellation; budget là yêu cầu hủy, phải ghép statement/socket timeout ở adapter.

## Bài 5 — Gauge in-flight

`Resource.make(active.update(_ + 1))(_ => active.update(_ - 1)).use(_ => work)` đặt increment/decrement vào vòng đời tài nguyên.
Finalizer vẫn chạy khi work thất bại hoặc bị hủy; Test dùng Deferred để chắc chắn fiber đang chạy trước khi cancel.
Đếm gauge bằng “increment trước, decrement trong map thành công” sẽ bị rò khi exception/cancel.
Đây là gauge dùng cho thực nghiệm; P2 metrics mặc định chỉ có count/duration/failure, chưa tự expose gauge này.

## Bài 6 — Commit rồi invalidation

Helper nhận `IO[Either[DomainError,A]]`; chỉ Right mới invalidate, Left/failed effect không invalidate.
Test đủ ba nhánh và bộ đếm chỉ tăng một. Đây là pattern P2 gắn vào endpoint sau khi service đã hoàn tất transaction.
Không invalidate trước write: reader có thể load snapshot cũ rồi cache nó trước commit.

Vẫn có cửa sổ process chết sau commit trước invalidate; helper không biến hai hệ thống thành transaction chung.
Trong P2, TTL 2 giây giới hạn cửa sổ cũ trong mỗi instance và checkout không tin cache.
Muốn mạnh hơn cần bỏ cache dữ liệu nhạy nhất hoặc dùng outbox/version/invalidation phân tán với đặc tả nhất quán rõ ràng.

**Vòng 2: chạy lại 6/6 test pass**, 0 cảnh báo với `-Werror`, ngày 17/10/2026. Kết quả chạy thật lưu tại [solutions.txt](../14-projects/p2-backend-service/evidence/solutions.txt).
Các test không cần PostgreSQL/Docker; không chứng minh tính đúng của SQL, assembly hay deployment P2.
