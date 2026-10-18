# Lời giải Chương 14 — REST APIs

Scala 3.9.0 · JDK 21 · cùng dependency với [order-service](../05-backend/order-service/project.scala) · munit 1.2.4, munit-cats-effect 2.2.0. Code nằm trong [ch14-code](ch14-code/), dùng lại source domain/app của lab; không sửa lab và không tạo bản sao service. Vòng 2 ngày 17/10/2026: **6/6 test pass, 0 fail, 0 ignored**, chạy với `-Werror`; output thật: [test-output-round2.txt](ch14-code/test-output-round2.txt). Log vòng 1 giữ tại [test-output.txt](ch14-code/test-output.txt).

Từ root repo, PowerShell:

```powershell
cd solutions/ch14-code
scala-cli.bat test .
```

Trong môi trường hạn chế như sandbox Codex, có thể dùng cache lab đã tải dependency và tắt Bloop. Đây là cấu hình theo môi trường, không phải bước bắt buộc:

```powershell
$env:COURSIER_CACHE = (Resolve-Path ../../05-backend/order-service).Path + '/.cache/coursier'
scala-cli.bat test . --server=false --scalac-option -Werror
```

Không cần Docker hay mạng sau lần tải dependency. Cả sáu bài có test trong [SolutionsSuite.scala](ch14-code/src/test/scala/vn/scalaeng/orders/solutions/SolutionsSuite.scala).

## 1. Tính tiền an toàn

[LineTotal.calculate](ch14-code/src/main/scala/vn/scalaeng/orders/solutions/Extensions.scala) từ chối đơn rỗng, giá/số lượng không dương, tính từng tích và tổng bằng `BigInt`, rồi kiểm tra `isValidLong` trước khi gọi `Money.fromMinor`. Kết quả vẫn dùng Long đơn vị nhỏ nhất, không thay kiểu dữ liệu của API.

Nếu dùng `price * quantity` bằng Long trước rồi mới kiểm tra dương, tràn có thể quay về một số dương khác và lọt validation. Nếu dùng `sum` bằng Long thì phép nhân không tràn vẫn chưa bảo vệ phép cộng. Test dùng `Long.MaxValue * 2` và `Long.MaxValue + 1` để bắt cả hai lỗi; trường hợp hợp lệ 100 × 3 + 50 × 2 cho 400 chính xác.

Trong checkout thật, `price` phải lấy từ bảng catalog phía server trong transaction thích hợp; hàm thuần này chỉ tính toán, không xác nhận nguồn giá đáng tin.

## 2. Lọc/sort order trước pagination

`OrderSearch.page` nhận tập order **đã giới hạn theo user** từ caller: filter status → sort `(totalMinor,id)` → reverse nếu giảm dần → slice. `total` là số phần tử của tập đã filter, không phải số phần tử trang.

Test đặt hai Paid cùng total 100 với ID 3 và 1, thêm một Pending giá 50. Filter Paid rồi offset 1/limit 1 phải lấy ID 3 với total 2. Nếu page trước filter, dữ liệu Pending có thể chiếm mất chỗ và total sẽ sai. Nếu chỉ sort total mà thiếu ID phụ, kết quả có thể phụ thuộc thứ tự Map.

Đây là helper thuần cho bài tập, chưa thêm query vào endpoint cơ sở. Khi triển khai DB, đưa predicate theo user/status, ORDER BY được whitelist và LIMIT/OFFSET vào repository; không tải toàn bộ bảng về rồi mới lọc ở HTTP.

## 3. Cursor theo ID

`CursorPagination.page` kiểm tra `after >= 0`, limit trong 1..100, giữ ID lớn hơn after, sort ID tăng, lấy `limit + 1`. Trang trả nhiều nhất limit dòng; nếu có dòng dư thì nextAfter là ID cuối của trang trả, nếu không thì None. ID có khoảng trống không gây vấn đề vì điều kiện là `>` thay vì cộng ID với limit.

Test duyệt `[1,4,8]`, trang đầu `[1,4]` có cursor 4; thêm ID 9, trang sau `[8,9]` không có cursor tiếp. Tập rỗng và input sai cũng được kiểm tra. Dữ liệu thêm sau cursor có thể xuất hiện ở trang sau: đây không phải snapshot tại thời điểm trang đầu. Nếu cần một snapshot nhất quán, phải thêm chiến lược snapshot/watermark thích hợp và nêu rõ semantics.

Khi triển khai endpoint, có thể encode cursor để client không phụ thuộc biểu diễn; encode không làm nó thành bí mật. Với sort `(name,id)` thì cursor phải mang cả name và ID; đổi name giữa các trang vẫn có thể di chuyển dòng. Không dùng keyset ID để giả vờ duy trì thứ tự name.

## 4. Optimistic concurrency bằng version

`ConditionalOrders` giữ `Map[OrderId,VersionedOrder]` trong Ref. `update` kiểm tra tồn tại → expectedVersion → transition → ghi và tăng version, tất cả trong cùng `modify`. Đọc version ngoài rồi ghi trong một lần khác sẽ có race giống kiểm tra email rồi insert riêng.

Test đầu tiên thử Pending → Shipped, nhận InvalidTransition mà version vẫn 0. Sau đó cho Paid và Cancelled cạnh tranh với expectedVersion 0: đúng một lần thành công version 1, lần còn lại PreconditionFailed. Thứ tự kiểm tra version trước transition đảm bảo caller dùng dữ liệu cũ được báo đúng nguyên nhân cạnh tranh.

Mapping HTTP **đề xuất**, chưa có endpoint ETag trong bài giải:

| Storage result | HTTP khi tích hợp |
|---|---|
| MissingOrder | 404 Problem |
| PreconditionFailed | 412 Problem, yêu cầu đọc lại representation |
| InvalidTransition | 409 Problem |
| Thành công version 1 | 200 + `ETag: "1"` |

Client lấy ETag từ GET rồi gửi `If-Match: "0"`. Thiếu If-Match cần một chính sách công khai (ví dụ yêu cầu header và trả 428); không tự giả định version 0. Lab mới kiểm chứng primitive lưu trữ, không tuyên bố đã xử lý toàn bộ cú pháp If-Match, weak ETag hay wildcard theo RFC 9110. `Math.addExact` ngăn version âm thầm quay vòng.

## 5. Fault injection kiểm tra 500

`FailingReadRepository` ủy quyền tất cả method trừ getUser; method này tạo failed IO với message `db password=secret; id=1`. Test dựng đúng `Api.httpApp`, gọi GET và đọc Problem. Phải được HTTP 500/body 500, detail `Internal server error`, media type application/problem+json, không có password/tên exception.

Không dùng `Left(UnknownUser(...))` để giả lỗi DB: nó sẽ kiểm tra nhánh 404 thay vì exception handler. Không expose route /boom trong ứng dụng để test. Logger backend của lab chưa được cấu hình; khi vận hành phải nối logging/request ID ở Ch17 để che lỗi cho client nhưng vẫn truy được nguyên nhân phía server.

## 6. Decode failure giữ status và header

[DecodeExample.scala](ch14-code/src/main/scala/vn/scalaeng/orders/solutions/DecodeExample.scala) khai báo GET `/protected` có `securityIn(auth.bearer[String]())`, tùy biến `DefaultDecodeFailureHandler.copy(response = ...)` và dùng `Api.problemBody`. Khi thiếu bearer, Tapir chọn 401 và header challenge; callback giữ cả hai rồi tạo Problem từ `code.code`.

Test so sánh HTTP 401, body status 401, title Unauthorized, header raw `WWW-Authenticate: Bearer`. Tra header theo tên CIString vì parser typed của http4s có thể không chấp nhận bare challenge như lưu ý Ch17. Gán cứng Problem(400, ...) sẽ làm test fail dù client nhìn thấy HTTP 401.

Endpoint bài này **chấp nhận mọi chuỗi bearer đã decode**, chỉ để quan sát decode failure. Nó không xác minh chữ ký, thời hạn, issuer hoặc quyền sở hữu resource. Dùng cơ chế của Ch17 khi thực hiện xác thực thật.
