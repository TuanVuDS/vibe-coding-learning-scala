# Lời giải Chương 28 — Kafka với Scala

Scala 3.9.0 · JDK 21 · kafka-clients 4.4.0 · circe-parser 0.14.17 · slf4j-nop 2.0.20 · munit 1.2.4. Đã chạy **7/7 test**, 0 fail, `-Werror`, ngày 17/10/2026; chờ review chéo. [Log thật](ch28-code/tests.txt).

[Đề bài](../10-streaming/ch28-kafka-scala.md#12-bài-tập-cơ-bản--nâng-cao), [code đầy đủ](ch28-code/src/main/scala/vn/scalaeng/solutions/ch28/Exercises.scala), [test](ch28-code/src/test/scala/vn/scalaeng/solutions/ch28/ExercisesSuite.scala). Các test này không cần broker:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test solutions/ch28-code --server=false
```

## 1. Adapter schema v2

`decodeV2` parse JSON, kiểm tra version đúng 2, rồi bắt buộc currency VND. Sau khi xác nhận hai điều kiện đó, adapter đổi version nội bộ về 1 và gọi decoder chung để kiểm tra ID/amount. Không dùng cùng cách adapter nếu v2 đã đổi nghĩa amount hoặc đơn vị; phải viết phép chuyển đổi rõ ràng.

Model nội bộ `OrderEvent` hiện dùng schemaVersion 1 sau bước chuẩn hóa, không phải bản sao nguyên payload v2. Khi cần provenance, hãy giữ thêm version gốc/raw ở envelope thay vì trông chờ model đã chuẩn hóa giữ mọi chi tiết wire format.

Test có payload v2 VND hợp lệ, USD bị từ chối, payload v1 đi nhầm decoder và JSON hỏng. Currency không được default từ giá trị thiếu. Adapter này chưa phải chính sách compatibility của Schema Registry và không chứng minh mọi thay đổi bổ sung đều an toàn.

## 2. Tổng lag

`Clients.lag` trả `Option[BigInt]`; phép fold cộng trong Option giữ `None` khi có partition chưa biết committed offset. Hai partition biết hết thì tổng các chênh end/commit; không lấy max và không cộng position đọc hiện tại.

Test dùng hai partition có lag 5 và 3, tổng 8; một partition chưa commit cho `None`; hai chênh `Long.MaxValue` vẫn cộng đúng bằng `BigInt`. Với tập partition rỗng, fold trả `Some(0)`: không có partition cần tổng hợp. Caller phải phân biệt tập rỗng đúng với lỗi không lấy được metadata; đừng biến lỗi API thành vector rỗng.

Khi topic có khoảng trống offset hoặc transaction, đây là khoảng chênh offset, không phải số record chính xác. Có thể hiển thị thêm từng partition và số partition chưa có commit để người vận hành không mất thông tin khi tổng chưa biết.

## 3. Commit frontier

Với **một partition**, giữ thứ tự những record đã poll. `takeWhile(đã_xong)` dừng tại record chưa hoàn tất đầu tiên; committed position là offset cuối prefix + 1. Offset có thể nhảy từ 0 sang 2 vì compaction/transaction: không chờ một record offset 1 mà API chưa hề trả về.

Test `0=true,2=false,5=true` chỉ cho commit 1; không được lấy max completed là 5 rồi commit 6. Khi 2 cũng hoàn tất, có thể commit 3; record 5 vẫn chưa được bỏ qua. Nếu phần tử đầu chưa xong hoặc vector rỗng, trả `None`, giữ commit trước đó.

Hàm đòi offset không âm và tăng chặt. `Math.addExact` ở đây là kiểm tra ranh giới cursor, **không** làm phép gộp doanh thu/Monoid; không có lời hứa về tính kết hợp. Production nên dùng metadata next-offset/leader epoch từ API phù hợp, theo từng partition và assignment generation. Bài tập chưa quản lý inflight jobs qua rebalance hay lưu trạng thái hoàn tất ra đĩa.

## 4. Backoff có cap

`backoff(base,cap,retries)` tạo dãy thời gian nghỉ sau lần đầu: nhân đôi rồi chặn ở cap. Dùng `BigInt` ở bước nhân, sau khi chặn mới chuyển về Long, tránh việc `Long.MaxValue * 2` cuộn thành số âm.

Test base 10/cap 25/retries 4 cho dãy 10,20,25,25. Không có retry thì dãy rỗng nhưng vẫn có **một initial attempt** nếu caller chạy action. Các tham số âm/zero không hợp lệ bị từ chối; số retry tối đa 1.000 để tránh cấu hình tạo vector khổng lồ.

Production có thể thêm jitter để nhiều consumer không đồng loạt thử lại, nhưng khi đó phải truyền nguồn random để test được, đặt tổng budget và giữ nhịp poll phù hợp. Dãy ở đây không tự phân loại lỗi hay điều khiển consumer; `Retry.run`/caller mới chịu trách nhiệm đó.

## 5. Snapshot dedup

`Snapshot` chứa cả `seen: eventId -> OrderEvent` và `totals`. `applyEvent` là hàm thuần: không có ID thì trả state mới; cùng fingerprint thì trả nguyên state; khác fingerprint thì trả `Left`, không thay số tiền. Tổng được cộng bằng `BigInt`.

Test giữ snapshot sau event đầu, tạo đối tượng restored rồi gửi lại. Tổng vẫn 100; đổi amount thành 200 trả conflict và state ban đầu không thay đổi. Việc so toàn bộ event tránh bỏ sót khác order ID hoặc version.

**Đây chưa phải persistence.** `copy()` trong test chỉ mô phỏng dữ liệu được phục hồi; không ghi database và không kill JVM. Muốn dùng thật phải lưu event ID/fingerprint và tổng trong cùng transaction bền vững. Bảng có unique event ID cần phân biệt duplicate hợp lệ với conflict payload; chỉ `ON CONFLICT DO NOTHING` rồi bỏ mọi khác biệt sẽ giấu xung đột. Cần kiểm chứng DB/rollback riêng trước khi gắn nhãn chịu crash.

## 6. Replay DLT và khoảng lỗi commit

`replay` kiểm tra envelope là JSON, đọc `raw` dạng chuỗi rồi dùng `Protocol.decode`. Record JSON hỏng trong raw vẫn hỏng dù được bọc thêm DLT; tombstone/null raw cũng không tự có dữ liệu để replay. Test chỉ chấp nhận sau khi raw được sửa thành một event hợp lệ.

Khi replay thật, giữ provenance nguyên bản và có audit thông tin sửa lỗi. Không tự sinh event ID mới cho cùng tác động vì sẽ vượt qua dedup. Xung đột ID cần quyết định nghiệp vụ, không chỉ đổi amount để decoder hết báo lỗi. Lời giải là bước preflight; không tự gửi dữ liệu tới broker/cluster bên ngoài.

Test bổ sung dùng processor thật với publisher ghi khóa vào vector, rồi làm commit ném exception. Lần đầu DLT đã tồn tại nhưng source chưa tiến; lần gọi thứ hai publish lại cùng khóa `input:0:2`. Assertion đòi hai khóa bằng nhau, minh họa việc DLT cũng at-least-once.

Consumer DLT có thể dùng `(source_topic, source_partition, source_offset)` làm khóa dedup trong kho bền vững, đồng thời giữ lịch sử sửa/replay riêng. Topic compaction có thể giữ bản mới theo key sau này, nhưng không ngăn consumer nhìn thấy hai record trước khi compaction diễn ra; không lấy compaction làm bảo đảm xử lý đúng một lần.
