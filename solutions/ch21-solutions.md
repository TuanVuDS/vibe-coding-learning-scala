# Lời giải Chương 21 — Data Formats và Serialization

> Scala **3.9.0** · JDK **21** · munit **1.2.4**. Dependency giữ cùng pin với [formats-lab/project.scala](../08-data-processing/formats-lab/project.scala): circe-parser 0.14.17, Commons CSV 1.14.1, Avro 1.12.2, parquet-avro 1.18.1, Hadoop client-api/client-runtime 3.4.2, slf4j-nop 2.0.17. Trạng thái: đã review chéo bởi Claude; Scala CLI vòng 2 **12/12 pass**, không cảnh báo compiler. Không cần Docker hay dịch vụ ngoài.

Mã đầy đủ: [Exercises.scala](ch21-code/src/main/scala/vn/scalaeng/solutions/ch21/Exercises.scala). Kiểm chứng: [ExercisesSuite.scala](ch21-code/src/test/scala/vn/scalaeng/solutions/ch21/ExercisesSuite.scala). Lời giải dùng source production của lab qua directive `using file`, không nạp test lab.

Từ gốc repo chạy:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test solutions/ch21-code --server=false --suppress-outdated-dependency-warning
```

Tùy chọn suppress chỉ tắt gợi ý cập nhật dependency; compiler vẫn bật `-deprecation -feature -Wunused:all -Werror`. [test-output-round2.txt](ch21-code/test-output-round2.txt) giữ transcript thật của vòng 2 ngày 17/10/2026; [test-output.txt](ch21-code/test-output.txt) giữ lần chạy vòng 1. Sáu bài có **12 test**, hai test mỗi bài. Các đoạn Money, Presence và DecimalWire trong chương được trích trực tiếp từ source lời giải đã biên dịch này.

## Bài 1 — Tiền có currency và chuyển đổi chính xác

`Money.parse` tra scale từ hợp đồng USD/VND trước khi parse; không suy ra currency từ số lượng dấu thập phân. Regex chỉ nhận chữ số và một phần thập phân, nên không nhận dấu âm, scientific notation hay dấu phân cách hàng nghìn.

Sau đó `Conversions.minor` dùng BigDecimal từ **chuỗi**, dịch dấu thập phân theo scale và gọi `longValueExact`. Nếu phần lẻ không thể biểu diễn hoặc kết quả vượt Long, hàm trả Left. `1.000 USD` vẫn biểu diễn chính xác thành 100 cent; quy tắc ở đây là không mất giá trị, không giới hạn số ký tự zero đuôi. Nếu hợp đồng yêu cầu chính xác tối đa hai ký tự phần lẻ, cần thêm validation lexical.

Kết quả được test: USD `12.30` → 1230; VND `12000` → 12000; zero và Long.MaxValue được giữ; USD `1.001`, VND `1.1`, EUR, `1e2`, số âm và overflow bị từ chối. Không dùng `Double` và không gọi `.toLong` để âm thầm cắt phần lẻ.

**Đánh đổi:** mapping currency nhỏ giúp bài tập có hợp đồng rõ; ứng dụng thật cần danh sách tiền tệ/version chính sách chính thức của mình. Hợp đồng thay scale là migration nghiệp vụ, không chỉ đổi tên biến.

## Bài 2 — Phân biệt missing, null và giá trị

`Presence` có ba nhánh:

| JSON | Kết quả ADT | Ý nghĩa ví dụ cho PATCH |
|---|---|---|
| Không có trường note | Missing | Giữ note cũ |
| `"note": null` | ExplicitNull | Xóa note |
| `"note": ""` | Value("") | Gán chuỗi rỗng nếu nghiệp vụ cho phép |
| `"note": "paid"` | Value("paid") | Gán giá trị mới |

Hàm đọc `JsonObject` rồi xét `obj("note")`; không decode thẳng thành `Option[String]` vì decoder đó gộp missing và null. Root array hoặc note là số đều bị từ chối. Khi storage chỉ cần optional value, `Option` của lab đủ; khi có thao tác cập nhật, giữ ADT đến sau bước quyết định nghiệp vụ rồi mới ánh xạ vào storage.

Hai test kiểm tra đủ bốn hàng trên và hai sai kiểu. Không dùng `.orElse(Right(None))` để nuốt lỗi kiểu.

## Bài 3 — Avro decimal bằng bytes

`DecimalWire` tạo schema bytes với logical type decimal, precision 12, scale 2. Chuyển chuỗi thành `java.math.BigDecimal`, gọi `setScale(2, UNNECESSARY)` để không cho làm tròn; kiểm tra precision rồi dùng `Conversions.DecimalConversion.toBytes` / `fromBytes`.

Test round-trip `12.30`, `-12.30`, `0.00`, `9999999999.99`. `10000000000.00` cần 13 chữ số ở scale 2 nên vượt precision; `1.001` không thể đổi về scale 2 mà không mất giá trị. Cả hai trả Left.

**Giới hạn phạm vi:** bài này kiểm chứng decimal **conversion bytes** trong Avro API, không phải một file Avro/Parquet decimal end-to-end. File lab chính dùng `amountMinor: Long` cùng currency và được round-trip file thật. Nếu chuyển storage sang DECIMAL, cần thêm schema resolution, file round-trip, khả năng đọc của engine đích và chính sách đổi precision/scale. Annotation logical type không thay thế validation tiền.

## Bài 4 — Thời điểm, timezone và độ chính xác

`TimestampWire.parse` dùng parser `OffsetDateTime` để yêu cầu offset; chuẩn hóa về Instant; từ chối nano không chia hết cho 1.000.000. Lựa chọn này làm hợp đồng timestamp-millis rõ ràng, tránh cắt ngầm dữ liệu micro/nanosecond.

Test chứng minh `2026-10-01T00:30:00+07:00` là `2026-09-30T17:30:00Z`, nhưng ngày báo cáo với `Asia/Ho_Chi_Minh` là 2026-10-01. Hai chuỗi `2026-11-01T01:30:00-04:00` và `2026-11-01T01:30:00-05:00` cách nhau một giờ dù đồng hồ địa phương giống nhau. Test dùng offset tường minh; không dựa vào timezone mặc định của máy.

Input thiếu offset và `.123456` bị từ chối. Nếu nguồn chỉ cung cấp LocalDateTime, phải có hợp đồng riêng về zone và giờ DST mơ hồ; không tự thêm `Z` vào chuỗi địa phương.

## Bài 5 — Bridge cho đổi tên, không làm mất consumer cũ

V1 có `id:int,label:string`. Bridge giữ hai trường này, thêm `name:string` default rỗng. `RenameBridge.upgrade` copy label vào **cả label và name** cho record mới. Hai chiều đều tương thích cấu trúc:

1. File v1 đọc bằng bridge: Avro cung cấp name rỗng. Consumer mới phải dùng name nếu có giá trị, fallback label khi name rỗng; hoặc backfill trước khi dựa hoàn toàn vào name.
2. File bridge đọc bằng v1: reader bỏ trường name; label vẫn giữ giá trị gốc.

Test gọi `SchemaCompatibility`, ghi/đọc file Avro thật và so nội dung hai tên. Test khác xác nhận alias-only trong schema mới không tạo full compatibility với schema cũ.

Rollout thực tế: triển khai reader hiểu bridge và fallback → triển khai producer dual-write → kiểm tra hai giá trị luôn bằng nhau → backfill dữ liệu lịch sử → xác nhận không còn reader cần label → phát hành schema bỏ label theo hợp đồng mới. Không đánh dấu bước cuối full compatible với v1, vì reader v1 không có default cho label. Alias hoặc bridge không tự giải quyết dữ liệu hai tên mâu thuẫn do producer lỗi.

## Bài 6 — JSONL sang Parquet có quarantine

`Quarantine.migrate` mở nguồn và file rejects qua `Using.Manager`, lấy từng dòng cùng số thứ tự, rồi:

1. Parse/decode JSON thành `Order`; lỗi decode thành danh sách một lỗi thư viện.
2. Với record decode được, gọi `Order.validate` để lấy tất cả lỗi nghiệp vụ.
3. Valid đi vào iterator mà Parquet writer tiêu thụ; invalid ghi ngay JSONL gồm `line`, `raw`, `errors`.
4. Đóng writer để hoàn thành footer, đóng rejects/source, rồi mới trả `MigrationReport`.

Không gom toàn input hoặc toàn rejects vào memory; số dòng bắt đầu từ 1. File JSONL lỗi syntax không làm dừng việc xử lý các dòng sau. Header CSV và các lỗi cấp file không tham gia bài tập này.

Test đầu có ba dòng: một valid, một syntax lỗi, một record vừa sai id vừa âm tiền. Kết quả **1 accepted / 2 rejected**; Parquet chứa đúng Order valid; rejected có dòng 2 và 3; record cuối giữ đủ hai lỗi nghiệp vụ. Test thứ hai bảo đảm zero accepted vẫn tạo Parquet rỗng đọc được, đồng thời từ chối output trùng input qua đường dẫn chuẩn hóa lexical.

**Giới hạn:** kiểm tra path chưa giải quyết alias qua symlink/hardlink; thư mục đích cần tồn tại. Lỗi I/O sẽ làm migration thất bại, có thể còn output dở dang; đây chưa phải publish nguyên tử hai file. Mini project của chương bổ sung staging/manifest và cleanup có kiểm soát. Bài giải không ghi đè input nhưng sẽ ghi đè các output đã chỉ định, vì vậy khi chạy thật nên cấp thư mục batch mới. Raw rejected có thể chứa dữ liệu nhạy cảm; quyền truy cập/retention thuộc bước triển khai.

## Gợi ý câu hỏi kiểm tra

1. Decode kiểm tra cấu trúc, chưa kiểm tra `amountMinor >= 0` hoặc millis precision. JSON âm tiền và Instant sáu chữ số phần lẻ đều có thể decode được.
2. Default được áp khi reader cần trường không có trong writer schema; null đã ghi là một giá trị và phải khớp schema. Nó không tự đổi thành default string.
3. Resolution dựa trên cặp kiểu writer/reader, không thử narrow từng giá trị. Promotion int→long không có nghĩa long→int được phép.
4. Alias ở reader v2 giúp đọc tên cũ. Reader v1 không tự biết alias mới; bridge giữ trường cũ để consumer đó tiếp tục đọc.
5. Không có escape thì chuỗi thật bằng sentinel có thể bị nhập chung với null. Cột tag của lab tránh nhập chung này.
6. Nội dung lặp giúp dictionary và compression; dữ liệu ngẫu nhiên có thể ít lặp nên khác cả kích thước lẫn CPU codec. Phải đo đúng workload.
7. Column read bớt parse/dựng các trường khác và bớt validation. Với Parquet còn giảm chunk đọc; phép đếm API byte bổ sung bằng chứng I/O nhưng không đo đĩa vật lý.
8. Instant chỉ giữ thời điểm, không giữ timezone nguồn. Lưu thêm zone/offset nếu cần tái hiện ngữ cảnh giờ ban đầu.

## Đọc kết quả benchmark

Đối chiếu [chương 21 mục 6 và 9](../08-data-processing/ch21-data-formats.md) với [benchmark.txt](../08-data-processing/formats-lab/verification/benchmark.txt), [raw.csv](../08-data-processing/formats-lab/verification/benchmark-raw.csv) và [projection.txt](../08-data-processing/formats-lab/verification/projection.txt). Luôn báo codec và cách đo; không biến một tỷ lệ nhanh hơn trên máy này thành cam kết cho mọi pipeline.

Nếu tăng payload note để nghiên cứu projection, chạy tất cả định dạng trên **cùng generator mới**, giữ nguyên kiểm tra digest, bổ sung warm-up/lượt đo và giữ transcript mới riêng. Không so kết quả từ hai dataset khác nhau như thể chỉ đổi format.
