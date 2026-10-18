# Chỉ mục bài tập

Đề bài đầy đủ nằm trong mục 12 của từng bài học (để đọc cùng ngữ cảnh lý thuyết).
Lời giải nằm trong [`../solutions/`](../solutions/) — hãy tự làm và viết test trước khi xem.

## Cách làm bài tập

1. Tạo file `Exercises.scala` trong package của chương, ví dụ
   `01-scala3-core/code/src/main/scala/scalaeng/ch03/Exercises.scala`.
2. Tạo test tương ứng trong `src/test/scala/scalaeng/ch03/ExercisesSuite.scala`.
3. Chạy `scala-cli test .` (hoặc `~test` trong sbt shell) cho tới khi pass.
4. So sánh với lời giải; đọc phần giải thích để thấy các lựa chọn thiết kế khác.

## Danh sách

| Chương | Bài | Mức | Chủ đề | Lời giải |
|---|---|---|---|---|
| 1 | 1–2 | Cơ bản | JVM, tương tác Java | [ch01-ch02](../solutions/ch01-ch02-solutions.md) |
| 1 | 3 | Trung bình | Chọn Scala/Java/Python/SQL | như trên |
| 1 | 4 | Nâng cao | Phản biện "Spark bằng Scala 3" | như trên |
| 2 | 1–2 | Cơ bản | Cài đặt, REPL | như trên |
| 2 | 3–4 | Trung bình | Scala CLI, chạy test | như trên |
| 2 | 5 | Nâng cao | Dependency Java, `%` vs `%%` | như trên |
| 3 | 1 | Cơ bản | String interpolation | [ch03](../solutions/ch03-solutions.md) |
| 3 | 2 | Cơ bản | Validate số điện thoại | như trên |
| 3 | 3 | Trung bình | VAT, làm tròn `BigDecimal` | như trên |
| 3 | 4 | Trung bình | `match` theo khoảng, boundary test | như trên |
| 3 | 5 | Nâng cao | Parse dòng CSV, xử lý lỗi | như trên |
| 3 | Mini project | — | Máy tính hóa đơn | (tự làm) |
| 4 | 1 | Cơ bản | Ghép hàm generic | [ch04](../solutions/ch04-solutions.md) |
| 4 | 2 | Cơ bản | Đệ quy đuôi | như trên |
| 4 | 3 | Trung bình | Phân trang an toàn | như trên |
| 4 | 4 | Trung bình | Trung bình theo nhóm | như trên |
| 4 | 5 | Nâng cao | Sessionization (DE) | như trên |
| 4 | Mini project | — | Bộ làm sạch dữ liệu người dùng | (tự làm) |
| 5 | 1–2 | Cơ bản | Enum có tham số, case class + copy | [ch05](../solutions/ch05-solutions.md) |
| 5 | 3–4 | Trung bình | Trait đa hình, đóng gói trạng thái | như trên |
| 5 | 5 | Nâng cao | Máy trạng thái suy ra | như trên |
| 5 | Mini project | — | Domain đơn hàng + voucher | (tự làm) |
| 6 | 1–2 | Cơ bản | Đếm từ tiếng Việt, top N | [ch06](../solutions/ch06-solutions.md) |
| 6 | 3–4 | Trung bình | Gộp Map, dedup giữ bản mới nhất (DE) | như trên |
| 6 | 5 | Nâng cao | Cửa sổ trượt phát hiện burst lỗi | như trên |
| 6 | Mini project | — | Báo cáo access log từ file | (tự làm) |
| 7 | 1–2 | Cơ bản | Option, for-comprehension | [ch07](../solutions/ch07-solutions.md) |
| 7 | 3 | Trung bình | Either + Try + ADT lỗi, ngày là tham số | như trên |
| 7 | 4 | Trung bình | Tích lũy lỗi không dùng thư viện | như trên |
| 7 | 5 | Nâng cao | Tồn kho theo sự kiện (event sourcing) | như trên |
| 7 | Mini project | — | Parser CSV an toàn + rejected.csv | (tự làm) |
| 8A | 1–2 | Cơ bản | Generic `Result[A]`, opaque `Email` | [ch08](../solutions/ch08-solutions.md) |
| 8A | 3–4 | Trung bình | Opaque `Percent`, upper bound giữ kiểu | như trên |
| 8A | 5 | Nâng cao | Variance: Pipeline/Sink | như trên |
| 8A | Mini project | — | Domain đơn hàng kiểu an toàn | (tự làm) |
| 8B | 1–2 | Cơ bản | Type class `Show`, `Ordering` trong companion | như trên |
| 8B | 3–4 | Trung bình | Conditional Monoid, `Validator` | như trên |
| 8B | 5 | Nâng cao | `aggregateByKey` — liên hệ `reduceByKey` | như trên |
| 8B | Mini project | — | Module báo cáo CSV | (tự làm) |
| 9 | 1–2 | Cơ bản | Cấu hình mới, decorator logger | [ch09-ch11](../solutions/ch09-ch11-solutions.md) |
| 9 | 3–4 | Trung bình | Tách module, exit code mới | như trên |
| 9 | 5 | Nâng cao | SLF4J + Logback (cần Internet) | như trên |
| 10 | 1–2 | Cơ bản | Boundary test, generator mới | như trên |
| 10 | 3–4 | Trung bình | Property bảo toàn, property bất biến khoảng trắng | như trên |
| 10 | 5 | Nâng cao | Tự cài shrinking | như trên |
| 11 | 1–2 | Cơ bản | Cờ JVM, kích thước stack | như trên |
| 11 | 3–4 | Trung bình | Boxing, GC theo kích thước heap | như trên |
| 11 | 5 | Nâng cao | Hai cách tránh deadlock | như trên |
| P1 | 1–6 | Cơ bản → Nâng cao | Mở rộng Scala Data Processor | [README P1 mục 10](../14-projects/p1-scala-data-processor/README.md) (đề mở) |
| 12 | 1–2 | Cơ bản | PUT idempotent, sort ổn định | [ch12-ch16a](../solutions/ch12-ch16a-solutions.md) |
| 12 | 3–4 | Trung bình | ETag/304, live/ready | như trên |
| 12 | 5 | Nâng cao | Optimistic concurrency (If-Match/412) | như trên |
| 16A | 1–2 | Cơ bản | Tài khoản CAS, fallback chain | như trên |
| 16A | 3–4 | Trung bình | Giới hạn đồng thời, circuit breaker | như trên |
| 16A | 5 | Nâng cao | Sửa thread starvation | như trên |
| 8C | 1 | Cơ bản — batch có giới hạn. | Viết BatchPolicy.bounded(inline size: Int) nhận hằng từ 1 đến 10000 bằng inline if/error. … | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 8C | 2 | Cơ bản — match type đệ quy. | Viết Leaf[X] bóc lặp List/Option tới kiểu cuối. Chứng minh bằng =:= rằng Leaf[List[Option[… | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 8C | 3 | Trung bình — tuple schema. | Viết arity[Ts <: Tuple] bằng erasedValue, EmptyTuple, *:. Viết acceptedRow(row) bọc từng c… | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 8C | 4 | Trung bình — báo cáo sự kiện. | Tạo EventReport(eventId: String, attempts: Int, accepted: Boolean, note: Option[String]) d… | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 8C | 5 | Nâng cao — tự derive `Show`. | Tự viết Show[A] với instance String, Int; derive product từ Mirror. OrderSummary("ORD-1", … | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 8C | 6 | Nâng cao — macro phần trăm. | Trong file riêng, viết PercentLiteral.checked(inline n: Int) nhận hằng 0..100; dùng Expr.v… | [ch08c-solutions.md](../solutions/ch08c-solutions.md) |
| 13 | 1 | Cơ bản — phân tích | Chấm lại bảng mục 3.4 cho bối cảnh: *team 3 người đã viết Java Spring 5 năm, cần một admin… | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 13 | 2 | Cơ bản — code | Thêm GET /api/v1/products?q=... (lọc theo tên, không phân biệt hoa thường) bằng cả http4s … | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 13 | 3 | Trung bình | Sửa bản http4s để GET /api/v1/products/abc trả 400 + Problem thay vì 404 Not found, mà khô… | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 13 | 4 | Trung bình | Làm cho bản Tapir trả Problem JSON (application/json) khi decode thất bại thay vì text/pla… | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 13 | 5 | Nâng cao | Viết integration test chạy server Ember thật trên cổng do OS chọn (cổng 0) và gọi bằng Emb… | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 13 | 6 | Nâng cao — nghiên cứu | Tra Maven Central để trả lời: Doobie đã có bản 1.0.0 final chưa? fs2-kafka 4.0 đã GA chưa? | [ch13-solutions.md](../solutions/ch13-solutions.md) |
| 14 | 1 | — | Tính tiền an toàn: viết LineTotal.calculate(List[(Long,Int)]). Giá đơn vị và số lượng phải… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 14 | 2 | — | Lọc/sort đơn hàng: viết hàm thuần nhận tập order của một user, status tùy chọn, sort total… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 14 | 3 | — | Cursor pagination: viết page users theo after (ID, 0 là bắt đầu), limit 1..100. Lấy dư một… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 14 | 4 | — | Cập nhật có điều kiện: tạo kho order riêng có version ban đầu 0. update(id, expectedVersio… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 14 | 5 | — | Fault injection: cài decorator repository khiến getUser thất bại bằng IO có message chứa b… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 14 | 6 | — | Decode failure không gán cứng 400: tạo endpoint Tapir nhỏ có bearer input, dùng cách xử lý… | [ch14-solutions.md](../solutions/ch14-solutions.md) |
| 15 | 1 | Cơ bản — SQL | Viết TopSellers.top(n): n sản phẩm bán nhiều nhất theo số lượng, kèm doanh thu, bỏ qua đơn… | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 15 | 2 | Cơ bản — migration | Thêm migration V3: cột orders.cancelled_at và ràng buộc "status = CANCELLED ⇔ cancelled_at… | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 15 | 3 | Trung bình — transaction | CancelOrder.cancel(id): chỉ hủy đơn PENDING, hoàn kho. Hai yêu cầu hủy đồng thời chỉ được … | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 15 | 4 | Trung bình — phân trang | Keyset pagination cho users theo (created_at, id). Test đi hết dữ liệu qua nhiều trang khô… | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 15 | 5 | Nâng cao — đồng thời | Sửa lost update không dùng SELECT ... FOR UPDATE. Giải thích vì sao cách của bạn đúng ở RE… | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 15 | 6 | Nâng cao — index | Truy vấn "10 đơn mới nhất của một user". Thiết kế index để plan không có node Sort; viết t… | [ch15-solutions.md](../solutions/ch15-solutions.md) |
| 16B | 1 | — | Replay: repeat(action,times) trả List kết quả. Hai lần tăng Ref cho hai giá trị kế tiếp; z… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 16B | 2 | — | Fallback: promotion(action) chỉ đổi TimeoutException thành 0L; giữ lỗi khác. Test thành cô… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 16B | 3 | — | Resource: session(log) mở connection rồi statement. Test đóng ngược lúc use lỗi/hủy; Defer… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 16B | 4 | — | Giới hạn: boundedTraverse(items,limit)(action) bằng Semaphore, giữ thứ tự input. Test peak… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 16B | 5 | — | Retry đọc: base 100 ms, nhân đôi, trần 1 giây; retries+1 lần chạy, chỉ IOException. TestCo… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 16B | 6 | — | Batch: dùng Checkout/pool, giới hạn request đồng thời, một Ref counter, giữ thứ tự kết quả… | [ch16b-solutions.md](../solutions/ch16b-solutions.md) |
| 17 | 1 | Cơ bản | Theo RFC 6750, token sai/hết hạn nên kèm WWW-Authenticate: Bearer error="invalid_token". L… | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 17 | 2 | Cơ bản | Middleware thêm header bảo mật cho API JSON: X-Content-Type-Options: nosniff, Cache-Contro… | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 17 | 3 | Trung bình | Đăng xuất: thu hồi một access token trước khi hết hạn. Token đã thu hồi phải bị từ chối vớ… | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 17 | 4 | Trung bình | Chống credential stuffing: giới hạn số lần đăng nhập sai theo email (dù đến từ nhiều IP). … | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 17 | 5 | Nâng cao | Chạy API qua HTTPS thật: sinh keystore bằng keytool, bật TLS cho Ember, viết test gọi bằng… | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 17 | 6 | Nâng cao — thiết kế | Một web app (frontend cùng domain) và một app mobile dùng chung API. Chọn session cookie h… | [ch17-solutions.md](../solutions/ch17-solutions.md) |
| 18 | 1 | — | Tách policy ownership thuần: owner và admin được phép; người khác nhận DomainError.Forbidd… | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 18 | 2 | — | Một cache dùng chung nhiều cửa hàng và hai tiền tệ. Thiết kế key gồm tenant, product id, c… | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 18 | 3 | — | Chỉ retry READ khi SQLSTATE là 40001 hoặc 40P01, kể cả exception bị bọc; không retry 23505… | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 18 | 4 | — | Ghép retry ở bài 3 với deadline toàn cục 1500 ms. Dùng TestControl chứng minh mỗi retry kh… | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 18 | 5 | — | Viết gauge in-flight bằng Resource; phải về 0 khi thành công, exception và cancellation. | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 18 | 6 | — | Viết helper commit-then-invalidate: chỉ invalidate sau Right, không invalidate khi Left ho… | [ch18-solutions.md](../solutions/ch18-solutions.md) |
| 19 | 1 | Cơ bản | SDK cũ gửi ts dạng "2026-10-15 08:00:00" không có múi giờ, tài liệu SDK nói là giờ Việt Na… | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 19 | 2 | Cơ bản | Báo cáo số impression theo (quốc gia, ngày VN) từ danh sách AdEvent đã sạch. | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 19 | 3 | Trung bình | Một SDK sinh event_id mới mỗi lần gửi lại. Dedup theo nội dung; nêu đánh đổi. | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 19 | 4 | Trung bình | Dedup với bộ nhớ giới hạn: chỉ nhớ id trong cửa sổ 10 phút theo thời gian sự kiện. Đo số i… | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 19 | 5 | Nâng cao | Sessionization: các hoạt động của một user cách nhau quá 30 phút thuộc hai phiên. Trả số s… | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 19 | 6 | Nâng cao | Đọc CSV đơn hàng order_id,order_date,amount_vnd,status theo luồng, tính doanh thu theo ngà… | [ch19-solutions.md](../solutions/ch19-solutions.md) |
| 20 | 1 | Cơ bản | Quy tắc chất lượng trên tổng hợp: tỷ lệ đơn Refunded mỗi ngày không vượt 20%. | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 20 | 2 | Cơ bản | Viết Monoid[Mean] đúng; kiểm tra bằng ScalaCheck rằng trung bình không phụ thuộc cách chia… | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 20 | 3 | Trung bình | Dedup tổng quát bằng type class Versioned[A] (khóa + phiên bản), giữ bản mới nhất, không p… | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 20 | 4 | Trung bình | Top-K sản phẩm theo doanh thu khi dữ liệu chia nhiều partition. Chỉ ra phản ví dụ cho cách… | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 20 | 5 | Nâng cao | Số user khác nhau mỗi ngày trên dữ liệu chia partition. Monoid nào? Bộ nhớ tăng theo gì? | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 20 | 6 | Nâng cao | Pipeline có quality gate: validation → kiểm tra chất lượng → chỉ tính báo cáo khi gate cho… | [ch20-solutions.md](../solutions/ch20-solutions.md) |
| 21 | 1 | — | Tiền theo currency: viết Money.parse(text,currency) chỉ nhận số thập phân không âm, USD sc… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 21 | 2 | — | Missing/null/value: viết ADT Presence đọc trường note từ JSON object, giữ riêng Missing, E… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 21 | 3 | — | Decimal binary: round-trip Avro decimal qua DecimalConversion, bytes, precision 12/scale 2… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 21 | 4 | — | Thời gian: parse input có offset thành Instant đúng millis; reject input thiếu offset và p… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 21 | 5 | — | Đổi tên có giai đoạn bridge: giữ label, thêm name default rỗng, dual-write cùng giá trị. G… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 21 | 6 | — | Migration có quarantine: đọc JSONL theo luồng, decode và tích lũy lỗi nghiệp vụ, ghi đơn h… | [ch21-solutions.md](../solutions/ch21-solutions.md) |
| 22 | 1 | Cơ bản | Viết lại Order và OrderStatus của Chương 15 bằng cú pháp Scala 2.13 (sealed trait, case ob… | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 22 | 2 | Cơ bản | Dự đoán số stage của: (a) select + write.parquet; (b) distinct().count(); (c) join hai Dat… | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 22 | 3 | Trung bình | Chứng minh cache có lợi khi một Dataset được dùng cho hai nhánh (đếm theo campaign và đếm … | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 22 | 4 | Trung bình | Đo ảnh hưởng của spark.sql.shuffle.partitions (200 vs 4) khi tắt AQE trên groupBy 1 triệu … | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 22 | 5 | Nâng cao | Thí nghiệm "lỗi dữ liệu không cứu được bằng chạy lại": task luôn lỗi ở một dòng xấu → job … | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 22 | 6 | Nâng cao | Viết Execution.partitioned của Chương 20 bằng RDD API (mapPartitions + reduceByKey) và so … | [ch22-solutions.md](../solutions/ch22-solutions.md) |
| 23 | 1 | Cơ bản | Đọc CSV ở chế độ PERMISSIVE với cột _corrupt_record; tách dòng tốt và dòng hỏng (giữ nguyê… | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 23 | 2 | Cơ bản | Viết doanh thu theo khách bằng Spark SQL và bằng DataFrame API; chứng minh hai kế hoạch tố… | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 23 | 3 | Trung bình | Top-N sku theo doanh thu mỗi ngày bằng window. | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 23 | 4 | Trung bình | Chuẩn hóa sku (trim + upper) bằng UDF và bằng hàm có sẵn: chốt hợp đồng trước (khoảng trắn… | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 23 | 5 | Nâng cao | Join trên khóa có thể NULL: đếm số cặp với === và <=>; giải thích. | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 23 | 6 | Nâng cao | Doanh thu theo khách bằng Dataset[OrderRow] + groupByKey/mapGroups; so kết quả và kế hoạch… | [ch23-solutions.md](../solutions/ch23-solutions.md) |
| 24 | 1 | Cơ bản | Thêm quy tắc liên trường: updated_at không được trước ngày đặt hàng day. Không cộng lỗi "p… | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 24 | 2 | Cơ bản | Báo cáo chất lượng theo lô: số lần vi phạm theo từng lý do, mỗi ingest_date. | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 24 | 3 | Trung bình | Quality gate theo lô: dừng (không ghi gì) nếu tỷ lệ dòng bị loại của một lô vượt ngưỡng; n… | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 24 | 4 | Trung bình | Write–audit–publish: ghi vào staging, kiểm tra, đổi tên sang thư mục phục vụ; kiểm tra thấ… | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 24 | 5 | Nâng cao | Tái hiện partition mồ côi của dynamic overwrite thuần (writePartitioned, không dọn), rồi v… | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 24 | 6 | Nâng cao | Compaction: gộp một partition 8 file nhỏ thành 2 file, chứng minh nội dung (multiset) khôn… | [ch24-solutions.md](../solutions/ch24-solutions.md) |
| 25 | 1 | — | Đọc chiến lược join cuối. Viết joinStrategy(plan) chỉ nhận final AQE plan (hoặc plan không… | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 25 | 2 | — | Báo cáo theo stage. Từ Vector[TaskSample], tính số attempt thành công/thất bại, upper medi… | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 25 | 3 | — | Ước lượng partition ban đầu. Viết hàm từ tổng byte và target byte, ceil, tối thiểu 1, có t… | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 25 | 4 | — | Salting có hợp đồng. Trước khi dùng saltedJoin, từ chối dimension duplicate key hoặc null … | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 25 | 5 | — | SUM hai tầng an toàn tiền nguyên. Group theo (customer_id,salt) rồi cộng về customer. Cast… | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 25 | 6 | — | Quản lý vòng đời cache. Viết withCache(df)(consume) cho DataFrame chưa cache, giải phóng k… | [ch25-solutions.md](../solutions/ch25-solutions.md) |
| 26 | 1 | Cơ bản | Phân loại lỗi thành Transient/Permanent/Data/Unknown qua toàn bộ chuỗi cause; test với Spa… | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 26 | 2 | Cơ bản | Backoff lũy thừa có trần và full jitter; không tràn Long ở lần thử 64; xác định được với s… | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 26 | 3 | Trung bình | Từ thư mục manifest, liệt kê các ngày trong khoảng chưa có lần chạy SUCCEEDED (lần lỗi khô… | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 26 | 4 | Trung bình | Kiểm tra khối lượng: cảnh báo khi số dòng hôm nay < X% trung vị các lần chạy trước; chưa đ… | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 26 | 5 | Nâng cao | Kiểm tra tương thích Spark/Scala/Java lúc khởi động; dừng sớm với thông báo rõ. | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 26 | 6 | Nâng cao | So checkpoint và localCheckpoint: số node plan, file trong thư mục checkpoint, kết quả. | [ch26-solutions.md](../solutions/ch26-solutions.md) |
| 27 | 1 | — | Hai đồng hồ. Viết clocks trả hai cửa sổ của một event và thời điểm nhận. Replay dịch proce… | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 27 | 2 | — | Sliding doanh thu. Viết tổng BigInt theo (key,window), width 20/slide 10. Có sự kiện tại b… | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 27 | 3 | — | Watermark nhiều nguồn. Mỗi nguồn gồm (idle, watermark tùy chọn). Bỏ nguồn idle; một nguồn … | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 27 | 4 | — | Bridge session. Tạo preview sau từng prefix đầu vào 0,18,9, gap 10. Hai phiên tạm phải đượ… | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 27 | 5 | — | Audit trạng thái. Đếm đầy đủ bốn quyết định; thu kết quả đã đóng và peak số cửa sổ mở. Thê… | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 27 | 6 | — | Backlog và backpressure. Tính backlog theo hai tốc độ cố định bằng BigInt; với nguồn 1.000… | [ch27-solutions.md](../solutions/ch27-solutions.md) |
| 28 | 1 | — | Schema v2. Thêm decoder cho version 2 có currency; chỉ chấp nhận VND và chuyển vào model h… | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 28 | 2 | — | Lag nhiều partition. Tổng lag bằng BigInt. Chỉ cần một partition chưa có committed offset … | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 28 | 3 | — | Commit frontier. Với một partition, nhận vector (offset, đã_xong) theo thứ tự poll; chỉ tr… | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 28 | 4 | — | Backoff. Tạo dãy nghỉ gấp đôi có cap, không tràn Long. Phân biệt số retry với tổng số atte… | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 28 | 5 | — | Snapshot dedup. Thiết kế state chứa seen + fingerprint + tổng. Mô phỏng khởi động lại từ s… | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 28 | 6 | — | Replay DLT và commit lỗi. Parse raw trong envelope DLT, chỉ trả event khi đã sửa hợp lệ. F… | [ch28-solutions.md](../solutions/ch28-solutions.md) |
| 29 | 1 | — | Đối soát: nhận vector receipts + số source kỳ vọng; trả counts, chặn source trùng, kind lạ… | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 29 | 2 | — | Watermark nhiều partition: lấy min watermark các partition active; một active chưa có mốc … | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 29 | 3 | — | Future event: chấp nhận eventTime <= now + skew, chặn thời gian âm, không tràn Long. Test … | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 29 | 4 | — | Đọc DLT: thu gọn envelope trùng theo provenance. Cùng provenance nhưng nội dung khác phải … | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 29 | 5 | — | Budget queue: tính partitions * queuedEach * maxRecordBytes bằng BigInt, so budget. Giải t… | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 29 | 6 | — | Đóng cửa sổ: với start, width, watermark không âm, trả cửa sổ đã đóng khi end <= watermark… | [ch29-solutions.md](../solutions/ch29-solutions.md) |
| 30 | 1 | — | Frontier: dùng helper của lab cho offsets có lỗ và task hoàn thành lệch thứ tự. Test empty… | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 30 | 2 | — | Backoff: trả maxAttempts-1 khoảng nghỉ gấp đôi, có cap, không tràn Long; maxAttempts 1..10… | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 30 | 3 | — | Ordering: nhận (key, sequence) và phát hiện sequence giảm theo từng key khi input xen kẽ. … | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 30 | 4 | — | Poll budget: tính worst duration từ attempts × thời gian mỗi attempt + waits, rồi nhân số … | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 30 | 5 | — | Lag: tổng end-committed của nhiều partition bằng BigInt. Thiếu committed ở một partition h… | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 30 | 6 | — | Redrive: chỉ nhận receipt DLT, validate raw đã sửa, key khớp orderId; trả Event giữ ID ngh… | [ch30-solutions.md](../solutions/ch30-solutions.md) |
| 31 | 1 | Cơ bản | Asset segment_totals không phân vùng phụ thuộc mọi partition daily_revenue trong một khoản… | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 2 | Cơ bản | provenance(asset, day): truy ngược tới các file raw, bắc cầu qua upstream. | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 3 | Trung bình | Kiểm tra SLA: liệt kê partition thiếu, cũ, hoặc tính đã quá maxAge. | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 4 | Trung bình | Cửa sổ tính lại N ngày; liệt kê partition cũ nằm ngoài cửa sổ thay vì bỏ qua âm thầm. | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 5 | Trung bình | Hợp đồng ở cửa ngõ: cách ly file raw sai header sang raw_quarantine/ trước khi pipeline th… | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 6 | Nâng cao | Replay từ raw + đối chiếu (ý tưởng Kappa): tính lại toàn bộ từ raw trên root mới, so từng partition với serving. | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 31 | 7 | Nâng cao | Dry-run: kế hoạch chạy mà không thực thi, chia "chắc chạy" / "có thể chạy" / skip. | [ch31-solutions.md](../solutions/ch31-solutions.md) |
| 32 | 1 | — | Giữ partitioner: viết doubleValues, kiểm metadata và giá trị; bổ sung input rỗng. Giải thí… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 2 | — | Prepare và join: viết prepare(input,n), persist/materialize rồi self-join với nhánh double… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 3 | — | Combine đúng khi vượt Long: tổng amount theo customer bằng BigInt + reduceByKey. Test Long… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 4 | — | Audit retry: chạy DistributedLab.retry, viết hàm trả business IDs bị append trùng từ journ… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 5 | — | Reliable checkpoint: nhận RDD mới chưa có job, persist, request checkpoint, count, unpersi… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 6 | — | Exact/approx contract: viết cardinality(input,approximate); test trùng/NULL/rỗng. Chạy ngu… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 32 | 7 | — | Throughput/SLO: viết hàm rows/s trả None nếu thời gian 0 ms, từ chối số âm; kiểm batch cuố… | [ch32-solutions.md](../solutions/ch32-solutions.md) |
| 33 | 1 | Cơ bản | Viết check theo tỷ lệ maxShare: vi phạm khi tỷ lệ dòng thỏa điều kiện vượt ngưỡng. Áp cho … | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 2 | Cơ bản | Viết check accepted values cho segment ∈ {Retail, Wholesale, UNKNOWN}; cho thấy nó bắt đượ… | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 3 | Trung bình | Sinh bản ghi hợp lệ từ contract (sampleRecord(contract, i)) và chứng minh mọi bản sinh ra … | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 4 | Trung bình | Đưa đối soát "mọi dòng vào đúng một ngăn" thành một Check[Output] chạy trong production (k… | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 5 | Trung bình | Drift trên tỷ lệ bị loại: tính tỷ lệ rejected/input, cảnh báo khi tăng quá ngưỡng. Mô phỏn… | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 6 | Nâng cao | Property: Incremental.step idempotent theo lô — áp lại cùng lô (replay) không đổi trạng th… | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 33 | 7 | Nâng cao | Backfill theo ngày: bảng daily đang phục vụ được tính trước khi lô 16/10 đến; viết backfil… | [ch33-solutions.md](../solutions/ch33-solutions.md) |
| 34 | 1 | — | Phân phối latency. Viết distribution trả mean/p50/p95/p99 nearest-rank. Test 99 mẫu 1 ms +… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 2 | — | Little's law. Viết requiredConsumers(rate, residenceMs, utilization) dùng mean, ceil và he… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 3 | — | Partition và skew. Viết layout tính partition đầu từ byte và mô phỏng task list trên slot.… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 4 | — | Byte và network cost. Viết wireCost từ tổng payload byte đo, số row đo, số row mục tiêu, e… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 5 | — | Cache break-even. Viết cacheSaving(B,U,R,k) tính kU−(B+kR). B=150/U=90/R=20: hai consumer … | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 6 | — | Chọn cấu hình theo SLO rồi theo chi phí. Viết cheapest từ candidate có node count, fleet t… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
| 34 | 7 | — | Kết hợp giới hạn CPU/memory. Viết feasibleSlots là min của CPU slot và ngân sách memory sa… | [ch34-solutions.md](../solutions/ch34-solutions.md) |
