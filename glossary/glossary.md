# Thuật ngữ Anh – Việt

Cập nhật đến Chương 34 (17/10/2026; mục sau Ch12 được gộp tự động từ mục 4 "Thuật ngữ tiếng Anh" của từng chương). Cột "Chương" cho biết nơi thuật ngữ được giải thích lần đầu.
Quy ước của giáo trình: giữ thuật ngữ tiếng Anh trong code và khi nói chuyện kỹ thuật; dùng giải thích tiếng Việt để hiểu nghĩa.

| Thuật ngữ | Nghĩa tiếng Việt / giải thích | Chương |
|---|---|---|
| 12-factor app | Bộ nguyên tắc cho ứng dụng cloud (cấu hình qua biến môi trường...) | 9 |
| `ConnectionIO`, `Transactor`, `Fragment`, `Meta`, `Read` | Các khái niệm Doobie (mục 3.4) | 15 |
| `Deferred` | Tín hiệu có giá trị một lần, cho phép nhiều fiber chờ | 16B |
| `Expr[T]` / `Quotes` | Biểu thức có kiểu T / ngữ cảnh thao tác code trong macro | 8C |
| `FAILFAST` / `PERMISSIVE` / `DROPMALFORMED` | Chế độ xử lý dòng hỏng khi đọc | 23 |
| `Fiber` | Tác vụ nhẹ có handle để chờ hoặc hủy, do runtime điều phối | 16B |
| `HttpApp[F]` | Route đã "đóng": luôn có response (`orNotFound` thêm 404 cho trường hợp không khớp) | 13 |
| `HttpRoutes[F]` | Route http4s: hàm `Request => OptionT[F, Response]` — có thể "không khớp" | 13 |
| `IO[A]` | Mô tả tác vụ trả A, có thể lỗi hoặc bị hủy; chưa chạy khi chỉ tạo giá trị | 16B |
| `IOApp` | Điểm vào ứng dụng, cấp và quản lý runtime để chạy IO | 16B |
| `Outcome` | Trạng thái kết thúc: Succeeded, Errored hoặc Canceled | 16B |
| `parMapN` / `parTraverse` | Syntax Cats để chạy các IO độc lập đồng thời rồi ghép kết quả | 16B |
| `PushedFilters`, `ReadSchema` | Điều kiện và cột được chuyển xuống bộ đọc file | 23 |
| `Ref` | Ô trạng thái dùng cập nhật nguyên tử; hàm cập nhật phải thuần | 16B |
| `Resource` | Mô tả acquire/release và giữ tài nguyên trong scope use | 16B |
| `Semaphore` | Bộ đếm permit để giới hạn số tác vụ trong một scope đồng thời | 16B |
| `spark-submit`, driver class path | Công cụ gửi job / classpath của tiến trình driver | 26 |
| `TestControl` | Test runtime điều khiển clock/scheduler cho IO, dùng kiểm tra thời gian ảo | 16B |
| `try_cast` | Ép kiểu, hỏng thì NULL thay vì lỗi | 24 |
| `Validated` / `ValidatedNec` | Kiểu kết quả gom lỗi (Applicative); `Nec` = NonEmptyChain | 20 |
| Abstract class | Lớp trừu tượng, không tạo trực tiếp được | 5 |
| Accumulator | Tham số mang kết quả trung gian qua các lời gọi đệ quy | 4 |
| ACID | Atomicity, Consistency, Isolation, Durability | 15 |
| Acquire / release / scope | Mở / đóng / phạm vi sống tài nguyên | 16B |
| Aggregate (sbt) | Gộp lệnh build cho nhiều module | 9 |
| Algebraic Data Type (ADT) | Kiểu dữ liệu đại số: sum type (enum) + product type (case class) | 7 |
| Allocation / live heap / RSS | Byte cấp phát / đối tượng còn sống trong heap / memory cư trú toàn tiến trình | 34 |
| Any / AnyVal / AnyRef | Kiểu gốc / gốc của kiểu giá trị / gốc của kiểu tham chiếu | 3 |
| Applicative vs Monad | `mapN` (độc lập) vs `flatMap` (phụ thuộc) | 20 |
| AQE | Tối ưu kế hoạch theo thống kê trong lúc chạy | 25 |
| AQE (Adaptive Query Execution) | Điều chỉnh kế hoạch lúc chạy theo kích thước thật | 22 |
| Argon2id | Hàm băm mật khẩu tốn bộ nhớ, chống GPU; khuyến nghị đầu tiên của OWASP | 17 |
| Artifact | Gói phát hành (thường là `.jar`) có tọa độ `group:name:version` | 2 |
| Asset, partition, materialization | bảng có định nghĩa / một phần của bảng (một ngày) / một lần tính partition | 31 |
| Associativity | Tính kết hợp: (a·b)·c = a·(b·c) | 8B |
| At-least-once delivery | Mỗi sự kiện được giao **ít nhất** một lần — có thể nhiều lần | 19 |
| At-most / at-least / exactly-once | Nhiều nhất / ít nhất / đúng một lần trong phạm vi đã định nghĩa | 30 |
| Atomic operation | Một thao tác không lộ trạng thái cập nhật dở dang | 14 |
| Atomic replacement | Thay toàn bộ snapshot bằng một thao tác nguyên tử trên filesystem hỗ trợ | 29 |
| Atomic, CAS | Nguyên tử; compare-and-set | 16A |
| Attempt / recomputation | Một lần chạy task / tính lại dữ liệu từ lineage | 25 |
| Authentication / authorization | Xác thực (bạn là ai) / phân quyền (bạn được làm gì) | 12 |
| Backfill | Chạy lại job cho ngày quá khứ | 26 |
| Backfill / replay / rerun | tính lại dữ liệu quá khứ / đưa lại event cũ / chạy lại cùng input | 33 |
| Backfill, reprocessing | tính quá khứ / tính lại vì code đổi | 31 |
| Backpressure | Tốc độ xử lý phía sau kìm tốc độ nhận phía trước | 30 |
| Backpressure / backlog | Điều tiết ngược / lượng việc chưa xử lý | 27 |
| Backward / forward / full compatibility | Reader mới đọc writer cũ / reader cũ đọc writer mới / cả hai | 21 |
| Batch pipeline | Xử lý dữ liệu theo lô, định kỳ | 24 |
| Bearer token | Token trong header `Authorization: Bearer ...` | 12 |
| Binary compatibility | Tương thích nhị phân: dùng được code đã biên dịch mà không cần biên dịch lại | 1 |
| Blocking / non-blocking I/O | I/O chặn thread / không chặn | 16A |
| BOM (Byte Order Mark) | Ký tự U+FEFF ở đầu file văn bản | 10 |
| Boundary case | Trường hợp biên (giá trị ngay tại ngưỡng, rỗng, 0, âm) | 3 |
| Bounded / unbounded | Đầu vào có / chưa biết điểm kết thúc | 27 |
| Breaking / compatible change | thay đổi làm vỡ / không làm vỡ consumer | 33 |
| Broker / KRaft controller | Máy chủ lưu/phục vụ log / thành phần điều phối metadata Kafka | 28 |
| Build side / probe side | Bên lập cấu trúc tra cứu / bên đem dòng đi tra khi hash join | 25 |
| Build tool | Công cụ biên dịch, quản lý dependency, chạy test, đóng gói | 2 |
| Bulkhead / backoff | Giới hạn việc đồng thời / tăng khoảng nghỉ giữa các retry | 18 |
| By-name parameter | Tham số đánh giá trễ (`=> A`), mỗi lần dùng tính lại | 4 |
| Bytecode | Mã trung gian trong file `.class`, do JVM thực thi | 1 |
| Cache / persist / unpersist | Giữ kết quả trong bộ nhớ (và/hoặc đĩa) / giải phóng | 22 |
| Call site / inline expansion | Nơi gọi / bước đưa phần thân inline vào nơi gọi | 8C |
| Cancellation | Hủy một tác vụ đang chạy | 16A |
| Case class | Class dữ liệu bất biến, so sánh theo giá trị | 5 |
| Catalyst, logical/physical plan | Bộ tối ưu của Spark SQL / kế hoạch logic, vật lý | 23 |
| Checkpoint | Mốc phục hồi bao gồm tiến độ và state theo giao thức của hệ thống | 29 |
| Checkpoint / local checkpoint | Ghi ra lưu trữ bền để cắt lineage / cắt lineage, lưu ở executor | 26 |
| Circuit breaker | Cơ chế ngắt tạm lời gọi tới hệ thống đang lỗi | 16A |
| Classpath | Danh sách thư mục/jar JVM dùng để tìm class | 1 |
| Closure | Hàm giữ lại biến từ phạm vi nơi nó được tạo | 4 |
| Closure capture | Trạng thái bên ngoài bị hàm giữ lại, có thể phải serialize | 32 |
| Code size | Kích thước code sinh ra, có thể tăng khi inline nhiều lần | 8C |
| Codec | Cặp encoder + decoder (circe `Codec.AsObject`) | 13 |
| Column expression | Biểu thức trên cột (`col("x") * 2`) — Catalyst hiểu được | 23 |
| Commit frontier | Vị trí sau prefix record đã hoàn tất của một partition | 30 |
| Companion object | `object` cùng tên, cùng file với class | 5 |
| Compare-and-set (CAS) | Ghi giá trị mới nếu giá trị hiện tại vẫn như đã đọc | 12 |
| Composition over inheritance | Ưu tiên ghép đối tượng hơn kế thừa | 5 |
| Composition root | `Main`: ghép các implementation và vòng đời | 18 |
| Concurrency / parallelism | Đồng thời (xen kẽ) / song song (cùng lúc trên nhiều CPU) | 16A |
| Conditional given | Given phụ thuộc given khác | 8B |
| Connection pool | Tập kết nối dùng lại | 15 |
| Constraint (`CHECK`, `UNIQUE`, `NOT NULL`) | Quy tắc database luôn thực thi | 15 |
| Consumer group / rebalance | Nhóm chia partition / phân công lại khi thành viên thay đổi | 28 |
| Context bound | `[A: TC]` — viết gọn của `(using TC[A])` | 8B |
| Contract test | Test hợp đồng HTTP độc lập với cài đặt; chạy được trên nhiều cài đặt | 13 |
| Contravariance | Nghịch biến (`-A`): tiêu thụ A | 8A |
| Cooperative cancellation | Hủy được quan sát tại điểm runtime/API hỗ trợ | 16B |
| CORS, origin, preflight | Chia sẻ tài nguyên khác origin; scheme+host+port; request `OPTIONS` thăm dò | 17 |
| Counterexample | Phản ví dụ | 10 |
| Covariance | Hiệp biến (`+A`): sinh ra A | 8A |
| Credential stuffing | Thử hàng loạt cặp email/mật khẩu bị lộ từ trang khác | 17 |
| Critical section | Vùng găng — đoạn chỉ một thread được vào | 16A |
| Cross-version suffix | Hậu tố `_3`, `_2.13` trong tên artifact Scala | 2 |
| Currying | Tách tham số thành nhiều danh sách tham số | 4 |
| CVE | Mã định danh lỗ hổng bảo mật công khai | 9 |
| DAG | Đồ thị có hướng không chu trình của các bước tính | 22 |
| Data contract | thỏa thuận producer–consumer về schema + ý nghĩa, máy kiểm tra được | 33 |
| Data locality | Mức gần giữa slot chạy task và nơi giữ dữ liệu | 32 |
| Data quality check | kiểm tra trên dữ liệu (thường là trên tập) mỗi lần pipeline chạy | 33 |
| Data quality rule / quality gate | Quy tắc chất lượng / cổng chặn pipeline khi vượt ngưỡng | 20 |
| Data skew | Một vài khóa chiếm phần lớn dữ liệu → một task gánh gần hết việc | 22 |
| Data skew / hot key | Dữ liệu lệch / key tập trung phần lớn dữ liệu hoặc chi phí | 25 |
| DataFrame / Dataset / Encoder | `Dataset[Row]` / dataset typed / bộ chuyển đối tượng ↔ dạng nội bộ của Spark | 23 |
| Deadlock | Các thread chờ khóa của nhau mãi mãi | 11 |
| Decode failure | Request không chuyển được thành input của endpoint (path sai kiểu, body hỏng) | 13 |
| Deduplication | Loại bản trùng | 19 |
| Delivery / effect | Record được giao / tác động nghiệp vụ được áp dụng | 30 |
| Dependency | Thư viện mà project phụ thuộc | 2 |
| Dependency injection | Truyền phụ thuộc từ bên ngoài vào (ví dụ qua constructor) | 5 |
| Dependency inversion | Chính sách phụ thuộc abstraction, adapter thực hiện abstraction ấy | 18 |
| Dependency scope | Phạm vi dependency: Compile, Test, Runtime | 9 |
| Derivation | Compiler tự sinh instance từ cấu trúc kiểu | 8B |
| Directive | Dòng `//> using ...` cấu hình Scala CLI | 2 |
| Disjoint types | Các kiểu không có giá trị chung, cho phép bỏ qua một pattern | 8C |
| Drift / profile | thay đổi phân phối của output / hồ sơ tóm tắt output | 33 |
| Driver / executor / cluster manager | Điều phối / nơi chạy task / nơi cấp tài nguyên | 22 |
| DTO | Data Transfer Object, cấu trúc dữ liệu ở biên giao tiếp | 14 |
| Dynamic partition overwrite | Ghi đè chỉ các partition có trong dữ liệu mới | 24 |
| Eager / lazy | Chạy ngay / chạy khi cần | 16A |
| Effect / effect system | Hiệu ứng / cách mô tả và ghép tương tác ngoài hàm thuần | 16B |
| Effective bandwidth | Tốc độ byte thực dùng sau các overhead và tranh chấp | 34 |
| Either / Left / Right | Lỗi hoặc kết quả | 7 |
| Ember | Server/client HTTP của http4s, viết trên fs2 | 13 |
| Encapsulation | Đóng gói trạng thái và chi tiết cài đặt | 5 |
| Endpoint description | Mô tả endpoint dưới dạng giá trị (Tapir): input, output, error output | 13 |
| Entity encoder/decoder | Type class của http4s chuyển giữa body HTTP và kiểu Scala | 13 |
| Enum | Kiểu liệt kê; có thể mang tham số | 5 |
| Eta-expansion | Tự động chuyển method thành function value | 4 |
| Event sourcing | Trạng thái = gộp chuỗi sự kiện | 7 |
| Event time / processing time | Thời điểm nghiệp vụ / thời điểm bộ xử lý thực hiện | 27 |
| ExecutionContext | Nơi (thread pool) mà Future chạy | 16A |
| Exhaustiveness checking | Compiler kiểm tra `match` đủ trường hợp | 5 |
| Exit code | Mã kết thúc tiến trình | 9 |
| Exponential backoff, jitter | Chờ tăng theo lũy thừa; thêm ngẫu nhiên | 16A |
| Expression | Biểu thức — luôn tạo ra một giá trị | 3 |
| Extension method | Method bổ sung cho kiểu có sẵn | 8A |
| Fail-fast / Error accumulation | Dừng ở lỗi đầu / thu thập mọi lỗi | 7 |
| Fallback / backoff / jitter | Phương án thay thế / lịch chờ tăng / lệch ngẫu nhiên | 16B |
| Fat (uber) jar, `Provided` | Jar gồm dependency / phụ thuộc do môi trường chạy cung cấp | 26 |
| Fencing | Ngăn writer/producer cũ tiếp tục ghi sau khi mất quyền | 30 |
| Finalizer / masking / poll | Dọn dẹp / che hủy / khôi phục khả năng hủy theo scope | 16B |
| Fingerprint | Nội dung chuẩn hóa để phân biệt trùng với xung đột ID | 29 |
| First-class function | Hàm là giá trị: gán, truyền, trả về được | 4 |
| Fixture | Tài nguyên được tạo/dọn cho test | 10 |
| Flaky test | Test lúc pass lúc fail | 10 |
| Floating point | Số dấu phẩy động (`Float`, `Double`) | 3 |
| Fold | Gộp collection thành một giá trị | 6 |
| For-comprehension | Cú pháp `for ... yield` cho `flatMap`/`map` | 7 |
| Framework vs library | Framework gọi code của bạn theo khuôn của nó (Play); library được code của bạn gọi (http4s, Tapir) | 13 |
| Freshness | dữ liệu mới nhất không quá cũ so với thời điểm kiểm tra | 33 |
| Function composition | Ghép hàm (`andThen`, `compose`) | 4 |
| Functional core, imperative shell | Lõi thuần, vỏ có side effect | 7 |
| Garbage collection (GC) pause | Khoảng dừng ứng dụng để thu gom rác | 11 |
| Garbage Collector (GC) | Bộ thu gom rác: tự giải phóng bộ nhớ không còn dùng | 1 |
| Generator | Bộ sinh dữ liệu test | 10 |
| Given / using | Khai báo / yêu cầu tham số ngữ cảnh | 8B |
| Golden dataset | Bộ dữ liệu + kết quả mong đợi cố định để so sánh | 19 |
| Golden dataset / snapshot test | output đã được duyệt, lưu lại để so | 33 |
| Golden test | So output với kết quả đã được duyệt | 10 |
| Graceful shutdown | Dừng có trật tự, hoàn tất việc đang làm | 12 |
| Guard | Điều kiện `if` gắn vào một `case` | 3 |
| Hashing / salt / pepper | Băm một chiều / chuỗi ngẫu nhiên riêng mỗi mật khẩu / bí mật chung giữ ngoài DB | 17 |
| Head-of-line blocking | Một record chậm chặn các record phía sau | 30 |
| Headroom / utilization | Dự phòng / tỷ lệ sử dụng theo mô hình | 34 |
| Heap / Stack | Vùng nhớ đối tượng / vùng nhớ lời gọi hàm của thread | 11 |
| Higher-order function (HOF) | Hàm nhận hoặc trả về hàm | 4 |
| HMAC / HS256 | Chữ ký bằng khóa bí mật chung; HMAC với SHA-256 | 17 |
| Idempotence / transaction | Lặp không thêm tác động / nhóm thay đổi được hoàn tất như một đơn vị | 28 |
| Idempotency | Lặp lại cùng lệnh không đổi kết quả nghiệp vụ sau lần đầu | 14 |
| Idempotency, rerun safety | Chạy nhiều lần = chạy một lần / chạy lại an toàn | 24 |
| Idempotent | Chạy nhiều lần cho cùng kết quả như chạy một lần | 7 |
| Idempotent (HTTP method) | Gọi nhiều lần cho cùng trạng thái như một lần | 12 |
| Idempotent / fingerprint | Lặp không thêm tác động / nội dung dùng phân biệt gửi lại và xung đột | 27 |
| IDOR, Broken Access Control | Truy cập trực tiếp đối tượng không kiểm tra quyền | 17 |
| Immutable / Mutable | Bất biến / khả biến | 3 |
| Implicit scope | Nơi compiler tự tìm given (companion của các kiểu liên quan) | 8B |
| Impression / Click / CTR | Lượt hiển thị / lượt bấm / tỷ lệ bấm = click ÷ impression | 19 |
| Index | Cấu trúc phụ (B-tree mặc định) giúp tìm dòng không cần quét toàn bảng | 15 |
| Ingestion | đưa dữ liệu từ nguồn vào nền tảng | 31 |
| Interpreter | Thành phần biến mô tả endpoint thành route server / tài liệu / client | 13 |
| Intersection type | Kiểu giao `A & B` | 8A |
| Invalidation / TTL | Làm cache mất hiệu lực / thời gian tối đa giữ entry | 18 |
| InvalidTransition | 409 Problem | 14 |
| Invariance | Bất biến về variance: không có quan hệ kiểu con | 8A |
| Invariant | Điều kiện luôn đúng với mọi đối tượng hợp lệ | 5 |
| Isolation level | Mức cô lập: READ COMMITTED (mặc định PostgreSQL), REPEATABLE READ, SERIALIZABLE | 15 |
| ISR / acks | Tập replica đang theo kịp / mức xác nhận producer yêu cầu | 28 |
| JFR (Java Flight Recorder) | Công cụ ghi sự kiện hiệu năng có sẵn trong JDK | 11 |
| JIT compiler | Biên dịch tức thời bytecode "nóng" sang mã máy | 1 |
| Job / stage / task | Việc do một action sinh ra / đoạn giữa hai shuffle / việc trên một partition | 22 |
| JSON Lines (`.jsonl`) | Mỗi dòng một JSON — đọc/ghi từng dòng, nối thêm dễ, hỏng một dòng không hỏng cả file | 19 |
| JVM | Máy ảo Java | 1 |
| JWT, claim, `iss`/`sub`/`exp`/`iat` | Token JSON có chữ ký; trường trong payload: người phát hành / chủ thể / hết hạn / thời điểm phát hành | 17 |
| Keyset / cursor pagination | Phân trang theo giá trị khóa cuối trang trước | 15 |
| Lag / retention / tombstone | Khoảng chậm offset / chính sách lưu / null value biểu diễn xóa theo hợp đồng | 28 |
| Lambda / anonymous function | Hàm không tên | 4 |
| Lambda / Kappa architecture | hai đường batch + stream / một đường, tính lại bằng replay | 31 |
| Late event, backfill | Sự kiện đến muộn / chạy lại cho dữ liệu quá khứ | 24 |
| Layer / module | Tầng trách nhiệm / đơn vị compiler-build; không đồng nghĩa tiến trình | 18 |
| Lazy / eager | Trì hoãn thân / khởi động khi tạo | 16B |
| Lazy evaluation | Trì hoãn thực thi đến khi cần kết quả | 22 |
| Lazy evaluation / view | Đánh giá khi cần | 6 |
| Left semi / left anti join | Giữ dòng trái có / không có cặp bên phải, không thêm cột | 23 |
| Lineage | Chuỗi transformation tạo ra một dataset — dùng để tính lại | 22 |
| Lineage / checkpoint | Cách tính lại / dữ liệu materialize để cắt chuỗi phụ thuộc | 32 |
| Lineage, provenance, impact analysis | quan hệ input–output / truy ngược nguồn / truy xuôi ảnh hưởng | 31 |
| Little's law | Quan hệ trung bình L=λW với điều kiện và ranh giới nhất quán | 34 |
| Liveness / readiness probe | Kiểm tra còn sống / sẵn sàng nhận traffic | 17 |
| Local mode | Driver + executor trong một JVM | 22 |
| Logical type | Ý nghĩa bổ sung cho kiểu vật lý, ví dụ long là timestamp-millis | 21 |
| Lost update | Lần ghi sau đè mất lần ghi trước do cả hai đọc cùng giá trị cũ | 15 |
| LTS | Dòng phiên bản hỗ trợ dài hạn | 1 |
| Map-side combine | Tổng hợp trước tại chỗ, trước khi gửi qua mạng | 20 |
| Match type / reduction | Kiểu chọn theo pattern / rút gọn về kiểu kết quả | 8C |
| Materialization / break-even | Thực sự tạo kết quả cache / điểm tiết kiệm vượt chi phí tạo | 34 |
| Materialization / eviction | Tạo dữ liệu thật từ mô tả / loại block khỏi cache | 25 |
| Maven Central | Kho artifact công khai của hệ sinh thái JVM | 2 |
| Medallion (bronze/silver/gold) | một cách đặt tên ba lớp | 31 |
| Metaprogramming | Viết chương trình phân tích/sinh chương trình khác | 8C |
| Method | Hàm khai báo bằng `def` | 4 |
| Middleware | Lớp bọc handler thêm hành vi xuyên suốt | 12 |
| Migration | Một bước thay đổi schema có version | 15 |
| Milestone (M), Release Candidate (RC) | Bản chưa ổn định; không dùng cho production | 13 |
| Mirror | Thông tin cấu trúc kiểu do compiler cung cấp cho derivation | 8C |
| MissingOrder | 404 Problem | 14 |
| Mixin | Trộn hành vi của trait vào class | 5 |
| Monoid / Semigroup | Phép gộp kết hợp có / không có phần tử trung hòa | 8B |
| Monoid, associativity, identity | Cấu trúc gộp được; tính kết hợp; phần tử trung hòa | 20 |
| Multi-module build | Build gồm nhiều module | 9 |
| Mutation testing | Cố ý cài lỗi để kiểm tra bộ test có phát hiện không | 19 |
| Narrow / wide dependency | Mỗi partition cha góp vào tối đa một con (không phân tán lại) / bản ghi của một cha bị chia ra nhiều con theo khóa (shuffle) | 22 |
| Negative compilation test | Test xác nhận một đoạn code phải bị compiler từ chối | 8C |
| Node-hour / cost per million | Số node nhân giờ cấp phát / chi phí trên một triệu event | 34 |
| Normalization | Chuẩn hóa về một dạng (mã chữ hoa, ISO country code, Instant) | 19 |
| Nothing | Kiểu con của mọi kiểu; kiểu của `throw` | 3 |
| Null | Kiểu của `null`; tránh dùng trong code Scala | 3 |
| Nullable | Kiểu cho phép một giá trị null; khác với có default | 21 |
| Observability | Suy ra trạng thái bên trong từ log, metrics, trace | 18 |
| Opaque type | Kiểu mới chỉ tồn tại lúc biên dịch | 8A |
| Open-loop / coordinated omission | Nguồn theo lịch độc lập / bỏ sót latency khi nguồn đợi consumer rảnh | 34 |
| OpenAPI | Chuẩn mô tả REST API dạng YAML/JSON (bản 3.1 trong lab) | 13 |
| Optimistic concurrency | Kiểm tra version khi ghi để phát hiện cập nhật cạnh tranh | 14 |
| Option / Some / None | Giá trị có thể vắng mặt | 7 |
| Orchestration vs processing | quyết định chạy gì khi nào / tính dữ liệu như thế nào | 31 |
| Orphan instance | Instance đặt ngoài companion của kiểu và của type class | 8B |
| Orphan record | Bản ghi tham chiếu tới thứ không tồn tại | 19 |
| Out-of-order / late | Đến sai thứ tự / đã nằm trước watermark theo chính sách | 27 |
| Out-of-order events | Sự kiện đến không theo thứ tự thời gian | 19 |
| Outbox / DLT | Bản ghi chờ phát bền vững / topic cách ly lỗi | 29 |
| Overflow | Tràn số | 3 |
| Pagination / keyset | Chia trang / chia trang dựa trên khóa sau cùng | 14 |
| Parallelism / concurrency | Công việc chạy đồng thời / công việc đang ở trong hệ thống | 34 |
| Parameter untupling | Lambda nhận trực tiếp các phần tử của tuple làm tham số (Scala 3) | 3 |
| Parameterized query | Truy vấn có tham số gắn riêng (`?`), không ghép chuỗi | 15 |
| Parsing / decoding | Đọc dữ liệu trên dây thành giá trị có kiểu | 14 |
| Partial application | Cố định một phần tham số để tạo hàm mới | 4 |
| Partial function | Hàm chỉ định nghĩa cho một phần đầu vào (`{ case ... }`) | 6 |
| Partition | Một phần dữ liệu, đơn vị song song | 22 |
| Partition / partitioner | Phần dữ liệu / hợp đồng ánh xạ key → phần dữ liệu | 32 |
| Partition discovery | Spark suy cột từ tên thư mục `cột=giá trị` | 24 |
| Partition pruning, `PartitionFilters` | Chỉ đọc thư mục partition thỏa điều kiện | 24 |
| Partition sizing | Chọn quy mô công việc mỗi partition | 25 |
| Percentile | Bách phân vị (p50, p95, p99) | 6 |
| Poison / transient / DLT | Dữ liệu không xử lý được / lỗi tạm / topic giữ lỗi | 28 |
| Polymorphism | Đa hình | 5 |
| Port / adapter | Interface do core định nghĩa / cài đặt ở biên | 9 |
| Position / committed offset / end offset | Vị trí đọc phiên hiện tại / vị trí phục hồi đã lưu / vị trí sau cuối log | 28 |
| Precision / scale | Tổng số chữ số / số chữ số ở phần thập phân | 21 |
| PreconditionFailed | 412 Problem, yêu cầu đọc lại representation | 14 |
| Predicate pushdown, column pruning | Đẩy điều kiện về sát nguồn / chỉ đọc cột cần | 23 |
| Primary constructor | Constructor khai báo sau tên class | 5 |
| Primary key (PK) / Foreign key (FK) | Khóa chính định danh dòng / khóa ngoại tham chiếu khóa chính bảng khác | 15 |
| Problem Details (RFC 9457) | Định dạng lỗi chuẩn cho HTTP API | 12 |
| Product / sum | Cấu trúc gồm các trường / lựa chọn giữa các nhánh | 8C |
| Projection | Chỉ yêu cầu những trường/cột cần dùng ở bước đọc | 21 |
| Promise | Đối tượng hoàn thành Future bằng tay | 16A |
| Property-based test | Test kiểm tra một quan hệ luôn đúng trên nhiều đầu vào | 19 |
| Property-based testing | Kiểm tra thuộc tính trên input sinh ngẫu nhiên | 10 |
| Property-based testing, generator, shrinking | Test thuộc tính; bộ sinh dữ liệu; thu nhỏ phản ví dụ | 20 |
| Provenance / receipt | Định danh nguồn / biên nhận quyết định đã bền | 29 |
| Pure function | Hàm thuần | 7 |
| Pure transformation | Biến đổi thuần: không side effect, cùng vào → cùng ra | 20 |
| Pushgateway, textfile collector | Cách đưa metric của job ngắn hạn vào Prometheus | 26 |
| Quarantine | Nơi lưu record bị loại cùng vị trí, nguyên nhân và raw để điều tra | 21 |
| Query plan / operator | Kế hoạch truy vấn / bước thực thi trong kế hoạch | 25 |
| Query plan / planner | Kế hoạch thực thi / thành phần chọn kế hoạch | 15 |
| Quote / splice / staging | Dựng code / chèn code / tách các giai đoạn compiler và runtime | 8C |
| Race condition, lost update | Tranh chấp; mất cập nhật | 16A |
| Rate limiting, token bucket | Giới hạn tần suất; thuật toán xô token | 17 |
| Raw / staging / clean | Dữ liệu thô / đang xử lý / đã kiểm tra | 19 |
| Raw / staging / serving layer | Lớp dữ liệu thô / trung gian / phục vụ người đọc | 24 |
| Reassignment | Gán lại giá trị cho biến | 3 |
| Reconciliation | Đối soát số dòng/tổng giữa các bước | 24 |
| Reconciliation / replay | Đối soát mọi đầu vào / đọc lại từ nguồn | 29 |
| Redaction | Che dữ liệu nhạy cảm trong log | 17 |
| Referential integrity | Toàn vẹn tham chiếu: khóa ngoại phải tồn tại | 20 |
| Referential transparency | Thay biểu thức bằng giá trị không đổi nghĩa chương trình | 7 |
| Regression test | Test chống lỗi cũ quay lại | 10 |
| Rejected / quarantine | Bản ghi bị loại / bị cách ly chờ xử lý | 24 |
| REPL | Môi trường gõ – chạy – in kết quả tương tác | 2 |
| Replay / backfill / redrive | Đọc lại / tính bổ sung lịch sử / đưa lại record đã cách ly | 30 |
| Repository | Cổng lưu trữ có hợp đồng, che cách lưu dữ liệu | 14 |
| Request id | Mã định danh request để truy vết log | 12 |
| Resource sizing | Chọn ngân sách CPU, memory, slot và số node theo workload/SLO | 34 |
| Round-trip | Ghi rồi đọc lại, kiểm tra giá trị và ý nghĩa được giữ nguyên | 21 |
| Rounding mode | Quy tắc làm tròn (`HALF_UP`, `HALF_EVEN`...) | 3 |
| Route / routing | Ánh xạ (method, path) → handler | 13 |
| Routing | Chọn endpoint bằng method/path | 14 |
| Row group / column chunk / page | Nhóm hàng / phần dữ liệu của một cột trong nhóm / đơn vị page của Parquet | 21 |
| Row lock (`FOR UPDATE`) | Khóa dòng đã đọc đến hết transaction | 15 |
| Run ID, run manifest | Định danh một lần chạy / bản ghi mô tả lần chạy | 26 |
| Runtime / fiber | Bộ thực thi / tác vụ nhẹ do runtime điều phối | 16B |
| Salting | Thêm khóa phụ để chia tải của key | 25 |
| SAM type | Trait/interface có đúng một method abstract, tạo được bằng lambda | 8A |
| Schema (Tapir) | Mô tả cấu trúc kiểu dữ liệu để sinh OpenAPI và validate | 13 |
| Schema evolution | Thay đổi hợp đồng dữ liệu khi nhiều phiên bản còn cùng tồn tại | 21 |
| Schema inference | Spark đọc dữ liệu để đoán kiểu | 23 |
| Schema, `StructType`/`StructField` | Mô tả cột: tên, kiểu, nullable | 23 |
| Seam | Điểm cho phép thay phụ thuộc khi test | 10 |
| Secret manager | Dịch vụ lưu và cấp phát bí mật | 17 |
| Seq Scan / Index Scan / Bitmap Heap Scan | Quét tuần tự / theo index / dùng bitmap từ index rồi đọc heap | 15 |
| Serialization / Encoder | Chuyển object thành byte / chuyển kiểu Dataset vào biểu diễn Spark SQL | 25 |
| Serialization / encoding | Biến giá trị thành JSON/bytes trả client | 14 |
| Serialization failure (SQLSTATE 40001) | PostgreSQL hủy transaction để giữ isolation; ứng dụng cần retry | 15 |
| Serializer / schema evolution | Chuyển giá trị sang byte / thay đổi hợp đồng dữ liệu qua phiên bản | 28 |
| Server logic | Hàm nghiệp vụ gắn vào endpoint: `Input => F[Either[Error, Output]]` | 13 |
| Service layer | Tầng validation và điều phối use case | 14 |
| Service time / residence time | Thời gian phục vụ / thời gian ở trong ranh giới đã chọn, có thể gồm chờ | 34 |
| Sessionization | Gom các hoạt động của một người thành "phiên" theo khoảng nghỉ | 19 |
| Shrinking | Thu nhỏ phản ví dụ | 10 |
| Shuffle | Chuyển dữ liệu giữa các partition theo khóa | 20 |
| Shuffle / `Exchange` | Trao đổi dữ liệu theo khóa giữa các partition / toán tử trong plan — `Exchange hashpartitioning` là shuffle, `BroadcastExchange` thì không | 22 |
| Shuffle block / fetch | Phần map output cho một reduce partition / thao tác lấy block | 32 |
| Side effect | Tác dụng phụ: in, ghi file, gửi request, đổi trạng thái bên ngoài | 3 |
| Singleton object | Đối tượng duy nhất, khai báo bằng `object` | 3 |
| Singleton type | Kiểu biểu diễn một giá trị cụ thể, như `4` | 8C |
| Skew / straggler | Lệch phân bố công việc / task chậm kéo đuôi | 34 |
| Small files problem, compaction | Quá nhiều file nhỏ / gộp file nhỏ thành file lớn | 24 |
| Smart constructor | Factory kiểm tra đầu vào trước khi tạo đối tượng | 5 |
| Snapshot | Ảnh chụp trạng thái tại một thời điểm đọc | 14 |
| Snapshot (input) | bản đọc một lần của mọi input; hash ghi vào lineage và phép tính dùng cùng các byte đó | 31 |
| Spill / GC | Đẩy dữ liệu tạm xuống đĩa / thu gom rác JVM | 25 |
| SQL injection | Chèn mã SQL qua dữ liệu đầu vào | 15 |
| Stable sort | Sắp xếp giữ thứ tự của phần tử bằng nhau | 6 |
| Stack frame / Stack overflow | Khung bộ nhớ của một lời gọi hàm / tràn stack | 4 |
| Stage boundary | Ranh giới thực thi do shuffle dependency; không phải mỗi transformation | 32 |
| Stale / fresh | partition cũ (input hoặc code đã đổi) / còn đúng — phân biệt "khớp cục bộ" (input trực tiếp) với "fresh tận nguồn" (cả upstream) | 31 |
| State | Thông tin tích lũy qua các phần tử (đã thấy id nào, đếm bao nhiêu) | 19 |
| State / checkpoint / replay | Trạng thái nhớ / ảnh phục hồi / phát lại dữ liệu | 27 |
| Stateless (HTTP) | Server không giữ trạng thái phiên giữa các request | 12 |
| Statement | Câu lệnh — chỉ thực hiện hành động | 3 |
| Statically typed | Kiểu được kiểm tra lúc biên dịch | 1 |
| Status code | Mã trạng thái HTTP | 12 |
| Storage result | HTTP khi tích hợp | 14 |
| Straggler | Task về cuối do chạy lâu; cần điều tra trước khi quy cho skew | 25 |
| Streaming (fs2 `Stream`) | Xử lý từng phần tử/khối khi đọc, không nạp toàn bộ | 19 |
| Strict equality | Chế độ chỉ cho `==` giữa các kiểu được phép (`CanEqual`) | 8B |
| String interpolation | Chèn giá trị vào chuỗi (`s`, `f`, `raw`) | 3 |
| Structural sharing | Chia sẻ dữ liệu chung giữa phiên bản cũ và mới của collection | 6 |
| Structured logging | Log dạng dữ liệu có field | 9 |
| Table format | lớp metadata trên file (Delta, Iceberg) cho commit nguyên tử, time travel | 31 |
| Tail latency, p95/p99 | Phân vị ở đuôi; định nghĩa nearest-rank trong lab | 34 |
| Tail recursion | Đệ quy đuôi: lời gọi đệ quy là thao tác cuối cùng | 4 |
| Task attempt / stage attempt | Một lần thử task / một lần thử stage; hai bộ đếm khác nhau | 32 |
| TASTy | Định dạng lưu cây cú pháp có kiểu của Scala 3 | 1 |
| Test double (stub, fake, spy, mock) | Đối tượng thay thế trong test | 10 |
| Thread pool, ExecutionContext | Nhóm thread tái sử dụng; nơi Future chạy | 16A |
| Thread starvation | Thread pool cạn vì các thread chờ lẫn nhau | 16A |
| Three-valued logic, null-safe equality (`<=>`) | Logic true/false/NULL / so sánh coi NULL bằng NULL | 23 |
| Throughput / capacity | Tốc độ hoàn tất quan sát / mức tối đa theo điều kiện đo | 34 |
| Throughput / latency | Khối lượng trên đơn vị thời gian / thời gian chờ kết quả theo định nghĩa cụ thể | 32 |
| Thành công version 1 | 200 + `ETag: "1"` | 14 |
| Timeout, cancellation | Hết thời gian chờ, hủy tác vụ | 16A |
| TLS, HSTS | Giao thức mã hóa của HTTPS; header buộc trình duyệt dùng HTTPS | 17 |
| Topic / partition / offset | Luồng logic / một log con có thứ tự / vị trí trong log con | 28 |
| Trait | Hợp đồng hành vi, có thể có cài đặt | 5 |
| Transaction | Nhóm thao tác chạy nguyên tử | 15 |
| Transaction boundary | Một use case được `transact` đúng một lần | 18 |
| Transformation / action | Mô tả dữ liệu mới (lazy) / bắt Spark chạy | 22 |
| Transient / permanent failure | Lỗi tạm thời / vĩnh viễn | 26 |
| Transitive dependency | Phụ thuộc kéo theo | 9 |
| Try / Success / Failure | Kết quả của code có thể ném exception | 7 |
| Tumbling / sliding / session | Cửa sổ không chồng / chồng theo bước / theo khoảng im lặng | 27 |
| Tuple cons `*:` | Thêm kiểu/phần tử đầu vào một tuple đuôi | 8C |
| Type alias | Tên khác cho một kiểu | 8A |
| Type annotation | Ghi kiểu tường minh | 3 |
| Type bound | Ràng buộc kiểu: upper `<:`, lower `>:` | 8A |
| Type class | Hành vi generic + instance cho từng kiểu | 8B |
| Type erasure | JVM xóa tham số kiểu lúc chạy | 8A |
| Type inference | Compiler tự suy luận kiểu | 3 |
| Type parameter | Tham số kiểu (`A` trong `Page[A]`) | 8A |
| Typeclass derivation | Sinh instance của type class theo cấu trúc kiểu | 8C |
| UDF (user-defined function) | Hàm tự viết — hộp đen với optimizer | 23 |
| Union type | Kiểu hợp `A \| B` (Scala 3) | 3 |
| Uniqueness / not null / accepted values | khóa không trùng / trường không rỗng / giá trị thuộc tập cho phép | 33 |
| Unit | Kiểu "không có giá trị có ý nghĩa", giá trị duy nhất `()` | 3 |
| User enumeration | Dò xem email nào đã đăng ký qua khác biệt trong response | 17 |
| Virtual time | Clock do test runtime điều khiển | 16B |
| Warm-up / JIT / DCE | Chạy làm nóng / biên dịch tức thời / loại bỏ tính toán không được dùng | 34 |
| Watermark / allowed lateness | Dấu tiến độ event time / khoảng cho phép cập nhật muộn bổ sung, lab không bật | 27 |
| Watermark / lateness | Mốc tiến độ event time / độ trễ sự kiện theo policy | 29 |
| Whitelist | Tập lựa chọn được phép, mặc định từ chối phần còn lại | 14 |
| Whole-stage codegen (`*(1)`) | Sinh một đoạn mã cho nhiều toán tử liên tiếp | 22 |
| Window function, frame | Hàm trên cửa sổ dòng liên quan / khung dòng của cửa sổ | 23 |
| Wire format | Cách byte được biểu diễn để truyền/lưu ngoài process | 21 |
| Writer / reader schema | Schema dùng lúc ghi / schema consumer yêu cầu khi đọc | 21 |
| Write–audit–publish | Ghi vào staging, kiểm tra, rồi mới công bố | 24 |
