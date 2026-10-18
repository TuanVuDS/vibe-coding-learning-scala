# Giải đáp các câu hỏi định hướng

Tài liệu này trả lời 6 câu hỏi mà người học (và người thiết kế giáo trình) cần rõ ngay từ đầu.
Mọi khẳng định về phiên bản đều dựa trên [04-compatibility-matrix.md](04-compatibility-matrix.md) (kiểm chứng 14/10/2026).

---

## 1. Scala 3 core và Scala dùng trong production khác nhau như thế nào?

**Scala 3 core** là ngôn ngữ và standard library: cú pháp, hệ thống kiểu, collections,
`Option/Either/Try`, `Future`. Đây là phần **ổn định nhất**, được bảo đảm tương thích theo chính sách LTS.

**Scala trong production** = Scala core + những thứ sau, và chính những thứ này mới quyết định độ khó:

| Khía cạnh | Thực tế production |
|---|---|
| Phiên bản Scala | Bị ràng buộc bởi thư viện. Ví dụ: job Spark **buộc** dùng Scala 2.13 dù team thích Scala 3. |
| Hệ sinh thái | Mỗi team chọn một "phong cách": Typelevel (Cats Effect), ZIO, Play/Pekko, hoặc "Java++" (Scala gọi thư viện Java). Code giữa các phong cách khác nhau rất nhiều. |
| Build | sbt multi-module, plugin, publish artifact, cross-build 2.13/3. |
| Vận hành | JVM tuning, GC, đóng gói Docker, log, metrics, graceful shutdown. |
| Code cũ | Nhiều codebase vẫn ở Scala 2.13 — cần đọc được cả `implicit` lẫn `given`. |
| Tương thích nhị phân | Thư viện Scala 3 publish bằng 3.9 không dùng được từ project 3.3. Chọn phiên bản ảnh hưởng tới người dùng thư viện của bạn. |

**Hệ quả cho giáo trình:** học Scala 3 cho tư duy và nền tảng, nhưng dành riêng thời gian
(Ch22) để đọc/viết Scala 2.13, vì phần lớn công việc Spark diễn ra ở đó.

---

## 2. Scala 3 có thể được sử dụng ở đâu trong Backend Engineering?

**Gần như toàn bộ.** Các thành phần chính của stack backend đều có artifact Scala 3:

- HTTP server/client: http4s 0.23, Tapir 1.13 (cũng có Play, ZIO HTTP — sẽ kiểm chứng ở Ch13).
- Effect system: Cats Effect 3.7, ZIO 2.
- Database: Doobie 1.0.0-RC13 (lưu ý: vẫn là RC), thư viện JDBC/HikariCP/Flyway là Java nên dùng trực tiếp.
- JSON: circe, jsoniter-scala.
- Kafka: kafka-clients (Java) dùng trực tiếp; fs2-kafka có artifact Scala 3.
- Actor/streaming: Apache Pekko hỗ trợ Scala 3.

Điểm mạnh riêng của Scala 3 cho backend: `enum` và ADT cho domain model, `opaque type`
cho ID/Money không tốn chi phí runtime, `given/using` và `derives` giúp sinh JSON codec gọn hơn Scala 2.

Điểm cần cẩn trọng: một số thư viện quan trọng vẫn ở trạng thái RC (Doobie) hoặc đang đổi groupId
(Doobie, fs2-kafka chuyển sang `org.typelevel`) — ví dụ trên Internet có thể dùng tọa độ cũ.

---

## 3. Các lựa chọn Spark/Scala có giới hạn tương thích gì?

| Spark | Scala được hỗ trợ | Java |
|---|---|---|
| 3.5.x | 2.12, 2.13 | 8 / 11 / 17 |
| 4.0.x – 4.1.x | **chỉ 2.13** | 17 / 21 |
| Scala 3 | **Không có bản Spark chính thức nào** (epic SPARK-54150 đang mở) | — |

Hệ quả:

1. Job Spark viết bằng Scala phải dùng **Scala 2.13** với Spark 4.x.
2. Không đưa thư viện `_3` vào classpath của job Spark.
3. Các phương án thực tế để vẫn tận dụng Scala 3 trong hệ thống có Spark:
   - **Tách theo ranh giới dữ liệu (khuyến nghị):** service/consumer viết Scala 3; job Spark viết Scala 2.13;
     giao tiếp qua Parquet/Avro/Kafka/bảng. Đây là cách giáo trình dùng.
   - **Chia sẻ logic thuần qua cross-build:** module `core` chỉ dùng standard library,
     cross-build cho 2.13 và 3 (dùng cú pháp chung). Phù hợp với rule validate, parse.
   - **Spark Connect:** client gửi kế hoạch truy vấn tới server Spark; giảm ràng buộc classpath phía client
     nhưng hiện chưa phải con đường Scala 3 chính thức — chủ đề [CS], cần kiểm chứng khi dùng.
   - **Thư viện cộng đồng chạy Spark bằng Scala 3:** không chính thức, không dùng trong đường học chính.

---

## 4. Khi nào nên dùng Scala collections và khi nào nên dùng Spark?

| Câu hỏi | Scala collections | Spark |
|---|---|---|
| Dữ liệu có vừa bộ nhớ một máy (và còn dư cho xử lý)? | Có → dùng collections | Không → cân nhắc Spark |
| Độ trễ khởi động chấp nhận được? | Mili-giây | Vài giây đến vài chục giây cho mỗi job |
| Chi phí vận hành | Một tiến trình JVM | Cluster (hoặc managed service), cấu hình, monitoring |
| Đánh giá | Eager (trừ `view`, `LazyList`, `Iterator`) | Lazy, tối ưu bởi Catalyst |
| Lỗi | Exception tại chỗ | Lỗi có thể xảy ra trên executor, khi action được gọi |
| Test | Rất nhanh | Cần SparkSession local, chậm hơn |

Ngưỡng thực tế: nếu dữ liệu chỉ vài trăm MB đến vài GB, một chương trình Scala (thậm chí
dùng streaming bằng `Iterator`/fs2 để không nạp hết vào RAM) thường đơn giản và rẻ hơn Spark.
Khi dữ liệu lên hàng chục GB trở lên, cần join các bảng lớn, hoặc tổ chức đã có hạ tầng Spark — dùng Spark.

**Cảnh báo bắt buộc:** code `List.map/filter/groupBy` trông giống DataFrame API nhưng
**không có cùng đặc tính hiệu năng**. `groupBy` trên `List` là thao tác trong bộ nhớ một máy;
`groupBy` trên DataFrame gây **shuffle** qua mạng. Ch20 dạy viết transformation thuần để
logic nghiệp vụ dùng lại được, nhưng việc chuyển sang Spark luôn cần thiết kế lại phần thực thi.

---

## 5. Nên học concurrency và effect systems ở thời điểm nào?

- **Không** học trong Giai đoạn 1. Người mới học IO/ZIO cùng lúc với cú pháp Scala thường
  không phân biệt được đâu là ngôn ngữ, đâu là thư viện.
- **Ch16A (concurrency nền tảng)** sau Ch11 (JVM: thread, heap) và Ch12 (backend: vì sao cần
  xử lý nhiều request đồng thời). Dùng `Future` của standard library vì đây là **tính năng của
  standard library**, không cần thư viện ngoài.
- **Ch16B (Cats Effect)** ngay sau đó, trước khi chọn framework (Ch13). Lúc này người học đã gặp các
  vấn đề của `Future` (chạy ngay khi tạo, khó hủy, khó quản lý resource) nên hiểu vì sao cần IO.

Phân biệt rõ trong giáo trình:

| Loại | Ví dụ |
|---|---|
| Tính năng ngôn ngữ/JVM | `Thread`, `synchronized`, `@volatile` |
| Standard library Scala | `Future`, `Promise`, `ExecutionContext` |
| Thư viện ngoài | `cats.effect.IO`, `Resource`, `Fiber`, `zio.ZIO` |

---

## 6. Kiến thức nào bắt buộc, kiến thức nào dành cho người muốn đi sâu?

**Bắt buộc với mọi người:**
Ch1–7, Ch8A, Ch9, Ch10; trong Ch11: thread, heap/stack, GC ở mức khái niệm, boxing.

**Bắt buộc theo nhánh:**

| BE | DE |
|---|---|
| Ch8B, Ch12, Ch14–18, Ch16A, Ch16B | Ch16A, Ch19–24, Ch26–30, Ch33 |

**Chuyên sâu (có thể bỏ qua lần học đầu):**

- Ch8C: match types, inline, macros, metaprogramming.
- Ch11: GC tuning chi tiết, JIT, phân tích bytecode.
- Ch18: Clean Architecture đầy đủ, tagless final nhiều tầng.
- Ch25: Adaptive Query Execution chi tiết, tùy biến Catalyst.
- Ch30: Kafka transactions và exactly-once trong Kafka Streams/Spark.
- Ch32: tối ưu distributed join, data locality.
- Spark bằng Scala 3 qua thư viện cộng đồng.
- Flink từ Scala qua Java API.
