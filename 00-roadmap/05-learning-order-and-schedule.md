# Thứ tự học đề xuất và lịch học tham khảo

## Giả định

- Người học đã biết lập trình ít nhất một ngôn ngữ (Java, Python, JavaScript, Go...).
- Quỹ thời gian: **10–12 giờ/tuần** (khoảng 2 giờ mỗi buổi tối trong tuần + 2–4 giờ cuối tuần).
- Mỗi tuần: ~40% đọc lý thuyết, ~60% code, làm bài tập và đọc lỗi compiler.
- Nếu học full-time (~35 giờ/tuần), chia thời lượng cho khoảng 3.

## Nguyên tắc thứ tự

1. **Ngôn ngữ trước framework.** Không đụng http4s hay Spark khi chưa xong Giai đoạn 1–2.
2. **Concurrency nền tảng trước effect system, effect system trước framework.**
3. **In-memory trước distributed.** Viết transformation bằng `List` (Ch19–20) rồi mới chuyển sang Spark,
   để thấy rõ cái gì thay đổi và cái gì giữ nguyên.
4. **Batch trước streaming.** Streaming thêm thời gian, thứ tự, trạng thái — học khi đã vững batch.
5. **Mỗi giai đoạn kết thúc bằng một project** để kiểm chứng kiến thức.

## Lịch học Full Track (BE + DE) — khoảng 44 tuần

| Tuần | Nội dung | Đầu ra kiểm chứng |
|---|---|---|
| 1 | Ch1 Introduction, Ch2 Environment | Chạy được `hello`, REPL, project sbt rỗng |
| 2 | Ch3 Variables, Types, Expressions | Bài tập Ch3 pass test |
| 3 | Ch4 Functions & Methods | Bài tập Ch4 pass test |
| 4 | Ch5 OOP | Domain model đơn hàng bằng case class/enum |
| 5–6 | Ch6 Collections | Bộ bài tập xử lý log bằng collection |
| 7–8 | Ch7 Functional Programming | Parser CSV trả về `Either` thay vì ném exception |
| 9 | Ch8A Types cốt lõi | Opaque type cho `UserId`, `Email`, `Money` |
| 10 | Ch8B Given/Using, type class | Type class `Encoder` tự viết |
| 11 | Ch9 Project Structure | Project sbt multi-module |
| 12 | Ch10 Testing | Unit + property-based test |
| 13 | Ch11 JVM Fundamentals | Đọc heap dump/JFR của một chương trình mẫu |
| 14–15 | **Project 1: Scala Data Processor** | Project chạy, test pass, README |
| 16 | Ch12 Backend Fundamentals | Gọi API bằng curl, đọc HTTP raw |
| 17 | Ch16A Concurrency, Future | Bài tập race condition và timeout |
| 18–19 | Ch16B Cats Effect | Chương trình dùng `Resource`, `Ref`, retry |
| 20 | Ch13 Chọn framework | Bảng so sánh có lập luận |
| 21–22 | Ch14 REST APIs | CRUD user với OpenAPI |
| 23–24 | Ch15 Database | Repository Doobie + Testcontainers |
| 25 | Ch17 Security & Production | JWT, health check, metrics |
| 26 | Ch18 Architecture | Refactor theo module |
| 27–28 | **Project 2: Backend Service** | Service chạy trong Docker Compose |
| 29 | Ch19 Data Processing | Làm sạch dataset đơn hàng |
| 30 | Ch20 Functional Transformation | Validation tích lũy lỗi |
| 31 | Ch21 Formats & Serialization | Đọc/ghi Avro, Parquet; thử schema evolution |
| 32 | Ch22 Spark Fundamentals (+ chuyển sang Scala 2.13) | Đọc Spark UI, giải thích DAG |
| 33 | Ch23 Spark SQL & DataFrames | Join + window function |
| 34 | Ch24 Batch Pipeline | Pipeline 10 bước |
| 35 | Ch25 Spark Performance | Đo trước/sau khi xử lý skew |
| 36 | Ch26 Spark Production | Job có data quality check, rerun an toàn |
| 37 | **Project 3: Batch Data Pipeline** | Pipeline chạy lại không sinh trùng dữ liệu |
| 38 | Ch27 Streaming Fundamentals | Mô phỏng late event trên giấy + code |
| 39 | Ch28 Kafka với Scala | Producer/consumer, DLQ |
| 40 | Ch29–30 Streaming Pipeline & Reliability | Test duplicate và replay |
| 41 | **Project 4: Kafka Event Processing** | Consumer có DLQ, metrics |
| 42 | Ch31, Ch33 Architecture & Data Quality | Data contract cho một topic |
| 43 | Ch32, Ch34 Distributed & Performance | Báo cáo đo đạc |
| 44+ | **Project 5: Data Platform** (2–4 tuần) | Hệ thống tích hợp, tài liệu vận hành |

## Lịch rút gọn

- **Backend only (~28 tuần):** tuần 1–28 ở bảng trên, bỏ tuần 14–15 nếu muốn (khuyến nghị vẫn làm P1).
- **Data Engineering only (~32 tuần):** tuần 1–15 → tuần 17 (Ch16A) → tuần 18–19 (Ch16B, rút gọn còn 1 tuần) → tuần 29–44.

## Khi nào học concurrency và effect system?

- **Concurrency nền tảng (Ch16A):** ngay sau khi xong Giai đoạn 2 và Ch12. Lý do: cần Ch11 (thread, heap)
  và cần một bài toán thực tế (xử lý nhiều request đồng thời) để động lực học rõ ràng.
- **Effect system (Ch16B):** sau Ch16A và Ch8B. Học sớm hơn sẽ dẫn đến "dùng IO như phép thuật" —
  viết được code nhưng không hiểu vì sao cần `IO`, `Resource` và vì sao không được gọi `unsafeRunSync` lung tung.
- **Với nhánh DE:** Ch16A là bắt buộc (Spark executor, Kafka consumer đều là chương trình đa luồng).
  Ch16B cần cho Kafka consumer viết bằng fs2-kafka; nếu chỉ dùng Spark có thể học ở mức đọc hiểu.
