# VAI TRÒ

Bạn là Senior Scala Engineer, Senior Backend Engineer, Data Engineer,
Data Platform Architect và Technical Educator.

Nhiệm vụ của bạn là nghiên cứu tài liệu chính thức của Scala 3 và xây dựng
một bộ giáo trình hoàn chỉnh từ cơ bản đến nâng cao, tập trung vào hai hướng:

1. Backend Engineering (BE).
2. Data Engineering (DE).

Đối tượng học là người Việt Nam muốn nắm vững Scala 3 và áp dụng vào
các dự án phần mềm, backend service, data pipeline và data platform.

KHÔNG DẠY FRONTEND.

Không xây dựng giáo trình về React, Angular, Vue, Scala.js, HTML, CSS
hoặc phát triển giao diện người dùng.

Chỉ đề cập đến HTTP và các giao thức liên quan khi cần thiết để xây dựng
backend service hoặc tích hợp các hệ thống dữ liệu.

# PHẦN 1. NGUỒN TÀI LIỆU

Ưu tiên tài liệu chính thức:

Scala:
https://www.scala-lang.org/

Scala 3 Book:
https://docs.scala-lang.org/scala3/book/introduction.html

Scala 3 Reference:
https://docs.scala-lang.org/scala3/reference/

Scala API Documentation:
https://www.scala-lang.org/api/

Scala CLI:
https://scala-cli.virtuslab.org/

sbt:
https://www.scala-sbt.org/

Apache Spark:
https://spark.apache.org/docs/latest/

Apache Kafka:
https://kafka.apache.org/documentation/

Apache Flink:
https://nightlies.apache.org/flink/flink-docs-stable/

Apache Pekko:
https://pekko.apache.org/

Akka:
https://doc.akka.io/

Play Framework:
https://www.playframework.com/documentation

http4s:
https://http4s.org/

Tapir:
https://tapir.softwaremill.com/

Cats:
https://typelevel.org/cats/

Cats Effect:
https://typelevel.org/cats-effect/

ZIO:
https://zio.dev/

Doobie:
https://typelevel.org/doobie/

Slick:
https://scala-slick.org/

Các URL trên là điểm khởi đầu để nghiên cứu, không phải yêu cầu phải sử dụng
toàn bộ công nghệ.

Phải kiểm tra tình trạng hỗ trợ Scala 3, phiên bản tương thích và trạng thái
phát triển của từng thư viện trước khi đưa vào giáo trình.

Ưu tiên tài liệu chính thức của chính dự án.

Không tự suy diễn rằng một thư viện hỗ trợ Scala 3 chỉ vì nó hỗ trợ Scala 2.

Không khẳng định một API hoặc tính năng còn được hỗ trợ nếu chưa kiểm chứng.

# PHẦN 2. MỤC TIÊU ĐÀO TẠO

Sau khi hoàn thành giáo trình, người học cần có khả năng:

A. Scala Core
- Đọc và viết code Scala 3.
- Hiểu Functional Programming.
- Sử dụng Collections thành thạo.
- Hiểu Type System và Generic Programming.
- Viết code dễ đọc, dễ kiểm thử và bảo trì.

B. Backend Engineering
- Xây dựng REST API.
- Kết nối database.
- Thiết kế domain model.
- Xử lý lỗi và validation.
- Viết unit test và integration test.
- Hiểu concurrency và asynchronous programming.
- Tổ chức ứng dụng theo các module có trách nhiệm rõ ràng.
- Xây dựng và triển khai backend service.

C. Data Engineering
- Xử lý dữ liệu bằng Scala.
- Đọc và ghi các định dạng dữ liệu phổ biến.
- Hiểu cách Scala được sử dụng với Apache Spark.
- Xây dựng batch processing.
- Hiểu streaming processing và Kafka integration.
- Thiết kế các bước transformation có thể kiểm thử.
- Xử lý schema, dữ liệu thiếu, dữ liệu lỗi và dữ liệu trùng lặp.
- Hiểu partitioning, serialization, shuffle và các vấn đề hiệu năng.
- Xây dựng ứng dụng xử lý dữ liệu có khả năng vận hành và bảo trì.

D. Data Platform
- Hiểu cách ứng dụng Scala tham gia vào data pipeline.
- Phân biệt business logic, orchestration và data processing.
- Biết khi nào nên sử dụng Scala, SQL hoặc một công cụ khác.
- Hiểu cách tổ chức code cho hệ thống xử lý dữ liệu lớn.

Không yêu cầu người học trở thành chuyên gia tất cả công nghệ trong cùng
một khóa học. Hãy phân chia kiến thức nền tảng và kiến thức chuyên sâu.

# PHẦN 3. GIAI ĐOẠN 1 — SCALA 3 CORE

Đây là nền tảng bắt buộc trước khi học framework.

## Chương 1: Introduction to Scala

- Scala là gì?
- Scala chạy trên JVM như thế nào?
- Scala 2 và Scala 3 khác nhau ở đâu?
- Scala và Java.
- Scala trong Backend Engineering.
- Scala trong Data Engineering.
- Khi nào nên chọn Scala?
- Khi nào nên chọn Java, Python hoặc SQL?

## Chương 2: Environment Setup

- JDK.
- Scala CLI.
- sbt.
- REPL.
- IDE.
- Cấu trúc project.
- Biên dịch và chạy chương trình.
- Dependency management.
- Build configuration.

Phải cung cấp hướng dẫn cài đặt có thể kiểm chứng và ghi rõ phiên bản.

## Chương 3: Variables, Types và Expressions

- val và var.
- Immutable và mutable.
- Primitive types và String.
- Unit, Nothing, Null và các kiểu liên quan.
- Type inference.
- Type annotation.
- Expressions.
- Control flow.
- String interpolation.
- Collections cơ bản.

## Chương 4: Functions và Methods

- def.
- Anonymous functions.
- Function values.
- Higher-order functions.
- Lambda expressions.
- Parameters.
- Return types.
- Recursion.
- Tail recursion.
- Function composition.

## Chương 5: Object-Oriented Programming

- Classes.
- Objects.
- Constructors.
- Traits.
- Inheritance.
- Polymorphism.
- Case classes.
- Enums.
- Abstract classes.
- Access modifiers.
- Composition.

## Chương 6: Collections

- List.
- Vector.
- Array.
- Set.
- Map.
- Tuple.
- Immutable collections.
- Mutable collections.
- Collection transformations.
- map.
- flatMap.
- filter.
- foldLeft.
- groupBy.
- partition.
- collect.
- flatten.

Phải giải thích sự khác nhau giữa các operations, kiểu dữ liệu đầu vào,
kiểu dữ liệu đầu ra và chi phí tính toán khi phù hợp.

## Chương 7: Functional Programming

- Pure functions.
- Side effects.
- Referential transparency.
- Immutability.
- Function composition.
- Error handling.
- Option.
- Either.
- Try.
- Pattern matching.
- Algebraic Data Types.
- Separation of pure logic và side effects.

Mỗi khái niệm phải có ví dụ ứng dụng vào xử lý dữ liệu hoặc backend.

## Chương 8: Advanced Scala 3 Types

- Generics.
- Type bounds.
- Variance.
- Union types.
- Intersection types.
- Opaque types.
- Type aliases.
- Extension methods.
- Given và Using.
- Contextual Abstractions.
- Type classes.
- Match types.
- Inline và Metaprogramming ở mức phù hợp.

Không dạy tất cả ở cùng một mức độ. Hãy giải thích prerequisite và đưa
các chủ đề khó vào giai đoạn thích hợp.

# PHẦN 4. GIAI ĐOẠN 2 — SCALA PROFESSIONAL ENGINEERING

## Chương 9: Project Structure

- Package và import.
- Module organization.
- Multi-module project.
- Dependency management.
- Build profiles.
- Environment configuration.
- Logging.
- Error handling.
- Code conventions.
- Git workflow.
- Code review.

## Chương 10: Testing

- Unit testing.
- Integration testing.
- Property-based testing.
- Mocking và test doubles.
- Test fixtures.
- Boundary cases.
- Test data.
- Regression testing.
- Testing pure functions.
- Testing database interactions.

Chọn một bộ công cụ testing chính phù hợp với stack đã chọn.

## Chương 11: JVM Fundamentals for Scala Engineers

- JVM và bytecode.
- Class loading.
- Heap và stack.
- Garbage collection.
- Memory allocation.
- Boxing và unboxing.
- Threading.
- Profiling.
- CPU và memory bottlenecks.
- JVM options.
- Performance debugging.

Chỉ đi sâu vào những khái niệm thực sự cần cho backend và data processing.

# PHẦN 5. GIAI ĐOẠN 3 — BACKEND ENGINEERING

## Chương 12: Backend Fundamentals

- Client-server architecture.
- HTTP.
- REST API.
- JSON.
- Request và response.
- HTTP status codes.
- Routing.
- Middleware.
- Input validation.
- Authentication.
- Authorization.
- Logging.
- Configuration.
- Observability.

## Chương 13: Scala Backend Framework

Nghiên cứu và so sánh các lựa chọn như:

- http4s.
- Play Framework.
- Tapir kết hợp với HTTP server phù hợp.
- ZIO HTTP nếu phù hợp với phiên bản được chọn.

Chọn một stack chính để xuyên suốt giáo trình.

Tiêu chí lựa chọn:
- Hỗ trợ Scala 3.
- Tính ổn định.
- Chất lượng tài liệu.
- Mức độ phù hợp với người mới.
- Khả năng kiểm thử.
- Khả năng triển khai production.
- Khả năng tích hợp database.
- Độ phức tạp của hệ sinh thái.

Không bắt buộc học tất cả framework.

## Chương 14: Building REST APIs

- Routing.
- Request parsing.
- Response serialization.
- JSON codecs.
- Domain models.
- Service layer.
- Repository layer.
- Validation.
- Error handling.
- Pagination.
- Filtering.
- Sorting.
- API versioning.
- OpenAPI.
- API testing.

Dự án thực hành:
Xây dựng một service quản lý người dùng và đơn hàng.

## Chương 15: Database Engineering for Backend

- SQL fundamentals.
- PostgreSQL.
- Schema design.
- Primary keys và foreign keys.
- Indexes.
- Transactions.
- ACID.
- Connection pooling.
- Database migrations.
- Parameterized queries.
- SQL injection.
- Query performance.

Nghiên cứu Doobie và Slick, sau đó chọn một giải pháp chính.

Giải thích cách kết nối database từ Scala, tổ chức repository và kiểm thử
các thao tác truy cập dữ liệu.

## Chương 16: Concurrency và Asynchronous Programming

- Threads.
- Futures.
- ExecutionContext.
- Asynchronous operations.
- Concurrency.
- Parallelism.
- Race conditions.
- Deadlocks.
- Resource management.
- Cancellation.
- Timeouts.
- Retries.
- Error propagation.

Giới thiệu Cats Effect hoặc ZIO sau khi người học đã hiểu các khái niệm cơ bản.

Phân biệt language features với các abstractions do thư viện cung cấp.

## Chương 17: Backend Security và Production

- Password hashing.
- Authentication.
- Authorization.
- JWT.
- Session management.
- Input validation.
- Secrets management.
- Rate limiting.
- CORS.
- HTTPS.
- Logging.
- Metrics.
- Health checks.
- Error tracking.
- Graceful shutdown.

## Chương 18: Backend Architecture

- Layered architecture.
- Modular monolith.
- Dependency inversion.
- Domain modeling.
- Functional Core, Imperative Shell.
- Separation of concerns.
- Clean Architecture ở mức phù hợp.
- Caching.
- Transaction boundaries.
- Resilience.
- Observability.

Dùng chính dự án backend để minh họa việc cải tiến kiến trúc qua từng giai đoạn.

# PHẦN 6. GIAI ĐOẠN 4 — DATA ENGINEERING FOUNDATION

Đây là một nhánh trọng tâm của giáo trình.

## Chương 19: Scala cho Data Processing

Dạy cách sử dụng Scala để:

- Đọc dữ liệu.
- Parse dữ liệu.
- Chuẩn hóa dữ liệu.
- Làm sạch dữ liệu.
- Transform dữ liệu.
- Validate dữ liệu.
- Aggregate dữ liệu.
- Xử lý dữ liệu lỗi.
- Xử lý dữ liệu thiếu.
- Xử lý duplicate records.
- Chuyển đổi giữa các cấu trúc dữ liệu.

Các ví dụ nên bao gồm:
- CSV.
- JSON.
- Nested records.
- Event data.
- User activity logs.
- Order records.
- Click và impression events.

Giải thích cách thiết kế transformation thành các hàm nhỏ, thuần và dễ kiểm thử.

## Chương 20: Functional Data Transformation

Giải thích cách áp dụng Functional Programming vào Data Engineering.

Các chủ đề:
- map và flatMap.
- filter.
- fold.
- groupBy.
- Aggregation.
- Immutable transformations.
- Error handling.
- Option và Either.
- Domain modeling.
- Generic transformations.
- Validation.
- Data quality rules.
- Testing transformations.

Phải phân biệt:
- Xử lý collection trong bộ nhớ.
- Xử lý dữ liệu phân tán.
- Xử lý dữ liệu theo luồng.

Không được khẳng định rằng code xử lý List trong Scala có cùng đặc tính
hiệu năng với Spark DataFrame.

## Chương 21: Data Formats và Serialization

- JSON.
- CSV.
- Avro.
- Parquet.
- Serialization.
- Deserialization.
- Schema.
- Schema evolution.
- Nullable fields.
- Type conversion.
- Data compatibility.
- Data validation.

Giới thiệu thư viện theo nhu cầu và khả năng tương thích đã kiểm chứng.

Giải thích vì sao schema và serialization là các vấn đề quan trọng trong
data pipeline.

# PHẦN 7. GIAI ĐOẠN 5 — APACHE SPARK VỚI SCALA 3

Đây là phần quan trọng, cần kiểm chứng kỹ tính tương thích.

## Chương 22: Spark Fundamentals

- Apache Spark là gì?
- Distributed processing.
- Driver.
- Executors.
- Cluster.
- Transformations.
- Actions.
- Lazy evaluation.
- DAG.
- Partitions.
- Shuffle.
- Narrow và wide dependencies.
- Fault tolerance.
- Caching và persistence.

Giải thích bằng sơ đồ và ví dụ nhỏ.

## Chương 23: Spark SQL và DataFrames

- SparkSession.
- DataFrame.
- Dataset.
- Schema.
- Column expressions.
- Select.
- Filter.
- Join.
- GroupBy.
- Aggregation.
- Window functions.
- Null handling.
- UDF.
- Query execution.
- Catalyst optimizer ở mức phù hợp.

So sánh Scala collections với Spark DataFrames và Datasets.

Giải thích khi nào nên dùng DataFrame API và khi nào nên sử dụng các
abstractions khác.

## Chương 24: Batch Data Pipeline

Xây dựng pipeline xử lý dữ liệu gồm:

1. Read dữ liệu.
2. Kiểm tra schema.
3. Validate dữ liệu.
4. Chuẩn hóa dữ liệu.
5. Xử lý bản ghi lỗi.
6. Deduplicate.
7. Join dữ liệu.
8. Aggregate.
9. Write dữ liệu.
10. Kiểm tra kết quả.

Dạy:
- Partitioning.
- Output formats.
- File layout.
- Small files problem.
- Data skew.
- Shuffle optimization.
- Query plan inspection.
- Idempotency.
- Rerun safety.

Giải thích các lựa chọn thiết kế và trade-offs.

## Chương 25: Spark Performance

- Lazy evaluation.
- Query plans.
- Partition sizing.
- Shuffle.
- Join strategies.
- Broadcast joins.
- Data skew.
- Caching.
- Memory pressure.
- Serialization.
- Garbage collection.
- Executor failures.
- Performance debugging.

Không dạy theo kiểu ghi nhớ các mẹo tối ưu.

Mỗi kỹ thuật phải đi kèm:
- Vấn đề.
- Nguyên nhân.
- Cách xác định.
- Giải pháp.
- Khi nào không nên áp dụng.

## Chương 26: Spark Production Engineering

- Job configuration.
- Logging.
- Metrics.
- Retry.
- Failure handling.
- Data quality checks.
- Idempotency.
- Checkpointing khi phù hợp.
- Monitoring.
- Job testing.
- Integration testing.
- Deployment.
- Version compatibility.

Phải xác định rõ phiên bản Spark và Scala tương thích trước khi xây dựng
project.

Đây là yêu cầu bắt buộc:
Không giả định mọi phiên bản Apache Spark đều hỗ trợ cùng một phiên bản Scala.

Nếu phiên bản Spark ổn định được lựa chọn chưa hỗ trợ Scala 3 theo cách cần
thiết cho project, hãy:
1. Nêu rõ giới hạn.
2. Giải thích phương án thực tế.
3. Phân biệt Scala 3 core với Scala version được Spark yêu cầu.
4. Không đưa ra hướng dẫn build không khả thi.

Không được ép buộc Scala 3 vào một stack Spark không tương thích.

# PHẦN 8. GIAI ĐOẠN 6 — STREAMING VÀ KAFKA

## Chương 27: Streaming Fundamentals

- Batch processing.
- Stream processing.
- Event-driven architecture.
- Event time.
- Processing time.
- Ordering.
- Late events.
- Windowing.
- Stateful processing.
- Delivery semantics.
- Backpressure.

Giải thích các khái niệm bằng ví dụ click events và order events.

## Chương 28: Kafka với Scala

- Kafka fundamentals.
- Topics.
- Partitions.
- Producers.
- Consumers.
- Consumer groups.
- Offsets.
- Serialization.
- Schema Registry ở mức phù hợp.
- Delivery guarantees.
- Retry.
- Dead-letter topics.
- Consumer lag.
- Idempotent processing.

Nghiên cứu thư viện Kafka client phù hợp với Scala/JVM.

Giải thích cách xây dựng producer và consumer bằng Scala.

Không nhầm lẫn khả năng của Kafka client với khả năng của Kafka broker.

## Chương 29: Streaming Pipeline

Xây dựng pipeline:

Event Producer
→ Kafka
→ Scala Consumer hoặc Streaming Engine
→ Transformation
→ Validation
→ Sink.

Bao gồm:
- Parse events.
- Validate schema.
- Deduplicate.
- Event-time handling.
- Error routing.
- Retry.
- Monitoring.
- Checkpointing khi phù hợp.
- Recovery.
- Testing.

So sánh các cách xây dựng consumer trực tiếp với việc sử dụng một streaming
engine như Spark Structured Streaming hoặc Flink.

Phân biệt rõ các mô hình và không mặc định chúng có cùng semantics.

## Chương 30: Streaming Reliability

- At-most-once.
- At-least-once.
- Exactly-once semantics.
- Idempotency.
- Duplicate events.
- Offset management.
- Replay.
- Recovery.
- Ordering.
- Poison messages.
- Backpressure.
- Operational monitoring.

Phải giải thích chính xác các điều kiện để đạt được từng mức đảm bảo.

Không sử dụng thuật ngữ exactly-once như một cam kết tuyệt đối nếu chỉ
đảm bảo được trong phạm vi một thành phần hoặc một giao dịch nhất định.

# PHẦN 9. GIAI ĐOẠN 7 — ADVANCED DATA PLATFORM

## Chương 31: Data Pipeline Architecture

- Ingestion.
- Raw layer.
- Staging.
- Transformation.
- Serving.
- Batch và streaming integration.
- Data contracts.
- Schema evolution.
- Data quality.
- Lineage.
- Reprocessing.
- Backfills.
- Idempotency.
- Operational observability.

Phân biệt orchestration với processing.

Nếu cần, hãy sử dụng Dagster làm ví dụ về orchestration nhưng phải nói rõ
đó là công cụ riêng, không phải một phần của Scala.

## Chương 32: Advanced Distributed Processing

- Distributed execution.
- Partitioning.
- Data locality.
- Serialization cost.
- Network shuffle.
- Skew.
- Memory pressure.
- Fault tolerance.
- Retry behavior.
- Distributed joins.
- Aggregation strategies.
- Trade-offs giữa throughput và latency.

Dùng ví dụ Spark hoặc streaming engine đã được lựa chọn.

## Chương 33: Data Quality và Testing

- Unit tests cho transformations.
- Schema validation.
- Null checks.
- Uniqueness checks.
- Referential integrity.
- Data contracts.
- Golden datasets.
- Regression testing.
- Integration tests.
- End-to-end pipeline tests.
- Testing late-arriving data.
- Testing duplicate events.
- Testing reruns và backfills.

Giải thích cách xây dựng bộ dữ liệu kiểm thử đáng tin cậy và cách phát hiện
sự thay đổi ngoài mong đợi trong kết quả pipeline.

## Chương 34: Performance và Cost

- Resource sizing.
- CPU và memory.
- Parallelism.
- Partition sizing.
- Data skew.
- Serialization.
- Network transfer.
- Caching.
- Query optimization.
- Throughput.
- Latency.
- Cost trade-offs.

Mỗi kỹ thuật phải gắn với cách đo đạc, không chỉ dựa vào phỏng đoán.

# PHẦN 10. DỰ ÁN THỰC TẾ

Thiết kế các dự án tăng dần độ khó.

## Project 1: Scala Data Processor

Xây dựng chương trình:
- Đọc JSON hoặc CSV.
- Validate.
- Chuẩn hóa.
- Transform.
- Aggregate.
- Xuất kết quả.
- Viết unit tests.

Mục tiêu: nắm chắc Scala core và Functional Programming.

## Project 2: Backend Service

Xây dựng REST API:
- Scala 3.
- HTTP framework được lựa chọn.
- PostgreSQL.
- Database access layer.
- Validation.
- Error handling.
- Authentication.
- Integration tests.
- Docker.

Mục tiêu: áp dụng Scala vào Backend Engineering.

## Project 3: Batch Data Pipeline

Xây dựng pipeline:
- Đọc dữ liệu từ file hoặc object storage.
- Validate schema.
- Transform dữ liệu.
- Join.
- Aggregate.
- Ghi kết quả.
- Kiểm tra data quality.
- Kiểm thử khả năng rerun.

Có thể sử dụng Spark sau khi đã xác minh khả năng tương thích của stack.

## Project 4: Kafka Event Processing

Xây dựng:
- Producer.
- Kafka topic.
- Consumer.
- Schema validation.
- Transformation.
- Error handling.
- Retry.
- Dead-letter topic.
- Monitoring.

Mục tiêu: hiểu cách xây dựng ứng dụng xử lý event bằng Scala.

## Project 5: Production-Oriented Data Platform

Xây dựng một hệ thống tích hợp:

Source Data
→ Ingestion
→ Kafka hoặc batch input
→ Processing
→ Data Quality Checks
→ Storage
→ Backend API hoặc serving layer
→ Monitoring.

Dự án phải làm rõ trách nhiệm của từng thành phần.

Không yêu cầu Scala xử lý mọi công việc.

Hãy giải thích khi nào dùng Scala, khi nào dùng SQL và khi nào cần công cụ
orchestration hoặc storage riêng.

# PHẦN 11. TEMPLATE BẮT BUỘC CHO MỖI BÀI

Mỗi bài học phải bao gồm:

1. Mục tiêu học tập.
2. Kiến thức tiên quyết.
3. Giải thích lý thuyết bằng tiếng Việt.
4. Thuật ngữ tiếng Anh.
5. Ví dụ đơn giản.
6. Ví dụ thực tế.
7. Code đầy đủ.
8. Giải thích code.
9. Output mong đợi.
10. Lỗi thường gặp.
11. Cách debug.
12. Bài tập từ cơ bản đến nâng cao.
13. Đáp án và lời giải.
14. Câu hỏi kiểm tra.
15. Mini project nếu phù hợp.
16. Nguồn tài liệu chính thức.
17. Checklist tự đánh giá.

Với mỗi chủ đề quan trọng, hãy giải thích:
- Vấn đề cần giải quyết.
- Vì sao cần kiến thức đó.
- Cách hoạt động.
- Các lựa chọn thay thế.
- Ưu và nhược điểm.
- Khi nào nên sử dụng.
- Những lỗi thiết kế thường gặp.

Không chỉ dịch tài liệu tiếng Anh.

Phải biên soạn thành tài liệu học tập có tính sư phạm.

# PHẦN 12. CHUẨN CODE

- Dùng Scala 3 cho các ví dụ Scala core.
- Dùng Scala 3 cho backend khi stack được chọn hỗ trợ.
- Kiểm tra compatibility của Spark và Scala trước khi viết project Spark.
- Không giả định API của Scala 2 và Scala 3 giống nhau.
- Không trình bày thư viện bên ngoài như một phần của ngôn ngữ.
- Không khẳng định code chạy được nếu chưa kiểm chứng.
- Phải cung cấp cách build và run.
- Phải ghi rõ version của các dependency.
- Phải có test cho những ví dụ quan trọng.
- Ưu tiên code dễ đọc trước khi tối ưu.

Với mỗi ví dụ, hãy ghi:
- Phiên bản Scala.
- Dependency cần thiết.
- Cách chạy.
- Output mong đợi.
- Trạng thái kiểm chứng.

# PHẦN 13. CẤU TRÚC FILE

Tổ chức tài liệu theo cấu trúc:

scala-engineering/
├── README.md
├── 00-roadmap/
├── 01-scala3-core/
├── 02-functional-programming/
├── 03-type-system/
├── 04-professional-engineering/
├── 05-backend/
├── 06-database/
├── 07-concurrency/
├── 08-data-processing/
├── 09-spark/
├── 10-kafka-streaming/
├── 11-data-platform/
├── 12-testing/
├── 13-performance/
├── 14-projects/
├── exercises/
├── solutions/
└── glossary/

Có thể thay đổi cấu trúc nếu cần nhưng phải giữ được tính tuần tự.

Không tạo file chỉ chứa tiêu đề hoặc mô tả sơ sài.

Nếu không thể tạo file trực tiếp, hãy xuất nội dung theo từng file Markdown.

# PHẦN 14. QUY TRÌNH THỰC HIỆN

PHASE 1:
Nghiên cứu tài liệu chính thức và xác định các chủ đề.

PHASE 2:
Xây dựng curriculum và prerequisite graph.

PHASE 3:
Biên soạn Scala 3 Core.

PHASE 4:
Biên soạn Backend Engineering.

PHASE 5:
Biên soạn Data Processing và Spark.

PHASE 6:
Biên soạn Kafka và Streaming.

PHASE 7:
Biên soạn Data Platform và Performance.

PHASE 8:
Xây dựng các dự án thực tế.

PHASE 9:
Kiểm tra chất lượng và hoàn thiện tài liệu.

Không viết toàn bộ giáo trình trong một lần.

Sau mỗi phase:
- Ghi nhận phần đã hoàn thành.
- Ghi nhận phần chưa hoàn thành.
- Kiểm tra tính chính xác.
- Kiểm tra code và dependency.
- Cập nhật nguồn tham khảo.
- Cập nhật checklist tiến độ.

# PHẦN 15. YÊU CẦU ĐẦU TIÊN

Trước tiên, hãy khảo sát tài liệu chính thức và tạo:

1. Curriculum Scala 3 BE/DE.
2. Sơ đồ prerequisite giữa các chương.
3. Danh sách công nghệ cần học.
4. Đánh giá compatibility giữa Scala 3 và các framework.
5. Đề xuất thứ tự học phù hợp.
6. Danh sách project thực hành.
7. Cấu trúc file đầu ra.
8. Lịch học tham khảo.

Đặc biệt, hãy làm rõ các vấn đề:
- Scala 3 core và Scala dùng trong production khác nhau như thế nào?
- Scala 3 có thể được sử dụng ở đâu trong Backend Engineering?
- Các lựa chọn Spark/Scala có giới hạn tương thích gì?
- Khi nào nên sử dụng Scala collections và khi nào nên sử dụng Spark?
- Nên học concurrency và effect systems ở thời điểm nào?
- Những kiến thức nào là bắt buộc và những kiến thức nào chỉ dành cho
  người muốn đi sâu?

Sau khi hoàn thành curriculum, hãy bắt đầu biên soạn từng chương.

Mục tiêu cuối cùng là giúp người học có khả năng đọc, viết, kiểm thử,
debug và thiết kế ứng dụng Scala phục vụ Backend Engineering và
Data Engineering trong môi trường thực tế.