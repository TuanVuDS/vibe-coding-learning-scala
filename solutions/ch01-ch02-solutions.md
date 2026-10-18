# Lời giải — Chương 1 và Chương 2

> Đây là đáp án tham khảo cho các bài tập mang tính khái niệm và cài đặt. Với câu hỏi mở,
> đáp án của bạn có thể khác — điều quan trọng là lập luận dựa trên tiêu chí kỹ thuật.

## Chương 1

### Bài 1 — Sơ đồ source → bytecode → JVM

```
Hello.scala ──scalac──► Hello.class (bytecode JVM)
                     └► Hello.tasty (cây cú pháp có kiểu, riêng Scala 3)
Hello.class + scala-library.jar ──java──► JVM: nạp class → thông dịch → JIT → chạy
```

- `.class`: thứ JVM thực sự chạy. Cùng định dạng với `.class` do `javac` sinh ra — vì vậy Scala và Java dùng chung JVM và thư viện.
- `.tasty`: JVM **không** dùng. Compiler và IDE dùng để đọc lại thông tin kiểu đầy đủ của thư viện Scala 3 (bytecode bị mất thông tin
  như union type, opaque type), phục vụ tương thích giữa các phiên bản Scala 3 và metaprogramming.

### Bài 2 — Thư viện Java dùng được từ Scala

Ví dụ: JDBC driver PostgreSQL, `kafka-clients`, Jackson, AWS SDK, HikariCP.
Lý do: Scala biên dịch ra bytecode JVM và gọi được mọi class Java như gọi class Scala. Lưu ý thực tế: thư viện Java trả về `null`,
dùng collection Java (`java.util.List`) và exception — cần bọc lại bằng `Option`, chuyển đổi collection (`scala.jdk.CollectionConverters`)
và xử lý lỗi (Chương 7).

### Bài 3 — Chọn công cụ

| Tình huống | Lựa chọn | Lập luận |
|---|---|---|
| a) Doanh thu theo tháng từ bảng đã sạch | SQL | Phép aggregate chuẩn; warehouse tối ưu sẵn; ai cũng đọc được. Viết bằng Scala chỉ thêm chi phí |
| b) API thanh toán cho đội 30 kỹ sư Java | Java | Năng lực đội ngũ và tuyển dụng quan trọng hơn lợi ích ngôn ngữ; Java 21 đủ hiện đại |
| c) 2 TB log/ngày, parse phức tạp, cluster Spark có sẵn | Scala 2.13 + Spark | Hạ tầng có sẵn, dữ liệu lớn; logic parse phức tạp hưởng lợi từ kiểu mạnh và không tốn chi phí UDF Python |
| d) Phân tích A/B trong notebook | Python | Hệ sinh thái thống kê; vòng lặp thử nghiệm nhanh |

### Bài 4 — Phản hồi đề xuất "toàn bộ bằng Scala 3 kể cả Spark" (mẫu)

> Đề xuất dùng Scala 3 cho backend và các service xử lý event là hợp lý: http4s, Cats Effect, Tapir, kafka-clients đều dùng được
> với Scala 3. Tuy nhiên phần Spark không khả thi theo cách đề xuất: Spark 4.x chỉ phát hành bản build cho Scala 2.13 và tài liệu
> chính thức yêu cầu ứng dụng dùng đúng phiên bản Scala mà Spark được biên dịch. Dataset encoder của Spark dựa trên reflection
> TypeTag của Scala 2, thứ không tồn tại trong Scala 3. Các cách chạy Spark bằng Scala 3 hiện chỉ là dự án cộng đồng, không được
> hỗ trợ chính thức, rủi ro khi debug trên cluster. Tôi đề xuất: backend và consumer viết Scala 3; job Spark là một build riêng dùng
> Scala 2.13; hai phần giao tiếp qua dữ liệu (Kafka topic, Parquet, bảng) với schema được quản lý rõ ràng. Logic nghiệp vụ thuần
> có thể đặt trong module cross-build 2.13/3 nếu cần dùng chung. Chúng ta theo dõi epic SPARK-54150 để đánh giá lại khi Spark hỗ trợ Scala 3.

### Câu hỏi kiểm tra — gợi ý trả lời

1. Lỗi kiểu bị chặn từ lúc biên dịch, trước khi deploy; refactor an toàn hơn vì compiler chỉ ra mọi chỗ bị ảnh hưởng.
2. Vì `List`, `Option`, `println`... nằm trong standard library của Scala, không có trong JDK.
3. (1) Spark chỉ build cho 2.13; (2) encoder dựa trên `TypeTag`/reflection Scala 2 không có trong Scala 3. Ngoài ra là chi phí bảo trì cross-build của một codebase rất lớn.
4. Ứng dụng chạy lâu được JIT tối ưu dần nên đạt tốc độ cao; CLI 50 ms kết thúc trước khi JIT kịp tối ưu, và phần lớn thời gian là khởi động JVM.
5. Java hiện đại đã có record, `var`, switch expression, sealed interface, pattern matching. Lý do mạnh hơn hiện nay là hệ thống kiểu, FP và hệ sinh thái.

## Chương 2

### Bài 2 — Kiểu trong REPL

| Biểu thức | Kết quả | Kiểu | Ghi chú |
|---|---|---|---|
| `1 / 2` | `0` | `Int` | Chia nguyên |
| `1.0 / 2` | `0.5` | `Double` | Có một toán hạng `Double` |
| `"a" * 3` | `"aaa"` | `String` | Phương thức của chuỗi |
| `List(1,2,3).sum` | `6` | `Int` | |

### Bài 3 — Playground

```scala
//> using scala 3.9.0

@main def run(): Unit =
  val name = "Nguyễn Văn An"
  val birthYear = 1998
  println(s"$name sinh năm $birthYear")
```

```bash
scala-cli run .
```

### Bài 5 — Thêm `kafka-clients`

```scala
// project.scala
//> using dep org.apache.kafka:kafka-clients:4.2.2
```

```scala
// build.sbt
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "4.2.2"
```

Dùng `:`/`%` vì `kafka-clients` là **thư viện Java**: tên artifact không có hậu tố phiên bản Scala (`_3`).
Nếu dùng `::`/`%%`, công cụ sẽ tìm `kafka-clients_3` — artifact không tồn tại — và báo lỗi không tìm thấy dependency.

Để xem transitive dependency: sbt shell → `show Compile / dependencyClasspath`, hoặc
`scala-cli compile . --print-class-path`. Danh sách cụ thể phụ thuộc phiên bản; hãy tự chạy và ghi lại
(thường gặp các thư viện nén và logging, ví dụ zstd, lz4, snappy, slf4j).

### Câu hỏi kiểm tra — gợi ý trả lời

1. Để build tái lập được (reproducible): mọi máy, mọi CI dùng cùng một phiên bản, không phụ thuộc cái gì cài sẵn.
2. `%%` thêm hậu tố phiên bản Scala (`_3`) vào tên artifact. Dùng `%` cho munit sẽ tìm artifact `munit` không hậu tố — không tồn tại (hoặc là bản cũ không đúng) → lỗi resolve.
3. Scala CLI: script, bài tập, prototype, một module đơn giản. sbt: multi-module, plugin, đóng gói, CI của dự án thật.
4. Không. Scope `Test` chỉ có trên classpath lúc chạy test.
5. Mỗi lần gõ `sbt <lệnh>` phải khởi động JVM và nạp build (nhiều giây); sbt shell giữ JVM "nóng" và dùng biên dịch tăng dần.
