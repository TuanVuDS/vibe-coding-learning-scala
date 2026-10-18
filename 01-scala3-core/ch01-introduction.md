# Chương 1 — Introduction to Scala

> **Phiên bản:** Scala 3.9.0 (LTS) · JDK 21 · Không cần dependency
> **Trạng thái kiểm chứng:** Ví dụ Scala đã biên dịch & chạy với `scalac 3.9.0` trên OpenJDK 21.0.10.
> Ví dụ Java chỉ dùng để so sánh, không nằm trong project.

---

## 1. Mục tiêu học tập

Sau chương này, bạn có thể:

- Giải thích Scala là gì và nó chạy trên JVM như thế nào (source → bytecode → JVM).
- Nêu được 5 khác biệt quan trọng giữa Scala 2 và Scala 3.
- So sánh Scala với Java, Python, SQL theo tiêu chí kỹ thuật thay vì cảm tính.
- Mô tả vai trò của Scala trong Backend Engineering và Data Engineering.
- Quyết định có cơ sở khi nào nên (và không nên) chọn Scala.

## 2. Kiến thức tiên quyết

- Đã lập trình ít nhất một ngôn ngữ (biến, hàm, vòng lặp, class).
- Biết dùng terminal ở mức cơ bản (`cd`, `ls`/`dir`, chạy lệnh).
- Không cần biết Java, nhưng biết Java sẽ giúp hiểu phần JVM nhanh hơn.

## 3. Lý thuyết

### 3.1. Scala là gì?

Scala (viết tắt của *scalable language*) là ngôn ngữ lập trình **kiểu tĩnh** (statically typed),
kết hợp **lập trình hướng đối tượng** (OOP) và **lập trình hàm** (FP). Ngôn ngữ do Martin Odersky
thiết kế tại EPFL (Thụy Sĩ), phiên bản đầu tiên ra năm 2004. Scala 3 ra mắt năm 2021.

- **Vấn đề cần giải quyết:** Java (thời điểm Scala ra đời) dài dòng, hệ thống kiểu kém biểu đạt,
  khó viết code xử lý dữ liệu theo phong cách khai báo. Các ngôn ngữ động (Python, Ruby) ngắn gọn
  nhưng lỗi kiểu chỉ phát hiện lúc chạy.
- **Cách Scala giải quyết:** cú pháp ngắn gọn + suy luận kiểu (type inference) + hệ thống kiểu
  mạnh + chạy trên JVM để dùng lại toàn bộ hệ sinh thái Java.
- **"Kiểu tĩnh" nghĩa là gì?** Compiler kiểm tra kiểu **trước khi chạy**. Nếu bạn truyền
  `String` vào chỗ cần `Int`, chương trình **không biên dịch được** — lỗi bị chặn từ máy của lập trình viên,
  không phải lúc 2 giờ sáng trên production.

### 3.2. Scala chạy trên JVM như thế nào?

```
  Hello.scala ──(scalac)──► Hello.class + Hello.tasty ──(JVM: java)──► chạy
     source                    bytecode     TASTy             class loading,
                                                              JIT compile, GC
```

1. **Compiler `scalac`** đọc file `.scala`, kiểm tra kiểu, sinh ra:
   - file `.class` chứa **JVM bytecode** — cùng định dạng mà Java compiler sinh ra;
   - file `.tasty` (*Typed Abstract Syntax Trees*) — riêng của Scala 3, lưu thông tin kiểu đầy đủ,
     dùng cho IDE, tương thích giữa các phiên bản và metaprogramming.
2. **JVM** (Java Virtual Machine) nạp các `.class`, thông dịch rồi dùng **JIT compiler** dịch phần code
   chạy nhiều ("hot") sang mã máy, quản lý bộ nhớ bằng **Garbage Collector**.
3. Chương trình Scala cần thêm **Scala standard library** trên classpath — chính là những thứ như
   `List`, `Option`, `println`. Trước Scala 3.8, standard library gồm `scala-library` của Scala 2.13 cộng
   `scala3-library_3`; từ Scala 3.8, standard library được biên dịch bằng Scala 3 và nằm trong
   `scala-library` phiên bản 3.x (artifact `scala3-library_3` vẫn được giữ cho tương thích).
   Build tool (Scala CLI, sbt) tự xử lý việc này cho bạn.

Hệ quả thực tế:

| Hệ quả | Ý nghĩa |
|---|---|
| Dùng được mọi thư viện Java | JDBC, Kafka client, Hadoop, AWS SDK... gọi trực tiếp từ Scala |
| Hiệu năng gần Java | Sau khi JIT "làm nóng", tốc độ tương đương Java cho cùng thuật toán |
| Khởi động chậm hơn native | JVM cần thời gian nạp class và JIT; không lý tưởng cho CLI chạy vài mili-giây |
| Kiến thức JVM là bắt buộc | GC, heap, thread — bạn sẽ học ở Chương 11 |

### 3.3. Scala 2 và Scala 3 khác nhau ở đâu?

Scala 3 (tên mã phát triển: Dotty) là bản thiết kế lại compiler và làm sạch ngôn ngữ.

| Chủ đề | Scala 2 | Scala 3 |
|---|---|---|
| Cú pháp khối lệnh | Ngoặc nhọn `{ }` bắt buộc | Có thể dùng **thụt lề** (indentation), ngoặc nhọn vẫn hợp lệ |
| Điều kiện | `if (x > 0) a else b` | Thêm dạng `if x > 0 then a else b` |
| Điểm vào chương trình | `object Main extends App` hoặc `def main(args: Array[String])` | `@main def run(): Unit` |
| Liệt kê giá trị | `sealed trait` + `case object` | `enum` |
| Tham số ngữ cảnh | `implicit` (một từ khóa cho nhiều mục đích) | `given` / `using` / `extension` (tách bạch mục đích) |
| Kiểu mới | — | union `A \| B`, intersection `A & B`, opaque type, match type |
| Macro | Scala 2 macro (thử nghiệm, không còn hỗ trợ) | Hệ thống `inline` + quotes/splices mới |
| Tương thích nhị phân | Mỗi bản 2.x (2.12, 2.13) không tương thích nhau | Scala 3 dùng lại thư viện 2.13; các bản 3.x tương thích tiến (forward) |

**Lưu ý quan trọng:** Scala 3 **dùng được** thư viện Scala 2.13 (và ngược lại có giới hạn), nhưng không phải
mọi thứ: thư viện dựa vào macro Scala 2 hoặc reflection `TypeTag` (như Dataset encoder của Spark) không chạy
trong Scala 3. Đây là gốc rễ của giới hạn Spark mà bạn sẽ gặp ở Chương 22.

### 3.4. Scala và Java

| Tiêu chí | Java (21+) | Scala 3 |
|---|---|---|
| Độ dài code | Đã cải thiện nhiều (record, `var`, switch expression, sealed) | Vẫn ngắn hơn, đặc biệt với xử lý collection và pattern matching |
| Mô hình lập trình | OOP là chính, FP bổ sung | OOP và FP ngang hàng |
| Null safety | `Optional`, nhưng `null` vẫn phổ biến | `Option` là quy ước chuẩn; có chế độ explicit nulls (thử nghiệm) |
| Hệ thống kiểu | Generics, sealed interface | Mạnh hơn: variance tại khai báo, type class, union/opaque type |
| Thời gian biên dịch | Nhanh | Chậm hơn đáng kể |
| Tuyển dụng (thị trường VN) | Rất nhiều ứng viên | Ít ứng viên hơn nhiều |
| Đường cong học tập | Thấp | Cao hơn, đặc biệt khi dùng hệ sinh thái FP |

Java hiện đại đã thu hẹp khoảng cách. Lý do chọn Scala ngày nay chủ yếu là **hệ thống kiểu biểu đạt**,
**FP thực thụ** và **hệ sinh thái** (Spark, Kafka tooling, Typelevel/ZIO), không còn là "Java quá dài dòng".

### 3.5. Scala trong Backend Engineering

- Viết HTTP API, microservice, service xử lý event (Kafka consumer).
- Thế mạnh: mô hình hóa domain bằng ADT và kiểu (sai dữ liệu → không biên dịch), xử lý đồng thời
  bằng effect system (Cats Effect, ZIO) với khả năng hủy tác vụ, quản lý tài nguyên an toàn.
- Hạn chế: đội ngũ cần thời gian đào tạo; nhiều "phong cách" Scala khác nhau trong cùng thị trường.

### 3.6. Scala trong Data Engineering

- **Apache Spark** được viết bằng Scala; Scala API là API "gốc", mới nhất và đầy đủ nhất cùng với Python.
- Kafka (broker) có lịch sử viết bằng Scala; các công cụ xung quanh dữ liệu (Delta Lake, nhiều connector) cũng vậy.
- Thế mạnh: kiểu mạnh cho schema, test transformation dễ, hiệu năng JVM, không có chi phí chuyển dữ liệu
  Python↔JVM khi dùng UDF.
- Hạn chế: Spark chưa hỗ trợ Scala 3 (cần dùng 2.13); nhiều team DE ưu tiên PySpark + SQL vì dễ tuyển người.

### 3.7. Khi nào chọn Scala, Java, Python hay SQL?

| Tình huống | Lựa chọn hợp lý | Lý do |
|---|---|---|
| Biến đổi trên dữ liệu dạng bảng đã sạch (lọc, join, aggregate) | **SQL** | Ngắn, ai cũng đọc, optimizer làm tốt; không cần ngôn ngữ đa năng |
| Phân tích khám phá, ML, notebook | **Python** | Hệ sinh thái pandas/numpy/scikit-learn; tốc độ thử nghiệm |
| Job Spark có logic phức tạp, UDF nặng, cần kiểu mạnh | **Scala** (2.13) | Không tốn chi phí serialize sang Python; lỗi bắt lúc compile |
| Backend service, đội lớn, tuyển dụng dễ, framework Spring | **Java** | Thị trường lao động, công cụ, quy trình quen thuộc |
| Backend có domain phức tạp, nhiều xử lý đồng thời, đội chấp nhận học FP | **Scala 3** | ADT, effect system, kiểu biểu đạt |
| Script vận hành nhỏ, glue code | Python/Bash (hoặc Scala CLI nếu cả team dùng JVM) | Khởi động nhanh, đơn giản |

**Quy tắc vàng:** chọn công cụ theo **bài toán + năng lực đội ngũ + hạ tầng sẵn có**, không chọn theo sở thích.
Một hệ thống dữ liệu tốt thường dùng *cả* SQL, Python và Scala, mỗi thứ ở đúng chỗ.

### 3.8. Lỗi thiết kế/quyết định thường gặp

- Chọn Scala cho cả team chỉ vì một người thích, không có kế hoạch đào tạo.
- Viết Scala như Java (mọi thứ `var`, `null`, mutable) — mất hết lợi ích mà vẫn chịu chi phí.
- Viết Scala như Haskell trong team chưa sẵn sàng — code đúng nhưng không ai bảo trì được.
- Dùng Spark cho dữ liệu vài trăm MB — trả chi phí cluster cho bài toán một máy làm được.
- Giả định "Scala 3 mới hơn nên Spark chạy được" — sai (xem Chương 22).

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Statically typed | Kiểu được kiểm tra lúc biên dịch |
| Type inference | Compiler tự suy ra kiểu khi bạn không ghi |
| JVM (Java Virtual Machine) | Máy ảo chạy bytecode |
| Bytecode | Mã trung gian trong file `.class` |
| JIT (Just-In-Time) compiler | Dịch bytecode "nóng" sang mã máy khi đang chạy |
| Garbage Collector (GC) | Tự động thu hồi bộ nhớ không còn dùng |
| TASTy | Định dạng lưu cây cú pháp có kiểu của Scala 3 |
| Classpath | Danh sách thư mục/jar mà JVM tìm class |
| Binary compatibility | Code đã biên dịch dùng được với phiên bản thư viện khác mà không cần biên dịch lại |
| LTS (Long Term Support) | Dòng phiên bản được bảo trì dài hạn |

## 5. Ví dụ đơn giản

Cùng một tác vụ: lọc các đơn hàng giá trị lớn và lấy mã đơn.

**Java 21:**

```java
record Order(String id, long amount) {}

List<String> bigOrderIds = orders.stream()
    .filter(o -> o.amount() >= 1_000_000)
    .map(Order::id)
    .toList();
```

**Scala 3:**

```scala
final case class Order(id: String, amount: Long)

val bigOrderIds = orders.filter(_.amount >= 1_000_000).map(_.id)
```

Khác biệt không chỉ là số ký tự: trong Scala, `List` mặc định là **bất biến** (immutable) và
`filter/map` là phương thức của chính collection, không cần chuyển sang `stream()` rồi quay lại.

## 6. Ví dụ thực tế

Một công ty thương mại điện tử có thể dùng Scala ở hai nơi:

```
[Mobile app] → HTTP → [Order Service — Scala 3, http4s] → PostgreSQL
                              │
                              └─ publish event "OrderCreated" → Kafka
                                                                  │
          ┌───────────────────────────────────────────────────────┘
          ▼
 [Consumer — Scala 3]  cập nhật tồn kho realtime
 [Spark job — Scala 2.13] chạy hằng đêm: doanh thu theo ngày, theo tỉnh → bảng báo cáo
 [SQL] analyst truy vấn bảng báo cáo
```

Chú ý: Scala 3 và Scala 2.13 **cùng tồn tại** trong một hệ thống, giao tiếp qua dữ liệu (Kafka, bảng),
không chia sẻ code biên dịch trực tiếp.

## 7. Code đầy đủ

Chương 1 chỉ có một chương trình nhỏ, dùng chung với Chương 2:
[`code/src/main/scala/scalaeng/ch02/Hello.scala`](code/src/main/scala/scalaeng/ch02/Hello.scala)

```scala
package scalaeng.ch02

@main def hello(): Unit =
  val scalaVersion = scala.util.Properties.versionNumberString
  val javaVersion  = System.getProperty("java.version")
  println(s"Xin chào Scala $scalaVersion trên JVM $javaVersion")
```

## 8. Giải thích code

- `package scalaeng.ch02` — không gian tên, giúp tránh trùng tên class.
- `@main def hello(): Unit` — annotation `@main` của Scala 3 biến method thành điểm vào chương trình.
  Compiler sinh ra một class tên `scalaeng.ch02.hello` có method `main` chuẩn của JVM.
- `Unit` — kiểu trả về khi hàm chỉ có tác dụng phụ (in ra màn hình), tương tự `void` của Java.
- `scala.util.Properties.versionNumberString` — lấy phiên bản Scala library đang chạy.
- `System.getProperty("java.version")` — gọi trực tiếp API Java: minh họa khả năng tương tác Java.
- `s"...$scalaVersion..."` — string interpolation (học ở Chương 3).

## 9. Output mong đợi

```
Xin chào Scala 3.9.0 trên JVM 21.0.10
```

(Phiên bản JVM trên máy bạn có thể khác, ví dụ `21.0.5`.) Cách chạy: xem Chương 2.

## 10. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Output hiện `Xin ch?o` thay vì `Xin chào` | Terminal/JVM không dùng UTF-8 (hay gặp trên Windows) | Chạy JVM với `-Dstdout.encoding=UTF-8`; trên Windows bật UTF-8 cho console (`chcp 65001`) |
| `UnsupportedClassVersionError` | Chạy bytecode bằng JDK cũ hơn JDK đã dùng để biên dịch | Kiểm tra `java -version`; dùng JDK 21 thống nhất |
| Ví dụ trên mạng không biên dịch | Ví dụ viết cho Scala 2 | Kiểm tra cú pháp (`implicit`, `extends App`...) và phiên bản trong bài viết |

## 11. Cách debug

- Luôn kiểm tra **ba phiên bản** khi có lỗi lạ: `java -version`, phiên bản Scala trong build,
  phiên bản thư viện. Phần lớn lỗi người mới gặp là lệch phiên bản.
- Đọc lỗi compiler **từ dòng đầu tiên**; lỗi phía sau thường là hệ quả của lỗi đầu.
- Thêm `-explain` vào lệnh biên dịch để compiler giải thích chi tiết lỗi.

## 12. Bài tập

1. **(Cơ bản)** Vẽ lại sơ đồ "source → bytecode → JVM" và giải thích bằng lời vai trò của `.class` và `.tasty`.
2. **(Cơ bản)** Liệt kê 3 thư viện Java bạn từng dùng (hoặc từng nghe) và giải thích vì sao Scala dùng được chúng.
3. **(Trung bình)** Với mỗi tình huống, chọn Scala/Java/Python/SQL và giải thích:
   a) Tính tổng doanh thu theo tháng từ bảng `orders` đã sạch trong data warehouse.
   b) Xây API thanh toán cho đội 30 kỹ sư Java.
   c) Job xử lý 2 TB log mỗi ngày với logic parse phức tạp, chạy trên cluster Spark có sẵn.
   d) Phân tích thử nghiệm A/B trong notebook.
4. **(Nâng cao)** Một đồng nghiệp đề xuất: "Hệ thống mới viết hết bằng Scala 3, kể cả job Spark."
   Viết một đoạn phản hồi (8–12 câu) nêu rõ vấn đề kỹ thuật và đề xuất phương án thay thế.

## 13. Đáp án và lời giải

Xem [`solutions/ch01-ch02-solutions.md`](../solutions/ch01-ch02-solutions.md).
Hãy tự làm trước khi đọc đáp án.

## 14. Câu hỏi kiểm tra

1. "Kiểu tĩnh" mang lại lợi ích gì cho một backend service đang chạy production?
2. Vì sao chương trình Scala cần `scala-library.jar` trên classpath khi chạy bằng lệnh `java`?
3. Nêu 2 lý do Spark 4.x chưa chạy với Scala 3.
4. JIT compiler ảnh hưởng thế nào tới hiệu năng một ứng dụng chạy lâu so với một CLI chạy 50 ms?
5. Vì sao "Java quá dài dòng" không còn là lý do mạnh nhất để chọn Scala?

## 15. Mini project

Không có mini project code. Thay vào đó: viết một **ghi chú quyết định kiến trúc** (Architecture Decision Record)
1 trang: "Dùng Scala ở đâu trong hệ thống thương mại điện tử của chúng ta", dùng sơ đồ ở mục 6 làm khởi điểm.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Introduction: https://docs.scala-lang.org/scala3/book/introduction.html
- Scala 3 Book — Scala Features: https://docs.scala-lang.org/scala3/book/scala-features.html
- Scala 3 Migration Guide: https://docs.scala-lang.org/scala3/guides/migration/compatibility-intro.html
- Thông báo Scala 3.9 LTS: https://www.scala-lang.org/news/3.9/
- Tài liệu Spark 4.1 (yêu cầu Scala): https://spark.apache.org/docs/4.1.1/

## 17. Checklist tự đánh giá

- [ ] Tôi giải thích được Scala biên dịch thành gì và chạy ở đâu.
- [ ] Tôi nêu được ít nhất 5 khác biệt Scala 2 / Scala 3.
- [ ] Tôi biết vì sao Scala dùng được thư viện Java.
- [ ] Tôi biết Spark hiện yêu cầu Scala 2.13 và hiểu sơ bộ lý do.
- [ ] Tôi có thể biện luận khi nào **không** nên chọn Scala.
