# Chương 8C — Lập trình kiểu nâng cao: inline, match types, derivation và macros

> **Phiên bản:** Scala 3.9.0 · JDK 21 (đã chạy 21.0.12.1) · Dependency runtime: không có ngoài thư viện chuẩn · Test: munit 1.2.4 · **Kiểm chứng:** đã biên dịch & chạy, 12/12 test lab và 13/13 test lời giải pass, 0 cảnh báo compiler với `-Werror`; 7/7 ca lỗi biên dịch đã chạy riêng ngày 17/10/2026. Claude Code review: ĐẠT; đã sửa 4 MINOR.

**Khi nào KHÔNG cần các kỹ thuật này?** Khi một hàm thường, một `given` viết tay hoặc một smart constructor trả `Either` đã diễn đạt rõ nghiệp vụ, hãy dùng chúng. Một dịch vụ nhận đơn hàng hay pipeline đọc sự kiện không cần tự viết macro. Người học Backend (BE) và Data Engineering (DE) có thể bỏ qua chương này lần đầu mà không hổng kiến thức các chương sau. Chương thuộc mức **[CS] — chuyên sâu, học sau Ch16**; giá trị chính là đọc hiểu derivation trong thư viện và biết giới hạn của API kiểm tra lúc biên dịch.

Ở [Ch8B](ch08b-contextual-abstractions.md), ta tự liệt kê các trường của `OrderReport` khi viết `CsvEncoder`. Nay ta thử sinh phần lặp lại từ cấu trúc case class. Việc chọn cột nào được phép xuất, định dạng tiền và tương thích schema vẫn là quyết định của kỹ sư.

## 1. Mục tiêu học tập

- Phân biệt `inline def`, `inline val`, tham số `inline`, `inline if` và `inline match`; chỉ ra điều gì xảy ra lúc compile và lúc chạy.
- Dùng `compiletime.error`, `constValue`, `erasedValue`, `summonInline` trong một API nhỏ, có test chấp nhận và từ chối code.
- Đọc được match type, giải thích reduction (rút gọn kiểu) và vì sao một kiểu abstract có thể làm reduction dừng lại.
- Đọc tuple type `head *: tail`, `EmptyTuple`, `Tuple.Map`; giữ kiểu riêng của từng cột.
- Tự viết một `CsvEncoder.derived` dùng `Mirror.ProductOf` và áp dụng bằng `derives` cho case class đơn hàng.
- Đọc macro có quotes/splices, biết tách nơi định nghĩa và nơi gọi; giữ validation runtime cho dữ liệu từ bên ngoài.
- Chọn kỹ thuật dựa trên độ rõ ràng, chi phí compile, code size và công sức bảo trì; không suy ra hiệu năng chỉ từ từ khóa.

## 2. Kiến thức tiên quyết

Đọc Ch8A về generics, type alias, opaque type và Ch8B về type class, `given`/`using`, companion và CSV. Cần Ch9–10 để hiểu project/test, Ch11 để phân biệt compiler với JVM/JIT và nên học xong Ch16 để đã trải nghiệm việc dùng thư viện thực tế. Chương không dựa vào kiến thức Cats Effect để thực hiện lab.

Nếu chưa viết được `def export[A](rows: List[A])(using CsvEncoder[A])`, quay lại Ch8B. Không dùng lập trình kiểu để che lấp một mô hình domain chưa rõ.

## 3. Lý thuyết

### 3.1. Inline: đưa phần thân vào nơi dùng

**Vấn đề và vì sao:** một cấu hình nằm ngay trong mã nguồn như batch size `100` có thể kiểm tra trước khi triển khai. Với một cờ cố định, compiler cũng có thể bỏ nhánh không cần. Một `def` thường không cung cấp giao ước rằng nó sẽ được mở rộng tại nơi gọi.

Trong lab:

```scala
inline val defaultBatchSize = 100

inline def positiveBatchSize(inline value: Int): Int =
  inline if value > 0 then value
  else scala.compiletime.error("batch size must be positive")
```

`inline val` yêu cầu biểu thức hằng; nó không đọc biến môi trường hay file cấu hình. `inline def` được mở rộng tại nơi gọi. Tham số `inline value` giữ biểu thức đối số để compiler dùng trong lần mở rộng đó. `inline if` yêu cầu điều kiện có thể quyết định lúc compile sau khi thay đối số vào. `positiveBatchSize(100)` hợp lệ; giá trị `0` làm nhánh `error` được chọn và compilation thất bại. Đây là quy tắc nhỏ, không phải một constructor bảo đảm mọi `Int` khác trong chương trình đều dương.

`inline def` tự nó **không bắt mọi đối số thành hằng**. `InlineRules.twice(next())` vẫn hợp lệ: tham số thường giữ ngữ nghĩa đánh giá một lần. Ngược lại `twiceInline(next())`, với phần thân `value + value`, thay biểu thức ở cả hai chỗ dùng. Test chứng minh số lần gọi lần lượt là một và hai. Vì vậy tránh truyền side effect vào tham số `inline`; nó không đồng nghĩa với memoization hay một tham số thường.

`inline match` chọn nhánh theo biểu thức/kiểu sau khi mở rộng. `formatName("csv")` chọn `"text/csv"`, `"json"` chọn `"application/json"`; nhánh mặc định gọi `compiletime.error`. `inline match` cũng không phải một bộ phân nhánh runtime tùy ý: compiler phải rút gọn được nó ở nơi gọi. Mục 10 minh họa giới hạn tương tự với `inline if`.

**Thay thế:** dùng `def validateBatch(n: Int): Either[String, Int]` khi `n` đến từ HTTP, biến môi trường hoặc file. `require` là kiểm tra runtime bằng exception, phù hợp một giao ước nội bộ khác, không tương đương lỗi compile. Cờ thay đổi khi vận hành phải dùng nhánh runtime.

**Ưu/nhược và khi dùng:** inline có ích cho nhánh biết trước, bộ khung derivation và API yêu cầu hằng. Nhưng phần thân có thể được lặp ở nhiều nơi, làm bytecode lớn hơn và compile lâu hơn; JVM JIT cũng có thể inline các hàm thường. Lab không benchmark nên **chưa kiểm chứng tăng tốc**. Chỉ tối ưu hot path sau profiling và benchmark có điều kiện tương đương. Lỗi thiết kế thường gặp là inline mọi thứ, đưa vòng lặp lớn vào mỗi call site, hoặc biến cấu hình vận hành thành hằng lúc build. Xem [Scala Reference — Inline](https://docs.scala-lang.org/scala3/reference/metaprogramming/inline.html).

### 3.2. Bốn công cụ trong `scala.compiletime`

| Công cụ | Vấn đề giải quyết | Cách dùng trong lab | Giới hạn / lựa chọn thay thế |
|---|---|---|---|
| `error` | Muốn báo lỗi rõ khi một nhánh compile không hợp lệ | Chặn batch không dương, format không hỗ trợ | Không bắt lỗi dữ liệu runtime; dùng `Either` |
| `constValue[T]` | Muốn lấy giá trị mà singleton type biểu diễn | `partitionCount[4]` lấy `4`; tên trường từ kiểu `"orderId"` | `Int` nói chung không đại diện một hằng; truyền giá trị thường nếu không có singleton |
| `erasedValue[T]` | Muốn xét hình dạng **kiểu** trong inline match | Phân biệt `EmptyTuple` và `head *: tail` | Không tạo giá trị T thật và không được còn trong code runtime; không dùng như giá trị mặc định |
| `summonInline[TC[T]]` | Chỉ biết kiểu trường sau khi inline match được mở rộng | Tìm `CellEncoder[head]` cho từng trường | Thiếu instance thì lỗi compile; giữ `summon`/`using` thông thường nếu đã biết kiểu tại chỗ |

Singleton type là kiểu biểu diễn một giá trị cụ thể, như `4` hoặc `"orderId"`. `constValue` đi từ thông tin kiểu ấy sang giá trị. `erasedValue` là công cụ giúp chọn nhánh theo kiểu; đừng hình dung nó tạo `null` rồi thử kiểu lúc chạy. `summonInline` dời việc tìm given đến lúc mở rộng, không dò classpath runtime.

**Ưu/nhược:** tổ hợp này giúp sinh code có kiểm tra kiểu, nhưng chẩn đoán có thể đi qua nhiều bước inline. Chỉ dùng trong helper nhỏ, tên rõ và có ca lỗi tối thiểu. Lỗi thiết kế là tự động tạo default cho mọi kiểu dữ liệu nghiệp vụ hoặc dựa vào `toString` làm định dạng xuất mặc định. Nguồn khái niệm: [Compile-time operations](https://docs.scala-lang.org/scala3/reference/metaprogramming/compiletime-ops.html).

### 3.3. Match types: một hàm ở mức kiểu

**Vấn đề:** API cần diễn tả "kiểu dữ liệu bên trong một container" mà không hạ mọi thứ xuống `Any`. `Payload` là tên kiểu có kết quả phụ thuộc vào kiểu đầu vào:

```scala
type Payload[X] = X match
  case Option[t] => t
  case List[t]   => t
  case _         => X
```

`Payload[Option[String]]` rút gọn thành `String`; `Payload[List[Int]]` thành `Int`; `Payload[Long]` thành `Long`. Lab dùng bằng chứng `=:=` để compiler xác nhận các quan hệ này. Định nghĩa trên chỉ bóc **một lớp**: `Payload[List[Option[Int]]]` là `Option[Int]`. Nó không tự tạo một hàm lấy giá trị, cũng không giải quyết danh sách rỗng hay `None`.

**Reduction:** compiler xét các case theo thứ tự. Với selector đã khớp một case, nó lấy kiểu bên phải. Để đi qua một case không khớp, compiler cần chứng minh selector và pattern **disjoint** — không có giá trị chung. Với `A` chưa có ràng buộc, không thể chắc `A` là `Option[t]`, cũng không thể chắc nó không phải `Option[t]`. Vì vậy không được nhảy thẳng xuống `case _`. Diagnostic thật ở mục 10 cho thấy chính chỗ bị kẹt. Chi tiết thuật toán: [Match Types — Reduction](https://docs.scala-lang.org/scala3/reference/new-types/match-types.html#match-type-reduction).

**Thay thế:** overload cho một số ít kiểu; type class có associated type khi cần cả kiểu và thuật toán; ADT + `Either` khi kết quả nghiệp vụ cần biểu diễn nhiều trường hợp runtime. **Ưu/nhược:** match type cho chữ ký chính xác nhưng khó suy luận khi generic quá rộng; đệ quy sâu còn tăng chi phí compiler. Dùng khi quan hệ giữa các kiểu là phần hữu ích của API. Lỗi thiết kế là ép `asInstanceOf[Payload[A]]` để "sửa" lỗi chưa chứng minh được: cast không tạo ra phép bóc container đúng.

### 3.4. Tuple types: nối và biến đổi từng cột

`String *: Int *: EmptyTuple` diễn tả tuple có hai phần tử; `*:` là cons ở mức kiểu, thêm đầu vào đuôi. Dạng ngắn là `(String, Int)`. Ở mức giá trị, `"ORD-1" *: 2 *: EmptyTuple` dựng tuple tương ứng. Một phần tử dùng `Tuple1[String]`, không phải `(String)`.

`Tuple.Map[(String, Int), Option]` cho kiểu `(Option[String], Option[Int])`. Lab dùng polymorphic function (hàm đa hình) `[A] => (value: A) => Option(value)` để map **giá trị**; match type chỉ tính kiểu, không tự bọc `Some`.

```scala
def optionalRow[Ts <: Tuple](row: Ts): Tuple.Map[row.type, Option] =
  row.map([A] => (value: A) => Option(value))
```

Kết quả `Tuple.map` trên Scala 3.9.0 giữ kiểu singleton của receiver. Chữ ký phụ thuộc `row.type` giữ đúng quan hệ đó; viết kiểu trả về generic `Tuple.Map[Ts, Option]` có thể kẹt reduction giữa singleton receiver và `Ts`. Ở nơi gọi cụ thể, test gán được thẳng vào `(Option[String], Option[Int])`. Không cần cast. `Option(value)` trả `None` nếu giá trị là `null`; lab dùng dữ liệu không null và không hứa rằng mọi ô đều là `Some`.

**Vấn đề và vì sao:** metadata của `Mirror` biểu diễn danh sách kiểu trường và tên trường bằng tuple types. Ta cần đi qua từng đầu `head` rồi phần `tail`. **Thay thế:** một case class có tên là lựa chọn dễ đọc cho domain; `List[String]` hợp với số cột động nhưng mất kiểu từng vị trí. **Ưu/nhược:** tuple types giúp generic programming chính xác; tuple dài trong API nghiệp vụ khiến người dùng phải nhớ vị trí. Dùng tuple ở bên trong derivation hoặc adapter nhỏ. Lỗi thiết kế là thay case class đơn hàng bằng một tuple mười cột chỉ vì có thể viết `Tuple.Map`.

### 3.5. Mirror và `derives`: sinh phần lặp của type class

**Vấn đề và vì sao:** Ch8B viết `header` và `encode` bằng tay. Thêm trường vào case class nhưng quên thêm vào encoder có thể làm mất cột mà không lỗi compile. Trong lab này, ta quyết định xuất tất cả trường của một DTO báo cáo phẳng nên có thể sinh phần khung.

`Mirror.ProductOf[A]` cung cấp hình dạng của product type như case class: `MirroredElemLabels` giữ tên trường, `MirroredElemTypes` giữ kiểu trường. `Mirror.SumOf[A]` dùng cho sum type như enum; lab **chỉ triển khai product**, chưa triển khai discriminator hay schema cho các nhánh sum.

```scala
inline def derived[A <: Product](using mirror: Mirror.ProductOf[A]): CsvEncoder[A] =
  productEncoder[A](
    TypeTools.labels[mirror.MirroredElemLabels],
    cellWriters[mirror.MirroredElemTypes]
  )
```

Đây là trích đoạn trong companion `CsvEncoder`; hai helper nội bộ nằm cùng file. `labels` dùng `constValue` lấy từng tên, còn `cellWriters` dùng `summonInline[CellEncoder[head]]` lấy encoder cho từng ô. Câu `case class OrderReport(...) derives CsvEncoder` yêu cầu compiler sinh given gọi `CsvEncoder.derived[OrderReport]`. Given nằm trong companion nên người gọi `CsvWriter.write(reports)` không cần import instance thủ công. Tổng quan cơ chế: [Type Class Derivation](https://docs.scala-lang.org/scala3/reference/contextual/derivation.html).

Lab tách `CellEncoder[A]` (đúng một ô, chưa escape) khỏi `CsvEncoder[A]` (một dòng gồm nhiều ô). Nhờ vậy `None` luôn là một ô rỗng và số ô khớp số header. `BigDecimal` dùng `toPlainString`; `Option[A]` yêu cầu có `CellEncoder[A]`. Trường lồng case class chưa có cell encoder sẽ bị từ chối; không âm thầm flatten hoặc `toString`.

**Ranh giới an toàn:** writer nội bộ nhận `Any` từ `Product.productIterator`, rồi cast về kiểu trường đã được Mirror xác định cùng vị trí. Cast được gom vào helper private; tính đúng dựa trên thứ tự chung của tuple kiểu và product được compiler sinh. Không dùng helper này để nhận một danh sách giá trị tùy ý. Hai danh sách phải cùng số trường và thứ tự; `zip` không tự xác nhận invariant đó. Code sinh được vẫn tạo list, iterator và chuỗi lúc chạy; derivation không đồng nghĩa "không cấp phát".

**Thay thế:** giữ instance viết tay của Ch8B khi cần tên cột khác (`order_id` thay cho `orderId`), bỏ dữ liệu nhạy cảm hoặc flatten có chủ đích. Dùng thư viện derivation khi cần annotations, generic recursion, sum types và báo lỗi tốt hơn. **Ưu/nhược:** ít code lặp và lỗi thiếu instance xuất hiện sớm, nhưng rename/reorder field sẽ đổi CSV. Chỉ derive trên DTO xuất dữ liệu có schema được review; đừng derive thẳng entity chứa email/token. Không tự động tái cấu trúc schema bên ngoài theo refactor nội bộ.

### 3.6. Macro nhỏ: kiểm tra tên topic cố định

**Vấn đề:** một topic được ghi cứng như `"bad topic"` có thể được phát hiện trước khi chạy service. Rule có kiểm tra ký tự và độ dài, khó đọc nếu ghép nhiều inline match. Macro cho phép chạy thuật toán validation bình thường **trong compiler**, rồi tạo code kết quả.

Trong [TopicLiteral.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/TopicLiteral.scala), API công khai là:

```scala
inline def checked(inline value: String): String = ${ checkedImpl('value) }
```

`'value` quote biểu thức đối số thành `Expr[String]`. `${ ... }` splice kết quả của implementation vào chương trình. Trong implementation, `value.value` lấy `Option[String]` khi biểu thức là hằng mà cơ chế quoted value nhận được; không phải mọi biểu thức Scala đều được tính tại compile time. `Expr(text)` dựng code cho chuỗi hằng. Nhánh hợp lệ dùng quote `'{ ... }` chứa splice:

```scala
case Some(text) if valid(text) =>
  '{
    val topic = ${ Expr(text) }
    topic
  }
```

Đoạn quote tạo code sẽ chạy ở chương trình đích; `valid(text)` được gọi ở compiler. Sau expansion, chương trình nhận chuỗi hằng, không phải lời gọi `parse` mỗi lần. Với literal sai hoặc giá trị không biết trước, `report.errorAndAbort` đặt lỗi vào biểu thức người dùng truyền. Lab nhận 1–249 ký tự ASCII chữ/số và `.`/`_`/`-`, loại `.` và `..`. Đây là **chính sách tên của lab**, không xác minh broker, quyền truy cập, topic có tồn tại hay Kafka configuration.

Nơi **gọi** macro phải ở source file khác với nơi định nghĩa implementation; public inline wrapper và implementation có thể ở cùng `TopicLiteral.scala`. Scala CLI biên dịch hai file trong cùng project được, không cần tách thành hai module sbt. Ca `SameFile` ở mục 10 cho thấy điều gì xảy ra nếu gọi ngay trong file định nghĩa. Đây là giới hạn staging: compiler cần implementation khả dụng trước khi thực thi nó. Xem [Scala Reference — Macros](https://docs.scala-lang.org/scala3/reference/metaprogramming/macros.html).

**Thay thế:** `TopicLiteral.parse(value): Either[String, String]` dùng cùng predicate cho input runtime; `inline if` đủ cho một kiểm tra số nguyên nhỏ; generator ngoài compiler hợp với schema được quản lý riêng. **Ưu/nhược:** macro diễn đạt rule tùy chỉnh và báo lỗi sớm, nhưng người bảo trì phải hiểu `Expr`, `Quotes`, staging và diagnostics. Với cross-version, không mang nguyên Scala 2 macro sang Scala 3; cần kiểm tra compatibility của compiler/quoted API khi nâng phiên bản. Lab chỉ kiểm chứng Scala 3.9.0/JDK 21, không khẳng định đã chạy trên các phiên bản khác. Không đọc mạng, database hay bí mật từ macro: build nên tái lập và người dùng không kỳ vọng validation gọi dịch vụ ngoài.

### 3.7. Chọn kỹ thuật và tính chi phí cho đồng đội

| Kỹ thuật | Thời điểm / đầu vào | Điểm mạnh | Giá phải trả và tình huống dùng |
|---|---|---|---|
| Hàm / given viết tay | Compile kiểm tra chữ ký; logic chạy runtime | Dễ đọc, debug và đổi hành vi từng trường | Có lặp; phù hợp phần lớn code ứng dụng, schema ổn định |
| `inline` + compiletime helpers | Expansion theo đối số/kiểu tại compile | Nhánh biết trước, derivation cấu trúc đơn giản | Code size, thời gian compile, chẩn đoán qua inline stack; helper ngắn |
| Quotes macro | Implementation chạy trong compiler, sinh `Expr` | Validation/sinh code cần phân tích biểu thức | Staging, API compiler, test negative và nâng version; chỉ khi inline thường quá hạn chế |
| Runtime reflection | Dò thông tin class/member khi chạy | Làm việc với kiểu chỉ biết khi tải plugin hoặc đọc cấu hình | Có thể lỗi muộn; giới hạn bởi erasure/quyền truy cập; cần cache metadata, không giả định rẻ |
| Code generation ngoài compiler | Đọc schema/file rồi sinh source trước compile | Source sinh ra review được, có thể phục vụ nhiều ngôn ngữ | Cần generator version, build hook và tránh generated files bị cũ; hợp với schema là nguồn sự thật |

Không có benchmark trong chương để xếp hạng tốc độ. Derivation dùng Mirror tạo metadata/encoder ở compile và khởi tạo instance, còn encode từng dòng vẫn chạy runtime. Runtime reflection trong bảng là reflection JVM; `quotes.reflect` của macro hoạt động trong compiler, không phải cùng cơ chế chạy ở service.

Trước khi chọn macro, một đồng đội mới nên đọc được chữ ký công khai và hiểu cách sửa khi thiếu instance mà không cần mở AST. Giữ runtime API nhỏ, tests minh họa lỗi và một đường chạy không cần Docker. Khi chính sách chỉ có hai trường hợp, instance viết tay có thể có tổng chi phí thấp hơn một framework derivation.

### 3.8. Liên hệ thư viện thực tế đã kiểm tra

Đã kiểm tra **circe-generic_3 0.14.17** có artifact Scala 3 trên [Maven Central POM](https://repo.maven.apache.org/maven2/io/circe/circe-generic_3/0.14.17/circe-generic_3-0.14.17.pom). [Mã nguồn `semiauto` tại tag v0.14.17](https://github.com/circe/circe/blob/v0.14.17/modules/generic/shared/src/main/scala-3/io/circe/generic/semiauto.scala) dùng `inline` với `Mirror.Of[A]` để gọi derivation cho codec. Đây là một nơi người học có thể gặp các kỹ thuật của chương trong thư viện JSON. Ngày kiểm tra: **17/10/2026**.

Lab không thêm circe dependency và chưa chạy kiểm thử tích hợp circe với lab; artifact `_3` cùng mã nguồn Scala 3 là bằng chứng đã kiểm tra, không phải tuyên bố mọi compiler Scala 3 đều tương thích. Hành vi CSV ở đây là code tự viết, không phải API của circe.

Khi đọc [Ch13](../05-backend/ch13-choosing-a-framework.md) và [Ch14](../05-backend/ch14-rest-apis.md), hãy đối chiếu cách derive codec JSON của circe với việc tìm instance cho từng trường ở đây. Ở [Project 2](../14-projects/p2-backend-service/README.md), xem thêm schema derivation của Tapir tại biên HTTP: cùng mục đích giảm code lặp theo cấu trúc kiểu, còn codec, schema và validation vẫn có trách nhiệm riêng.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong chương |
|---|---|
| Metaprogramming | Viết chương trình phân tích/sinh chương trình khác |
| Call site / inline expansion | Nơi gọi / bước đưa phần thân inline vào nơi gọi |
| Singleton type | Kiểu biểu diễn một giá trị cụ thể, như `4` |
| Match type / reduction | Kiểu chọn theo pattern / rút gọn về kiểu kết quả |
| Disjoint types | Các kiểu không có giá trị chung, cho phép bỏ qua một pattern |
| Tuple cons `*:` | Thêm kiểu/phần tử đầu vào một tuple đuôi |
| Typeclass derivation | Sinh instance của type class theo cấu trúc kiểu |
| Product / sum | Cấu trúc gồm các trường / lựa chọn giữa các nhánh |
| Mirror | Thông tin cấu trúc kiểu do compiler cung cấp cho derivation |
| Quote / splice / staging | Dựng code / chèn code / tách các giai đoạn compiler và runtime |
| `Expr[T]` / `Quotes` | Biểu thức có kiểu T / ngữ cảnh thao tác code trong macro |
| Negative compilation test | Test xác nhận một đoạn code phải bị compiler từ chối |
| Code size | Kích thước code sinh ra, có thể tăng khi inline nhiều lần |

## 5. Ví dụ đơn giản

Các biểu thức sau nằm trong lab/demo hoặc được kiểm tra ở `AdvancedTypesSuite`; chúng dùng Scala 3.9.0, JDK 21, không có runtime dependency ngoài thư viện chuẩn:

```scala
val size: Int = InlineRules.positiveBatchSize(100)
val payload: TypeTools.Payload[Option[String]] = "ORD-1"
val optional: (Option[String], Option[Int]) = TypeTools.optionalRow(("ORD-1", 2))
val topic: String = TopicLiteral.checked("orders.created")
```

`size` và `topic` được kiểm tra khi compile; `payload` chứng minh quan hệ kiểu nhưng không tự bóc `Option` nào. `optional` là giá trị tuple được biến đổi, giữ kiểu từng vị trí. Mục 7 có lệnh chạy, mục 9 có output thật; không cần copy từng đoạn vào REPL vì macro cần quy tắc tách file.

## 6. Ví dụ thực tế (BE và DE)

DTO `OrderReport(orderId, customer, total, paidOn)` dùng chung cho báo cáo đơn hàng của API nội bộ (BE) và file giao cho downstream (DE). Thêm `derives CsvEncoder` để header theo tên trường và dùng instance của từng ô. `CsvWriter` xử lý quote, dấu phẩy, CR, LF; dòng được nối bằng CRLF, không thêm newline cuối. Giá trị `None` tạo ô rỗng và số `1E+3` tạo `1000`.

**CSV injection (chèn công thức):** escape CSV không ngăn bảng tính diễn giải ô thành công thức. File cho **người** mở bằng Excel/Sheets cần chính sách riêng với ô bắt đầu bằng `=`, `+`, `-`, `@`, cùng ký tự điều khiển và biến thể liên quan; kiểm thử trên ứng dụng đích, kể cả lưu rồi mở lại. File cho **máy** downstream phải giữ dữ liệu theo hợp đồng, không tự thêm tiền tố bảo vệ làm đổi giá trị. Lab chưa triển khai chính sách xuất cho bảng tính; xem [OWASP — CSV Injection](https://community.owasp.org/attacks/CSV_Injection), kiểm tra ngày 17/10/2026.

Hợp đồng của lab cố ý nhỏ: không parse CSV ngược lại, không phân biệt `None` với chuỗi rỗng trong file, không quản lý schema evolution và không xử lý input `null`. Dùng DTO được review để tránh vô tình xuất dữ liệu mới khi ai đó thêm trường vào entity. Nếu downstream đã chốt header `order_id`, giữ encoder thủ công của Ch8B hoặc thêm mapping có test; derivation không được tự ý đổi hợp đồng.

Topic hằng `orders.created` được macro kiểm tra ở build. Topic do người vận hành cung cấp phải qua `parse` lúc khởi động, sau đó ứng dụng tự thực hiện kiểm tra Kafka cần thiết. Mẫu này cho thấy compile time và runtime bổ sung nhau.

## 7. Code đầy đủ

Lab là **một module**, không cần `build.sbt`. Mọi package bắt đầu bằng `vn.scalaeng.ch08c`.

| File | Vai trò |
|---|---|
| [project.scala](ch08c-lab/project.scala) | Pin Scala/JVM/test; bật `-deprecation -feature -Wunused:all -Werror`, UTF-8 cho output Windows |
| [InlineRules.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/InlineRules.scala) | Inline val/def/if/match/parameter, kiểm tra batch và format |
| [TypeTools.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/TypeTools.scala) | Match type, tuple labels, `Tuple.Map` |
| [CsvEncoder.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/CsvEncoder.scala) | Cell instances, product derivation, CSV writer và OrderReport |
| [TopicLiteral.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/TopicLiteral.scala) | Macro và parser runtime dùng chung predicate |
| [Ch08cDemo.scala](ch08c-lab/src/main/scala/vn/scalaeng/ch08c/Ch08cDemo.scala) | Demo chạy được, gọi macro từ file khác |
| [AdvancedTypesSuite.scala](ch08c-lab/src/test/scala/vn/scalaeng/ch08c/AdvancedTypesSuite.scala) | 12 test, gồm ca compile bị từ chối |
| [negative/](ch08c-lab/negative/) và [verify-negative.ps1](ch08c-lab/verify-negative.ps1) | 7 source cố ý sai và script chạy compiler thật, giữ log trong `.cache/t22/negative/` |

Chạy từ **gốc repo** trên PowerShell, sau khi cài JDK/Scala CLI theo Ch2:

```powershell
$env:COURSIER_CACHE = Join-Path (Get-Location) '.cache/coursier'
scala-cli.bat test 03-type-system/ch08c-lab --server=false
scala-cli.bat run 03-type-system/ch08c-lab --server=false --main-class vn.scalaeng.ch08c.Ch08cDemo
scala-cli.bat test solutions/ch08c-code --server=false
scala-cli.bat run solutions/ch08c-code --server=false --main-class vn.scalaeng.ch08c.solutions.SolutionsDemo
powershell -NoProfile -ExecutionPolicy Bypass -File 03-type-system/ch08c-lab/verify-negative.ps1
```

Lần đầu cần mạng để tải compiler/JDK/munit; test không gọi mạng, Kafka, database hay Docker. `--server=false` tránh Bloop trong sandbox. Scala CLI 1.9.1 có thể in hint munit mới hơn; repo cố định 1.2.4. Muốn log gọn có thể thêm `--suppress-outdated-dependency-warning`; hint cập nhật dependency khác với cảnh báo compiler. `.scala-build/`, `.bsp/` và `.cache/` đã được ignore ở gốc repo.

## 8. Giải thích code

1. `positiveBatchSize` chỉ dùng `error` trong nhánh không hợp lệ. Khi truyền hằng hợp lệ, nhánh đó biến mất sau expansion; dữ liệu runtime không đủ để quyết định `inline if`.
2. `labels` đệ quy trên tuple **kiểu**, không duyệt dữ liệu đơn hàng. `EmptyTuple` dừng, `head *: tail` lấy tên hiện tại và tiếp tục.
3. `cellWriters` dùng kiểu `head` đã cụ thể để tìm `CellEncoder[head]`. Instance `Option[LocalDate]` được ghép từ instance `LocalDate`; thiếu instance của trường nào thì compile dừng ở đó.
4. `derived` ghép metadata cùng thứ tự thành một `CsvEncoder[A]`. Factory `productEncoder` là `def` thường để anonymous class không bị khai báo trong thân inline ở mọi nơi gọi. Không cần tự bật cờ bỏ qua cảnh báo.
5. `encode` tạo các ô **chưa escape**; `CsvWriter` escape đúng một lần cả header và ô. Nếu escape trong cả `CellEncoder` lẫn writer, dấu nháy sẽ bị nhân đôi sai.
6. `checkedImpl` nhận code của đối số, trích constant, kiểm tra rồi sinh expression. File demo chỉ nhìn thấy API `checked`, không thao tác `Quotes`.
7. `typeCheckErrors` ở test biến một đoạn code sai thành dữ liệu chẩn đoán để assertion kiểm tra. Nó không làm project test bị fail compile. Các fixture `.scala.txt` bổ sung lần gọi compiler độc lập cho lỗi được trích trong bài.

## 9. Output mong đợi

Output **thật** từ demo ngày 17/10/2026, Scala CLI 1.9.1, JDK được directive `jvm 21` chọn là **21.0.12.1**. JDK của lệnh `java` ngoài shell có thể là bản khác. Nội dung stdout dưới đây bỏ mã màu, không sửa giá trị:

```text
Scala=3.9.0; Java=21.0.12.1
batch=100
format=text/csv; partitions=4
labels=orderId,total
optional=(Some(ORD-1),Some(2))
topic=orders.created
orderId,customer,total,paidOn
ORD-1,Nguyễn An,495000,2026-10-01
ORD-2,"Công ty ""ABC"", HN",1000,
```

Tóm tắt munit thật (thời gian thay đổi giữa các lần chạy):

```text
Test run vn.scalaeng.ch08c.AdvancedTypesSuite finished: 0 failed, 0 ignored, 12 total 0.122s
Test run vn.scalaeng.ch08c.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 13 total 0.116s
```

Không có integration test bị skip. Tất cả 25 test là test local; nhiều assertion dùng compiler để kiểm tra các đoạn code sai.

## 10. Lỗi thường gặp

Các đoạn diagnostic dưới đây được trích **nguyên văn phần thông báo** của compiler Scala 3.9.0 chạy trên fixture trong `ch08c-lab/negative/`. Đã lược bỏ đường dẫn tuyệt đối, caret và inline stack khi không cần; không dịch thông báo. Mỗi ca chạy `compile --server=false`, exit code `1` đúng kỳ vọng. Fixture mang đuôi `.scala.txt` nên không làm hỏng lần test thông thường.

### 10.1. Dùng `A` như thể `Payload[A]` đã rút gọn

Trong [MatchStuck.scala.txt](ch08c-lab/negative/MatchStuck.scala.txt): `def broken[A](value: A): Payload[A] = value`.

```text
Found:    (value : A)
Required: vn.scalaeng.ch08c.TypeTools.Payload[A]

Note: a match type could not be fully reduced:

  trying to reduce  vn.scalaeng.ch08c.TypeTools.Payload[A]
  failed since selector A
  does not match  case Option[t] => t
  and cannot be shown to be disjoint from it either.
  Therefore, reduction cannot advance to the remaining cases

    case List[t] => t
    case _ => A
```

Ngay cả khi định gọi `broken[Long]`, phần thân `def` vẫn phải đúng cho mọi `A`. Sửa bằng chữ ký giữ `A` nếu đó là identity, hoặc viết API riêng có thuật toán cho từng container. Không thêm cast chỉ để ép qua compiler.

### 10.2. Vi phạm rule do `compiletime.error` đặt ra

[Batch.scala.txt](ch08c-lab/negative/Batch.scala.txt) gọi `InlineRules.positiveBatchSize(0)`:

```text
batch size must be positive
```

Đây là diagnostic compile do tác giả API đặt, không phải exception runtime. Sửa literal/config build thành số dương; dùng validator runtime nếu giá trị chưa biết khi build.

### 10.3. Đưa dữ liệu runtime vào `inline if`

[RuntimeInline.scala.txt](ch08c-lab/negative/RuntimeInline.scala.txt) có `def configuredBatch(value: Int): Int = InlineRules.positiveBatchSize(value)`:

```text
Cannot reduce `inline if` because its condition is not a constant value: value > 0
```

Không thể đổi một `val` đọc từ file thành hằng bằng cách thêm `inline` ở caller. Viết `Either` validation runtime; hoặc nếu chính caller là API nhận inline thì tiếp tục truyền inline tới call site thật, với điều kiện cuối cùng phải biết hằng.

### 10.4. Macro nhận literal sai hoặc biểu thức runtime

[BadTopic.scala.txt](ch08c-lab/negative/BadTopic.scala.txt) truyền `"bad topic"`:

```text
invalid topic literal
```

[RuntimeTopic.scala.txt](ch08c-lab/negative/RuntimeTopic.scala.txt) truyền tham số `value: String` của một `def` thường:

```text
topic must be a constant string
```

Ca đầu sửa tên topic theo policy; ca sau gọi `TopicLiteral.parse(value)` và xử lý `Left`. Macro không thể biết nội dung request tương lai.

### 10.5. Gọi macro ngay trong file định nghĩa implementation

[SameFile.scala.txt](ch08c-lab/negative/SameFile.scala.txt) khai báo wrapper, implementation và `val topic` gọi wrapper trong cùng file:

```text
Cannot call macro method inline$impl defined in the same source file
```

Cách sửa là chuyển lời gọi sang file khác như `Ch08cDemo.scala`. Không cần tách riêng public wrapper và implementation thành hai file.

### 10.6. Derive DTO có trường thiếu `CellEncoder`

Fixture thứ bảy, [MissingCellEncoder.scala.txt](ch08c-lab/negative/MissingCellEncoder.scala.txt), khai báo `case class Tagged(orderId: String, tags: List[String]) derives CsvEncoder`. Lab có encoder cho `String` nhưng chưa quy định cách ghi một danh sách vào một ô. Trích nguyên văn phần diagnostic và đầu inline stack từ lần chạy compiler thật:

```text
   |No given instance of type vn.scalaeng.ch08c.CellEncoder[List[String]] was found
   |----------------------------------------------------------------------------
   |Inline stack trace
   |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   |This location contains code that was inlined from CsvEncoder.scala:44
44 |        writer(summonInline[CellEncoder[head]]) :: cellWriters[tail]
   |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

Dòng thông báo đầu chỉ nguyên nhân: thiếu given `CellEncoder[List[String]]`. *Inline stack trace* là dấu vết các bước mở rộng inline ở compiler, không phải stack trace exception lúc chạy. Phần dưới cho thấy `summonInline` được mở rộng ở đâu khi `head` trở thành `List[String]`; các frame tiếp theo lần ngược qua đệ quy `cellWriters` tới `derived`. Vì vậy bắt đầu từ kiểu instance bị thiếu, rồi dùng trace để tìm helper liên quan; không cần sửa mọi dòng trong trace.

Có ba cách sửa tùy hợp đồng dữ liệu: viết given `CellEncoder[List[String]]` có chủ đích và khả dụng tại nơi derive, sau khi chốt danh sách được biểu diễn thế nào trong **một ô**; đổi DTO thành các trường đã có encoder hoặc tách thành nhiều dòng theo schema được thống nhất; hoặc viết `CsvEncoder[Tagged]` bằng tay và bỏ `derives`. Với encoder cho một ô, trả nội dung chưa escape để `CsvWriter` escape đúng một lần. Không thêm fallback `toString` chỉ để compiler im lặng: nó chưa xác định hợp đồng với downstream.

Mọi ví dụ chạy đúng ở mục 7 đã dùng cấu trúc tách file. Không đưa fixture sai vào thư mục `src/main/scala`.

## 11. Cách debug

Thu nhỏ lỗi theo từng tầng. Trước hết thử một kiểu cụ thể: `summon[Payload[Option[String]] =:= String]`. Nếu đúng, lỗi generic có thể nằm ở thiếu bằng chứng disjointness thay vì sai case cuối. Với derivation, thử `summon[CellEncoder[KiểuTrường]]` ở chỗ độc lập để xác định instance bị thiếu; đừng đọc cả inline stack trước khi biết trường nào gây lỗi.

Sau đó kiểm tra metadata: header đúng thứ tự không, tên cột có đổi theo case class không, số ô có bằng số header không. Test dữ liệu có dấu phẩy, quote, CR/LF, `None`, số dạng exponent và product rỗng. Khi đối chiếu file output trên Windows, đọc UTF-8; project đặt encoding stdout/stderr để giữ tiếng Việt khi redirect.

Với macro, bắt đầu từ literal hợp lệ rồi thử literal sai và tham số runtime ở **file khác**. `value.value` trả `None` là tình huống API phải xử lý có chủ đích. Chạy `verify-negative.ps1` để phân biệt lỗi mong muốn với việc compiler/JDK không khởi động được: script kiểm tra cả exit code lẫn chuỗi diagnostic. Khi nâng Scala, chạy lại toàn bộ ca dương/âm; không chỉ xem code có compile.

## 12. Bài tập (cơ bản → nâng cao)

Mỗi bài phải có test munit, bao gồm input hợp lệ và trường hợp compiler từ chối khi phù hợp. Không đặt code cố ý sai trực tiếp vào source được build.

1. **Cơ bản — batch có giới hạn.** Viết `BatchPolicy.bounded(inline size: Int)` nhận hằng từ `1` đến `10000` bằng `inline if`/`error`. Test cả hai biên, `0`, `10001` và đối số runtime. Giải thích vì sao function này chưa thay được validator HTTP.
2. **Cơ bản — match type đệ quy.** Viết `Leaf[X]` bóc lặp `List`/`Option` tới kiểu cuối. Chứng minh bằng `=:=` rằng `Leaf[List[Option[List[String]]]]` là `String` và `Leaf[Long]` là `Long`. Thử gán `A` vào `Leaf[A]` trong `def` generic rồi giải thích failure, không cast.
3. **Trung bình — tuple schema.** Viết `arity[Ts <: Tuple]` bằng `erasedValue`, `EmptyTuple`, `*:`. Viết `acceptedRow(row)` bọc từng cột thành `Either[String, A]`, có kiểu trả về `Tuple.Map`. Test tuple rỗng, tuple một trường, ba trường và test không thể đảo kiểu hai cột của kết quả.
4. **Trung bình — báo cáo sự kiện.** Tạo `EventReport(eventId: String, attempts: Int, accepted: Boolean, note: Option[String]) derives CsvEncoder`. Test header/order, `None`, chuỗi có dấu phẩy và case class rỗng. Thử thêm trường `Map[String, Int]` mà chưa có cell encoder; test phải xác nhận lỗi compile. Nêu rủi ro schema nếu rename `eventId`.
5. **Nâng cao — tự derive `Show`.** Tự viết `Show[A]` với instance `String`, `Int`; derive product từ `Mirror`. `OrderSummary("ORD-1", 2)` phải thành `OrderSummary(orderId=ORD-1,quantity=2)`. Test product rỗng và thiếu instance trường `BigDecimal`. Gom cast nội bộ, giữ phần tạo anonymous class ngoài inline. `Show` này chỉ để hiển thị, không dùng làm format dữ liệu ổn định.
6. **Nâng cao — macro phần trăm.** Trong file riêng, viết `PercentLiteral.checked(inline n: Int)` nhận hằng `0..100`; dùng `Expr.value` và `report.errorAndAbort`. Thêm `parse(n)` cho runtime. Test `0`, `100`, số giữa, `-1`, `101`, tham số runtime. So với lời giải inline của bài 1, giải thích vì sao rule này tự nó chưa đủ lý do dùng macro trong production.

## 13. Đáp án và lời giải

Xem [solutions/ch08c-solutions.md](../solutions/ch08c-solutions.md) để đọc giải thích sáu bài, gợi ý câu hỏi kiểm tra và mini project. Code ở [solutions/ch08c-code/](../solutions/ch08c-code/), không thay file bài học. Project lời giải dùng `using files` để tham chiếu source main của lab; chỉ chạy test của lời giải, không sao chép implementation lab sang một bản khác.

## 14. Câu hỏi kiểm tra

1. `inline def f(x: Int)` khác `inline def f(inline x: Int)` thế nào khi đối số có side effect?
2. Vì sao `inline if` với biến đọc từ môi trường không thể quyết định lúc build? Validator runtime nên trả gì?
3. Vì sao `constValue[4]` hợp lệ còn `constValue[Int]` không đại diện một hằng cụ thể?
4. `erasedValue[T]` có tạo một đối tượng T để dò kiểu runtime không? Vì sao không được dùng nó ngoài nhánh bị loại khi inline?
5. Vì sao `case _ => A` không đủ làm `Payload[A]` rút gọn khi A còn abstract?
6. `Tuple.Map` đổi kiểu và `.map` đổi giá trị liên quan nhau ra sao? Vì sao helper lab trả về kiểu phụ thuộc `row.type`?
7. `Mirror.ProductOf` cung cấp gì? `derives CsvEncoder` lấy instance cho kiểu trường từ đâu?
8. Tại sao `derives` không tự bảo đảm schema CSV tương thích với downstream, và không tự che thông tin nhạy cảm?
9. Phần nào của macro chạy trong compiler, phần nào của `'{ ... }` chạy ở chương trình đích? Vì sao nơi gọi cần khác file implementation?
10. Khi nào chọn instance viết tay, inline, macro, runtime reflection hoặc code generator? Đồng đội phải biết gì để debug lựa chọn đó?

## 15. Mini project — Export sự kiện đơn hàng có kiểm tra cấu hình

Xây một core thuần `EventExport.render(events)` trả `ExportResult(topic, batchSize, csv)`. Dùng DTO bài 4, topic hằng `orders.audit` được macro kiểm tra, batch `100` qua `bounded` và writer CSV. Không kết nối Kafka; topic là metadata để shell ở chương sau dùng.

Tiêu chí nghiệm thu: tập rỗng vẫn có header; đúng thứ tự cột; quote đúng `note` chứa dấu phẩy; topic/batch được kiểm tra lúc compile; không đưa token/email ngoài DTO vào output; test kiểm tra toàn bộ CSV và metadata. [EventExport và SolutionsDemo](../solutions/ch08c-code/src/main/scala/vn/scalaeng/ch08c/solutions/Exercises.scala) là bản tham chiếu có test, chạy bằng lệnh mục 7.

Để mở rộng sau khi xong: cho phép cấu hình topic/batch runtime bằng smart constructor `Either`, giữ core export không I/O; viết encoder thủ công khi downstream yêu cầu rename header. Đánh giá thời gian compile/code size trước khi định thêm macro mới. Phần mở rộng này là bài tự làm, chưa có benchmark hay tích hợp Kafka trong lab.

## 16. Nguồn tài liệu chính thức

Đã truy cập/kiểm tra ngày **17/10/2026**. Scala Reference trực tuyến hiện hiển thị 3.10.0; ví dụ và diagnostics trong chương đã chạy lại bằng compiler **3.9.0**, không lấy output của phiên bản trang docs làm bằng chứng chạy.

- [Scala Reference — Inline](https://docs.scala-lang.org/scala3/reference/metaprogramming/inline.html): ngữ nghĩa inline và tham số.
- [Compile-time operations](https://docs.scala-lang.org/scala3/reference/metaprogramming/compiletime-ops.html): `error`, `constValue`, `erasedValue`, `summonInline`.
- [Match Types](https://docs.scala-lang.org/scala3/reference/new-types/match-types.html): reduction và disjointness.
- [Type Class Derivation](https://docs.scala-lang.org/scala3/reference/contextual/derivation.html): `Mirror`, product/sum và `derived`.
- [Macros](https://docs.scala-lang.org/scala3/reference/metaprogramming/macros.html) và [Macros Spec](https://docs.scala-lang.org/scala3/reference/metaprogramming/macros-spec.html): quotes, splices và staging.
- [Scala compiler 3.9.0 — Maven Central POM](https://repo.maven.apache.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom): version compiler được pin.
- [munit_3 1.2.4 — Maven Central POM](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom): artifact test Scala 3 đã tải và chạy.
- [circe-generic_3 0.14.17 — Maven Central POM](https://repo.maven.apache.org/maven2/io/circe/circe-generic_3/0.14.17/circe-generic_3-0.14.17.pom) và [source semiauto cùng tag](https://github.com/circe/circe/blob/v0.14.17/modules/generic/shared/src/main/scala-3/io/circe/generic/semiauto.scala): liên hệ thực tế, không phải dependency của lab.

## 17. Checklist tự đánh giá

- [ ] Tôi biết khi nào nên dừng ở hàm thường, given viết tay và `Either`.
- [ ] Tôi phân biệt đối số thường với inline parameter; biết code size và tác dụng phụ có thể tăng.
- [ ] Tôi dùng được inline val/def/if/match và giải thích vai trò bốn compiletime helpers.
- [ ] Tôi giải thích được lỗi match type không reduce mà không ép cast.
- [ ] Tôi đọc được `head *: tail`, `EmptyTuple`, `Tuple.Map` và kiểu phụ thuộc của helper tuple.
- [ ] Tôi derive được một type class product; hiểu giới hạn nested/sum và vị trí cast nội bộ.
- [ ] Tôi giữ schema CSV được review thay vì để refactor entity tự đổi hợp đồng.
- [ ] Tôi phân biệt quote/splice, compile/runtime và đặt nơi gọi macro khác source implementation.
- [ ] Tôi chạy đủ 25 test cùng 7 ca compilation thất bại, và đọc được diagnostic thật.
- [ ] Tôi không khẳng định tăng tốc khi chưa có benchmark, cũng không bỏ validation runtime cho dữ liệu ngoài.
