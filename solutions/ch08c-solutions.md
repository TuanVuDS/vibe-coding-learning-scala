> **Phiên bản:** Scala 3.9.0 · JDK 21 (đã chạy 21.0.12.1) · Dependency runtime: thư viện chuẩn và source main của lab Ch8C · Test: munit 1.2.4 · **Kiểm chứng:** đã biên dịch & chạy, 13/13 test pass, 0 cảnh báo compiler với `-Werror`, ngày 17/10/2026; chờ review chéo.

# Lời giải — Chương 8C: Lập trình kiểu nâng cao

Đề bài ở [Ch8C, mục 12](../03-type-system/ch08c-advanced-type-programming.md#12-bài-tập-cơ-bản--nâng-cao). Lời giải dùng package `vn.scalaeng.ch08c.solutions`. Đây là project một module; [project.scala](ch08c-code/project.scala) tham chiếu source của lab bằng `using files`, không sao chép code lab và không lấy test lab vào bộ 13 test này.

- [Exercises.scala](ch08c-code/src/main/scala/vn/scalaeng/ch08c/solutions/Exercises.scala): bài 1–5, mini project và demo.
- [PercentLiteral.scala](ch08c-code/src/main/scala/vn/scalaeng/ch08c/solutions/PercentLiteral.scala): macro bài 6, tách file khỏi nơi gọi.
- [ExercisesSuite.scala](ch08c-code/src/test/scala/vn/scalaeng/ch08c/solutions/ExercisesSuite.scala): hai test cho mỗi bài và một test mini project.

## Bài 1 — Batch có giới hạn

`BatchPolicy.bounded` dùng một `inline if` kiểm tra `size >= 1 && size <= 10000`. Nếu đúng trả lại số gốc; nếu sai, `compiletime.error` báo `batch size must be in [1, 10000]`. Khoảng đóng nên `1` và `10000` đều hợp lệ. Không dùng điều kiện `< 10000` vì sẽ loại nhầm biên trên.

Hai test `BT1` kiểm tra hai biên, literal `0`/`10001` và một `def f(n: Int)` truyền dữ liệu runtime. Các trường hợp sai nằm trong `typeCheckErrors`, vì đưa trực tiếp vào source sẽ khiến cả project không compile được. API này chỉ bảo vệ cấu hình biết trước trong mã nguồn; batch từ HTTP/config cần smart constructor runtime trả `Either` và được xử lý ở biên ứng dụng.

## Bài 2 — Bóc kiểu nhiều lớp

```scala
type Leaf[X] = X match
  case Option[t] => Leaf[t]
  case List[t]   => Leaf[t]
  case _         => X
```

Khác `Payload` trong lab, bên phải hai case đầu gọi lại `Leaf`, do đó bóc hết `List[Option[List[String]]]` tới `String`. Case cuối giữ kiểu đã không thuộc hai container. `Leaf` chỉ là phép tính kiểu; không thể dùng nó để lấy một `String` từ list rỗng hay `None`.

Test `BT2` dùng `summon[Leaf[...] =:= ...]` để compiler chứng minh kết quả, rồi kiểm tra ca generic không hợp lệ. `def f[A](a: A): Leaf[A] = a` không đúng cho mọi A: nếu A là `Option[Int]`, đầu vào là một Option còn kết quả yêu cầu Int. Khi A abstract, compiler cũng không đủ chứng cứ đi qua pattern đầu. Không giải quyết bằng cast; đổi giao ước hoặc dùng type class có thuật toán rõ cho dữ liệu.

Đệ quy kiểu quá sâu làm compiler tốn công; không đưa mọi cấu trúc nghiệp vụ vào Leaf chỉ để tránh viết tên DTO.

## Bài 3 — Arity và Tuple.Map

`arity` dùng `inline erasedValue[Ts] match`: tuple rỗng trả 0, còn `head *: tail` trả `1 + arity[tail]`. Đệ quy diễn ra theo hình dạng tuple type; không cần tạo một tuple giá trị để đếm. Test bao gồm `EmptyTuple`, `Tuple1[String]` và tuple ba trường.

`acceptedRow` dùng type lambda `[A] =>> Either[String, A]` làm tham số biến đổi của `Tuple.Map`, và polymorphic function `[A] => (value: A) => ...` để biến đổi giá trị. Hai dấu `=>>` và `=>` giữ hai vai trò khác nhau: một bên dựng kiểu, bên kia dựng hàm.

Chữ ký trả `Tuple.Map[row.type, ...]` khớp chữ ký `Tuple.map` của compiler 3.9.0. Khi gọi bằng `("ORD-1", 2)`, test gán được vào `(Either[String, String], Either[String, Int])`, còn kiểu đảo hai vị trí bị từ chối. `Right(value)` được ascribe thành `Either[String, A]` để không vô tình hứa kết quả chỉ có subtype `Right`.

Tất cả cột của `acceptedRow` đều là Right; bài này không triển khai nghiệp vụ validation. Muốn có Left thật, cần thêm rule theo từng trường/kiểu và policy gom lỗi, không chỉ đổi tên helper.

## Bài 4 — Derive DTO sự kiện

`EventReport` dùng các kiểu đã có `CellEncoder`: String, Int, Boolean và Option[String]. Chỉ thêm `derives CsvEncoder`, không viết instance lặp. Test `BT4` xác nhận header `eventId,attempts,accepted,note`, giá trị None thành một ô rỗng, dấu phẩy được quote và product không trường trả header/ô rỗng.

Test với một trường `Map[String, Int]` xác nhận compilation thiếu `CellEncoder`, thay vì gọi `toString` âm thầm. Nếu thật sự muốn nhét map vào một ô, phải chọn serialization rõ ràng và có test escape; nếu muốn nhiều cột, cần thiết kế flatten/schema khác với `CellEncoder` một ô.

Rename `eventId` thành `id` sẽ đổi header tự động; compiler không thể biết downstream đã phụ thuộc tên cũ. Hãy derive trên DTO xuất dữ liệu đã được review. Khi tên cột ngoài hệ thống cần cố định, instance viết tay hoặc lớp mapping tường minh là lựa chọn đơn giản.

## Bài 5 — Tự viết Show.derived

`Show.derived[A <: Product]` lấy tên kiểu bằng `constValue[mirror.MirroredLabel]`, tên trường qua helper `labels` và renderer của từng kiểu bằng `summonInline[Show[head]]`. `Show[String]` và `Show[Int]` đặt trong companion để compiler tự tìm.

Helper `productShow` là một `def` thường tạo instance. Lúc `show`, nó ghép ba iterator theo thứ tự: label, giá trị trường, renderer; kết quả được nối thành `Type(field=value,...)`. Kiểu rỗng tạo `EmptyReport()`. `writer` chứa cast tại một chỗ private; tính đúng dựa vào Mirror/product có cùng thứ tự, không dựa vào kiểm tra generic runtime.

Test `BT5` kiểm tra chuỗi hoàn chỉnh cho OrderSummary, product rỗng và lỗi compile khi trường BigDecimal chưa có `Show`. Cần giữ hai danh sách metadata đồng bộ; không gọi factory private với dữ liệu từ một schema khác. Đây là bản product tối thiểu, không hỗ trợ sum types hay recursive derivation tự động.

Không dùng format Show này làm JSON, schema CSV hoặc giao thức lưu trữ: nó không escape delimiter và không cam kết tương thích khi refactor. Cũng không tự đưa dữ liệu nhạy cảm vào log qua derivation.

## Bài 6 — Macro phần trăm

`PercentLiteral.scala` chứa public `checked` và implementation riêng. Implementation lấy `Expr[Int].value`; nếu constant trong khoảng đóng `0..100`, nó trả `Expr(number)`. Với literal ngoài khoảng hoặc biểu thức không biết trước, `report.errorAndAbort` đưa ra diagnostic riêng. Không cần quote rỗng chỉ để bọc `Expr` rồi splice ngay trở lại; một `Expr(number)` đã là code có kiểu Int.

`parse` dùng cùng predicate `valid` để kiểm tra input runtime, trả `Either`. Hai test `BT6` bao gồm cả hai biên, giá trị 25, ngoài biên ở cả hai phía và tham số runtime. `checked` vẫn trả Int bình thường, không tạo một kiểu Percent ngăn mọi phép gán số khác. Nếu domain cần invariant bền vững, kết hợp opaque type và smart constructor như Ch8A.

Rule này có thể viết ngắn hơn bằng inline if như bài 1. Macro là bài luyện staging/diagnostics; không có benchmark hay nhu cầu AST ở đây để biện minh rằng macro tốt hơn trong production. Chỉ thêm độ phức tạp khi có yêu cầu vượt khả năng helper inline nhỏ.

## Mini project — EventExport

`EventExport.render` ghép ba quyết định: topic hằng `orders.audit` được macro lab kiểm tra, batch 100 qua bài 1 và CSV từ DTO bài 4. `ExportResult` trả metadata và nội dung thuần; không kết nối broker hay ghi file. Test cuối xác nhận cả metadata lẫn nguyên chuỗi CSV, gồm CRLF và note chứa dấu phẩy; input rỗng giữ header.

Nếu triển khai shell Kafka hoặc file sink sau này, truyền result này sang tầng I/O. Với topic/batch từ config runtime, thêm một bước parse trả `Either` trước khi gọi core; không truyền chúng vào macro rồi cố làm compiler đoán dữ liệu vận hành.

## Chạy và output thật

Từ gốc repo:

```powershell
$env:COURSIER_CACHE = Join-Path (Get-Location) '.cache/coursier'
scala-cli.bat test solutions/ch08c-code --server=false
scala-cli.bat run solutions/ch08c-code --server=false --main-class vn.scalaeng.ch08c.solutions.SolutionsDemo
```

Scala CLI có thể gợi ý nâng munit; repo cố định munit 1.2.4. Test không cần Docker hoặc dịch vụ ngoài. Cấu hình bật `-Werror` và encoding UTF-8, giống lab.

Output demo thật ngày 17/10/2026:

```text
batch=10000
arity=3
accepted=(Right(ORD-1),Right(2))
OrderSummary(orderId=ORD-1,quantity=2)
percent=100
topic=orders.audit; batch=100
eventId,attempts,accepted,note
EV-1,1,true,"ok, retry"
```

Tóm tắt munit thật:

```text
Test run vn.scalaeng.ch08c.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 13 total 0.116s
```

Log được giữ trong `.cache/t22/solutions-demo.log`, `.cache/t22/solutions-test.log` khi biên soạn; không cần các file cache để chạy lại project. Toàn bộ 13 test đã chạy, không có ca ignored.

## Gợi ý câu hỏi kiểm tra

1. Tham số thường được đánh giá một lần; inline parameter thay bằng biểu thức ở nơi sử dụng, có thể lặp side effect.
2. Compiler không biết giá trị môi trường tương lai. Validator runtime thường trả `Either[Error, ValidatedValue]` để caller xử lý lỗi.
3. `4` là singleton type có một giá trị; `Int` chứa nhiều giá trị, không chọn được một hằng đại diện.
4. `erasedValue` là công cụ compile để xét kiểu; không tạo T thật. Nó phải được loại trong quá trình inline, không được dùng làm dữ liệu runtime.
5. Compiler cần chứng minh đã qua được case trước. A abstract chưa chắc khớp hay disjoint với Option, nên chưa đi tới default.
6. Match type tính kiểu kết quả; map ở mức giá trị thực hiện phép bọc cột. `row.type` giữ receiver type mà `Tuple.map` dùng trên 3.9.0.
7. Product Mirror cung cấp tên/kiểu trường; helper dùng `summonInline` tìm cell instance trong implicit scope khi mở rộng.
8. Schema và quyền xuất trường là giao ước nghiệp vụ; Mirror chỉ biết cấu trúc hiện tại.
9. Implementation macro và validation chạy trong compiler; quote tạo code cho chương trình đích. Tách nơi gọi giúp compiler có implementation để thực thi.
10. Chọn cách đơn giản nhất diễn đạt đủ yêu cầu; xem bảng mục 3.7 của bài học. Đồng đội cần biết nơi instance được tìm, chỗ code được sinh và cách chạy test chẩn đoán.

## Phiên bản và nguồn

Ngày kiểm tra **17/10/2026**: [scala3-compiler_3 3.9.0](https://repo.maven.apache.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom), [munit_3 1.2.4](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom). Scala Reference hiện theo bản mới hơn; implementation lời giải được test riêng bằng 3.9.0. Tham khảo chính thức cho [derivation](https://docs.scala-lang.org/scala3/reference/contextual/derivation.html) và [macros](https://docs.scala-lang.org/scala3/reference/metaprogramming/macros.html); danh sách nguồn đầy đủ trong chương mục 16.
