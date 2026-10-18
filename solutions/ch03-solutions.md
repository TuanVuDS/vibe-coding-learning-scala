# Lời giải — Chương 3: Variables, Types và Expressions

> **Code:** [`code/src/main/scala/scalaeng/solutions/Ch03Solutions.scala`](code/src/main/scala/scalaeng/solutions/Ch03Solutions.scala)
> **Test:** [`code/src/test/scala/scalaeng/solutions/Ch03SolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch03SolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 6/6 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Chạy: `cd solutions/code && scala-cli test .` hoặc `sbt test`.

## Bài 1 — `describeProduct`

```scala
def describeProduct(name: String, price: BigDecimal, quantity: Int): String =
  s"$name x$quantity = ${price * quantity}"
```

Biểu thức `price * quantity` phải nằm trong `${...}` vì không phải một tên đơn. Kết quả: `"Bút bi x3 = 15000"`.

## Bài 2 — `isValidPhone`

```scala
def isValidPhone(raw: String): Boolean =
  val digits = raw.trim
  digits.length == 10 && digits.startsWith("0") && digits.forall(_.isDigit)
```

- Thứ tự điều kiện: kiểm tra rẻ (`length`) trước, đắt (`forall` duyệt cả chuỗi) sau; `&&` dừng sớm khi gặp `false`.
- `forall(_.isDigit)`: `String` dùng được các HOF như collection của `Char`.
- Đây là quy tắc đơn giản hóa cho bài tập. Validate số điện thoại thật (đầu số nhà mạng, định dạng `+84`) nên dùng thư viện chuyên dụng như libphonenumber.

## Bài 3 — `vatAmount`

```scala
import scala.math.BigDecimal.RoundingMode

def vatAmount(subtotal: BigDecimal, rate: BigDecimal): BigDecimal =
  (subtotal * rate).setScale(0, RoundingMode.HALF_UP)
```

- `12345 * 0.1 = 1234.5` → HALF_UP → `1235`; `12344 * 0.1 = 1234.4` → `1234`.
- **Làm tròn ở đâu là quyết định nghiệp vụ.** Làm tròn từng dòng hàng rồi cộng có thể khác cộng rồi làm tròn. Phải thống nhất với bộ phận kế toán và ghi lại trong code/test.
- Chọn `RoundingMode` rõ ràng; `HALF_EVEN` (làm tròn kiểu ngân hàng) cho kết quả khác `HALF_UP` ở các giá trị `.5`.

## Bài 4 — `classifyStatus`

```scala
def classifyStatus(status: Int): String = status match
  case s if s >= 100 && s < 200 => "informational"
  case s if s >= 200 && s < 300 => "success"
  case s if s >= 300 && s < 400 => "redirection"
  case s if s >= 400 && s < 500 => "client-error"
  case s if s >= 500 && s < 600 => "server-error"
  case _                        => "unknown"
```

Cách viết khác gọn hơn: `status / 100 match { case 1 => ...; case 2 => ... }`. Cả hai đều đúng; bản dùng khoảng
rõ ràng hơn khi đọc, bản chia 100 ngắn hơn nhưng phải cẩn thận với số âm (`-150 / 100 == -1`).

Test chú ý giá trị biên: 199, 200, 599, 600. Lỗi "lệch một" (off-by-one, `<=` thay vì `<`) là lỗi phổ biến nhất của hàm loại này.

## Bài 5 — `parseOrderLine`

```scala
def parseOrderLine(line: String): (String, BigDecimal, Int) =
  val parts = line.split(",").map(_.trim)
  if parts.length != 3 then
    throw IllegalArgumentException(s"Cần 3 cột, nhận ${parts.length}: '$line'")
  val sku   = parts(0)
  val price = parts(1).toDoubleOption match
    case Some(_) => BigDecimal(parts(1))
    case None    => throw IllegalArgumentException(s"Đơn giá không hợp lệ: '${parts(1)}'")
  val qty   = parts(2).toIntOption.getOrElse(
    throw IllegalArgumentException(s"Số lượng không hợp lệ: '${parts(2)}'")
  )
  if sku.isEmpty then throw IllegalArgumentException("SKU rỗng")
  if price < 0 || qty <= 0 then throw IllegalArgumentException(s"Giá trị âm/0: '$line'")
  (sku, price, qty)
```

Giải thích:

- `toDoubleOption` chỉ dùng để **kiểm tra** chuỗi là số; giá trị thật được tạo bằng `BigDecimal(parts(1))` **từ chuỗi** để không qua `Double`.
- `getOrElse(throw ...)` hợp lệ vì `throw` có kiểu `Nothing` — con của `Int`.
- Thông điệp lỗi chứa dữ liệu gốc: khi xử lý file 1 triệu dòng, thông điệp "invalid input" không có ngữ cảnh gần như vô dụng.
- Giới hạn: `split(",")` không xử lý CSV có dấu phẩy trong dấu ngoặc kép (`"Áo, size L"`). CSV thật cần thư viện parser (Chương 19).

**Trả lời câu hỏi suy nghĩ:** ném exception **không** phù hợp cho pipeline 1 triệu dòng:
(1) một dòng lỗi làm dừng cả pipeline nếu không `try/catch` từng dòng;
(2) exception tốn chi phí tạo stack trace;
(3) kiểu trả về `(String, BigDecimal, Int)` "nói dối" — nó không cho biết hàm có thể thất bại.
Chương 7 sẽ đổi kiểu trả về thành `Either[ParseError, OrderLine]` để lỗi trở thành **dữ liệu** có thể đếm, ghi ra file `rejected.csv`.

## Câu hỏi kiểm tra — gợi ý trả lời

1. Không lỗi. `val` chỉ cố định tham chiếu; `ArrayBuffer` là đối tượng mutable nên sửa nội dung được.
2. Giá trị `()`, kiểu `Unit` (compiler còn cảnh báo "Discarded non-Unit value of type Int").
3. `throw` có kiểu `Nothing`, là kiểu con của mọi kiểu, nên phù hợp với bất kỳ kiểu mong đợi nào.
4. Nên ghi cho method công khai, giá trị là hợp đồng giữa các module, khi kiểu suy luận không như ý. Bắt buộc cho method đệ quy.
5. `BigDecimal(0.1 + 0.2)` → `0.30000000000000004` (sai số đã có từ phép cộng `Double`); `BigDecimal("0.1") + BigDecimal("0.2")` → `0.3`;
   `new java.math.BigDecimal(0.1)` → `0.1000000000000000055511151231257827021181583404541015625`.
6. Lỗi **lúc biên dịch**: `Found: String, Required: Double, Float, BigDecimal` — `f` interpolator được compiler kiểm tra kiểu.
7. Scala 3.9 suy ra `List[Int | String | Boolean]`. Một collection chứa các kiểu không liên quan thường là thiết kế sai:
   nên dùng case class hoặc ADT (Chương 5, 7) để mỗi phần tử có cấu trúc rõ ràng.
