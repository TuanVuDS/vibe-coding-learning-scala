# Chương 3 — Variables, Types và Expressions

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4
> **Code:** [`code/src/main/scala/scalaeng/ch03/`](code/src/main/scala/scalaeng/ch03/) · Test: [`code/src/test/scala/scalaeng/ch03/`](code/src/test/scala/scalaeng/ch03/)
> **Trạng thái kiểm chứng:** Code chính **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không có cảnh báo với `-Wunused:all`).
> Test: **đã biên dịch & chạy** — 8/8 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Thông báo lỗi compiler trong mục 10 là output thật.

---

## 1. Mục tiêu học tập

- Dùng `val` mặc định, biết khi nào `var` là chấp nhận được.
- Phân biệt **biến bất biến** (immutable reference) với **đối tượng bất biến** (immutable object).
- Nắm các kiểu cơ bản và cạm bẫy: tràn số `Int`, sai số `Double`, chọn `BigDecimal` cho tiền.
- Hiểu `Any`, `Unit`, `Nothing`, `Null` nằm ở đâu trong hệ thống kiểu.
- Viết code theo tư duy **expression** (mọi thứ trả về giá trị), dùng `if`/`match` như biểu thức.
- Dùng string interpolation `s`, `f`, `raw` đúng cách.
- Làm quen `List`, `Map`, `Set` ở mức cơ bản.

## 2. Kiến thức tiên quyết

- Chương 2: chạy được code bằng Scala CLI hoặc sbt.

## 3. Lý thuyết

### 3.1. `val` và `var`

```scala
val price = 120_000     // không gán lại được
var counter = 0         // gán lại được
counter = counter + 1
```

- **Vấn đề:** trong chương trình lớn, biến bị gán lại ở nhiều nơi khiến khó biết giá trị tại một thời điểm,
  và trở thành nguồn lỗi khi nhiều thread cùng truy cập (Chương 16).
- **Giải pháp của Scala:** khuyến khích `val`. Một `val` luôn có đúng một giá trị suốt vòng đời — đọc code dễ,
  test dễ, an toàn đa luồng.
- **Khi nào `var` chấp nhận được:** biến cục bộ trong một hàm nhỏ, tối ưu hiệu năng đã đo đạc, hoặc
  khi viết lại thuật toán imperative quen thuộc. **Không** dùng `var` cho trạng thái chia sẻ giữa các phần của chương trình.
- **Lỗi thiết kế thường gặp:** dùng `var` vì quen tay từ Java/Python, sau đó phải thêm `synchronized` khắp nơi.

### 3.2. Immutable reference ≠ immutable object

```scala
import scala.collection.mutable.ArrayBuffer

val buffer = ArrayBuffer(1, 2)
buffer += 3          // HỢP LỆ: không gán lại `buffer`, mà sửa nội dung đối tượng
// buffer = ArrayBuffer(9)   // LỖI: Reassignment to val buffer

var list = List(1, 2)
list = list :+ 3     // HỢP LỆ: `list` trỏ sang một List MỚI; List cũ không đổi
```

| | Đối tượng immutable (`List`) | Đối tượng mutable (`ArrayBuffer`) |
|---|---|---|
| `val` | An toàn nhất — **mặc định nên dùng** | Tham chiếu cố định nhưng nội dung đổi được |
| `var` | Đổi tham chiếu sang giá trị mới | Kém an toàn nhất — tránh |

### 3.3. Các kiểu cơ bản

| Kiểu | Kích thước | Ví dụ | Ghi chú |
|---|---|---|---|
| `Byte` | 8 bit | `val b: Byte = 1` | Hiếm dùng trực tiếp |
| `Short` | 16 bit | | Hiếm dùng |
| `Int` | 32 bit | `42`, `1_000_000` | Mặc định cho số nguyên; tối đa ~2,1 tỷ |
| `Long` | 64 bit | `3_000_000_000L` | Timestamp, ID, đếm lớn |
| `Float` | 32 bit | `1.5f` | Tránh dùng |
| `Double` | 64 bit | `0.1` | Đo lường, thống kê — **không dùng cho tiền** |
| `BigDecimal` | Tùy ý | `BigDecimal("0.1")` | Tiền tệ, kế toán |
| `BigInt` | Tùy ý | `BigInt(2).pow(100)` | Số nguyên rất lớn |
| `Boolean` | | `true` | |
| `Char` | 16 bit | `'A'` | Nháy đơn |
| `String` | | `"Scala"` | Chính là `java.lang.String` |

Ba cạm bẫy bạn **phải nhớ** (output thật từ `TypesTour.scala`):

```
0.1 + 0.2 (Double)     = 0.30000000000000004
0.1 + 0.2 (BigDecimal) = 0.3
Int.MaxValue + 1       = -2147483648
```

- `Double` là số dấu phẩy động nhị phân: không biểu diễn chính xác `0.1`. Cộng tiền bằng `Double` sẽ lệch vài đồng sau hàng triệu giao dịch.
- `Int` **tràn số im lặng**: không có exception. Đếm số bản ghi của một bảng lớn bằng `Int` có thể ra số âm.
- Tạo `BigDecimal` từ **chuỗi** hoặc số nguyên: `BigDecimal("0.1")`, `BigDecimal(120_000)`.
  Không tạo từ kết quả phép tính `Double` — sai số đã có sẵn trước khi vào `BigDecimal`:
  `BigDecimal(0.1 + 0.2)` cho `0.30000000000000004` (output thật).
- Lưu ý khác biệt Scala/Java: `scala.math.BigDecimal(0.1)` dùng biểu diễn thập phân của `Double` nên cho `0.1`,
  còn `new java.math.BigDecimal(0.1)` của Java cho `0.1000000000000000055511151231257827021181583404541015625`
  (giá trị nhị phân chính xác). Khi nhận `java.math.BigDecimal` từ thư viện Java (JDBC, Kafka...), hãy nhớ điều này.

Dấu `_` trong số (`1_000_000`) chỉ để dễ đọc, compiler bỏ qua.

### 3.4. Hệ thống kiểu: `Any`, `AnyVal`, `AnyRef`, `Unit`, `Nothing`, `Null`

```
                    Any
                 /       \
            AnyVal        AnyRef (= java.lang.Object)
   Int Long Double         String  List  ... mọi class
   Boolean Char Unit       |
            \              Null   (con của mọi kiểu tham chiếu)
             \            /
               Nothing         (con của MỌI kiểu)
```

- **`Any`**: kiểu cha của mọi kiểu. Nếu một collection có kiểu `List[Any]` (hoặc compiler suy ra union như
  `List[Int | String | Boolean]`), thường là dấu hiệu bạn trộn kiểu nhầm.
- **`Unit`**: kiểu của biểu thức "không có giá trị có ý nghĩa", có đúng một giá trị là `()`. Hàm trả `Unit` là hàm chỉ có **side effect** (in, ghi file, gửi request).
- **`Nothing`**: không có giá trị nào. Kiểu của biểu thức **không bao giờ kết thúc bình thường**: `throw`, vòng lặp vô hạn. Vì `Nothing` là con của mọi kiểu nên `throw` dùng được ở bất cứ đâu:
  ```scala
  val qty: Int = if raw.nonEmpty then raw.toInt else throw IllegalArgumentException("rỗng")
  ```
- **`Null`**: kiểu của `null`. Chỉ xuất hiện khi làm việc với thư viện Java. Trong code Scala, **dùng `Option` thay `null`** (Chương 7).

### 3.5. Type inference và type annotation

```scala
val name = "An"                    // suy ra String
val total = 120_000 * 2            // suy ra Int
val amount: BigDecimal = 240_000   // annotation: ép kiểu mong muốn
```

**Quy tắc thực tế:**

| Nên ghi kiểu tường minh | Có thể để compiler suy luận |
|---|---|
| Kiểu trả về của method `public` | Biến cục bộ hiển nhiên (`val n = 10`) |
| Method đệ quy (**bắt buộc**) | Lambda ngắn |
| Khi kiểu suy ra không như mong muốn (ví dụ muốn `BigDecimal` thay vì `Int`) | |
| Giá trị là "hợp đồng" giữa các module | |

### 3.6. Expressions — tư duy "mọi thứ trả về giá trị"

Trong Java, `if` là **câu lệnh** (statement). Trong Scala, `if`, `match`, khối `{ }` đều là **biểu thức** (expression):

```scala
// Java style (vẫn chạy nhưng không phải idiomatic)
var fee = BigDecimal(0)
if subtotal < 500_000 then fee = 30_000

// Scala style: if là expression
val fee = if subtotal >= 500_000 then BigDecimal(0) else BigDecimal(30_000)
```

**Khối lệnh** (block) có giá trị là **biểu thức cuối cùng**:

```scala
val total =
  val sub = subtotal(items)
  sub + shippingFee(sub)     // giá trị của cả khối
```

**Lợi ích:** không cần biến tạm `var`, không thể quên nhánh `else` (compiler sẽ suy ra kiểu `Unit` hoặc `Any` và bạn bị lỗi kiểu — xem mục 10).

### 3.7. Control flow

```scala
// if / else if / else
val level = if score >= 90 then "A" else if score >= 70 then "B" else "C"

// match với guard
def quantityCategory(quantity: Int): String = quantity match
  case q if q <= 0 => "invalid"
  case 1           => "single"
  case q if q < 10 => "small"
  case _           => "bulk"

// for ... do : vòng lặp có side effect, trả về Unit
for i <- 1 to 3 do println(i)

// for ... yield : tạo collection mới (là expression)
val squares = for i <- 1 to 5 yield i * i     // Vector(1, 4, 9, 16, 25)

// while : hiếm dùng trong code Scala thông thường
var i = 0
while i < 3 do i += 1
```

`match` sẽ được học sâu ở Chương 7 (pattern matching, ADT). Ở đây chỉ cần dùng như `switch` mạnh hơn.

### 3.8. String interpolation

| Interpolator | Ví dụ | Kết quả |
|---|---|---|
| `s` | `s"Xin chào $name, tổng ${a + b}"` | Chèn giá trị; biểu thức phức tạp đặt trong `${}` |
| `f` | `f"$ratio%.2f"` | Định dạng kiểu `printf`, **có kiểm tra kiểu lúc biên dịch** |
| `raw` | `raw"C:\new\table"` | Không xử lý escape `\n`, `\t` — hữu ích cho regex, đường dẫn Windows |

Chuỗi nhiều dòng: dùng `"""..."""` kèm `.stripMargin`:

```scala
val sql =
  """SELECT id, amount
    |FROM orders
    |WHERE status = 'PAID'""".stripMargin
```

> ⚠️ Không bao giờ dùng `s"..."` để ghép **giá trị người dùng nhập** vào câu SQL — đó là lỗ hổng SQL injection (Chương 15).

**Cạm bẫy Locale:** định dạng số phụ thuộc Locale của máy. Trên máy đặt `vi_VN`, `"%,d"` in `1.234.567`;
trên máy `en_US` in `1,234,567`. Code cho production (log, file xuất, test) phải **chỉ định Locale rõ ràng**
— xem hàm `formatVnd` trong code.

### 3.9. Collections cơ bản (giới thiệu — học sâu ở Chương 6)

```scala
val skus  = List("SKU-1", "SKU-2", "SKU-3")              // danh sách có thứ tự, bất biến
val stock = Map("SKU-1" -> 10, "SKU-2" -> 0)             // key → value
val tags  = Set("sale", "new", "sale")                   // Set("sale", "new") — không trùng

skus.size                 // 3
skus.head                 // "SKU-1"
stock("SKU-1")            // 10  (key không tồn tại → exception!)
stock.getOrElse("X", 0)   // 0   (an toàn)
tags.contains("new")      // true
skus.map(_.toLowerCase)   // List("sku-1", "sku-2", "sku-3")
```

Tuple — nhóm vài giá trị khác kiểu mà không cần định nghĩa class:

```scala
val item: (BigDecimal, Int) = (BigDecimal(120_000), 2)
val (price, qty) = item     // tách tuple
```

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Immutable / Mutable | Không đổi được / đổi được |
| Reassignment | Gán lại giá trị cho biến |
| Type inference / annotation | Suy luận kiểu / ghi kiểu tường minh |
| Expression / Statement | Biểu thức (có giá trị) / câu lệnh (chỉ thực hiện hành động) |
| Side effect | Tác dụng ngoài việc trả về giá trị (in, ghi, đổi biến bên ngoài) |
| Overflow | Tràn số |
| Floating point | Dấu phẩy động |
| Guard | Điều kiện `if` trong một `case` |
| String interpolation | Chèn giá trị vào chuỗi |
| Bottom type | Kiểu con của mọi kiểu (`Nothing`) |

## 5. Ví dụ đơn giản

```scala
@main def simple(): Unit =
  val product  = "Áo thun"
  val price    = BigDecimal(120_000)
  val quantity = 2
  val total    = price * quantity
  val label    = if total > 200_000 then "đơn lớn" else "đơn nhỏ"
  println(s"$product x$quantity = $total ($label)")
// Áo thun x2 = 240000 (đơn lớn)
```

## 6. Ví dụ thực tế — tính tiền một đơn hàng

Bài toán (backend thương mại điện tử): nhận giỏ hàng gồm các dòng (đơn giá, số lượng), số lượng có thể đến từ
input người dùng (có khoảng trắng, có thể sai định dạng). Tính tạm tính, phí ship (miễn phí từ 500.000đ), tổng tiền,
và in ra theo định dạng tiền Việt Nam.

Các quyết định thiết kế:

- Tiền dùng `BigDecimal` → không sai số.
- Hàm tính toán **không in gì** (không có side effect), chỉ `@main` mới in → các hàm test được dễ dàng.
- Input sai định dạng được xử lý tại **biên** (hàm `parseQuantity`), phần lõi chỉ nhận dữ liệu đúng kiểu.
- Viết hai phiên bản `subtotal` (functional và imperative) để so sánh — test đảm bảo chúng cho cùng kết quả.

## 7. Code đầy đủ

[`code/src/main/scala/scalaeng/ch03/OrderBasics.scala`](code/src/main/scala/scalaeng/ch03/OrderBasics.scala):

```scala
package scalaeng.ch03

object OrderBasics:

  /** Ngưỡng miễn phí vận chuyển (giá trị minh họa, không phải chính sách thật). */
  val FreeShippingThreshold: BigDecimal = BigDecimal(500_000)
  val StandardShippingFee: BigDecimal   = BigDecimal(30_000)

  /** `if` trong Scala là một expression: nó trả về giá trị. */
  def shippingFee(subtotal: BigDecimal): BigDecimal =
    if subtotal >= FreeShippingThreshold then BigDecimal(0)
    else StandardShippingFee

  def lineTotal(unitPrice: BigDecimal, quantity: Int): BigDecimal =
    unitPrice * quantity

  /** `match` cũng là expression. Guard (`if ...`) giúp phân loại theo khoảng. */
  def quantityCategory(quantity: Int): String = quantity match
    case q if q <= 0  => "invalid"
    case 1            => "single"
    case q if q < 10  => "small"
    case _            => "bulk"

  /** Đọc số lượng từ chuỗi do người dùng nhập.
    * `toIntOption` trả về `Option[Int]`; chuỗi lỗi sẽ thành 0.
    */
  def parseQuantity(raw: String): Int =
    raw.trim.toIntOption.getOrElse(0)

  /** Định dạng số có dấu phân cách hàng nghìn, chỉ định Locale rõ ràng. */
  def formatVnd(amount: BigDecimal): String =
    val rounded = amount.setScale(0, BigDecimal.RoundingMode.HALF_UP).toLong
    "%,d VND".formatLocal(java.util.Locale.US, rounded)

  /** Kiểu `Nothing`: method này không bao giờ trả về giá trị bình thường. */
  def invalidInput(message: String): Nothing =
    throw IllegalArgumentException(message)

  /** Tổng tiền giỏ hàng: List các cặp (đơn giá, số lượng). */
  def subtotal(items: List[(BigDecimal, Int)]): BigDecimal =
    items.map((price, qty) => lineTotal(price, qty)).sum

  /** Cùng phép tính nhưng dùng `var` và vòng lặp — để so sánh phong cách. */
  def subtotalImperative(items: List[(BigDecimal, Int)]): BigDecimal =
    var total = BigDecimal(0)
    for (price, qty) <- items do
      total = total + lineTotal(price, qty)
    total

  def orderTotal(items: List[(BigDecimal, Int)]): BigDecimal =
    val sub = subtotal(items)
    sub + shippingFee(sub)

@main def ch03Demo(): Unit =
  import OrderBasics.*

  val customerName = "Nguyễn Văn An"
  val items: List[(BigDecimal, Int)] = List(
    (BigDecimal(120_000), 2),
    (BigDecimal(85_000), parseQuantity(" 3 "))
  )

  val sub   = subtotal(items)
  val total = orderTotal(items)

  println(s"Khách hàng: $customerName")
  println(s"Số dòng hàng: ${items.size}")
  println(s"Tạm tính: ${formatVnd(sub)}")
  println(s"Phí ship: ${formatVnd(shippingFee(sub))}")
  println(s"Tổng cộng: ${formatVnd(total)}")
  println(s"Phân loại số lượng 3: ${quantityCategory(3)}")
  println(s"Phân loại số lượng 25: ${quantityCategory(25)}")
  println(s"Input lỗi 'abc' -> ${parseQuantity("abc")}")
  println(s"Imperative == functional: ${subtotalImperative(items) == sub}")
```

File thứ hai, [`TypesTour.scala`](code/src/main/scala/scalaeng/ch03/TypesTour.scala), minh họa các kiểu cơ bản và cạm bẫy số học.
Test: [`OrderBasicsSuite.scala`](code/src/test/scala/scalaeng/ch03/OrderBasicsSuite.scala) gồm 8 test, chú trọng **boundary case**
(đúng ngưỡng 500.000, số lượng 0 và âm, giỏ hàng rỗng, input rỗng).

## 8. Giải thích code

- `object OrderBasics:` — `object` là một **singleton**: chỉ có một thể hiện, dùng để gom các hàm liên quan
  (tương tự class chứa toàn `static` trong Java). Chương 5 học kỹ.
- Dấu `:` cuối dòng và **thụt lề** thay cho `{ }` — cú pháp Scala 3. Ngoặc nhọn vẫn hợp lệ.
- `BigDecimal(500_000)` — tạo từ `Int`, chính xác tuyệt đối. Thứ cần tránh là tạo `BigDecimal` từ **kết quả tính toán `Double`**.
- `shippingFee` — `if/then/else` là expression nên thân hàm chỉ có một biểu thức, không cần `return`.
  **Trong Scala gần như không bao giờ dùng `return`.**
- `quantityCategory` — `case q if q <= 0` gắn giá trị vào tên `q` rồi kiểm tra guard. Các `case` được thử
  **theo thứ tự từ trên xuống**; `case _` là "mọi trường hợp còn lại".
- `parseQuantity` — `toIntOption` không ném exception mà trả `Option[Int]`; `getOrElse(0)` lấy giá trị hoặc mặc định.
  (Trả 0 cho input lỗi là đơn giản hóa; Chương 7 sẽ làm tốt hơn bằng `Either` để không mất thông tin lỗi.)
- `formatVnd` — `setScale(0, HALF_UP)` làm tròn tới đồng trước khi đổi sang `Long`, tránh **cắt** phần thập phân âm thầm;
  `formatLocal(Locale.US, ...)` cố định dấu phân cách.
- `invalidInput(...): Nothing` — kiểu `Nothing` cho biết hàm không trả về bình thường.
- `items.map((price, qty) => lineTotal(price, qty)).sum` — Scala 3 cho phép lambda nhận **trực tiếp hai tham số**
  khi phần tử là tuple 2 (parameter untupling). `.sum` cộng tất cả (cần kiểu số).
- `subtotalImperative` — `var` chỉ sống trong hàm, không thoát ra ngoài: đây là cách dùng `var` chấp nhận được.
  `for (price, qty) <- items do` tách tuple ngay trong vòng lặp.
- `import OrderBasics.*` — `*` là wildcard import của Scala 3 (Scala 2 dùng `_`).

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch03.ch03Demo
Khách hàng: Nguyễn Văn An
Số dòng hàng: 2
Tạm tính: 495,000 VND
Phí ship: 30,000 VND
Tổng cộng: 525,000 VND
Phân loại số lượng 3: small
Phân loại số lượng 25: bulk
Input lỗi 'abc' -> 0
Imperative == functional: true
```

```
$ scala-cli run . --main-class scalaeng.ch03.ch03Types
0.1 + 0.2 (Double)     = 0.30000000000000004
0.1 + 0.2 (BigDecimal) = 0.3
Int.MaxValue + 1       = -2147483648
Long lớn               = 3000000000
Int(42), String(Scala), Boolean(true), Unit
```

## 10. Lỗi thường gặp

Các thông báo dưới đây là **output thật** của `scalac 3.9.0`.

**Lỗi 1 — gán lại `val`:**

```scala
val price = 100
price = 200
```
```
-- [E052] Type Error: E1.scala:3:8
3 |  price = 200
  |  ^^^^^^^^^^^
  |  Reassignment to val price
```
Cách sửa: tạo `val` mới (`val discounted = price - 10`) — hầu như luôn tốt hơn đổi sang `var`.

**Lỗi 2 — `if` thiếu `else`:**

```scala
def fee(total: Int) = if total > 100 then 0
val x: Int = fee(5)
```
```
-- [E007] Type Mismatch Error: E3.scala:3:18
3 |  val x: Int = fee(5)
  |               ^^^^^^
  |               Found:    Unit
  |               Required: Int
-- [E190] Potential Issue Warning: E3.scala:2:44
2 |  def fee(total: Int) = if total > 100 then 0
  |                                            ^
  |   Discarded non-Unit value of type Int. Add `: Unit` to discard silently.
```
Giải thích: `if` không có `else` có kiểu `Unit`, nên `fee` trả `Unit`. Cách sửa: luôn viết đủ `else`
và **ghi kiểu trả về** cho method (`def fee(total: Int): Int = ...`) để lỗi xuất hiện ngay tại chỗ định nghĩa.

**Lỗi 3 — tiền tính bằng `Double`:** không có lỗi compile, chỉ sai kết quả (`0.30000000000000004`). Nguy hiểm hơn lỗi compile.

**Lỗi 4 — tràn `Int`:** không có lỗi, chỉ ra số âm. Dùng `Long` cho bộ đếm/ID, hoặc `Math.addExact` nếu muốn ném exception khi tràn.

**Lỗi 5 — `Map.apply` với key không tồn tại:** `stock("X")` ném `NoSuchElementException: key not found: X`. Dùng `get`/`getOrElse`.

**Lỗi 6 — output phụ thuộc Locale:** test `formatVnd` pass trên máy bạn nhưng fail trên CI. Luôn chỉ định Locale.

## 11. Cách debug

- **Hỏi compiler kiểu của một biểu thức:** trong REPL gõ biểu thức; trong IDE rê chuột. Nếu thấy `Any`, `AnyVal`,
  `Matchable` hoặc `Unit` ở chỗ không ngờ tới → có nhánh trả về kiểu khác.
- **Ghi kiểu tường minh tạm thời** (`val x: BigDecimal = ...`) để compiler báo lỗi đúng chỗ.
- **In giá trị trung gian:** `println(s"debug: sub=$sub")` — nhớ xóa trước khi commit, hoặc dùng logging (Chương 9).
- **Viết test boundary trước** khi sửa bug: tái hiện lỗi bằng test, sửa, giữ test lại làm regression test.

## 12. Bài tập

Viết lời giải trong một file mới (ví dụ `src/main/scala/scalaeng/ch03/Exercises.scala`) và viết test cho từng bài.

1. **(Cơ bản)** Viết `describeProduct(name: String, price: BigDecimal, quantity: Int): String` trả về
   chuỗi dạng `"Bút bi x3 = 15000"`.
2. **(Cơ bản)** Viết `isValidPhone(raw: String): Boolean`: hợp lệ khi (sau khi `trim`) có đúng 10 ký tự, bắt đầu bằng `0`,
   và toàn bộ là chữ số. Gợi ý: `startsWith`, `forall(_.isDigit)`.
3. **(Trung bình)** Viết `vatAmount(subtotal: BigDecimal, rate: BigDecimal): BigDecimal` làm tròn **HALF_UP** tới đơn vị đồng.
   Kiểm tra: `vatAmount(12345, 0.1) == 1235`, `vatAmount(12344, 0.1) == 1234`.
4. **(Trung bình)** Viết `classifyStatus(status: Int): String` trả về `"informational" | "success" | "redirection" |
   "client-error" | "server-error" | "unknown"` theo dải HTTP status 1xx–5xx. Viết test cho các giá trị biên 199, 200, 599, 600.
5. **(Nâng cao)** Viết `parseOrderLine(line: String): (String, BigDecimal, Int)` cho dòng CSV dạng `"SKU-001, 120000, 2"`.
   Yêu cầu: bỏ khoảng trắng thừa; ném `IllegalArgumentException` có thông điệp rõ ràng khi: sai số cột, đơn giá không phải số,
   số lượng không phải số nguyên, SKU rỗng, giá âm hoặc số lượng ≤ 0.
   *Câu hỏi suy nghĩ:* ném exception có phải cách tốt cho pipeline xử lý 1 triệu dòng? (Chương 7 sẽ trả lời.)

## 13. Đáp án và lời giải

- Code lời giải: [`solutions/code/src/main/scala/scalaeng/solutions/Ch03Solutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch03Solutions.scala)
- Test lời giải: [`solutions/code/src/test/scala/scalaeng/solutions/Ch03SolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch03SolutionsSuite.scala)
- Giải thích: [`solutions/ch03-solutions.md`](../solutions/ch03-solutions.md)

## 14. Câu hỏi kiểm tra

1. `val xs = ArrayBuffer(1)` rồi `xs += 2` có lỗi không? Vì sao?
2. Kết quả và kiểu của `if false then 1`?
3. Vì sao `throw` dùng được ở vị trí cần `Int`?
4. Khi nào nên ghi kiểu trả về cho method? Trường hợp nào **bắt buộc**?
5. `BigDecimal(0.1 + 0.2)` và `BigDecimal("0.1") + BigDecimal("0.2")` cho kết quả gì? Còn `new java.math.BigDecimal(0.1)`? (Thử trong REPL.)
6. `f"$x%.2f"` với `x: String` sẽ thế nào — lỗi lúc biên dịch hay lúc chạy?
7. Kiểu mà compiler suy ra cho `List(1, "a", true)` là gì? (Gợi ý: gán nó vào một `val n: Int` và đọc thông báo lỗi.) Điều đó gợi ý vấn đề gì?

## 15. Mini project — Máy tính hóa đơn

Viết chương trình `@main def invoice(lines: String*): Unit` nhận các tham số dòng lệnh dạng `"SKU,giá,số lượng"`, ví dụ:

```bash
scala-cli run . --main-class scalaeng.ch03.invoice -- "AO-01,120000,2" "TAT-02,85000,3"
```

Yêu cầu: dùng `parseOrderLine` (bài 5), in từng dòng, tạm tính, VAT 10% (dùng `vatAmount`), phí ship, tổng cộng bằng `formatVnd`.
Dòng lỗi được in ra kèm lý do và **không** làm dừng chương trình (gợi ý: `try/catch` quanh từng dòng).
Viết test cho phần tính toán (không test phần `println`).

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — A Taste of Scala: https://docs.scala-lang.org/scala3/book/taste-intro.html
- Variables and Data Types: https://docs.scala-lang.org/scala3/book/taste-vars-data-types.html
- Control Structures: https://docs.scala-lang.org/scala3/book/control-structures.html
- First Look at Types (hệ thống kiểu): https://docs.scala-lang.org/scala3/book/first-look-at-types.html
- String Interpolation: https://docs.scala-lang.org/scala3/book/string-interpolation.html
- Scala 3 Reference — Optional Braces: https://docs.scala-lang.org/scala3/reference/other-new-features/indentation.html
- API `scala.math.BigDecimal`: https://www.scala-lang.org/api/current/scala/math/BigDecimal.html

## 17. Checklist tự đánh giá

- [ ] Tôi mặc định dùng `val` và giải thích được khi nào `var` chấp nhận được.
- [ ] Tôi phân biệt được immutable reference và immutable object.
- [ ] Tôi dùng `BigDecimal` cho tiền và biết vì sao.
- [ ] Tôi biết `Int` tràn số im lặng.
- [ ] Tôi giải thích được `Unit`, `Nothing`, `Null`, `Any`.
- [ ] Tôi viết `if`/`match` như expression, không dùng `return`.
- [ ] Tôi dùng được `s`, `f`, `raw` và biết cạm bẫy Locale.
- [ ] Tôi đã làm xong 5 bài tập và test của tôi pass.
