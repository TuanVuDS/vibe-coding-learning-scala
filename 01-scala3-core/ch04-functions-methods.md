# Chương 4 — Functions và Methods

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4
> **Code:** [`code/src/main/scala/scalaeng/ch04/Functions.scala`](code/src/main/scala/scalaeng/ch04/Functions.scala) · Test: [`code/src/test/scala/scalaeng/ch04/FunctionsSuite.scala`](code/src/test/scala/scalaeng/ch04/FunctionsSuite.scala)
> **Trạng thái kiểm chứng:** Code chính **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo với `-Wunused:all`).
> Test: **đã biên dịch & chạy** — 12/12 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Thông báo lỗi trong mục 10 và hiện tượng `StackOverflowError` là kết quả chạy thật.

---

## 1. Mục tiêu học tập

- Phân biệt **method** (`def`) và **function value** (`A => B`).
- Viết lambda, dùng cú pháp rút gọn `_`.
- Viết và dùng **higher-order function**: nhận hàm, trả về hàm.
- Dùng tham số mặc định, tham số có tên, nhiều danh sách tham số (currying), tham số by-name.
- Viết đệ quy và **đệ quy đuôi** với `@tailrec`; hiểu vì sao đệ quy thường có thể làm tràn stack.
- Ghép hàm bằng `andThen`/`compose` để xây pipeline xử lý dữ liệu.

## 2. Kiến thức tiên quyết

- Chương 3: `val`, kiểu, expression, `match`, `List` cơ bản.

## 3. Lý thuyết

### 3.1. Method: `def`

```scala
def lineTotal(unitPrice: BigDecimal, quantity: Int): BigDecimal =
  unitPrice * quantity
```

- Thân method là một **expression**; giá trị của nó là giá trị trả về. Không cần `return`.
- Kiểu tham số **bắt buộc** ghi; kiểu trả về nên ghi cho method công khai, **bắt buộc** cho method đệ quy.
- Method thuộc về một `object`, `class` hoặc `trait`, hoặc là *top-level definition* trong file (Scala 3).

### 3.2. Function value và lambda

- **Vấn đề:** muốn truyền "một hành vi" làm tham số (ví dụ: tiêu chí lọc, bước làm sạch dữ liệu) hoặc lưu hành vi vào biến.
- **Giải pháp:** trong Scala, hàm là **giá trị** (first-class), có kiểu `A => B`.

```scala
val trim: String => String = s => s.trim      // lambda đầy đủ
val lower: String => String = _.toLowerCase   // rút gọn: _ là tham số duy nhất
val add: (Int, Int) => Int = (a, b) => a + b
val add2: (Int, Int) => Int = _ + _           // mỗi _ là một tham số khác nhau, theo thứ tự
```

**Method và function khác nhau thế nào?**

| | Method (`def`) | Function value (`val f: A => B`) |
|---|---|---|
| Bản chất | Thành viên của object/class | Một đối tượng có method `apply` |
| Generic `[A]` | Có | Hạn chế (Scala 3 có polymorphic function type, mức nâng cao) |
| Tham số mặc định, by-name, nhiều danh sách tham số | Có | Không |
| Truyền làm đối số | Được — compiler tự chuyển (eta-expansion) | Được |

Trong Scala 3, `list.map(lineTotalFor)` với `lineTotalFor` là một `def` hoạt động trực tiếp nhờ **eta-expansion tự động**.
Thực tế: viết `def` cho logic, dùng function value khi cần lưu/ghép/truyền hành vi.

### 3.3. Higher-order function (HOF)

HOF là hàm **nhận hàm làm tham số** hoặc **trả về hàm**.

```scala
def applyTwice(f: Int => Int, x: Int): Int = f(f(x))   // nhận hàm
def multiplier(factor: Int): Int => Int = x => x * factor  // trả về hàm
```

Bạn đã dùng HOF từ Chương 3: `map`, `filter`, `getOrElse` của collection đều là HOF.

- **Vì sao quan trọng cho BE/DE:** tách *"làm gì với từng phần tử"* khỏi *"duyệt dữ liệu thế nào"*. Cùng một hàm `normalizeEmail`
  dùng được trong `List.map` (in-memory), trong một Kafka consumer, hay đóng gói thành UDF Spark.
- **Lỗi thiết kế thường gặp:** HOF nhận quá nhiều hàm (5–6 tham số hàm) → khó đọc; lúc đó nên gom thành `trait` (Chương 5).

### 3.4. Tham số: mặc định, có tên, nhiều danh sách, by-name

```scala
// Default + named parameters
def paginate(total: Int, pageSize: Int = 20, page: Int = 1): String = ...
paginate(total = 95)                        // dùng mặc định
paginate(total = 95, page = 3, pageSize = 50) // đổi thứ tự nhờ đặt tên

// Multiple parameter lists (currying)
def discount(rate: BigDecimal)(price: BigDecimal): BigDecimal = price - price * rate
val tenPercentOff: BigDecimal => BigDecimal = discount(BigDecimal("0.1"))  // cố định rate

// By-name parameter: block chỉ được đánh giá khi dùng tới
def timed[A](clock: () => Long)(block: => A): (A, Long) = ...
```

| Kỹ thuật | Giải quyết vấn đề gì | Khi nào dùng |
|---|---|---|
| Default parameter | Tránh viết nhiều overload | Cấu hình có giá trị hợp lý mặc định (page size, timeout) |
| Named argument | Gọi hàm nhiều tham số cùng kiểu dễ nhầm | Hàm có ≥ 3 tham số, đặc biệt `Boolean`/`Int` |
| Currying | Cố định một phần tham số để tạo hàm chuyên biệt; hỗ trợ suy luận kiểu | Tạo "nhà máy hàm", API kiểu `retry(3)(action)` |
| By-name `=> A` | Trì hoãn tính toán | Đo thời gian, log có điều kiện, thử lại (retry) |

**Cạm bẫy by-name:** block được đánh giá **mỗi lần** tham số được dùng. Dùng 2 lần = chạy 2 lần (ví dụ gửi email 2 lần!).
Nếu cần dùng nhiều lần, gán vào `val` trước.

**Thiết kế để test được:** `timed` nhận `clock: () => Long` thay vì gọi `System.nanoTime()` bên trong. Production truyền
đồng hồ thật, test truyền đồng hồ giả trả giá trị cố định. Đây là dạng đơn giản nhất của **dependency injection bằng hàm** — ý tưởng
sẽ quay lại ở Chương 7 (tách logic thuần khỏi side effect) và Chương 18 (kiến trúc).

### 3.5. Recursion và tail recursion

**Đệ quy thường:**

```scala
def factorial(n: Int): BigInt =
  if n <= 1 then BigInt(1) else n * factorial(n - 1)
```

Mỗi lời gọi chiếm một **stack frame** trên stack của thread (kích thước mặc định thường vài trăm KB đến 1 MB).
Với 200.000 phần tử, hàm `totalDurationNaive` trong code **ném `StackOverflowError`** (đã chạy thật).

**Đệ quy đuôi (tail recursion):** lời gọi đệ quy là **việc cuối cùng** hàm làm — không còn phép tính nào chờ kết quả.
Compiler biến nó thành vòng lặp, không tốn thêm stack.

```scala
import scala.annotation.tailrec

def totalDuration(events: List[ClickEvent]): Long =
  @tailrec
  def loop(remaining: List[ClickEvent], acc: Long): Long = remaining match
    case Nil          => acc
    case head :: tail => loop(tail, acc + head.durationMs)  // gọi đệ quy là việc cuối cùng
  loop(events, 0L)
```

- Kỹ thuật **accumulator** (`acc`): mang kết quả trung gian theo lời gọi thay vì cộng sau khi trả về.
- `@tailrec` **không làm** hàm thành tail-recursive; nó yêu cầu compiler **kiểm tra** và báo lỗi nếu không phải. Luôn thêm annotation này.
- `case head :: tail` tách list thành phần tử đầu và phần còn lại (pattern matching, Chương 7).

**Trong thực tế:** bạn hiếm khi tự viết đệ quy để duyệt collection — `foldLeft`, `sum`, `groupMapReduce` đã làm việc đó (Chương 6).
Đệ quy hữu ích cho cấu trúc dạng cây (JSON lồng nhau, cây danh mục sản phẩm) và thuật toán có trạng thái (tách session — bài tập 5).

### 3.6. Function composition

```scala
val normalizeName  = trim.andThen(collapseSpaces)  // trim trước, rồi collapseSpaces
val normalizeEmail = lower.compose(trim)           // trim trước, rồi lower  (đọc từ phải sang trái)
```

- `f.andThen(g)` = `x => g(f(x))` — đọc theo thứ tự thực thi, **nên ưu tiên**.
- `f.compose(g)` = `x => f(g(x))` — thứ tự toán học.

Ghép một danh sách bước bất kỳ thành một hàm:

```scala
def pipeline(steps: List[String => String]): String => String =
  steps.foldLeft(identity[String])(_ andThen _)
```

`identity` là hàm "không làm gì" (`x => x`), là phần tử trung hòa của phép ghép: pipeline rỗng trả lại đúng đầu vào.
Đây là mầm mống của cách thiết kế pipeline làm sạch dữ liệu có cấu hình (Chương 19–20): danh sách bước có thể đọc từ config.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Method | Hàm định nghĩa bằng `def`, thuộc về object/class |
| Function value / first-class function | Hàm là giá trị, gán vào biến, truyền đi được |
| Lambda / anonymous function | Hàm không tên: `x => x + 1` |
| Higher-order function (HOF) | Hàm nhận hoặc trả về hàm |
| Eta-expansion | Tự động chuyển method thành function value |
| Currying | Tách tham số thành nhiều danh sách |
| Partial application | Cố định một phần tham số |
| By-name parameter | Tham số được đánh giá trễ, mỗi lần dùng |
| Tail recursion | Đệ quy mà lời gọi là thao tác cuối cùng |
| Accumulator | Tham số mang kết quả trung gian |
| Stack frame / Stack overflow | Khung bộ nhớ của một lời gọi / tràn stack |
| Function composition | Ghép hàm |

## 5. Ví dụ đơn giản

```scala
@main def simpleFn(): Unit =
  def square(x: Int): Int = x * x
  val inc: Int => Int = _ + 1
  println(List(1, 2, 3).map(square))         // List(1, 4, 9)   — method truyền như function
  println(List(1, 2, 3).map(inc.andThen(square)))  // List(4, 9, 16)
```

## 6. Ví dụ thực tế

**Backend — làm sạch dữ liệu đăng ký người dùng.** Tên người dùng nhập có khoảng trắng thừa, email viết hoa lẫn lộn.
Ta xây các bước nhỏ (`trim`, `lower`, `collapseSpaces`) rồi ghép lại. Mỗi bước test riêng được, ghép lại vẫn đúng.

**Data Engineering — tổng hợp click events.** Một danh sách `ClickEvent(userId, page, durationMs)`:
tính tổng thời gian (đệ quy đuôi), lọc click dài (HOF `filter` nhận ngưỡng), tổng thời gian theo trang (`groupMapReduce`).

> Đây là xử lý **trong bộ nhớ một máy**. `groupMapReduce` trên `List` không có cùng đặc tính với `groupBy` trên Spark DataFrame
> (vốn gây shuffle qua mạng). Logic `_.durationMs`, `_ + _` thì dùng lại được ý tưởng; cách thực thi thì khác hoàn toàn.

## 7. Code đầy đủ

[`code/src/main/scala/scalaeng/ch04/Functions.scala`](code/src/main/scala/scalaeng/ch04/Functions.scala):

```scala
package scalaeng.ch04

import scala.annotation.tailrec

final case class ClickEvent(userId: String, page: String, durationMs: Long)

object TextCleaning:
  val trim: String => String           = _.trim
  val lower: String => String          = _.toLowerCase
  val collapseSpaces: String => String = _.replaceAll("\\s+", " ")

  val normalizeName: String => String  = trim.andThen(collapseSpaces)
  val normalizeEmail: String => String = trim.andThen(lower)
  val normalizeEmailCompose: String => String = lower.compose(trim)

  def pipeline(steps: List[String => String]): String => String =
    steps.foldLeft(identity[String])(_ andThen _)

object HigherOrder:
  def applyTwice(f: Int => Int, x: Int): Int = f(f(x))

  def multiplier(factor: Int): Int => Int = x => x * factor

  def discount(rate: BigDecimal)(price: BigDecimal): BigDecimal =
    price - price * rate

  def paginate(total: Int, pageSize: Int = 20, page: Int = 1): String =
    val pages = if total == 0 then 0 else (total + pageSize - 1) / pageSize
    s"page $page/$pages (size=$pageSize, total=$total)"

  def timed[A](clock: () => Long)(block: => A): (A, Long) =
    val start  = clock()
    val result = block
    val end    = clock()
    (result, end - start)

object Recursion:
  def factorial(n: Int): BigInt =
    if n <= 1 then BigInt(1) else n * factorial(n - 1)

  def totalDuration(events: List[ClickEvent]): Long =
    @tailrec
    def loop(remaining: List[ClickEvent], acc: Long): Long = remaining match
      case Nil          => acc
      case head :: tail => loop(tail, acc + head.durationMs)
    loop(events, 0L)

  def totalDurationNaive(events: List[ClickEvent]): Long = events match
    case Nil          => 0L
    case head :: tail => head.durationMs + totalDurationNaive(tail)

object ClickAnalytics:
  def longClicks(events: List[ClickEvent], minMs: Long): List[ClickEvent] =
    events.filter(_.durationMs >= minMs)

  def durationByPage(events: List[ClickEvent]): Map[String, Long] =
    events.groupMapReduce(_.page)(_.durationMs)(_ + _)

@main def ch04Demo(): Unit = ...   // xem file đầy đủ
```

## 8. Giải thích code

- `final case class ClickEvent(...)` — một bản ghi bất biến có field tên rõ ràng, tự có `equals`, `toString`. `final` để không bị kế thừa. Chi tiết ở Chương 5.
- `_.replaceAll("\\s+", " ")` — regex `\s+` (một hoặc nhiều khoảng trắng); trong chuỗi Scala thường phải viết `\\s`. Có thể dùng `raw"\s+"` nếu muốn.
- `pipeline` — `foldLeft` bắt đầu từ `identity`, mỗi bước ghép thêm một hàm bằng `andThen`. `_ andThen _` là lambda hai tham số `(acc, f) => acc.andThen(f)`.
- `multiplier(3)` trả về một hàm "nhớ" giá trị `factor = 3` — gọi là **closure**.
- `discount(BigDecimal("0.1"))` — gọi với danh sách tham số đầu tiên, được một hàm `BigDecimal => BigDecimal`. Ta ghi kiểu tường minh cho `tenPercentOff` để rõ ý.
- `paginate` — `(total + pageSize - 1) / pageSize` là phép chia làm tròn lên với số nguyên dương. Hàm này **cố tình chưa kiểm tra** `page` vượt quá số trang: output `page 3/2` bên dưới là bug, bạn sẽ sửa ở bài tập 3.
- `timed` — `block: => A` không được tính lúc gọi hàm, mà ở dòng `val result = block`, **giữa** hai lần đọc đồng hồ. Nếu khai báo `block: A` (by-value), nó đã được tính trước khi vào hàm và thời gian đo luôn bằng 0.
- `loop` lồng trong `totalDuration` — giấu tham số `acc` khỏi người gọi; API bên ngoài vẫn gọn.
- `groupMapReduce(key)(value)(combine)` — nhóm theo trang, lấy `durationMs`, cộng dồn. Một lượt duyệt, không tạo `Map[String, List[...]]` trung gian như `groupBy(...).map(...)`.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch04.ch04Demo
[Trần Thị Bình]
an.nguyen@example.com
applyTwice(+3, 10) = 16
triple(7) = 21
Giảm 10% của 250000 = 225000.0
page 1/5 (size=20, total=95)
page 3/2 (size=50, total=95)
Tổng thời gian = 10500 ms
Click dài (>= 3000ms) = List(u2, u1)
Theo trang = List((/cart,800), (/home,1200), (/product/42,8500))
20! = 2432902008176640000
Tail-recursive trên 200000 phần tử = 200000
```

Hai điểm cần để ý:

- `225000.0` — `BigDecimal` giữ **scale** (số chữ số thập phân) từ phép nhân với `0.1`. Giá trị đúng, chỉ cách hiển thị khác; dùng `formatVnd` (Chương 3) khi xuất cho người dùng.
- `page 3/2` — bug có chủ đích (bài tập 3).
- `Theo trang` được sắp xếp bằng `sortBy` vì **thứ tự của `Map` không được đảm bảo**; đừng viết test phụ thuộc thứ tự `Map`.

## 10. Lỗi thường gặp

Thông báo lỗi dưới đây là output thật của `scalac 3.9.0`.

**Lỗi 1 — `@tailrec` trên hàm không tail-recursive:**

```scala
@tailrec def sum(xs: List[Int]): Int = xs match
  case Nil => 0
  case h :: t => h + sum(t)
```
```
-- Error: E2.scala:5:26
5 |    case h :: t => h + sum(t)
  |                       ^^^^^^
  |                 Cannot rewrite recursive call: it is not in tail position
```
Nguyên nhân: sau khi `sum(t)` trả về, còn phép `h + ...`. Sửa: thêm accumulator như `totalDuration`.

**Lỗi 2 — method đệ quy thiếu kiểu trả về:**

```scala
def fact(n: Int) = if n <= 1 then 1 else n * fact(n - 1)
```
```
-- [E044] Cyclic Error: E4.scala:2:47
2 |  def fact(n: Int) = if n <= 1 then 1 else n * fact(n - 1)
  |                                               ^
  |                     Overloaded or recursive method fact needs return type
```

**Lỗi 3 — `StackOverflowError` lúc chạy:** `totalDurationNaive` với 200.000 phần tử ném `StackOverflowError` (đã kiểm chứng).
Không có cảnh báo lúc biên dịch. Đây là lý do luôn dùng `@tailrec` hoặc hàm có sẵn của collection.

**Lỗi 4 — `_` dùng sai:** `list.map(_ + _)` nghĩa là hàm **hai** tham số, không phải "cộng phần tử với chính nó". Muốn dùng một tham số hai lần thì viết rõ `x => x + x`.

**Lỗi 5 — by-name bị đánh giá nhiều lần:** hàm dùng tham số by-name trong vòng lặp hoặc ở hai chỗ sẽ chạy block nhiều lần. Gán vào `val` nếu chỉ muốn chạy một lần.

**Lỗi 6 — hàm "thuần" nhưng phụ thuộc thời gian/ngẫu nhiên ẩn:** gọi `System.currentTimeMillis()` hay `Random` bên trong khiến test không ổn định. Truyền vào dưới dạng tham số (như `clock` của `timed`).

## 11. Cách debug

- **Kiểm tra kiểu của hàm:** trong REPL gõ `TextCleaning.normalizeName` sẽ thấy `String => String`. Nếu thấy kiểu lạ (ví dụ `Any => String`), lambda đang bị suy luận sai.
- **Debug pipeline:** chèn bước "nhìn trộm" vào giữa:
  ```scala
  def peek(label: String): String => String = s => { println(s"[$label] '$s'"); s }
  pipeline(List(trim, peek("after trim"), lower))
  ```
- **Debug đệ quy:** thử với input 0, 1, 2 phần tử trước; nếu chạy được với 10 mà chết với 100.000 → tràn stack.
- **Đọc stack trace của `StackOverflowError`:** hàng trăm dòng lặp lại cùng một method là dấu hiệu rõ ràng.

## 12. Bài tập

1. **(Cơ bản)** Viết `compose3[A, B, C, D](f: A => B, g: B => C, h: C => D): A => D` chạy `f` rồi `g` rồi `h`.
2. **(Cơ bản)** Viết `countWhere[A](xs: List[A])(p: A => Boolean): Int` bằng **đệ quy đuôi** có `@tailrec`. Test với list 100.000 phần tử.
3. **(Trung bình)** Sửa bug `page 3/2`: viết `paginateSafe(total: Int, pageSize: Int = 20, page: Int = 1): (Int, Int)` trả về `(trang hiện tại, tổng số trang)`;
   trang luôn nằm trong `[1, tổng số trang]`, tổng số trang tối thiểu là 1, `pageSize <= 0` thì ném `IllegalArgumentException` (gợi ý: `require`).
4. **(Trung bình)** Viết `averageDurationByUser(events: List[(String, Long)]): Map[String, Double]` — thời lượng trung bình theo user. List rỗng trả về `Map` rỗng.
5. **(Nâng cao — DE)** Viết `countSessions(sortedTimestamps: List[Long], gapMs: Long): Int`. Các timestamp (ms, đã sắp xếp) của một user;
   hai sự kiện liên tiếp cách nhau **lớn hơn** `gapMs` thì bắt đầu session mới. Dùng `@tailrec`. Ví dụ: `List(0, 500, 1500, 5000, 5100)` với gap 1000 → 2 session.
   Viết test cho: list rỗng, 1 phần tử, khoảng cách **đúng bằng** gap.

## 13. Đáp án và lời giải

- Code: [`solutions/code/src/main/scala/scalaeng/solutions/Ch04Solutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch04Solutions.scala)
- Test: [`solutions/code/src/test/scala/scalaeng/solutions/Ch04SolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch04SolutionsSuite.scala)
- Giải thích: [`solutions/ch04-solutions.md`](../solutions/ch04-solutions.md)

## 14. Câu hỏi kiểm tra

1. `f.andThen(g)(x)` và `f.compose(g)(x)` lần lượt bằng gì?
2. Vì sao `@tailrec` không tự biến hàm thành tail-recursive?
3. `def log(msg: => String)` khác `def log(msg: String)` thế nào khi log level đang tắt?
4. `List(1,2,3).map(_ * 2)` và `List(1,2,3).map(x => x * x)` — vì sao không thể viết cái thứ hai là `map(_ * _)`?
5. Vì sao truyền `clock: () => Long` giúp test dễ hơn gọi `System.nanoTime()` bên trong hàm?
6. Closure là gì? Cho ví dụ từ code của chương.

## 15. Mini project — Bộ làm sạch dữ liệu người dùng

Viết `UserCleaner` với:
- `normalizeName`, `normalizeEmail`, `normalizePhone` (bỏ khoảng trắng, dấu chấm, gạch nối; đổi `+84` đầu số thành `0`).
- `cleanAll(rows: List[(String, String, String)]): List[(String, String, String)]` áp dụng cả ba.
- Danh sách bước làm sạch của tên phải cấu hình được: `pipeline(List(...))`.
- Test cho từng hàm và cho `cleanAll` với 5 dòng dữ liệu "bẩn" tự soạn (có dấu tiếng Việt, khoảng trắng đầu/cuối, chữ hoa).

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Methods: https://docs.scala-lang.org/scala3/book/methods-intro.html
- Scala 3 Book — Functions: https://docs.scala-lang.org/scala3/book/fun-intro.html
- Higher-Order Functions: https://docs.scala-lang.org/scala3/book/fun-hofs.html
- Eta Expansion: https://docs.scala-lang.org/scala3/book/fun-eta-expansion.html
- Scala 3 Reference — Parameter Untupling: https://docs.scala-lang.org/scala3/reference/other-new-features/parameter-untupling.html
- API `scala.annotation.tailrec`: https://www.scala-lang.org/api/current/scala/annotation/tailrec.html

## 17. Checklist tự đánh giá

- [ ] Tôi phân biệt được method và function value.
- [ ] Tôi viết lambda bằng cả cú pháp đầy đủ và `_`, và biết khi nào `_` gây nhầm.
- [ ] Tôi viết được HOF nhận hàm và trả về hàm.
- [ ] Tôi dùng được default, named, curried và by-name parameter, và biết cạm bẫy của by-name.
- [ ] Tôi viết được đệ quy đuôi với accumulator và `@tailrec`.
- [ ] Tôi giải thích được vì sao đệ quy thường gây `StackOverflowError`.
- [ ] Tôi ghép được pipeline bằng `andThen`/`foldLeft`.
- [ ] Tôi đã làm xong 5 bài tập và test pass.
