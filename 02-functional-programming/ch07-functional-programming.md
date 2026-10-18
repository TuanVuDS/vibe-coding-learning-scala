# Chương 7 — Functional Programming

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4 · Không dùng thư viện FP bên ngoài (Cats sẽ học ở Chương 8B/16B)
> **Code:** [`../01-scala3-core/code/src/main/scala/scalaeng/ch07/`](../01-scala3-core/code/src/main/scala/scalaeng/ch07/) ·
> Test: [`OrderParsingSuite.scala`](../01-scala3-core/code/src/test/scala/scalaeng/ch07/OrderParsingSuite.scala)
> **Trạng thái kiểm chứng:** Code **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo). 13/13 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Code của chương nằm chung project `01-scala3-core/code` để dùng chung cấu hình build.

---

## 1. Mục tiêu học tập

- Nhận biết **pure function**, **side effect**, **referential transparency** và vì sao chúng quan trọng cho test và bảo trì.
- Dùng `Option` thay `null`, `Either` thay exception cho lỗi nghiệp vụ, `Try` để bọc API ném exception.
- Viết **for-comprehension** trên `Option`/`Either`.
- Mô hình hóa lỗi và dữ liệu bằng **Algebraic Data Types** và xử lý bằng **pattern matching** có kiểm tra đầy đủ.
- Phân biệt **fail-fast** và **tích lũy lỗi**.
- Tổ chức chương trình theo **Functional Core, Imperative Shell**.
- Áp dụng vào pipeline parse–validate–dedup–aggregate dữ liệu đơn hàng.

## 2. Kiến thức tiên quyết

- Chương 5 (case class, enum), Chương 6 (`map`, `flatMap`, `foldLeft`, `partitionMap`).

## 3. Lý thuyết

### 3.1. Pure function và side effect

Một hàm **thuần** (pure):
1. Cùng input luôn cho cùng output.
2. Không gây tác động quan sát được bên ngoài (không in, không ghi file/DB, không sửa biến bên ngoài, không ném exception như một phần kết quả thông thường).

```scala
def applyVat(amount: BigDecimal, rate: BigDecimal): BigDecimal = amount + amount * rate   // thuần

private var globalRate = BigDecimal("0.1")
def applyVatImpure(amount: BigDecimal): BigDecimal = amount + amount * globalRate        // không thuần
```

Output thật: `Impure: lần 1 = 110.0, lần 2 = 108.00 (cùng input, khác output)`.

| Side effect thường gặp | BE | DE |
|---|---|---|
| I/O | Gọi DB, gọi HTTP, gửi email | Đọc/ghi file, Kafka, object storage |
| Thời gian, ngẫu nhiên | `Instant.now()`, UUID | `current_date` trong job → rerun cho kết quả khác |
| Trạng thái chia sẻ | Biến toàn cục, cache | Accumulator, biến trong closure gửi lên executor |
| Exception | Ném lỗi nghiệp vụ | Một dòng lỗi giết cả job |

**Vì sao quan trọng:** hàm thuần test được bằng một dòng `assertEquals`, chạy song song an toàn, chạy lại (rerun/backfill) cho cùng kết quả.
Chương trình không thể *không có* side effect — mục tiêu là **đẩy side effect ra rìa** (mục 3.8).

### 3.2. Referential transparency

Một biểu thức là *referentially transparent* nếu thay nó bằng giá trị của nó không làm thay đổi ý nghĩa chương trình.
`applyVat(100, 0.1)` luôn thay được bằng `110.0`. `applyVatImpure(100)` thì không — giá trị phụ thuộc lúc gọi.
Đây là tiêu chí kiểm tra "thuần" trong thực tế: *"tôi có thể cache/gọi lại/bỏ lời gọi này mà chương trình không đổi?"*

### 3.3. Immutability (nhắc lại có mục đích)

Dữ liệu bất biến (case class, collection immutable) là điều kiện để hàm thuần có ý nghĩa: nếu input có thể bị sửa sau khi gọi, "cùng input" không còn rõ ràng.
Trong DE: bản ghi bất biến cho phép giữ bản gốc (raw) song song với bản đã làm sạch — nền tảng của rerun và audit.

### 3.4. `Option` — giá trị có thể vắng mặt

- **Vấn đề:** `null` khiến mọi chỗ dùng đều phải nhớ kiểm tra; quên → `NullPointerException` lúc chạy.
- **Giải pháp:** `Option[A]` là `Some(a)` hoặc `None`. Kiểu nói rõ "có thể không có", compiler buộc xử lý.

```scala
final case class Customer(id: String, name: String, email: Option[String], referrerId: Option[String])

def emailDomain(db: Map[String, Customer], id: String): Option[String] =
  for
    customer <- db.get(id)           // Option[Customer]
    email    <- customer.email       // Option[String]
    at        = email.indexOf('@')   // giá trị trung gian (không phải Option)
    if at > 0                        // điều kiện: sai → None
  yield email.substring(at + 1).toLowerCase
```

| Thao tác | Ý nghĩa |
|---|---|
| `map(f)` | Biến đổi giá trị nếu có |
| `flatMap(f)` | Nối tiếp một bước cũng trả `Option` |
| `getOrElse(d)` | Lấy giá trị hoặc mặc định |
| `fold(ifEmpty)(f)` | Xử lý cả hai trường hợp |
| `toRight(err)` | Chuyển thành `Either`, `None` → `Left(err)` |
| `.get` | **Tránh** — ném `NoSuchElementException: None.get` |

Từ thư viện Java có thể trả `null`: bọc bằng `Option(x)` (`Option(null) == None`).

### 3.5. `Either` — kết quả hoặc lỗi

`Either[E, A]` là `Left(e)` (lỗi) hoặc `Right(a)` (thành công). Từ Scala 2.12, `Either` *right-biased*: `map`/`flatMap`/for-comprehension làm việc trên `Right`.

```scala
def parseLine(lineNo: Int, line: String): Either[ParseError, OrderRecord] =
  val cols = line.split(",", -1).map(_.trim).toList
  for
    _       <- Either.cond(cols.size == 5, (), WrongColumnCount(lineNo, cols.size))
    orderId <- nonEmpty(lineNo, "order_id", cols(0))
    sku     <- nonEmpty(lineNo, "sku", cols(1))
    price   <- parseDecimal(lineNo, "unit_price", cols(2)).flatMap(positive(lineNo, "unit_price", _))
    qty     <- parseInt(lineNo, "quantity", cols(3)).flatMap(positive(lineNo, "quantity", _))
    date    <- parseDate(lineNo, cols(4))
  yield OrderRecord(orderId, sku, price, qty, date)
```

So với Chương 3 (`parseOrderLine` ném exception):

| | Exception | `Either` |
|---|---|---|
| Kiểu trả về nói rằng có thể lỗi | Không | Có |
| Compiler buộc xử lý | Không | Có |
| Lỗi là dữ liệu (đếm, ghi file, gửi DLQ) | Phải bắt rồi chuyển | Sẵn có |
| Chi phí | Tạo stack trace | Một đối tượng nhỏ |
| Dùng khi | Lỗi lập trình/không thể phục hồi (bug, hết bộ nhớ) | Lỗi nghiệp vụ, input sai — **có thể dự đoán** |

### 3.6. `Try` — cầu nối với code ném exception

```scala
def parseDate(lineNo: Int, raw: String): Either[ParseError, LocalDate] =
  Try(LocalDate.parse(raw)).toEither.left.map(_ => InvalidDate(lineNo, raw))
```

`Try[A]` là `Success(a)` hoặc `Failure(throwable)`. Dùng ở **biên** với API Java/thư viện ném exception, rồi chuyển ngay sang `Either` với lỗi của domain.
Không để `Throwable` chung chung lan vào logic nghiệp vụ. Lưu ý: `Try` **không** bắt các lỗi nghiêm trọng như `OutOfMemoryError` (chỉ bắt `NonFatal`).

### 3.7. ADT và pattern matching

**Algebraic Data Type** = kết hợp của:
- **Product type** (VÀ): case class — `OrderRecord` có orderId **và** sku **và** ...
- **Sum type** (HOẶC): enum — `ParseError` là `WrongColumnCount` **hoặc** `InvalidNumber` **hoặc** ...

```scala
enum ParseError:
  case WrongColumnCount(lineNo: Int, found: Int)
  case InvalidNumber(lineNo: Int, field: String, raw: String)
  case InvalidDate(lineNo: Int, raw: String)
  case NonPositive(lineNo: Int, field: String)
  case EmptyField(lineNo: Int, field: String)

  def message: String = this match
    case WrongColumnCount(n, found)   => s"dòng $n: cần 5 cột, nhận $found"
    case InvalidNumber(n, field, raw) => s"dòng $n: $field không phải số: '$raw'"
    ...
```

Pattern matching **phân rã** (destructure) dữ liệu và compiler **kiểm tra đầy đủ** (exhaustiveness, xem Chương 5 mục 10).
Nguyên tắc "*make illegal states unrepresentable*": thiết kế kiểu sao cho trạng thái sai không biểu diễn được — thay vì `status: String` + `errorMessage: String` có thể null, dùng enum có tham số.

### 3.8. Fail-fast và tích lũy lỗi

- for-comprehension trên `Either` dừng ở **lỗi đầu tiên** (test: dòng `", S1, x, 0, bad"` chỉ báo `EmptyField(order_id)`). Phù hợp khi các bước phụ thuộc nhau.
- Khi các kiểm tra **độc lập** (form đăng ký có 5 field), người dùng muốn thấy **mọi** lỗi cùng lúc → **tích lũy lỗi**. Không có sẵn trong standard library;
  bài tập 4 tự cài bằng `List`; Chương 8B giới thiệu `cats.data.Validated`/`ValidatedNel` (thư viện ngoài).
- Ở mức **tập dữ liệu**, `partitionMap` cho cả hai: mỗi dòng fail-fast, nhưng toàn bộ file vẫn được xử lý và mọi dòng lỗi được thu thập.

### 3.9. Functional Core, Imperative Shell

```
┌────────────── Imperative Shell (@main, HTTP handler, Kafka consumer) ─────────────┐
│ đọc file/DB/request ──► [ FUNCTIONAL CORE: hàm thuần ] ──► ghi file/DB/response    │
│                         parse, validate, dedup, aggregate, quyết định nghiệp vụ    │
└────────────────────────────────────────────────────────────────────────────────────┘
```

- `OrderPipeline.run(lines: List[String]): Report` — **thuần**: không đọc file, không in.
- `@main ch07Demo` — **shell**: lấy dữ liệu, gọi core, in kết quả.
- Test cho core không cần file, mock hay database. Test "chạy lại cho cùng kết quả" là một dòng.
- Trong DE, đây là cách làm job **idempotent và rerun được**: nếu core thuần và input cố định, output cố định.
- Lỗi thiết kế thường gặp: lấy `LocalDate.now()` bên trong core → backfill ngày cũ cho kết quả sai. Truyền ngày xử lý vào như một tham số.

### 3.10. Trạng thái theo sự kiện: `foldLeft` + `Either`

```scala
def replay(initial: BigDecimal, events: List[WalletEvent]): Either[WalletError, BigDecimal] =
  events.foldLeft[Either[WalletError, BigDecimal]](Right(initial)): (acc, e) =>
    acc.flatMap(applyEvent(_, e))
```

Trạng thái hiện tại = fold các sự kiện trên trạng thái ban đầu. Đây là ý tưởng cốt lõi của **event sourcing** (BE) và
**stateful stream processing** (DE, Chương 27): state là kết quả gộp của luồng sự kiện.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Pure function | Hàm thuần |
| Side effect | Tác dụng phụ |
| Referential transparency | Tính trong suốt tham chiếu |
| Option / Some / None | Giá trị có thể vắng mặt |
| Either / Left / Right | Lỗi hoặc kết quả |
| Try / Success / Failure | Kết quả của code có thể ném exception |
| Right-biased | `map`/`flatMap` tác động lên nhánh `Right` |
| For-comprehension | Cú pháp `for ... yield` cho `flatMap`/`map`/`withFilter` |
| Algebraic Data Type (ADT) | Kiểu dữ liệu đại số: sum + product type |
| Exhaustiveness checking | Kiểm tra `match` đủ trường hợp |
| Fail-fast / error accumulation | Dừng ở lỗi đầu / thu thập mọi lỗi |
| Functional core, imperative shell | Lõi thuần, vỏ có side effect |
| Idempotent | Chạy nhiều lần cho cùng kết quả như chạy một lần |
| Event sourcing | Lưu trạng thái dưới dạng chuỗi sự kiện |

## 5. Ví dụ đơn giản

```scala
def parseAge(s: String): Either[String, Int] =
  s.toIntOption.toRight(s"'$s' không phải số").flatMap: n =>
    if n >= 0 && n <= 150 then Right(n) else Left(s"tuổi ngoài khoảng: $n")

parseAge("30")   // Right(30)
parseAge("abc")  // Left('abc' không phải số)
parseAge("200")  // Left(tuổi ngoài khoảng: 200)

val nick: Option[String] = None
nick.map(_.toUpperCase).getOrElse("(ẩn danh)")   // "(ẩn danh)"
```

## 6. Ví dụ thực tế — pipeline đơn hàng (DE) và tra cứu khách hàng (BE)

**DE:** file CSV đơn hàng có dòng sai số cột, giá không phải số, SKU rỗng, số lượng 0, ngày không tồn tại (`2026-13-40`), và một `order_id` trùng.
Yêu cầu: không dừng ở dòng lỗi; báo cáo từng lỗi kèm số dòng; dedup giữ bản ghi đầu tiên; doanh thu theo ngày; chạy lại cho cùng kết quả.

**BE:** tra cứu khách hàng mà email và người giới thiệu là tùy chọn; người giới thiệu có thể không còn tồn tại trong DB.

## 7. Code đầy đủ

- [`OrderParsing.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch07/OrderParsing.scala) — `ParseError`, `OrderRecord`, `OrderParsing`, `OrderPipeline`, `Customer`, `CustomerLookup`, `@main ch07Demo`.
- [`Purity.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch07/Purity.scala) — pure/impure, `WalletEvent`, `replay`, `@main ch07Purity`.

Trích đoạn chưa có ở mục 3:

```scala
object OrderParsing:
  def parseDecimal(lineNo: Int, field: String, raw: String): Either[ParseError, BigDecimal] =
    Try(BigDecimal(raw)).toOption.toRight(InvalidNumber(lineNo, field, raw))

  def positive[A](lineNo: Int, field: String, value: A)(using num: Numeric[A]): Either[ParseError, A] =
    if num.gt(value, num.zero) then Right(value) else Left(NonPositive(lineNo, field))

  def parseAll(lines: List[String]): (List[ParseError], List[OrderRecord]) =
    lines.zipWithIndex.partitionMap((line, idx) => parseLine(idx + 1, line))

object OrderPipeline:
  def dedup(records: List[OrderRecord]): (List[OrderRecord], List[String]) =
    val (kept, dups, _) = records.foldLeft((List.empty[OrderRecord], List.empty[String], Set.empty[String])):
      case ((kept, dups, seen), r) =>
        if seen.contains(r.orderId) then (kept, r.orderId :: dups, seen)
        else (r :: kept, dups, seen + r.orderId)
    (kept.reverse, dups.reverse)

  def revenueByDate(records: List[OrderRecord]): List[(LocalDate, BigDecimal)] =
    records.groupMapReduce(_.orderDate)(_.total)(_ + _).toList.sortBy(_._1)

  def run(lines: List[String]): Report =
    val (errors, records) = OrderParsing.parseAll(lines)
    val (unique, dups)    = dedup(records)
    Report(unique.size, errors, dups, revenueByDate(unique))
```

## 8. Giải thích code

- `line.split(",", -1)` — tham số `-1` giữ các cột rỗng ở cuối; không có nó, `"a,b,"` chỉ cho 2 phần tử.
- `Either.cond(điều kiện, giá trị, lỗi)` — tạo `Right` hoặc `Left`; ở đây kiểm tra số cột trước khi truy cập `cols(i)`. Thứ tự trong for-comprehension đảm bảo `cols(0)` chỉ được gọi khi đã đủ 5 cột.
- `.toRight(err)` trên `Option` — chuyển "không có" thành lỗi có nghĩa.
- `positive[A](...)(using num: Numeric[A])` — một hàm dùng cho cả `BigDecimal` và `Int` nhờ **type class** `Numeric` của standard library. `using` là tham số ngữ cảnh — Chương 8B giải thích; ở đây chỉ cần biết compiler tự tìm `Numeric[Int]`/`Numeric[BigDecimal]`.
- `zipWithIndex` + `partitionMap` — gắn số dòng (bắt đầu từ 0 nên `+1`) và tách `Left`/`Right` trong một lượt.
- `dedup` dùng `foldLeft` với tuple ba thành phần (đã giữ, id trùng, tập id đã thấy). Prepend `::` rồi `reverse` ở cuối — tránh `:+` O(n) mỗi bước.
  `Set` cho `contains` gần như hằng số.
- Quy tắc dedup "giữ bản đầu tiên" là **quyết định nghiệp vụ** và được ghi trong comment + test. Bài tập Chương 6 dùng quy tắc khác (giữ bản mới nhất theo `updatedAt`). Không có quy tắc "đúng" chung.
- `Report` là case class → `assertEquals(run(x), run(x))` kiểm tra tính xác định của toàn pipeline.
- `CustomerLookup.referrerName` — chuỗi `flatMap`: tìm khách → lấy `referrerId` → tìm người giới thiệu → lấy tên → mặc định. Bất kỳ bước nào `None` đều cho kết quả mặc định, không có `if (x != null)` lồng nhau.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch07.ch07Demo
Hợp lệ (sau dedup): 3
Bị loại: 5
  - dòng 3: unit_price không phải số: 'abc'
  - dòng 4: sku rỗng
  - dòng 5: quantity phải > 0
  - dòng 6: ngày không hợp lệ: '2026-13-40'
  - dòng 7: cần 5 cột, nhận 3
Trùng order_id: List(ORD-1)
  2026-10-01: 495000
  2026-10-02: 350000
Domain C1: Some(example.com)
Domain C2: None
Domain C3: None
Domain C404: None
Người giới thiệu của C2: An
Người giới thiệu của C3: (không có)
```

```
$ scala-cli run . --main-class scalaeng.ch07.ch07Purity
Pure: 110.0, 110.0  (thay biểu thức bằng giá trị không đổi nghĩa)
Impure: lần 1 = 110.0, lần 2 = 108.00 (cùng input, khác output)
Right(350)
Left(InsufficientFunds(100,300))
```

Kiểm tra số liệu: ngày 01/10 có ORD-1 (120.000 × 2) + ORD-2 (85.000 × 3) = 495.000; bản ORD-1 thứ hai bị loại vì trùng. Ngày 02/10 chỉ ORD-8 hợp lệ = 350.000.

## 10. Lỗi thường gặp

**Lỗi 1 — trộn `Option` và `Either` trong một for-comprehension** (output thật):
```scala
val r: Either[String, Int] = for
  a <- Right(1)
  b <- Option(2)
yield a + b
```
```
-- [E007] Type Mismatch Error: F.scala:4:4
4 |    b <- Option(2)
  |    ^
  |    Found:    Option[Int]
  |    Required: Either[String, Int]
```
Mọi generator trong một for-comprehension phải cùng "loại hộp". Chuyển đổi: `Option(2).toRight("thiếu b")`.

**Lỗi 2 — `.get` trên `None`:** `java.util.NoSuchElementException: None.get` (output thật). Gần như luôn có cách tốt hơn: `getOrElse`, `fold`, `match`, hoặc trả `Option` tiếp.

**Lỗi 3 — `match` thiếu trường hợp trên ADT:** cảnh báo `match may not be exhaustive` (Chương 5). Với `Either`, thiếu `case Left(_)` cũng bị cảnh báo.

**Lỗi 4 — nuốt lỗi:** `Try(...).toOption` hoặc `.getOrElse(0)` biến lỗi thành giá trị "bình thường", mất thông tin. Chương 3 `parseQuantity` trả 0 cho input sai là ví dụ — dữ liệu sai lặng lẽ đi vào báo cáo.

**Lỗi 5 — `Either[String, A]` khắp nơi:** lỗi là chuỗi tự do → không thể đếm theo loại, không thể xử lý khác nhau. Dùng ADT lỗi.

**Lỗi 6 — "FP giả":** hàm trả `Either` nhưng bên trong vẫn gọi `println`, ghi DB, đọc `LocalDate.now()`. Chữ ký đẹp không làm hàm thành thuần.

## 11. Cách debug

- Tách for-comprehension dài thành từng bước `val step1: Either[ParseError, X] = ...` để thấy bước nào trả `Left`.
- In `Left` bằng `message` của ADT thay vì `toString` — viết `message` ngay từ đầu giúp log đọc được.
- Với pipeline dữ liệu: luôn đếm `input = valid + rejected + duplicates`. Nếu không cân → có bản ghi bị mất ở đâu đó (ở ví dụ: 9 = 3 + 5 + 1).
- Kiểm tra tính thuần: chạy hàm hai lần với cùng input, so sánh; tìm trong thân hàm các lời gọi `now`, `random`, `println`, biến `var` bên ngoài.

## 12. Bài tập

1. **(Cơ bản)** `parsePositiveInt(s: String): Option[Int]` — `Some` khi là số nguyên > 0 (cho phép khoảng trắng hai đầu).
2. **(Cơ bản)** Với `Map[String, User]` trong đó `User(id, managerId: Option[String], email: Option[String])`,
   viết `managerEmail(db, userId): Option[String]` bằng for-comprehension.
3. **(Trung bình)** Viết ADT `DateError` (`Empty`, `BadFormat(raw)`, `InFuture(date)`) và `parseBirthDate(raw: String, today: LocalDate): Either[DateError, LocalDate]`.
   Chú ý: `today` là **tham số** — vì sao?
4. **(Trung bình)** Tích lũy lỗi: `validateSignup(name: String, email: String, age: String): Either[List[SignupError], Signup]`
   trả **tất cả** lỗi (tên rỗng, email thiếu `@`, tuổi không phải số hoặc < 13) chứ không chỉ lỗi đầu tiên. Không dùng thư viện ngoài.
5. **(Nâng cao)** Hệ thống tồn kho theo sự kiện: `enum StockEvent { Received(sku, qty), Reserved(sku, qty), Released(sku, qty) }`.
   Viết `applyAll(initial: Map[String, Int], events: List[StockEvent]): Either[StockError, Map[String, Int]]` — lỗi khi giữ hàng vượt tồn kho
   hoặc số lượng ≤ 0; dừng ở lỗi đầu tiên và báo **chỉ số** sự kiện gây lỗi.

## 13. Đáp án và lời giải

- Code: [`Ch07Solutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch07Solutions.scala) · Test: [`Ch07SolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch07SolutionsSuite.scala)
- Giải thích: [`solutions/ch07-solutions.md`](../solutions/ch07-solutions.md)

## 14. Câu hỏi kiểm tra

1. Nêu hai điều kiện của một pure function.
2. Vì sao `LocalDate.now()` bên trong logic làm hỏng khả năng backfill của một job dữ liệu?
3. Khi nào dùng exception, khi nào dùng `Either`?
4. Vai trò của `Try` trong code nghiệp vụ là gì, và nên dừng ở đâu?
5. Vì sao `Either[String, A]` kém hơn `Either[MyError, A]`?
6. For-comprehension trên `Either` là fail-fast hay tích lũy lỗi? Khi nào cần cái còn lại?
7. Mô tả Functional Core, Imperative Shell cho một Kafka consumer ghi vào PostgreSQL.
8. Trong ví dụ, vì sao `input = valid + rejected + duplicates` là một kiểm tra quan trọng?

## 15. Mini project — Parser CSV an toàn (tiền thân của Project 1)

Mở rộng `OrderPipeline`:
- Đọc file CSV thật ở shell (bỏ dòng header), core giữ nguyên thuần.
- Ghi `rejected.csv` gồm `line_no, error_type, message, raw_line`.
- Thêm kiểm tra **cấp tập dữ liệu**: nếu tỷ lệ dòng lỗi > 5% thì `Report` mang cờ `qualityFailed = true` và shell trả exit code khác 0.
- Test: dữ liệu toàn dòng tốt, toàn dòng lỗi, file rỗng, file chỉ có header, chạy hai lần cho cùng kết quả.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Functional Programming: https://docs.scala-lang.org/scala3/book/fp-intro.html
- Pure Functions: https://docs.scala-lang.org/scala3/book/fp-pure-functions.html
- Functional Error Handling: https://docs.scala-lang.org/scala3/book/fp-functional-error-handling.html
- Domain Modeling: FP (ADT): https://docs.scala-lang.org/scala3/book/domain-modeling-fp.html
- Pattern matching: https://docs.scala-lang.org/scala3/book/control-structures.html
- Scala 3 Reference — Algebraic Data Types: https://docs.scala-lang.org/scala3/reference/enums/adts.html
- API `scala.util.Either`: https://www.scala-lang.org/api/current/scala/util/Either.html

## 17. Checklist tự đánh giá

- [ ] Tôi nhận ra side effect trong một hàm và biết cách đẩy nó ra rìa.
- [ ] Tôi không dùng `null` và `.get` trong code Scala của mình.
- [ ] Tôi dùng `Either` với ADT lỗi cho lỗi nghiệp vụ, `Try` chỉ ở biên.
- [ ] Tôi viết được for-comprehension trên `Option` và `Either`.
- [ ] Tôi phân biệt fail-fast và tích lũy lỗi, và tự cài được tích lũy lỗi đơn giản.
- [ ] Tôi tách được core thuần và shell trong một chương trình xử lý dữ liệu.
- [ ] Tôi viết được test chứng minh pipeline xác định (chạy lại cho cùng kết quả).
