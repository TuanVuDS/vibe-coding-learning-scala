# Chương 8A — Types cốt lõi: Generics, Variance, Union/Intersection, Opaque Types, Extension Methods

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4 · Không dùng thư viện ngoài
> **Code:** [`../01-scala3-core/code/src/main/scala/scalaeng/ch08a/`](../01-scala3-core/code/src/main/scala/scalaeng/ch08a/) ·
> Test: [`TypesSuite.scala`](../01-scala3-core/code/src/test/scala/scalaeng/ch08a/TypesSuite.scala)
> **Trạng thái kiểm chứng:** Code **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo). 10/10 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Mọi thông báo lỗi trong mục 10 là output thật của compiler.

---

## Vị trí của chương trong lộ trình

Chương 8 được chia làm ba phần theo mức độ (xem [curriculum](../00-roadmap/01-curriculum.md)):

| Phần | Nội dung | Mức | Khi nào học |
|---|---|---|---|
| **8A (chương này)** | Generics, bounds, variance, union/intersection, type alias, opaque type, extension method | **Bắt buộc** | Ngay sau Chương 7 |
| 8B | given/using, type class, context bound, derivation cơ bản | Bắt buộc cho BE, nên biết cho DE | Ngay sau 8A |
| 8C | Match types, inline, macros, metaprogramming | Chuyên sâu | Sau Chương 16 — khi đã dùng thư viện (circe, Tapir) có derivation |

Không cần thành thạo variance ngay lần đầu. Mục tiêu tối thiểu: **đọc hiểu** chữ ký như `trait Source[+A]`, `def prepend[B >: A]`
trong tài liệu thư viện, và **viết được** opaque type + extension method cho domain của mình.

## 1. Mục tiêu học tập

- Viết class và method generic (`Page[A]`, `def indexById[A <: Entity]`).
- Dùng upper bound `<:` và lower bound `>:` đúng chỗ.
- Giải thích covariance `+A`, contravariance `-A`, invariance — và đọc được lỗi variance của compiler.
- Dùng union type `A | B` và intersection type `A & B` để mô hình hóa mà không cần cây kế thừa.
- Phân biệt type alias (chỉ là tên khác) với opaque type (kiểu mới, không tốn chi phí runtime).
- Thêm hành vi cho kiểu có sẵn bằng extension method.

## 2. Kiến thức tiên quyết

- Chương 5 (trait, case class, enum), Chương 6 (`List[A]`, `Map[K, V]`), Chương 7 (`Option`, `Either`, pattern matching).

## 3. Lý thuyết

### 3.1. Generics

- **Vấn đề:** API trả về danh sách phân trang cho sản phẩm, khách hàng, đơn hàng. Không muốn viết `ProductPage`, `CustomerPage`, `OrderPage`.
- **Giải pháp:** tham số kiểu.

```scala
final case class Page[A](items: List[A], page: Int, pageSize: Int, total: Long):
  def totalPages: Long = if total == 0 then 0 else (total + pageSize - 1) / pageSize
  def map[B](f: A => B): Page[B] = Page(items.map(f), page, pageSize, total)
```

- `A` là **type parameter**; `Page[Product]`, `Page[String]` là các kiểu cụ thể.
- Method có thể có tham số kiểu riêng: `map[B]`.
- JVM **xóa** thông tin kiểu generic lúc chạy (*type erasure*): lúc chạy `Page[Int]` và `Page[String]` cùng là `Page`.
  Hệ quả: `case p: Page[Int] =>` trong `match` **không kiểm tra được** `Int` lúc chạy. Kiểm chứng thật:
  ```
  -- [E092] Pattern Match Unchecked Warning
     |the type test for Page[Int] cannot be checked at runtime because its type arguments can't be determined from Any
  ```
  và hàm `isIntPage(Page(List("a")))` trả về **`true`** — một `Page[String]` bị coi là `Page[Int]`. Đừng bỏ qua cảnh báo này.

### 3.2. Type bounds

**Upper bound `A <: T`:** "A phải là T hoặc con của T" — để dùng được thành viên của T.

```scala
trait Entity:
  def id: String

def indexById[A <: Entity](items: List[A]): Map[String, A] = items.map(a => a.id -> a).toMap
```

Vì sao không viết `def indexById(items: List[Entity]): Map[String, Entity]`? Vì sẽ **mất kiểu cụ thể**: truyền `List[Product]` nhận về `Map[String, Entity]`,
phải ép kiểu ngược lại. Với bound, truyền `List[Product]` nhận `Map[String, Product]`.

**Lower bound `B >: A`:** "B phải là A hoặc cha của A" — dùng chủ yếu trong collection bất biến covariant (mục 3.3).

### 3.3. Variance

Câu hỏi: `OrderPlaced <: Event`. Vậy `Container[OrderPlaced]` có phải là `Container[Event]` không? Tùy cách khai báo:

| Khai báo | Tên | Quan hệ | Ví dụ thực tế |
|---|---|---|---|
| `Source[+A]` | Covariant | `Source[OrderPlaced] <: Source[Event]` | Thứ **sinh ra** A: `List`, `Option`, `Either`, producer, `() => A` |
| `Handler[-A]` | Contravariant | `Handler[Event] <: Handler[OrderPlaced]` | Thứ **tiêu thụ** A: hàm `A => B` (phía tham số), encoder, handler, `Ordering` |
| `Cell[A]` | Invariant | Không có quan hệ | Thứ vừa đọc vừa ghi A: `Array`, collection mutable, `Ref` |

Trực giác:
- Một nguồn chỉ phát `OrderPlaced` thì dùng được ở chỗ cần "một nguồn phát `Event`" — người nhận đọc ra `OrderPlaced`, mà `OrderPlaced` là `Event`. ✅
- Một handler xử lý được **mọi** `Event` thì dùng được ở chỗ cần "handler cho `OrderPlaced`". ✅ Ngược lại không được: handler chỉ biết `OrderPlaced` nhận phải `UserSignedUp` sẽ hỏng.
- Một ô mutable chứa `String` mà coi như ô chứa `Any` thì ai đó có thể ghi `42` vào — phá vỡ an toàn kiểu. Vì vậy mutable phải invariant.

Compiler **kiểm tra** khai báo variance: không cho đặt `A` covariant ở vị trí tham số (mục 10). Đó là lý do `List[+A]` cần lower bound khi thêm phần tử:

```scala
final case class NonEmpty[+A](head: A, tail: List[A]):
  def prepend[B >: A](b: B): NonEmpty[B] = NonEmpty(b, head :: tail)
```

Thêm `UserSignedUp` vào `NonEmpty[OrderPlaced]` cho ra `NonEmpty[Event]` — kiểu tự **mở rộng** đến cha chung.

**Mức độ cần thiết:** người viết ứng dụng ít khi tự khai báo variance; người viết thư viện thì thường xuyên. Nhưng **ai cũng phải đọc** được chữ ký có `+A`, `-A`, `B >: A`.

### 3.4. Union types `A | B` (mới trong Scala 3)

```scala
type ImportError = ParseFailure | RuleViolation

def timeoutSeconds(raw: Int | String): Either[String, Int] = raw match
  case n: Int                       => Right(n)
  case s: String if s.endsWith("s") => s.dropRight(1).toIntOption.toRight(s"sai: $s")
  ...
```

| | Union `A \| B` | Enum / sealed trait |
|---|---|---|
| Cần sửa A, B? | Không — ghép bất kỳ kiểu có sẵn | Các case phải khai báo cùng chỗ |
| Đặt tên cho tổng thể | Bằng type alias | Có tên riêng, có method chung |
| Dùng khi | Kết hợp tạm thời kiểu đã có (lỗi từ hai module, input `Int \| String`) | Mô hình domain chính thức |

Suy luận kiểu (đã kiểm chứng với `scalac 3.9.0`): `val x = if c then 1 else "a"`, `def m(b: Boolean) = if b then 1 else "a"` và `List(1, "a")`
đều được suy ra là `Int | String` (hoặc `List[Int | String]`). Một union "tình cờ" như vậy thường là dấu hiệu nhầm lẫn kiểu.
Với API công khai, luôn **ghi kiểu tường minh** để chữ ký không phụ thuộc vào quy tắc suy luận (vốn có thể khác giữa các phiên bản Scala).

### 3.5. Intersection types `A & B`

```scala
def latestById[A <: HasId & HasUpdatedAt](rows: List[A]): List[A] = ...
```

`A & B` = giá trị có **đồng thời** mọi thành viên của A và B. Ở đây một hàm dedup "giữ bản mới nhất" dùng được cho **mọi** bảng có `id` và `updatedAt`
(khách hàng, sản phẩm...) mà không cần trait cha `HasIdAndUpdatedAt`. Đây là bài tập 4 Chương 6, nay viết một lần cho mọi loại bản ghi.

### 3.6. Type alias

```scala
type Row = Map[String, String]
type Validator = Row => List[String]
```

Chỉ là **tên khác** cho kiểu sẵn có: `Row` và `Map[String, String]` hoán đổi tự do. Giúp chữ ký dễ đọc; **không** giúp phân biệt `UserId` với `OrderId`.

### 3.7. Opaque types

- **Vấn đề:** `def findOrder(userId: String, orderId: String)` — gọi nhầm thứ tự tham số vẫn biên dịch. Bọc bằng `case class UserId(value: String)` thì an toàn
  nhưng mỗi ID là một đối tượng cấp phát trên heap — với hàng triệu bản ghi trong pipeline dữ liệu, đó là áp lực GC thật.
- **Giải pháp Scala 3:** opaque type — **khác kiểu lúc biên dịch, giống hệt kiểu gốc lúc chạy**.

```scala
object Identifiers:
  opaque type UserId = String         // bên TRONG object: UserId chính là String
  object UserId:
    def parse(raw: String): Either[String, UserId] = ...
  extension (id: UserId) def value: String = id

// bên NGOÀI: UserId không phải String, không phải OrderId
```

| | Type alias | Opaque type | Case class wrapper |
|---|---|---|---|
| Phân biệt UserId/OrderId lúc compile | ❌ | ✅ | ✅ |
| Chi phí runtime | Không | **Không** (nhưng boxing như kiểu gốc khi đưa vào generic) | Một đối tượng mỗi giá trị |
| Có method | Của kiểu gốc | Chỉ những gì bạn định nghĩa (extension) | Tự định nghĩa |
| Pattern matching, `toString` riêng | — | Không (in ra như kiểu gốc) | Có |

`Money.Vnd` trong code là `Long` lúc chạy nhưng không thể cộng nhầm với một `Long` tùy ý (ví dụ số lượng) — chỉ có các phép `+`, `*`, `>=` mà ta cho phép.
Thay đổi so với Chương 5: `Money(BigDecimal)` → `Vnd` (Long, đơn vị đồng). Đây là **quyết định domain**: VND không có đơn vị lẻ trong thực tế; với ngoại tệ có xu
hoặc phép tính tỷ giá, vẫn nên dùng `BigDecimal`.

### 3.8. Extension methods

```scala
object StringSyntax:
  extension (s: String)
    def normalizeEmail: String = s.trim.toLowerCase
```

Thêm method cho kiểu không sửa được (String, kiểu thư viện Java, opaque type) mà không kế thừa, không wrapper. Phải `import StringSyntax.*` để dùng —
giữ rõ ràng nguồn gốc của cú pháp. Thay thế cho *implicit class* của Scala 2.

**Lỗi thiết kế thường gặp:** extension method có side effect hoặc logic nghiệp vụ nặng ẩn sau cú pháp "trông như method có sẵn"; tràn lan extension trên `String`
khiến người đọc không biết method từ đâu ra. Giữ extension ngắn, thuần, gom trong object có tên rõ ràng.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Type parameter | Tham số kiểu (`A` trong `Page[A]`) |
| Type erasure | JVM xóa thông tin tham số kiểu lúc chạy |
| Upper / lower bound | Ràng buộc trên `<:` / dưới `>:` |
| Covariance / contravariance / invariance | Hiệp biến / nghịch biến / bất biến |
| Variance position | Vị trí (tham số, kiểu trả về) quyết định variance hợp lệ |
| Union type | Kiểu hợp `A \| B` |
| Intersection type | Kiểu giao `A & B` |
| Type alias | Tên khác cho kiểu |
| Opaque type | Kiểu mới chỉ tồn tại lúc biên dịch |
| Zero-cost abstraction | Trừu tượng không tốn chi phí lúc chạy |
| Extension method | Method bổ sung cho kiểu có sẵn |

## 5. Ví dụ đơn giản

```scala
def firstOr[A](xs: List[A], default: A): A = xs.headOption.getOrElse(default)
firstOr(List(1, 2), 0)          // 1
firstOr(List.empty[String], "?") // "?"

opaque type Celsius = Double
object Celsius:
  def apply(d: Double): Celsius = d
extension (c: Celsius) def toFahrenheit: Double = c * 9 / 5 + 32
```

## 6. Ví dụ thực tế

| Nhu cầu | Kỹ thuật | Ở đâu trong code |
|---|---|---|
| Kết quả phân trang dùng chung cho mọi API (BE) | Generic `Page[A]` + `map` | `Generics.scala` |
| Hàm tiện ích cho mọi entity có id | Upper bound | `Repo.duplicateIds` |
| Hệ thống event: nguồn và handler | Variance | `Source[+A]`, `Handler[-A]` |
| Gộp lỗi từ hai module khác nhau | Union | `ImportError` |
| Cấu hình timeout nhận `30` hoặc `"30s"` | Union | `timeoutSeconds` |
| Dedup CDC cho mọi bảng (DE) | Intersection | `latestById` |
| Không nhầm UserId/OrderId, tiền không trộn với số lượng | Opaque type | `Identifiers`, `Money.Vnd` |

## 7. Code đầy đủ

- [`Identifiers.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08a/Identifiers.scala) — opaque `UserId`, `OrderId`, `Vnd`, extension methods.
- [`Generics.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08a/Generics.scala) — `Page[A]`, `Repo` (upper bound), `Source`/`Handler` (variance), `NonEmpty` (lower bound).
- [`UnionIntersection.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08a/UnionIntersection.scala) — union, intersection, type alias.
- [`Ch08aDemo.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08a/Ch08aDemo.scala) — `@main ch08aDemo`.

Trích đoạn opaque type tiền tệ:

```scala
object Money:
  opaque type Vnd = Long

  object Vnd:
    val Zero: Vnd = 0L
    def apply(dong: Long): Either[String, Vnd] =
      if dong >= 0 then Right(dong) else Left(s"Số tiền âm: $dong")
    def unsafe(dong: Long): Vnd = dong

  extension (a: Vnd)
    def +(b: Vnd): Vnd = a + b
    def *(qty: Int): Vnd = a * qty
    def dong: Long = a
    def >=(b: Vnd): Boolean = a >= b
    def format: String = "%,d VND".formatLocal(java.util.Locale.US, a)
```

Variance:

```scala
trait Source[+A]:
  def poll(): List[A]

trait Handler[-A]:
  def handle(a: A): String

object Variance:
  val orderSource: Source[OrderPlaced] = () => List(OrderPlaced("ORD-1", 100L), OrderPlaced("ORD-2", 200L))
  val anySource: Source[Event] = orderSource            // covariance
  val auditHandler: Handler[Event] = e => s"audit@${e.occurredAt}"
  val orderHandler: Handler[OrderPlaced] = auditHandler // contravariance
  def dispatch[A](source: Source[A], handler: Handler[A]): List[String] = source.poll().map(handler.handle)
```

## 8. Giải thích code

- **Bên trong `object Money`**, `Vnd` **là** `Long`: `a + b` trong extension gọi phép cộng của `Long` (thành viên của kiểu được ưu tiên hơn extension), không đệ quy.
  **Bên ngoài**, `Vnd` không có thành viên `+` nên `price * 3 + ...` dùng extension.
- Hai smart constructor: `Vnd.apply` trả `Either` cho dữ liệu từ bên ngoài; `Vnd.unsafe` cho dữ liệu đã được đảm bảo (từ DB có ràng buộc `CHECK`).
  Đặt tên `unsafe` để người review chú ý mỗi lần nó được dùng.
- `UserId` và `OrderId` cùng là `String` bên trong `Identifiers`, nên extension của chúng phải đặt **tên khác nhau** (`value`, `orderValue`) — nếu trùng tên sẽ xung đột vì
  bên trong object compiler không phân biệt được hai kiểu. Thực tế nên đặt mỗi opaque type trong object riêng.
- `Source[OrderPlaced]` được tạo bằng lambda `() => List(...)`: trait có đúng một method abstract là **SAM type**, lambda tự chuyển thành nó.
- `NonEmpty(...).prepend(UserSignedUp(...))` — compiler chọn `B = Event` (cha chung nhỏ nhất), kết quả `NonEmpty[Event]`.
- `timeoutSeconds("xs")` trả `Left("sai: xs")` chứ không phải "không hiểu đơn vị": `"xs"` kết thúc bằng `s` nên khớp nhánh giây trước. Thứ tự `case` là logic.
- `latestById` nhận `A <: HasId & HasUpdatedAt` và **trả về `List[A]`** — giữ kiểu cụ thể (`List[CustomerRow]`), nên `.map(_.name)` vẫn dùng được.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch08a.ch08aDemo
Right(U-42)
Left(UserId không hợp lệ: '42')
value = U-7
Tổng: 390,000 VND, Long bên dưới: 390000
Left(Số tiền âm: -5)
an.nguyen@example.com
Trang 2/3: List(P4, P5, P6), hasNext=true
List(Sản phẩm 4, Sản phẩm 5, Sản phẩm 6)
Set(C1)
List(audit@100, audit@200)
List(audit@100, audit@200)
List(UserSignedUp, OrderPlaced)
List(parse price: 'abc', vi phạm: qty > 0)
List(Right(45), Right(30), Right(300), Left(không hiểu đơn vị: 1h), Left(sai: xs))
List((C1,An Nguyễn), (C2,Bình))
List(A)
List()
List(email thiếu hoặc sai, name rỗng)
```

Chú ý dòng đầu: `Right(U-42)` — opaque type **in ra như String** vì lúc chạy nó chính là String.

## 10. Lỗi thường gặp

Tất cả là output thật của `scalac 3.9.0`.

**Lỗi 1 — dùng opaque type như kiểu gốc:**
```
3 |  val s: String = UserId.unsafe("U-1")
  |                  Found:    scalaeng.ch08a.Identifiers.UserId
  |                  Required: String
```
Đúng như mong muốn. Dùng `.value` khi thật sự cần String (ghi log, gửi DB).

**Lỗi 2 — truyền nhầm ID (lỗi mà opaque type sinh ra để bắt):**
```
4 |  val r = findOrder(UserId.unsafe("U-1"))
  |                    Found:    scalaeng.ch08a.Identifiers.UserId
  |                    Required: scalaeng.ch08a.Identifiers.OrderId
```

**Lỗi 3 — tham số covariant ở vị trí tham số:**
```scala
trait Box[+A]:
  def set(a: A): Unit
```
```
2 |  def set(a: A): Unit
  |covariant type A occurs in contravariant position in type A of parameter a
```
Cách sửa: bỏ `+` (invariant), hoặc dùng lower bound `def set[B >: A](b: B): Box[B]` nếu trả về đối tượng mới.

**Lỗi 4 — kiểu invariant không nhận kiểu con:**
```
3 |  val c: Cell[Any] = Cell[String]("x")
  |                     Found:    Cell[String]
  |                     Required: Cell[Any]
```
`Array` cũng invariant vì lý do tương tự: `val a: Array[Any] = Array[String]()` không biên dịch.

**Lỗi 5 — vi phạm upper bound:**
```
3 |  val m = Repo.indexById(List(1, 2, 3))
  |                              Found:    (1 : Int)
  |                              Required: scalaeng.ch08a.Entity
```

**Lỗi thiết kế:**
- Generic hóa sớm: viết `Repository[F[_], K, V, E <: Entity[K]]` khi mới có một bảng. Bắt đầu cụ thể, generic hóa khi có trường hợp thứ ba.
- Dùng type alias `type UserId = String` và tưởng đã an toàn kiểu.
- Opaque type không có smart constructor → ai cũng tạo được giá trị sai.

## 11. Cách debug

- Hỏi compiler kiểu suy luận: rê chuột trong IDE, hoặc tạm ghi kiểu sai (`val x: Int = expr`) để lỗi in ra kiểu thật.
- Lỗi variance: đọc phần "occurs in ... position in type ... of parameter ..." — nó chỉ chính xác vị trí vi phạm.
- Khi kiểu suy luận "quá rộng" (`Event` thay vì `OrderPlaced`, union bị widen): ghi tham số kiểu tường minh tại lời gọi: `prepend[OrderPlaced](...)`.
- In giá trị opaque type sẽ ra giá trị gốc — đừng ngạc nhiên khi log hiện `U-7` chứ không phải `UserId(U-7)`.

## 12. Bài tập

1. **(Cơ bản)** Viết `final case class Result[A](data: A, warnings: List[String])` với `map[B](f: A => B): Result[B]` và `addWarning(w: String): Result[A]`.
2. **(Cơ bản)** Viết opaque type `Email` với `Email.parse(raw): Either[String, Email]` (trim, lowercase, có đúng một `@`, phần sau `@` có dấu `.`) và extension `domain: String`.
3. **(Trung bình)** Viết opaque type `Percent` (0–100, kiểu gốc `Int`) và extension `applyTo(amount: Long): Long` (làm tròn xuống). Viết test chứng minh `Percent.parse(101)` thất bại.
4. **(Trung bình)** Viết `def maxByField[A <: HasUpdatedAt](rows: List[A]): Option[A]` trả về bản ghi mới nhất, **giữ kiểu cụ thể** của A.
5. **(Nâng cao)** Thiết kế `trait Sink[-A] { def write(a: A): String }` và `trait Pipeline[+A] { def run(): List[A] }`.
   Viết `def connect[A](p: Pipeline[A], s: Sink[A]): List[String]`. Chứng minh bằng test: `Pipeline[OrderPlaced]` nối được với `Sink[Event]`.
   Giải thích bằng lời vì sao `Sink` phải contravariant.

## 13. Đáp án và lời giải

- Code: [`Ch08aSolutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch08aSolutions.scala) · Test: [`Ch08aSolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch08aSolutionsSuite.scala)
- Giải thích: [`solutions/ch08-solutions.md`](../solutions/ch08-solutions.md)

## 14. Câu hỏi kiểm tra

1. Vì sao `def f(xs: List[Entity]): Map[String, Entity]` kém hơn `def f[A <: Entity](xs: List[A]): Map[String, A]`?
2. `List` là covariant. Vì sao `prepend` cần `B >: A`?
3. Vì sao hàm `A => B` contravariant ở `A` và covariant ở `B`?
4. Vì sao collection mutable phải invariant?
5. Type alias và opaque type khác nhau thế nào về an toàn kiểu và chi phí runtime?
6. Khi nào chọn union type, khi nào chọn enum?
7. Extension method khác kế thừa/wrapper thế nào? Vì sao cần import?

## 15. Mini project — Kiểu an toàn cho domain đơn hàng

Viết lại domain Chương 5 dùng: opaque `OrderId`, `CustomerId`, `Sku`, `Vnd`, `Quantity` (Int > 0), mỗi loại có smart constructor trả `Either`.
`OrderLine.total` chỉ biên dịch được khi nhân `Vnd` với `Quantity`. Viết parser dòng CSV (Chương 7) trả `Either[ParseError, OrderLine]` dùng các smart constructor.
Viết thử 3 lời gọi sai (đổi chỗ `CustomerId`/`OrderId`, cộng `Vnd` với `Quantity`, truyền `String` thô) và ghi lại thông báo lỗi compiler.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Generics: https://docs.scala-lang.org/scala3/book/types-generics.html
- Variance: https://docs.scala-lang.org/scala3/book/types-variance.html
- Union Types: https://docs.scala-lang.org/scala3/book/types-union.html
- Intersection Types: https://docs.scala-lang.org/scala3/book/types-intersection.html
- Opaque Types: https://docs.scala-lang.org/scala3/book/types-opaque-types.html
- Extension Methods: https://docs.scala-lang.org/scala3/book/ca-extension-methods.html
- Scala 3 Reference — Opaque Type Aliases: https://docs.scala-lang.org/scala3/reference/other-new-features/opaques.html

## 17. Checklist tự đánh giá

- [ ] Tôi viết được class/method generic và giữ được kiểu cụ thể nhờ bound.
- [ ] Tôi giải thích được `+A`, `-A`, invariant bằng ví dụ producer/consumer/mutable.
- [ ] Tôi đọc được lỗi variance và biết cách sửa.
- [ ] Tôi phân biệt được union/enum, type alias/opaque type.
- [ ] Tôi tạo được opaque type với smart constructor và extension methods.
- [ ] Tôi biết opaque type in ra và lưu trữ như kiểu gốc.
