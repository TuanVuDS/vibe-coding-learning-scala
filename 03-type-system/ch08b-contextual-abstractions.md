# Chương 8B — Contextual Abstractions: given, using và Type Classes

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4 · Không dùng thư viện ngoài
> **Code:** [`../01-scala3-core/code/src/main/scala/scalaeng/ch08b/`](../01-scala3-core/code/src/main/scala/scalaeng/ch08b/) ·
> Test: [`TypeClassSuite.scala`](../01-scala3-core/code/src/test/scala/scalaeng/ch08b/TypeClassSuite.scala)
> **Trạng thái kiểm chứng:** Code **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo). 8/8 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Thông báo lỗi trong mục 10 là output thật.

---

## 1. Mục tiêu học tập

- Hiểu **type class** là gì và vì sao nó là mẫu thiết kế trung tâm của hệ sinh thái Scala (circe, Doobie, Cats, Tapir đều dựa vào nó).
- Định nghĩa instance bằng `given`, yêu cầu instance bằng `using` hoặc context bound `[A: TC]`.
- Viết **conditional given** (có `TC[A]` thì có `TC[Option[A]]`).
- Biết compiler **tìm** given ở đâu: phạm vi hiện tại, `import x.given`, companion object.
- Dùng `using` để truyền **ngữ cảnh** (request id, clock) qua nhiều tầng.
- Dùng type class có sẵn trong standard library: `Ordering`, `Numeric`, `CanEqual`.
- Hiểu **Monoid** và vì sao tính kết hợp (associativity) là điều kiện của aggregation song song/phân tán.

## 2. Kiến thức tiên quyết

- Chương 8A (generics, extension method). Chương 7 đã dùng `using num: Numeric[A]` mà chưa giải thích — chương này giải thích.

## 3. Lý thuyết

### 3.1. Vấn đề: thêm hành vi cho kiểu không sở hữu

Cần xuất báo cáo CSV cho `OrderReport`, `Int`, `LocalDate`, `Option[...]`. Các cách tiếp cận:

| Cách | Vấn đề |
|---|---|
| Cho mọi class kế thừa `trait CsvEncodable` | Không sửa được `Int`, `LocalDate` (thư viện Java) |
| `def encode(a: Any): String = a match { ... }` | Mất an toàn kiểu, quên kiểu nào thì lỗi lúc chạy |
| Wrapper/adaptor từng kiểu | Dài dòng, phải tự gọi wrapper mọi nơi |
| **Type class** | Hành vi tách khỏi dữ liệu, compiler tự tìm và **bắt lỗi thiếu instance lúc biên dịch** |

### 3.2. Ba thành phần của type class

```scala
// 1. Định nghĩa: một trait generic mô tả hành vi
trait CsvEncoder[A]:
  def header: List[String]
  def encode(a: A): List[String]

// 2. Instance: cài đặt cho từng kiểu cụ thể, khai báo bằng `given`
given CsvEncoder[Int] = CsvEncoder.column("value")(_.toString)

// 3. Sử dụng: yêu cầu instance bằng `using` (hoặc context bound)
def write[A](rows: List[A])(using enc: CsvEncoder[A]): String = ...
def write[A: CsvEncoder](rows: List[A]): String = ...   // cách viết gọn tương đương
```

Gọi `CsvWriter.write(reports)` — compiler tự tìm `given CsvEncoder[OrderReport]` và truyền vào. Không tìm thấy → **lỗi biên dịch** (mục 10), không phải lỗi lúc chạy.

`summon[CsvEncoder[A]]` lấy instance đang có trong ngữ cảnh. Quy ước phổ biến: định nghĩa `def apply[A](using e: CsvEncoder[A]) = e` trong companion để viết `CsvEncoder[A]`.

### 3.3. Compiler tìm `given` ở đâu?

Theo thứ tự (đơn giản hóa):

1. **Phạm vi hiện tại** — given khai báo trong cùng scope hoặc lớp bao quanh.
2. **Đã import tường minh** — `import CsvInstances.given` (hoặc `import CsvInstances.{given CsvEncoder[?]}`).
   **`import CsvInstances.*` KHÔNG import given** — khác Scala 2, thiết kế có chủ đích để thấy rõ nguồn gốc của given.
3. **Implicit scope của kiểu** — companion object của kiểu liên quan: `CsvEncoder` và `OrderReport`. Đây là lý do `given CsvEncoder[OrderReport]` đặt trong
   `object OrderReport` được tìm thấy **mà không cần import**.

**Quy tắc đặt instance:**
- Kiểu của bạn → instance trong **companion object của kiểu đó**.
- Kiểu không sở hữu (`Int`, `LocalDate`) và type class của bạn → trong **companion của type class** (được tìm tự động) hoặc object riêng cần import (khi muốn người dùng chọn).
- Không sở hữu cả hai → object riêng, import tường minh ("orphan instance" — hạn chế dùng).

### 3.4. Conditional given

```scala
given [A](using inner: CsvEncoder[A]): CsvEncoder[Option[A]] with
  def header: List[String] = inner.header
  def encode(a: Option[A]): List[String] = a match
    case Some(v) => inner.encode(v)
    case None    => inner.header.map(_ => "")
```

"Nếu có encoder cho A thì có encoder cho `Option[A]`." Compiler **ghép** instance: cần `CsvEncoder[Option[LocalDate]]` → dùng given này với `inner = CsvEncoder[LocalDate]`.
Cũng là cách `Monoid[Map[K, V]]` được suy ra từ `Monoid[V]`. Thư viện như circe xây dựng encoder cho `List[Option[Map[String, User]]]` hoàn toàn bằng cơ chế này.

### 3.5. Extension method + type class = cú pháp tự nhiên

```scala
extension [A](a: A)(using enc: CsvEncoder[A])
  def toCsvRow: String = enc.encode(a).map(CsvEncoder.escape).mkString(",")

report.toCsvRow   // chỉ biên dịch được nếu có CsvEncoder[OrderReport]
```

Đây là cách các thư viện cung cấp `user.asJson` (circe) hay `x.show` (Cats).

### 3.6. `using` cho ngữ cảnh (không phải type class)

```scala
def entry(action: String)(using ctx: RequestContext, clock: Clock): String = ...

def placeOrder(orderId: String)(using RequestContext, Clock): List[String] =
  List(AuditLog.entry(s"validate $orderId"), AuditLog.entry(s"place $orderId"))
```

`placeOrder` không cần nhắc lại `ctx`, `clock` khi gọi `entry` — chúng được truyền tiếp. Hữu ích cho thông tin xuyên suốt request (trace id, user), cấu hình,
`ExecutionContext` (Chương 16).

**Cảnh báo thiết kế:** đừng dùng `using` cho **phụ thuộc nghiệp vụ** chính (repository, service). Truyền chúng qua constructor (Chương 5) — rõ ràng hơn.
`using` hợp với thứ "luôn có mặt, một giá trị trong một ngữ cảnh".

### 3.7. Type class có sẵn trong standard library

| Type class | Dùng ở đâu | Ví dụ |
|---|---|---|
| `Ordering[A]` | `sorted`, `max`, `min`, `TreeMap` | `products.sorted(using byPriceAsc)` |
| `Numeric[A]` | `sum`, `product`, hàm số học generic | `positive[A](...)(using Numeric[A])` ở Chương 7 |
| `CanEqual[A, B]` | So sánh `==` khi bật strict equality | `case class Sku(...) derives CanEqual` |
| `ClassTag[A]` | Tạo `Array[A]` generic (do type erasure) | `Array.ofDim[A](n)` cần `ClassTag[A]` |

`derives CanEqual` là ví dụ đơn giản của **type class derivation**: compiler sinh instance từ cấu trúc của kiểu. Derivation cho type class tự viết (như `derives CsvEncoder`)
cần `Mirror` và `inline` — thuộc Chương 8C. Trong thực tế bạn sẽ dùng derivation do thư viện cung cấp: `derives Codec.AsObject` (circe), `derives Schema` (Tapir).

### 3.8. Monoid — type class của aggregation

```scala
trait Monoid[A]:
  def empty: A
  def combine(x: A, y: A): A
```

Luật (laws): `combine(empty, x) == x`, `combine(x, empty) == x`, và **kết hợp**: `combine(combine(x, y), z) == combine(x, combine(y, z))`.

**Vì sao quan trọng cho Data Engineering:** tính kết hợp cho phép **gộp từng phần rồi gộp các kết quả** mà kết quả không đổi — chính là cách
Spark tính aggregate: mỗi partition gộp cục bộ, rồi gộp kết quả qua shuffle (Chương 22). Output thật:

```
Gộp từng lô rồi gộp tiếp: Stats(4,1103,900), avg=275.75
Gộp một lần:            Stats(4,1103,900)
```

**Trung bình không phải monoid** (trung bình của các trung bình ≠ trung bình tổng). Vì vậy `Stats` lưu `count` và `sum` (là monoid), tính `avg` ở cuối.
Đây là lỗi kinh điển trong báo cáo dữ liệu.

Thư viện Cats cung cấp `Monoid`, `Semigroup`, `Validated` (tích lũy lỗi — xem Chương 7), `Show`... cùng các luật đã được kiểm thử.
Cats là **thư viện ngoài**, không phải một phần của Scala; ta sẽ dùng nó từ Chương 16B.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Type class | Mẫu thiết kế: hành vi generic + instance cho từng kiểu |
| Instance | Cài đặt type class cho một kiểu cụ thể |
| Given / using | Khai báo instance / yêu cầu instance (tham số ngữ cảnh) |
| Context bound | `[A: TC]` — viết gọn của `(using TC[A])` |
| Summon | Lấy instance từ ngữ cảnh |
| Implicit scope | Nơi compiler tự tìm given (companion object của các kiểu liên quan) |
| Conditional given | Given phụ thuộc given khác |
| Orphan instance | Instance đặt ngoài companion của cả kiểu lẫn type class |
| Derivation | Compiler tự sinh instance từ cấu trúc kiểu |
| Monoid / Semigroup | Phép gộp kết hợp có / không có phần tử trung hòa |
| Associativity | Tính kết hợp |
| Strict equality | Chế độ chỉ cho `==` giữa các kiểu được phép |

## 5. Ví dụ đơn giản

```scala
trait Show[A]:
  def show(a: A): String

given Show[Int] with
  def show(a: Int) = s"Int($a)"

given [A](using s: Show[A]): Show[List[A]] with
  def show(xs: List[A]) = xs.map(s.show).mkString("[", ", ", "]")

def display[A: Show](a: A): String = summon[Show[A]].show(a)

display(42)            // "Int(42)"
display(List(1, 2))    // "[Int(1), Int(2)]"
// display("x")        // lỗi biên dịch: No given instance of type Show[String]
```

## 6. Ví dụ thực tế

- **Xuất báo cáo CSV (DE/BE):** `CsvEncoder` với escape đúng RFC 4180 (tên công ty có dấu phẩy, dấu nháy kép), `BigDecimal` không ra dạng `1E+3`, `None` thành ô rỗng.
- **Aggregation có thể song song hóa (DE):** `Monoid[Stats]`, `Monoid[Map[K, V]]` — đếm request theo path từ nhiều file log rồi gộp.
- **Audit log mang ngữ cảnh request (BE):** `RequestContext` và `Clock` truyền qua `using`; test dùng `Clock.fixed`.
- **Sắp xếp sản phẩm theo nhiều tiêu chí (BE):** `Ordering.by(...).orElseBy(...)`.

## 7. Code đầy đủ

- [`CsvEncoder.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08b/CsvEncoder.scala) — type class, instance, conditional given, syntax, writer, instance trong companion.
- [`Monoids.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08b/Monoids.scala) — `Monoid`, `combineAll`, `Stats`.
- [`Context.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08b/Context.scala) — `RequestContext`, `Clock`, `Ordering`, `CanEqual`.
- [`Ch08bDemo.scala`](../01-scala3-core/code/src/main/scala/scalaeng/ch08b/Ch08bDemo.scala) — `@main ch08bDemo`.

Trích đoạn instance trong companion object:

```scala
final case class OrderReport(orderId: String, customer: String, total: BigDecimal, paidOn: Option[LocalDate])

object OrderReport:
  import CsvInstances.given

  given CsvEncoder[OrderReport] with
    def header: List[String] = List("order_id", "customer", "total", "paid_on")
    def encode(r: OrderReport): List[String] =
      List(r.orderId, r.customer) ++
        summon[CsvEncoder[BigDecimal]].encode(r.total) ++
        summon[CsvEncoder[Option[LocalDate]]].encode(r.paidOn)
```

Monoid cho Map:

```scala
given [K, V](using mv: Monoid[V]): Monoid[Map[K, V]] with
  def empty = Map.empty
  def combine(x: Map[K, V], y: Map[K, V]) =
    y.foldLeft(x) { case (acc, (k, v)) => acc.updated(k, acc.get(k).fold(v)(mv.combine(_, v))) }

extension [A](xs: List[A])(using m: Monoid[A])
  def combineAll: A = xs.foldLeft(m.empty)(m.combine)
```

## 8. Giải thích code

- `given CsvEncoder[Int] = CsvEncoder.column(...)` — given **có tên tự sinh** (`given_CsvEncoder_Int`). Muốn đặt tên: `given intEncoder: CsvEncoder[Int] = ...`.
- `given ... with` — cú pháp tạo instance mới với thân là các method; tương đương `new CsvEncoder[...] { ... }`.
- `_.bigDecimal.toPlainString` — `BigDecimal("1E+3").toString` ra `1E+3`, file CSV sẽ bị hệ thống khác đọc sai. Test chứng minh ra `1000`.
- `escape` theo RFC 4180: trường chứa `,`, `"` hoặc xuống dòng được bọc bằng `"..."` và `"` bên trong nhân đôi. Output thật: `"Công ty ""ABC"", chi nhánh HN"`.
- Trong `object OrderReport`, `import CsvInstances.given` cần để `summon[CsvEncoder[BigDecimal]]` hoạt động; nhưng **người dùng** `OrderReport` không cần import gì.
- `Stats.empty` dùng `max = Long.MinValue` — phần tử trung hòa cho `max`. Danh sách rỗng `combineAll` ra `Stats(0, 0, Long.MinValue)`: đúng về mặt toán, nhưng khi hiển thị
  phải xử lý `count == 0`. Một lựa chọn khác là `Option[Long]` cho `max`.
- `Monoid` có `given Monoid[Int]` trong companion `object Monoid` → `List.empty[Int].combineAll` tìm được instance mà không cần import (output `0`).
- `given RequestContext = ...` trong `@main` — given cục bộ, chỉ có hiệu lực trong hàm đó.
- `Ordering.by[Product, Double](p => -p.rating).orElseBy(_.price)` — rating giảm dần, cùng rating thì giá tăng dần: `List(B, C, A)` với B (100) rẻ hơn C (200).

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch08b.ch08bDemo
order_id,customer,total,paid_on
ORD-1,Nguyễn Văn An,495000,2026-10-01
ORD-2,"Công ty ""ABC"", chi nhánh HN",1250000.50,
ORD-1,Nguyễn Văn An,495000,2026-10-01
value
1

3
Gộp từng lô rồi gộp tiếp: Stats(4,1103,900), avg=275.75
Gộp một lần:            Stats(4,1103,900)
Map(/a -> 5, /b -> 1, /c -> 7)
0
1700000000000 [req-123] user=U-42 validate ORD-9
1700000000000 [req-123] user=U-42 place ORD-9
List(B, C, A)
List(B, C, A)
B
```

## 10. Lỗi thường gặp

Output thật của `scalac 3.9.0`.

**Lỗi 1 — thiếu instance:**
```
3 |  val s = CsvWriter.write(List(Map(1 -> 2)))
  |No given instance of type scalaeng.ch08b.CsvEncoder[Map[K, Int]] was found for a context parameter of method write in object CsvWriter
```
Lỗi lúc biên dịch — điểm mạnh của type class. Sửa: viết instance, hoặc chuyển dữ liệu sang kiểu đã có instance.

**Lỗi 2 — `import X.*` không import given:**
```
4 |  val s = CsvWriter.write(List(1, 2))
  |No given instance of type scalaeng.ch08b.CsvEncoder[Int] was found for a context parameter of method write in object CsvWriter
  |Note: given instance given_CsvEncoder_Int in object CsvInstances was not considered because it was not imported with `import given`.
```
Compiler gợi ý chính xác cách sửa: `import CsvInstances.given`. Đây là lỗi gặp **rất thường xuyên** khi chuyển từ Scala 2 hoặc làm theo ví dụ cũ.

**Lỗi 3 — hai instance cùng kiểu trong một phạm vi:**
```
4 |  val x = List(3, 1, 2).sorted
  |Ambiguous given instances: both given instance asc in object G and given instance desc in object G match type Ordering[B] of parameter ord of method sorted ...
```
Sửa: chỉ để một given, hoặc truyền tường minh `sorted(using desc)`. Nhiều cách sắp xếp → dùng `val` thường + `using` tường minh như `ProductOrderings`.

**Lỗi 4 — so sánh hai kiểu không liên quan khi bật strict equality:**
```scala
import scala.language.strictEquality
Sku("A") == "A"
```
```
4 |  val same = Sku("A") == "A"
  |Values of types scalaeng.ch08b.Sku and String cannot be compared with == or !=
```
Không bật strict equality, biểu thức trên biên dịch và luôn ra `false` — một bug âm thầm.

**Lỗi thiết kế:**
- Instance "bất ngờ": given có hành vi khác kỳ vọng (ví dụ `Ordering[Money]` giảm dần) đặt ở nơi được tìm tự động.
- Dùng `using` thay cho tham số bình thường chỉ để "đỡ phải truyền" → code khó đọc, khó biết giá trị đến từ đâu.
- Monoid không thỏa luật kết hợp (trung bình, "giá trị cuối cùng" khi thứ tự không xác định) → kết quả aggregate song song sai ngẫu nhiên.

## 11. Cách debug

- Đọc kỹ dòng `Note:` trong lỗi "No given instance" — compiler thường chỉ ra given ở gần mà bị bỏ qua và lý do.
- Kiểm tra instance có tồn tại: viết tạm `summon[CsvEncoder[MyType]]` ở dòng riêng — lỗi sẽ ngắn và rõ hơn khi nằm sâu trong lời gọi lớn.
- Với conditional given lồng nhau, tìm kiểu **trong cùng** bị thiếu: thiếu `CsvEncoder[Option[Foo]]` thực chất là thiếu `CsvEncoder[Foo]`.
- Test luật Monoid bằng property-based testing (Chương 10): sinh ngẫu nhiên x, y, z và kiểm tra tính kết hợp.

## 12. Bài tập

1. **(Cơ bản)** Định nghĩa type class `Show[A]` với instance cho `Int`, `String`, `Boolean` và conditional instance cho `List[A]`. Viết `def display[A: Show](a: A): String`.
2. **(Cơ bản)** Viết `given Ordering[Version]` cho `final case class Version(major: Int, minor: Int, patch: Int)` đặt trong companion; `List(v2, v1).sorted` phải đúng mà không cần import.
3. **(Trung bình)** Viết `Monoid[Option[A]]` cho mọi `A` có `Monoid[A]` (`None` là phần tử trung hòa) và `Monoid[(A, B)]` cho tuple. Test: `List(Some(1), None, Some(4)).combineAll == Some(5)`.
4. **(Trung bình)** Viết type class `Validator[A]` với `validate(a: A): List[String]` và instance cho `final case class Signup(name: String, age: Int)`.
   Viết `def validateAll[A: Validator](xs: List[A]): Map[Int, List[String]]` — chỉ số phần tử → lỗi, bỏ qua phần tử hợp lệ.
5. **(Nâng cao — DE)** Dùng `Monoid` để viết `def aggregateByKey[K, V: Monoid](batches: List[List[(K, V)]]): Map[K, V]` — mỗi lô gộp cục bộ thành `Map` trước, rồi gộp các `Map`.
   Viết test chứng minh kết quả bằng cách gộp toàn bộ một lần. Giải thích liên hệ với `reduceByKey` của Spark.

## 13. Đáp án và lời giải

- Code: [`Ch08bSolutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch08bSolutions.scala) · Test: [`Ch08bSolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch08bSolutionsSuite.scala)
- Giải thích: [`solutions/ch08-solutions.md`](../solutions/ch08-solutions.md)

## 14. Câu hỏi kiểm tra

1. Ba thành phần của một type class là gì?
2. Vì sao `import X.*` không đủ để dùng given trong `X`? Lợi ích của thiết kế này?
3. Instance cho kiểu của bạn nên đặt ở đâu? Vì sao?
4. `[A: Ordering]` tương đương chữ ký nào?
5. Vì sao trung bình không phải monoid? Làm sao tính trung bình phân tán đúng?
6. Khi nào dùng `using` cho ngữ cảnh, khi nào truyền qua constructor?
7. Type class khác interface + kế thừa ở điểm nào?

## 15. Mini project — Thư viện báo cáo nhỏ

Mở rộng `CsvEncoder` thành một module báo cáo:
- Instance cho `Vnd` (opaque type, Chương 8A) định dạng không dấu phân cách để máy đọc được.
- `CsvEncoder` cho tuple hai phần tử ghép header của hai encoder.
- `CsvWriter.writeTo(path, rows)` ở shell (ghi file), core vẫn là `write` thuần.
- Báo cáo doanh thu theo ngày từ `OrderPipeline` (Chương 7) xuất ra CSV; báo cáo access log theo path (Chương 6) dùng `Monoid[Map[String, Stats]]` gộp nhiều file.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Contextual Abstractions: https://docs.scala-lang.org/scala3/book/ca-contextual-abstractions-intro.html
- Given Instances and Using Clauses: https://docs.scala-lang.org/scala3/book/ca-context-parameters.html
- Type Classes: https://docs.scala-lang.org/scala3/book/ca-type-classes.html
- Multiversal Equality: https://docs.scala-lang.org/scala3/book/ca-multiversal-equality.html
- Scala 3 Reference — Importing Givens: https://docs.scala-lang.org/scala3/reference/contextual/given-imports.html
- Scala 3 Reference — Type Class Derivation: https://docs.scala-lang.org/scala3/reference/contextual/derivation.html
- Cats — Monoid (thư viện ngoài, để đọc thêm): https://typelevel.org/cats/typeclasses/monoid.html

## 17. Checklist tự đánh giá

- [ ] Tôi định nghĩa được type class, instance và hàm dùng nó.
- [ ] Tôi biết ba nơi compiler tìm given và nhớ `import x.given`.
- [ ] Tôi viết được conditional given.
- [ ] Tôi đặt instance đúng chỗ (companion của kiểu hoặc của type class).
- [ ] Tôi dùng `Ordering` tùy biến và hiểu `CanEqual`.
- [ ] Tôi giải thích được vì sao associativity quan trọng cho aggregation phân tán.
- [ ] Tôi đọc được và sửa được lỗi "No given instance" và "Ambiguous given instances".
