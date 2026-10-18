# Chương 20 — Functional Data Transformation

> **Phiên bản:** Scala 3.9.0 · JDK 21 · cats-core 2.13.0 · fs2-core 3.13.0 · munit 1.2.4 · munit-scalacheck 1.2.0 (ScalaCheck 1.19.0)
> **Code:** [`transform-lab/`](transform-lab/) — validation tích lũy (`ValidatedNec`), data quality rule + quality gate, tổng hợp tổng quát bằng `Monoid`,
> cùng một phép tổng hợp chạy theo 3 mô hình: trong bộ nhớ, theo luồng, chia partition
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **16/16 test pass** (`scala-cli test .` và `sbt test`; 5 trong số đó là
> property test ScalaCheck), lời giải 9/9. Đã qua review chéo (Codex): 6 MAJOR + 4 MINOR đã sửa, mỗi lỗi có test hồi quy (mục 10). Kết quả demo khớp **tính tay**; số đo 1 triệu dòng và output ScalaCheck khi thất bại là output thật.

---

## Vị trí trong lộ trình

Chương 19 đọc dữ liệu bẩn và đưa về dạng sạch. Chương này hỏi: **khi đã có dữ liệu sạch, viết biến đổi thế nào để đúng, tái sử dụng được, và chạy được ở
quy mô lớn hơn?** Câu trả lời của functional programming: biến đổi là **hàm thuần**, lỗi là **giá trị**, quy tắc chất lượng là **dữ liệu**, và phép tổng hợp
có **luật** (Monoid). Luật đó chính là điều kiện để Spark (Chương 22–26) và hệ thống streaming (Chương 27–30) chia việc ra nhiều máy mà vẫn ra đúng kết quả.

## 1. Mục tiêu học tập

1. Dùng `map`, `flatMap`, `filter`, `fold`, `groupBy`/`groupMapReduce` để biến đổi dữ liệu **không thay đổi** (immutable).
2. Phân biệt `Either` (dừng ở lỗi đầu) và `Validated` (gom mọi lỗi); dùng `ValidatedNec` + `mapN` cho validation tích lũy.
3. Mô hình hóa **data quality rule** như dữ liệu, gồm quy tắc trên dòng, trên cả tập (duy nhất, tham chiếu), trên tổng hợp; dựng **quality gate**.
4. Viết tổng hợp **tổng quát** bằng type class `Monoid`; giải thích vì sao tính **kết hợp** (associativity) quyết định việc tính được theo phần.
5. Phân biệt ba mô hình xử lý: **trong bộ nhớ**, **theo luồng**, **phân tán** — cùng logic, khác chỗ dữ liệu nằm và chi phí.
6. Kiểm thử biến đổi: golden tính tay, **property test** với ScalaCheck (luật Monoid, tương đương giữa các mô hình), và mutation test.

## 2. Kiến thức tiên quyết

- Chương 6 (collections), Chương 7 (`Either`, ADT), Chương 8B (type class, `given`, Monoid tự viết ở `ch08b/Monoids.scala`).
- Chương 19 (dữ liệu thô → sạch, golden, test thuộc tính tự viết).
- Chương 10 (property-based testing với generator tự viết) — chương này dùng thư viện ScalaCheck.

## 3. Lý thuyết

### 3.1. Biến đổi là hàm thuần, dữ liệu không đổi

Một pipeline dữ liệu là chuỗi bước `A => B`. Mỗi bước **thuần**: cùng đầu vào → cùng đầu ra, không đọc đồng hồ, không ghi file, không sửa biến dùng chung.

```scala
val accepted = orders
  .distinctBy(o => (o.orderId, o.sku))          // bỏ bản trùng (giữ bản đầu)
  .filter(o => customers.contains(o.customerId)) // bỏ tham chiếu không tồn tại
  .filterNot(_.day.isAfter(today))               // bỏ ngày ở tương lai — `today` là THAM SỐ, không phải LocalDate.now()
```

Không bước nào sửa `orders`; mỗi bước trả danh sách **mới**. Lợi ích: chạy lại cho cùng kết quả (dễ debug, dễ backfill), test bằng dữ liệu cố định, ghép bước tùy ý.

| Phép | Kiểu | Dùng khi |
|---|---|---|
| `map` | `F[A] => (A => B) => F[B]` | Đổi từng phần tử, số lượng giữ nguyên |
| `filter` | `F[A] => (A => Boolean) => F[A]` | Giữ một phần |
| `flatMap` | `F[A] => (A => F[B]) => F[B]` | Mỗi phần tử sinh 0..n phần tử (một đơn → nhiều dòng hàng) |
| `foldLeft` | `F[A] => B => ((B, A) => B) => B` | Gom cả tập thành một giá trị |
| `groupMapReduce` | nhóm theo khóa + đổi + gộp | Tổng hợp theo khóa trong **một** lượt (không tạo danh sách con như `groupBy(...).map(_.size)`) |

### 3.2. Either dừng sớm, Validated gom tất cả

`Either` là một **Monad**: `flatMap` cho phép bước sau **phụ thuộc** kết quả bước trước — nên khi bước trước lỗi, không thể chạy bước sau. Đó là hành vi đúng khi
có phụ thuộc (đọc file rồi mới parse). Với các kiểm tra **độc lập** của một dòng, ta muốn **mọi** lỗi:

```scala
val asEither    = (Left("lỗi 1"): Either[String, Int]).flatMap(_ => Left("lỗi 2"))   // Left("lỗi 1")
val asValidated = ("lỗi 1".invalidNec[Int], "lỗi 2".invalidNec[Int]).mapN(_ + _)     // Invalid(Chain(lỗi 1, lỗi 2))
```

`Validated` chỉ là **Applicative** (không có `flatMap` hợp lệ), nên `mapN` chạy được mọi thành phần rồi gom lỗi. `ValidatedNec[E, A]` = `Validated[NonEmptyChain[E], A]`:
khi `Invalid` thì **chắc chắn có ít nhất một** lỗi, và nối lỗi là O(1).

```scala
def order(r: RawOrder): V[Order] =
  (
    nonEmpty("order_id", r.orderId),
    nonEmpty("customer_id", r.customerId).map(_.toUpperCase(java.util.Locale.ROOT)),
    date("day", r.day),
    enumOf("status", r.status, Status.values),
    nonEmpty("sku", r.sku).map(_.toUpperCase(java.util.Locale.ROOT)),
    intIn("quantity", r.quantity, 1, 1000),
    positiveLong("unit_price_vnd", r.unitPriceVnd)
  ).mapN(Order.apply)
```

So với Chương 19 (tuple `Either` + `collect { case Left(e) => e }`): cùng ý tưởng, nhưng `mapN` dựng `Order` **chỉ khi** mọi trường hợp lệ — compiler bảo đảm,
không cần pattern match tuple 6 phần tử. Dòng `O6` của dữ liệu mẫu nhận **5** lỗi một lúc (mục 9).

`DataError(field, value, rule)` là lỗi dạng **dữ liệu**: trường nào, giá trị gì, quy tắc nào — đủ để người sửa nguồn hành động, và đếm được theo trường.

**Lựa chọn thay thế:** tự gom bằng `List` (Chương 19) — ít khái niệm hơn, dài hơn; `Either` + dừng sớm — đủ cho kiểm tra phụ thuộc. **Lỗi thường gặp:** dùng
`flatMap`/for-comprehension trên `Validated` (không có — phải `andThen` nếu thật sự có phụ thuộc); quên import `cats.syntax.all.*` (mục 10).

### 3.3. Data quality rule là dữ liệu

Validation trả lời "**dòng này** có hợp lệ không?". Nhiều câu hỏi chất lượng không trả lời được trên một dòng:

| Quy tắc | Phạm vi | Ở lab |
|---|---|---|
| Trường bắt buộc, khoảng giá trị | dòng | `Validate.order` (loại dòng) |
| Khóa duy nhất | **cả tập** | `Rule.unique` — `(order_id, sku)` |
| Toàn vẹn tham chiếu | **cả tập + bảng khác** | `Rule.references` — `customer_id` phải có trong customers |
| Ngày không ở tương lai | dòng (cần `today`) | `Rule.row` |
| Ngưỡng nghiệp vụ "lạ" | dòng, mức **Warn** | đơn giá > 50 triệu |
| Tỷ lệ hoàn tiền theo ngày | **tổng hợp** | lời giải bài 1 |

```scala
final case class Rule[A](name: String, severity: Severity, run: List[A] => RuleResult)

def unique[A, K](name: String, severity: Severity)(key: A => K): Rule[A] =
  Rule(name, severity, xs =>
    val dups = xs.groupBy(key).filter(_._2.size > 1)
    RuleResult(name, severity, xs.size, dups.values.map(_.size).sum, dups.keys.take(3).map(_.toString).toList))
```

Quy tắc là **giá trị**: có tên, mức độ, đầu ra có cấu trúc (`checked`, `failed`, ví dụ vi phạm). Nhờ vậy ta in báo cáo, lưu lịch sử, so sánh giữa các ngày, và dựng
**quality gate**: dừng pipeline nếu một quy tắc mức `Error` vượt ngưỡng tỷ lệ lỗi. Dữ liệu mẫu: 3 quy tắc Error vượt 10% → gate **CHẶN** (mục 9).

**Chặn hay cho qua?** Gate chặn bảo vệ người dùng báo cáo khỏi số liệu sai, đổi lại báo cáo bị trễ. Quy tắc `Warn` (đơn giá bất thường) chỉ cảnh báo — dữ liệu có thể
đúng (đơn sỉ lớn). Phân loại Warn/Error là quyết định **nghiệp vụ**, cần chủ dữ liệu đồng ý. Chương 33 bàn sâu về data quality và data contract.

### 3.4. Tổng hợp tổng quát với Monoid

Một **Monoid** cho kiểu `V` gồm `empty: V` và `combine: (V, V) => V` thỏa hai luật:

- **Kết hợp:** `(a |+| b) |+| c == a |+| (b |+| c)`
- **Trung hòa:** `empty |+| a == a == a |+| empty`

```scala
final case class Stats(count: Long, sum: BigInt, min: Long, max: Long)

given CommutativeMonoid[Stats] with            // Monoid + giao hoán (mục 3.5)
  def empty: Stats = Stats(0, BigInt(0), Long.MaxValue, Long.MinValue)
  def combine(a: Stats, b: Stats): Stats =
    Stats(a.count + b.count, a.sum + b.sum, a.min.min(b.min), a.max.max(b.max))

def by[A, K, V: Monoid](xs: Iterable[A])(key: A => K)(value: A => V): Map[K, V] =
  xs.foldLeft(Map.empty[K, V]) { (acc, a) =>
    val k = key(a)
    acc.updated(k, acc.getOrElse(k, Monoid[V].empty) |+| value(a))
  }
```

`Aggregate.by` viết **một lần**, dùng cho mọi loại tổng hợp: doanh thu (`Stats`), số user khác nhau (`Set` — hợp tập là Monoid), trung bình (`Mean` — lời giải bài 2),
cộng theo khóa (`Map[K, BigInt]` — Cats cung cấp sẵn Monoid).

**Vì sao `sum` là `BigInt`, không phải `Long` + `Math.addExact`?** Bản đầu dùng `addExact` để tràn số là lỗi chứ không phải số âm lặng lẽ (bài học Chương 15).
Nhưng review chéo chỉ ra nó **phá luật kết hợp**: `(MaxValue |+| 1) |+| -1` ném `ArithmeticException`, còn `MaxValue |+| (1 |+| -1)` thì ra `MaxValue`.
Cách đặt ngoặc — tức cách chia khối/partition — quyết định chạy được hay không, nên đó **không** phải Monoid. Phép gộp phải **toàn phần** (luôn ra kết quả):
`BigInt` cộng không tràn. Ở **ranh giới** (ghi vào cột `BIGINT` của DB) mới kiểm tra vừa `Long` và báo lỗi. Property test chỉ thấy lỗi này sau khi generator
sinh cả giá trị **biên** (`Long.MinValue/MaxValue`) — với ±1.000.000 nó xanh mãi.

**Vì sao luật quan trọng — trung bình của các trung bình.** Trung bình **không** gộp được bằng cách lấy trung bình các phần:

```
dữ liệu chia 2 phần: List(List(100, 100, 100, 100), List(1000))
trung bình của trung bình = 550
trung bình đúng (sum/count) = 280
```

Chia khác đi (`[100, 100] + [100, 100, 1000]`) lại ra 250. Kết quả **phụ thuộc cách chia** → không dùng được khi dữ liệu chia ra nhiều khối/máy. Sửa: gộp
`(sum, count)` — một Monoid — và chỉ chia ở bước cuối.

### 3.5. Trong bộ nhớ, theo luồng, phân tán

`Execution.scala` chạy **cùng** `(key, value, Monoid)` theo ba mô hình:

```scala
// 1. Trong bộ nhớ: cả tập đã nằm trong List
Aggregate.by(xs)(key)(value)

// 2. Theo luồng: từng khối 4096 phần tử; chỉ giữ bảng tổng hợp đang tích lũy
xs.chunkN(chunkSize).fold(Map.empty[K, V])((acc, chunk) => Aggregate.merge(acc, Aggregate.by(chunk.toList)(key)(value)))

// 3. Chia partition (mô phỏng ý tưởng phân tán, TRONG một JVM):
//   (a) chia ngẫu nhiên n phần  (b) mỗi phần tổng hợp riêng  (c) "shuffle" theo hash(key) % n  (d) gộp
```

Property test kiểm tra trên **nhiều mẫu ngẫu nhiên** (danh sách, kích thước khối, n, cách chia) rằng ba mô hình cho **cùng** kết quả — đó là bằng chứng thực nghiệm,
không phải chứng minh. Lý do nó đúng: `Stats` là Monoid **giao hoán**.

**Kết hợp chưa đủ cho partition.** Theo khối (mô hình 2) thứ tự được giữ, chỉ cần kết hợp. Chia partition (mô hình 3) xáo thứ tự, nên cần thêm **giao hoán**.
Bản đầu `partitioned` chỉ đòi `V: Monoid`; review chéo cho ví dụ nối chuỗi (`String` là Monoid nhưng không giao hoán): bộ nhớ ra `"abcdef"`, chia 3 partition ra
`"adbcfe"` hay `"befadc"` tùy seed. Giờ chữ ký là `V: CommutativeMonoid` và compiler từ chối trường hợp đó (lỗi thật):

```
No given instance of type cats.kernel.CommutativeMonoid[String] was found for a context parameter of method partitioned in object Execution
```

Spark đặt cùng yêu cầu này nhưng **không** kiểm tra bằng kiểu: `reduceByKey` với hàm không giao hoán vẫn chạy và cho kết quả phụ thuộc lịch thực thi.

Số đo thật, 1.000.000 đơn hàng, 50 khóa, cùng JVM, trung vị 3 lượt sau 1 lượt khởi động:

| Mô hình | Thời gian | Cùng kết quả |
|---|---|---|
| Trong bộ nhớ (`List`) | 83,8 ms | ✓ |
| Theo luồng (fs2, khối 4096) | 135,6 ms | ✓ |
| Chia 8 partition + shuffle | 242,5 ms | ✓ |

Đọc bảng **đúng cách**: ở đây dữ liệu **đã** nằm trong RAM, nên "trong bộ nhớ" nhanh nhất; theo luồng tốn thêm việc cắt khối; chia partition tốn thêm sao chép và
"shuffle". **Phân tán không làm việc nhanh hơn** — nó làm được việc **lớn hơn một máy**. Khi dữ liệu vừa một máy, mô hình đơn giản nhất thường tốt nhất.

| | Trong bộ nhớ | Theo luồng | Phân tán (Spark, Chương 22) |
|---|---|---|---|
| Giới hạn | RAM một máy | Thời gian; trạng thái phải nhỏ | Số máy, chi phí mạng |
| Thứ tự | Có | Có (theo luồng đến) | **Không** đảm bảo |
| Lỗi giữa chừng | Chạy lại | Chạy lại / checkpoint | Engine tự chạy lại phần lỗi |
| Tối ưu | Không có optimizer | Không | Catalyst tối ưu kế hoạch (DataFrame) |
| Đòi hỏi ở phép gộp | Không | Kết hợp (theo khối) | **Kết hợp + giao hoán** (thứ tự các phần không xác định) |

**Không được khẳng định** code xử lý `List` có đặc tính hiệu năng giống Spark DataFrame: khác nơi lưu, khác cách thực thi, khác khả năng chịu lỗi. Điều **chuyển được**
sang Spark là **tư duy**: biến đổi thuần, khóa + giá trị + phép gộp có luật.

### 3.6. Kiểm thử biến đổi

- **Golden tính tay** (`TransformSuite`): doanh thu theo `(segment, ngày)` của dữ liệu mẫu, so **cả bốn** trường `count, sum, min, max`.
- **Property test** (`PropertySuite`, ScalaCheck): luật Monoid; tương đương ba mô hình; một **phản ví dụ** tìm bằng `Prop.exists` (trung bình của trung bình).
- **Mutation test:** cố ý sửa `combine` thành `min = a.min` (bỏ qua `b.min`). Kết quả thật:
  - Test luật **trung hòa** đỏ — ScalaCheck in đầu vào gây lỗi và seed để tái hiện (mục 9).
  - Test "ba mô hình cho cùng kết quả" **vẫn xanh**: cả ba cùng sai giống nhau! Tương đương không bảo đảm đúng.
  - Golden ban đầu (chỉ so `count, sum`) **vẫn xanh** — lỗ hổng; sau khi so cả `min, max`, golden đỏ đúng như mong đợi.

Bài học: mỗi loại test bắt một loại lỗi; **chính bộ test cũng cần được kiểm tra** bằng cách cài lỗi có chủ ý.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Pure transformation | Biến đổi thuần: không side effect, cùng vào → cùng ra |
| `Validated` / `ValidatedNec` | Kiểu kết quả gom lỗi (Applicative); `Nec` = NonEmptyChain |
| Applicative vs Monad | `mapN` (độc lập) vs `flatMap` (phụ thuộc) |
| Data quality rule / quality gate | Quy tắc chất lượng / cổng chặn pipeline khi vượt ngưỡng |
| Referential integrity | Toàn vẹn tham chiếu: khóa ngoại phải tồn tại |
| Monoid, associativity, identity | Cấu trúc gộp được; tính kết hợp; phần tử trung hòa |
| Map-side combine | Tổng hợp trước tại chỗ, trước khi gửi qua mạng |
| Shuffle | Chuyển dữ liệu giữa các partition theo khóa |
| Property-based testing, generator, shrinking | Test thuộc tính; bộ sinh dữ liệu; thu nhỏ phản ví dụ |
| Mutation testing | Cài lỗi có chủ ý để kiểm tra bộ test |

## 5. Ví dụ đơn giản

```scala
import cats.syntax.all.*

val revenue = List(("KB-01", 450000L), ("MS-01", 250000L), ("KB-01", 400000L))
  .groupMapReduce(_._1)(_._2)(_ + _)
// Map(KB-01 -> 850000, MS-01 -> 250000)

val bySkuParts = List(Map("KB-01" -> 450000L), Map("KB-01" -> 400000L, "MS-01" -> 250000L)).combineAll
// cùng kết quả: Monoid[Map[String, Long]] của Cats cộng theo khóa — gộp các kết quả từng phần
```

## 6. Ví dụ thực tế — tính tay dữ liệu mẫu

`data/customers.csv`: C1 Retail, C2 Wholesale, `c3,retail` (chuẩn hóa → C3 Retail), C4 `Vip` (**sai** segment).
`data/orders.csv` — 10 dòng:

| Dòng | Kết luận tính tay |
|---|---|
| O1 KB-01 `c1`, `kb-01`, 2 × 450.000 | hợp lệ (chuẩn hóa) |
| O1 MS-01, 1 × 250.000 | hợp lệ |
| O2 KB-01 `paid`, 50 × 400.000 | hợp lệ |
| O3 HP-01 REFUNDED | hợp lệ (không tính doanh thu PAID) |
| O4 C9 | hợp lệ về dòng; **C9 không tồn tại** |
| O5 quantity 0 | ❌ validation |
| O6 thiếu customer, ngày 2026-13-01, SHIPPED, thiếu sku, giá -5 | ❌ **5 lỗi** |
| O7 60.000.000 | hợp lệ; **Warn** giá > 50 triệu |
| O2 KB-01 lần nữa | **trùng** `(O2, KB-01)` |
| O8 ngày 2026-10-20 | **tương lai** (hôm nay 2026-10-17) |

Quality (trên 8 dòng hợp lệ): trùng 2/8 = 25%, tham chiếu 1/8, tương lai 1/8, giá 1/8 (Warn). Gate ngưỡng 10% → **chặn** 3 quy tắc Error.
Sau `Clean.accept` (bỏ bản trùng thứ hai, O4, O8) còn 5 dòng; doanh thu PAID. **Grain** (một dòng = gì?) là **dòng hàng** `(order_id, sku)`, không phải đơn:
O1 có 2 dòng hàng nên Retail 15-10 đếm 2 dù chỉ có 1 đơn. Bản đầu ghi nhãn `đơn=2` — sai nghĩa, đã sửa sau review chéo.

| Segment | Ngày | Dòng hàng | Tổng | Min | Max |
|---|---|---|---|---|---|
| Retail | 15-10 | 2 (O1 KB 900.000, O1 MS 250.000) | 1.150.000 | 250.000 | 900.000 |
| Retail | 16-10 | 1 (O7) | 60.000.000 | 60.000.000 | 60.000.000 |
| Wholesale | 15-10 | 1 (O2 = 20.000.000) | 20.000.000 | 20.000.000 | 20.000.000 |

## 7. Code đầy đủ

```
08-data-processing/transform-lab/
├── project.scala / build.sbt / project/build.properties
├── data/customers.csv, data/orders.csv
├── src/main/scala/vn/scalaeng/transform/
│   ├── Model.scala          ← RawOrder/RawCustomer, Order/Customer, DataError, Validate (ValidatedNec + mapN)
│   ├── Quality.scala        ← Rule (row / unique / references), QualityReport, gate, OrderRules
│   ├── Aggregate.scala      ← Stats + Monoid, Aggregate.by / merge, averageOfAverages (cố ý sai)
│   ├── Execution.scala      ← inMemory / streaming / partitioned
│   ├── TransformDemo.scala  ← Clean, Reports, demo đầy đủ
│   └── ExecutionBench.scala ← đo 3 mô hình trên 1 triệu dòng
└── src/test/scala/vn/scalaeng/transform/
    ├── TransformSuite.scala ← 11 test: validation, quality, golden, phản ví dụ, hồi quy từ review
    └── PropertySuite.scala  ← 5 property ScalaCheck
```

```bash
cd 08-data-processing/transform-lab
scala-cli test .                                                    # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.transform.TransformDemo
scala-cli run . --main-class vn.scalaeng.transform.ExecutionBench --java-opt -Xmx2g
```

## 8. Giải thích code

- **`Validate.partition`** — `rows.partitionMap(r => f(r).toEither.leftMap(r -> _))`: một lượt, tách `(hợp lệ, (dòng gốc, lỗi))`; giữ dòng gốc cạnh lỗi để truy vết.
- **`enumOf`** — tìm theo `toString` không phân biệt hoa thường; thông báo lỗi liệt kê giá trị hợp lệ (`Paid/Refunded/Cancelled`) — người sửa nguồn biết phải sửa thành gì.
- **`Rule.unique`** — mọi bản ghi của một khóa trùng đều tính là vi phạm (2/8 cho `(O2, KB-01)`), ví dụ là chính khóa đó.
- **`QualityReport.blocking`** — chỉ quy tắc `Error` mới chặn; trả **danh sách** quy tắc vượt ngưỡng để in lý do. Ngưỡng qua `requireRate`: `NaN` hay ngoài
  `[0, 1]` → `IllegalArgumentException` (mục 10).
- **`text` (trong `Validate`)** — CSV parser có thể trả `null` cho ô thiếu; `Option(s).map(_.trim).getOrElse("")` biến `null` thành chuỗi rỗng để quy tắc "bắt buộc" báo lỗi
  có cấu trúc thay vì `NullPointerException`. `toUpperCase(Locale.ROOT)` để kết quả không đổi theo locale máy (Chương 19).
- **`Stats` / `CommutativeMonoid`** — `empty` có `min = Long.MaxValue`, `max = Long.MinValue` để `empty |+| a == a`; `sum: BigInt` để phép gộp toàn phần.
- **`Execution.partitioned`** — `Random(seed)`: test truyền seed để mỗi lần chạy một cách chia khác nhưng **tái hiện được**; `Math.floorMod` vì `hashCode` có thể âm.
- **`PropertySuite`** — `Gen.frequency(1 -> empty, 5 -> stats)`: cố ý cho `empty` xuất hiện thường xuyên vì đó là chỗ hay sai; `genStats` trộn giá trị thường
  với giá trị **biên** (`Long.MinValue`, `Long.MaxValue`, -1, 0, 1) — nơi lỗi tràn số ẩn nấp. Có thêm property **giao hoán**. Mặc định 100 lượt mỗi property.

## 9. Output mong đợi (output thật)

**`TransformDemo`:**

```
== Validation: 3 customer hợp lệ, 8 dòng đơn hàng hợp lệ
  loại RawCustomer(C4,Vip,Huế) -> segment='Vip': phải là một trong Retail/Wholesale
  loại RawOrder(O5,C2,2026-10-16,PAID,MS-01,0,250000) -> quantity='0': số nguyên trong 1..1000
  loại RawOrder(O6,,2026-13-01,SHIPPED,,1,-5) -> customer_id='': bắt buộc; day='2026-13-01': ngày dạng yyyy-MM-dd; status='SHIPPED': phải là một trong Paid/Refunded/Cancelled; sku='': bắt buộc; unit_price_vnd='-5': số nguyên trong 1..1000000000000
== Data quality
(order_id, sku) duy nhất           Error    2/8     25.00%  (O2,KB-01)
customer_id tồn tại                Error    1/8     12.50%  C9
ngày không ở tương lai             Error    1/8     12.50%  O8@2026-10-20
đơn giá <= 50 triệu                Warn     1/8     12.50%  O7:60000000
== Quality gate (Error > 10%): CHẶN — (order_id, sku) duy nhất, customer_id tồn tại, ngày không ở tương lai
== Doanh thu PAID theo (segment, ngày) trên 5 dòng được chấp nhận
  Retail    2026-10-15  dòng hàng=2  tổng=1,150,000  min=250,000  max=900,000
  Retail    2026-10-16  dòng hàng=1  tổng=60,000,000  min=60,000,000  max=60,000,000
  Wholesale 2026-10-15  dòng hàng=1  tổng=20,000,000  min=20,000,000  max=20,000,000
== Trung bình của các trung bình (SAI) vs trung bình đúng
  dữ liệu chia 2 phần: List(List(100, 100, 100, 100), List(1000))
  trung bình của trung bình = 550
  trung bình đúng (sum/count) = 280
```

Khớp từng số với bảng tính tay ở mục 6.

**`ExecutionBench`** (`-Xmx2g`):

```
1000000 đơn hàng, 50 khóa (sku)
  trong bộ nhớ (List)            trung vị     83.8 ms   cùng kết quả: true
  theo luồng (fs2, khối 4096)    trung vị    135.6 ms   cùng kết quả: true
  chia 8 partition + shuffle     trung vị    242.5 ms   cùng kết quả: true
```

**ScalaCheck khi một luật bị phá** (mutation `min = a.min`):

```
==> X vn.scalaeng.transform.PropertySuite.Monoid[Stats]: phần tử trung hòa — empty |+| a == a == a |+| empty
Failing seed: Fd5LngFQQbyqkMYHViPBeCoXbtv_gRpMu2txAKpEYlD=
You can reproduce this failure by adding the following override to your suite:

  override def scalaCheckInitialSeed = "Fd5LngFQQbyqkMYHViPBeCoXbtv_gRpMu2txAKpEYlD="

Falsified after 0 passed tests.
> ARG_0: Stats(1,503318,503318,503318)
```

`empty |+| Stats(1,503318,...)` cho `min = Long.MaxValue` ≠ 503318. Seed in ra để **chạy lại đúng đầu vào đó** khi debug.

**Test:** `PropertySuite 5/5`, `TransformSuite 11/11`; `sbt test`: `Passed: Total 16, Failed 0`.

## 10. Lỗi thường gặp (thật)

**1. `NonEmptyChain` không có `.toList`?**

```
[error] value toList is not a member of cats.data.NonEmptyChainImpl.Type[vn.scalaeng.transform.DataError], but could be made available as an extension method.
[error]
[error] One of the following imports might fix the problem:
[error]
[error]   import cats.implicits.toFoldableOps
[error]   ...
[error]   import cats.syntax.all.toFoldableOps
```

`NonEmptyChain` của Cats là một **opaque type** (Chương 8A): bên ngoài, nó không lộ phương thức của kiểu bên dưới. `toList` đến từ cú pháp `Foldable` — cần
`import cats.syntax.all.*`. Đây là lỗi hay gặp nhất khi bắt đầu dùng Cats; một import cho toàn bộ cú pháp là đủ.

**2. Trung bình của các trung bình** — không có thông báo lỗi nào, chỉ có con số sai (550 thay vì 280). Mọi phép gộp chạy trên dữ liệu chia phần phải là Monoid.

**3. Top-K theo phần rồi gộp** (lời giải bài 4) — sản phẩm có doanh thu cao nhất bị cắt mất một phần vì không đứng đầu ở **mọi** partition chứa nó. Gộp theo khóa **trước**, cắt top-K **sau**.

**4. Test tương đương không phát hiện lỗi chung** — ba mô hình cùng sai thì vẫn "bằng nhau" (mục 3.6). Cần thêm test luật và golden.

**5. Golden so thiếu trường** — so `(count, sum)` mà bỏ `min, max` → lỗi ở `min` lọt qua (mục 3.6). So **toàn bộ** giá trị kết quả.

Các lỗi dưới đây do **review chéo** (Codex) tìm ra trên bản đầu; mỗi lỗi giờ có test hồi quy:

**6. `Math.addExact` phá luật kết hợp** (mục 3.4) — tràn hay không phụ thuộc cách đặt ngoặc. Sửa: `sum: BigInt`; generator thêm giá trị biên.

**7. `partitioned` chỉ đòi `Monoid`** (mục 3.5) — nối chuỗi cho kết quả khác nhau theo seed. Sửa: `CommutativeMonoid`; compiler chặn `String`.

**8. Ngưỡng `NaN` làm gate không bao giờ chặn** — `failRate > Double.NaN` luôn `false`; một biến môi trường gõ sai (`"10%"` parse lỗi thành `NaN`) tắt cả cổng
chất lượng mà không ai biết. Sửa: `QualityReport.requireRate` từ chối `NaN` và ngoài `[0, 1]` — cấu hình sai là lỗi **khởi động**.

**9. Sai grain** — đếm dòng hàng nhưng gọi là "đơn" (mục 6); ở lời giải bài 1, tỷ lệ hoàn tiền chia theo **dòng**: O1 10 sku Paid + O2 1 sku Refunded ra 1/11 = 9%
(qua ngưỡng 20%) trong khi theo **đơn** là 1/2 = 50%. Luôn ghi rõ một dòng của bảng là gì.

**10. Dedup chọn bừa khi hòa phiên bản** (lời giải bài 3) — `maxBy` lấy bản gặp trước, nên hai bản cùng phiên bản khác nội dung cho kết quả phụ thuộc thứ tự đến.
Sửa: báo **xung đột**; test chạy mọi hoán vị.

**11. Tổng Top-K bằng `Long`** — tràn thành số âm và xếp hạng ngược. Sửa: `BigInt`.

## 11. Cách debug

- **Validation sai:** gọi `Validate.order(RawOrder(...))` trên đúng dòng gốc (lấy từ danh sách bị loại, đã giữ dòng gốc).
- **Số tổng hợp lệch:** tính trên 5–10 dòng bằng tay (như mục 6); so từng khóa.
- **Property test đỏ:** chép `scalaCheckInitialSeed` vào suite để tái hiện; đọc `ARG_0...` — ScalaCheck đã thu nhỏ đầu vào hết mức có thể.
- **Nghi kết quả phụ thuộc cách chia:** chạy `Execution.partitioned` với nhiều `n` và `seed`; khác nhau nghĩa là phép gộp không phải Monoid.
- **Chậm:** đo từng mô hình riêng (`ExecutionBench`) trước khi tối ưu; thường chi phí nằm ở sao chép dữ liệu, không ở logic.

## 12. Bài tập

1. **(Cơ bản)** Quy tắc chất lượng **trên tổng hợp**: tỷ lệ đơn `Refunded` mỗi ngày không vượt 20%.
2. **(Cơ bản)** Viết `Monoid[Mean]` đúng; kiểm tra bằng ScalaCheck rằng trung bình không phụ thuộc cách chia.
3. **(Trung bình)** Dedup **tổng quát** bằng type class `Versioned[A]` (khóa + phiên bản), giữ bản mới nhất, không phụ thuộc thứ tự đến (hai bản cùng phiên bản thì sao?).
4. **(Trung bình)** Top-K sản phẩm theo doanh thu khi dữ liệu chia nhiều partition. Chỉ ra phản ví dụ cho cách "top-K từng phần rồi gộp".
5. **(Nâng cao)** Số user **khác nhau** mỗi ngày trên dữ liệu chia partition. Monoid nào? Bộ nhớ tăng theo gì?
6. **(Nâng cao)** Pipeline có **quality gate**: validation → kiểm tra chất lượng → chỉ tính báo cáo khi gate cho qua; trả kết quả có cấu trúc cho cả hai trường hợp.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch20-solutions.md`](../solutions/ch20-solutions.md)
- Code + test (9 test, 1 property): [`solutions/ch20-code/`](../solutions/ch20-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao `Either` dừng ở lỗi đầu còn `Validated` gom được mọi lỗi? Khi nào **nên** dừng ở lỗi đầu?
2. Kể hai quy tắc chất lượng không thể kiểm tra trên một dòng riêng lẻ.
3. Hai luật của Monoid là gì? Luật nào cho phép tính theo khối? Theo partition không có thứ tự còn cần thêm gì?
4. Vì sao "trung bình của các trung bình" sai? Sửa thế nào?
5. Trong `ExecutionBench`, vì sao chia partition chậm nhất? Vậy phân tán có ích khi nào?
6. Test "ba mô hình cho cùng kết quả" bỏ sót loại lỗi nào?
7. `Failing seed` của ScalaCheck dùng để làm gì?
8. Phần nào của chương này chuyển được sang Spark, phần nào không?

<details><summary>Gợi ý đáp án</summary>

1. `Either` là Monad — bước sau có thể phụ thuộc bước trước; `Validated` là Applicative — các kiểm tra độc lập. Dừng sớm khi bước sau thật sự cần kết quả bước trước (parse rồi mới validate).
2. Khóa duy nhất; toàn vẹn tham chiếu (cần bảng khác); tỷ lệ theo nhóm.
3. Kết hợp và phần tử trung hòa. Kết hợp cho phép gộp theo khối; không có thứ tự thì cần thêm **giao hoán**.
4. Mỗi phần có trọng số (số phần tử) khác nhau; gộp `(sum, count)` rồi chia cuối.
5. Thêm sao chép và shuffle, dữ liệu vốn đã vừa RAM. Phân tán có ích khi dữ liệu/phép tính vượt một máy.
6. Lỗi mà cả ba mô hình cùng mắc (ví dụ `combine` sai) — vẫn "bằng nhau".
7. Chạy lại đúng đầu vào gây lỗi để debug.
8. Chuyển được: hàm thuần, khóa/giá trị/phép gộp có luật, quality rule. Không chuyển được: đặc tính hiệu năng của `List`, thứ tự phần tử, cách đo.
</details>

## 15. Mini project

**"Báo cáo hằng ngày có cổng chất lượng"**: dùng dữ liệu sự kiện đã làm sạch ở Chương 19 (`AdEvent`):
- Định nghĩa ≥5 quy tắc chất lượng (dòng, cả tập, tổng hợp), phân loại Warn/Error, ngưỡng gate có lý do.
- Báo cáo CTR theo `(campaign, ngày)` bằng `Aggregate.by` với một Monoid tự viết `(impressions, clicks, users: Set)`.
- Property test: luật Monoid, ba mô hình thực thi cho cùng báo cáo, gửi lặp/đảo thứ tự không đổi báo cáo.
- Một mutation test cho mỗi property; ghi lại test nào bắt được lỗi nào.
- Đo ba mô hình trên 1 triệu sự kiện; giải thích số đo.

## 16. Nguồn tài liệu chính thức

- Cats — `Validated`: https://typelevel.org/cats/datatypes/validated.html
- Cats — `Monoid`, `Semigroup`: https://typelevel.org/cats/typeclasses/monoid.html
- Cats — `NonEmptyChain`: https://typelevel.org/cats/datatypes/chain.html
- ScalaCheck: https://scalacheck.org/ · munit-scalacheck: https://scalameta.org/munit/docs/integrations/scalacheck.html
- fs2 — `Stream.chunkN`, `fold`: https://fs2.io/
- Scala collections — `groupMapReduce`, `partitionMap`: https://docs.scala-lang.org/overviews/collections-2.13/introduction.html
- Maven Central (tra phiên bản): https://repo1.maven.org/maven2/

## 17. Checklist tự đánh giá

- [ ] Viết biến đổi thuần, nhận "hôm nay" làm tham số, không sửa dữ liệu đầu vào.
- [ ] Dùng `ValidatedNec` + `mapN` để gom mọi lỗi; biết khi nào dùng `Either`.
- [ ] Mô hình hóa quy tắc chất lượng như dữ liệu; dựng quality gate có ngưỡng.
- [ ] Viết Monoid và kiểm tra hai luật bằng ScalaCheck.
- [ ] Giải thích vì sao trung bình của trung bình và top-K từng phần sai.
- [ ] Giải thích ba mô hình thực thi và đọc đúng bảng số đo.
- [ ] Thử một mutation test và giải thích test nào bắt được lỗi.
- [ ] Chạy được 12 test của lab và 6 test lời giải.
