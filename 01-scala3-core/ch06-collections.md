# Chương 6 — Collections

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4
> **Code:** [`code/src/main/scala/scalaeng/ch06/AccessLogs.scala`](code/src/main/scala/scalaeng/ch06/AccessLogs.scala) · Test: [`AccessLogsSuite.scala`](code/src/test/scala/scalaeng/ch06/AccessLogsSuite.scala)
> **Trạng thái kiểm chứng:** Code **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo). 12/12 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Bảng độ phức tạp ở mục 3.3 tóm tắt từ trang *Performance Characteristics* trong tài liệu chính thức (link ở mục 16).

---

## 1. Mục tiêu học tập

- Chọn đúng collection (`List`, `Vector`, `Array`, `Set`, `Map`, tuple) theo cách truy cập dữ liệu.
- Phân biệt immutable và mutable collection, biết khi nào mutable là chấp nhận được.
- Thành thạo `map`, `flatMap`, `filter`, `collect`, `partition`, `foldLeft`, `groupBy`, `groupMapReduce`, `flatten`, `sliding`.
- Với mỗi operation: biết **kiểu đầu vào, kiểu đầu ra, chi phí**.
- Hiểu đánh giá **eager** và **lazy** (`view`, `iterator`).
- Áp dụng vào bài toán thực tế: phân tích access log của backend service.

## 2. Kiến thức tiên quyết

- Chương 4 (lambda, HOF), Chương 5 (case class).

## 3. Lý thuyết

### 3.1. Bản đồ collection

```
Iterable
├── Seq (có thứ tự, truy cập theo vị trí)
│   ├── List         — immutable, danh sách liên kết
│   ├── Vector       — immutable, cây rộng, truy cập ngẫu nhiên nhanh
│   ├── Range        — 1 to 10, không lưu từng phần tử
│   ├── ArrayBuffer  — mutable, mảng co giãn
│   └── (Array)      — mảng JVM, mutable, kích thước cố định (không thật sự là Seq, nhưng dùng như Seq)
├── Set  — không trùng: HashSet (immutable mặc định), mutable.HashSet, TreeSet (có thứ tự)
└── Map  — key → value: HashMap (mặc định), mutable.HashMap, TreeMap (có thứ tự)
```

Mặc định (`List`, `Set`, `Map` không import gì) là **immutable** — mọi phép "thay đổi" trả về collection mới,
phần dữ liệu không đổi được **chia sẻ cấu trúc** (structural sharing) nên không phải sao chép toàn bộ.

### 3.2. Chọn collection

| Nhu cầu | Chọn | Lý do |
|---|---|---|
| Duyệt tuần tự, thêm vào đầu, đệ quy head/tail | `List` | Prepend và head/tail hằng số |
| Truy cập theo chỉ số, thêm vào cuối, collection lớn | `Vector` | Gần như hằng số cho mọi thao tác |
| Hiệu năng tối đa với số nguyên/double, interop Java | `Array` | Không boxing với kiểu nguyên thủy, bộ nhớ liền mạch |
| Xây dựng dần trong một hàm | `ListBuffer` / `ArrayBuffer` / `Vector.newBuilder` → `.toList` | Builder mutable cục bộ, trả kết quả immutable |
| Kiểm tra tồn tại, loại trùng | `Set` | Tra cứu gần như hằng số |
| Tra cứu theo khóa | `Map` | Tra cứu gần như hằng số |
| Cần thứ tự theo khóa | `TreeMap`/`TreeSet` hoặc `sortBy` khi xuất | `HashMap` **không đảm bảo thứ tự** |
| Nhóm vài giá trị tạm thời | Tuple `(a, b)` | Nhanh; nhưng > 3 phần tử hoặc dùng rộng rãi → case class |

### 3.3. Chi phí các thao tác

Ký hiệu: **C** hằng số · **eC** gần như hằng số (effectively constant) · **aC** hằng số trung bình (amortized) · **L** tuyến tính theo kích thước.

| Collection | head | tail | apply(i) | update | prepend | append | contains/lookup |
|---|---|---|---|---|---|---|---|
| `List` | C | C | L | L | C | L | L |
| `Vector` | eC | eC | eC | eC | eC | eC | L |
| `ArrayBuffer` | C | L | C | C | L | aC | L |
| `Array` | C | L | C | C | L | L | L |
| `HashSet`/`HashMap` | — | — | — | — | — | — | eC |

**Cạm bẫy kinh điển:** `list(i)` trong vòng lặp `for i <- 0 until list.size` → O(n²). Với 100.000 phần tử là 5 tỷ bước.
`list :+ x` trong vòng lặp cũng O(n²). Dùng `Vector` hoặc builder.

### 3.4. Các operation cốt lõi: vào gì, ra gì

| Operation | Chữ ký (đơn giản hóa) | Ra | Ghi chú |
|---|---|---|---|
| `map` | `List[A].map(A => B)` | `List[B]` | Cùng số phần tử |
| `filter` / `filterNot` | `List[A].filter(A => Boolean)` | `List[A]` | ≤ số phần tử |
| `flatMap` | `List[A].flatMap(A => IterableOnce[B])` | `List[B]` | Mỗi phần tử sinh 0..n phần tử |
| `flatten` | `List[List[A]]` | `List[A]` | = `flatMap(identity)` |
| `collect` | `List[A].collect(PartialFunction[A, B])` | `List[B]` | `filter` + `map` trong một bước, dùng pattern |
| `partition` | `List[A].partition(A => Boolean)` | `(List[A], List[A])` | Một lượt duyệt |
| `partitionMap` | `List[A].partitionMap(A => Either[L, R])` | `(List[L], List[R])` | Rất hữu ích cho tách lỗi (Chương 7) |
| `foldLeft` | `List[A].foldLeft(z: B)((B, A) => B)` | `B` | Tổng quát nhất; mọi phép gộp viết được bằng fold |
| `reduce` | `List[A].reduce((A, A) => A)` | `A` | **Ném exception với list rỗng** |
| `groupBy` | `List[A].groupBy(A => K)` | `Map[K, List[A]]` | Giữ toàn bộ phần tử trong bộ nhớ |
| `groupMap` | `.groupMap(A => K)(A => V)` | `Map[K, List[V]]` | |
| `groupMapReduce` | `.groupMap...(A => K)(A => V)((V, V) => V)` | `Map[K, V]` | Gộp ngay, không giữ List trung gian |
| `sortBy` | `.sortBy(A => K)` | `List[A]` | Ổn định (stable); O(n log n) |
| `sliding(n)` | | `Iterator[List[A]]` | Cửa sổ trượt |
| `zipWithIndex` | | `List[(A, Int)]` | Gắn số thứ tự |
| `find` / `exists` / `forall` / `count` | | `Option[A]` / `Boolean` / `Boolean` / `Int` | `find`, `exists`, `forall` dừng sớm |

**`groupBy` vs `groupMapReduce`:** đếm request theo path bằng `groupBy(_.path).map((k, v) => k -> v.size)` giữ **toàn bộ log** trong các List con
chỉ để đếm. `groupMapReduce(_.path)(_ => 1)(_ + _)` chỉ giữ một số đếm mỗi key.

### 3.5. Immutable và mutable

```scala
import scala.collection.mutable
val buffer = mutable.ListBuffer.empty[String]
for log <- logs if log.isError do buffer += log.path
buffer.toList
```

Quy tắc: **mutable collection được phép nếu không thoát khỏi phạm vi hàm** (builder cục bộ). Không trả về, không lưu vào field chia sẻ.
Lý do: an toàn đa luồng và lập luận được về code.

Import `scala.collection.mutable` rồi viết `mutable.Map` — **không** `import scala.collection.mutable.Map`, vì sẽ che `Map` immutable và gây nhầm lẫn.

### 3.6. Eager và lazy: `view`, `iterator`

```scala
logs.map(f).filter(p).take(1)           // eager: map TOÀN BỘ, filter TOÀN BỘ, rồi lấy 1
logs.view.map(f).filter(p).take(1).toList  // lazy: xử lý từng phần tử cho đến khi đủ 1
```

Output thật trong demo: `find trên view: Some(/api/orders), số phần tử đã map: 4 / 7` — chỉ 4 trên 7 phần tử được xử lý.

| | Khi nào dùng |
|---|---|
| Collection thường (eager) | Mặc định; dễ debug |
| `view` | Chuỗi nhiều bước trên collection lớn, chỉ cần một phần kết quả |
| `iterator` | Duyệt **một lần** dữ liệu lớn hơn bộ nhớ (đọc file từng dòng) — dùng xong là hết |
| `LazyList` | Dãy vô hạn hoặc tính toán lười có ghi nhớ |

**Liên hệ Data Engineering:** Spark cũng lazy (transformation không chạy cho đến khi có action — Chương 22), nhưng lý do và cơ chế khác hẳn:
Spark lazy để **tối ưu toàn bộ kế hoạch truy vấn** và phân phối lên cluster; `view` chỉ tránh tạo collection trung gian trong một tiến trình.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Immutable / mutable collection | Collection không đổi được / đổi được |
| Structural sharing | Chia sẻ phần dữ liệu chung giữa phiên bản cũ và mới |
| Linked list | Danh sách liên kết |
| Random access | Truy cập theo chỉ số |
| Prepend / append | Thêm vào đầu / cuối |
| Amortized constant | Hằng số trung bình qua nhiều lần |
| Partial function | Hàm chỉ định nghĩa cho một phần đầu vào (`{ case ... }`) |
| Fold / reduce | Gộp collection thành một giá trị |
| Strict (eager) / lazy evaluation | Tính ngay / tính khi cần |
| Builder | Đối tượng mutable dùng để xây collection |
| Percentile | Bách phân vị (p50, p95, p99) |
| Stable sort | Sắp xếp giữ thứ tự ban đầu của các phần tử bằng nhau |

## 5. Ví dụ đơn giản

```scala
val nums = List(1, 2, 3, 4, 5, 6)
nums.map(_ * 10)                     // List(10, 20, 30, 40, 50, 60)
nums.filter(_ % 2 == 0)              // List(2, 4, 6)
nums.partition(_ > 3)                // (List(4, 5, 6), List(1, 2, 3))
nums.foldLeft(0)(_ + _)              // 21
nums.groupBy(_ % 3)                  // Map(0 -> List(3, 6), 1 -> List(1, 4), 2 -> List(2, 5)) (thứ tự key có thể khác)
List("a b", "c").flatMap(_.split(" "))  // List(a, b, c)
List(List(1), Nil, List(2, 3)).flatten   // List(1, 2, 3)
List(1, "x", 2).collect { case i: Int => i * 2 }  // List(2, 4)
```

## 6. Ví dụ thực tế — phân tích access log

Backend service ghi log mỗi request: `timestampMs userId method path status latencyMs`. Đội vận hành cần:
số dòng log hỏng, tỷ lệ lỗi 5xx, endpoint được gọi nhiều nhất, số user, latency trung bình/p50/p95, request chậm nhất của từng user,
và cảnh báo khi có "burst" lỗi liên tiếp.

Lưu ý thiết kế:
- Dòng log hỏng **không làm dừng** chương trình mà được đếm (data quality).
- Mỗi chỉ số là một hàm thuần nhận `List[AccessLog]` → test độc lập.
- `latencyStats` dùng `foldLeft` để tính count/sum/max **trong một lượt duyệt** thay vì ba lượt.
- Percentile cần truy cập theo chỉ số → chuyển sang `Vector`.

## 7. Code đầy đủ

Xem [`AccessLogs.scala`](code/src/main/scala/scalaeng/ch06/AccessLogs.scala). Các phần chính:

```scala
final case class AccessLog(timestampMs: Long, userId: String, method: String,
                           path: String, status: Int, latencyMs: Long):
  def isError: Boolean = status >= 500
  def isAnonymous: Boolean = userId == "-"

final case class LatencyStats(count: Int, total: Long, max: Long):
  def average: Double = if count == 0 then 0.0 else total.toDouble / count
  def add(latency: Long): LatencyStats = LatencyStats(count + 1, total + latency, math.max(max, latency))

object AccessLogs:
  private def isNumber(s: String): Boolean = s.nonEmpty && s.forall(_.isDigit)

  def parseAll(lines: List[String]): List[AccessLog] =
    lines
      .map(_.trim.split("\\s+"))
      .collect:
        case Array(ts, user, method, path, status, latency)
            if isNumber(ts) && isNumber(status) && isNumber(latency) =>
          AccessLog(ts.toLong, user, method, path, status.toInt, latency.toLong)

  def splitErrors(logs: List[AccessLog]): (List[AccessLog], List[AccessLog]) = logs.partition(_.isError)

  def requestsByPath(logs: List[AccessLog]): Map[String, Int] =
    logs.groupMapReduce(_.path)(_ => 1)(_ + _)

  def topPaths(logs: List[AccessLog], n: Int): List[(String, Int)] =
    requestsByPath(logs).toList.sortBy((path, count) => (-count, path)).take(n)

  def latencyStats(logs: List[AccessLog]): LatencyStats =
    logs.foldLeft(LatencyStats.Empty)((acc, log) => acc.add(log.latencyMs))

  def latencyPercentile(logs: List[AccessLog], p: Int): Long =
    require(p > 0 && p <= 100, s"p ngoài (0, 100]: $p")
    require(logs.nonEmpty, "không có dữ liệu")
    val sorted: Vector[Long] = logs.map(_.latencyMs).toVector.sorted
    val rank = math.ceil(p / 100.0 * sorted.size).toInt
    sorted(rank - 1)

  def slowestPerUser(logs: List[AccessLog]): Map[String, AccessLog] =
    logs.filterNot(_.isAnonymous).groupBy(_.userId).view.mapValues(_.maxBy(_.latencyMs)).toMap

  def hasErrorBurst(logs: List[AccessLog], window: Int, threshold: Int): Boolean =
    logs.sortBy(_.timestampMs).sliding(window).exists(_.count(_.isError) >= threshold)
```

## 8. Giải thích code

- `split("\\s+")` trả về `Array[String]`; pattern `case Array(ts, user, ...)` chỉ khớp khi **đúng 6 phần tử** — tự động loại dòng thiếu/thừa cột.
- `collect` với guard: dòng có `status = "abc"` không khớp guard → bị loại, không ném exception. Cú pháp `.collect:` + khối thụt lề là Scala 3; tương đương `.collect { case ... }`.
- `sortBy((path, count) => (-count, path))` — sắp theo tuple: tiêu chí 1 là count giảm dần (đổi dấu), tiêu chí 2 là path tăng dần. Tiêu chí phụ giúp kết quả **xác định** (deterministic) khi bằng nhau — quan trọng cho test và báo cáo.
- `LatencyStats.add` trả về đối tượng mới; `foldLeft` mang nó qua từng phần tử. Không có `var` nào.
- `latencyPercentile` dùng phương pháp *nearest-rank*. Có nhiều định nghĩa percentile khác (nội suy tuyến tính...) cho kết quả khác nhau với dữ liệu nhỏ — luôn ghi rõ định nghĩa dùng.
- `.view.mapValues(...).toMap` — cách đúng trong Scala 2.13+/3; `Map.mapValues` trực tiếp đã deprecated (mục 10).
- `maxBy` **ném exception với collection rỗng**; ở đây an toàn vì mỗi nhóm của `groupBy` luôn có ít nhất một phần tử.
- `sliding(3)` trên 7 phần tử cho 5 cửa sổ; `exists` dừng ở cửa sổ đầu tiên thỏa mãn.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch06.ch06Demo
Parse được 7/9 dòng, lỗi định dạng: 2
Lỗi 5xx: 2, thành công: 5
Error rate: 28.6%
Top paths: List((/api/orders,3), (/api/products,2))
Users: List(u1, u2, u3)
Latency: count=7, avg=384.7 ms, max=1500 ms
p50=60 ms, p95=1500 ms
Chậm nhất theo user: List(u1->120, u2->60, u3->1500)
Segments: List(42, 7, api, orders, products)
Có burst lỗi (2 lỗi trong 3 request)? true
Error paths: List(/api/orders, /api/orders)
find trên view: Some(/api/orders), số phần tử đã map: 4 / 7
```

Nhận xét vận hành: trung bình 384,7 ms nhưng p50 chỉ 60 ms — **trung bình bị kéo bởi vài request rất chậm**. Đây là lý do giám sát latency luôn dùng percentile.

## 10. Lỗi thường gặp

**Lỗi runtime thường gặp** (output thật):

| Code | Exception |
|---|---|
| `Map("a" -> 1)("b")` | `java.util.NoSuchElementException: key not found: b` |
| `List(1,2,3)(5)` | `java.lang.IndexOutOfBoundsException: 5` |
| `List.empty[Int].head` | `java.util.NoSuchElementException: head of empty list` |
| `List.empty[Int].maxBy(identity)` | `java.lang.UnsupportedOperationException: empty.maxBy` |

Cách tránh: `get`/`getOrElse`, `lift(i)`, `headOption`, `maxByOption` — các phiên bản trả `Option` (Chương 7).

**Cảnh báo deprecated** khi dùng `mapValues` kiểu cũ (biên dịch với `-deprecation`):
```
-- Deprecation Warning: E.scala:2:49
2 |  val m = List(("a", 1), ("a", 2)).groupBy(_._1).mapValues(_.size)
  |method mapValues in trait MapOps is deprecated since 2.13.0: Use .view.mapValues(f).
  |A future version will include a strict version of this method (for now, .view.mapValues(f).toMap).
```
Nhiều bài viết trên mạng (cho Scala 2.12) vẫn dùng cách cũ.

**Lỗi hiệu năng:**
- `list(i)` hoặc `list :+ x` trong vòng lặp → O(n²).
- `groupBy` chỉ để đếm → giữ toàn bộ dữ liệu trong bộ nhớ; dùng `groupMapReduce`.
- `.size` trên `List` là O(n); gọi nhiều lần trong vòng lặp thì lưu vào `val`.
- Dùng `Set`/`Map` khi cần tra cứu nhiều lần thay vì `list.contains` (O(n) mỗi lần).

**Lỗi logic:**
- Test phụ thuộc thứ tự của `Map`/`Set` → pass trên máy này, fail trên máy khác. Sắp xếp trước khi so sánh, hoặc so sánh `Map` với `Map`.
- `reduce` trên list có thể rỗng → exception. Dùng `foldLeft` với giá trị khởi đầu.
- Chia nguyên: `count / size` với hai `Int` → 0. Chuyển sang `Double` trước.

## 11. Cách debug

- **In kiểu trung gian:** tách chuỗi dài thành nhiều `val` có ghi kiểu: `val grouped: Map[String, List[AccessLog]] = ...`. Compiler báo ngay bước nào sai kiểu.
- **`tapEach`** để nhìn dữ liệu giữa các bước mà không phá chuỗi: `logs.tapEach(l => println(l)).filter(...)`.
- **Dữ liệu nhỏ, có chủ đích:** mỗi test dùng 3–5 bản ghi tự tạo làm lộ rõ trường hợp cần kiểm tra (bằng nhau, rỗng, biên).
- **Nghi ngờ hiệu năng:** đo trước khi sửa. Ghi thời gian với `System.nanoTime` cho n = 10³, 10⁴, 10⁵ — nếu thời gian tăng 100 lần khi n tăng 10 lần, có thao tác O(n²).

## 12. Bài tập

1. **(Cơ bản)** `wordFrequency(text: String): Map[String, Int]` — đếm từ (không phân biệt hoa thường), tách theo ký tự không phải chữ/số. Phải đúng với tiếng Việt có dấu.
   Gợi ý regex: `"[^\\p{L}\\p{N}]+"`.
2. **(Cơ bản)** `topN(scores: Map[String, Long], n: Int): List[(String, Long)]` — điểm giảm dần, bằng điểm thì tên tăng dần.
3. **(Trung bình)** `mergeInventories(a: Map[String, Int], b: Map[String, Int]): Map[String, Int]` — cộng tồn kho theo SKU; SKU chỉ có ở một bên vẫn giữ.
4. **(Trung bình — DE)** `dedupLatest(records: List[(String, Long, String)]): List[(String, Long, String)]` — bản ghi `(id, updatedAt, payload)`;
   mỗi id giữ bản có `updatedAt` lớn nhất; kết quả sắp theo id. Đây chính là bước dedup trong mọi pipeline CDC.
5. **(Nâng cao)** `errorBurstStarts(statuses: List[Int], window: Int, threshold: Int): List[Int]` — trả về **chỉ số bắt đầu** của mọi cửa sổ có ≥ `threshold` mã lỗi 5xx.
   Gợi ý: `sliding`, `zipWithIndex`, `collect`. Xử lý trường hợp list ngắn hơn `window`.

## 13. Đáp án và lời giải

- Code: [`Ch06Solutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch06Solutions.scala) · Test: [`Ch06SolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch06SolutionsSuite.scala)
- Giải thích: [`solutions/ch06-solutions.md`](../solutions/ch06-solutions.md)

## 14. Câu hỏi kiểm tra

1. Vì sao `list :+ x` trong vòng lặp chậm, còn `x :: list` thì nhanh?
2. Khi nào chọn `Vector` thay cho `List`?
3. `map` và `flatMap` khác nhau ở kiểu của hàm truyền vào thế nào?
4. `collect` thay thế được tổ hợp operation nào?
5. Viết lại `filter(p)` và `map(f)` bằng `foldLeft`.
6. Vì sao `groupMapReduce` tiết kiệm bộ nhớ hơn `groupBy` khi chỉ cần đếm?
7. `view` và Spark DataFrame đều "lazy" — khác nhau thế nào về mục đích?
8. Khi nào dùng mutable collection là chấp nhận được?

## 15. Mini project — Báo cáo access log

Viết `@main def logReport(path: String)` đọc file log thật (dùng `scala.io.Source.fromFile(path).getLines()` trong `try/finally` để đóng file),
gọi các hàm của chương và in báo cáo: tổng request, dòng hỏng, error rate, top 5 path, p50/p95/p99 theo **từng path**, top 3 user gọi nhiều nhất.
Tạo file log mẫu ≥ 1.000 dòng bằng một chương trình Scala sinh dữ liệu ngẫu nhiên có seed cố định (`scala.util.Random(42)`).
Phần tính toán phải là hàm thuần có test; chỉ `@main` được đọc file và in.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Collections: https://docs.scala-lang.org/scala3/book/collections-intro.html
- Collections Methods: https://docs.scala-lang.org/scala3/book/collections-methods.html
- Performance Characteristics: https://docs.scala-lang.org/overviews/collections-2.13/performance-characteristics.html
- Views: https://docs.scala-lang.org/overviews/collections-2.13/views.html
- Mutable/Immutable: https://docs.scala-lang.org/overviews/collections-2.13/overview.html
- API `scala.collection.immutable.List`: https://www.scala-lang.org/api/current/scala/collection/immutable/List.html

## 17. Checklist tự đánh giá

- [ ] Tôi chọn được collection phù hợp dựa trên cách truy cập.
- [ ] Tôi thuộc chi phí của `apply`, `prepend`, `append` trên `List` và `Vector`.
- [ ] Với mỗi operation trong bảng 3.4, tôi nói được kiểu đầu vào và đầu ra.
- [ ] Tôi viết được bất kỳ phép gộp nào bằng `foldLeft`.
- [ ] Tôi dùng `groupMapReduce` khi chỉ cần giá trị gộp.
- [ ] Tôi biết các phương thức nào ném exception với collection rỗng.
- [ ] Tôi không viết test phụ thuộc thứ tự của `Map`/`Set`.
- [ ] Tôi phân biệt được lazy của `view` và lazy của Spark.
