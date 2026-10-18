# Chương 10 — Testing

> **Phiên bản:** Scala 3.9.0 · JDK 21 · munit 1.2.4 · munit-scalacheck 1.2.0 (mục 3.5, chưa kiểm chứng)
> **Code:** test của [`order-platform`](../04-professional-engineering/order-platform/) — module `testkit` và thư mục `src/test` của `domain`, `core`, `app`
> **Trạng thái kiểm chứng:** 25/25 test **đã chạy** bằng `sbt test` (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Thư viện property-based testing tự viết trong `testkit` **đã chạy thật**, phản ví dụ trích trong bài là output thật.
> Ví dụ ScalaCheck ở mục 3.5: **chưa kiểm chứng**.

---

## 1. Mục tiêu học tập

- Chọn đúng loại test cho từng tầng: unit, property-based, golden, integration.
- Viết test cho hàm thuần nhanh, rõ, không phụ thuộc môi trường.
- Thiết kế **seam** (điểm thay thế) để test code có side effect bằng **test double** (fake, stub, spy) — không cần thư viện mocking.
- Dùng **fixture** để tạo/dọn tài nguyên (thư mục tạm) an toàn.
- Tìm và test **boundary case** có hệ thống; xây **test data** đáng tin cậy.
- Hiểu cơ chế **property-based testing** bằng cách tự viết một phiên bản nhỏ, rồi biết dùng ScalaCheck.
- Viết **regression test** cho bug thật và **golden test** cho output toàn cục.
- Biết cách tiếp cận **test với database** (chuẩn bị cho Chương 15).

## 2. Kiến thức tiên quyết

- Chương 7 (hàm thuần), Chương 9 (project `order-platform`, `Main.run` trả exit code).

## 3. Lý thuyết

### 3.1. Kim tự tháp test áp dụng cho project

```
            ▲  ít, chậm, giá trị cao về "toàn hệ thống chạy được"
           ╱ ╲      Integration: Main.run + file system thật (4 test)
          ╱───╲     Golden: output toàn pipeline so với file đã duyệt (3 test)
         ╱─────╲    Property: bất biến đúng với MỌI input sinh ngẫu nhiên (6 test)
        ╱───────╲   Unit: từng hàm thuần (12 test)
       ▔▔▔▔▔▔▔▔▔▔▔  nhiều, nhanh, chỉ đúng chỗ hỏng
```

Nhờ Functional Core / Imperative Shell (Chương 7, 9), **phần lớn logic là hàm thuần** → đáy kim tự tháp rộng, toàn bộ 25 test chạy trong vài giây.

### 3.2. Unit test cho hàm thuần

```scala
test("CsvLine.split theo RFC 4180"):
  assertEquals(CsvLine.split("a,\"b,c\",d"), Right(List("a", "b,c", "d")))
  assert(CsvLine.split("\"open").isLeft)
```

Một test tốt: **một hành vi**, tên mô tả hành vi (tiếng Việt cũng được), dữ liệu nhỏ có chủ đích, kết quả mong đợi viết tường minh (không tính lại bằng chính logic đang test).

### 3.3. Boundary case — liệt kê có hệ thống

| Loại biên | Ví dụ trong project |
|---|---|
| Rỗng | File rỗng, file chỉ có header, trường rỗng `a,,` |
| Một phần tử | Một dòng dữ liệu |
| Ngưỡng | `MAX_REJECT_RATIO` đúng bằng tỷ lệ lỗi; số lượng `0` và `1` |
| Giới hạn kiểu | `Vnd.unsafe(Long.MaxValue) + 1` → `ArithmeticException` |
| Định dạng lạ | Dấu phẩy trong nháy kép, nháy kép lồng `""`, BOM UTF-8, khoảng trắng hai đầu |
| Ngày không tồn tại | `2026-02-30`, `2026-13-40` |
| Trùng lặp | Cùng `order_id` hai lần, ba lần |
| Unicode | `đ`, `ệ` trong dữ liệu và log |

### 3.4. Property-based testing — hiểu từ gốc

- **Vấn đề:** unit test chỉ kiểm tra những ví dụ **bạn nghĩ ra**. Bug thường nằm ở ví dụ bạn không nghĩ ra.
- **Ý tưởng:** phát biểu **thuộc tính** đúng với *mọi* input, để máy sinh hàng trăm input ngẫu nhiên và tìm phản ví dụ.

Module `testkit` tự cài phiên bản tối giản (khoảng 50 dòng) để thấy cơ chế:

```scala
final case class Gen[A](sample: Random => A):
  def map[B](f: A => B): Gen[B] = Gen(r => f(sample(r)))
  def flatMap[B](f: A => Gen[B]): Gen[B] = Gen(r => f(sample(r)).sample(r))

object Prop:
  def forAll[A](gen: Gen[A], runs: Int = 200, seed: Long = 42L)(property: A => Boolean): Option[Failure]
```

**Các dạng thuộc tính hay dùng trong BE/DE:**

| Dạng | Ví dụ trong project |
|---|---|
| Round-trip (mã hóa rồi giải mã được giá trị cũ) | `split(join(escape(fields))) == fields` |
| Bất biến (invariant) | `input = valid + rejected + duplicates`; không còn `order_id` trùng |
| Bảo toàn (conservation) | Tổng doanh thu theo ngày = tổng doanh thu các dòng hợp lệ |
| Xác định (determinism) | `run(x) == run(x)` |
| Luật đại số | Tính kết hợp của Monoid (Chương 8B) |
| So với cài đặt tham chiếu | Hàm tối ưu cho cùng kết quả với hàm đơn giản |

**Property sai cũng có ích:** test "một property SAI bị phát hiện" phát biểu giả định ngây thơ *số cột = số dấu phẩy + 1*.
Output thật — phản ví dụ tìm được **ngay lần thử đầu tiên** với seed 42:

```
Some(Failure(42,0,List(a;a" , "đđ, ,a; ),property trả về false))
```

Trường thứ hai chứa dấu phẩy và nháy kép; sau khi escape thành `"""đđ, ,a; "` thì dòng có nhiều dấu phẩy hơn số cột. Đây chính là loại bug
mà `split(",")` ở Chương 7 mắc phải với `"GIAY,DEN"`. **Seed cố định** cho phép tái hiện chính xác: chạy lại luôn ra cùng phản ví dụ.

**Generator quyết định chất lượng test.** `trickyChar` được chọn có chủ đích: chữ thường, chữ có dấu, số, khoảng trắng, `,`, `"`, `;`.
Sinh ký tự ngẫu nhiên hoàn toàn trong toàn bộ Unicode sẽ hiếm khi tạo ra `,` và `"` — đúng những ký tự gây lỗi.
`Gen.frequency(7 -> validRow, 3 -> brokenRow)` trộn 70% dòng hợp lệ, 30% dòng lỗi và tự nhiên tạo `order_id` trùng (id chỉ trong khoảng 1–30).

### 3.5. ScalaCheck — công cụ thật

Phiên bản tự viết thiếu **shrinking**: khi tìm ra phản ví dụ, ScalaCheck thu nhỏ nó về dạng tối giản (ví dụ từ 6 trường dài xuống `List("", ",")`) — dễ hiểu hơn nhiều.
Ngoài ra còn có generator cho mọi kiểu chuẩn, phân bố ưu tiên giá trị biên, thống kê phân bố dữ liệu sinh ra.

```scala
// build.sbt
libraryDependencies += "org.scalameta" %% "munit-scalacheck" % "1.2.0" % Test
```

```scala
import munit.ScalaCheckSuite
import org.scalacheck.{Gen, Prop}

class CsvLineProps extends ScalaCheckSuite:
  private val field = Gen.listOf(Gen.oneOf('a', 'đ', ',', '"', ' ')).map(_.mkString)

  property("round-trip"):
    Prop.forAll(Gen.nonEmptyListOf(field)) { fs =>
      CsvLine.split(fs.map(CsvLine.escape).mkString(",")) == Right(fs)
    }
```

(Trạng thái: **chưa kiểm chứng** — cú pháp theo tài liệu munit-scalacheck; phiên bản 1.2.0 ghi nhận trên Scaladex.)

### 3.6. Test doubles và seam

Code có side effect (đọc đồng hồ, ghi log, gọi API) cần **seam**: chỗ mà test thay được thứ thật bằng thứ giả.

| Loại | Định nghĩa | Ví dụ trong project |
|---|---|---|
| **Stub** | Trả về giá trị cố định | `fixedClock: () => Instant` luôn trả `2026-10-14T00:00:00Z` |
| **Fake** | Cài đặt thật nhưng đơn giản | Log ghi vào `ByteArrayOutputStream` thay vì stderr; thư mục tạm thay thư mục dữ liệu thật |
| **Spy** | Ghi lại các lời gọi để kiểm tra sau | Đọc nội dung log đã ghi để assert có `"balanced":"true"` |
| **Mock** | Đối tượng do thư viện tạo, kiểm tra kỳ vọng tương tác | Không dùng |

Seam trong project:
- `Main.run(env: Map[String, String], err: PrintStream = System.err, now: () => Instant = ...)` — môi trường, luồng log, đồng hồ đều là **tham số** có giá trị mặc định cho production.
- `AppConfig.load(vars: Map[...])` — không đọc `sys.env` trực tiếp.
- `trait Logger` trong `core` — port (Chương 9).

**Vì sao không dùng thư viện mocking (Mockito...)?** Trong code Scala thiết kế theo hàm thuần và trait nhỏ, fake viết tay ngắn hơn, an toàn kiểu, và không gắn test vào
**chi tiết cài đặt** ("hàm X được gọi đúng 2 lần với tham số Y"). Test kiểm tra **kết quả**, không kiểm tra cách làm. Mocking hữu ích khi làm việc với thư viện Java lớn khó thay thế.

### 3.7. Fixture

```scala
private val tempDir = FunFixture[Path](
  setup = _ => Files.createTempDirectory("order-platform-test"),
  teardown = dir => Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p => Files.delete(p))
)

tempDir.test("dữ liệu sạch -> exit 0, ghi đủ file output"):
  dir =>
    ...
```

`FunFixture` của munit tạo tài nguyên **trước mỗi test** và dọn **sau mỗi test, kể cả khi test thất bại**. Mỗi test có thư mục riêng → chạy song song không giẫm chân nhau,
không để rác trên máy CI. munit còn có fixture cấp suite (`beforeAll`/`afterAll`) cho tài nguyên đắt như container database (mục 3.10).

### 3.8. Golden test và regression test

**Golden test** (còn gọi snapshot/approval test): chạy pipeline trên `golden/input.csv` (10 dòng chứa đủ loại lỗi), so **toàn bộ** output với `expected_*.csv` đã được người duyệt.

```scala
test("rejected khớp golden file"):
  assertNoDiff(Reports.rejectedCsv(result.rejected ++ result.duplicates), resource("expected_rejected.csv"))
```

- Bắt mọi thay đổi ngoài ý muốn ở output: thứ tự cột, định dạng số, thông điệp lỗi, escape.
- Khi output **cố ý** thay đổi: xem diff, duyệt, cập nhật file expected **trong cùng commit** với thay đổi code. Reviewer thấy rõ output đổi thế nào.
- `assertNoDiff` in diff từng dòng khi khác — dễ đọc hơn `assertEquals` trên chuỗi dài.
- Với Data Engineering, golden dataset là nền tảng của regression test cho pipeline (Chương 33).

**Regression test:** mỗi bug được sửa phải kèm một test **tái hiện bug trước khi sửa**. Bug thật trong project: file CSV xuất từ Excel có BOM UTF-8 (`U+FEFF`) ở đầu,
nên header `"\uFEFForder_id,..."` không khớp `order_id` — cả file bị từ chối. Kiểm chứng lại hành vi trước khi sửa: so sánh header có BOM với header mong đợi cho `false`, ký tự đầu là `feff`.
Quy trình đúng: (1) viết test thất bại, (2) sửa `header.stripPrefix("\uFEFF")`, (3) test pass, (4) commit cả hai.

### 3.9. Test data đáng tin cậy

- **Nhỏ và có chủ đích:** mỗi dòng trong `golden/input.csv` tồn tại vì một lý do (một loại lỗi). Ghi chú lý do khi cần.
- **Tự đọc hiểu được:** người review nhìn file input và expected là đoán được kết quả.
- **Không lấy dữ liệu production thật** vào repo (thông tin cá nhân, pháp lý). Tạo dữ liệu tổng hợp có cùng đặc điểm.
- **Xác định:** không phụ thuộc ngày giờ hiện tại, thứ tự `Map`, seed ngẫu nhiên không cố định.

### 3.10. Test với database (giới thiệu cho Chương 15)

| Cách | Ưu | Nhược |
|---|---|---|
| Fake repository trong bộ nhớ | Rất nhanh, test logic service | Không kiểm tra SQL thật |
| Database nhúng / khác loại (H2 thay PostgreSQL) | Nhanh | SQL dialect khác → test pass, production lỗi |
| **Testcontainers** — PostgreSQL thật trong Docker | Đúng phiên bản, đúng SQL, đúng migration | Chậm hơn, cần Docker |

Chiến lược trong giáo trình: logic service test bằng fake repository; **repository** test bằng PostgreSQL thật qua Testcontainers, chạy migration Flyway trước,
mỗi test trong transaction rollback hoặc dọn bảng. Chi tiết và phiên bản thư viện sẽ được kiểm chứng ở Chương 15.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Unit / integration / end-to-end test | Test một đơn vị / nhiều thành phần thật / toàn hệ thống |
| Property-based testing | Kiểm tra thuộc tính trên input sinh ngẫu nhiên |
| Generator | Bộ sinh dữ liệu test |
| Counterexample / shrinking | Phản ví dụ / thu nhỏ phản ví dụ |
| Invariant | Bất biến |
| Round-trip property | Mã hóa rồi giải mã ra giá trị ban đầu |
| Test double: stub, fake, spy, mock | Các loại đối tượng thay thế trong test |
| Seam | Điểm trong code cho phép thay thế phụ thuộc khi test |
| Fixture / setup / teardown | Tài nguyên test / chuẩn bị / dọn dẹp |
| Golden (snapshot, approval) test | So output với kết quả đã duyệt |
| Regression test | Test chống lỗi cũ quay lại |
| Flaky test | Test lúc pass lúc fail không do code thay đổi |
| BOM (Byte Order Mark) | Ký tự `U+FEFF` đầu file văn bản |

## 5. Ví dụ đơn giản

```scala
import munit.FunSuite

class VatSuite extends FunSuite:
  def vat(amount: Long): Long = amount / 10

  test("10% của 1000"):
    assertEquals(vat(1000), 100L)

  test("biên: số nhỏ hơn 10 cho 0 (chia nguyên)"):
    assertEquals(vat(9), 0L)
```

## 6. Ví dụ thực tế

Bộ test của `order-platform` (25 test):

| Suite | Module | Loại | Kiểm tra |
|---|---|---|---|
| `DomainSuite` | domain | Unit | Smart constructor, tràn số |
| `CoreSuite` | core | Unit | CSV RFC 4180, header, cân bằng, escape output |
| `PropertySuite` | core | Property | Round-trip CSV, bất biến pipeline, bảo toàn doanh thu, xác định, phát hiện property sai |
| `GoldenSuite` | core | Golden + regression | Toàn bộ output, BOM |
| `AppConfigSuite` | app | Unit | Cấu hình theo môi trường, tích lũy lỗi, logger |
| `MainIntegrationSuite` | app | Integration | Exit code 0/1/2/3, file output thật, log |

## 7. Code đầy đủ

- [`modules/testkit/.../Gen.scala`](../04-professional-engineering/order-platform/modules/testkit/src/main/scala/vn/scalaeng/order/testkit/Gen.scala), [`Prop.scala`](../04-professional-engineering/order-platform/modules/testkit/src/main/scala/vn/scalaeng/order/testkit/Prop.scala)
- [`PropertySuite.scala`](../04-professional-engineering/order-platform/modules/core/src/test/scala/vn/scalaeng/order/core/PropertySuite.scala)
- [`GoldenSuite.scala`](../04-professional-engineering/order-platform/modules/core/src/test/scala/vn/scalaeng/order/core/GoldenSuite.scala) và [`golden/`](../04-professional-engineering/order-platform/modules/core/src/test/resources/golden/)
- [`MainIntegrationSuite.scala`](../04-professional-engineering/order-platform/modules/app/src/test/scala/vn/scalaeng/order/app/MainIntegrationSuite.scala)

Trích đoạn generator dữ liệu đơn hàng:

```scala
private val validRow: Gen[String] = for
  id    <- Gen.choose(1, 30)
  price <- Gen.choose(1, 500_000)
  qty   <- Gen.choose(1, 5)
  day   <- Gen.choose(1, 28)
yield f"ORD-$id,SKU-$id,$price,$qty,2026-02-$day%02d"

private val brokenRow: Gen[String] =
  Gen.oneOf("ORD-1,,1,1,2026-01-01", "ORD-x,A,1,1,2026-01-01", "ORD-2,A,-5,1,2026-01-01", "a,b", "\"open")

private val rows: Gen[List[String]] = Gen.listOf(40, Gen.frequency(7 -> validRow, 3 -> brokenRow))

test("bất biến: input = valid + rejected + duplicates với mọi dữ liệu"):
  assertEquals(Prop.forAll(rows) { rs => OrderPipeline.run(header :: rs).exists(_.isBalanced) }, None)
```

## 8. Giải thích code

- `Gen` là một hàm `Random => A` được bọc lại; `map`/`flatMap` cho phép dùng for-comprehension để ghép generator — cùng ý tưởng với `Option`/`Either` (Chương 7).
- `Prop.forAll` trả `Option[Failure]` thay vì ném exception → test viết `assertEquals(..., None)`; khi fail, munit in ra nguyên `Failure` gồm seed, lần thử, input.
- `Try(property(input))` bắt cả exception: một property ném lỗi cũng là phản ví dụ (thường quý hơn cả `false`).
- `day <- Gen.choose(1, 28)` tránh tạo ngày không tồn tại trong generator dòng **hợp lệ** — dòng có ngày sai thuộc về generator dòng lỗi.
- `Main.run(env, PrintStream(buf, true, "UTF-8"), fixedClock)` — chạy toàn bộ shell với stub đồng hồ và fake luồng log; assert cả exit code, file và log.
- `Files.walk(dir).sorted(Comparator.reverseOrder())` — xóa file trước, thư mục sau (thứ tự ngược) trong teardown.
- `testkit % Test` trong `build.sbt` — tiện ích test không lọt vào artifact production.

## 9. Output mong đợi

```
$ sbt test
...
vn.scalaeng.order.core.PropertySuite:
  + round-trip: split(join(escape(fields))) == fields
  + một property SAI bị phát hiện, kèm seed để tái hiện
  + bất biến: input = valid + rejected + duplicates với mọi dữ liệu
  + bất biến: không còn order_id trùng sau dedup
  + bất biến: tổng doanh thu theo ngày = tổng doanh thu các dòng hợp lệ
  + chạy lại cho cùng kết quả (deterministic)
...
[info] Passed: Total 25, Failed 0, Errors 0, Passed 25
```

(Định dạng chính xác do munit/sbt quyết định. Môi trường biên soạn ghi nhận: `Passed: 25, Failed: 0`.)

## 10. Lỗi thường gặp

| Vấn đề | Triệu chứng | Cách xử lý |
|---|---|---|
| Test phụ thuộc thời gian thật | Fail vào nửa đêm, cuối tháng, khác múi giờ | Truyền đồng hồ làm tham số (stub) |
| Test phụ thuộc thứ tự `Map`/`Set` | Pass local, fail CI | Sắp xếp trước khi so sánh hoặc so sánh `Map` với `Map` |
| Test dùng chung tài nguyên | Chạy song song thì fail ngẫu nhiên (flaky) | Fixture riêng cho từng test |
| Không dọn tài nguyên | Ổ đĩa CI đầy, test sau bị ảnh hưởng | `FunFixture` có teardown |
| Seed ngẫu nhiên không cố định | Không tái hiện được lỗi | Seed cố định, in seed khi fail |
| Generator "quá ngẫu nhiên" | Property luôn pass nhưng không tìm ra bug | Chọn phân bố có chủ đích (ký tự nguy hiểm, giá trị biên) |
| Kết quả mong đợi tính bằng chính code đang test | Test luôn pass kể cả khi code sai | Viết giá trị mong đợi tường minh |
| Mock chi tiết cài đặt | Refactor không đổi hành vi mà test vỡ hàng loạt | Test kết quả qua seam, dùng fake |
| Cập nhật golden file mà không đọc diff | Bug được "duyệt" thành kỳ vọng | Review diff golden như review code |

## 11. Cách debug

- Chạy một suite: `sbt "core/testOnly vn.scalaeng.order.core.PropertySuite"`; một test: `sbt "core/testOnly *PropertySuite -- *round-trip*"`.
- Tự động chạy lại khi lưu file: `sbt ~core/test`.
- Property fail: lấy `input` trong `Failure`, viết thành **unit test cụ thể** (regression), sửa code, giữ unit test lại.
- Golden fail: đọc diff của `assertNoDiff`; nếu thay đổi đúng ý, cập nhật file expected; nếu không, đó là bug.
- Test flaky: chạy lặp lại nhiều lần (`for i in $(seq 20); do sbt -batch core/test || break; done`), tìm phụ thuộc ẩn vào thời gian, thứ tự, tài nguyên chung.

## 12. Bài tập

1. **(Cơ bản)** Viết 5 unit test boundary cho `Quantity.parse`: `"0"`, `"1"`, `" 2 "`, `"2147483648"` (vượt `Int`), `"+3"`. Ghi lại hành vi thực tế của `"+3"` và quyết định nó có nên hợp lệ không.
2. **(Cơ bản)** Thêm `Gen.alphaNumString(maxLen)` vào `testkit` và viết property: với mọi chuỗi chữ-số không rỗng, `Sku.parse(s)` thành công khi `s.length <= 32`.
3. **(Trung bình)** Viết property **bảo toàn số lượng**: tổng `orders` trong `daily` bằng `valid.size`. Sau đó cố tình sửa sai `OrderPipeline` (ví dụ đếm `rejected` vào `orders`) và xem property phát hiện.
4. **(Trung bình)** Thêm generator dòng có **khoảng trắng thừa quanh các trường** và property: thêm khoảng trắng không làm thay đổi kết quả `valid`.
5. **(Nâng cao)** Cài đặt **shrinking** đơn giản cho `Prop.forAll` với `Gen[List[A]]`: khi tìm ra phản ví dụ là list, thử bỏ lần lượt từng phần tử, giữ lại list ngắn nhất vẫn làm property sai.
   Áp dụng cho property sai về số dấu phẩy và so sánh độ dài phản ví dụ trước/sau.

## 13. Đáp án và lời giải

Xem [`solutions/ch09-ch11-solutions.md`](../solutions/ch09-ch11-solutions.md).

## 14. Câu hỏi kiểm tra

1. Vì sao Functional Core / Imperative Shell làm đáy kim tự tháp test rộng hơn?
2. Kể 4 dạng property hay dùng và cho ví dụ trong pipeline dữ liệu.
3. Vì sao seed phải cố định và được in ra khi fail?
4. Stub, fake, spy khác nhau thế nào? Project dùng loại nào ở đâu?
5. Vì sao không cần thư viện mocking trong project này?
6. Golden test bắt được loại lỗi nào mà unit test thường bỏ sót?
7. Quy trình 4 bước khi sửa một bug?
8. Vì sao không nên dùng H2 để test code viết cho PostgreSQL?

## 15. Mini project

Thêm bộ test cho tính năng `FAIL_ON_DUPLICATES` (bài tập 4 Chương 9): unit test cấu hình, property "khi bật cờ và có trùng thì exit 4",
integration test với thư mục tạm, cập nhật golden file nếu output thay đổi. Mục tiêu: tính năng mới vào repo kèm đủ 4 tầng test.

## 16. Nguồn tài liệu chính thức

- munit — Getting started: https://scalameta.org/munit/docs/getting-started.html
- munit — Fixtures: https://scalameta.org/munit/docs/fixtures.html
- munit — Assertions (`assertNoDiff`): https://scalameta.org/munit/docs/assertions.html
- munit — ScalaCheck integration: https://scalameta.org/munit/docs/integrations/scalacheck.html
- ScalaCheck User Guide: https://github.com/typelevel/scalacheck/blob/main/doc/UserGuide.md
- sbt — Testing: https://www.scala-sbt.org/1.x/docs/Testing.html
- Testcontainers (Java): https://java.testcontainers.org/

## 17. Checklist tự đánh giá

- [ ] Tôi xác định được loại test phù hợp cho mỗi tầng của project.
- [ ] Tôi liệt kê boundary case có hệ thống trước khi viết code.
- [ ] Tôi viết được property round-trip, invariant, conservation, determinism.
- [ ] Tôi hiểu Gen/forAll hoạt động thế nào và vì sao shrinking quan trọng.
- [ ] Code có side effect của tôi có seam để thay bằng stub/fake.
- [ ] Tôi dùng fixture để tạo và dọn tài nguyên.
- [ ] Mỗi bug tôi sửa đều có regression test.
- [ ] Tôi biết khi nào dùng golden test và cách review khi nó thay đổi.
