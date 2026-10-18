# Chương 5 — Object-Oriented Programming trong Scala

> **Phiên bản:** Scala 3.9.0 · JDK 21 · Test: munit 1.2.4
> **Code:** [`code/src/main/scala/scalaeng/ch05/`](code/src/main/scala/scalaeng/ch05/) · Test: [`OrderDomainSuite.scala`](code/src/test/scala/scalaeng/ch05/OrderDomainSuite.scala)
> **Trạng thái kiểm chứng:** Code **đã biên dịch & chạy** (`scalac 3.9.0`, OpenJDK 21.0.10, không cảnh báo với `-Wunused:all`).
> 12/12 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Thông báo lỗi trong mục 10 là output thật của compiler.

---

## 1. Mục tiêu học tập

- Khai báo `class`, constructor, field, method; tạo đối tượng không cần `new` (Scala 3).
- Dùng `object` (singleton) và **companion object** (factory, hằng số, smart constructor).
- Dùng `trait` làm interface và mixin; hiểu `abstract class` khác `trait` ở đâu.
- Dùng **case class** cho dữ liệu và **enum** cho tập giá trị hữu hạn.
- Áp dụng access modifier (`private`, `protected`) để đóng gói.
- Biết vì sao **composition** thường tốt hơn **inheritance**, và áp dụng vào một service backend.
- Mô hình hóa domain đơn hàng có **invariant** (bất biến nghiệp vụ) được kiểm tra khi tạo đối tượng.

## 2. Kiến thức tiên quyết

- Chương 3 (kiểu, expression, `match`), Chương 4 (method, function value).

## 3. Lý thuyết

### 3.1. Class, constructor và đối tượng

```scala
final class SmsNotifier(maxLength: Int = 160) extends Notifier("sms"):
  protected def format(to: String, message: String): String = s"$to: ${message.take(maxLength)}"

val n = SmsNotifier(maxLength = 20)   // Scala 3: không cần `new`
```

- Tham số trong ngoặc sau tên class là **primary constructor**. Thân class (sau `:`) chạy khi tạo đối tượng.
- `maxLength` (không có `val`) là tham số riêng, chỉ dùng được bên trong class.
  `class Notifier(val channel: String)` — có `val` → trở thành **field công khai**.
- Scala 3 có *creator applications*: `SmsNotifier(...)` tương đương `new SmsNotifier(...)`.
- `final class`: không cho kế thừa. Nên là mặc định — chỉ bỏ `final` khi **thiết kế** để kế thừa.

### 3.2. `object` và companion object

```scala
object Money:                 // cùng tên, cùng file với case class Money → companion
  val Zero: Money = Money(BigDecimal(0))
  def vnd(value: Long): Money = Money(BigDecimal(value))
```

- `object` là singleton, khởi tạo lười khi dùng lần đầu. Thay cho `static` của Java.
- **Companion object** truy cập được thành viên `private` của class đồng hành và ngược lại.
- Công dụng điển hình: **factory method**, hằng số, **smart constructor** (`Order.create` luôn tạo đơn ở trạng thái `Pending`).

### 3.3. Trait

```scala
trait DiscountPolicy:
  def name: String
  def discount(order: Order): Money
```

- **Vấn đề:** service thanh toán cần áp dụng nhiều loại giảm giá; thêm loại mới không nên sửa service.
- **Giải pháp:** trait định nghĩa **hợp đồng**; mỗi chính sách là một implementation. Service chỉ biết đến trait → **polymorphism**.
- Trait có thể chứa method cụ thể (`Auditable.auditLabel`) → dùng như **mixin**: `class X extends A, B, C` (Scala 3 dùng dấu phẩy; Scala 2 dùng `with`).
- Scala 3 cho phép trait có tham số: `trait Named(val name: String)`.

### 3.4. Abstract class

```scala
abstract class Notifier(val channel: String):
  protected def format(to: String, message: String): String   // abstract
  final def send(to: String, message: String): String = s"[$channel] ${format(to, message)}"
```

| | `trait` | `abstract class` |
|---|---|---|
| Kế thừa nhiều | Có (mixin) | Chỉ một |
| Tham số constructor | Có (Scala 3) | Có |
| Gọi từ Java dễ dàng | Kém hơn | Tốt hơn |
| Dùng khi | Hầu hết trường hợp | Cần interop Java, hoặc muốn ép quan hệ "là một" duy nhất |

`send` là `final`, `format` là `protected` abstract → mẫu **Template Method**: lớp cha cố định quy trình, lớp con điền chi tiết.

### 3.5. Case class

```scala
final case class OrderLine(sku: String, unitPrice: Money, quantity: Int):
  require(quantity > 0, s"Số lượng phải > 0: $quantity")
  def total: Money = unitPrice * quantity
```

Compiler tự sinh: field `val` công khai, `equals`/`hashCode` **theo giá trị**, `toString` dễ đọc, `copy`, `apply`, `unapply` (cho pattern matching).

- **Dùng cho:** dữ liệu bất biến — DTO, domain entity, event, bản ghi dữ liệu. Đây là "đơn vị dữ liệu" chính trong cả BE lẫn DE.
- **`copy` thay cho setter:** `order.copy(status = next)` tạo đối tượng mới, đối tượng cũ giữ nguyên.
- **`require` trong thân:** kiểm tra **invariant** ngay khi tạo. Đối tượng không hợp lệ không thể tồn tại.
  Hạn chế: lỗi là exception. Chương 7 thay bằng smart constructor trả `Either`.

**Lỗi thiết kế thường gặp với case class:**
- Field `var` trong case class (`case class A(var x: Int)`) — phá vỡ `hashCode` khi đối tượng nằm trong `Set`/`Map`.
- Kế thừa từ case class — bị cấm giữa hai case class, và gây `equals` sai. Dùng `final case class`.
- Case class chứa hàng chục field "để dành" — tách thành các case class nhỏ có ý nghĩa.

### 3.6. Enum

```scala
enum OrderStatus:
  case Pending, Paid, Shipped, Cancelled

enum PaymentMethod:
  case Cod
  case Card(last4: String)
  case EWallet(provider: String)
```

- Enum đơn giản có sẵn `values`, `valueOf("Paid")`, `ordinal`.
- Enum có tham số (`Card(last4)`) là **Algebraic Data Type** — "một trong các dạng sau, mỗi dạng mang dữ liệu riêng".
  Chương 7 sẽ khai thác sức mạnh của ADT với pattern matching.
- Compiler kiểm tra `match` trên enum có **đủ trường hợp** không (mục 10).
- Scala 2 dùng `sealed trait` + `case object` để làm việc này; bạn sẽ gặp nhiều trong code cũ và trong Spark (Chương 22).

### 3.7. Access modifiers

| Modifier | Truy cập từ |
|---|---|
| (mặc định) | Mọi nơi — Scala **mặc định public** |
| `private` | Chỉ class đó và companion |
| `protected` | Class đó và lớp con |
| `private[ch05]` | Mọi thứ trong package `ch05` |

`RequestCounter` giữ `private var count` — trạng thái mutable **được đóng gói**, bên ngoài chỉ thấy `increment()` và `current`.
Đây là cách duy nhất chấp nhận được để có trạng thái mutable trong OOP. (Với nhiều thread, vẫn chưa an toàn — Chương 16.)

### 3.8. Composition hơn inheritance

```scala
final class CheckoutService(policy: DiscountPolicy, notifier: Notifier) extends Auditable:
  def checkout(order: Order, contact: String): (Money, String) = ...
```

| Inheritance (`class EmailCheckout extends Checkout`) | Composition (truyền phụ thuộc vào constructor) |
|---|---|
| Mỗi tổ hợp policy × kênh cần một lớp con → bùng nổ số lớp | Ghép tự do lúc tạo đối tượng: 3 policy × 2 kênh = 0 lớp mới |
| Lớp con phụ thuộc chi tiết của lớp cha (fragile base class) | Chỉ phụ thuộc hợp đồng (trait) |
| Khó test | Test truyền implementation giả |

Đây là **dependency injection bằng constructor** — không cần framework. Chương 18 phát triển thành kiến trúc module.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Primary constructor | Constructor khai báo ngay sau tên class |
| Singleton object | Đối tượng duy nhất (`object`) |
| Companion object | `object` cùng tên, cùng file với class |
| Factory method / Smart constructor | Method tạo đối tượng, có thể kiểm tra đầu vào |
| Trait / Mixin | Hợp đồng hành vi / trộn hành vi vào class |
| Abstract member | Thành viên chưa có cài đặt |
| Polymorphism | Đa hình: cùng lời gọi, hành vi khác theo implementation |
| Case class | Class dữ liệu bất biến có equals theo giá trị |
| Enum / ADT | Kiểu liệt kê / kiểu dữ liệu đại số |
| Invariant | Điều kiện luôn đúng với mọi đối tượng hợp lệ |
| Encapsulation | Đóng gói trạng thái và chi tiết cài đặt |
| Composition over inheritance | Ưu tiên ghép đối tượng hơn kế thừa |
| Dependency injection | Truyền phụ thuộc từ bên ngoài vào |

## 5. Ví dụ đơn giản

```scala
trait Shape:
  def area: Double

final case class Rect(w: Double, h: Double) extends Shape:
  def area: Double = w * h

final case class Circle(r: Double) extends Shape:
  def area: Double = math.Pi * r * r

val shapes: List[Shape] = List(Rect(2, 3), Circle(1))
shapes.map(_.area)   // List(6.0, 3.141592653589793)
```

## 6. Ví dụ thực tế — domain đơn hàng và checkout

Yêu cầu nghiệp vụ:

1. Đơn hàng có ít nhất một dòng; mỗi dòng có SKU không rỗng, số lượng > 0; tiền không âm.
2. Trạng thái chỉ chuyển theo: `Pending → Paid | Cancelled`, `Paid → Shipped | Cancelled`.
3. Nhiều phương thức thanh toán, mỗi loại mang thông tin riêng.
4. Nhiều chính sách giảm giá; thông báo qua email hoặc SMS; có thể thêm mới mà không sửa service.

Ánh xạ sang Scala:

| Yêu cầu | Công cụ |
|---|---|
| (1) invariant | `require` trong case class + smart constructor `Order.create` |
| (2) máy trạng thái | `enum OrderStatus` + `canTransitionTo` + `transitionTo` trả đối tượng mới |
| (3) dạng dữ liệu khác nhau | `enum PaymentMethod` có tham số |
| (4) mở rộng hành vi | `trait DiscountPolicy`, `abstract class Notifier`, composition trong `CheckoutService` |

## 7. Code đầy đủ

- [`OrderDomain.scala`](code/src/main/scala/scalaeng/ch05/OrderDomain.scala) — `OrderStatus`, `PaymentMethod`, `Money`, `OrderLine`, `Order`.
- [`Policies.scala`](code/src/main/scala/scalaeng/ch05/Policies.scala) — `DiscountPolicy` và 3 implementation, `Notifier`, `Auditable`, `RequestCounter`, `CheckoutService`, `@main ch05Demo`.

Trích đoạn cốt lõi:

```scala
enum OrderStatus:
  case Pending, Paid, Shipped, Cancelled

  def canTransitionTo(next: OrderStatus): Boolean = (this, next) match
    case (Pending, Paid) | (Pending, Cancelled) => true
    case (Paid, Shipped) | (Paid, Cancelled)    => true
    case _                                      => false

final case class Money(amount: BigDecimal):
  require(amount >= 0, s"Số tiền không được âm: $amount")
  def +(other: Money): Money = Money(amount + other.amount)
  def -(other: Money): Money = Money((amount - other.amount).max(0))
  def *(quantity: Int): Money = Money(amount * quantity)
  def >=(other: Money): Boolean = amount >= other.amount

final case class Order(id: String, customerId: String, lines: List[OrderLine],
                       status: OrderStatus, payment: PaymentMethod):
  def subtotal: Money = Money(lines.map(_.total.amount).sum)

  def transitionTo(next: OrderStatus): Order =
    require(status.canTransitionTo(next), s"Không thể chuyển $status -> $next")
    copy(status = next)

object Order:
  def create(id: String, customerId: String, lines: List[OrderLine], payment: PaymentMethod): Order =
    require(lines.nonEmpty, "Đơn hàng phải có ít nhất 1 dòng")
    Order(id, customerId, lines, OrderStatus.Pending, payment)

final class PercentageDiscount(percent: Int) extends DiscountPolicy:
  require(percent >= 0 && percent <= 100, s"percent ngoài [0, 100]: $percent")
  val name = s"Giảm $percent%"
  def discount(order: Order): Money =
    Money((order.subtotal.amount * percent / 100).setScale(0, BigDecimal.RoundingMode.DOWN))

final class CheckoutService(policy: DiscountPolicy, notifier: Notifier) extends Auditable:
  val createdBy = "checkout-service"
  def checkout(order: Order, contact: String): (Money, String) =
    val payable = order.subtotal - policy.discount(order)
    val receipt = notifier.send(contact,
      s"Đơn ${order.id}: ${payable.amount} VND (${policy.name}, ${order.payment.label})")
    (payable, receipt)
```

## 8. Giải thích code

- `(this, next) match` — so khớp trên **tuple** để mô tả máy trạng thái như một bảng; `|` gộp nhiều pattern cùng kết quả.
- `Money` định nghĩa các toán tử `+`, `-`, `*`, `>=` như method bình thường. Trong Scala, `a + b` chính là `a.+(b)`.
  `-` kẹp kết quả về 0 để giữ invariant "tiền không âm" (giảm giá lớn hơn tổng không làm âm số phải trả).
- `Money` vẫn là wrapper có chi phí cấp phát đối tượng; Chương 8 thay bằng **opaque type** không tốn chi phí runtime.
- `Order.subtotal` là `def` (tính mỗi lần gọi) chứ không phải `val` — đơn giản và luôn đúng vì `lines` bất biến.
- `transitionTo` dùng `copy` → đơn gốc không đổi (output: `Sau thanh toán: Paid; đơn gốc vẫn là: Pending`).
- `object NoDiscount extends DiscountPolicy` — chính sách không có trạng thái thì là singleton.
- `val name = ...` cài đặt `def name: String` của trait: trong Scala một `val` có thể cài đặt một `def` không tham số.
- `SmsNotifier(maxLength = 20)` — named argument làm lời gọi tự giải thích.
- `CheckoutService(p, EmailNotifier())` trong vòng lặp — cùng một service ghép với 3 policy khác nhau, không lớp con nào.
- Notifier **trả về chuỗi** thay vì gửi thật: giữ cho ví dụ test được. Gửi email thật là side effect — Chương 7 và 16 sẽ xử lý.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch05.ch05Demo
Pending
Tạm tính: 495000
Sau thanh toán: Paid; đơn gốc vẫn là: Pending
Pending -> Shipped hợp lệ? false
Các trạng thái: Pending, Paid, Shipped, Cancelled
Parse từ chuỗi: Paid
Không giảm giá: 495000
  [email] To: an@example.com | Đơn ORD-001: 495000 VND (Không giảm giá, Thẻ ****4242)
Giảm 10%: 445500
  [email] To: an@example.com | Đơn ORD-001: 445500 VND (Giảm 10%, Thẻ ****4242)
Giảm 50000 cho đơn từ 400000: 445000
  [email] To: an@example.com | Đơn ORD-001: 445000 VND (Giảm 50000 cho đơn từ 400000, Thẻ ****4242)
[sms] 0912345678: Đơn ORD-001: 495000 
tạo bởi checkout-service
Counter: 3
OrderLine(AO-01,Money(120000),2)
true
Lỗi: requirement failed: Số lượng phải > 0: 0
```

Chú ý dòng `true`: hai `OrderLine` tạo riêng biệt nhưng **bằng nhau theo giá trị** — đặc tính của case class.

## 10. Lỗi thường gặp

Thông báo dưới đây là output thật của `scalac 3.9.0`.

**Lỗi 1 — tạo đối tượng từ abstract class:**
```scala
abstract class Notifier(val channel: String):
  def send(msg: String): String
val n = Notifier("x")
```
```
-- [E006] Not Found Error: A.scala:4:10
4 |  val n = Notifier("x")
  |          ^^^^^^^^
  |          Expected a term, but found a type: Notifier
```
Thông báo hơi khó hiểu: creator application không áp dụng cho abstract class, nên compiler không tìm thấy "giá trị" nào tên `Notifier`. Cần tạo từ một lớp con cụ thể.

**Lỗi 2 — ghi đè method đã có cài đặt mà thiếu `override`:**
```scala
trait P:
  def name: String = "base"
class Q extends P:
  def name: String = "q"
```
```
-- [E164] Declaration Error: B.scala:4:6
4 |  def name: String = "q"
  |      ^
  |      error overriding method name in trait P of type => String;
  |        method name of type => String needs `override` modifier
```
Cài đặt member **abstract** không cần `override`; ghi đè member **đã có thân** thì bắt buộc. Đây là lưới an toàn chống ghi đè vô tình.

**Lỗi 3 — truy cập thành viên `private`:**
```
-- [E173] Reference Error: C.scala:5:12
5 |  val v = c.count
  |          ^^^^^^^
  |variable count cannot be accessed as a member of (C.c : Counter) from object C.
  |  private variable count can only be accessed from class Counter.
```

**Lỗi 4 — `match` trên enum thiếu trường hợp (cảnh báo, không phải lỗi):**
```
-- [E029] Pattern Match Exhaustivity Warning: D.scala:4:33
4 |  def label(s: Status): String = s match
  |                                 ^
  |                                 match may not be exhaustive.
  |                                 It would fail on pattern case: Shipped
```
Khi thêm một case mới vào enum, compiler chỉ ra **mọi** `match` cần cập nhật. Đừng thêm `case _ =>` chỉ để tắt cảnh báo — bạn sẽ mất lưới an toàn này.
Nên bật `-Werror` trong CI để cảnh báo này làm fail build (Chương 9).

**Lỗi 5 — invariant vi phạm lúc chạy:** `IllegalArgumentException: requirement failed: Số lượng phải > 0: 0`. Đúng như thiết kế; vấn đề là người gọi
không biết trước hàm có thể ném. Chương 7 xử lý.

**Lỗi thiết kế:** "God class" `OrderService` 2.000 dòng làm mọi thứ; cây kế thừa sâu 4–5 tầng; dùng `class` với setter thay vì case class + `copy`.

## 11. Cách debug

- `toString` của case class in rõ toàn bộ field — `println(order)` là công cụ debug nhanh nhất.
- Kiểm tra equality: nếu `Set` chứa "trùng" hai đối tượng, đó có phải `class` thường (so sánh theo tham chiếu) thay vì case class?
- Lỗi lúc khởi tạo `object` (exception trong thân object) xuất hiện dưới dạng `ExceptionInInitializerError` ở **lần đầu dùng** object — đọc phần `Caused by` trong stack trace.
- Với máy trạng thái, viết test liệt kê **toàn bộ** cặp chuyển hợp lệ (xem test "máy trạng thái đầy đủ") — phát hiện ngay khi ai đó vô tình mở thêm đường chuyển.

## 12. Bài tập

1. **(Cơ bản)** Viết `enum Priority(val weight: Int)` với `Low(1)`, `Medium(5)`, `High(10)`; viết `highest(ps: List[Priority]): Priority` (list không rỗng — dùng `require`).
2. **(Cơ bản)** Viết `final case class Address(street: String, ward: String, district: String, city: String)` với method `full: String`.
   Dùng `copy` để đổi phường. Viết test chứng minh đối tượng gốc không đổi.
3. **(Trung bình)** Viết `trait ShippingPolicy { def fee(subtotal: Money): Money }` với hai cài đặt: `FlatRate(fee: Money)` và `FreeOver(threshold: Money, fee: Money)`.
4. **(Trung bình)** Viết `final class Inventory(initial: Map[String, Int])` đóng gói tồn kho mutable:
   `available(sku): Int`, `reserve(sku, qty): Boolean` (chỉ trừ khi đủ hàng, `qty > 0`).
5. **(Nâng cao)** Mở rộng máy trạng thái: thêm `Refunded`, chỉ chuyển được từ `Shipped`. Viết `allowedNext(s: OrderStatus): Set[OrderStatus]`
   **suy ra** từ `canTransitionTo` và `OrderStatus.values` (không viết tay danh sách). Viết test cho toàn bộ bảng chuyển trạng thái.

## 13. Đáp án và lời giải

- Code: [`Ch05Solutions.scala`](../solutions/code/src/main/scala/scalaeng/solutions/Ch05Solutions.scala) · Test: [`Ch05SolutionsSuite.scala`](../solutions/code/src/test/scala/scalaeng/solutions/Ch05SolutionsSuite.scala)
- Giải thích: [`solutions/ch05-solutions.md`](../solutions/ch05-solutions.md)

## 14. Câu hỏi kiểm tra

1. `class A(x: Int)` và `class A(val x: Int)` khác nhau thế nào?
2. Companion object được dùng cho những việc gì? Nó có quyền gì đặc biệt?
3. Khi nào chọn `abstract class` thay vì `trait`?
4. Case class tự sinh những gì? Vì sao không nên dùng `var` trong case class?
5. `enum` đơn giản và enum có tham số khác nhau thế nào về mặt mô hình hóa?
6. Vì sao không nên thêm `case _ =>` vào mọi `match` trên enum?
7. Nêu hai lý do composition dễ test hơn inheritance.

## 15. Mini project — Domain model đơn hàng mở rộng

Bổ sung vào domain của chương:
- `ShippingPolicy` (bài 3) vào `CheckoutService`; số phải trả = subtotal − giảm giá + phí ship.
- `enum Voucher` với `Percent(p)`, `FixedAmount(m)`, `FreeShipping`; viết hàm chuyển `Voucher` thành cặp (`DiscountPolicy`, `ShippingPolicy`).
- Test cho mọi tổ hợp voucher × đơn hàng dưới/trên ngưỡng.

## 16. Nguồn tài liệu chính thức

- Scala 3 Book — Domain Modeling: https://docs.scala-lang.org/scala3/book/domain-modeling-intro.html
- Domain Modeling: OOP: https://docs.scala-lang.org/scala3/book/domain-modeling-oop.html
- Domain Modeling: Tools (class, object, trait, enum, case class): https://docs.scala-lang.org/scala3/book/domain-modeling-tools.html
- Scala 3 Reference — Enumerations: https://docs.scala-lang.org/scala3/reference/enums/enums.html
- Scala 3 Reference — Trait Parameters: https://docs.scala-lang.org/scala3/reference/other-new-features/trait-parameters.html
- Scala 3 Reference — Creator Applications: https://docs.scala-lang.org/scala3/reference/other-new-features/creator-applications.html

## 17. Checklist tự đánh giá

- [ ] Tôi viết được class với primary constructor, field `val`, tham số riêng.
- [ ] Tôi dùng companion object cho factory và smart constructor.
- [ ] Tôi phân biệt được trait và abstract class, và biết dùng mixin.
- [ ] Tôi mặc định dùng `final case class` cho dữ liệu và `copy` thay cho setter.
- [ ] Tôi mô hình hóa được tập giá trị hữu hạn bằng `enum`, kể cả enum có tham số.
- [ ] Tôi hiểu cảnh báo exhaustivity và không tắt nó bằng `case _`.
- [ ] Tôi thiết kế service bằng composition và test được bằng implementation giả.
