# Lời giải — Chương 8A và 8B

> **Code:** [`Ch08aSolutions.scala`](code/src/main/scala/scalaeng/solutions/Ch08aSolutions.scala), [`Ch08bSolutions.scala`](code/src/main/scala/scalaeng/solutions/Ch08bSolutions.scala)
> **Test:** [`Ch08aSolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch08aSolutionsSuite.scala), [`Ch08bSolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch08bSolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 10/10 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).
> Lời giải 8B dùng `Monoid` của bài học; một bản sao được đặt ở `code/src/main/scala/scalaeng/ch08b/Monoids.scala` để project lời giải chạy độc lập.

## 8A — Bài 1: `Result[A]`

`map` đổi kiểu dữ liệu nhưng giữ cảnh báo; `addWarning` dùng `copy`. Đây là dạng đơn giản của kiểu "Writer" — mang theo log bên cạnh kết quả.

## 8A — Bài 2: opaque `Email`

```scala
s.split("@", -1) match
  case Array(local, domain) if local.nonEmpty && domain.contains('.')
                               && !domain.startsWith(".") && !domain.endsWith(".") => Right(s)
  case _ => Left(s"Email không hợp lệ: '$raw'")
```

- `split("@", -1)` + pattern `Array(local, domain)` đảm bảo **đúng một** `@` (`a@b@c.vn` cho 3 phần → không khớp).
- Chuẩn hóa (trim, lowercase) **trong** smart constructor: mọi `Email` tồn tại đều đã chuẩn hóa — so sánh hai email không cần nhớ chuẩn hóa lại.
- Đây là kiểm tra hợp lý cho bài tập, không phải validator RFC 5322 đầy đủ. Trong thực tế cách xác thực email đáng tin nhất là gửi mail xác nhận.

## 8A — Bài 3: opaque `Percent`

`Math.multiplyExact(amount, p)` ném `ArithmeticException` khi tràn `Long` thay vì trả số âm âm thầm (Chương 3). Test chứng minh với `Long.MaxValue`.
Nhân trước rồi chia (`amount * p / 100`) để không mất độ chính xác; chia trước (`amount / 100 * p`) sẽ sai với số nhỏ.

## 8A — Bài 4: `maxByField`

`rows.maxByOption(_.updatedAt)` trả `Option[A]` — kiểu cụ thể `Row` được giữ nhờ upper bound, test gán thẳng vào `Option[Row]`.
`maxByOption` an toàn với list rỗng (khác `maxBy`, Chương 6).

## 8A — Bài 5: `Sink[-A]`

`Sink[Event]` biết ghi **mọi** event, nên dùng được ở chỗ cần `Sink[OrderPlaced]`. Nếu `Sink` là covariant, ta có thể dùng một `Sink[OrderPlaced]`
(chỉ biết ghi đơn hàng) ở chỗ cần `Sink[Event]` rồi đưa cho nó `UserSignedUp` — lỗi. Compiler ngăn điều đó: tham số `a: A` của `write` là vị trí contravariant.
Khi gọi `connect(p, s)`, compiler chọn `A = OrderPlaced`: `Pipeline[OrderPlaced]` khớp trực tiếp, `Sink[Event] <: Sink[OrderPlaced]` nhờ contravariance.

## 8B — Bài 1: `Show`

Instance cho `Int`, `String`, `Boolean` và conditional instance cho `List[A]` đặt trong **companion của type class** (`object Show`) → tìm được tự động.

## 8B — Bài 2: `Ordering[Version]`

```scala
object Version:
  given Ordering[Version] = Ordering.by(v => (v.major, v.minor, v.patch))
```

`Ordering` cho tuple so sánh từng thành phần theo thứ tự — `1.10.0 > 1.2.3` (so sánh số, không phải chuỗi). Sắp xếp chuỗi `"1.10.0" < "1.2.3"` là lỗi kinh điển.

## 8B — Bài 3: `Monoid[Option[A]]`, `Monoid[(A, B)]`

`None` là phần tử trung hòa; `Some(a) combine Some(b) = Some(a combine b)`.

**Bài học thật từ quá trình biên dịch lời giải:** test ban đầu chỉ có `import Ch08bSolutions.*` và bị lỗi:

```
No given instance of type scalaeng.ch08b.Monoid[Option[Int]] was found for parameter m of method combineAll in package scalaeng.ch08b
Note: given instance optionMonoid in object Ch08bSolutions was not considered because it was not imported with `import given`.
```

Vì `optionMonoid` nằm trong `object Ch08bSolutions` — **không phải** companion của `Monoid` hay `Option` — nó không thuộc implicit scope, phải `import Ch08bSolutions.given`.
Đặt instance này trong `object Monoid` (companion của type class) thì không cần import. Đây chính là quy tắc đặt instance ở mục 3.3.

## 8B — Bài 4: `Validator`

`Option.when(điều kiện)(lỗi)` tạo `Some(lỗi)` khi điều kiện đúng; `.flatten` bỏ các `None` — cách gọn để thu thập lỗi độc lập.
Instance nằm trong `object Signup` nên `validateAll(xs)` tìm thấy không cần import.

## 8B — Bài 5: `aggregateByKey`

```scala
val partials = batches.map(batch => batch.groupMapReduce(_._1)(_._2)(m.combine))
partials.combineAll
```

Liên hệ Spark (Chương 22–23): `reduceByKey(f)` chạy `f` **cục bộ trên từng partition** trước (map-side combine), chỉ gửi kết quả gộp qua mạng (shuffle),
rồi gộp tiếp. Kết quả đúng **chỉ khi `f` kết hợp** (và giao hoán, vì thứ tự đến của các partition không xác định). Test "gộp từng phần = gộp một lần" là cách kiểm tra điều đó.

## Câu hỏi kiểm tra — gợi ý

**8A:** (1) bản generic giữ kiểu cụ thể, không cần ép kiểu ngược; (2) `List` covariant nên tham số phương thức không được có kiểu `A` — lower bound cho phép mở rộng kiểu;
(3) hàm tiêu thụ `A` và sinh ra `B`; (4) đọc + ghi: nếu covariant thì ghi được giá trị sai kiểu; (5) alias chỉ là tên, opaque là kiểu mới, cả hai không tốn runtime;
(6) union cho kết hợp tạm kiểu có sẵn, enum cho mô hình domain; (7) không sửa/bọc kiểu gốc; import để nguồn gốc cú pháp rõ ràng.

**8B:** (1) trait, instance, hàm có `using`; (2) để nguồn gốc given rõ ràng, tránh given "lén" vào phạm vi; (3) companion của kiểu — được tìm tự động, không orphan;
(4) `def f[A](...)(using Ordering[A])`; (5) avg(avg) ≠ avg tổng — lưu (sum, count) rồi chia ở cuối; (6) `using` cho ngữ cảnh xuyên suốt, constructor cho phụ thuộc nghiệp vụ;
(7) type class thêm hành vi cho kiểu không sở hữu, chọn instance theo kiểu lúc biên dịch, có conditional instance.
