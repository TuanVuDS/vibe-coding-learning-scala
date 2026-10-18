# Lời giải — Chương 5: OOP

> **Code:** [`code/src/main/scala/scalaeng/solutions/Ch05Solutions.scala`](code/src/main/scala/scalaeng/solutions/Ch05Solutions.scala) ·
> **Test:** [`Ch05SolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch05SolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 5/5 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).

## Bài 1 — enum có tham số

```scala
enum Priority(val weight: Int):
  case Low    extends Priority(1)
  case Medium extends Priority(5)
  case High   extends Priority(10)

def highest(ps: List[Priority]): Priority =
  require(ps.nonEmpty, "danh sách rỗng")
  ps.maxBy(_.weight)
```

Mỗi case phải `extends Priority(...)` để truyền tham số. `require` trước `maxBy` vì `maxBy` ném `UnsupportedOperationException` với list rỗng —
`require` cho thông điệp rõ ràng hơn. Chương 7 có cách tốt hơn: trả `Option[Priority]` (hoặc `maxByOption`).

## Bài 2 — case class và `copy`

```scala
final case class Address(street: String, ward: String, district: String, city: String):
  def full: String = List(street, ward, district, city).filter(_.nonEmpty).mkString(", ")
```

`filter(_.nonEmpty)` tránh chuỗi `"12 Lê Lợi, , Quận 1"` khi thiếu phường. Test chứng minh `a.copy(ward = ...)` không đổi `a`.

## Bài 3 — trait `ShippingPolicy`

`FlatRate` và `FreeOver` là `case class` (không phải `class`) vì chúng là **dữ liệu cấu hình**: so sánh theo giá trị, in ra đọc được —
hữu ích khi log cấu hình đang chạy. `List[ShippingPolicy]` gọi `fee` đa hình trên cả hai.

## Bài 4 — `Inventory`

```scala
final class Inventory(initial: Map[String, Int]):
  private val stock = scala.collection.mutable.Map.from(initial)
  def available(sku: String): Int = stock.getOrElse(sku, 0)
  def reserve(sku: String, qty: Int): Boolean =
    if qty <= 0 || available(sku) < qty then false
    else
      stock.update(sku, available(sku) - qty)
      true
```

- `Map.from(initial)` **sao chép** dữ liệu — thay đổi bên trong không ảnh hưởng `Map` truyền vào.
- `private val stock`: bên ngoài không thể sửa trực tiếp; mọi thay đổi đi qua `reserve` → invariant "không âm" luôn đúng.
- **Không an toàn đa luồng**: hai request đồng thời có thể cùng đọc `available = 5` và cùng giữ 3. Chương 16 giải quyết bằng `Ref`/khóa; Chương 15 bằng transaction DB.
- Trả `Boolean` làm mất lý do thất bại (hết hàng hay qty sai?). Chương 7 thay bằng `Either`.

## Bài 5 — máy trạng thái suy ra

```scala
def allowedNext(s: Status): Set[Status] = Status.values.filter(s.canTransitionTo).toSet
```

Chỉ có **một nguồn sự thật** là `canTransitionTo`. Nếu viết tay `allowedNext` riêng, hai nơi sẽ lệch nhau theo thời gian.
`s.canTransitionTo` được truyền thẳng làm hàm nhờ eta-expansion (Chương 4).

## Câu hỏi kiểm tra — gợi ý

1. `class A(x: Int)`: `x` chỉ dùng bên trong; `class A(val x: Int)`: `x` là field công khai.
2. Factory, hằng số, smart constructor, `apply`/`unapply`, type class instance (Chương 8B). Truy cập được thành viên `private` của class đồng hành.
3. Khi cần gọi/kế thừa từ Java dễ dàng, hoặc muốn ràng buộc chỉ một lớp cha.
4. `equals`/`hashCode` theo giá trị, `toString`, `copy`, `apply`, `unapply`, field `val`. `var` làm `hashCode` đổi khi đối tượng đang nằm trong `Set`/`Map` → không tìm thấy nữa.
5. Enum đơn giản: tập hằng; enum có tham số: mỗi trường hợp mang dữ liệu khác nhau (sum type).
6. `case _` tắt kiểm tra exhaustivity; thêm case mới vào enum sẽ không được compiler nhắc ở các `match` cũ.
7. Thay implementation giả không cần lớp con của class thật; mỗi phụ thuộc test độc lập; không phụ thuộc trạng thái kế thừa.
