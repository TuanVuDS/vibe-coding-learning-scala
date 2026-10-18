# Lời giải — Chương 15: Database Engineering (PostgreSQL + Flyway + Doobie)

> Code: [`ch15-code/`](ch15-code/) — dùng lại mã nguồn, migration và `PostgresSuite` của `06-database/db-lab` qua `//> using file`.
> Chạy: `cd solutions/ch15-code && scala-cli test .` → **7/7 test pass** trên PostgreSQL 18.6 thật (nhúng), 17/10/2026 (chạy lại sau review chéo).

## Bài 1 — Top sản phẩm bán chạy

```scala
sql"""select p.sku, p.name, sum(i.quantity)::bigint, sum(i.quantity * i.unit_price_vnd)::bigint
      from order_items i
      join orders o   on o.id = i.order_id
      join products p on p.id = i.product_id
      where o.status <> 'CANCELLED'
      group by p.sku, p.name
      order by 3 desc, p.sku
      limit $n""".query[Row].to[List]
```

- Doanh thu dùng `unit_price_vnd` (giá lúc mua), **không** `products.price_vnd` (giá hiện tại).
- `sum(...)` của `int` trong PostgreSQL trả `bigint`, của `bigint` trả `numeric`; `::bigint` ép tường minh về kiểu khớp `Long` trong `Row`,
  để kiểu cột không phụ thuộc vào việc driver JDBC tự chuyển `numeric` sang `long`.
- `order by 3 desc, p.sku`: hòa số lượng thì theo sku → kết quả xác định, test không chập chờn.
- Test: 3 đơn, đơn thứ ba (5 tai nghe) bị hủy → tai nghe chỉ còn 1 chiếc, rơi khỏi top 2.

## Bài 2 — Migration V3

```sql
alter table orders add column cancelled_at timestamptz;
alter table orders add constraint orders_cancelled_consistent
  check ((status = 'CANCELLED') = (cancelled_at is not null));
```

- Không sửa V1/V2: Flyway lưu checksum; sửa file đã chạy → `validate` thất bại ở mọi môi trường đã chạy file cũ.
- Ràng buộc so sánh hai biểu thức boolean: đúng khi **cả hai cùng đúng hoặc cùng sai**.
- Đặt V3 ở `classpath:db/solutions` và cấu hình `locations("classpath:db/migration", "classpath:db/solutions")`: Flyway gộp các thư mục, sắp theo version.
- Bảng lớn có dữ liệu: `add constraint ... not valid` (không kiểm tra dữ liệu cũ, khóa ngắn) rồi `validate constraint` riêng.
- Test: `flyway_schema_history` có `1, 2, 3`; insert đơn `CANCELLED` không có `cancelled_at` bị từ chối với tên ràng buộc `orders_cancelled_consistent`.

## Bài 3 — Hủy đơn an toàn khi đồng thời

```scala
sql"""update orders set status = 'CANCELLED', cancelled_at = now()
      where id = $id and status = 'PENDING'""".update.run.flatMap {
  case 1 => /* hoàn kho */ ...
  case _ => /* phân biệt NotFound / NotCancellable */ ...
}
```

- `where ... and status = 'PENDING'` là "compare-and-set" của SQL. Hai transaction cùng hủy: transaction thứ hai **chờ khóa dòng**; khi thứ nhất
  commit, PostgreSQL (READ COMMITTED) đánh giá lại điều kiện trên phiên bản mới của dòng → `status` đã là `CANCELLED` → cập nhật **0 dòng** → không hoàn kho lần hai.
- Hoàn kho bằng `update ... from order_items` trong **cùng** transaction: hoặc cả trạng thái và kho cùng đổi, hoặc không.
- Test đồng thời (`parTupled` hai lần hủy): đúng một kết quả `Cancelled`, kho trở về 10.
- Sai lầm thường gặp: `select status` → kiểm tra trong Scala → `update` — lại là lost update.

## Bài 4 — Keyset pagination

```scala
val cond = after.map(c => fr"where (created_at, id) > (${c.createdAt}, ${c.id})").getOrElse(Fragment.empty)
(fr"select ... from users" ++ cond ++ fr"order by created_at, id limit ${size + 1}")
```

- So sánh **bộ** `(created_at, id) > (?, ?)` của PostgreSQL: đúng thứ tự từ điển, gồm cả trường hợp trùng `created_at`.
- Lấy `size + 1` dòng: dòng thừa cho biết còn trang sau mà không cần `count(*)`.
- **Tiền điều kiện** `1 <= size <= 100`, trả `Left` nếu sai: `size = 0` làm `page.last` ném exception, `size = Int.MaxValue` làm `size + 1` tràn số.
- Test chèn 5 user trong **một** câu lệnh nên cả 5 có cùng `created_at` (test khẳng định `count(distinct created_at) = 1`) — chỉ `id` phân biệt thứ tự,
  đúng trường hợp so sánh bộ `(created_at, id)` phải xử lý.
- Cursor trả cho client nên là chuỗi **mờ** (opaque, ví dụ base64 của JSON) để client không phụ thuộc định dạng nội bộ. Base64 **không** chống sửa:
  client vẫn giải mã, sửa và mã hóa lại được — server phải validate cursor sau khi giải mã. Muốn chống sửa thật sự thì ký cursor (HMAC) hoặc dùng
  token ngẫu nhiên lưu phía server, và vẫn kiểm tra quyền truy cập dữ liệu (review chéo, m1).
- Cần index `(created_at, id)` khi bảng lớn (bài tập mở rộng: kiểm tra bằng `EXPLAIN`).

## Bài 5 — Sửa lost update không cần FOR UPDATE

```scala
sql"update products set stock = stock - 1 where id = $id and stock >= 1".update.run.map(_ == 1)
```

Vì sao đúng ở READ COMMITTED: UPDATE luôn làm việc trên phiên bản **mới nhất** của dòng. Nếu một transaction khác đang sửa dòng, UPDATE chờ;
sau khi bên kia commit, điều kiện `stock >= 1` được **kiểm tra lại** trên giá trị mới (0) → 0 dòng. Kiểm tra và ghi là một thao tác nguyên tử
trong database, không còn khoảng hở "đọc ở ứng dụng rồi ghi". Test: hai mua đồng thời → `List(false, true)`, stock 0.

Giới hạn: chỉ dùng được khi điều kiện diễn đạt được trong SQL. Nếu cần tính giá/khuyến mãi phức tạp ở Scala trước khi ghi, dùng `FOR UPDATE` như `OrderService`.

## Bài 6 — Index cho "10 đơn mới nhất của user"

```sql
create index orders_user_created_idx on orders (user_id, created_at desc, id desc);
```

Truy vấn: `where user_id = ? order by created_at desc, id desc limit 10`. Index có `user_id` trước (lọc bằng), rồi đúng thứ tự sắp xếp → PostgreSQL đọc
10 entry đầu tiên của phần index ứng với user 42 và dừng; **không cần node `Sort`**. Test nạp 50.000 đơn của 500 user, `analyze`, rồi khẳng định plan
dùng `orders_user_created_idx` và không chứa `Sort`.

Index `orders_user_id_idx` (chỉ `user_id`, có từ V1) vẫn lọc được nhưng phải sắp xếp các đơn của user đó. Sau V3 có thể xóa index cũ vì index mới
có cùng tiền tố `user_id` — đó là một migration V4 (bài tập mở rộng).

## Ghi chú kiểm chứng

- `using file` đưa `PostgresSuite.scala` (nằm trong thư mục test của lab) vào scope **main** của project lời giải, nên munit/zonky được khai báo
  bằng `dep` thay vì `test.dep`. Lần đầu khai báo `test.dep` cho kết quả `Not found: munit` khi biên dịch `PostgresSuite`.
