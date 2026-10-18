# Chương 15 — Database Engineering cho Backend: PostgreSQL + Flyway + Doobie

> **Phiên bản:** Scala 3.9.0 · JDK 21 · PostgreSQL **18.6** · Doobie **1.0.0-RC13** (`org.typelevel`) · Flyway 13.10.3 · PostgreSQL JDBC 42.7.14 ·
> HikariCP (qua `doobie-hikari`) · munit 1.2.4 · munit-cats-effect 2.2.0 · embedded-postgres 2.2.2 (binary PostgreSQL 18.6.0)
> **Code:** [`db-lab/`](db-lab/) — schema users/products/orders, migration, repository Doobie, transaction đặt hàng, thí nghiệm race, injection, query plan
> **Trạng thái kiểm chứng:** Phiên bản tra trên `repo1.maven.org` ngày 16/10/2026. **18/18 test pass** (`scala-cli test .` và `sbt test`) trên
> **PostgreSQL 18.6 thật** (chạy nhúng, không cần Docker); lời giải 7/7. Mọi query plan, thông báo lỗi PostgreSQL và output trong bài là output thật.
> Review chéo của Codex tìm ra 4 lỗi lớn (bắt lỗi SQL làm hỏng transaction, tràn số, lệch phiên bản munit, diễn đạt sai về REPEATABLE READ) — đã sửa,
> mỗi lỗi có test tái hiện trên PostgreSQL thật.
> `docker-compose.yml`: **đã kiểm chứng 18/10/2026** với Docker Desktop (engine 28.5.1): `postgres:18.6` healthy, `DbDemo` chạy với container thật (output ở mục 9).

---

## Vị trí trong lộ trình

Chương 13 chọn stack; Chương 14 xây REST API với repository **trong bộ nhớ** đứng sau một trait. Chương này thay bộ nhớ bằng
**PostgreSQL**: dữ liệu bền vững, ràng buộc toàn vẹn, transaction, đồng thời giữa nhiều request, hiệu năng truy vấn.
Project 2 ghép Chương 14 (HTTP) với Chương 15 (database). Nhánh Data Engineering cũng cần chương này: SQL, index và query plan
là nền tảng của Spark SQL (Chương 23) và kho dữ liệu.

## 1. Mục tiêu học tập

1. Thiết kế schema quan hệ có khóa chính, khóa ngoại, ràng buộc `CHECK`/`UNIQUE`, và giải thích vì sao **database là tuyến phòng thủ cuối**.
2. Quản lý thay đổi schema bằng **migration** (Flyway) thay vì sửa tay.
3. Truy vấn PostgreSQL từ Scala 3 bằng Doobie: `sql"..."`, `ConnectionIO`, `transact`, ánh xạ kiểu (opaque type, enum).
4. Đặt **ranh giới transaction** ở tầng service; chứng minh atomicity bằng test rollback.
5. Tái hiện và sửa **lost update** khi hai request cùng mua món hàng cuối — bằng `SELECT ... FOR UPDATE`, UPDATE có điều kiện, hoặc isolation level.
6. Chứng minh SQL injection và chặn bằng truy vấn tham số hóa.
7. Đọc `EXPLAIN` / `EXPLAIN ANALYZE`, thiết kế index theo truy vấn.
8. Cấu hình connection pool (HikariCP) và test với PostgreSQL thật.

## 2. Kiến thức tiên quyết

- Chương 7 (Either, ADT), Chương 8A (opaque type), Chương 8B (`given`) — ánh xạ kiểu dùng `given Meta[...]`.
- Chương 16A (race condition, `Thread.yield` mở rộng cửa sổ race) và Chương 16B (`IO`, `Resource`, `parTupled`).
- Chương 12–14 (domain users/orders, Problem JSON).
- Không yêu cầu biết SQL trước; mục 3.1 tóm tắt phần cần dùng.

## 3. Lý thuyết

### 3.1. SQL tối thiểu cho backend

**Vấn đề.** Ứng dụng cần lưu dữ liệu lâu dài, truy vấn linh hoạt, và nhiều request cùng đọc/ghi mà không làm hỏng dữ liệu.
Database quan hệ giải quyết bằng: bảng (table) có kiểu cột, ngôn ngữ truy vấn khai báo (SQL), ràng buộc, transaction.

| Nhóm | Câu lệnh | Dùng trong lab |
|---|---|---|
| Định nghĩa (DDL) | `create table`, `alter table`, `create index` | migration V1, V2, V3 |
| Thao tác (DML) | `insert`, `update`, `delete` | `UserRepo.create`, `ProductRepo.decrementStock` |
| Truy vấn | `select ... from ... join ... where ... group by ... order by ... limit` | `UserRepo.list`, `TopSellers.top` (lời giải bài 1) |
| Transaction | `begin`, `commit`, `rollback`, `select ... for update` | `OrderService.placeOrder` |

SQL là **khai báo**: bạn nói *muốn gì* (`where user_id = 42`), planner của PostgreSQL quyết định *làm thế nào* (quét tuần tự hay dùng index — mục 3.7).

### 3.2. Thiết kế schema: khóa, ràng buộc, kiểu dữ liệu

Schema của lab (`V1__users_products_orders.sql`):

```
users (id PK, email, name, created_at)          unique index trên lower(email)
products (id PK, sku UNIQUE, name, price_vnd CHECK > 0, stock CHECK >= 0)
orders (id PK, user_id FK -> users, status CHECK in (...), total_vnd, created_at)   index trên user_id
order_items (order_id FK -> orders, product_id FK -> products, quantity CHECK > 0, unit_price_vnd)  PK (order_id, product_id)
```

**Quyết định và lý do:**

| Quyết định | Vì sao | Lựa chọn thay thế / đánh đổi |
|---|---|---|
| `id bigint generated always as identity` | Khóa nhân tạo, ổn định, nhỏ, PostgreSQL tự sinh | UUID: sinh được ở client, không lộ số lượng; nhưng to hơn, index kém cục bộ |
| `unique index on lower(email)` | `An@X.vn` và `an@x.vn` là một người | Cột `citext` (extension); hoặc chuẩn hóa ở ứng dụng — nhưng ứng dụng có thể quên |
| `price_vnd bigint` | Tiền là số nguyên đơn vị nhỏ nhất — không lỗi làm tròn | `numeric(12,2)`: chính xác nhưng chậm hơn; **không bao giờ** `float`/`double` |
| `stock int check (stock >= 0)` | Kể cả khi code có bug, DB không cho tồn kho âm | Chỉ kiểm tra ở ứng dụng → một bug là dữ liệu hỏng vĩnh viễn |
| `unit_price_vnd` trong `order_items` | Giá **tại thời điểm mua**; đổi giá sau đó không làm sai đơn cũ | Join sang `products.price_vnd` — sai về nghiệp vụ |
| `status text check (status in (...))` | Đọc được bằng mắt, dễ thêm trạng thái bằng migration | Kiểu `enum` của PostgreSQL: chặt hơn nhưng đổi tên/xóa giá trị khó |
| PK ghép `(order_id, product_id)` | Một sản phẩm xuất hiện tối đa một dòng trong một đơn | Cột `id` riêng + unique — thừa |
| `create index orders_user_id_idx` | **PostgreSQL không tự tạo index cho foreign key** | Không có index → "đơn của user X" quét toàn bảng |

**Database là tuyến phòng thủ cuối.** Validation ở ứng dụng (Chương 12) cho thông báo lỗi thân thiện; ràng buộc ở database
đảm bảo **mọi** đường ghi (ứng dụng khác, script sửa tay, bug) đều không phá dữ liệu. Test
`CHECK constraint chặn dữ liệu sai dù ứng dụng quên validate` chứng minh điều này.

**Lỗi thiết kế thường gặp:** lưu tiền bằng `double`; không có ràng buộc vì "ứng dụng đã kiểm tra"; dùng `varchar(255)` theo thói quen
(PostgreSQL `text` không chậm hơn); quên index cho foreign key; lưu nhiều giá trị trong một cột (`"1,2,3"`).

### 3.3. Migration với Flyway

**Vấn đề.** Schema thay đổi theo thời gian, trên nhiều môi trường (máy dev, CI, staging, production). Sửa tay bằng `psql` thì không ai
biết môi trường nào đang ở phiên bản nào.

**Cách hoạt động.** Mỗi thay đổi là một file `V<số>__<mô tả>.sql` trong `src/main/resources/db/migration`. Flyway:
1. Tạo bảng `flyway_schema_history` nếu chưa có.
2. So danh sách file với lịch sử; chạy **theo thứ tự** các file chưa chạy, mỗi file trong một transaction (PostgreSQL hỗ trợ DDL trong transaction).
3. Lưu checksum từng file. File đã chạy mà bị sửa → Flyway báo lỗi validate (đây là tính năng, không phải bug).

**Quy tắc:** không sửa migration đã chạy ở môi trường khác — thêm `V3`. Lời giải bài 2 thêm cột `cancelled_at` bằng V3 đặt ở thư mục khác,
chứng minh Flyway gộp được nhiều `locations`.

**Lựa chọn thay thế:** Liquibase (XML/YAML/SQL, có rollback khai báo); tự viết script. Flyway chọn vì đơn giản: migration là SQL thuần.

**Lỗi thường gặp:** chạy migration song song từ nhiều instance khi khởi động (Flyway có khóa, nhưng nên chạy migration như một bước deploy riêng
ở production); migration khóa bảng lớn lâu (`alter table ... add column ... default` trên bảng hàng trăm triệu dòng — kiểm tra tài liệu PostgreSQL
cho phiên bản của bạn); migration không tương thích ngược khi deploy rolling (xóa cột trước khi code mới chạy hết).

### 3.4. Doobie: SQL là giá trị

**Doobie** là thư viện JDBC thuần functional của hệ Typelevel. Ba khái niệm:

| Khái niệm | Ý nghĩa |
|---|---|
| `Fragment` (`sql"..."`, `fr"..."`) | Một đoạn SQL **kèm tham số**. `$email` trong `sql"..."` trở thành `?` + giá trị gắn riêng — **không** ghép chuỗi |
| `ConnectionIO[A]` | **Mô tả** các thao tác trên một kết nối, trả `A`. Chưa chạy gì |
| `Transactor[IO]` | Biết lấy kết nối (từ pool). `program.transact(xa)` chạy `ConnectionIO` trong **một transaction**: thành công → `commit`, lỗi → `rollback` |

```scala
def find(id: UserId): ConnectionIO[Option[User]] =
  sql"select id, email, name, created_at from users where id = $id".query[User].option
```

- `.query[User]` cần `Read[User]` — ánh xạ cột → field. `.option` (0 hoặc 1 dòng), `.unique` (đúng 1), `.to[List]` (nhiều).
- `.update.run` trả số dòng bị ảnh hưởng; `.withUniqueGeneratedKeys[User]("id", ...)` đọc lại dòng vừa insert (PostgreSQL `RETURNING`).

**Ánh xạ kiểu** (`Codecs.scala`):

```scala
given Meta[UserId]      = Meta[Long].timap(UserId(_))(_.value)          // opaque type: cùng biểu diễn Long
given Meta[OrderStatus] = Meta[String].tiemap(OrderStatus.fromDb)(_.dbName) // chuỗi lạ trong DB -> lỗi khi đọc
given Read[User]        = Read.derived                                   // derive từ case class
```

**Bắt lỗi SQL bên trong transaction — cẩn thận.** Bản đầu của `UserRepo.create` dùng `.attemptSomeSqlState { case UNIQUE_VIOLATION => ... }`
để đổi lỗi email trùng thành `Left`. Phía Scala trông ổn, nhưng **phía PostgreSQL transaction đã bị abort** sau lỗi 23505: mọi câu lệnh tiếp theo trong
**cùng** transaction đều nhận `25P02 current transaction is aborted` (test `attemptSomeSqlState để lại transaction bị abort`). Vì repository trả
`ConnectionIO` để service **ghép** nhiều thao tác, đây là lỗi thật chứ không phải lý thuyết. Bản hiện tại để PostgreSQL tự bỏ qua dòng trùng:

```sql
insert into users (email, name) values (?, ?)
on conflict ((lower(email))) do nothing        -- conflict target là BIỂU THỨC của unique index
returning id, email, name, created_at          -- trùng → không trả dòng nào → None → DuplicateEmail
```

Cách khác: savepoint trước câu lệnh có thể lỗi, hoặc để exception thoát ra tới ranh giới transaction (rollback) rồi mới đổi thành lỗi nghiệp vụ.

**Bắt lỗi SQL bên trong transaction — cẩn thận.** Bản đầu của `UserRepo.create` dùng `.attemptSomeSqlState { case UNIQUE_VIOLATION => ... }`
để đổi lỗi email trùng thành `Left`. Phía Scala trông ổn, nhưng **phía PostgreSQL transaction đã bị abort** sau lỗi 23505: mọi câu lệnh tiếp theo trong
**cùng** transaction đều nhận `25P02 current transaction is aborted` (test `attemptSomeSqlState để lại transaction bị abort`). Vì repository trả
`ConnectionIO` để service **ghép** nhiều thao tác, đây là lỗi thật chứ không phải lý thuyết. Bản hiện tại để PostgreSQL tự bỏ qua dòng trùng:

```sql
insert into users (email, name) values (?, ?)
on conflict ((lower(email))) do nothing        -- conflict target là BIỂU THỨC của unique index
returning id, email, name, created_at          -- trùng → không trả dòng nào → None → DuplicateEmail
```

Cách khác: savepoint trước câu lệnh có thể lỗi, hoặc để exception thoát ra tới ranh giới transaction (rollback) rồi mới đổi thành lỗi nghiệp vụ.

**Bắt lỗi SQL bên trong transaction — cẩn thận.** Bản đầu của `UserRepo.create` dùng `.attemptSomeSqlState { case UNIQUE_VIOLATION => ... }`
để đổi lỗi email trùng thành `Left`. Phía Scala trông ổn, nhưng **phía PostgreSQL transaction đã bị abort** sau lỗi 23505: mọi câu lệnh tiếp theo trong
**cùng** transaction đều nhận `25P02 current transaction is aborted` (test `attemptSomeSqlState để lại transaction bị abort`). Vì repository trả
`ConnectionIO` để service **ghép** nhiều thao tác, đây là lỗi thật chứ không phải lý thuyết. Bản hiện tại để PostgreSQL tự bỏ qua dòng trùng:

```sql
insert into users (email, name) values (?, ?)
on conflict ((lower(email))) do nothing        -- conflict target là BIỂU THỨC của unique index
returning id, email, name, created_at          -- trùng → không trả dòng nào → None → DuplicateEmail
```

Cách khác: savepoint trước câu lệnh có thể lỗi, hoặc để exception thoát ra tới ranh giới transaction (rollback) rồi mới đổi thành lỗi nghiệp vụ.

**Vì sao repository trả `ConnectionIO` chứ không trả `IO`?** Để **tầng service quyết định transaction**. `OrderService.placeOrderTx` gộp
`UserRepo.find`, `ProductRepo.lockForUpdate`, `OrderRepo.insertOrder`, `insertItems`, `decrementStock` thành **một** `ConnectionIO`,
rồi `transact` một lần. Nếu mỗi repository tự `transact`, mỗi bước là một transaction riêng — mất atomicity.

> **Thay đổi quan trọng ở Doobie 1.0.0-RC13:** groupId **và package** đổi sang `org.typelevel`. Code ví dụ trên mạng `import doobie.*`
> sẽ báo `Not found: doobie` (mục 10). Doobie 1.0 vẫn là **Release Candidate** — chưa có bản 1.0.0 final (tra ngày 16/10/2026).

### 3.5. Transaction và ACID

| Tính chất | Nghĩa | Test trong lab |
|---|---|---|
| **A**tomicity | Tất cả hoặc không gì | `lỗi giữa chừng -> ROLLBACK toàn bộ`: insert đơn + trừ kho rồi ném lỗi → 0 đơn, kho nguyên 5 |
| **C**onsistency | Transaction đưa DB từ trạng thái hợp lệ sang trạng thái hợp lệ (ràng buộc luôn đúng) | `CHECK constraint chặn dữ liệu sai...` |
| **I**solation | Transaction đồng thời không thấy trạng thái dở dang của nhau — **ở mức độ tùy isolation level** | 3 test đồng thời (mục 3.6) |
| **D**urability | Đã commit thì không mất khi server sập | Không test được trong unit test; dựa vào WAL của PostgreSQL |

**Ranh giới transaction đặt ở đâu?** Ở **một use case** (đặt hàng, hủy đơn) — tầng service. Không mở transaction quanh cả request HTTP
có gọi dịch vụ ngoài (giữ kết nối và khóa quá lâu); không chia một use case thành nhiều transaction (mất atomicity).

### 3.6. Đồng thời: lost update và ba cách sửa

**Vấn đề.** Hai khách cùng mua món hàng **cuối cùng** (stock = 1). Code "hiển nhiên":

```scala
p  <- ProductRepo.readWithoutLock(id)               // cả hai đọc stock = 1
ok <- if p.stock >= 1 then ProductRepo.setStock(id, p.stock - 1).as(true) else false.pure  // cả hai ghi 0
```

PostgreSQL mặc định chạy ở **READ COMMITTED**: mỗi câu lệnh thấy dữ liệu đã commit tại lúc câu lệnh bắt đầu. Cả hai transaction đọc 1,
cả hai ghi 0, cả hai thành công: **bán 2 món khi kho có 1**, và stock = 0 trông như đúng — ràng buộc `CHECK (stock >= 0)` **không** bắt được lỗi này.
Đây là **lost update**: lần ghi thứ hai đè lên lần ghi thứ nhất.

Lab tái hiện bằng một **rào chắn** (`CountDownLatch(2)`) giữa đọc và ghi: mỗi transaction đọc xong thì chờ transaction kia cũng đọc xong, rồi mới ghi.
Nhờ vậy **chắc chắn** cả hai đọc giá trị cũ — bản đầu dùng `Thread.sleep(300)` chỉ làm điều đó *có khả năng* xảy ra, máy chậm có thể cho kết quả khác
(review chéo, m3). Hai transaction chạy bằng `parTupled`:

| Cách | Cơ chế | Kết quả test (thật) |
|---|---|---|
| Đọc-rồi-ghi, READ COMMITTED | Không có gì ngăn | `(true, true)`, stock = 0 → **bán quá** |
| Cùng code, **REPEATABLE READ** | Transaction thứ hai ghi vào **cùng dòng** đã bị transaction khác sửa sau snapshot của nó → PostgreSQL hủy | Một bên `Right(true)`, bên kia **SQLSTATE 40001** `could not serialize access due to concurrent update` |
| **`SELECT ... FOR UPDATE`** (`OrderService`) | Khóa dòng khi đọc; transaction thứ hai **chờ** đến khi thứ nhất commit, rồi đọc stock = 0 | Đúng 1 đơn; bên kia `OutOfStock(kb, 1, 0)` |
| **UPDATE có điều kiện** (lời giải bài 5) | `update ... set stock = stock - 1 where id = ? and stock >= 1` — kiểm tra và ghi trong **một** câu lệnh | Một bên cập nhật 1 dòng, bên kia 0 dòng |

**Khi nào dùng cách nào:**
- UPDATE có điều kiện: đơn giản, nhanh nhất, khi logic kiểm tra diễn đạt được trong SQL.
- `FOR UPDATE`: khi phải đọc nhiều dòng, tính toán ở ứng dụng (giá, khuyến mãi) rồi mới ghi. **Khóa theo thứ tự cố định** (`order by id`)
  để tránh deadlock khi hai đơn khóa cùng các sản phẩm theo thứ tự ngược nhau.
- **REPEATABLE READ** của PostgreSQL là *snapshot isolation*: chặn được lost update **trên cùng một dòng** (thí nghiệm trên), nhưng **vẫn cho phép**
  bất thường giữa nhiều dòng (*write skew*): hai transaction mỗi bên đọc dòng A và B, mỗi bên sửa một dòng khác nhau dựa trên giá trị cũ của dòng kia —
  cả hai commit thành công, không có 40001, bất biến nghiệp vụ vẫn bị phá. Đừng coi REPEATABLE READ là "đủ an toàn cho mọi logic phức tạp".
- **SERIALIZABLE** bảo đảm kết quả tương đương một thứ tự chạy tuần tự nào đó, đổi lại nhiều transaction bị hủy (40001) hơn.
- Cả hai mức đều bắt ứng dụng **retry toàn bộ transaction** khi gặp 40001 (dùng `Retry` của Chương 16B). Với bất biến nhiều dòng, cân nhắc
  khóa tường minh (`FOR UPDATE`), ràng buộc của database, hoặc SERIALIZABLE + retry.

**Lỗi thiết kế:** dùng `synchronized`/`Ref` trong ứng dụng để "khóa" — chỉ đúng khi có **một** instance; scale lên 2 instance là hỏng.
Khóa phải nằm ở nơi dữ liệu nằm: database.

### 3.7. Index và query plan

**Vấn đề.** `select count(*) from page_views where user_id = 42 and created_at >= '2026-10-20'` trên 200.000 dòng.
Không có index, PostgreSQL phải đọc **mọi** dòng.

**`EXPLAIN`** cho xem kế hoạch; **`EXPLAIN ANALYZE`** chạy thật và đo. Output thật trên PostgreSQL 18.6 (mục 9):

| | Trước index | Sau `create index ... on page_views (user_id, created_at)` |
|---|---|---|
| Kế hoạch | `Parallel Seq Scan on page_views` + `Rows Removed by Filter: 99984` (mỗi worker) | `Bitmap Index Scan on page_views_user_created_idx` |
| Execution Time | **46.523 ms** | **0.218 ms** |

**Thiết kế index theo truy vấn**, không theo bảng:
- Cột trong `where` so sánh bằng (`user_id = ?`) đứng **trước**, cột so sánh khoảng (`created_at >= ?`) đứng sau.
- Nếu truy vấn còn `order by created_at desc limit 10`, index `(user_id, created_at desc, id desc)` cho phép đọc thẳng theo thứ tự, **không cần Sort**
  (lời giải bài 6 kiểm tra plan không có node `Sort`).

**Đánh đổi:** mỗi index làm chậm `insert/update`, tốn dung lượng, và cần được bảo trì. Không tạo index "phòng khi cần".

**Test plan, không test thời gian.** `QueryPlanSuite` khẳng định "trước có `Seq Scan`, sau dùng `page_views_user_created_idx`" — ổn định
giữa các máy. Thời gian 46 ms → 0.2 ms thì thay đổi theo máy, chỉ dùng để minh họa.

**Lỗi thường gặp:** quên `analyze` sau khi nạp nhiều dữ liệu (planner dùng thống kê cũ); bọc cột trong hàm (`where lower(email) = ?`) mà
index không phải index biểu thức tương ứng (lab tạo đúng `unique index ... (lower(email))`); thứ tự cột trong index ngược với truy vấn.

### 3.8. Connection pool (HikariCP)

**Vấn đề.** Mở một kết nối PostgreSQL tốn thời gian (TCP, xác thực, tạo tiến trình backend). Mỗi request mở kết nối mới là lãng phí;
mở không giới hạn thì database sập vì quá nhiều tiến trình.

**Cách hoạt động.** Pool giữ sẵn tối đa N kết nối. `transact` mượn một kết nối, chạy transaction, trả lại. Request thứ N+1 **chờ**.
`Database.transactor` bọc `HikariTransactor` trong `Resource` — pool đóng khi ứng dụng dừng (Chương 16B).

**Chọn N.** Không phải "càng lớn càng tốt": PostgreSQL xử lý song song bị giới hạn bởi CPU và đĩa. Tài liệu HikariCP
("About Pool Sizing") gợi ý điểm bắt đầu `connections = (số core × 2) + số đĩa hiệu dụng`, rồi **đo**. Tổng kết nối của **mọi** instance
ứng dụng phải nhỏ hơn `max_connections` của PostgreSQL.

**Lỗi thường gặp:** giữ kết nối trong khi gọi HTTP ra ngoài (pool cạn — giống starvation ở Chương 16A); pool quá lớn ở mỗi instance × nhiều instance;
không đặt timeout chờ kết nối.

### 3.9. Truy vấn động an toàn: lọc, sắp xếp, phân trang

`UserRepo.list` ghép truy vấn từ các mảnh:

```scala
val where     = Fragments.whereAndOpt(nameContains.map(n => fr"name ilike ${"%" + n + "%"}"))  // giá trị: tham số
val direction = if descending then fr"desc" else fr"asc"
fr"select ... from users" ++ where ++ fr"order by" ++ Fragment.const(sort.column) ++ direction ++ fr", id" ++ fr"limit $limit offset $offset"
```

- **Giá trị** (tên tìm kiếm, limit, offset) luôn là tham số `$x`.
- **Tên cột** không thể là tham số trong SQL; `Fragment.const` chèn chuỗi thô — **chỉ** an toàn vì `sort.column` lấy từ enum `UserSort`
  (whitelist), không bao giờ từ input của client.
- `, id` ở cuối `order by`: khi hai user cùng tên, thứ tự vẫn **xác định** — nếu không, phân trang có thể trùng/sót dòng.
- **Offset vs keyset.** `offset 10000` vẫn phải đọc và bỏ 10.000 dòng; dữ liệu mới chèn vào làm trang "trượt". Keyset/cursor
  (`where (created_at, id) > (?, ?) order by created_at, id limit ?`) dùng index và ổn định — lời giải bài 4. Chương 14 bàn phía API.

### 3.10. SQL injection

```scala
// SAI — ghép chuỗi:
Fragment.const(s"select ... from users where name = '$name'")
```

Với `name = "x' or '1'='1"`, câu SQL thực sự là:

```sql
select id, email, name, created_at from users where name = 'x' or '1'='1'
```

`'1'='1'` luôn đúng → trả **mọi** user. Test `SQL injection: ghép chuỗi lộ toàn bộ bảng` xác nhận `unsafe.size == 2` (toàn bộ bảng),
còn bản tham số hóa `sql"... where name = $name"` trả `Nil`: chuỗi tấn công chỉ là một **giá trị** để so sánh.

**Quy tắc:** mọi giá trị đi qua `$x` trong `sql`/`fr`. `Fragment.const` chỉ cho chuỗi do **chính code** sinh ra (whitelist). Tìm trong code review
mọi chỗ dùng `Fragment.const` và string interpolation `s"..."` gần SQL.

### 3.11. Doobie hay Slick? (và các lựa chọn khác)

Tra Maven Central ngày 16/10/2026:

| Thư viện | Bản `_3` | Mô hình | Phù hợp |
|---|---|---|---|
| **Doobie** | `1.0.0-RC13` (`org.typelevel`) | Viết SQL thật trong `sql"..."`, kết quả `ConnectionIO`, chạy trên Cats Effect | Stack Typelevel; người muốn kiểm soát SQL |
| **Slick** | `3.6.1` ổn định (`4.0.0-RC2` đang thử) | Query DSL kiểu collection (`users.filter(_.email === e)`) sinh SQL; API trả `Future` (`db.run`) | Team thích DSL type-safe, dùng Play/Pekko |
| Skunk | `2.0.0-RC3` (`org.tpolecat`) | Giao thức PostgreSQL thuần (không JDBC), Cats Effect + fs2 | Chỉ PostgreSQL, muốn non-blocking hoàn toàn |

**Chọn Doobie** vì: cùng runtime Cats Effect với http4s/Tapir (Chương 13) — không cần cầu nối `Future`; SQL viết tay nên những gì học ở
mục 3.1–3.10 dùng trực tiếp, `EXPLAIN` đúng câu bạn viết; ít "phép màu" khi debug.
**Đánh đổi:** Doobie 1.0 vẫn là RC (theo dõi changelog khi nâng cấp); SQL viết tay không được compiler kiểm tra tên cột — bù bằng test với
PostgreSQL thật (lab này). Slick kiểm tra được nhiều hơn lúc biên dịch nhưng thêm một DSL phải học và SQL sinh ra đôi khi khó tối ưu.

### 3.12. Test với database thật

| Cách | Ưu | Nhược |
|---|---|---|
| Mock repository (Chương 10, 14) | Nhanh, không cần gì | Không phát hiện SQL sai, ràng buộc, race |
| H2/SQLite "giả PostgreSQL" | Nhanh | **Khác** PostgreSQL (cú pháp, isolation, plan) — test pass, production fail |
| **PostgreSQL nhúng** (zonky embedded-postgres — lab này) | PostgreSQL **thật** 18.6, không cần Docker, chạy trên CI | Tải binary lần đầu; mỗi suite khởi động ~10–20 giây |
| Testcontainers | PostgreSQL thật trong Docker, đúng image production | Cần Docker daemon |

Lab chọn PostgreSQL nhúng để người học chạy được ngay. `PostgresSuite`: một PostgreSQL cho mỗi suite (`ResourceSuiteLocalFixture`),
migrate bằng Flyway **như production**, `truncate ... restart identity cascade` trước mỗi test để test độc lập.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Primary key (PK) / Foreign key (FK) | Khóa chính định danh dòng / khóa ngoại tham chiếu khóa chính bảng khác |
| Constraint (`CHECK`, `UNIQUE`, `NOT NULL`) | Quy tắc database luôn thực thi |
| Index | Cấu trúc phụ (B-tree mặc định) giúp tìm dòng không cần quét toàn bảng |
| Migration | Một bước thay đổi schema có version |
| Transaction | Nhóm thao tác chạy nguyên tử |
| ACID | Atomicity, Consistency, Isolation, Durability |
| Isolation level | Mức cô lập: READ COMMITTED (mặc định PostgreSQL), REPEATABLE READ, SERIALIZABLE |
| Lost update | Lần ghi sau đè mất lần ghi trước do cả hai đọc cùng giá trị cũ |
| Row lock (`FOR UPDATE`) | Khóa dòng đã đọc đến hết transaction |
| Serialization failure (SQLSTATE 40001) | PostgreSQL hủy transaction để giữ isolation; ứng dụng cần retry |
| Query plan / planner | Kế hoạch thực thi / thành phần chọn kế hoạch |
| Seq Scan / Index Scan / Bitmap Heap Scan | Quét tuần tự / theo index / dùng bitmap từ index rồi đọc heap |
| Connection pool | Tập kết nối dùng lại |
| Parameterized query | Truy vấn có tham số gắn riêng (`?`), không ghép chuỗi |
| SQL injection | Chèn mã SQL qua dữ liệu đầu vào |
| `ConnectionIO`, `Transactor`, `Fragment`, `Meta`, `Read` | Các khái niệm Doobie (mục 3.4) |
| Keyset / cursor pagination | Phân trang theo giá trị khóa cuối trang trước |

## 5. Ví dụ đơn giản

```scala
import cats.effect.IO
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*

val xa: Transactor[IO] = ???  // lấy từ Database.transactor(cfg) trong Resource

val countUsers: ConnectionIO[Long] = sql"select count(*) from users".query[Long].unique
val program: IO[Long]              = countUsers.transact(xa)   // MỘT transaction: begin; select; commit
```

Ba bước luôn giống nhau: viết `Fragment` → chọn cách đọc kết quả (`.query[A].unique/option/to[List]` hoặc `.update.run`) → `transact`.

## 6. Ví dụ thực tế — đặt hàng

`OrderService.placeOrderTx(userId, lines)`:

1. Kiểm tra **từng dòng** (số lượng 1..1000) rồi mới gộp dòng trùng sản phẩm (`KB-01 x2` + `KB-01 x1` → `KB-01 x3`) bằng `Long`; tổng một sản phẩm
   cũng không vượt 1000. Gộp thẳng trên `Int` thì `(Int.MaxValue, Int.MaxValue, 3)` tràn số thành `1` — review chéo đã chứng minh (mục 10, lỗi 7).
2. Kiểm tra user tồn tại.
3. `select ... from products where id in (...) order by id for update` — khóa các sản phẩm theo thứ tự id.
4. Hàm **thuần** `check`: thiếu sản phẩm? đủ kho? → `Either[PlaceOrderError, items]`.
5. Tính tổng tiền bằng `Math.multiplyExact`/`addExact` (tràn số → `TotalTooLarge`, không âm thầm quay vòng về 0); ghi `orders`, `order_items`
   (batch `updateMany`), trừ kho.

Bước 1–5 là một `ConnectionIO`, chạy bằng một `transact`. Lỗi nghiệp vụ (`OutOfStock`) là **giá trị** `Left`, còn lỗi hạ tầng (mất kết nối)
là exception → rollback. Cách tách này nối thẳng sang Problem JSON ở Chương 14: `Left(OutOfStock)` → `409`, exception → `500`.

## 7. Code đầy đủ

```
06-database/db-lab/
├── project.scala / build.sbt / project/build.properties
├── docker-compose.yml                         ← PostgreSQL 18.6 cho chạy tay (đã kiểm chứng với Docker 28.5.1)
├── src/main/resources/db/migration/
│   ├── V1__users_products_orders.sql
│   └── V2__events.sql
├── src/main/scala/vn/scalaeng/db/
│   ├── Model.scala         ← opaque id, User/Product/Order, OrderStatus, lỗi nghiệp vụ, UserSort
│   ├── Codecs.scala        ← Meta/Read: ánh xạ kiểu, tách khỏi domain
│   ├── Repositories.scala  ← UserRepo, ProductRepo, OrderRepo: trả ConnectionIO
│   ├── OrderService.scala  ← ranh giới transaction; NaiveStock (cố ý sai) cho thí nghiệm race
│   ├── Database.scala      ← DbConfig, migrate (Flyway), transactor (HikariCP), explain
│   └── DbDemo.scala        ← chạy tay với PostgreSQL thật
└── src/test/scala/vn/scalaeng/db/
    ├── PostgresSuite.scala     ← PostgreSQL nhúng + Flyway + truncate trước mỗi test
    ├── RepositorySuite.scala   ← 8 test: migration, CRUD, unique (+ trong cùng transaction), 25P02, CHECK, list, injection
    ├── TransactionSuite.scala  ← 9 test: atomicity, rollback, tràn số, lost update, 40001, FOR UPDATE
    └── QueryPlanSuite.scala    ← 1 test: Seq Scan -> index
```

Chạy test (không cần Docker; lần đầu tải binary PostgreSQL ~ vài chục MB):

```bash
cd 06-database/db-lab
scala-cli test .        # hoặc: sbt test
```

Chạy `DbDemo` với PostgreSQL thật:

```bash
docker compose up -d                                      # cách 1: Docker
scala-cli run . --main-class vn.scalaeng.db.DbDemo

# cách 2: PostgreSQL đã cài sẵn — trỏ biến môi trường tới nó
DB_URL=jdbc:postgresql://localhost:5432/mydb DB_USER=me DB_PASSWORD=... scala-cli run . --main-class vn.scalaeng.db.DbDemo
```

## 8. Giải thích code

- **`Model.scala`** không import Doobie: domain không biết database. `UserSort` mang tên cột — whitelist cho `order by`.
- **`Codecs.scala`** — `timap` (total map) cho opaque id; `tiemap` cho enum: chuỗi lạ trong DB (`'DONE'`) thành **lỗi đọc** thay vì giá trị sai.
  (Ràng buộc `CHECK` đã chặn ghi `'DONE'`, nhưng lớp đọc vẫn phòng thủ khi DB do hệ thống khác ghi.)
- **`UserRepo.create`** — `insert ... on conflict ((lower(email))) do nothing returning ...` + `.query[User].option`: trùng email là `None` → `Left(DuplicateEmail)`,
  transaction **không** bị abort (mục 3.4). Không dùng `findByEmail` rồi mới insert — giữa hai bước, request khác có thể chèn cùng email (lại là race);
  để unique index quyết định. `createCatchingUniqueViolation` là bản **cố ý sai** giữ lại cho test 25P02.
- **`ProductRepo.lockForUpdate`** — `Fragments.in(fr"id", ids)` sinh `id IN (?, ?, ...)` với `NonEmptyList` (danh sách rỗng là lỗi cú pháp SQL,
  nên kiểu dữ liệu buộc ta xử lý trường hợp rỗng trước — `EmptyOrder`).
- **`OrderService.check`** là hàm thuần — test được không cần database; DB chỉ cung cấp dữ liệu đã khóa.
- **`NaiveStock`** nhận `pause: ConnectionIO[Unit]` để test chèn rào chắn giữa đọc và ghi, và `isolation` để chạy cùng code ở REPEATABLE READ.
  `set transaction isolation level ...` phải là câu lệnh **đầu tiên** của transaction.
- **`Database.transactor`** — `HikariTransactor.fromHikariConfig[IO](hc)` trả `Resource`; đặt `poolName` để nhận ra pool trong log/metrics.
- **`PostgresSuite.beforeEach`** dọn bảng bằng `truncate ... restart identity cascade` — nhanh hơn `delete`, đặt lại bộ đếm id để test có id dự đoán được.

## 9. Output mong đợi (output thật)

**Test** (`scala-cli test .`):

```
Test run vn.scalaeng.db.QueryPlanSuite finished: 0 failed, 0 ignored, 1 total 10.966s
Test run vn.scalaeng.db.RepositorySuite finished: 0 failed, 0 ignored, 8 total 9.743s
Test run vn.scalaeng.db.TransactionSuite finished: 0 failed, 0 ignored, 9 total 9.83s
```

`sbt test`: `[info] Passed: Total 18, Failed 0, Errors 0, Passed 18`. Thời gian chủ yếu là khởi động PostgreSQL cho mỗi suite.

**`DbDemo`** chạy hai lần liên tiếp trên cùng database:

```
=== lần 1
migration mới áp dụng: 2
user: an@example.vn (id=1)
đơn 1: Right((Pending,900000))
đơn 2: Left(OutOfStock(1,2,1))
tồn kho còn: Some(1)
=== lần 2
migration mới áp dụng: 0
user: an@example.vn (id=1)
đơn 1: Right((Pending,900000))
đơn 2: Left(OutOfStock(2,2,1))
tồn kho còn: Some(1)
```

Lần 2: Flyway không chạy lại migration (`0`); user được tìm lại theo email thay vì tạo trùng; mỗi lần chạy tạo một sản phẩm `DEMO-<thời gian>` mới
(id 2), nên `OutOfStock(2, ...)`.

**`EXPLAIN` / `EXPLAIN ANALYZE`** — PostgreSQL 18.6, 200.000 dòng, `analyze` đã chạy:

```
--- EXPLAIN ANALYZE trước index
Finalize Aggregate  (cost=4235.97..4235.98 rows=1 width=8) (actual time=41.281..46.490 rows=1.00 loops=1)
  ->  Gather  (cost=4235.75..4235.96 rows=2 width=8) (actual time=26.639..46.483 rows=2.00 loops=1)
        Workers Planned: 1
        Workers Launched: 1
        ->  Partial Aggregate  (cost=3235.75..3235.76 rows=1 width=8) (actual time=13.121..13.121 rows=1.00 loops=2)
              ->  Parallel Seq Scan on page_views  (cost=0.00..3235.71 rows=19 width=0) (actual time=1.613..13.106 rows=16.00 loops=2)
                    Filter: ((created_at >= '2026-10-20 00:00:00+07'::timestamp with time zone) AND (user_id = 42))
                    Rows Removed by Filter: 99984
Planning Time: 0.074 ms
Execution Time: 46.523 ms

--- EXPLAIN ANALYZE sau index
Aggregate  (cost=119.15..119.16 rows=1 width=8) (actual time=0.183..0.184 rows=1.00 loops=1)
  ->  Bitmap Heap Scan on page_views  (cost=4.75..119.07 rows=32 width=0) (actual time=0.148..0.177 rows=32.00 loops=1)
        Recheck Cond: ((user_id = 42) AND (created_at >= '2026-10-20 00:00:00+07'::timestamp with time zone))
        Heap Blocks: exact=32
        ->  Bitmap Index Scan on page_views_user_created_idx  (cost=0.00..4.74 rows=32 width=0) (actual time=0.125..0.125 rows=32.00 loops=1)
              Index Cond: ((user_id = 42) AND (created_at >= '2026-10-20 00:00:00+07'::timestamp with time zone))
              Index Searches: 1
Planning Time: 0.115 ms
Execution Time: 0.218 ms
```

Đọc plan từ trong ra ngoài (dòng thụt sâu nhất chạy trước). `rows=19` (ước lượng) so với `rows=16.00` (thật, mỗi worker) — ước lượng tốt vì đã `analyze`.
`+07` là múi giờ phiên làm việc (máy biên soạn ở UTC+7). Con số thời gian **sẽ khác** trên máy bạn; cấu trúc plan mới là điều cần so sánh.


`DbDemo` với `docker compose up -d` (Docker Desktop, engine 28.5.1, volume mới; 18/10/2026) — Flyway áp 2 migration lên container `postgres:18.6`, rồi:

```
migration mới áp dụng: 2
user: an@example.vn (id=1)
đơn 1: Right((Pending,900000))
đơn 2: Left(OutOfStock(1,2,1))
tồn kho còn: Some(1)
```

`docker compose exec db psql -U scalaeng -d scalaeng -c '\dt'` liệt kê đúng 6 bảng: `flyway_schema_history`, `order_items`, `orders`, `page_views`,
`products`, `users`. Chạy `DbDemo` lần hai trên cùng volume sẽ in `migration mới áp dụng: 0` — muốn bắt đầu lại: `docker compose down -v`.

## 10. Lỗi thường gặp (thông báo thật)

**1. Code Doobie cũ với package cũ.** `import doobie.*` với Doobie 1.0.0-RC13:

```
[error] .\Spike.scala:10:8
[error] Not found: doobie - did you mean Double?
[error] import doobie.*
[error]        ^^^^^^
```

Sửa: `import org.typelevel.doobie.*`, `import org.typelevel.doobie.implicits.*`, và groupId `org.typelevel` trong dependency.

**2. Thiếu cú pháp Cats.** `<*` / `*>` trên `ConnectionIO` mà chỉ import `org.typelevel.doobie.implicits.*`:

```
[error] .\src/test/scala/vn/scalaeng/db/QueryPlanSuite.scala:10:5
[error] value <* is not a member of org.typelevel.doobie.free.connection.ConnectionIO[Int], but could be made available as an extension method.
[error]
[error] One of the following imports might fix the problem:
[error]
[error]   import cats.Applicative.ops.toAllApplicativeOps
[error]   ...
[error]   import cats.syntax.all.catsSyntaxApplyOps
```

Compiler liệt kê 8 lựa chọn; chọn `import cats.syntax.all.*` — một import cho toàn bộ cú pháp Cats, thống nhất với Chương 16B.

**3. Lỗi ràng buộc từ PostgreSQL** (`SQLException.getSQLState` + `getMessage`):

```
SQLState=23514
ERROR: new row for relation "products" violates check constraint "products_price_vnd_check"
  Detail: Failing row contains (1, BAD-1, Giá âm, -1, 1).

SQLState=23505
ERROR: duplicate key value violates unique constraint "users_email_lower_uq"
  Detail: Key (lower(email))=(an@x.vn) already exists.

SQLState=23503
ERROR: insert or update on table "orders" violates foreign key constraint "orders_user_id_fkey"
  Detail: Key (user_id)=(999) is not present in table "users".
```

Mã SQLSTATE ổn định (`23505` unique, `23503` FK, `23514` check) — so khớp theo **mã**, không theo câu chữ. **Không** trả nguyên `getMessage`
cho client: dòng `Detail` chứa dữ liệu thật (email người khác!). Chuyển thành lỗi nghiệp vụ như `DuplicateEmail`.

**4. Serialization failure ở REPEATABLE READ:**

```
SQLState=40001
ERROR: could not serialize access due to concurrent update
```

Không phải bug của database: đó là cơ chế bảo vệ. Ứng dụng phải retry **toàn bộ transaction** (không retry riêng câu lệnh cuối).

**5. Bắt lỗi SQL rồi chạy tiếp trong cùng transaction.** Với bản `createCatchingUniqueViolation` (dùng `attemptSomeSqlState`):

```
Scala thấy: Left(DuplicateEmail(AN@example.vn))
SQLState=25P02
ERROR: current transaction is aborted, commands ignored until end of transaction block
```

Dòng đầu: Scala tưởng đã xử lý xong lỗi. Dòng sau: câu `select` kế tiếp bị PostgreSQL từ chối. Sửa: `on conflict ... do nothing` (mục 3.4).

**6. Cảnh báo SLF4J NOP** khi chạy test — giống Chương 13: thư viện (HikariCP) gọi SLF4J nhưng chưa có backend log. Flyway log qua
`java.util.logging` nên vẫn in dòng `INFO: Migrating schema ...`.

**7. Tràn số âm thầm — không có thông báo lỗi nào.** Bản đầu gộp số lượng bằng `_ + _` trên `Int` và nhân tiền bằng `*` trên `Long`. Probe của review chéo
gọi đúng hàm của lab:

```
PROBE merged positive quantities = 1
PROBE service check = Right(List((1,4,4611686018427387904)))
PROBE total using service arithmetic = 0
```

Ba dòng (`Int.MaxValue`, `Int.MaxValue`, `3`) gộp thành **1** món; giá `4611686018427387904` × 4 thành tổng **0** — vẫn qua `CHECK (total_vnd >= 0)`.
JVM không ném lỗi khi `Int`/`Long` tràn. Sửa: kiểm tra từng dòng trước khi gộp, gộp bằng `Long`, giới hạn nghiệp vụ (1..1000), và
`Math.addExact`/`multiplyExact` cho tiền (test `số lượng không hợp lệ hoặc tràn số...`, `tổng tiền vượt miền Long...`).

## 11. Cách debug

- **Xem SQL Doobie thực sự gửi:** `fragment.toString` / `fragment.update.sql` in câu lệnh có `?`; bật `LogHandler` của Doobie khi cần thời gian từng truy vấn.
- **Lỗi đọc kết quả** (`Invalid column index`, sai kiểu): đếm cột trong `select` so với field case class; dùng `.query[(Long, String)]` tạm để cô lập.
- **Truy vấn chậm:** `EXPLAIN (ANALYZE)` đúng câu đó với đúng tham số; kiểm tra `Rows Removed by Filter`, ước lượng `rows=` lệch xa thực tế → `analyze`.
- **Treo khi chạy song song:** xem khóa đang chờ: `select pid, state, wait_event_type, query from pg_stat_activity where datname = current_database();`
- **Pool cạn:** log HikariCP (sau khi thêm backend SLF4J ở Chương 17) cho biết `active/idle/waiting`.
- **Migration lỗi:** đọc **exception/log của Flyway trước** (tên file, câu SQL, SQLSTATE). Với PostgreSQL, migration SQL chạy trong transaction, nên khi lỗi
  cả thay đổi schema **lẫn** dòng lịch sử đều bị rollback — thường **không** còn dòng `success = false` trong `flyway_schema_history` để tìm. Dòng
  `success = false` chỉ xuất hiện với migration không chạy trong transaction hoặc database không hỗ trợ DDL trong transaction. Không dùng `flyway repair`
  để che một migration hỏng.
- **Dùng `psql`:** `docker compose exec db psql -U scalaeng -d scalaeng`, rồi `\d orders` xem bảng, index, ràng buộc.

## 12. Bài tập

1. **(Cơ bản — SQL)** Viết `TopSellers.top(n)`: n sản phẩm bán nhiều nhất theo số lượng, kèm doanh thu, **bỏ qua đơn đã hủy**; hòa thì theo sku.
2. **(Cơ bản — migration)** Thêm migration V3: cột `orders.cancelled_at` và ràng buộc "status = CANCELLED ⇔ cancelled_at không null". Không sửa V1, V2.
3. **(Trung bình — transaction)** `CancelOrder.cancel(id)`: chỉ hủy đơn `PENDING`, hoàn kho. Hai yêu cầu hủy **đồng thời** chỉ được hoàn kho một lần. Viết test đồng thời.
4. **(Trung bình — phân trang)** Keyset pagination cho users theo `(created_at, id)`. Test đi hết dữ liệu qua nhiều trang không trùng không sót.
5. **(Nâng cao — đồng thời)** Sửa lost update **không dùng** `SELECT ... FOR UPDATE`. Giải thích vì sao cách của bạn đúng ở READ COMMITTED.
6. **(Nâng cao — index)** Truy vấn "10 đơn mới nhất của một user". Thiết kế index để plan **không có node Sort**; viết test kiểm tra plan.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch15-solutions.md`](../solutions/ch15-solutions.md)
- Code + test (7 test, PostgreSQL thật): [`solutions/ch15-code/`](../solutions/ch15-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao repository trả `ConnectionIO` thay vì `IO`? Điều gì xảy ra nếu mỗi hàm repository tự gọi `transact`?
2. `CHECK (stock >= 0)` có ngăn được lost update ở mục 3.6 không? Vì sao?
3. Ở READ COMMITTED, vì sao `update ... set stock = stock - 1 where stock >= 1` an toàn còn đọc-rồi-ghi thì không?
4. Vì sao `lockForUpdate` sắp xếp theo `id`?
5. Khi nào dùng `Fragment.const`? Rủi ro là gì?
6. PostgreSQL có tự tạo index cho foreign key không? Hậu quả?
7. Vì sao test kiểm tra **plan** chứ không kiểm tra **thời gian** truy vấn?
8. Vì sao không dùng H2 để test thay PostgreSQL?

<details><summary>Gợi ý đáp án</summary>

1. Để service gộp nhiều thao tác vào **một** transaction. Mỗi hàm tự `transact` → mỗi bước commit riêng, lỗi giữa chừng để lại dữ liệu dở dang.
2. Không: cả hai transaction ghi `0`, giá trị hợp lệ với ràng buộc. Lỗi nằm ở **số lần bán**, không ở giá trị cuối.
3. UPDATE đọc giá trị **mới nhất** của dòng (chờ khóa nếu dòng đang bị sửa) rồi kiểm tra `where` lại; kiểm tra và ghi là một thao tác nguyên tử.
4. Mọi transaction khóa theo cùng thứ tự → không có vòng chờ → không deadlock.
5. Chỉ với chuỗi do code sinh (whitelist như `UserSort.column`). Dùng với input người dùng = SQL injection.
6. Không. Truy vấn theo FK (đơn của user) và `delete` ở bảng cha phải quét toàn bảng con.
7. Thời gian phụ thuộc máy, tải, cache → test chập chờn. Cấu trúc plan ổn định khi dữ liệu và thống kê giống nhau.
8. H2 khác PostgreSQL về cú pháp, kiểu, isolation, planner — test pass không chứng minh production đúng.
</details>

## 15. Mini project

**"Kho hàng an toàn"**: mở rộng lab với bảng `stock_movements (id, product_id, delta, reason, created_at)` — mọi thay đổi tồn kho phải ghi
một dòng *movement* trong **cùng transaction**. Yêu cầu:
- Migration V3+ (không sửa V1, V2); ràng buộc `delta <> 0`.
- `placeOrder` và `cancel` ghi movement; tổng `delta` của một sản phẩm + tồn kho ban đầu = `stock` hiện tại (viết truy vấn kiểm tra).
- Test đồng thời: 20 đơn song song cho sản phẩm có stock 10 → đúng 10 đơn thành công, 10 movement, stock 0.
- `EXPLAIN` truy vấn "lịch sử movement của một sản phẩm trong 7 ngày" và thêm index phù hợp.

## 16. Nguồn tài liệu chính thức

- PostgreSQL 18 docs: https://www.postgresql.org/docs/18/ — đặc biệt các mục *Transaction Isolation*, *Explicit Locking*,
  *Using EXPLAIN*, *Indexes*, *Constraints*, *Appendix A: PostgreSQL Error Codes*
- Doobie: https://typelevel.org/doobie/ · releases (đổi groupId/package ở RC13): https://github.com/typelevel/doobie/releases
- Flyway: https://documentation.red-gate.com/flyway
- HikariCP — About Pool Sizing: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
- PostgreSQL JDBC: https://jdbc.postgresql.org/documentation/
- Slick: https://scala-slick.org/ · Skunk: https://typelevel.org/skunk/
- zonky embedded-postgres: https://github.com/zonkyio/embedded-postgres
- OWASP SQL Injection Prevention Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html
- Maven Central (tra phiên bản): https://repo1.maven.org/maven2/

## 17. Checklist tự đánh giá

- [ ] Thiết kế được bảng có PK, FK, CHECK, UNIQUE và giải thích từng ràng buộc bảo vệ điều gì.
- [ ] Thêm được migration mới mà không sửa migration cũ.
- [ ] Viết được truy vấn Doobie đọc một/nhiều dòng, insert trả về dòng mới, ánh xạ opaque type và enum.
- [ ] Gộp nhiều thao tác vào một transaction ở tầng service; viết được test rollback.
- [ ] Giải thích và tái hiện được lost update; sửa bằng ít nhất hai cách.
- [ ] Giải thích được SQLSTATE 40001 và vì sao phải retry cả transaction.
- [ ] Chỉ ra được SQL injection trong code review; biết khi nào `Fragment.const` an toàn.
- [ ] Đọc được `EXPLAIN ANALYZE`: Seq Scan, Index Scan, Rows Removed by Filter, ước lượng vs thực tế.
- [ ] Chọn kích thước connection pool có lý do và biết đo lại.
- [ ] Giải thích được vì sao bắt lỗi SQL rồi chạy tiếp trong cùng transaction dẫn tới 25P02, và hai cách tránh.
- [ ] Chạy được 18 test của lab trên máy mình.
