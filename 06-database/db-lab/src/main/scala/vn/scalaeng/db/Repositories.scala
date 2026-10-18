package vn.scalaeng.db

import cats.data.NonEmptyList
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.postgres.sqlstate
import vn.scalaeng.db.Codecs.given

/** Repository trả `ConnectionIO[A]` — một MÔ TẢ thao tác trên một kết nối. Chưa chạy gì cả.
  * Tầng service quyết định gộp các thao tác vào cùng transaction nào rồi mới `transact(xa)`.
  */
object UserRepo:
  private val columns = fr"id, email, name, created_at"

  /** `on conflict ... do nothing` để PostgreSQL tự bỏ qua dòng trùng mà KHÔNG làm hỏng transaction:
    * trùng → `returning` không trả dòng nào → `None` → `DuplicateEmail`. Transaction vẫn dùng tiếp được.
    * Conflict target là BIỂU THỨC của unique index `users_email_lower_uq`, nên phải viết `((lower(email)))`.
    */
  def create(email: String, name: String): ConnectionIO[Either[CreateUserError, User]] =
    sql"""insert into users (email, name) values ($email, $name)
          on conflict ((lower(email))) do nothing
          returning id, email, name, created_at""".query[User].option
      .map(_.toRight(CreateUserError.DuplicateEmail(email)))

  /** CỐ Ý SAI — giữ lại để minh họa (mục 3.4, test `attemptSomeSqlState để lại transaction bị abort`).
    * `attemptSomeSqlState` chỉ đổi exception thành `Left` ở phía Scala; phía PostgreSQL transaction ĐÃ bị abort sau lỗi 23505.
    * Mọi câu lệnh tiếp theo trong CÙNG transaction đều thất bại với SQLSTATE 25P02.
    */
  def createCatchingUniqueViolation(email: String, name: String): ConnectionIO[Either[CreateUserError, User]] =
    sql"insert into users (email, name) values ($email, $name)".update
      .withUniqueGeneratedKeys[User]("id", "email", "name", "created_at")
      .attemptSomeSqlState { case sqlstate.class23.UNIQUE_VIOLATION => CreateUserError.DuplicateEmail(email) }

  def find(id: UserId): ConnectionIO[Option[User]] =
    (fr"select" ++ columns ++ fr"from users where id = $id").query[User].option

  def findByEmail(email: String): ConnectionIO[Option[User]] =
    (fr"select" ++ columns ++ fr"from users where lower(email) = lower($email)").query[User].option

  /** Lọc + sắp xếp (whitelist) + phân trang offset. `id` là khóa phụ để thứ tự ỔN ĐỊNH khi trùng giá trị sort. */
  def list(nameContains: Option[String], sort: UserSort, descending: Boolean, limit: Int, offset: Int): ConnectionIO[List[User]] =
    val where     = Fragments.whereAndOpt(nameContains.map(n => fr"name ilike ${"%" + n + "%"}"))
    val direction = if descending then fr"desc" else fr"asc"
    (fr"select" ++ columns ++ fr"from users" ++ where ++
      fr"order by" ++ Fragment.const(sort.column) ++ direction ++ fr", id" ++
      fr"limit $limit offset $offset").query[User].to[List]

  /** CỐ Ý SAI — chỉ để minh họa SQL injection (mục 3.10). Đừng bao giờ ghép chuỗi input vào SQL. */
  def unsafeFindByName(name: String): ConnectionIO[List[User]] =
    Fragment.const(s"select id, email, name, created_at from users where name = '$name'").query[User].to[List]

  def safeFindByName(name: String): ConnectionIO[List[User]] =
    (fr"select" ++ columns ++ fr"from users where name = $name").query[User].to[List]

object ProductRepo:
  def create(sku: String, name: String, priceVnd: Long, stock: Int): ConnectionIO[ProductId] =
    sql"insert into products (sku, name, price_vnd, stock) values ($sku, $name, $priceVnd, $stock)".update
      .withUniqueGeneratedKeys[ProductId]("id")

  def find(id: ProductId): ConnectionIO[Option[Product]] =
    sql"select id, sku, name, price_vnd, stock from products where id = $id".query[Product].option

  /** `for update` khóa các dòng được đọc đến hết transaction. `order by id` để mọi transaction khóa theo
    * CÙNG một thứ tự — hai đơn hàng khóa (A rồi B) và (B rồi A) là công thức của deadlock.
    */
  def lockForUpdate(ids: NonEmptyList[ProductId]): ConnectionIO[List[Product]] =
    (fr"select id, sku, name, price_vnd, stock from products where" ++ Fragments.in(fr"id", ids) ++
      fr"order by id for update").query[Product].to[List]

  def readWithoutLock(id: ProductId): ConnectionIO[Option[Product]] = find(id)

  def setStock(id: ProductId, stock: Int): ConnectionIO[Int] =
    sql"update products set stock = $stock where id = $id".update.run

  def decrementStock(id: ProductId, quantity: Int): ConnectionIO[Int] =
    sql"update products set stock = stock - $quantity where id = $id".update.run

object OrderRepo:
  def insertOrder(userId: UserId, totalVnd: Long): ConnectionIO[Order] =
    sql"insert into orders (user_id, total_vnd) values ($userId, $totalVnd)".update
      .withUniqueGeneratedKeys[Order]("id", "user_id", "status", "total_vnd", "created_at")

  def insertItems(orderId: OrderId, items: List[(ProductId, Int, Long)]): ConnectionIO[Int] =
    Update[(OrderId, ProductId, Int, Long)](
      "insert into order_items (order_id, product_id, quantity, unit_price_vnd) values (?, ?, ?, ?)"
    ).updateMany(items.map((p, q, price) => (orderId, p, q, price)))

  def countFor(userId: UserId): ConnectionIO[Int] =
    sql"select count(*) from orders where user_id = $userId".query[Int].unique

  def updateStatus(id: OrderId, from: OrderStatus, to: OrderStatus): ConnectionIO[Boolean] =
    sql"update orders set status = $to where id = $id and status = $from".update.run.map(_ == 1)
