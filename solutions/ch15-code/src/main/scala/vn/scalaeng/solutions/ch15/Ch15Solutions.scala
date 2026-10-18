package vn.scalaeng.solutions.ch15

import cats.effect.IO
import cats.syntax.all.*
import java.time.Instant
import org.flywaydb.core.Flyway
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.postgres.implicits.*
import vn.scalaeng.db.*
import vn.scalaeng.db.Codecs.given

/** Bài 1 — top sản phẩm bán chạy: JOIN + GROUP BY + ORDER BY + LIMIT. Hòa thì theo sku để kết quả ổn định. */
object TopSellers:
  final case class Row(sku: String, name: String, unitsSold: Long, revenueVnd: Long)

  def top(n: Int): ConnectionIO[List[Row]] =
    sql"""select p.sku, p.name, sum(i.quantity)::bigint, sum(i.quantity * i.unit_price_vnd)::bigint
          from order_items i
          join orders o   on o.id = i.order_id
          join products p on p.id = i.product_id
          where o.status <> 'CANCELLED'
          group by p.sku, p.name
          order by 3 desc, p.sku
          limit $n""".query[Row].to[List]

/** Bài 2 — chạy migration của lab (V1, V2) CỘNG migration của lời giải (V3) từ một thư mục khác. */
object Migrations:
  def migrateWithSolutions(cfg: DbConfig): IO[Int] = IO.blocking {
    Flyway.configure().dataSource(cfg.url, cfg.user, cfg.password)
      .locations("classpath:db/migration", "classpath:db/solutions")
      .load().migrate().migrationsExecuted
  }

/** Bài 3 — hủy đơn: chỉ đơn PENDING; hoàn kho; an toàn khi hai yêu cầu hủy đến cùng lúc. */
object CancelOrder:
  enum Result:
    case Cancelled, NotFound, NotCancellable

  def cancel(id: OrderId): ConnectionIO[Result] =
    // UPDATE có điều kiện là "compare-and-set" của SQL: chỉ MỘT transaction đổi được PENDING -> CANCELLED.
    // Transaction thứ hai chờ khóa dòng, rồi thấy status đã khác PENDING -> cập nhật 0 dòng.
    sql"""update orders set status = 'CANCELLED', cancelled_at = now()
          where id = $id and status = 'PENDING'""".update.run.flatMap {
      case 1 =>
        sql"""update products p set stock = p.stock + i.quantity
              from order_items i where i.order_id = $id and i.product_id = p.id""".update.run.as(Result.Cancelled)
      case _ =>
        sql"select 1 from orders where id = $id".query[Int].option.map(_.fold(Result.NotFound)(_ => Result.NotCancellable))
    }

/** Bài 4 — keyset (cursor) pagination theo (created_at, id). Không dùng OFFSET: không trượt khi có dữ liệu mới. */
object KeysetPage:
  final case class Cursor(createdAt: Instant, id: Long)

  val MaxSize = 100

  /** Tiền điều kiện: 1 <= size <= MaxSize. Thiếu kiểm tra này, `size = 0` làm `page.last` ném exception và
    * `size = Int.MaxValue` làm `size + 1` tràn số (review chéo, m2).
    */
  def page(after: Option[Cursor], size: Int): ConnectionIO[Either[String, (List[User], Option[Cursor])]] =
    if size < 1 || size > MaxSize then Left(s"size phải trong 1..$MaxSize").pure[ConnectionIO]
    else
      val cond = after.map(c => fr"where (created_at, id) > (${c.createdAt}, ${c.id})").getOrElse(Fragment.empty)
      (fr"select id, email, name, created_at from users" ++ cond ++ fr"order by created_at, id limit ${size + 1}")
        .query[User].to[List].map { rows =>
          val page = rows.take(size)
          val next = Option.when(rows.size > size)(page.last).map(u => Cursor(u.createdAt, u.id.value))
          Right((page, next))
        }

/** Bài 5 — sửa lost update KHÔNG cần SELECT FOR UPDATE: để database tự kiểm tra và trừ trong một câu lệnh. */
object AtomicStock:
  def buyOne(id: ProductId): ConnectionIO[Boolean] =
    sql"update products set stock = stock - 1 where id = $id and stock >= 1".update.run.map(_ == 1)

/** Bài 6 — truy vấn cần index (user_id, created_at desc, id desc) để vừa lọc vừa khỏi sort. */
object LatestOrders:
  def query(userId: UserId): Fragment =
    fr"select id from orders where user_id = $userId order by created_at desc, id desc limit 10"
