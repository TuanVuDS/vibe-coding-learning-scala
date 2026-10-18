package vn.scalaeng.p5

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Json
import io.circe.syntax.*
import org.http4s.{HttpRoutes, MediaType, Response, Status}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.headers.`Content-Type`
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import org.typelevel.doobie.postgres.implicits.*

import java.time.{Duration, Instant, LocalDate}

final case class DailyRow(day: LocalDate, segment: String, revenueVnd: BigDecimal, orderLines: Int, orders: Int)
final case class LoadRecord(day: LocalDate, outputHash: String, runId: String, loadedAt: Instant)

/** STORAGE + SERVING: PostgreSQL giữ bảng đã tổng hợp cho API. Vì sao không cho API đọc thẳng file staging? Vì người đọc cần truy vấn theo khoảng,
  * index, transaction và quyền truy cập — việc của database, không phải của Scala.
  */
final class ServingDb(xa: Transactor[IO]):
  /** DDL nhỏ, chạy lại an toàn. Project 2 dùng Flyway cho migration có phiên bản — với bảng phục vụ thật nên làm như vậy. */
  def migrate: IO[Unit] =
    (sql"""CREATE TABLE IF NOT EXISTS daily_revenue (
             day DATE NOT NULL, segment TEXT NOT NULL,
             revenue_vnd NUMERIC(38,0) NOT NULL CHECK (revenue_vnd >= 0),
             order_lines INT NOT NULL, orders INT NOT NULL CHECK (orders BETWEEN 1 AND order_lines),
             PRIMARY KEY (day, segment))""".update.run *>
      sql"""CREATE TABLE IF NOT EXISTS serving_loads (
             day DATE PRIMARY KEY, output_hash TEXT NOT NULL, run_id TEXT NOT NULL, loaded_at TIMESTAMPTZ NOT NULL)""".update.run *>
      // SQL cho phép tổng hợp đơn giản trên bảng đã sạch: ngắn, ai cũng đọc được, optimizer làm tốt — không cần Scala.
      sql"""CREATE OR REPLACE VIEW monthly_revenue AS
             SELECT to_char(date_trunc('month', day), 'YYYY-MM') AS month, segment,
                    SUM(revenue_vnd) AS revenue_vnd, SUM(orders)::BIGINT AS orders
             FROM daily_revenue GROUP BY 1, 2""".update.run).transact(xa).void

  /** Nạp MỘT ngày trong MỘT transaction: khóa theo ngày → xóa partition cũ → chèn mới → ghi nhận lần nạp. Cùng `outputHash` đã nạp → "unchanged"
    * (idempotent: orchestrator thử lại không tạo gì mới). Người đọc thấy ngày đó ở bản cũ hoặc bản mới, không bao giờ nửa vời.
    */
  def load(day: LocalDate, rows: List[DailyRow], outputHash: String, runId: String, now: Instant): IO[String] =
    (for
      _    <- sql"SELECT pg_advisory_xact_lock(${day.toEpochDay})".query[Unit].unique // hai lần nạp cùng ngày chạy tuần tự
      prev <- sql"SELECT output_hash FROM serving_loads WHERE day = $day".query[String].option
      r <-
        if prev.contains(outputHash) then "unchanged".pure[ConnectionIO]
        else
          sql"DELETE FROM daily_revenue WHERE day = $day".update.run *>
            Update[DailyRow]("INSERT INTO daily_revenue (day, segment, revenue_vnd, order_lines, orders) VALUES (?, ?, ?, ?, ?)").updateMany(rows) *>
            sql"""INSERT INTO serving_loads (day, output_hash, run_id, loaded_at) VALUES ($day, $outputHash, $runId, $now)
                  ON CONFLICT (day) DO UPDATE SET output_hash = EXCLUDED.output_hash, run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at""".update.run
              .as("loaded")
    yield r).transact(xa)

  def daily(from: LocalDate, to: LocalDate): IO[List[DailyRow]] =
    sql"""SELECT day, segment, revenue_vnd, order_lines, orders FROM daily_revenue
          WHERE day BETWEEN $from AND $to ORDER BY day, segment""".query[DailyRow].to[List].transact(xa)

  def monthly: IO[List[(String, String, BigDecimal, Long)]] =
    sql"SELECT month, segment, revenue_vnd, orders FROM monthly_revenue ORDER BY month, segment".query[(String, String, BigDecimal, Long)].to[List].transact(xa)

  def loads: IO[List[LoadRecord]] =
    sql"SELECT day, output_hash, run_id, loaded_at FROM serving_loads ORDER BY day".query[LoadRecord].to[List].transact(xa)

  def ready: IO[Boolean] = sql"SELECT 1".query[Int].unique.transact(xa).as(true).handleError(_ => false)

object Api:
  private def json(rows: List[DailyRow]): Json = Json.arr(rows.map(r => Json.obj(
    "day" -> r.day.toString.asJson, "segment" -> r.segment.asJson, "revenue_vnd" -> r.revenueVnd.asJson,
    "order_lines" -> r.orderLines.asJson, "orders" -> r.orders.asJson))*)

  /** Khoảng tối đa, tính CẢ HAI ĐẦU (`BETWEEN` của SQL bao gồm from và to): 366 ngày = một năm nhuận. */
  val MaxDays = 366

  /** Tham số khoảng ngày: thiếu, lặp lại (`to=a&to=b`), sai định dạng hay quá dài → 400 có lý do — không 404, không lấy bừa giá trị đầu
    * (review P5 N1: bản đầu dùng matcher nên thiếu tham số rơi xuống 404).
    */
  def range(params: Map[String, collection.Seq[String]]): Either[String, (LocalDate, LocalDate)] =
    def one(k: String) = params.get(k) match
      case Some(vs) if vs.sizeIs == 1 => Order.dayOf(vs.head).toRight(s"$k phải là ngày yyyy-MM-dd")
      case Some(_)      => Left(s"$k chỉ được xuất hiện một lần")
      case None         => Left(s"thiếu $k")
    (one("from"), one("to")).tupled.flatMap { (from, to) =>
      if to.isBefore(from) then Left("from phải <= to")
      else if java.time.temporal.ChronoUnit.DAYS.between(from, to) + 1 > MaxDays then Left(s"tối đa $MaxDays ngày (tính cả hai đầu)")
      else Right((from, to))
    }

  /** MONITORING: Prometheus text. Freshness = tuổi của lần nạp gần nhất và ngày dữ liệu mới nhất — người trực nhìn hai số này trước tiên. */
  def metrics(loads: List[LoadRecord], now: Instant): String =
    val last = loads.maxByOption(_.day)
    val newestLoad = loads.maxByOption(_.loadedAt)
    List(
      "# TYPE p5_serving_days_loaded gauge", s"p5_serving_days_loaded ${loads.size}",
      "# TYPE p5_serving_last_day_epoch_day gauge", s"p5_serving_last_day_epoch_day ${last.fold(-1L)(_.day.toEpochDay)}",
      "# TYPE p5_serving_last_load_age_seconds gauge",
      s"p5_serving_last_load_age_seconds ${newestLoad.fold(-1L)(l => Duration.between(l.loadedAt, now).getSeconds)}"
    ).mkString("", "\n", "\n")

  def routes(db: ServingDb, now: () => Instant): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ GET -> Root / "api" / "v1" / "revenue" / "daily" =>
      range(req.multiParams) match
        case Right((from, to)) => db.daily(from, to).flatMap(rows => Ok(json(rows)))
        case Left(why)         => BadRequest(Json.obj("error" -> why.asJson))
    case GET -> Root / "api" / "v1" / "revenue" / "monthly" =>
      db.monthly.flatMap(rows => Ok(Json.arr(rows.map((m, s, r, o) => Json.obj("month" -> m.asJson, "segment" -> s.asJson,
        "revenue_vnd" -> r.asJson, "orders" -> o.asJson))*)))
    case GET -> Root / "health" / "ready" =>
      db.ready.flatMap(up => if up then Ok(Json.obj("status" -> "ready".asJson)) else ServiceUnavailable(Json.obj("status" -> "db down".asJson)))
    case GET -> Root / "metrics" =>
      db.loads.map(ls => Response[IO](Status.Ok).withEntity(metrics(ls, now())).withContentType(`Content-Type`(MediaType.text.plain)))
  }
