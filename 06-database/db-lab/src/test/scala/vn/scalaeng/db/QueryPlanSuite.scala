package vn.scalaeng.db

import cats.syntax.all.*
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*

/** Index thay đổi KẾ HOẠCH thực thi thế nào. Kiểm tra trên plan (ổn định), không trên thời gian (dao động). */
class QueryPlanSuite extends PostgresSuite:
  // 200.000 lượt xem trang của 2.000 user trong 30 ngày — đủ lớn để planner thấy index có lợi.
  private val seed: ConnectionIO[Int] =
    sql"""insert into page_views (user_id, path, created_at)
          select (g % 2000) + 1, '/p/' || (g % 50), timestamptz '2026-10-01' + (g % 43200) * interval '1 minute'
          from generate_series(1, 200000) g""".update.run <*
      sql"analyze page_views".update.run

  private val query: Fragment =
    fr"select count(*) from page_views where user_id = 42 and created_at >= timestamptz '2026-10-20'"

  test("không có index -> Seq Scan; thêm index (user_id, created_at) -> dùng index"):
    for
      inserted <- seed.transact(xa)
      before   <- Database.explain(query).transact(xa)
      _        <- sql"create index page_views_user_created_idx on page_views (user_id, created_at)".update.run.transact(xa)
      after    <- Database.explain(query).transact(xa)
      count    <- query.query[Long].unique.transact(xa)
    yield
      assertEquals(inserted, 200000)
      assert(before.exists(_.contains("Seq Scan on page_views")), before.mkString("\n"))
      assert(after.exists(_.contains("page_views_user_created_idx")), after.mkString("\n"))
      assert(!after.exists(_.contains("Seq Scan")), after.mkString("\n"))
      assert(count > 0)
