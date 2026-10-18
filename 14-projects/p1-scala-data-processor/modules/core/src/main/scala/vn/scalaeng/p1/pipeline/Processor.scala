package vn.scalaeng.p1.pipeline

import vn.scalaeng.p1.csv.Csv
import vn.scalaeng.p1.model.*
import vn.scalaeng.p1.parse.{OrderParser, UserParser}
import java.time.LocalDate

final case class ReportRow(date: LocalDate, city: City, paidOrders: Int, items: Int, revenue: Vnd)

final case class Stats(
    orderRows: Int, ordersValid: Int, ordersRejected: Int, ordersSuperseded: Int,
    userItems: Int, usersValid: Int, usersRejected: Int,
    orphanOrders: Int, paid: Int, cancelled: Int, refunded: Int, paidRevenue: Long
):
  /** Không bản ghi nào biến mất giữa input và output. */
  def balanced: Boolean =
    orderRows == ordersValid + ordersRejected + ordersSuperseded && userItems == usersValid + usersRejected
  def orderRejectRatio: Double = if orderRows == 0 then 0.0 else ordersRejected.toDouble / orderRows

final case class Result(report: List[ReportRow], rejected: List[Rejected], stats: Stats)

object Dedup:
  /** Giữ bản có updated_at MỚI NHẤT cho mỗi order_id; bằng nhau thì giữ dòng xuất hiện TRƯỚC.
    * Trả về (bản được giữ, danh sách (bản bị thay, bản thắng)). Kết quả không phụ thuộc thứ tự input khi không có tie.
    */
  def latestByOrderId(orders: List[Order]): (List[Order], List[(Order, Order)]) =
    val groups = orders.groupBy(_.orderId).values.toList
    val winners = groups.map(_.reduce((a, b) =>
      if b.updatedAt.isAfter(a.updatedAt) || (b.updatedAt == a.updatedAt && b.lineNo < a.lineNo) then b else a
    ))
    val losers = groups.zip(winners).flatMap((g, w) => g.filterNot(_ eq w).map(_ -> w))
    (winners.sortBy(_.orderId.value), losers.sortBy(_._1.lineNo))

  /** Users: trùng user_id thì giữ phần tử đầu tiên. */
  def firstByUserId(users: List[(Int, User)]): (List[User], List[(Int, User, Int)]) =
    val firstPos: Map[UserId, Int] = users.groupMapReduce(_._2.userId)(_._1)(math.min)
    val (kept, dups) = users.partition((pos, u) => firstPos(u.userId) == pos)
    (kept.map(_._2), dups.map((pos, u) => (pos, u, firstPos(u.userId))))

object Processor:
  def run(orderLines: List[String], usersText: String): Either[String, Result] =
    orderLines match
      case Nil => Left("orders.csv rỗng (không có header)")
      case header :: body =>
        val headerCols = Csv.parseLine(Csv.stripBom(header)).map(_.map(_.trim.toLowerCase))
        if headerCols != Right(OrderParser.Header) then
          Left(s"header orders.csv không đúng: '$header' (cần ${OrderParser.Header.mkString(",")})")
        else UserParser.parseAll(usersText).map(users => process(body, users))

  private def process(body: List[String], userResults: List[(String, Either[Rejected, User])]): Result =
    // ---- users (giữ JSON gốc của từng phần tử để ghi vào rejected.csv)
    val rawUser: Map[Int, String] = userResults.zipWithIndex.map((r, i) => (i + 1) -> r._1).toMap
    val (userRejects, userOks) = userResults.zipWithIndex.partitionMap {
      case ((_, Left(r)), _)  => Left(r)
      case ((_, Right(u)), i) => Right((i + 1, u))
    }
    val (users, userDups) = Dedup.firstByUserId(userOks)
    val userDupRejects = userDups.map((pos, u, first) =>
      Rejected(Source.Users, pos, rawUser(pos), List(Issue("user_id", s"${u.userId.value} trùng với phần tử #$first, giữ phần tử đầu tiên"))))
    val userById: Map[UserId, User] = users.map(u => u.userId -> u).toMap

    // ---- orders (dòng trống bị bỏ qua, nhưng số dòng vẫn theo file gốc: header là dòng 1)
    val rows = body.zipWithIndex.collect { case (l, i) if l.trim.nonEmpty => (i + 2, l) }
    val rawByLine = rows.toMap
    val (orderRejects, orders) = rows.partitionMap((n, l) => OrderParser.parseRow(n, l))
    val (kept, superseded) = Dedup.latestByOrderId(orders)
    val supersededRejects = superseded.map { (loser, winner) =>
      val reason =
        if loser.updatedAt == winner.updatedAt then
          if loser.copy(lineNo = 0) == winner.copy(lineNo = 0) then s"trùng hoàn toàn với dòng ${winner.lineNo}"
          else s"xung đột: cùng updated_at với dòng ${winner.lineNo} nhưng khác nội dung, giữ dòng xuất hiện trước"
        else s"bị thay bởi dòng ${winner.lineNo} (updated_at mới hơn)"
      Rejected(Source.Orders, loser.lineNo, rawByLine(loser.lineNo), List(Issue("order_id", reason)))
    }

    // ---- join + aggregate (chỉ PAID tính doanh thu)
    val cityOf: Order => City = o => userById.get(o.userId).map(_.city).getOrElse(City.Unknown)
    val paid = kept.filter(_.status == OrderStatus.Paid)
    val report = paid
      .groupBy(o => (o.orderDate, cityOf(o)))
      .toList
      .map { case ((d, c), os) =>
        ReportRow(d, c, os.size, os.map(_.quantity.count).sum, os.map(_.total).foldLeft(Vnd.Zero)((acc, v) => acc.plus(v)))
      }
      .sortBy(r => (r.date, r.city.value))

    val stats = Stats(
      orderRows = rows.size, ordersValid = kept.size, ordersRejected = orderRejects.size, ordersSuperseded = superseded.size,
      userItems = userResults.size, usersValid = users.size, usersRejected = userRejects.size + userDupRejects.size,
      orphanOrders = kept.count(o => !userById.contains(o.userId)),
      paid = paid.size,
      cancelled = kept.count(_.status == OrderStatus.Cancelled),
      refunded = kept.count(_.status == OrderStatus.Refunded),
      paidRevenue = report.map(_.revenue.dong).sum
    )
    val rejected = (orderRejects ++ supersededRejects ++ userRejects ++ userDupRejects)
      .sortBy(r => (r.source.ordinal, r.position))
    Result(report, rejected, stats)
