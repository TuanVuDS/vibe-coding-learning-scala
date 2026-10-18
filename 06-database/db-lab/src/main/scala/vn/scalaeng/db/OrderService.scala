package vn.scalaeng.db

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*

/** Tầng service: ĐÂY là nơi đặt ranh giới transaction (transaction boundary).
  * Mọi bước của `placeOrder` chạy trong MỘT transaction: hoặc tất cả thành công, hoặc không gì thay đổi.
  */
final class OrderService(xa: Transactor[IO]):

  def placeOrder(userId: UserId, lines: List[OrderLine]): IO[Either[PlaceOrderError, Order]] =
    placeOrderTx(userId, lines).transact(xa)

  /** Gộp các dòng trùng sản phẩm, khóa sản phẩm, kiểm tra tồn kho, ghi đơn + chi tiết + trừ kho. */
  def placeOrderTx(userId: UserId, lines: List[OrderLine]): ConnectionIO[Either[PlaceOrderError, Order]] =
    OrderService.mergeLines(lines) match
      case Left(err) => err.asLeft[Order].pure[ConnectionIO]
      case Right(merged) => placeMerged(userId, merged)

  private def placeMerged(userId: UserId, merged: Map[ProductId, Int]): ConnectionIO[Either[PlaceOrderError, Order]] =
    NonEmptyList.fromList(merged.keys.toList.sortBy(_.value)) match
      case None => PlaceOrderError.EmptyOrder.asLeft[Order].pure[ConnectionIO]
      case Some(ids) =>
        UserRepo.find(userId).flatMap {
          case None => PlaceOrderError.UnknownUser(userId).asLeft[Order].pure[ConnectionIO]
          case Some(_) =>
            ProductRepo.lockForUpdate(ids).flatMap { products =>
              check(merged, products) match
                case Left(err)    => err.asLeft[Order].pure[ConnectionIO]
                case Right(items) => write(userId, items)
            }
        }

  /** Logic thuần: không chạm DB, test được không cần PostgreSQL. */
  private def check(wanted: Map[ProductId, Int], found: List[Product]): Either[PlaceOrderError, List[(ProductId, Int, Long)]] =
    val byId    = found.map(p => p.id -> p).toMap
    val missing = wanted.keys.filterNot(byId.contains).toList.sortBy(_.value)
    if missing.nonEmpty then Left(PlaceOrderError.UnknownProducts(missing))
    else
      found.sortBy(_.id.value).traverse { p =>
        val q = wanted(p.id)
        if q > p.stock then Left(PlaceOrderError.OutOfStock(p.id, q, p.stock)) else Right((p.id, q, p.priceVnd))
      }

  private def write(userId: UserId, items: List[(ProductId, Int, Long)]): ConnectionIO[Either[PlaceOrderError, Order]] =
    OrderService.total(items) match
      case Left(err) => err.asLeft[Order].pure[ConnectionIO]
      case Right(total) =>
        for
          order <- OrderRepo.insertOrder(userId, total)
          _     <- OrderRepo.insertItems(order.id, items)
          _     <- items.traverse_((p, q, _) => ProductRepo.decrementStock(p, q))
        yield Right(order)

object OrderService:
  val MaxQuantity = 1000

  /** Kiểm tra TỪNG dòng trước khi gộp, và gộp bằng Long: nếu cộng thẳng trên Int, ba dòng
    * (Int.MaxValue, Int.MaxValue, 3) "gộp" thành 1 vì tràn số — mua được 1 món dù yêu cầu vô lý (review chéo, M2).
    */
  def mergeLines(lines: List[OrderLine]): Either[PlaceOrderError, Map[ProductId, Int]] =
    lines.find(l => l.quantity < 1 || l.quantity > MaxQuantity) match
      case Some(bad) => Left(PlaceOrderError.InvalidQuantity(bad.productId, bad.quantity.toLong))
      case None =>
        val merged = lines.groupMapReduce(_.productId)(_.quantity.toLong)(_ + _)
        merged.find((_, q) => q > MaxQuantity) match
          case Some((id, q)) => Left(PlaceOrderError.InvalidQuantity(id, q))
          case None          => Right(merged.view.mapValues(_.toInt).toMap)

  /** Tổng tiền bằng phép tính KIỂM TRA tràn số: `multiplyExact`/`addExact` ném ArithmeticException thay vì âm thầm quay vòng. */
  def total(items: List[(ProductId, Int, Long)]): Either[PlaceOrderError, Long] =
    scala.util.Try(items.foldLeft(0L)((acc, it) => Math.addExact(acc, Math.multiplyExact(it._3, it._2.toLong)))).toOption
      .toRight(PlaceOrderError.TotalTooLarge)

/** Phiên bản SAI để minh họa "lost update" (mục 3.8): đọc tồn kho KHÔNG khóa, tính ở ứng dụng, ghi đè.
  * `pause` mở rộng cửa sổ race giữa đọc và ghi — giống thí nghiệm `Thread.yield()` ở Chương 16A.
  */
final class NaiveStock(xa: Transactor[IO], pause: ConnectionIO[Unit]):
  def buyOne(id: ProductId, isolation: Option[String] = None): IO[Boolean] =
    val tx = for
      _  <- isolation.traverse_(level => Fragment.const(s"set transaction isolation level $level").update.run)
      p  <- ProductRepo.readWithoutLock(id)
      ok <- p.filter(_.stock >= 1) match
              case None       => false.pure[ConnectionIO]
              case Some(prod) => pause *> ProductRepo.setStock(id, prod.stock - 1).as(true)
    yield ok
    tx.transact(xa)
