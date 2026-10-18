package vn.scalaeng.effects

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import scala.concurrent.duration.*

object Retry:
  /** Số phần tử là số lần thử LẠI; tối đa delays.size + 1 lần chạy. */
  def backoff(base: FiniteDuration, cap: FiniteDuration, retries: Int): List[FiniteDuration] =
    require(base > Duration.Zero && cap >= base && retries >= 0, "invalid backoff")
    List.iterate(base, retries)(d => if d > (cap - d) then cap else d * 2)

  def apply[A](action: IO[A], delays: List[FiniteDuration])(
      retryable: Throwable => Boolean
  ): IO[A] =
    IO.defer(action).handleErrorWith { error =>
      delays match
        case delay :: rest if retryable(error) =>
          IO.sleep(delay) *> apply(action, rest)(retryable)
        case _ => IO.raiseError(error)
    }

final case class Connection(id: Int)

/** Pool giả: Semaphore giới hạn lượt mượn; không có socket hay JDBC thật. */
final class FakePool private (permits: Semaphore[IO], ids: Ref[IO, Int], log: Ref[IO, Vector[String]]):
  def connection: Resource[IO, Connection] =
    permits.permit *> Resource.make(
      ids.updateAndGet(_ + 1).flatMap(id => log.update(_ :+ s"open connection $id").as(Connection(id)))
    )(c => log.update(_ :+ s"close connection ${c.id}"))

object FakePool:
  def resource(size: Int, log: Ref[IO, Vector[String]]): Resource[IO, FakePool] =
    Resource.eval(IO.raiseWhen(size <= 0)(IllegalArgumentException("pool size must be positive"))) *>
      Resource.make(
        for
          permits <- Semaphore[IO](size.toLong)
          ids <- Ref.of[IO, Int](0)
          _ <- log.update(_ :+ "open pool")
        yield new FakePool(permits, ids, log)
      )(_ => log.update(_ :+ "close pool"))

final case class Services(price: IO[Long], stock: IO[Int], promotion: IO[Long])
final case class Quote(price: Long, stock: Int, discount: Long):
  def payable: Long = (price - discount).max(0L)

object Checkout:
  /** Giá bắt buộc; tồn kho timeout -> không bán; khuyến mãi timeout -> không giảm. */
  def quote(services: Services, pool: FakePool, requests: Ref[IO, Long], budget: FiniteDuration): IO[Quote] =
    def borrowed[A](action: IO[A]): IO[A] = pool.connection.use(_ => action)
    requests.update(_ + 1) *>
      (
        borrowed(services.price).timeout(budget),
        borrowed(services.stock).timeoutTo(budget, IO.pure(0)),
        borrowed(services.promotion).timeoutTo(budget, IO.pure(0L))
      ).parMapN(Quote.apply)

  val demoServices: Services = Services(
    IO.sleep(20.millis).as(120_000L),
    IO.sleep(30.millis).as(3),
    IO.never[Long]
  )
