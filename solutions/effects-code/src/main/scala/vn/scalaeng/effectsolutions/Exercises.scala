package vn.scalaeng.effectsolutions

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import vn.scalaeng.effects.{Checkout, FakePool, Quote, Retry, Services}

object Exercises:
  // Bài 1: lặp mô tả, không memoize side effect.
  def repeat[A](action: IO[A], times: Int): IO[List[A]] =
    IO.raiseWhen(times < 0)(IllegalArgumentException("times must be non-negative")) *>
      List.fill(times.max(0))(action).sequence

  // Bài 2: chỉ timeout mới được coi là khuyến mãi không có.
  def promotion(action: IO[Long]): IO[Long] = action.handleErrorWith {
    case _: TimeoutException => IO.pure(0L)
    case error => IO.raiseError(error)
  }

  // Bài 3: dùng lồng Resource để đóng ngược thứ tự.
  def session(log: Ref[IO, Vector[String]]): Resource[IO, Unit] =
    def part(name: String): Resource[IO, Unit] =
      Resource.make(log.update(_ :+ s"open $name"))(_ => log.update(_ :+ s"close $name"))
    part("connection") *> part("statement")

  // Bài 4: thứ tự kết quả giống input; số action đang chạy có giới hạn.
  def boundedTraverse[A, B](items: List[A], limit: Int)(action: A => IO[B]): IO[List[B]] =
    IO.raiseWhen(limit <= 0)(IllegalArgumentException("limit must be positive")) *>
      Semaphore[IO](limit.toLong).flatMap { semaphore =>
        items.parTraverse(a => semaphore.permit.use(_ => action(a)))
      }

  // Bài 5: chỉ retry IOException, tối đa retries + 1 lần chạy.
  def retryRead[A](action: IO[A], retries: Int): IO[A] =
    IO.defer(Retry(action, Retry.backoff(100.millis, 1.second, retries))(
      _.isInstanceOf[java.io.IOException]))

  // Bài 6 / mini project: một pool có scope, counter dùng chung cho batch.
  final case class Batch(quotes: List[Quote], requests: Long, events: Vector[String])

  def batch(items: List[Services], concurrency: Int, poolSize: Int, budget: FiniteDuration): IO[Batch] =
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      requests <- Ref.of[IO, Long](0L)
      quotes <- FakePool.resource(poolSize, log).use { pool =>
        boundedTraverse(items, concurrency)(services => Checkout.quote(services, pool, requests, budget))
      }
      n <- requests.get
      events <- log.get
    yield Batch(quotes, n, events)
