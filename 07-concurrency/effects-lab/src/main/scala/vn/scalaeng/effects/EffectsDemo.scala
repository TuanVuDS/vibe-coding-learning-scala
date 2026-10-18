package vn.scalaeng.effects

import cats.effect.{IO, IOApp, Ref}
import scala.concurrent.duration.*

object SimpleExample extends IOApp.Simple:
  val run: IO[Unit] = for
    counter <- Ref.of[IO, Int](0)
    action = counter.updateAndGet(_ + 1)
    first <- action
    second <- action
    _ <- IO.println(s"first=$first second=$second")
  yield ()

object EffectsDemo extends IOApp.Simple:
  val run: IO[Unit] = for
    eager <- Comparisons.eager
    _ <- IO.println(s"eager: Future before=${eager._1} after=${eager._3}; IO before=${eager._2} after=${eager._4}")
    cancel <- Comparisons.cancellation
    _ <- IO.println(s"cancel: canceled=${cancel._1} writes=${cancel._2} finalizer=${cancel._3}")
    timeout <- Comparisons.timeout
    _ <- IO.println(s"timeout: Future error=${timeout._1} before=${timeout._2} after=${timeout._3}; IO error=${timeout._4} writes=${timeout._5} finalizer=${timeout._6}")
    retry <- Comparisons.retries
    _ <- IO.println(s"retry: Future cached=${retry._1} fresh=${retry._2}; IO=${retry._3}")
    resources <- Comparisons.resources
    _ <- IO.println(s"resource: Future closedBeforeRead=${resources._1}")
    _ <- IO.println(resources._2.mkString(" -> "))
    _ <- IO.println(resources._3)
    starvation <- Comparisons.starvation
    _ <- IO.println(s"starvation: Future timedOut=${starvation._1}; blocking+compute=${starvation._2}; fiber join=${starvation._3}")
    interrupted <- Comparisons.interruptible
    _ <- IO.println(s"interruptible: interrupted=$interrupted")
    log <- Ref.of[IO, Vector[String]](Vector.empty)
    requests <- Ref.of[IO, Long](0L)
    quote <- FakePool.resource(3, log).use(pool => Checkout.quote(Checkout.demoServices, pool, requests, 100.millis))
    count <- requests.get
    events <- log.get
    _ <- IO.println(s"quote: price=${quote.price} stock=${quote.stock} discount=${quote.discount} payable=${quote.payable} requests=$count")
    _ <- IO.println(s"pool: opened=${events.count(_.startsWith("open connection"))} closed=${events.count(_.startsWith("close connection"))} last=${events.last}")
  yield ()

/** Lỗi runtime có chủ đích; được bắt để demo vẫn thoát 0. */
object ErrorDemo extends IOApp.Simple:
  val run: IO[Unit] = for
    _ <- IO.raiseError[Int](IllegalArgumentException("unknown sku X")).attempt.flatMap {
      case Left(error) => IO.println(error.toString)
      case Right(value) => IO.println(value)
    }
    _ <- IO.never[Unit].timeout(20.millis).attempt.flatMap {
      case Left(error) => IO.println(error.toString)
      case Right(_) => IO.unit
    }
    log <- Ref.of[IO, Vector[String]](Vector.empty)
    _ <- FakePool.resource(0, log).use(_ => IO.unit).attempt.flatMap {
      case Left(error) => IO.println(error.toString)
      case Right(_) => IO.unit
    }
  yield ()
