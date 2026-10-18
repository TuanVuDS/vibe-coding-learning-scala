package vn.scalaeng.effects

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.kernel.Outcome
import cats.syntax.all.*
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Try, Using}

/** Chỉ harness đối chiếu mới dùng Await; không dùng trong service IO. */
private final class FutureEnv(val ec: ExecutionContextExecutorService, scheduler: ScheduledExecutorService):
  given ExecutionContext = ec

  def shutdown(): Unit =
    ec.shutdown()
    scheduler.shutdown()

  def delay[A](duration: FiniteDuration)(body: => A): Future[A] =
    val result = Promise[A]()
    scheduler.schedule((() => { result.complete(Try(body)); () }): Runnable,
      duration.toNanos, TimeUnit.NANOSECONDS)
    result.future

  def timeout[A](source: Future[A], duration: FiniteDuration): Future[A] =
    val result = Promise[A]()
    val timer = scheduler.schedule((() => {
      result.tryFailure(TimeoutException("Future deadline")); ()
    }): Runnable, duration.toNanos, TimeUnit.NANOSECONDS)
    source.onComplete { value => timer.cancel(false); result.tryComplete(value); () }
    result.future

  def retry[A](action: () => Future[A], delays: List[FiniteDuration]): Future[A] =
    action().recoverWith {
      case _: java.io.IOException if delays.nonEmpty =>
        delay(delays.head)(()).flatMap(_ => retry(action, delays.tail))
    }

private object FutureEnv:
  val resource: Resource[IO, FutureEnv] = Resource.make(IO.delay {
    new FutureEnv(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2)),
      Executors.newSingleThreadScheduledExecutor())
  })(env => IO.delay(env.shutdown()))

object Comparisons:
  def eager: IO[(Int, Int, Int, Int)] = FutureEnv.resource.use { env =>
    given ExecutionContext = env.ec
    for
      counts <- IO.delay((new AtomicInteger(0), new AtomicInteger(0)))
      (futureCount, ioCount) = counts
      future <- IO.delay(Future { futureCount.incrementAndGet() })
      _ <- IO.fromFuture(IO.pure(future)) // đợi thân Future đã chạy, chưa map Future
      description = IO.delay(ioCount.incrementAndGet())
      before <- IO.delay((futureCount.get(), ioCount.get()))
      _ <- description
      _ <- description
      after <- IO.delay((futureCount.get(), ioCount.get()))
    yield (before._1, before._2, after._1, after._2)
  }

  def cancellation: IO[(Boolean, Int, Boolean)] =
    for
      started <- Deferred[IO, Unit]
      gate <- Deferred[IO, Unit]
      writes <- Ref.of[IO, Int](0)
      canceled <- Ref.of[IO, Boolean](false)
      fiber <- (started.complete(()).void *> gate.get *> writes.update(_ + 1))
        .onCancel(canceled.set(true)).start
      _ <- started.get
      _ <- fiber.cancel // đợi onCancel xong
      outcome <- fiber.join
      _ <- gate.complete(())
      n <- writes.get
      cleaned <- canceled.get
    yield (outcome match
      case Outcome.Canceled() => true
      case _ => false, n, cleaned)

  def timeout: IO[(Boolean, Int, Int, Boolean, Int, Boolean)] = FutureEnv.resource.use { env =>
    given ExecutionContext = env.ec
    for
      writes <- IO.delay(new AtomicInteger(0))
      gate <- IO.delay(Promise[Unit]())
      began <- IO.delay(Promise[Unit]())
      slow <- IO.delay(Future {
        began.success(())
        Await.result(gate.future, 5.seconds)
        writes.incrementAndGet()
      })
      data <- (for
        _ <- IO.fromFuture(IO.pure(began.future))
        result <- IO.fromFuture(IO(env.timeout(slow, 40.millis))).attempt
        before <- IO.delay(writes.get())
        _ <- IO.delay(gate.trySuccess(()))
        _ <- IO.fromFuture(IO.pure(slow))
        after <- IO.delay(writes.get())
        ioGate <- Deferred[IO, Unit]
        ioWrites <- Ref.of[IO, Int](0)
        canceled <- Ref.of[IO, Boolean](false)
        resultIO <- (ioGate.get *> ioWrites.update(_ + 1)).onCancel(canceled.set(true))
          .timeout(40.millis).attempt
        _ <- ioGate.complete(())
        ioCount <- ioWrites.get
        cleaned <- canceled.get
      yield (result.left.exists(_.isInstanceOf[TimeoutException]), before, after,
        resultIO.left.exists(_.isInstanceOf[TimeoutException]), ioCount, cleaned))
        .guarantee(IO.delay(gate.trySuccess(())).void)
    yield data
  }

  def retries: IO[(Int, Int, Int)] = FutureEnv.resource.use { env =>
    given ExecutionContext = env.ec
    for
      cachedCount <- IO.delay(new AtomicInteger(0))
      cached <- IO.delay(Future { cachedCount.incrementAndGet(); throw java.io.IOException("cached") })
      _ <- IO.fromFuture(IO(env.retry(() => cached, List(1.millis, 2.millis)))).attempt
      freshCount <- IO.delay(new AtomicInteger(0))
      fresh = () => Future {
        val n = freshCount.incrementAndGet()
        if n < 3 then throw java.io.IOException("transient") else n
      }
      _ <- IO.fromFuture(IO(env.retry(fresh, List(1.millis, 2.millis))))
      ioCount <- Ref.of[IO, Int](0)
      action = ioCount.updateAndGet(_ + 1).flatMap { n =>
        if n < 3 then IO.raiseError[Int](java.io.IOException("transient")) else IO.pure(n)
      }
      _ <- Retry(action, List(1.millis, 2.millis))(_.isInstanceOf[java.io.IOException])
      n <- ioCount.get
    yield (cachedCount.get(), freshCount.get(), n)
  }

  def resources: IO[(Boolean, Vector[String], String)] = FutureEnv.resource.use { env =>
    given ExecutionContext = env.ec
    for
      closed <- IO.delay(new AtomicBoolean(false))
      gate <- IO.delay(Promise[Unit]())
      reader <- IO.delay {
        val connection = new AutoCloseable:
          def close(): Unit = closed.set(true)
        Using.resource(connection) { _ =>
          Future { Await.result(gate.future, 5.seconds); closed.get() }
        }
      }
      _ <- IO.delay(gate.success(()))
      closedTooSoon <- IO.fromFuture(IO.pure(reader))
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- nestedResources(log).use(_ => IO.cede *> IO.raiseError[Unit](IllegalStateException("query failed"))).attempt
      events <- log.get
    yield (closedTooSoon, events, result.fold(_.toString, _ => "success"))
  }

  def nestedResources(log: Ref[IO, Vector[String]]): Resource[IO, Unit] =
    def named(name: String): Resource[IO, Unit] =
      Resource.make(log.update(_ :+ s"open $name"))(_ => log.update(_ :+ s"close $name"))
    named("connection") *> named("statement")

  def starvation: IO[(Boolean, Int, Int)] =
    val single = Resource.make(IO.delay(ExecutionContext.fromExecutorService(
      Executors.newSingleThreadExecutor())))(ec => IO.delay(ec.shutdown()))
    single.use { ec =>
      given ExecutionContext = ec
      for
        result <- IO.fromFuture(IO(Future {
          val inner = Future(42)
          Await.result(inner, 100.millis)
        })).attempt
        gate <- IO.delay(new CountDownLatch(1))
        began <- IO.delay(new CountDownLatch(1))
        blocked <- IO.blocking { began.countDown(); gate.await(); 7 }.start
        _ <- IO.blocking(began.await())
        compute <- IO.delay(42).guarantee(IO.delay(gate.countDown()))
        answer <- blocked.joinWithNever
        child <- IO.pure(42).start
        joined <- child.joinWithNever
      yield (result.left.exists(_.isInstanceOf[TimeoutException]), compute + answer, joined)
    }

  def interruptible: IO[Boolean] =
    for
      began <- IO.delay(new CountDownLatch(1))
      interrupted <- IO.delay(new AtomicBoolean(false))
      fiber <- IO.interruptible {
        began.countDown()
        try Thread.sleep(60_000)
        catch
          case error: InterruptedException => interrupted.set(true); throw error
      }.start
      _ <- IO.blocking(began.await())
      _ <- fiber.cancel
      result <- IO.delay(interrupted.get())
    yield result
