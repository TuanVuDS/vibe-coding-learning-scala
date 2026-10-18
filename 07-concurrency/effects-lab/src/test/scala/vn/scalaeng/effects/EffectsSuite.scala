package vn.scalaeng.effects

import cats.effect.{Deferred, IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class EffectsSuite extends CatsEffectSuite:
  test("Future eager và IO lazy: chạy lại mô tả không phải cache kết quả") {
    Comparisons.eager.map(value => assertEquals(value, (1, 0, 1, 2)))
  }

  test("cancel đợi onCancel, join trả Canceled, không ghi sau khi mở gate") {
    Comparisons.cancellation.map(value => assertEquals(value, (true, 0, true)))
  }

  test("Future timeout vẫn ghi; IO timeout hủy và hoàn tất finalizer") {
    Comparisons.timeout.map(value => assertEquals(value, (true, 0, 1, true, 0, true)))
  }

  test("retry Future cần factory; retry IO chạy lại mô tả") {
    Comparisons.retries.map(value => assertEquals(value, (1, 3, 3)))
  }

  test("Using quanh Future đóng quá sớm; Resource đóng ngược sau async error") {
    Comparisons.resources.map { case (closed, events, error) =>
      assert(closed)
      assertEquals(events, Vector("open connection", "open statement", "close statement", "close connection"))
      assertEquals(error, "java.lang.IllegalStateException: query failed")
    }
  }

  test("Resource đóng đủ khi cancellation, không chỉ khi error") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      ready <- Deferred[IO, Unit]
      fiber <- Comparisons.nestedResources(log).use(_ => ready.complete(()) *> IO.never[Unit]).start
      _ <- ready.get
      _ <- fiber.cancel
      events <- log.get
    yield assertEquals(events, Vector("open connection", "open statement", "close statement", "close connection"))
  }

  test("blocking tách compute; join fiber không Await") {
    Comparisons.starvation.map(value => assertEquals(value, (true, 49, 42)))
  }

  test("interruptible gửi Thread.interrupt tới blocking call hỗ trợ nó") {
    Comparisons.interruptible.map(value => assert(value))
  }

  test("backoff có trần và zero retries") {
    IO {
      assertEquals(Retry.backoff(100.millis, 1.second, 6),
        List(100.millis, 200.millis, 400.millis, 800.millis, 1.second, 1.second))
      assertEquals(Retry.backoff(100.millis, 1.second, 0), Nil)
      assertEquals(Retry.backoff(1.nano, 3.nanos, 4), List(1.nano, 2.nanos, 3.nanos, 3.nanos))
      intercept[IllegalArgumentException](Retry.backoff(Duration.Zero, 1.second, 1))
    }
  }

  test("TestControl: retry lỗi tạm thời, thời gian ảo chính xác 100+200ms") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      begin <- IO.monotonic
      result <- Retry(count.updateAndGet(_ + 1).flatMap { n =>
        if n < 3 then IO.raiseError[Int](java.io.IOException("retry")) else IO.pure(n)
      }, Retry.backoff(100.millis, 1.second, 4))(_.isInstanceOf[java.io.IOException])
      end <- IO.monotonic
    yield
      assertEquals(result, 3)
      assertEquals(end - begin, 300.millis)
    )
  }

  test("retry không lặp lỗi nghiệp vụ") {
    for
      count <- Ref.of[IO, Int](0)
      result <- Retry(count.update(_ + 1) *> IO.raiseError[Int](IllegalArgumentException("bad sku")),
        List(1.second))(_.isInstanceOf[java.io.IOException]).attempt
      n <- count.get
    yield
      assertEquals(n, 1)
      assertEquals(result.left.map(_.getMessage), Left("bad sku"))
  }

  test("retry hết lượt giữ lỗi cuối cùng") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      result <- Retry(count.updateAndGet(_ + 1).flatMap(n =>
        IO.raiseError[Int](java.io.IOException(s"attempt $n"))), List(1.second, 2.seconds))(_ => true).attempt
      n <- count.get
    yield
      assertEquals(n, 3)
      assertEquals(result.left.map(_.getMessage), Left("attempt 3"))
    )
  }

  test("cancel trong backoff ngăn lần thử tiếp theo") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      began <- Deferred[IO, Unit]
      fiber <- Retry(count.update(_ + 1) *> began.complete(()).void *>
        IO.raiseError[Int](java.io.IOException("retry")), List(1.hour))(_ => true).start
      _ <- began.get
      _ <- IO.sleep(1.second)
      _ <- fiber.cancel
      _ <- IO.sleep(2.hours)
      n <- count.get
    yield assertEquals(n, 1))
  }

  test("uncancelable: poll mở lại vùng use; finalizer vẫn chạy") {
    for
      ready <- Deferred[IO, Unit]
      finalized <- Ref.of[IO, Boolean](false)
      fiber <- IO.uncancelable { poll =>
        poll(ready.complete(()) *> IO.never[Unit]).guarantee(finalized.set(true))
      }.start
      _ <- ready.get
      _ <- fiber.cancel
      done <- finalized.get
    yield assert(done)
  }

  test("Ref giữ đủ 1000 cập nhật đồng thời") {
    for
      count <- Ref.of[IO, Int](0)
      _ <- List.fill(1000)(count.update(_ + 1)).parSequence_
      n <- count.get
    yield assertEquals(n, 1000)
  }

  test("Deferred complete chỉ thắng một lần; nhiều reader cùng giá trị") {
    for
      signal <- Deferred[IO, Int]
      readers <- List.fill(3)(signal.get).parSequence.start
      first <- signal.complete(42)
      second <- signal.complete(99)
      result <- readers.joinWithNever
    yield
      assert(first)
      assert(!second)
      assertEquals(result, List(42, 42, 42))
  }

  test("pool giới hạn connection đang mượn và trả permit sau lỗi") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      active <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      _ <- FakePool.resource(2, log).use { pool =>
        val query = pool.connection.use { _ =>
          (active.updateAndGet(_ + 1).flatMap(n => peak.update(_.max(n))) *>
            IO.cede *> IO.raiseError[Unit](java.io.IOException("query")))
            .guarantee(active.update(_ - 1))
        }.attempt
        List.fill(20)(query).parSequence_ *> pool.connection.use(_ => IO.unit)
      }
      max <- peak.get
      n <- active.get
      events <- log.get
    yield
      assert(max >= 1 && max <= 2)
      assertEquals(n, 0)
      assertEquals(events.count(_.startsWith("open connection")), 21)
      assertEquals(events.count(_.startsWith("close connection")), 21)
      assertEquals(events.last, "close pool")
  }

  test("quote chạy đồng thời ba dịch vụ: gate bắt buộc cả ba bắt đầu") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      count <- Ref.of[IO, Long](0L)
      starts <- Ref.of[IO, Int](0)
      allStarted <- Deferred[IO, Unit]
      service = starts.updateAndGet(_ + 1).flatMap(n =>
        (if n == 3 then allStarted.complete(()).void else IO.unit) *> allStarted.get)
      result <- FakePool.resource(3, log).use(pool => Checkout.quote(
        Services(service.as(120_000L), service.as(3), service.as(20_000L)), pool, count, 5.seconds))
      n <- count.get
    yield
      assertEquals(result, Quote(120_000L, 3, 20_000L))
      assertEquals(result.payable, 100_000L)
      assertEquals(n, 1L)
  }

  test("TestControl: stock và promotion timeout dùng fallback, giá vẫn bắt buộc") {
    TestControl.executeEmbed(for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      count <- Ref.of[IO, Long](0L)
      canceled <- Ref.of[IO, Int](0)
      result <- FakePool.resource(3, log).use(pool => Checkout.quote(
        Services(IO.pure(120_000L), IO.never[Int].onCancel(canceled.update(_ + 1)),
          IO.never[Long].onCancel(canceled.update(_ + 1))), pool, count, 1.second))
      n <- canceled.get
      events <- log.get
    yield
      assertEquals(result, Quote(120_000L, 0, 0L))
      assertEquals(n, 2)
      assertEquals(events.count(_.startsWith("close connection")), 3)
    )
  }

  test("parMapN lỗi giá hủy hai sibling và trả mọi connection") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      count <- Ref.of[IO, Long](0L)
      stockReady <- Deferred[IO, Unit]
      promoReady <- Deferred[IO, Unit]
      canceled <- Ref.of[IO, Int](0)
      services = Services(
        stockReady.get *> promoReady.get *> IO.raiseError[Long](IllegalArgumentException("unknown sku X")),
        (stockReady.complete(()) *> IO.never[Int]).onCancel(canceled.update(_ + 1)),
        (promoReady.complete(()) *> IO.never[Long]).onCancel(canceled.update(_ + 1))
      )
      result <- FakePool.resource(3, log).use(pool => Checkout.quote(services, pool, count, 5.seconds)).attempt
      n <- canceled.get
      events <- log.get
    yield
      assertEquals(result.left.map(_.getMessage), Left("unknown sku X"))
      assertEquals(n, 2)
      assertEquals(events.count(_.startsWith("close connection")), 3)
      assertEquals(events.last, "close pool")
  }

  test("timeout giá trả lỗi thay vì giá giả") {
    TestControl.executeEmbed(for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      count <- Ref.of[IO, Long](0L)
      result <- FakePool.resource(3, log).use(pool => Checkout.quote(
        Services(IO.never[Long], IO.pure(3), IO.pure(0L)), pool, count, 1.second)).attempt
    yield assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException])))
  }

  test("attempt và handleErrorWith; pure không trì hoãn biểu thức") {
    val failure = IO.raiseError[Int](IllegalArgumentException("unknown sku X"))
    for
      error <- failure.attempt
      fallback <- failure.handleErrorWith(_ => IO.pure(10))
    yield
      assertEquals(error.left.map(_.getMessage), Left("unknown sku X"))
      assertEquals(fallback, 10)
      intercept[ArithmeticException](IO.pure(1 / 0))
  }
