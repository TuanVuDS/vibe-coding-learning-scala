package vn.scalaeng.effectsolutions

import cats.effect.{Deferred, IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*
import vn.scalaeng.effects.{Quote, Services}

class ExercisesSuite extends CatsEffectSuite:
  test("bài 1: hai lần chạy, không chạy nếu times=0") {
    for
      count <- Ref.of[IO, Int](0)
      empty <- Exercises.repeat(count.updateAndGet(_ + 1), 0)
      values <- Exercises.repeat(count.updateAndGet(_ + 1), 2)
      invalid <- Exercises.repeat(IO.unit, -1).attempt
    yield
      assertEquals(empty, Nil)
      assertEquals(values, List(1, 2))
      assert(invalid.isLeft)
  }

  test("bài 2: fallback chỉ cho timeout, giữ lỗi nghiệp vụ") {
    for
      fallback <- Exercises.promotion(IO.raiseError(new java.util.concurrent.TimeoutException("slow")))
      success <- Exercises.promotion(IO.pure(20_000L))
      error <- Exercises.promotion(IO.raiseError(IllegalArgumentException("bad discount"))).attempt
    yield
      assertEquals(fallback, 0L)
      assertEquals(success, 20_000L)
      assertEquals(error.left.map(_.getMessage), Left("bad discount"))
  }

  test("bài 3: release ngược khi lỗi") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- Exercises.session(log).use(_ => IO.raiseError[Unit](java.io.IOException("broken"))).attempt
      events <- log.get
    yield
      assert(result.isLeft)
      assertEquals(events, Vector("open connection", "open statement", "close statement", "close connection"))
  }

  test("bài 3: release ngược khi hủy") {
    for
      log <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      fiber <- Exercises.session(log).use(_ => started.complete(()) *> IO.never[Unit]).start
      _ <- started.get
      _ <- fiber.cancel
      events <- log.get
    yield assertEquals(events.takeRight(2), Vector("close statement", "close connection"))
  }

  test("bài 4: đúng giới hạn và giữ thứ tự input dù thời gian khác nhau") {
    TestControl.executeEmbed(for
      active <- Ref.of[IO, Int](0)
      peak <- Ref.of[IO, Int](0)
      values <- Exercises.boundedTraverse(List(3, 2, 1, 4), 2) { n =>
        (active.updateAndGet(_ + 1).flatMap(k => peak.update(_.max(k))) *>
          IO.sleep(n.seconds).as(n * 10)).guarantee(active.update(_ - 1))
      }
      p <- peak.get
      remaining <- active.get
    yield
      assertEquals(values, List(30, 20, 10, 40))
      assertEquals(p, 2)
      assertEquals(remaining, 0)
    )
  }

  test("bài 4: lỗi một action hủy sibling và chạy finalizer") {
    for
      began <- Deferred[IO, Unit]
      cleaned <- Ref.of[IO, Boolean](false)
      result <- Exercises.boundedTraverse(List(1, 2), 2) {
        case 1 => (began.complete(()) *> IO.never[Int]).onCancel(cleaned.set(true))
        case _ => began.get *> IO.raiseError[Int](java.io.IOException("failed"))
      }.attempt
      done <- cleaned.get
    yield
      assert(result.isLeft)
      assert(done)
  }

  test("bài 4: empty và limit không hợp lệ") {
    for
      empty <- Exercises.boundedTraverse(List.empty[Int], 2)(IO.pure)
      invalid <- Exercises.boundedTraverse(List(1), 0)(IO.pure).attempt
    yield
      assertEquals(empty, Nil)
      assert(invalid.isLeft)
  }

  test("bài 5: retry 2 lần, tổng chờ ảo 300ms") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      begin <- IO.monotonic
      result <- Exercises.retryRead(count.updateAndGet(_ + 1).flatMap { n =>
        if n < 3 then IO.raiseError[Int](java.io.IOException("temporary")) else IO.pure(n)
      }, 2)
      end <- IO.monotonic
    yield
      assertEquals(result, 3)
      assertEquals(end - begin, 300.millis)
    )
  }

  test("bài 5: không retry validation; hết lượt giữ lỗi cuối") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO, Int](0)
      validation <- Exercises.retryRead(count.update(_ + 1) *>
        IO.raiseError[Int](IllegalArgumentException("bad input")), 5).attempt
      n <- count.get
      exhausted <- Exercises.retryRead(count.updateAndGet(_ + 1).flatMap(k =>
        IO.raiseError[Int](java.io.IOException(s"attempt $k"))), 1).attempt
    yield
      assertEquals(validation.left.map(_.getMessage), Left("bad input"))
      assertEquals(n, 1)
      assertEquals(exhausted.left.map(_.getMessage), Left("attempt 3"))
    )
  }

  test("bài 6: batch có fallback, counter chính xác, pool đóng sau connections") {
    TestControl.executeEmbed(for
      result <- Exercises.batch(List.fill(4)(Services(IO.pure(120_000L), IO.pure(3), IO.never[Long])),
        concurrency = 2, poolSize = 3, budget = 1.second)
    yield
      assertEquals(result.quotes, List.fill(4)(Quote(120_000L, 3, 0L)))
      assertEquals(result.requests, 4L)
      assertEquals(result.events.head, "open pool")
      assertEquals(result.events.last, "close pool")
      val opens = result.events.count(_.startsWith("open connection"))
      assert(opens > 0)
      assertEquals(result.events.count(_.startsWith("close connection")), opens)
    )
  }

  test("bài 6: giá lỗi làm batch thất bại và giữ nguyên lỗi") {
    Exercises.batch(List(Services(IO.raiseError(IllegalArgumentException("missing price")),
      IO.pure(1), IO.pure(0L))), 1, 3, 1.second).attempt.map { result =>
      assertEquals(result.left.map(_.getMessage), Left("missing price"))
    }
  }

  test("bài 6: batch rỗng không gọi service, vẫn đóng pool") {
    Exercises.batch(Nil, 2, 3, 1.second).map { result =>
      assertEquals(result.quotes, Nil)
      assertEquals(result.requests, 0L)
      assertEquals(result.events, Vector("open pool", "close pool"))
    }
  }
