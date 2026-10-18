package vn.scalaeng.p2.service

import cats.effect.{IO,Ref,Deferred}
import cats.effect.testkit.TestControl
import scala.concurrent.duration.*

class ResilienceSuite extends munit.CatsEffectSuite:
  test("cache hit, hết TTL và invalidate") {
    TestControl.executeEmbed(for
      count <- Ref.of[IO,Int](0)
      c <- TtlCache.make[String,Int](1.second,2)
      load = count.updateAndGet(_+1)
      a <- c.get("p")(load)
      b <- c.get("p")(load)
      _ <- IO.sleep(2.seconds)
      d <- c.get("p")(load)
      _ <- c.invalidate
      e <- c.get("p")(load)
    yield assertEquals(List(a,b,d,e),List(1,1,2,3)))
  }
  test("invalidate ngăn stale in-flight fill quay lại cache") {
    for
      c <- TtlCache.make[String,Int](1.minute,2)
      started <- Deferred[IO,Unit]
      gate <- Deferred[IO,Unit]
      f <- c.get("p")(started.complete(()) *> gate.get.as(1)).start
      _ <- started.get *> c.invalidate *> gate.complete(())
      _ <- f.joinWithNever
      value <- c.get("p")(IO.pure(2))
    yield assertEquals(value,2)
  }
  test("retry chỉ lỗi tạm thời, giới hạn số lần và backoff") {
    TestControl.executeEmbed(for
      n <- Ref.of[IO,Int](0)
      read = n.updateAndGet(_+1).flatMap(i => if i<3 then IO.raiseError[Int](new java.sql.SQLTransientException("tạm")) else IO.pure(i))
      v <- Resilience.retryRead(read,2,10.millis)(_.isInstanceOf[java.sql.SQLTransientException])
      bad <- Resilience.retryRead(n.update(_+1) *> IO.raiseError[Int](new IllegalArgumentException("không retry")),3,1.second)(_.isInstanceOf[java.sql.SQLTransientException]).attempt
      count <- n.get
    yield {assertEquals(v,3);assert(bad.isLeft);assertEquals(count,4)})
  }
  test("bulkhead từ chối quá tải và trả permit khi cancel") {
    for
      b <- Bulkhead.make(1)
      entered <- Deferred[IO,Unit]
      f <- b(entered.complete(()) *> IO.never[Unit]).start
      _ <- entered.get
      rejected <- b(IO.unit).attempt
      _ <- f.cancel
      accepted <- b(IO.pure(42))
    yield {assert(rejected.swap.toOption.get.isInstanceOf[Saturated]);assertEquals(accepted,42)}
  }
  test("timeout hủy công việc và trả permit") {
    TestControl.executeEmbed(for
      b <- Bulkhead.make(1)
      result <- b(IO.never[Int]).timeout(1.second).attempt
      ok <- b(IO.pure(7))
    yield {assert(result.isLeft);assertEquals(ok,7)})
  }
