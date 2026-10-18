package vn.scalaeng.p2.security

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import scala.concurrent.duration.*

class RateLimiterSuite extends munit.CatsEffectSuite:
  test("giới hạn số IP; không đẩy quota đang hoạt động ra để né rate limit") {
    TestControl.executeEmbed(for
      limiter <- RateLimiter.make(1,1.minute,maxBuckets=2)
      first <- List("a","b","c","a").traverse(limiter.acquire)
      size <- limiter.bucketCount
      _ <- IO.sleep(1.minute)
      next <- limiter.acquire("c")
      after <- limiter.bucketCount
    yield {
      assertEquals(first.map(_.isRight),List(true,true,false,false))
      assertEquals(size,2);assert(next.isRight);assertEquals(after,1)
    })
  }
  test("refill giữ đúng quota và chờ token tiếp theo") {
    TestControl.executeEmbed(for
      limiter <- RateLimiter.make(2,1.minute)
      first <- List.fill(3)(limiter.acquire("a")).sequence
      _ <- IO.sleep(30.seconds)
      next <- limiter.acquire("a")
      last <- limiter.acquire("a")
    yield {
      assertEquals(first.take(2),List(Right(()),Right(())))
      assertEquals(first.last,Left(30.seconds));assert(next.isRight);assertEquals(last,Left(30.seconds))
    })
  }
