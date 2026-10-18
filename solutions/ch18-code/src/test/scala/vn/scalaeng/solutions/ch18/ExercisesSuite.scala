package vn.scalaeng.solutions.ch18

import cats.effect.{IO,Ref,Deferred}
import cats.effect.testkit.TestControl
import scala.concurrent.duration.*
import vn.scalaeng.p2.domain.*
import Exercises.*

class ExercisesSuite extends munit.CatsEffectSuite:
  private val id=UserId.from(1).toOption.get
  private val other=UserId.from(2).toOption.get
  test("1 — owner, admin, stranger") {
    IO {assert(allowed(Principal(id,false),id).isRight);assert(allowed(Principal(other,true),id).isRight);assertEquals(allowed(Principal(other,false),id),Left(DomainError.Forbidden))}
  }
  test("2 — tenant và currency không đụng cache key") {
    IO {assertNotEquals(key("shop-a",1,"VND"),key("shop-b",1,"VND"));assertNotEquals(key("shop-a",1,"VND"),key("shop-a",1,"USD"));assertEquals(key("shop-a",1," vnd "),key("shop-a",1,"VND"));assert(key("",0,"EUR").isLeft)}
  }
  test("3 — retry read chỉ 40001/40P01, không retry 23505 hay lỗi lập trình") {
    IO {
      assert(transientRead(new RuntimeException(new java.sql.SQLException("serialization","40001"))))
      assert(transientRead(new java.sql.SQLException("deadlock","40P01")))
      assert(!transientRead(new java.sql.SQLException("unique","23505")))
      assert(!transientRead(new IllegalArgumentException("bad input")))
    }
  }
  test("4 — ngân sách toàn cục chặn retry kéo dài và hủy read") {
    TestControl.executeEmbed(for
      n <- Ref.of[IO,Int](0)
      read=n.update(_+1) *> IO.sleep(1.second) *> IO.raiseError[Int](new java.sql.SQLException("retry","40001"))
      a <- readWithin(read,1500.millis).attempt
      count <- n.get
    yield {assert(a.swap.toOption.get.isInstanceOf[java.util.concurrent.TimeoutException]);assertEquals(count,2)})
  }
  test("5 — gauge về 0 khi thành công, lỗi và cancel") {
    for
      active <- Ref.of[IO,Int](0)
      _ <- tracked(active)(IO.unit)
      _ <- tracked(active)(IO.raiseError[Unit](new RuntimeException("failure"))).attempt
      started <- Deferred[IO,Unit]
      fiber <- tracked(active)(started.complete(()) *> IO.never[Unit]).start
      _ <- started.get
      during <- active.get
      _ <- fiber.cancel
      end <- active.get
    yield {assertEquals(during,1);assertEquals(end,0)}
  }
  test("6 — chỉ commit thành công invalidate; domain error và failed IO không invalidate") {
    for
      n <- Ref.of[IO,Int](0)
      _ <- commitThenInvalidate(IO.pure(Right(1)),n.update(_+1))
      _ <- commitThenInvalidate(IO.pure(Left(DomainError.OutOfStock)),n.update(_+1))
      _ <- commitThenInvalidate(IO.raiseError[Either[DomainError,Int]](new RuntimeException("rollback")),n.update(_+1)).attempt
      calls <- n.get
    yield assertEquals(calls,1)
  }
