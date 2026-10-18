package vn.scalaeng.conc

import munit.FunSuite
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.Using

class ToolkitSuite extends FunSuite:
  given ExecutionContext = ExecutionContext.global

  test("backoffDelays: lũy thừa có trần (thuần, không cần chờ)"):
    assertEquals(Toolkit.backoffDelays(100.millis, 2.0, 1.second, 6).map(_.toMillis), List(100L, 200L, 400L, 800L, 1000L, 1000L))
    assertEquals(Toolkit.backoffDelays(100.millis, 2.0, 1.second, 0), Nil)

  test("retry: thành công sau 2 lần lỗi tạm thời"):
    val n = AtomicInteger(0)
    val f = Toolkit.retry(List(10.millis, 10.millis, 10.millis))(() =>
      Future { if n.incrementAndGet() < 3 then throw java.io.IOException("tạm thời") else "ok" })
    assertEquals(Await.result(f, 2.seconds), "ok")
    assertEquals(n.get, 3)

  test("retry: KHÔNG retry lỗi không thuộc loại retryable"):
    val n = AtomicInteger(0)
    val f = Toolkit.retry(List(10.millis, 10.millis), _.isInstanceOf[java.io.IOException])(() =>
      Future { n.incrementAndGet(); throw IllegalArgumentException("dữ liệu sai") })
    intercept[IllegalArgumentException](Await.result(f, 2.seconds))
    assertEquals(n.get, 1)

  test("retry: hết lượt thì trả lỗi cuối cùng"):
    val n = AtomicInteger(0)
    val f = Toolkit.retry(List(5.millis, 5.millis))(() => Future { throw java.io.IOException(s"lần ${n.incrementAndGet()}") })
    assertEquals(intercept[java.io.IOException](Await.result(f, 2.seconds)).getMessage, "lần 3")

  test("withTimeout: lỗi TimeoutException; Future nhanh thì giữ kết quả"):
    intercept[TimeoutException](Await.result(Toolkit.withTimeout(Toolkit.delay(1.second)(1), 50.millis), 2.seconds))
    assertEquals(Await.result(Toolkit.withTimeout(Future.successful(7), 1.second), 2.seconds), 7)

  test("delay không chiếm thread: 50 lần chờ 200ms song song xong trong ~200ms"):
    val t0 = System.nanoTime()
    Await.result(Future.sequence((1 to 50).map(i => Toolkit.delay(200.millis)(i))), 5.seconds)
    val ms = (System.nanoTime() - t0) / 1_000_000
    assert(ms < 1500, s"mất ${ms}ms")

  test("AtomicLong đúng dưới đồng thời"):
    val c = AtomicLong(0)
    val ts = (1 to 4).map(_ => Thread(() => (1 to 100_000).foreach(_ => c.incrementAndGet())))
    ts.foreach(_.start()); ts.foreach(_.join())
    assertEquals(c.get, 400_000L)

  test("Using.Manager đóng tài nguyên theo thứ tự ngược, kể cả khi lỗi"):
    val log = scala.collection.mutable.ListBuffer.empty[String]
    final class R(n: String) extends AutoCloseable:
      log += s"open $n"
      def close(): Unit = { log += s"close $n"; () }
    val r = Using.Manager { use => use(R("a")); use(R("b")); throw RuntimeException("x") }
    assert(r.isFailure)
    assertEquals(log.toList, List("open a", "open b", "close b", "close a"))

  test("Future.traverse thất bại nếu một phần tử thất bại"):
    val f = Future.traverse(List(1, 0, 2))(d => Future(10 / d))
    intercept[ArithmeticException](Await.result(f, 1.second))
