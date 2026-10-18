package vn.scalaeng.solutions

import munit.FunSuite
import vn.scalaeng.http.*
import vn.scalaeng.conc.Toolkit
import java.util.concurrent.{Callable, Executors}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class BackendSolutionsSuite extends FunSuite:
  import Ch12Solutions.*, Ch16aSolutions.*

  test("12.2 sort ổn định và validate"):
    val ps = List(Product(3, "C", "c", 100), Product(1, "A", "a", 100), Product(2, "B", "b", 50))
    assertEquals(sortProducts(ps, Some("price_asc")).map(_.map(_.id)), Right(List(2L, 1L, 3L)))
    assertEquals(sortProducts(ps, Some("price_desc")).map(_.map(_.id)), Right(List(1L, 3L, 2L)))
    assertEquals(sortProducts(ps, None).map(_.map(_.id)), Right(List(1L, 2L, 3L)))
    assertEquals(sortProducts(ps, Some("name")).left.map(_.status), Left(400))

  test("12.3 ETag và 304"):
    val h = conditionalGet(_ => Response.json(200, Product(1, "A", "a", 1).toJson))
    val first = h(Request(Method.GET, "/x"))
    val tag = first.headers("ETag")
    val second = h(Request(Method.GET, "/x", headers = Map("if-none-match" -> tag)))
    assertEquals((second.status, second.body, second.headers.get("ETag")), (304, "", Some(tag)))
    assertEquals(h(Request(Method.GET, "/x", headers = Map("if-none-match" -> "\"khac\""))).status, 200)
    assertEquals(conditionalGet(_ => Response(201, body = "x"))(Request(Method.POST, "/x")).headers.get("ETag"), None)

  test("16A.1 rút tiền đồng thời không vượt số dư"):
    val acc = BankAccount(1_000)
    val pool = Executors.newFixedThreadPool(8)
    try
      val tasks = (1 to 200).map(_ => (() => acc.withdraw(7)): Callable[Boolean])
      val ok = pool.invokeAll(tasks.asJava).asScala.count(_.get)
      assertEquals(ok, 142)               // 142 * 7 = 994 <= 1000 < 143 * 7
      assertEquals(acc.current, 6L)
    finally pool.shutdown()

  given ExecutionContext = ExecutionContext.global

  test("16A.2 firstSuccessful"):
    val calls = AtomicInteger(0)
    def fail() = Future { calls.incrementAndGet(); throw RuntimeException("x") }
    assertEquals(Await.result(firstSuccessful(List(() => fail(), () => Future("B"), () => Future("C"))), 1.second), "B")
    assertEquals(calls.get, 1)
    intercept[RuntimeException](Await.result(firstSuccessful(List(() => fail(), () => fail())), 1.second))

  test("16A.3 traverseLimited: giữ thứ tự, không vượt giới hạn đồng thời"):
    val inFlight = AtomicInteger(0); val maxSeen = AtomicInteger(0)
    val r = traverseLimited((1 to 20).toList, 3) { i =>
      val now = inFlight.incrementAndGet()
      maxSeen.accumulateAndGet(now, math.max)
      Toolkit.delay(20.millis)(i * 10).andThen(_ => inFlight.decrementAndGet())
    }
    assertEquals(Await.result(r, 5.seconds), (1 to 20).map(_ * 10).toList)
    assertEquals(maxSeen.get, 3)
    assertEquals(Await.result(traverseLimited(List.empty[Int], 3)(Future(_)), 1.second), Nil)

  test("16A.4 circuit breaker: Closed -> Open -> HalfOpen -> Closed/Open"):
    val p = BreakerPolicy(maxFailures = 3, openMillis = 1000)
    val s3 = (1 to 3).foldLeft[Breaker](Breaker.Closed(0))((s, _) => p.onFailure(s, now = 0))
    assertEquals(s3, Breaker.Open(1000))
    assertEquals(p.allow(s3, now = 999), (s3, false))
    val (half, ok) = p.allow(s3, now = 1000)
    assertEquals((half, ok), (Breaker.HalfOpen, true))
    assertEquals(p.allow(half, 1001)._2, false)
    assertEquals(p.reset, Breaker.Closed(0))
    assertEquals(p.onFailure(half, 2000), Breaker.Open(3000))

  test("16A.5 không Await -> không starvation trên pool 1 thread"):
    val tiny: ExecutionContext = Toolkit.fixedPool("tiny", 1)
    assertEquals(Await.result(noStarvation(using tiny), 1.second), 42)
