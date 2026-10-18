package vn.scalaeng.conc

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Using}

private def timed[A](body: => A): (A, Long) =
  val t0 = System.nanoTime(); val a = body; (a, (System.nanoTime() - t0) / 1_000_000)

/** Thí nghiệm 1: race condition trên biến dùng chung. */
@main def raceLab(): Unit =
  val threads = 4; val perThread = 2_000_000
  var unsafe = 0L
  val lock = Object(); var locked = 0L
  val atomic = AtomicLong(0)
  def runAll(body: () => Unit): Long =
    val ts = (1 to threads).map(_ => Thread(() => (1 to perThread).foreach(_ => body())))
    timed { ts.foreach(_.start()); ts.foreach(_.join()) }._2
  val t1 = runAll(() => unsafe += 1)
  val t2 = runAll(() => lock.synchronized { locked += 1 })
  val t3 = runAll(() => { atomic.incrementAndGet(); () })
  val expected = threads.toLong * perThread
  println(s"CPU: ${Runtime.getRuntime.availableProcessors()}, kỳ vọng: $expected")
  println(f"var không đồng bộ : $unsafe%,d  (mất ${expected - unsafe}%,d)  ${t1}ms")
  println(f"synchronized      : $locked%,d  ${t2}ms")
  println(f"AtomicLong        : ${atomic.get}%,d  ${t3}ms")

  // Mở rộng "cửa sổ race": đọc -> nhường CPU -> ghi. Mô phỏng thread bị hệ điều hành tạm dừng giữa đọc và ghi.
  val n2 = 20_000
  var widened = 0L
  val ts = (1 to threads).map(_ => Thread(() => (1 to n2).foreach { _ => val v = widened; Thread.`yield`(); widened = v + 1 }))
  ts.foreach(_.start()); ts.foreach(_.join())
  val exp2 = threads.toLong * n2
  println(f"var + cửa sổ race : $widened%,d / $exp2%,d  (mất ${exp2 - widened}%,d)")

/** Thí nghiệm 2: Future chạy NGAY khi được tạo (eager) — không phải "mô tả" phép tính. */
@main def eagerLab(): Unit =
  given ExecutionContext = ExecutionContext.global
  val log = java.util.concurrent.ConcurrentLinkedQueue[String]()
  log.add("1. trước khi tạo Future")
  val _ = Future { log.add("2. thân Future đang chạy"); 42 }   // không giữ tham chiếu — vẫn chạy
  Thread.sleep(100)
  log.add("3. sau khi tạo, CHƯA gọi map hay Await")
  println(log.toArray.mkString("\n"))
  // Hệ quả: hai val dưới đây là HAI lần gửi email, không phải "một mô tả dùng hai lần"
  val sends = AtomicInteger(0)
  def sendEmail(): Future[Unit] = Future { sends.incrementAndGet(); () }
  val a = sendEmail(); val b = sendEmail()
  Await.result(a.zip(b), 1.second)
  println(s"Số email đã gửi: ${sends.get}")

/** Thí nghiệm 3: tuần tự vs song song với tác vụ I/O (chờ) — và cái bẫy pool nhỏ. */
@main def ioLab(): Unit =
  def call(name: String)(using ExecutionContext): Future[String] = Future { Thread.sleep(300); name }   // giả lập I/O CHẶN thread
  def callBlocking(name: String)(using ExecutionContext): Future[String] = Future { blocking { Thread.sleep(300) }; name }

  locally {
    given ExecutionContext = ExecutionContext.global
    val (_, seq) = timed(Await.result(for a <- call("A"); b <- call("B"); c <- call("C") yield List(a, b, c), 5.seconds))
    println(s"global, for-comprehension (tuần tự)             : ${seq}ms")
    val (_, par) = timed { val fa = call("A"); val fb = call("B"); val fc = call("C"); Await.result(Future.sequence(List(fa, fb, fc)), 5.seconds) }
    println(s"global, tạo trước rồi gộp (song song?)            : ${par}ms")
    val (_, blk) = timed(Await.result(Future.sequence(List("A", "B", "C").map(callBlocking)), 5.seconds))
    println(s"global + blocking {}                              : ${blk}ms")
  }
  locally {
    given ExecutionContext = Toolkit.fixedPool("io", 8)
    val (_, io) = timed(Await.result(Future.sequence(List("A", "B", "C").map(call)), 5.seconds))
    println(s"pool I/O riêng 8 thread                           : ${io}ms")
  }
  locally {
    given ExecutionContext = ExecutionContext.global
    val (_, nb) = timed(Await.result(Future.sequence(List("A", "B", "C").map(n => Toolkit.delay(300.millis)(n))), 5.seconds))
    println(s"global, chờ KHÔNG chặn thread (scheduler)          : ${nb}ms")
  }

/** Thí nghiệm 4: song song cho tác vụ CPU — phụ thuộc số CPU thật. */
@main def cpuLab(): Unit =
  def countPrimes(from: Int, to: Int): Int =
    (from until to).count(n => n > 1 && (2 to math.sqrt(n.toDouble).toInt).forall(n % _ != 0))
  val ranges = (0 until 4).map(i => (i * 500_000, (i + 1) * 500_000)).toList
  countPrimes(0, 200_000): Unit   // warm-up JIT
  val (s1, tSeq) = timed(ranges.map(countPrimes).sum)
  given ExecutionContext = Toolkit.fixedPool("cpu", 4)
  val (s2, tPar) = timed(Await.result(Future.traverse(ranges)((a, b) => Future(countPrimes(a, b))).map(_.sum), 60.seconds))
  println(s"CPU khả dụng: ${Runtime.getRuntime.availableProcessors()}")
  println(s"tuần tự : $s1 số nguyên tố, ${tSeq}ms")
  println(s"4 Future: $s2 số nguyên tố, ${tPar}ms  (tăng tốc x${"%.2f".format(tSeq.toDouble / tPar)})")

/** Thí nghiệm 5: lỗi lan truyền và phục hồi. */
@main def errorLab(): Unit =
  given ExecutionContext = ExecutionContext.global
  def price(sku: String): Future[Long] =
    if sku == "X" then Future.failed(IllegalArgumentException(s"không có sku $sku")) else Future.successful(100L)
  def show[A](label: String, f: Future[A]): Unit = println(f"$label%-34s ${Await.ready(f, 1.second).value.get}")
  show("map trên Future lỗi:", price("X").map(_ * 2))
  show("recover:", price("X").recover { case _: IllegalArgumentException => 0L })
  show("recoverWith sang nguồn khác:", price("X").recoverWith { case _ => price("DEFAULT") })
  show("traverse, một phần tử lỗi:", Future.traverse(List("A", "X", "B"))(price))
  show("exception trong map:", Future.successful(1).map(_ / 0))
  show("transform thành Try:", price("X").transform(t => Success(t.toEither.left.map(_.getMessage))))

/** Thí nghiệm 6: timeout KHÔNG hủy công việc đang chạy. */
@main def timeoutLab(): Unit =
  given ExecutionContext = Toolkit.fixedPool("work", 2)
  val sideEffects = AtomicInteger(0)
  val slow = Future { Thread.sleep(500); sideEffects.incrementAndGet(); "ghi DB xong" }
  val guarded = Toolkit.withTimeout(slow, 200.millis)
  println(s"Kết quả có timeout: ${Await.ready(guarded, 2.seconds).value.get}")
  println(s"Ngay sau timeout, side effect = ${sideEffects.get}")
  Thread.sleep(500)
  println(s"0.5 giây sau,      side effect = ${sideEffects.get}  <- công việc VẪN chạy và VẪN ghi DB")

/** Thí nghiệm 7: retry với backoff. */
@main def retryLab(): Unit =
  given ExecutionContext = ExecutionContext.global
  val attempts = AtomicInteger(0)
  val delays = Toolkit.backoffDelays(100.millis, 2.0, 1.second, retries = 4)
  println(s"Lịch chờ: ${delays.map(_.toMillis).mkString(", ")} ms")
  val flaky = () => Future {
    val n = attempts.incrementAndGet()
    if n < 3 then throw java.io.IOException(s"timeout kết nối lần $n") else s"thành công ở lần $n"
  }
  val (r, ms) = timed(Await.result(Toolkit.retry(delays, _.isInstanceOf[java.io.IOException])(flaky), 5.seconds))
  println(s"$r sau ${ms}ms")

/** Thí nghiệm 8: thread starvation — chờ chặn bên trong pool không còn thread trống. */
@main def starvationLab(): Unit =
  given ExecutionContext = Toolkit.fixedPool("tiny", 1)
  val outer = Future {
    val inner = Future(42)                 // xếp hàng trên CÙNG pool 1 thread
    Await.result(inner, 1.second)          // chặn thread duy nhất để chờ inner -> inner không bao giờ được chạy
  }
  println(s"Kết quả: ${Await.ready(outer, 3.seconds).value.get}")

/** Thí nghiệm 9: quản lý tài nguyên — đóng đúng thứ tự, kể cả khi lỗi. */
@main def resourceLab(): Unit =
  final class Res(name: String, log: StringBuilder) extends AutoCloseable:
    log.append(s"mở $name; ")
    def close(): Unit = { log.append(s"đóng $name; "); () }
  val log = StringBuilder()
  val r = Using.Manager { use =>
    use(Res("connection", log)); use(Res("statement", log))
    throw IllegalStateException("query lỗi")
  }
  println(log.toString)
  println(r match { case Failure(e) => s"Failure(${e.getMessage})"; case Success(_) => "Success" })
