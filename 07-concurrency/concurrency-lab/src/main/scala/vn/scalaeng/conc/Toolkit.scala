package vn.scalaeng.conc

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Try

/** Các công cụ concurrency viết bằng standard library — để thấy rõ những gì Cats Effect/ZIO làm sẵn (Chương 16B). */
object Toolkit:
  /** Thread factory đặt tên thread: thread dump và log đọc được. Daemon để không giữ JVM sống. */
  def named(prefix: String): ThreadFactory =
    val n = AtomicInteger(0)
    r => { val t = Thread(r, s"$prefix-${n.incrementAndGet()}"); t.setDaemon(true); t }

  def fixedPool(prefix: String, threads: Int): ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads, named(prefix)))

  val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(named("scheduler"))

  /** Future hoàn thành sau `d` mà KHÔNG chiếm thread nào trong lúc chờ (khác Thread.sleep). */
  def delay[A](d: FiniteDuration)(value: => A): Future[A] =
    val p = Promise[A]()
    scheduler.schedule((() => p.complete(Try(value))): Runnable, d.toMillis, TimeUnit.MILLISECONDS)
    p.future

  /** Timeout không chặn thread: ai xong trước thắng.
    * LƯU Ý: Future gốc KHÔNG bị hủy — nó vẫn chạy tiếp. Future trong Scala không có cancellation.
    */
  def withTimeout[A](f: Future[A], d: FiniteDuration)(using ExecutionContext): Future[A] =
    val p = Promise[A]()
    val task = scheduler.schedule((() => { p.tryFailure(TimeoutException(s"quá $d")); () }): Runnable, d.toMillis, TimeUnit.MILLISECONDS)
    f.onComplete { r => task.cancel(false); p.tryComplete(r); () }
    p.future

  /** Độ trễ backoff lũy thừa có trần — hàm THUẦN, test được không cần chờ. */
  def backoffDelays(base: FiniteDuration, factor: Double, max: FiniteDuration, retries: Int): List[FiniteDuration] =
    List.iterate(base.toMillis.toDouble, retries)(_ * factor).map(ms => math.min(ms, max.toMillis.toDouble).toLong.millis)

  /** Retry với backoff; chỉ retry khi `retryable(e)` — lỗi nghiệp vụ không nên retry. */
  def retry[A](delays: List[FiniteDuration], retryable: Throwable => Boolean = _ => true)(action: () => Future[A])(using ExecutionContext): Future[A] =
    action().recoverWith {
      case e if retryable(e) && delays.nonEmpty => delay(delays.head)(()).flatMap(_ => retry(delays.tail, retryable)(action))
    }
