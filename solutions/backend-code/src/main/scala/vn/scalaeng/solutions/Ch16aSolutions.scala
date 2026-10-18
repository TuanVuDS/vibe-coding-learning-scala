package vn.scalaeng.solutions

import java.util.NoSuchElementException
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

object Ch16aSolutions:
  // Bài 1: số dư không âm, an toàn đa luồng bằng CAS
  final class BankAccount(initial: Long):
    private val balance = AtomicReference[java.lang.Long](initial)
    def current: Long = balance.get()
    def deposit(amount: Long): Unit =
      require(amount > 0); balance.updateAndGet(b => b + amount); ()
    @tailrec final def withdraw(amount: Long): Boolean =
      require(amount > 0)
      val b = balance.get()
      if b < amount then false
      else if balance.compareAndSet(b, b - amount) then true
      else withdraw(amount)

  // Bài 2: thử lần lượt, trả nguồn thành công đầu tiên
  def firstSuccessful[A](sources: List[() => Future[A]])(using ExecutionContext): Future[A] =
    sources match
      case Nil          => Future.failed(NoSuchElementException("không có nguồn nào"))
      case last :: Nil  => last()
      case head :: tail => head().recoverWith { case _ => firstSuccessful(tail) }

  // Bài 3: tối đa `maxConcurrent` tác vụ cùng lúc; mỗi "worker" lấy việc tiếp theo khi xong việc cũ
  def traverseLimited[A, B](xs: List[A], maxConcurrent: Int)(f: A => Future[B])(using ExecutionContext): Future[List[B]] =
    require(maxConcurrent > 0)
    val items = xs.toVector
    val next = AtomicInteger(0)
    def worker(acc: List[(Int, B)]): Future[List[(Int, B)]] =
      val i = next.getAndIncrement()
      if i >= items.size then Future.successful(acc)
      else f(items(i)).flatMap(b => worker((i, b) :: acc))
    Future.sequence(List.fill(math.min(maxConcurrent, items.size))(worker(Nil)))
      .map(_.flatten.sortBy(_._1).map(_._2))

  // Bài 4: circuit breaker — máy trạng thái thuần, đồng hồ là tham số
  enum Breaker:
    case Closed(failures: Int)
    case Open(until: Long)
    case HalfOpen

  final case class BreakerPolicy(maxFailures: Int, openMillis: Long):
    /** Trả (trạng thái mới, có cho gọi không). */
    def allow(s: Breaker, now: Long): (Breaker, Boolean) = s match
      case Breaker.Open(until) if now >= until => (Breaker.HalfOpen, true)    // cho thử MỘT lần
      case Breaker.Open(_)                     => (s, false)
      case Breaker.HalfOpen                    => (s, false)                   // đang có lần thử
      case Breaker.Closed(_)                   => (s, true)
    /** Thành công ở bất kỳ trạng thái nào -> đóng lại, xóa bộ đếm lỗi. */
    def reset: Breaker = Breaker.Closed(0)
    def onFailure(s: Breaker, now: Long): Breaker = s match
      case Breaker.Closed(n) if n + 1 >= maxFailures => Breaker.Open(now + openMillis)
      case Breaker.Closed(n)                         => Breaker.Closed(n + 1)
      case Breaker.HalfOpen                          => Breaker.Open(now + openMillis)
      case open: Breaker.Open                        => open

  // Bài 5a: không Await — kết hợp bằng flatMap, chạy được trên pool 1 thread
  def noStarvation(using ExecutionContext): Future[Int] =
    Future(1).flatMap(_ => Future(42))
