package vn.scalaeng.p2.service

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore
import scala.concurrent.duration.*

final class Saturated extends RuntimeException("bulkhead đầy")
object Resilience:
  /** The caller must supply an idempotent read. At most retries + 1 attempts. */
  def retryRead[A](read: IO[A], retries: Int, pause: FiniteDuration)(transient: Throwable => Boolean): IO[A] =
    read.handleErrorWith(e =>
      if retries > 0 && transient(e) then IO.sleep(pause) *> retryRead(read, retries - 1, pause * 2)(transient)
      else IO.raiseError(e))

final class Bulkhead private (permits: Semaphore[IO]):
  def apply[A](work: IO[A]): IO[A] =
    IO.uncancelable { poll =>
      permits.tryAcquire.flatMap {
        case false => IO.raiseError(new Saturated)
        case true => poll(work).guarantee(permits.release)
      }
    }
object Bulkhead:
  def make(n: Long): IO[Bulkhead] = IO.raiseWhen(n <= 0)(new IllegalArgumentException("permits phải dương")) *>
    Semaphore[IO](n).map(new Bulkhead(_))

/** Global generation avoids resurrecting stale values after invalidation, including in-flight fills. */
final class TtlCache[K, V] private (ttl: FiniteDuration, capacity: Int, state: Ref[IO, TtlCache.State[K, V]]):
  def get(key: K)(load: IO[V]): IO[V] =
    for
      now <- IO.monotonic
      snapshot <- state.get
      value <- snapshot.entries.get(key).filter(_._1 > now) match
        case Some((_, v)) => IO.pure(v)
        case None => load.flatTap { v =>
          IO.monotonic.flatMap { end => state.update { current =>
            if current.generation != snapshot.generation then current
            else
              val live = current.entries.filter(_._2._1 > end)
              val kept = if live.size >= capacity then Map.empty[K, (FiniteDuration, V)] else live
              current.copy(entries = kept.updated(key, (end + ttl, v)))
          }}
        }
    yield value
  def invalidate: IO[Unit] = state.update(s => TtlCache.State(s.generation + 1, Map.empty))
object TtlCache:
  private final case class State[K, V](generation: Long, entries: Map[K, (FiniteDuration, V)])
  def make[K, V](ttl: FiniteDuration, capacity: Int): IO[TtlCache[K, V]] =
    IO.raiseWhen(ttl <= Duration.Zero || capacity <= 0)(new IllegalArgumentException("TTL/capacity phải dương")) *>
      Ref.of[IO, State[K, V]](State(0L, Map.empty)).map(new TtlCache(ttl, capacity, _))
