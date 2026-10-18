package vn.scalaeng.p2.security

import cats.effect.{Clock, IO, Ref}
import scala.concurrent.duration.*

/** Token bucket: mỗi khóa (IP, email...) có tối đa `capacity` token, hồi `capacity` token mỗi `per`.
  * Mỗi request tiêu 1 token; hết token → 429 kèm thời gian phải chờ.
  */
final class RateLimiter private (capacity: Int, per: FiniteDuration, maxBuckets: Int, buckets: Ref[IO, Map[String, RateLimiter.Bucket]]):
  import RateLimiter.Bucket

  private val refillPerNano: Double = capacity.toDouble / per.toNanos
  private[security] def bucketCount: IO[Int] = buckets.get.map(_.size)

  /** Right(()) nếu được phép; Left(thời gian chờ) nếu bị chặn. Dùng đồng hồ MONOTONIC (không lùi khi chỉnh giờ hệ thống). */
  def acquire(key: String): IO[Either[FiniteDuration, Unit]] =
    Clock[IO].monotonic.flatMap { now =>
      buckets.modify { all =>
        // Prune idle, fully refilled buckets when a new key arrives. Never evict an active quota.
        val live = if all.contains(key) then all else all.filter { case (_, b) =>
          b.tokens + (now - b.updatedAt).toNanos.max(0L) * refillPerNano < capacity.toDouble
        }
        val b       = live.getOrElse(key, Bucket(capacity.toDouble, now))
        val elapsed = (now - b.updatedAt).toNanos.max(0L)
        val tokens  = (b.tokens + elapsed * refillPerNano).min(capacity.toDouble)
        if !live.contains(key) && live.size >= maxBuckets then (live, Left(per))
        else if tokens >= 1 then (live.updated(key, Bucket(tokens - 1, now)), Right(()))
        else
          val waitNanos = math.ceil((1 - tokens) / refillPerNano).toLong
          (live.updated(key, Bucket(tokens, now)), Left(waitNanos.nanos))
      }
    }

object RateLimiter:
  private final case class Bucket(tokens: Double, updatedAt: FiniteDuration)

  def make(capacity: Int, per: FiniteDuration, maxBuckets: Int = 10000): IO[RateLimiter] =
    IO(require(capacity > 0 && per > Duration.Zero && maxBuckets > 0)) *>
      Ref.of[IO, Map[String, Bucket]](Map.empty).map(new RateLimiter(capacity, per, maxBuckets, _))
