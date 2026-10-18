package vn.scalaeng.sec

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import scala.concurrent.duration.*

/** Đồng hồ điều khiển được cho test JWT hết hạn — không `Thread.sleep` 15 phút. */
final class MutableClock(private var now: Instant) extends Clock:
  def advance(d: FiniteDuration): Unit  = now = now.plusMillis(d.toMillis)
  override def instant(): Instant       = now
  override def getZone: ZoneId          = ZoneOffset.UTC
  override def withZone(z: ZoneId): Clock = this

object Fixtures:
  val config: SecurityConfig = SecurityConfig.load(Map(
    "JWT_SECRET"                -> "test-secret-chi-dung-cho-test-0123456789",
    "CORS_ORIGINS"              -> "https://shop.scalaeng.vn",
    "LOGIN_ATTEMPTS_PER_MINUTE" -> "3"
  )).fold(e => sys.error(e.toString), identity)

  def clock(): MutableClock = MutableClock(Instant.parse("2026-10-17T00:00:00Z"))
