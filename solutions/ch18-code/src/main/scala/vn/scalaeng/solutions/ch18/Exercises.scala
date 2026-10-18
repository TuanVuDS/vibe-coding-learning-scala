package vn.scalaeng.solutions.ch18

import cats.effect.{IO,Ref,Resource}
import java.sql.SQLException
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.service.*
import scala.concurrent.duration.*

object Exercises:
  // 1. Pure ownership policy: reusable in transports other than HTTP.
  def allowed(p: Principal,owner: UserId): Either[DomainError,Unit] =
    Either.cond(p.admin || p.id==owner,(),DomainError.Forbidden)

  // 2. Key every independent input; tenant data must never collide in a shared cache.
  final case class CatalogKey(tenant: String,id: Long,currency: String)
  def key(tenant: String,id: Long,currency: String): Either[String,CatalogKey] =
    val t=tenant.trim
    val c=currency.trim.toUpperCase(java.util.Locale.ROOT)
    Either.cond(t.nonEmpty && id>0 && Set("VND","USD").contains(c),CatalogKey(t,id,c),"khóa không hợp lệ")

  // 3. Classify only safe READ retries, including wrapped JDBC exceptions.
  def transientRead(e: Throwable): Boolean =
    val seen=scala.collection.mutable.Set.empty[Throwable]
    def loop(t: Throwable): Boolean =
      if t==null || seen(t) then false
      else
        seen += t
        t match
          case sql: SQLException if Set("40001","40P01").contains(sql.getSQLState) => true
          case _ => loop(t.getCause)
    loop(e)

  // 4. Bounded read budget: retries stay INSIDE the overall deadline, never refresh it per retry.
  def readWithin[A](read: IO[A],budget: FiniteDuration): IO[A] =
    Resilience.retryRead(read,2,100.millis)(transientRead).timeout(budget)

  // 5. Track in-flight work with Resource, including errors/cancellation.
  def tracked[A](active: Ref[IO,Int])(work: IO[A]): IO[A] =
    Resource.make(active.update(_+1))(_ => active.update(_-1)).use(_ => work)

  // 6. Cache invalidation is attached to successful commits only. Errors remain failures.
  def commitThenInvalidate[A](write: IO[Either[DomainError,A]],invalidate: IO[Unit]): IO[Either[DomainError,A]] =
    write.flatTap {
      case Right(_) => invalidate
      case Left(_) => IO.unit
    }
