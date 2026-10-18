package vn.scalaeng.p2.service

import cats.effect.{IO,IOApp,Ref}
import scala.concurrent.duration.*

object ArchitectureDemo extends IOApp.Simple:
  def run: IO[Unit] =
    for
      calls <- Ref.of[IO,Int](0)
      cache <- TtlCache.make[String,Int](1.minute,100)
      load = IO.sleep(10.millis) *> calls.updateAndGet(_+1)
      _ <- load.replicateA_(3) // warm up scheduling; this is a simulated upstream, not PostgreSQL.
      _ <- calls.set(0)
      t0 <- IO.monotonic
      _ <- load.replicateA_(20)
      t1 <- IO.monotonic
      before <- calls.get
      _ <- calls.set(0)
      t2 <- IO.monotonic
      _ <- cache.get("product:1")(load).replicateA_(20)
      t3 <- IO.monotonic
      after <- calls.get
      _ <- IO.println(s"20 reads, upstream=10ms: uncached calls=$before elapsed_ms=${(t1-t0).toMillis}")
      _ <- IO.println(s"20 reads, upstream=10ms: cached calls=$after elapsed_ms=${(t3-t2).toMillis}")
      _ <- cache.invalidate
      _ <- cache.get("product:1")(load)
      invalidated <- calls.get
      _ <- IO.println(s"after invalidation: upstream calls=$invalidated")
      bad <- Bulkhead.make(0).attempt
      _ <- IO.println(bad.swap.toOption.get.toString)
    yield ()
