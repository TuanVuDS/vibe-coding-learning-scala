package vn.scalaeng.streaming

import cats.effect.{Deferred, IO, IOApp, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*
import fs2.Stream
import scala.concurrent.duration.*

final case class Pressure(capacity: Int, peak: Int, blocked: Boolean, maxOfferMs: Long, consumed: Int)

object Backpressure:
  def measure: IO[Pressure] = for
    queue <- Queue.bounded[IO, Int](2)
    peak <- Ref.of[IO, Int](0)
    waits <- Ref.of[IO, Long](0L)
    thirdStarted <- Deferred[IO, Unit]
    thirdDone <- Deferred[IO, Unit]
    producer = Stream.range(0, 12).covary[IO].evalMap { i =>
      (if i == 2 then thirdStarted.complete(()).void else IO.unit) *>
        queue.offer(i).timed.flatMap { (elapsed, _) =>
          waits.update(_.max(elapsed.toMillis)) *> queue.size.flatMap(n => peak.update(_.max(n))) *>
            (if i == 2 then thirdDone.complete(()).void else IO.unit)
        }
    }.compile.drain
    result <- producer.background.use { completed => for
      _ <- thirdStarted.get
      _ <- IO.sleep(40.millis) // no consumer yet: third offer cannot fit
      blocked <- thirdDone.tryGet.map(_.isEmpty)
      values <- Stream.repeatEval(queue.take).take(12).evalTap(_ => IO.sleep(10.millis)).compile.toVector
      _ <- completed
      p <- peak.get
      wait <- waits.get
    yield Pressure(2, p, blocked, wait, values.size) }
  yield result

object StreamingDemo extends IOApp.Simple:
  val clicks = Vector(Event("c1", "CMP-1", 2), Event("c2", "CMP-1", 12),
    Event("c3", "CMP-1", 8), Event("c4", "CMP-1", 21), Event("c5", "CMP-1", 4))

  def run: IO[Unit] = for
    _ <- IO.println(s"JDK=${System.getProperty("java.version")} processors=${Runtime.getRuntime.availableProcessors()}")
    _ <- IO.println(s"clocks: event=${Clocks.buckets(clicks.map(e => Arrival(e, 100)), 10, true).toVector.sortBy(_._1.start)} processing=${Clocks.buckets(clicks.map(e => Arrival(e, 100)), 10, false).toVector.sortBy(_._1.start)}")
    state <- Stream.emits(clicks).covary[IO].evalMapAccumulate(State()) { (s, e) =>
      val r = EventTime.step(s, e, 10, 5)
      IO.println(s"${e.id}: event=${e.eventTime} watermark=${r.state.watermark.get} ${r.decision} closed=${r.emitted}")
        .as((r.state, r))
    }.compile.lastOrError.map(_._1)
    _ <- IO.println(s"finish=${EventTime.finish(state)} open=${state.open.size} seen=${state.seen.size}")
    _ <- IO.println(s"sliding(12,20,10)=${Windows.sliding(12, 20, 10)}")
    _ <- IO.println(s"sessions=${Windows.sessions(Vector(Event("o1", "U-1", 2, 100), Event("o2", "U-1", 9, 200), Event("o3", "U-1", 20, 300)), 10)}")
    _ <- Delivery.values.toVector.traverse_(mode => IO.println(s"$mode: ${Crash.run(Vector("o1", "o2", "o3"), mode, 1, false)}"))
    _ <- IO.println(s"dedup: ${Crash.run(Vector("o1", "o2", "o3"), Delivery.AtLeastOnce, 1, true)}")
    pressure <- Backpressure.measure
    _ <- IO.println(s"backpressure=$pressure")
    _ <- IO.delay(Windows.tumbling(0, 0)).attempt.flatMap(r => IO.println(r.left.toOption.get.toString))
  yield ()
