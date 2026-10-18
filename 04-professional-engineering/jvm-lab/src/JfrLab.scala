package jvmlab

import java.nio.file.Files
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import scala.jdk.CollectionConverters.*

/** Thí nghiệm 8: Java Flight Recorder — profiler có sẵn trong JDK, chi phí thấp, dùng được ở production.
  * Thường bật bằng cờ -XX:StartFlightRecording và đọc bằng JDK Mission Control hoặc lệnh `jfr`.
  * Ở đây điều khiển bằng API để chạy được trong mọi môi trường.
  */
@main def jfrLab(): Unit =
  val rec = Recording()
  rec.enable("jdk.GarbageCollection")
  rec.enable("jdk.ExecutionSample").withPeriod(java.time.Duration.ofMillis(10))
  rec.start()
  // workload: phân tích chuỗi tốn CPU + tạo rác
  var acc = 0L
  for i <- 1 to 3_000_000 do acc += s"user-$i,page-${i % 97}".split(",").map(_.length).sum
  rec.stop()
  val file = Files.createTempFile("lab", ".jfr")
  rec.dump(file)
  val events = RecordingFile.readAllEvents(file).asScala.toList
  val gcs = events.count(_.getEventType.getName == "jdk.GarbageCollection")
  val samples = events.filter(_.getEventType.getName == "jdk.ExecutionSample")
  println(s"acc=$acc; số lần GC: $gcs; số mẫu CPU: ${samples.size}")
  val topFrames = samples.flatMap(e => Option(e.getStackTrace).flatMap(_.getFrames.asScala.headOption))
    .map(f => s"${f.getMethod.getType.getName}.${f.getMethod.getName}")
    .groupMapReduce(identity)(_ => 1)(_ + _).toList.sortBy(-_._2).take(5)
  println("Top 5 method đang chạy khi lấy mẫu (hot spots):")
  topFrames.foreach((m, c) => println(f"  $c%4d  $m"))
