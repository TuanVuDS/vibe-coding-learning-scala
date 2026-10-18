package jvmlab

import java.lang.management.ManagementFactory

/** Thí nghiệm 7: phát hiện deadlock và lấy "thread dump" ngay trong code
  * (tương đương một phần `jcmd <pid> Thread.print`).
  */
@main def deadlockLab(): Unit =
  val lockA = Object(); val lockB = Object()
  def worker(name: String, first: Object, second: Object) =
    val t = Thread(() =>
      first.synchronized {
        Thread.sleep(100)
        second.synchronized { println(s"$name xong") }
      }, name)
    t.setDaemon(true); t.start(); t
  worker("transfer-A-to-B", lockA, lockB)
  worker("transfer-B-to-A", lockB, lockA)
  Thread.sleep(500)
  val mx = ManagementFactory.getThreadMXBean
  Option(mx.findDeadlockedThreads()) match
    case None => println("Không có deadlock")
    case Some(ids) =>
      println(s"Phát hiện deadlock giữa ${ids.length} thread:")
      for info <- mx.getThreadInfo(ids, true, true) do
        println(s"  \"${info.getThreadName}\" ${info.getThreadState} chờ ${info.getLockName} đang bị giữ bởi \"${info.getLockOwnerName}\"")
