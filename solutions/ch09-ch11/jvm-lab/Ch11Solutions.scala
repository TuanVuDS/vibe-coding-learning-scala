package jvmlab.solutions

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/** Lời giải Ch11 bài 3: so sánh thêm arr.sum và while trên Vector. */
@main def boxingMore(): Unit =
  val n = 5_000_000
  val arr: Array[Int] = Array.tabulate(n)(i => i + 1_000)
  val vec: Vector[Int] = Vector.tabulate(n)(i => i + 1_000)
  def time(label: String)(body: => Long): Unit =
    for _ <- 1 to 10 do body
    val t0 = System.nanoTime(); var sink = 0L
    for _ <- 1 to 20 do sink += body
    println(f"  $label%-26s ${(System.nanoTime() - t0) / 1e6 / 20}%8.2f ms/lần (sink=${sink % 7})")
  time("Array while")       { var i = 0; var s = 0L; while i < arr.length do { s += arr(i); i += 1 }; s }
  time("Array .sum (Int)")  { arr.sum.toLong }
  time("Array foldLeft")    { arr.foldLeft(0L)(_ + _) }
  time("Vector while")      { var i = 0; var s = 0L; while i < vec.length do { s += vec(i); i += 1 }; s }
  time("Vector foldLeft")   { vec.foldLeft(0L)(_ + _) }

/** Lời giải Ch11 bài 5: hai cách tránh deadlock. */
@main def noDeadlock(): Unit =
  // (a) Thứ tự khóa toàn cục: luôn khóa theo id tăng dần, bất kể chiều chuyển tiền
  final class Account(val id: Int):
    var balance = 100
  def transferOrdered(from: Account, to: Account, amount: Int): Unit =
    val (first, second) = if from.id < to.id then (from, to) else (to, from)
    first.synchronized { second.synchronized { from.balance -= amount; to.balance += amount } }

  val a = Account(1); val b = Account(2)
  val t1 = Thread(() => for _ <- 1 to 10_000 do transferOrdered(a, b, 1), "t1")
  val t2 = Thread(() => for _ <- 1 to 10_000 do transferOrdered(b, a, 1), "t2")
  t1.start(); t2.start(); t1.join(); t2.join()
  println(s"(a) Thứ tự khóa: xong, tổng tiền = ${a.balance + b.balance} (phải là 200)")

  // (b) tryLock có timeout: không lấy được khóa thứ hai thì nhả khóa thứ nhất và thử lại
  val lockA = ReentrantLock(); val lockB = ReentrantLock()
  var retries = 0
  def work(first: ReentrantLock, second: ReentrantLock): Unit =
    var done = false
    while !done do
      first.lock()
      try
        if second.tryLock(10, TimeUnit.MILLISECONDS) then
          try done = true finally second.unlock()
        else synchronized { retries += 1 }
      finally first.unlock()
      if !done then Thread.sleep(scala.util.Random.nextInt(5).toLong) // lùi ngẫu nhiên để tránh livelock
  val u1 = Thread(() => for _ <- 1 to 200 do work(lockA, lockB))
  val u2 = Thread(() => for _ <- 1 to 200 do work(lockB, lockA))
  u1.start(); u2.start(); u1.join(); u2.join()
  println(s"(b) tryLock: xong, số lần phải thử lại = $retries (thay đổi theo lần chạy)")
