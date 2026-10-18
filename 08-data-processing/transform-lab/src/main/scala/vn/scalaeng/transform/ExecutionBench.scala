package vn.scalaeng.transform

import fs2.Stream
import java.time.LocalDate

/** Đo 3 mô hình thực thi trên CÙNG dữ liệu trong bộ nhớ (1 triệu dòng). Mục đích: thấy cùng kết quả và chi phí tương đối.
  * Không phải benchmark nghiêm ngặt (không JMH): chạy 1 lượt khởi động + 3 lượt đo, lấy trung vị.
  */
object ExecutionBench:
  private def time[A](f: => A): (A, Double) =
    val t0 = System.nanoTime
    val r  = f
    (r, (System.nanoTime - t0) / 1e6)

  def main(args: Array[String]): Unit =
    val n      = args.headOption.flatMap(_.toIntOption).getOrElse(1_000_000)
    val base   = LocalDate.parse("2026-10-01")
    val orders = List.tabulate(n)(i =>
      Order(s"O$i", s"C${i % 1000}", base.plusDays((i % 30).toLong), Status.Paid, s"SKU-${i % 50}", 1 + i % 5, 10_000L + i % 997))
    val key   = (o: Order) => o.sku
    val value = (o: Order) => Stats.of(o.revenueVnd)

    val runs = List(
      "trong bộ nhớ (List)"          -> (() => Execution.inMemory(orders)(key)(value)),
      "theo luồng (fs2, khối 4096)"  -> (() => Execution.streaming(Stream.emits(orders), 4096)(key)(value)),
      "chia 8 partition + shuffle"   -> (() => Execution.partitioned(orders, 8, seed = 42L)(key)(value))
    )
    val expected = runs.head._2()
    println(s"$n đơn hàng, ${expected.size} khóa (sku)")
    runs.foreach { (name, run) =>
      val _ = run() // khởi động JIT
      val measured = List.fill(3)(time(run()))
      val median   = measured.map(_._2).sorted.apply(1)
      println(f"  $name%-30s trung vị ${median}%8.1f ms   cùng kết quả: ${measured.forall(_._1 == expected)}")
    }
