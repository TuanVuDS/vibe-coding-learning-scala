package vn.scalaeng.solutions.ch23

import org.apache.spark.sql.functions._
import vn.scalaeng.sparksql.SqlDemo

/** Bài 4 — ĐO thời gian, tách khỏi unit test (review chéo, m3): unit test kiểm tra ĐÚNG; số đo là quan sát của một máy, không phải hợp đồng.
  * Chạy: scala-cli run . --main-class vn.scalaeng.solutions.ch23.UdfBench
  */
object UdfBench {
  def main(args: Array[String]): Unit = {
    val spark = SqlDemo.session()
    spark.sparkContext.setLogLevel("OFF")
    try {
      val df = spark.range(0, 2000000, 1, 4).select(concat(lit(" sku-"), (col("id") % 1000).cast("string"), lit(" ")).as("sku")).cache()
      df.count()
      val (b, u) = (UdfVsBuiltin.builtin(df), UdfVsBuiltin.withUdf(df))
      UdfVsBuiltin.timeMs(b); UdfVsBuiltin.timeMs(u) // khởi động JIT, không tính
      (1 to 3).foreach { i => println(s"lần $i: builtin ${UdfVsBuiltin.timeMs(b)} ms, UDF ${UdfVsBuiltin.timeMs(u)} ms") }
      df.unpersist(blocking = true)
    } finally spark.stop()
  }
}
