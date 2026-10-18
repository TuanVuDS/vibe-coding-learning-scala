package jvmlab

import scala.collection.mutable.ArrayBuffer

/** Thí nghiệm 5: OutOfMemoryError khi giữ tham chiếu quá nhiều (ví dụ: gom cả file lớn vào bộ nhớ). */
@main def oomLab(): Unit =
  val retained = ArrayBuffer.empty[Array[Byte]]
  try
    while true do retained += new Array[Byte](1024 * 1024) // giữ 1 MB mỗi vòng, không bao giờ giải phóng
  catch
    case e: OutOfMemoryError =>
      val n = retained.size
      retained.clear() // giải phóng để còn bộ nhớ in thông báo
      println(s"OutOfMemoryError sau ~$n MB: ${e.getMessage}")

/** Thí nghiệm 6: độ sâu đệ quy tối đa phụ thuộc kích thước stack của thread (-Xss). */
@main def stackLab(): Unit =
  def depth(n: Int): Int = try depth(n + 1) catch case _: StackOverflowError => n
  // chạy trong thread mới để -Xss áp dụng chắc chắn (thread main có thể có stack khác)
  val t = Thread(() => println(s"Độ sâu đệ quy đạt được: ~${depth(0)}"))
  t.start(); t.join()
