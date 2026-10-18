package jvmlab

/** Thí nghiệm 2: boxing. Array[Int] lưu int nguyên thủy liền nhau; List[Int]/Vector[Int] lưu
  * con trỏ tới các đối tượng java.lang.Integer rải rác trên heap.
  * Đây là micro-benchmark ĐƠN GIẢN để minh họa — đo nghiêm túc phải dùng JMH (sbt-jmh).
  */
object BoxingTools:
  def usedMb(): Long =
    System.gc(); Thread.sleep(200)
    val rt = Runtime.getRuntime
    (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)

  def time[A](label: String, rounds: Int)(body: => A): Unit =
    for _ <- 1 to rounds do body // warm-up: cho JIT thời gian biên dịch đoạn code "nóng"
    val start = System.nanoTime()
    var sink = 0L
    for _ <- 1 to rounds do sink += body.hashCode
    val ms = (System.nanoTime() - start) / 1e6 / rounds
    println(f"  $label%-28s ${ms}%8.2f ms/lần   (sink=${sink % 10})")

@main def boxingLab(): Unit =
  import BoxingTools.*
  val n = 5_000_000
  val base = usedMb()
  val arr: Array[Int] = Array.tabulate(n)(identity)
  val afterArray = usedMb()
  val vec: Vector[Int] = Vector.tabulate(n)(i => i + 1_000) // tránh cache Integer của JVM (-128..127)
  val afterVector = usedMb()
  val list: List[Int] = List.tabulate(n)(i => i + 1_000)
  val afterList = usedMb()
  println(s"Bộ nhớ cho $n phần tử (xấp xỉ, đo bằng Runtime sau System.gc):")
  println(s"  Array[Int]  : ~${afterArray - base} MB")
  println(s"  Vector[Int] : ~${afterVector - afterArray} MB")
  println(s"  List[Int]   : ~${afterList - afterVector} MB")
  println("Thời gian tính tổng:")
  time("Array[Int] while-loop", 20) { var i = 0; var s = 0L; while i < arr.length do { s += arr(i); i += 1 }; s }
  time("Array[Int].sum", 20)(arr.foldLeft(0L)(_ + _))
  time("Vector[Int].foldLeft", 20)(vec.foldLeft(0L)(_ + _))
  time("List[Int].foldLeft", 20)(list.foldLeft(0L)(_ + _))
  println(s"(giữ tham chiếu: ${arr.length + vec.length + list.length})")
