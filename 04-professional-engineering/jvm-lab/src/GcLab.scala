package jvmlab

/** Thí nghiệm 4: tạo nhiều đối tượng sống ngắn -> GC trẻ (young GC) thường xuyên.
  * Chạy với -Xlog:gc để thấy từng lần GC.
  */
final case class Event(userId: Long, page: String, ms: Long)

@main def gcLab(): Unit =
  var total = 0L
  for _ <- 1 to 30 do
    val events = Vector.tabulate(200_000)(i => Event(i.toLong, s"/p/${i % 50}", i % 1000))
    total += events.iterator.map(_.ms).sum
  println(s"total=$total")
