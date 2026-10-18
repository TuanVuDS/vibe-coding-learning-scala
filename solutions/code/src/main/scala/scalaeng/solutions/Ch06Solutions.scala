package scalaeng.solutions

/** Lời giải bài tập Chương 6. */
object Ch06Solutions:

  // Bài 1: \p{L} = mọi chữ cái Unicode (kể cả tiếng Việt có dấu), \p{N} = chữ số
  def wordFrequency(text: String): Map[String, Int] =
    text.toLowerCase
      .split("[^\\p{L}\\p{N}]+")
      .filter(_.nonEmpty)
      .toList
      .groupMapReduce(identity)(_ => 1)(_ + _)

  // Bài 2
  def topN(scores: Map[String, Long], n: Int): List[(String, Long)] =
    scores.toList.sortBy((name, score) => (-score, name)).take(n)

  // Bài 3: hợp hai tập key, cộng giá trị
  def mergeInventories(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] =
    (a.keySet ++ b.keySet).iterator.map(k => k -> (a.getOrElse(k, 0) + b.getOrElse(k, 0))).toMap

  // Bài 4: dedup giữ bản updatedAt lớn nhất
  def dedupLatest(records: List[(String, Long, String)]): List[(String, Long, String)] =
    records.groupMapReduce(_._1)(identity)((x, y) => if y._2 > x._2 then y else x)
      .values.toList.sortBy(_._1)

  // Bài 5: chỉ số bắt đầu của các cửa sổ có >= threshold lỗi 5xx
  def errorBurstStarts(statuses: List[Int], window: Int, threshold: Int): List[Int] =
    require(window > 0, "window phải > 0")
    if statuses.size < window then Nil
    else
      statuses.sliding(window).zipWithIndex.collect {
        case (w, idx) if w.count(_ >= 500) >= threshold => idx
      }.toList
