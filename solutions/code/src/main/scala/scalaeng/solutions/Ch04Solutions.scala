package scalaeng.solutions

import scala.annotation.tailrec

/** Lời giải bài tập Chương 4. */
object Ch04Solutions:

  // Bài 1: ghép 3 hàm theo thứ tự f -> g -> h
  def compose3[A, B, C, D](f: A => B, g: B => C, h: C => D): A => D =
    f.andThen(g).andThen(h)

  // Bài 2: đếm phần tử thỏa điều kiện, đệ quy đuôi
  def countWhere[A](xs: List[A])(p: A => Boolean): Int =
    @tailrec
    def loop(rest: List[A], acc: Int): Int = rest match
      case Nil                => acc
      case h :: t if p(h)     => loop(t, acc + 1)
      case _ :: t             => loop(t, acc)
    loop(xs, 0)

  // Bài 3: phân trang có kiểm tra biên — page luôn nằm trong [1, pages]
  def paginateSafe(total: Int, pageSize: Int = 20, page: Int = 1): (Int, Int) =
    require(pageSize > 0, "pageSize phải > 0")
    val pages = math.max(1, (math.max(total, 0) + pageSize - 1) / pageSize)
    val current = math.min(math.max(page, 1), pages)
    (current, pages)

  // Bài 4: thời lượng trung bình theo user
  def averageDurationByUser(events: List[(String, Long)]): Map[String, Double] =
    events.groupMap(_._1)(_._2).view
      .mapValues(ds => ds.sum.toDouble / ds.size)
      .toMap

  // Bài 5: đếm số "session": hai sự kiện liên tiếp cách nhau > gapMs thì mở session mới
  def countSessions(sortedTimestamps: List[Long], gapMs: Long): Int =
    @tailrec
    def loop(rest: List[Long], prev: Long, sessions: Int): Int = rest match
      case Nil => sessions
      case t :: tail =>
        if t - prev > gapMs then loop(tail, t, sessions + 1)
        else loop(tail, t, sessions)
    sortedTimestamps match
      case Nil          => 0
      case first :: tail => loop(tail, first, 1)
