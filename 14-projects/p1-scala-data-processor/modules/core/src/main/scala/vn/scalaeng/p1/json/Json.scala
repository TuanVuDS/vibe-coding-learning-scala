package vn.scalaeng.p1.json

import scala.annotation.tailrec
import java.nio.CharBuffer

/** JSON ADT tối giản. Trong thực tế dùng circe/jsoniter-scala (Chương 19, 21);
  * ở đây tự viết để Project 1 chỉ dùng standard library và để hiểu cách parser hoạt động.
  */
enum Json:
  case JNull
  case JBool(value: Boolean)
  case JNum(value: BigDecimal)
  case JStr(value: String)
  case JArr(items: Vector[Json])
  case JObj(fields: Vector[(String, Json)])

  def render: String = this match
    case JNull     => "null"
    case JBool(b)  => b.toString
    case JNum(n)   => n.bigDecimal.toPlainString
    case JStr(s)   => Json.quote(s)
    case JArr(xs)  => xs.map(_.render).mkString("[", ",", "]")
    case JObj(fs)  => fs.map((k, v) => s"${Json.quote(k)}:${v.render}").mkString("{", ",", "}")

object Json:
  def quote(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c    => c.toString
    }.prepended('"').appended('"')

/** Recursive descent parser. Mỗi hàm nhận vị trí, trả (giá trị, vị trí kế tiếp) hoặc lỗi kèm vị trí. */
object JsonParser:
  import Json.*
  val MaxDepth = 64
  private type Res[A] = Either[String, (A, Int)]
  private val NumberRe = """-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?""".r

  def parse(s: String): Either[String, Json] =
    value(s, skipWs(s, 0), 0).flatMap { (v, i) =>
      val end = skipWs(s, i)
      if end == s.length then Right(v) else Left(s"ký tự thừa ở vị trí $end")
    }

  @tailrec private def skipWs(s: String, i: Int): Int =
    if i < s.length && " \t\n\r".contains(s.charAt(i)) then skipWs(s, i + 1) else i

  private def value(s: String, i: Int, depth: Int): Res[Json] =
    if depth > MaxDepth then Left(s"lồng quá sâu (> $MaxDepth) ở vị trí $i")
    else if i >= s.length then Left("hết dữ liệu khi đang chờ một giá trị")
    else s.charAt(i) match
      case 'n' => literal(s, i, "null", JNull)
      case 't' => literal(s, i, "true", JBool(true))
      case 'f' => literal(s, i, "false", JBool(false))
      case '"' => string(s, i).map((str, j) => (JStr(str), j))
      case '[' => array(s, skipWs(s, i + 1), depth + 1, Vector.empty, first = true)
      case '{' => obj(s, skipWs(s, i + 1), depth + 1, Vector.empty, first = true)
      case c if c == '-' || c.isDigit => number(s, i)
      case c => Left(s"ký tự không mong đợi '$c' ở vị trí $i")

  private def literal(s: String, i: Int, word: String, v: Json): Res[Json] =
    if s.startsWith(word, i) then Right((v, i + word.length)) else Left(s"mong đợi '$word' ở vị trí $i")

  private def number(s: String, i: Int): Res[Json] =
    NumberRe.findPrefixMatchOf(CharBuffer.wrap(s, i, s.length)) match
      case Some(m) => Right((JNum(BigDecimal(m.matched)), i + m.end))
      case None    => Left(s"số không hợp lệ ở vị trí $i")

  /** i trỏ vào dấu nháy mở. */
  private def string(s: String, i: Int): Res[String] =
    @tailrec def loop(j: Int, acc: List[Char]): Res[String] =
      if j >= s.length then Left(s"chuỗi không được đóng (bắt đầu ở vị trí $i)")
      else s.charAt(j) match
        case '"'  => Right((acc.reverse.mkString, j + 1))
        case '\\' =>
          if j + 1 >= s.length then Left(s"escape dở dang ở vị trí $j")
          else s.charAt(j + 1) match
            case '"'  => loop(j + 2, '"' :: acc)
            case '\\' => loop(j + 2, '\\' :: acc)
            case '/'  => loop(j + 2, '/' :: acc)
            case 'b'  => loop(j + 2, '\b' :: acc)
            case 'f'  => loop(j + 2, '\f' :: acc)
            case 'n'  => loop(j + 2, '\n' :: acc)
            case 'r'  => loop(j + 2, '\r' :: acc)
            case 't'  => loop(j + 2, '\t' :: acc)
            case 'u' if j + 6 <= s.length && s.substring(j + 2, j + 6).forall(c => Character.digit(c, 16) >= 0) =>
              loop(j + 6, Integer.parseInt(s.substring(j + 2, j + 6), 16).toChar :: acc)
            case other => Left(s"escape không hợp lệ '\\$other' ở vị trí $j")
        case c if c < ' ' => Left(s"ký tự điều khiển chưa escape ở vị trí $j")
        case c            => loop(j + 1, c :: acc)
    loop(i + 1, Nil)

  private def array(s: String, i: Int, depth: Int, acc: Vector[Json], first: Boolean): Res[Json] =
    if i < s.length && s.charAt(i) == ']' && first then Right((JArr(acc), i + 1))
    else value(s, i, depth).flatMap { (v, j0) =>
      val j = skipWs(s, j0)
      if j < s.length && s.charAt(j) == ',' then array(s, skipWs(s, j + 1), depth, acc :+ v, first = false)
      else if j < s.length && s.charAt(j) == ']' then Right((JArr(acc :+ v), j + 1))
      else Left(s"mong đợi ',' hoặc ']' ở vị trí $j")
    }

  private def obj(s: String, i: Int, depth: Int, acc: Vector[(String, Json)], first: Boolean): Res[Json] =
    if i < s.length && s.charAt(i) == '}' && first then Right((JObj(acc), i + 1))
    else if i >= s.length || s.charAt(i) != '"' then Left(s"mong đợi tên trường (chuỗi) ở vị trí $i")
    else string(s, i).flatMap { (key, j0) =>
      val j = skipWs(s, j0)
      if j >= s.length || s.charAt(j) != ':' then Left(s"mong đợi ':' ở vị trí $j")
      else value(s, skipWs(s, j + 1), depth).flatMap { (v, k0) =>
        val k = skipWs(s, k0)
        if k < s.length && s.charAt(k) == ',' then obj(s, skipWs(s, k + 1), depth, acc :+ (key -> v), first = false)
        else if k < s.length && s.charAt(k) == '}' then Right((JObj(acc :+ (key -> v)), k + 1))
        else Left(s"mong đợi ',' hoặc '}' ở vị trí $k")
      }
    }
