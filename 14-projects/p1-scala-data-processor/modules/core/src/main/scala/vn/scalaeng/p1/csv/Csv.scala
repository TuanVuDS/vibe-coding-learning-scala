package vn.scalaeng.p1.csv

import scala.annotation.tailrec

/** Parser một dòng CSV theo RFC 4180, viết THUẦN: máy trạng thái + đệ quy đuôi, không `var`.
  * Nghiêm ngặt: dấu nháy kép chỉ được mở ở ĐẦU trường (`a, "b"` là lỗi). Không hỗ trợ trường nhiều dòng.
  */
object Csv:
  private enum State:
    case FieldStart, Unquoted, Quoted, QuoteInQuoted

  def parseLine(line: String): Either[String, Vector[String]] =
    import State.*
    @tailrec
    def loop(i: Int, st: State, cur: List[Char], fields: Vector[String]): Either[String, Vector[String]] =
      def field = cur.reverse.mkString
      if i == line.length then
        if st == Quoted then Left("thiếu dấu nháy kép đóng") else Right(fields :+ field)
      else
        val c = line.charAt(i)
        st match
          case FieldStart if c == '"' => loop(i + 1, Quoted, Nil, fields)
          case FieldStart | Unquoted =>
            if c == ',' then loop(i + 1, FieldStart, Nil, fields :+ field)
            else if c == '"' then Left(s"dấu nháy kép giữa trường ở vị trí ${i + 1}")
            else loop(i + 1, Unquoted, c :: cur, fields)
          case Quoted =>
            if c == '"' then loop(i + 1, QuoteInQuoted, cur, fields) else loop(i + 1, Quoted, c :: cur, fields)
          case QuoteInQuoted =>
            if c == '"' then loop(i + 1, Quoted, '"' :: cur, fields)
            else if c == ',' then loop(i + 1, FieldStart, Nil, fields :+ field)
            else Left(s"ký tự '$c' ngay sau dấu nháy đóng ở vị trí ${i + 1}")
    loop(0, FieldStart, Nil, Vector.empty)

  def escape(s: String): String =
    if s.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r') then "\"" + s.replace("\"", "\"\"") + "\"" else s

  def renderRow(fields: Seq[String]): String = fields.map(escape).mkString(",")

  /** Bỏ BOM UTF-8 (file xuất từ Excel) — regression từ Chương 10. */
  def stripBom(s: String): String = s.stripPrefix("\uFEFF")
