package vn.scalaeng.order.core

/** Tách một dòng CSV theo RFC 4180 (hỗ trợ trường trong dấu nháy kép, "" là nháy kép thật).
  * Không hỗ trợ trường nhiều dòng — giới hạn được ghi rõ và có test.
  */
object CsvLine:
  def split(line: String): Either[String, List[String]] =
    val fields = List.newBuilder[String]
    val cur = new StringBuilder
    var inQuotes = false
    var i = 0
    while i < line.length do
      val c = line.charAt(i)
      if inQuotes then
        if c == '"' then
          if i + 1 < line.length && line.charAt(i + 1) == '"' then
            cur += '"'; i += 1
          else inQuotes = false
        else cur += c
      else if c == '"' then inQuotes = true
      else if c == ',' then
        fields += cur.toString; cur.clear()
      else cur += c
      i += 1
    if inQuotes then Left("thiếu dấu nháy kép đóng")
    else
      fields += cur.toString
      Right(fields.result())

  def escape(s: String): String =
    if s.exists(c => c == ',' || c == '"' || c == '\n') then "\"" + s.replace("\"", "\"\"") + "\"" else s
