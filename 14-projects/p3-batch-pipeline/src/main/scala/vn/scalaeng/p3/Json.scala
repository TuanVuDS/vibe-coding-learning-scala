package vn.scalaeng.p3

/** JSON tối giản, đủ cho log và manifest — tránh thêm thư viện vào job Spark (mỗi dependency là một rủi ro xung đột classpath với cụm). */
object Json {
  def str(s: String): String = {
    val b = new StringBuilder("\"")
    s.foreach {
      case '"'          => b ++= "\\\""
      case '\\'         => b ++= "\\\\"
      case '\n'         => b ++= "\\n"
      case '\r'         => b ++= "\\r"
      case '\t'         => b ++= "\\t"
      case c if c < ' ' => b ++= f"\\u${c.toInt}%04x"
      case c            => b += c
    }
    b += '"'
    b.toString
  }

  def value(v: Any): String = v match {
    case null                 => "null"
    case s: String            => str(s)
    case b: Boolean           => b.toString
    case n: Int               => n.toString
    case n: Long              => n.toString
    case n: Double            => if (n.isNaN || n.isInfinite) str(n.toString) else n.toString
    case n: BigDecimal        => n.bigDecimal.toPlainString
    case o: Option[_]         => o.fold("null")(value)
    case m: Map[_, _]         => obj(m.toSeq.map { case (k, x) => k.toString -> x }: _*)
    case xs: Iterable[_]      => xs.map(value).mkString("[", ",", "]")
    case other                => str(other.toString)
  }

  def obj(fields: (String, Any)*): String = fields.map { case (k, v) => s"${str(k)}:${value(v)}" }.mkString("{", ",", "}")
}
