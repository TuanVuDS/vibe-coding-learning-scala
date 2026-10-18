package vn.scalaeng.ch08c

import scala.quoted.*

object TopicLiteral:
  private def valid(value: String): Boolean =
    value.nonEmpty && value.length <= 249 && value != "." && value != ".." &&
      value.forall(c => c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
        c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-')

  /** Chính sách tên topic của lab; không thay thế kiểm tra cấu hình broker. */
  def parse(value: String): Either[String, String] =
    Either.cond(valid(value), value, "invalid topic name")

  inline def checked(inline value: String): String = ${ checkedImpl('value) }

  private def checkedImpl(value: Expr[String])(using Quotes): Expr[String] =
    value.value match
      case Some(text) if valid(text) =>
        '{
          val topic = ${ Expr(text) }
          topic
        }
      case Some(_) => quotes.reflect.report.errorAndAbort("invalid topic literal", value)
      case None => quotes.reflect.report.errorAndAbort("topic must be a constant string", value)
