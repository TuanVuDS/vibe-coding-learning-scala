package vn.scalaeng.ch08c.solutions

import scala.quoted.*

object PercentLiteral:
  private def valid(value: Int): Boolean = value >= 0 && value <= 100

  def parse(value: Int): Either[String, Int] =
    Either.cond(valid(value), value, "percent must be in [0, 100]")

  inline def checked(inline value: Int): Int = ${ checkedImpl('value) }

  private def checkedImpl(value: Expr[Int])(using Quotes): Expr[Int] =
    value.value match
      case Some(number) if valid(number) => Expr(number)
      case Some(_) => quotes.reflect.report.errorAndAbort("percent literal must be in [0, 100]", value)
      case None => quotes.reflect.report.errorAndAbort("percent must be a constant integer", value)
