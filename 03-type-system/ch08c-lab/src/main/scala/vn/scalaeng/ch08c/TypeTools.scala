package vn.scalaeng.ch08c

import scala.compiletime.{constValue, erasedValue}

object TypeTools:
  type Payload[X] = X match
    case Option[t] => t
    case List[t]   => t
    case _         => X

  inline def labels[Labels <: Tuple]: List[String] =
    inline erasedValue[Labels] match
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => constValue[head].toString :: labels[tail]

  def optionalRow[Ts <: Tuple](row: Ts): Tuple.Map[row.type, Option] =
    row.map([A] => (value: A) => Option(value))
