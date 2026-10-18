package vn.scalaeng.ch08c

import scala.compiletime.{constValue, error}

object InlineRules:
  inline val defaultBatchSize = 100

  inline def positiveBatchSize(inline value: Int): Int =
    inline if value > 0 then value
    else error("batch size must be positive")

  inline def formatName(inline format: String): String =
    inline format match
      case "csv"  => "text/csv"
      case "json" => "application/json"
      case _      => error("format must be csv or json")

  inline def partitionCount[N <: Int]: Int = constValue[N]

  // Tham số thường vẫn chỉ được đánh giá một lần dù def được inline.
  inline def twice(value: Int): Int = value + value

  // Tham số inline được thay bằng biểu thức ở từng nơi dùng.
  inline def twiceInline(inline value: Int): Int = value + value
