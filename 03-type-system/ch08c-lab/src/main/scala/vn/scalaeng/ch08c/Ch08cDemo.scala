package vn.scalaeng.ch08c

import java.time.LocalDate

object Ch08cDemo:
  def main(args: Array[String]): Unit =
    println(s"Scala=${scala.util.Properties.versionNumberString}; Java=${System.getProperty("java.version")}")
    println(s"batch=${InlineRules.positiveBatchSize(InlineRules.defaultBatchSize)}")
    println(s"format=${InlineRules.formatName("csv")}; partitions=${InlineRules.partitionCount[4]}")
    println(s"labels=${TypeTools.labels[("orderId", "total")].mkString(",")}")
    val row: Tuple.Map[(String, Int), Option] = TypeTools.optionalRow(("ORD-1", 2))
    println(s"optional=$row")
    println(s"topic=${TopicLiteral.checked("orders.created")}")
    val reports = List(
      OrderReport("ORD-1", "Nguyễn An", BigDecimal("495000"), Some(LocalDate.of(2026, 10, 1))),
      OrderReport("ORD-2", "Công ty \"ABC\", HN", BigDecimal("1E+3"), None)
    )
    println(CsvWriter.write(reports))
