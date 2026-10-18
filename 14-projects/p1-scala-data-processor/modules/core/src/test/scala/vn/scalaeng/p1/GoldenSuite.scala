package vn.scalaeng.p1

import munit.FunSuite
import vn.scalaeng.p1.pipeline.Processor
import vn.scalaeng.p1.report.Reports
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters.*

/** Golden test trên bộ dữ liệu mẫu. report.csv và summary.json được TÍNH TAY trước khi chạy chương trình (xem README). */
class GoldenSuite extends FunSuite:
  // Thư mục gốc project: sbt chạy test từ root (không fork) hoặc từ thư mục module (khi fork).
  private val root = Paths.get(sys.props.getOrElse("p1.root",
    if Files.exists(Paths.get("data/orders.csv")) then "." else "../.."))
  private def read(p: String) = Files.readString(root.resolve(p), UTF_8)
  private lazy val result = Processor.run(
    Files.readAllLines(root.resolve("data/orders.csv"), UTF_8).asScala.toList, read("data/users.json")).toOption.get

  test("report.csv"):
    assertNoDiff(Reports.reportCsv(result.report), read("expected/report.csv"))
  test("summary.json"):
    assertNoDiff(Reports.summaryJson(result.stats), read("expected/summary.json"))
  test("rejected.csv"):
    assertNoDiff(Reports.rejectedCsv(result.rejected), read("expected/rejected.csv"))
