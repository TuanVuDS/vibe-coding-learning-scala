package vn.scalaeng.app

import munit.FunSuite
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import vn.scalaeng.pipeline.ReportSink
import vn.scalaeng.testkit.InMemorySink

/** Lời giải Ch10 bài 4 — contract test cho ReportSink.
  * Port không cần thêm method "read": khả năng đọc lại là nhu cầu của TEST, nên mỗi suite tự cung cấp.
  */
abstract class ReportSinkContract extends FunSuite:
  /** Trả về sink mới và một hàm đọc lại nội dung theo tên. */
  def newSink(): (ReportSink, String => Option[String])

  test("ghi rồi đọc lại đúng nội dung (kể cả UTF-8)"):
    val (sink, read) = newSink()
    assertEquals(sink.write("a.csv", "x,y\nđơn hàng\n"), Right(()))
    assertEquals(read("a.csv"), Some("x,y\nđơn hàng\n"))

  test("ghi đè cùng tên giữ bản mới nhất"):
    val (sink, read) = newSink()
    sink.write("a.csv", "cũ"); sink.write("a.csv", "mới")
    assertEquals(read("a.csv"), Some("mới"))

class InMemorySinkContractSuite extends ReportSinkContract:
  def newSink() =
    val s = InMemorySink()
    (s, s.written.get)

class FileSinkContractSuite extends ReportSinkContract:
  def newSink() =
    val dir: Path = Files.createTempDirectory("sink-contract")
    dir.toFile.deleteOnExit()
    (FileSink(dir), name => Option.when(Files.exists(dir.resolve(name)))(Files.readString(dir.resolve(name), UTF_8)))
