package vn.scalaeng.app

import munit.FunSuite
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/** Lời giải Ch9 bài 4 — test kiến trúc: module domain không được nhắc tới pipeline/app.
  * Đặt file này trong modules/app/src/test/scala/vn/scalaeng/app/.
  */
class Ch09ArchitectureSuite extends FunSuite:
  /** Tìm thư mục gốc build bằng cách đi ngược lên tới khi gặp build.sbt — không phụ thuộc thư mục làm việc. */
  private def buildRoot: Path =
    Iterator.iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(p => Files.exists(p.resolve("build.sbt")) && Files.isDirectory(p.resolve("modules/domain")))
      .getOrElse(fail("không tìm thấy thư mục gốc order-platform"))

  test("domain không import pipeline hoặc app"):
    val files = Files.walk(buildRoot.resolve("modules/domain/src/main")).iterator.asScala
      .filter(_.toString.endsWith(".scala")).toList
    assert(files.nonEmpty, "không tìm thấy file nào — đường dẫn sai sẽ làm test pass vô nghĩa")
    val violations = for
      f    <- files
      line <- Files.readAllLines(f).asScala
      if line.contains("vn.scalaeng.pipeline") || line.contains("vn.scalaeng.app")
    yield s"${f.getFileName}: $line"
    assertEquals(violations, Nil)
