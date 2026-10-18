package vn.scalaeng.p2.service

import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters.*

class ArchitectureSuite extends munit.FunSuite:
  private def projectRoot: Path =
    val here=Path.of("").toAbsolutePath
    Iterator.iterate(here)(_.getParent).takeWhile(_ != null)
      .flatMap(p => List(p,p.resolve("14-projects/p2-backend-service")))
      .find(p => Files.isDirectory(p.resolve("domain/src/main/scala"))).get
  test("domain thuần và service không import adapter HTTP/DB/security") {
    val root=projectRoot
    List("domain","service").foreach { module =>
      val stream=Files.walk(root.resolve(s"$module/src/main/scala"))
      try stream.iterator.asScala.filter(_.toString.endsWith(".scala")).foreach { f =>
        val source=Files.readString(f)
        val forbidden=if module=="domain" then List("cats.","http4s","doobie","circe","tapir","p2.db","p2.http","p2.security")
          else List("http4s","doobie","tapir","p2.db","p2.http","p2.security")
        source.linesIterator.filter(_.startsWith("import ")).foreach(line =>
          forbidden.foreach(word => assert(!line.contains(word),s"$f: $line")))
      } finally stream.close()
    }
  }
