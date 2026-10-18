package scalaeng.ch02

/** Chương 2 — chương trình đầu tiên.
  *
  * `@main` biến một method thành điểm vào (entry point) của chương trình.
  * Tên class sinh ra là `scalaeng.ch02.hello`.
  */
@main def hello(): Unit =
  val scalaVersion = scala.util.Properties.versionNumberString
  val javaVersion  = System.getProperty("java.version")
  println(s"Xin chào Scala $scalaVersion trên JVM $javaVersion")
