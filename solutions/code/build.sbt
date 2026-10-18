// Lời giải bài tập Giai đoạn 1 — cấu hình sbt tương đương project.scala.
// Chạy:  sbt test
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "scala3-core-solutions",
    sourcesInBase := false, // không biên dịch project.scala (file cấu hình của Scala CLI)
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all"),
    libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
  )
