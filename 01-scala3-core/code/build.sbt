// Cấu hình sbt tương đương với project.scala (dùng khi học Chương 2 phần sbt).
// Chạy:  sbt test   |   sbt "runMain scalaeng.ch03.ch03Demo"
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "scala3-core-examples",
    sourcesInBase := false, // không biên dịch project.scala (file cấu hình của Scala CLI)
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all"),
    libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
  )
