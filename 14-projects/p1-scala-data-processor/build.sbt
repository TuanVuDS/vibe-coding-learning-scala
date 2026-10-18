// Project 1 — Scala Data Processor. build.sbt dùng cú pháp Scala 2 (sbt 1.x biên dịch build bằng Scala 2.12).
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / version      := "1.0.0"

val isCI = sys.env.get("CI").contains("true")

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard") ++
    (if (isCI) Seq("-Werror") else Seq.empty),
  libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
)

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings, name := "p1-core")

lazy val app = project
  .in(file("modules/app"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name := "p1-app",
    Compile / mainClass := Some("vn.scalaeng.p1.app.Main"),
    run / fork := true,
    run / baseDirectory := (ThisBuild / baseDirectory).value,
    run / javaOptions ++= Seq("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
  )

lazy val root = project
  .in(file("."))
  .aggregate(core, app)
  .settings(name := "p1-scala-data-processor", publish / skip := true)
