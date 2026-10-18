// Chương 33 — Quality lab. Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "quality-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies ++= Seq(
      "io.circe"      %% "circe-parser"     % "0.14.17",
      "org.scalameta" %% "munit"            % "1.2.4" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.2.0" % Test
    ),
    run / fork := true,
    Test / fork := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value // test đọc data/ và golden/ theo đường dẫn tương đối
  )
