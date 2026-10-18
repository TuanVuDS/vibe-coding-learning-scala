// Chương 20 — Transform lab. Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "transform-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core"        % "2.13.0",
      "co.fs2"        %% "fs2-core"         % "3.13.0",
      "org.scalameta" %% "munit"            % "1.2.4" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.2.0" % Test // POM 1.2.0 phụ thuộc munit 1.1.1 → bị nâng lên 1.2.4; bản 1.3.x kéo munit 1.3.x
    ),
    run / fork := true,
    Test / fork := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value
  )
