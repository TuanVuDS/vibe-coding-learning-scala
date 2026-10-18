// Chương 19 — Ingest lab. Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "ingest-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies ++= Seq(
      "co.fs2"        %% "fs2-io"               % "3.13.0",
      "org.gnieh"     %% "fs2-data-csv"         % "1.14.1",
      "org.gnieh"     %% "fs2-data-csv-generic" % "1.14.1",
      "io.circe"      %% "circe-parser"         % "0.14.17",
      "org.scalameta" %% "munit"                % "1.2.4" % Test,
      "org.typelevel" %% "munit-cats-effect"    % "2.2.0" % Test
    ),
    run / fork := true,
    // Test đọc data/ và expected/ theo đường dẫn tương đối: chạy test trong thư mục project.
    Test / fork := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value
  )
