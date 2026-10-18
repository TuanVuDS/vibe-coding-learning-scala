// Chương 15 — Database lab. Phiên bản kiểm chứng trên repo1.maven.org ngày 16/10/2026.
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

val doobieV   = "1.0.0-RC13"
val flywayV   = "13.10.3"
val pgBinV    = "18.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "db-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies ++= Seq(
      "org.typelevel"  %% "doobie-core"                 % doobieV,
      "org.typelevel"  %% "doobie-postgres"             % doobieV,
      "org.typelevel"  %% "doobie-hikari"               % doobieV,
      "org.flywaydb"    % "flyway-core"                 % flywayV,
      "org.flywaydb"    % "flyway-database-postgresql"  % flywayV,
      "org.postgresql"  % "postgresql"                  % "42.7.14",
      "org.scalameta"  %% "munit"                       % "1.2.4" % Test,
      "org.typelevel"  %% "munit-cats-effect"           % "2.2.0" % Test,
      "io.zonky.test"   % "embedded-postgres"           % "2.2.2" % Test,
      // Binary PostgreSQL 18 thật cho từng hệ điều hành; zonky chọn đúng bản lúc chạy.
      "io.zonky.test.postgres" % "embedded-postgres-binaries-windows-amd64"  % pgBinV % Test,
      "io.zonky.test.postgres" % "embedded-postgres-binaries-linux-amd64"    % pgBinV % Test,
      "io.zonky.test.postgres" % "embedded-postgres-binaries-darwin-arm64v8" % pgBinV % Test
    ),
    run / fork := true
  )
