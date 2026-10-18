// Chương 17 — Security & production lab. Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

val http4sV = "0.23.38"
val tapirV  = "1.13.33"

lazy val root = (project in file("."))
  .settings(
    name := "security-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies ++= Seq(
      "org.http4s"                  %% "http4s-ember-server"       % http4sV,
      "org.http4s"                  %% "http4s-dsl"                % http4sV,
      "org.http4s"                  %% "http4s-circe"              % http4sV,
      "io.circe"                    %% "circe-core"                % "0.14.17",
      "io.circe"                    %% "circe-parser"              % "0.14.17",
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"       % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe"          % tapirV,
      "com.password4j"               % "password4j"                % "1.8.4",
      "com.github.jwt-scala"        %% "jwt-circe"                 % "11.0.4",
      "org.http4s"                  %% "http4s-prometheus-metrics" % "0.25.0",
      "org.typelevel"               %% "log4cats-slf4j"            % "2.8.0",
      "ch.qos.logback"               % "logback-classic"           % "1.6.5",
      "org.scalameta"               %% "munit"                     % "1.2.4"  % Test,
      "org.typelevel"               %% "munit-cats-effect"         % "2.2.0"  % Test,
      "org.typelevel"               %% "cats-effect-testkit"       % "3.7.1"  % Test,
      "org.http4s"                  %% "http4s-ember-client"       % http4sV  % Test
    ),
    run / fork := true
  )
