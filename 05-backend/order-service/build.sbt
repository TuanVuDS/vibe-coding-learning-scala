ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard")
ThisBuild / javacOptions ++= Seq("--release", "21")

val http4sV = "0.23.38"
val tapirV = "1.13.33"
val tests = Seq(
  "org.scalameta" %% "munit" % "1.2.4" % Test,
  "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test
)

lazy val root = (project in file("."))
  .aggregate(domain, app)
  .settings(name := "order-service", sourcesInBase := false, publish / skip := true)

lazy val domain = (project in file("domain"))
  .settings(name := "orders-domain", libraryDependencies += tests.head)

lazy val app = (project in file("app"))
  .dependsOn(domain)
  .settings(
    name := "orders-app",
    libraryDependencies ++= tests ++ Seq(
      "org.typelevel" %% "cats-effect" % "3.7.1",
      "org.http4s" %% "http4s-ember-server" % http4sV,
      "org.http4s" %% "http4s-dsl" % http4sV,
      "io.circe" %% "circe-core" % "0.14.17",
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapirV,
      "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % "0.11.10",
      "org.http4s" %% "http4s-ember-client" % http4sV % Test
    ),
    Compile / run / fork := true,
    Compile / mainClass := Some("vn.scalaeng.orders.Main")
  )
