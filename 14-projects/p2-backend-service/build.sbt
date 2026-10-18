ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard", "-Werror")
ThisBuild / javacOptions ++= Seq("--release", "21")
val tests = Seq("org.scalameta" %% "munit" % "1.2.4" % Test,
  "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test,
  "org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test)
lazy val root = (project in file(".")).aggregate(domain,service,db,security,http,app)
  .settings(name := "p2-backend-service", sourcesInBase := false, publish / skip := true)
lazy val domain = (project in file("domain")).settings(libraryDependencies += tests.head)
lazy val service = (project in file("service")).dependsOn(domain)
  .settings(libraryDependencies ++= tests :+ ("org.typelevel" %% "cats-effect" % "3.7.1"))
lazy val db = (project in file("db")).dependsOn(service)
  .settings(libraryDependencies ++= tests ++ Seq(
    "org.typelevel" %% "doobie-core" % "1.0.0-RC13",
    "org.typelevel" %% "doobie-postgres" % "1.0.0-RC13",
    "org.typelevel" %% "doobie-hikari" % "1.0.0-RC13",
    "org.flywaydb" % "flyway-core" % "13.10.3",
    "org.flywaydb" % "flyway-database-postgresql" % "13.10.3",
    "org.postgresql" % "postgresql" % "42.7.14",
    "com.zaxxer" % "HikariCP" % "7.0.2",
    "ch.qos.logback" % "logback-classic" % "1.6.5" % Test,
    "io.zonky.test" % "embedded-postgres" % "2.2.2" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-windows-amd64" % "18.6.0" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-linux-amd64" % "18.6.0" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-darwin-arm64v8" % "18.6.0" % Test),
    Test / fork := true, Test / parallelExecution := false)
lazy val security = (project in file("security")).dependsOn(service)
  .settings(libraryDependencies ++= tests ++ Seq("com.password4j" % "password4j" % "1.8.4",
    "com.github.jwt-scala" %% "jwt-circe" % "11.0.4"))
lazy val http = (project in file("http")).dependsOn(security)
  .settings(libraryDependencies ++= tests ++ Seq(
    "org.http4s" %% "http4s-dsl" % "0.23.38",
    "org.http4s" %% "http4s-ember-server" % "0.23.38",
    "org.http4s" %% "http4s-ember-client" % "0.23.38" % Test,
    "io.circe" %% "circe-core" % "0.14.17",
    "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % "1.13.33",
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % "1.13.33",
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % "1.13.33",
    "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % "0.11.10",
    "org.typelevel" %% "log4cats-slf4j" % "2.8.0",
    "ch.qos.logback" % "logback-classic" % "1.6.5"))
lazy val app = (project in file("app")).dependsOn(db,http)
  .settings(libraryDependencies ++= tests ++ Seq(
    "io.zonky.test" % "embedded-postgres" % "2.2.2" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-windows-amd64" % "18.6.0" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-linux-amd64" % "18.6.0" % Test,
    "io.zonky.test.postgres" % "embedded-postgres-binaries-darwin-arm64v8" % "18.6.0" % Test),
    Test / fork := true, Test / parallelExecution := false, Compile / run / fork := true,
    Compile / mainClass := Some("vn.scalaeng.p2.app.Main"),
    assembly / mainClass := Some("vn.scalaeng.p2.app.Main"),
    assembly / assemblyJarName := "p2.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    })
