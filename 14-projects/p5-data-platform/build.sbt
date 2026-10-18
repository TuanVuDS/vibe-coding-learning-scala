// Project 5 — Production-Oriented Data Platform. Phiên bản giống P2/P4 (đã kiểm chứng trên repo1.maven.org ngày 17/10/2026).
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard", "-Werror")

val munit = "org.scalameta" %% "munit" % "1.2.4" % Test
val pgEmbedded = Seq(
  "io.zonky.test" % "embedded-postgres" % "2.2.2" % Test,
  "io.zonky.test.postgres" % "embedded-postgres-binaries-windows-amd64" % "18.6.0" % Test,
  "io.zonky.test.postgres" % "embedded-postgres-binaries-linux-amd64" % "18.6.0" % Test,
  "io.zonky.test.postgres" % "embedded-postgres-binaries-darwin-arm64v8" % "18.6.0" % Test)
val testSettings = Seq(Test / fork := true, Test / parallelExecution := false,
  Test / baseDirectory := (ThisBuild / baseDirectory).value) // test đọc data/ theo đường dẫn tương đối

lazy val root = (project in file(".")).aggregate(core, ingest, serving, app)
  .settings(name := "p5-data-platform", publish / skip := true)

lazy val core = (project in file("core")).settings(testSettings,
  libraryDependencies ++= Seq("io.circe" %% "circe-parser" % "0.14.17", "io.circe" %% "circe-generic" % "0.14.17", munit))

lazy val ingest = (project in file("ingest")).dependsOn(core).settings(testSettings,
  libraryDependencies ++= Seq("org.apache.kafka" % "kafka-clients" % "4.4.0", "org.slf4j" % "slf4j-nop" % "2.0.20", munit,
    "io.github.embeddedkafka" %% "embedded-kafka" % "4.4.0" % Test))

lazy val serving = (project in file("serving")).dependsOn(core).settings(testSettings,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "3.7.1",
    "org.typelevel" %% "doobie-core" % "1.0.0-RC13", "org.typelevel" %% "doobie-postgres" % "1.0.0-RC13",
    "org.postgresql" % "postgresql" % "42.7.14",
    "org.http4s" %% "http4s-dsl" % "0.23.38", "org.http4s" %% "http4s-ember-server" % "0.23.38", "org.http4s" %% "http4s-circe" % "0.23.38",
    munit, "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test, "org.http4s" %% "http4s-ember-client" % "0.23.38" % Test) ++ pgEmbedded)

lazy val app = (project in file("app")).dependsOn(ingest, serving, core % "compile->compile;test->test").settings(testSettings,
  libraryDependencies ++= Seq(munit, "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test,
    "org.http4s" %% "http4s-ember-client" % "0.23.38" % Test) ++ pgEmbedded,
  Compile / run / fork := true, Compile / mainClass := Some("vn.scalaeng.p5.Main"),
  assembly / mainClass := Some("vn.scalaeng.p5.Main"), assembly / assemblyJarName := "p5.jar", assembly / test := {},
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
    case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
    case _                                        => MergeStrategy.first
  })
