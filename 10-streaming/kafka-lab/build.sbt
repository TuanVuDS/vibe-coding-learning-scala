ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
name := "kafka-lab"
sourcesInBase := false
scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Werror")
libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "4.4.0",
    "io.circe" %% "circe-parser" % "0.14.17",
    "org.slf4j" % "slf4j-nop" % "2.0.20",
    "org.scalameta" %% "munit" % "1.2.4" % Test,
    "io.github.embeddedkafka" %% "embedded-kafka" % "4.4.0" % Test
)
Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= {
  val temp = baseDirectory.value / ".cache" / "tmp"
  IO.createDirectory(temp)
  Seq("-Djava.io.tmpdir=" + temp.getAbsolutePath)
}
Compile / run / fork := true
