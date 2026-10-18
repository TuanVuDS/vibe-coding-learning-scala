ThisBuild / scalaVersion := "3.9.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Werror")
libraryDependencies ++= Seq(
  "io.circe" %% "circe-parser" % "0.14.17",
  "org.apache.avro" % "avro" % "1.12.2",
  "org.slf4j" % "slf4j-nop" % "2.0.17",
  "org.scalameta" %% "munit" % "1.2.4" % Test
)
Test / parallelExecution := false
