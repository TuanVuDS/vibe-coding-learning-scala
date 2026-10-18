ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / version := "0.1.0"
scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Werror")
libraryDependencies ++= Seq(
  "io.circe" %% "circe-parser" % "0.14.17",
  "org.apache.commons" % "commons-csv" % "1.14.1",
  "org.apache.avro" % "avro" % "1.12.2",
  "org.apache.parquet" % "parquet-avro" % "1.18.1",
  "org.apache.hadoop" % "hadoop-client-api" % "3.4.2",
  "org.apache.hadoop" % "hadoop-client-runtime" % "3.4.2",
  "org.slf4j" % "slf4j-nop" % "2.0.17",
  "org.scalameta" %% "munit" % "1.2.4" % Test
)
Test / fork := true
