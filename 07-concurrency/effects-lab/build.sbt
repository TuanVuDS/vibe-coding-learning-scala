ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / version := "0.1.0"

name := "effects-lab"
scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Werror")
libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.7.1",
  "org.scalameta" %% "munit" % "1.2.4" % Test,
  "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test,
  "org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test
)
