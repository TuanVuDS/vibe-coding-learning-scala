ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
name := "streaming-lab"
sourcesInBase := false
scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Werror")
libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % "3.13.0",
    "org.typelevel" %% "cats-effect" % "3.7.1",
    "org.scalameta" %% "munit" % "1.2.4" % Test,
    "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test
)
Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= {
  val temp = baseDirectory.value / ".cache" / "tmp"
  IO.createDirectory(temp)
  Seq("-Djava.io.tmpdir=" + temp.getAbsolutePath)
}
Compile / run / fork := true
