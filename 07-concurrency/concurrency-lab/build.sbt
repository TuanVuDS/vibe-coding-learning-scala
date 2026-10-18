ThisBuild / scalaVersion := "3.9.0"
lazy val root = (project in file("."))
  .settings(
    name := "concurrency-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test,
    run / fork := true
  )
