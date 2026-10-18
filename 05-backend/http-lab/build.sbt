ThisBuild / scalaVersion := "3.9.0"
lazy val root = (project in file("."))
  .settings(
    name := "http-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard"),
    libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test,
    Compile / mainClass := Some("vn.scalaeng.http.Main"),
    run / fork := true
  )
