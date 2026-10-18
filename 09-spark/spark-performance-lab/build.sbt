// Chương 25 — Spark Performance lab. BUILD RIÊNG, Scala 2.13: Spark 4.x không có bản build cho Scala 3.
// 17/10/2026: POM Maven Central trong Coursier cache + Scala CLI compile/run; sbt chờ lead chạy.
ThisBuild / scalaVersion := "2.13.18"
ThisBuild / organization := "vn.scalaeng"

val sparkV = "4.2.0"

// Đúng các cờ --add-opens mà spark-submit tự thêm (org.apache.spark.launcher.JavaModuleOptions, spark-launcher 4.2.0).
val sparkJavaOpts = Seq(
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-performance-lab",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint:_", "-Wunused:_"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"                   % sparkV,
      "com.globalmentor"  % "hadoop-bare-naked-local-fs"  % "0.1.0", // chỉ cần trên Windows
      "org.scalameta"    %% "munit"                       % "1.2.4" % Test
    ),
    // Spark phải chạy trong JVM RIÊNG (fork) để nhận javaOptions và không xung đột classloader của sbt.
    run / fork := true,
    Test / fork := true,
    run / javaOptions ++= sparkJavaOpts,
    Test / javaOptions ++= sparkJavaOpts,
    Test / parallelExecution := false, // suite dùng chung SparkContext và SQLConf
    Test / baseDirectory := (ThisBuild / baseDirectory).value
  )
