// Project 3 — Batch Data Pipeline. Scala 2.13 vì Spark 4.2 chỉ có bản _2.13 (Chương 26 mục 3.8). BUILD RIÊNG, Scala 2.13: Spark 4.x không có bản build cho Scala 3.
// Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
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
    name := "p3-batch-pipeline",
    sourcesInBase := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint:_", "-Wunused:imports,privates,locals,params"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"                   % sparkV % Provided, // cụm/spark-submit cung cấp — KHÔNG đóng vào jar
      "com.globalmentor"  % "hadoop-bare-naked-local-fs"  % "0.1.0", // chỉ cần trên Windows
      "org.scalameta"    %% "munit"                       % "1.2.4" % Test
    ),
    // Spark phải chạy trong JVM RIÊNG (fork) để nhận javaOptions và không xung đột classloader của sbt.
    // `Provided` không nằm trên classpath của `run` mặc định — thêm lại để chạy local bằng sbt.
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    assembly / mainClass := Some("vn.scalaeng.p3.Job"),
    assembly / assemblyJarName := "p3-batch-pipeline.jar",
    // Spark trên cụm đã có scala-library 2.13 — đóng thêm một bản vào jar chỉ gây xung đột phiên bản và làm jar to gấp chục lần.
    assembly / assemblyOption ~= (_.withIncludeScala(false)),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    },
    run / fork := true,
    Test / fork := true,
    run / javaOptions ++= sparkJavaOpts,
    Test / javaOptions ++= sparkJavaOpts,
    Test / parallelExecution := false, // một SparkSession mỗi JVM
    Test / baseDirectory := (ThisBuild / baseDirectory).value // test đọc data/ theo đường dẫn tương đối
  )
