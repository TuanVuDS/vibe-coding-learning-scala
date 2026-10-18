// LƯU Ý: build.sbt của sbt 1.x được biên dịch bằng Scala 2.12
// -> dùng cú pháp Scala 2 ở file này (if (...) ... else ..., không dùng `then`).

ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"
ThisBuild / version      := "0.1.0-SNAPSHOT"

// Trong CI (GitHub Actions đặt CI=true) mọi cảnh báo trở thành lỗi.
val isCI = sys.env.get("CI").contains("true")

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all", "-Wvalue-discard") ++
    (if (isCI) Seq("-Werror") else Seq.empty),
  libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
)

// Tiện ích test dùng chung (Chương 10): chỉ được các module khác dùng ở scope Test
lazy val testkit = project
  .in(file("modules/testkit"))
  .settings(commonSettings, name := "order-testkit", publish / skip := true)

// Module thuần: không phụ thuộc thư viện ngoài nào
lazy val domain = project
  .in(file("modules/domain"))
  .settings(commonSettings, name := "order-domain")

// Logic nghiệp vụ: chỉ phụ thuộc domain
lazy val core = project
  .in(file("modules/core"))
  .dependsOn(domain, testkit % Test)                      // testkit chỉ có trên classpath khi test
  .settings(commonSettings, name := "order-core")

// Shell: đọc cấu hình, file, log, exit code
lazy val app = project
  .in(file("modules/app"))
  .dependsOn(core, testkit % Test)
  .settings(
    commonSettings,
    name := "order-app",
    Compile / mainClass := Some("vn.scalaeng.order.app.Main"),
    run / fork := true,                                   // chạy trong JVM riêng: exit code, env, javaOptions đúng
    run / javaOptions += "-Dstdout.encoding=UTF-8",
    run / envVars := Map("APP_ENV" -> "dev")
  )

lazy val root = project
  .in(file("."))
  .aggregate(testkit, domain, core, app)                           // `sbt test` ở root chạy test mọi module
  .settings(name := "order-platform", publish / skip := true)
