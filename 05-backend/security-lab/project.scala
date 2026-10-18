// Security & production lab (Chương 17). Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
//   scala-cli test .
//   JWT_SECRET=<>=32 ký tự> scala-cli run . --main-class vn.scalaeng.sec.SecureMain      # http://127.0.0.1:8090
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using resourceDir src/main/resources
//> using dep org.http4s::http4s-ember-server:0.23.38
//> using dep org.http4s::http4s-dsl:0.23.38
//> using dep org.http4s::http4s-circe:0.23.38
//> using dep io.circe::circe-core:0.14.17
//> using dep io.circe::circe-parser:0.14.17
//> using dep com.softwaremill.sttp.tapir::tapir-http4s-server:1.13.33
//> using dep com.softwaremill.sttp.tapir::tapir-json-circe:1.13.33
//> using dep com.password4j:password4j:1.8.4
//> using dep com.github.jwt-scala::jwt-circe:11.0.4
//> using dep org.http4s::http4s-prometheus-metrics:0.25.0
//> using dep org.typelevel::log4cats-slf4j:2.8.0
//> using dep ch.qos.logback:logback-classic:1.6.5
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
//> using test.dep org.typelevel::cats-effect-testkit:3.7.1
//> using test.dep org.http4s::http4s-ember-client:0.23.38
