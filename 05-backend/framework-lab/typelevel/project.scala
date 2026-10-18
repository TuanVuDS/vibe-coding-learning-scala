// Framework lab (Chương 13): cùng một API `products` viết bằng http4s và bằng Tapir (chạy trên http4s).
//   scala-cli run . --main-class vn.scalaeng.fw.Http4sMain     # cổng 8081
//   scala-cli run . --main-class vn.scalaeng.fw.TapirMain      # cổng 8082, có /docs/openapi.yaml
//   scala-cli run . --main-class vn.scalaeng.fw.PrintOpenApi
//   scala-cli test .
// Phiên bản kiểm chứng trên Maven Central ngày 16/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep org.http4s::http4s-ember-server:0.23.38
//> using dep org.http4s::http4s-dsl:0.23.38
//> using dep org.http4s::http4s-circe:0.23.38
//> using dep io.circe::circe-core:0.14.17
//> using dep com.softwaremill.sttp.tapir::tapir-http4s-server:1.13.33
//> using dep com.softwaremill.sttp.tapir::tapir-json-circe:1.13.33
//> using dep com.softwaremill.sttp.tapir::tapir-openapi-docs:1.13.33
//> using dep com.softwaremill.sttp.apispec::openapi-circe-yaml:0.11.10
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
