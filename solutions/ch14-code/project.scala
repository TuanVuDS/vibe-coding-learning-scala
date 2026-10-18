// Dùng source chính của lab, không nạp test của lab; không sao chép domain/service.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using file ../../05-backend/order-service/domain/src/main/scala
//> using file ../../05-backend/order-service/app/src/main/scala
//> using dep org.typelevel::cats-effect:3.7.1
//> using dep org.http4s::http4s-ember-server:0.23.38
//> using dep org.http4s::http4s-dsl:0.23.38
//> using dep io.circe::circe-core:0.14.17
//> using dep com.softwaremill.sttp.tapir::tapir-http4s-server:1.13.33
//> using dep com.softwaremill.sttp.tapir::tapir-json-circe:1.13.33
//> using dep com.softwaremill.sttp.tapir::tapir-openapi-docs:1.13.33
//> using dep com.softwaremill.sttp.apispec::openapi-circe-yaml:0.11.10
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
