// Project 5 — Scala CLI biên dịch MỌI module cùng nhau (core, ingest, serving, app). sbt (build.sbt) giữ ranh giới module.
//   scala-cli test project.scala core ingest serving app     (unit; RUN_DB_TESTS=1: PostgreSQL nhúng, RUN_KAFKA_IT=1: Kafka KRaft nhúng)
//   (liệt kê module để KHÔNG kéo công cụ dev trong orchestration/ vào build)
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard -Werror
//> using dep io.circe::circe-parser:0.14.17
//> using dep io.circe::circe-generic:0.14.17
//> using dep org.apache.kafka:kafka-clients:4.4.0
//> using dep org.slf4j:slf4j-nop:2.0.20
//> using dep org.typelevel::cats-effect:3.7.1
//> using dep org.typelevel::doobie-core:1.0.0-RC13
//> using dep org.typelevel::doobie-postgres:1.0.0-RC13
//> using dep org.postgresql:postgresql:42.7.14
//> using dep org.http4s::http4s-dsl:0.23.38
//> using dep org.http4s::http4s-ember-server:0.23.38
//> using dep org.http4s::http4s-circe:0.23.38
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
//> using test.dep org.http4s::http4s-ember-client:0.23.38
//> using test.dep io.github.embeddedkafka::embedded-kafka:4.4.0
//> using test.dep io.zonky.test:embedded-postgres:2.2.2
//> using test.dep io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0
