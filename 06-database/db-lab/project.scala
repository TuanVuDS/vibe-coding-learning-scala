// Database lab (Chương 15): PostgreSQL + Flyway + Doobie (Cats Effect).
//   scala-cli test .                     # test chạy PostgreSQL THẬT nhúng (zonky embedded-postgres), không cần Docker
//   docker compose up -d                 # PostgreSQL 18 cho chương trình chạy tay
//   scala-cli run . --main-class vn.scalaeng.db.DbDemo
// Phiên bản kiểm chứng trên repo1.maven.org ngày 16/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using resourceDir src/main/resources
//> using dep org.typelevel::doobie-core:1.0.0-RC13
//> using dep org.typelevel::doobie-postgres:1.0.0-RC13
//> using dep org.typelevel::doobie-hikari:1.0.0-RC13
//> using dep org.flywaydb:flyway-core:13.10.3
//> using dep org.flywaydb:flyway-database-postgresql:13.10.3
//> using dep org.postgresql:postgresql:42.7.14
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
//> using test.dep io.zonky.test:embedded-postgres:2.2.2
//> using test.dep io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0
//> using test.dep io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0
//> using test.dep io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0
