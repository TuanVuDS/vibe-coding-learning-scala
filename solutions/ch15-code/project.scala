// Lời giải code Chương 15. Dùng lại mã nguồn + migration + PostgresSuite của 06-database/db-lab.
// Vì `using file` đưa PostgresSuite vào scope main, các thư viện test được khai báo bằng `dep` thay vì `test.dep`.
//   scala-cli test .
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using file ../../06-database/db-lab/src/main/scala
//> using file ../../06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala
//> using resourceDir ../../06-database/db-lab/src/main/resources
//> using resourceDir src/main/resources
//> using dep org.typelevel::doobie-core:1.0.0-RC13
//> using dep org.typelevel::doobie-postgres:1.0.0-RC13
//> using dep org.typelevel::doobie-hikari:1.0.0-RC13
//> using dep org.flywaydb:flyway-core:13.10.3
//> using dep org.flywaydb:flyway-database-postgresql:13.10.3
//> using dep org.postgresql:postgresql:42.7.14
//> using dep org.scalameta::munit:1.2.4
//> using dep org.typelevel::munit-cats-effect:2.2.0
//> using dep io.zonky.test:embedded-postgres:2.2.2
//> using dep io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0
//> using dep io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0
//> using dep io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8:18.6.0
