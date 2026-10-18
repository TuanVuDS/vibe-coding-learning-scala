// Quality lab (Chương 33): kiểm thử pipeline dữ liệu — checks, data contract + tương thích schema, golden dataset + regression,
// phát hiện drift giữa hai lần chạy, property test cho dữ liệu đến trễ / trùng / chạy lại / backfill.
//   scala-cli test .
//   scala-cli run . --main-class vn.scalaeng.quality.QualityDemo
// Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep io.circe::circe-parser:0.14.17
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.scalameta::munit-scalacheck:1.2.0
