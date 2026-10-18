// Transform lab (Chương 20): biến đổi dữ liệu theo phong cách functional — validation tích lũy, data quality rule,
// tổng hợp tổng quát bằng Monoid, cùng một phép tổng hợp chạy 3 kiểu: trong bộ nhớ, theo luồng, chia partition.
//   scala-cli test .
//   scala-cli run . --main-class vn.scalaeng.transform.TransformDemo
// Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep org.typelevel::cats-core:2.13.0
//> using dep co.fs2::fs2-core:3.13.0
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.scalameta::munit-scalacheck:1.2.0
