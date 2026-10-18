// Ingest lab (Chương 19): xử lý sự kiện quảng cáo (impression/click) dạng JSON Lines + bảng campaign CSV.
//   scala-cli test .
//   scala-cli run . --main-class vn.scalaeng.ingest.IngestMain -- data/events.jsonl data/campaigns.csv out
//   scala-cli run . --main-class vn.scalaeng.ingest.GenerateEvents -- big.jsonl 1000000
//   scala-cli run . --main-class vn.scalaeng.ingest.NaiveIngest --java-opt -Xmx64m -- big.jsonl
// Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep co.fs2::fs2-io:3.13.0
//> using dep org.gnieh::fs2-data-csv:1.14.1
//> using dep org.gnieh::fs2-data-csv-generic:1.14.1
//> using dep io.circe::circe-parser:0.14.17
//> using test.dep org.scalameta::munit:1.2.4
//> using test.dep org.typelevel::munit-cats-effect:2.2.0
