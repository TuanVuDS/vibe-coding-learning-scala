// Architecture lab (Chương 31): các lớp raw → staging → serving, asset phân vùng theo ngày, lineage, phát hiện partition cũ, backfill idempotent.
// Orchestration (lập lịch, chọn partition nào chạy) tách khỏi processing (tính một partition) — dagster/ là ví dụ orchestrator Python gọi job này.
//   scala-cli test .
//   scala-cli run . --main-class vn.scalaeng.platform.Main -- status --root=<thư-mục> --from=2026-10-15 --to=2026-10-17
// Phiên bản kiểm chứng trên repo1.maven.org ngày 17/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep io.circe::circe-parser:0.14.17
//> using dep io.circe::circe-generic:0.14.17
//> using test.dep org.scalameta::munit:1.2.4
