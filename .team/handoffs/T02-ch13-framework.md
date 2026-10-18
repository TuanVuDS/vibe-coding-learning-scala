# Handoff T02 — Chương 13: Chọn framework (tác giả: Claude Code)

## File đã tạo / sửa
- `05-backend/ch13-choosing-a-framework.md` (bài học, 17 mục)
- `05-backend/framework-lab/typelevel/` (http4s + Tapir, 13 test), `05-backend/framework-lab/zio-http/` (ZIO HTTP, 5 test)
- `solutions/ch13-solutions.md`, `solutions/ch13-code/` (5 test, trong đó 1 test qua socket thật)

## Kiểm chứng đã chạy (16/10/2026, Windows 11, JDK 21, scala-cli 1.9.1)
- `framework-lab/typelevel`: `scala-cli test .` → Http4sContractSuite 5/5, TapirContractSuite 5/5, OpenApiSuite 3/3; 0 cảnh báo
- `framework-lab/zio-http`: 5/5; 0 cảnh báo
- `solutions/ch13-code`: 5/5
- Server Tapir (8082) và http4s (8081) chạy thật, trao đổi curl trích ở mục 9 là output thật.
- Lỗi compiler ở mục 10 lấy từ bản sao lab bị xóa import (thật).

## Phiên bản (repo1.maven.org maven-metadata.xml, 16/10/2026)
http4s 0.23.38 · Tapir 1.13.33 · sttp-apispec 0.11.10 · circe 0.14.17 · Play 3.0.12 (3.1.0-M9) · ZIO 2.1.27 · ZIO HTTP 3.11.6 ·
munit-cats-effect 2.2.1 · Cats Effect 3.7.1 · Doobie 1.0.0-RC13 (org.typelevel) · Slick 3.6.1 (4.0.0-RC2) · Flyway 13.10.3 ·
fs2-kafka 4.1.3 (org.typelevel; GA) · munit mới nhất 1.3.6 (giáo trình giữ 1.2.4).

## Đề xuất cập nhật file dùng chung
- compatibility matrix: Play 3.0.12 ✅ Scala 3; ZIO 2.1.27/ZIO HTTP 3.11.6 ✅; fs2-kafka 4.1.3 GA (thay 4.0.0-RC1); http4s 0.23.38; Tapir 1.13.33.
- Ghi chú: search.maven.org trả dữ liệu cũ — dùng repo1 maven-metadata.xml.

## Chưa kiểm chứng
- Play: không có lab (lý do trong bài, mục 3.2).
- Hành vi http4s khi thiếu Content-Type đã đo; Tapir khi thiếu Content-Type chưa đo.
