# Handoff T05 — Chương 17: Security & Production (tác giả: Claude Code)

## File
- `05-backend/ch17-security-production.md` (17 mục)
- `05-backend/security-lab/` (project.scala + build.sbt; 23 test)
- `solutions/ch17-solutions.md`, `solutions/ch17-code/` (5 test; bài 5 sinh keystore bằng keytool, HTTPS thật)

## Kiểm chứng (17/10/2026)
- scala-cli test: CryptoSuite 9, ApiSuite 6, ProductionSuite 8; sbt test: Passed 23. Lời giải 5/5.
- SecureMain chạy thật cổng 8090; curl ở mục 9 là thật; số đo Argon2id 44.4 ms / SHA-256 0.023 ms đo trên máy biên soạn (12 CPU logic).
- 3 lỗi thiết kế phát hiện khi chạy thật đã sửa + có test (mục 10.1, 10.2, 10.6).

## Phiên bản
password4j 1.8.4 · jwt-circe 11.0.4 · http4s-prometheus-metrics 0.25.0 (dùng prometheus simpleclient) · log4cats-slf4j 2.8.0 · logback 1.6.5.

## Chưa kiểm chứng
- Graceful shutdown qua tín hiệu SIGTERM thật (đã kiểm chứng bằng release Resource trong test; Windows không gửi SIGTERM dễ dàng).
