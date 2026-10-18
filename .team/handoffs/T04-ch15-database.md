# Handoff T04 — Chương 15: Database Engineering (tác giả: Claude Code)

## File
- `06-database/ch15-database-engineering.md` (17 mục)
- `06-database/db-lab/` (project.scala + build.sbt + docker-compose.yml; 14 test trên PostgreSQL 18.6 nhúng)
- `solutions/ch15-solutions.md`, `solutions/ch15-code/` (7 test)

## Kiểm chứng (16–17/10/2026, Windows 11, JDK 21)
- `scala-cli test 06-database/db-lab`: QueryPlanSuite 1, RepositorySuite 6, TransactionSuite 7 — pass. `sbt test`: Passed 14.
- `scala-cli test solutions/ch15-code`: 7/7.
- DbDemo chạy 2 lần với PostgreSQL 18.6 thật (nhúng, cổng 54330) — output mục 9.
- EXPLAIN / lỗi SQLSTATE trong bài là output thật (script ở scratchpad của Claude).
- docker-compose.yml: CHƯA chạy (Docker daemon tắt).

## Phiên bản (repo1.maven.org)
Doobie 1.0.0-RC13 (org.typelevel; package org.typelevel.doobie) · Flyway 13.10.3 (+ flyway-database-postgresql) · pgjdbc 42.7.14 ·
embedded-postgres 2.2.2 + binaries 18.6.0 · Slick 3.6.1 / 4.0.0-RC2 · Skunk 2.0.0-RC3 (org.tpolecat) · Docker postgres:18.6.

## Lưu ý cho Codex (Ch14 / P2)
- Repository Doobie trả `ConnectionIO`, service gọi `transact`. Lỗi nghiệp vụ là `Either`, lỗi hạ tầng là exception.
- Khi nối trait repository của Ch14 với Doobie: cài trait bằng `ConnectionIO` bên trong và `transact` ở cài đặt, hoặc đổi service sang nhận `Transactor`.

## Chưa kiểm chứng
- docker-compose.yml (volume path `/var/lib/postgresql` cho image 18).
