# Scala 3 Engineering — Giáo trình Backend & Data Engineering

Giáo trình tiếng Việt từ cơ bản đến nâng cao về **Scala 3** cho hai hướng nghề nghiệp:
**Backend Engineering** và **Data Engineering**. Không bao gồm frontend.

> **Trạng thái:** Phase 1–3 hoàn thành (Chương 1–11 + **Project 1**); **Phase 4 Backend**: Chương 12–17 xong, Ch18 + Project 2 đang làm;
> **Phase 5 Data**: Ch19, Ch21 đang review, Ch20 đang viết. Biên soạn bởi team **Claude Code + Codex** với review chéo — xem [AGENTS.md](AGENTS.md).
> Chi tiết: [PROGRESS.md](PROGRESS.md). Ngày kiểm chứng phiên bản: **14/10/2026**.

## Bắt đầu từ đâu?

1. Đọc [00-roadmap/07-key-questions.md](00-roadmap/07-key-questions.md) — 6 câu hỏi định hướng (Scala 3 vs production, Spark/Scala, khi nào dùng Spark...).
2. Xem [curriculum](00-roadmap/01-curriculum.md) và [sơ đồ prerequisite](00-roadmap/02-prerequisite-graph.md).
3. Chọn lộ trình và lịch học: [05-learning-order-and-schedule.md](00-roadmap/05-learning-order-and-schedule.md).
4. Cài đặt môi trường theo [Chương 2](01-scala3-core/ch02-environment-setup.md), rồi học từ [Chương 1](01-scala3-core/ch01-introduction.md).

## Stack và phiên bản

| Thành phần | Phiên bản | Dùng cho |
|---|---|---|
| JDK | 21 (LTS) | Toàn bộ |
| Scala | **3.9.0 LTS** | Core, Backend, Data processing, Kafka |
| Scala | **2.13.x** | **Chỉ** các job Spark (Spark 4.x chưa hỗ trợ Scala 3) |
| Scala CLI | đi kèm lệnh `scala` | Giai đoạn 1 |
| sbt | 1.12.11 | Từ Giai đoạn 2 |
| munit | 1.2.4 | Testing |
| Backend | Cats Effect 3.7 · http4s 0.23 · Tapir 1.13 · Doobie 1.0.0-RC13 · PostgreSQL | Giai đoạn 3 |
| Data | Spark 4.1.x · Kafka 4.2.x · fs2-kafka | Giai đoạn 4–7 |

Lý do lựa chọn và nguồn kiểm chứng: [00-roadmap/04-compatibility-matrix.md](00-roadmap/04-compatibility-matrix.md),
[00-roadmap/03-technology-stack.md](00-roadmap/03-technology-stack.md).

## Cấu trúc thư mục

```
scala-engineering/
├── README.md                      ← bạn đang ở đây
├── PROGRESS.md                    ← checklist tiến độ theo phase
├── AGENTS.md                      ← quy ước làm việc của team biên soạn (Claude Code + Codex)
├── .team/                         ← kế hoạch, brief, handoff, review chéo của từng chương (đọc để biết vì sao)
├── 00-roadmap/                    ✅ curriculum, prerequisite, stack, compatibility, lịch học, project, template
├── 01-scala3-core/                ✅ Ch1–6 (+ code/ chạy được cho Ch2–8)
├── 02-functional-programming/     ✅ Ch7
├── 03-type-system/                ✅ Ch8A, Ch8B, Ch8C chuyên sâu (inline, match types, derivation, macro) + ch08c-lab
├── 04-professional-engineering/   ✅ Ch9 (order-platform), Ch11 (jvm-lab)
├── 05-backend/                    ✅ Ch12 http-lab · Ch13 framework-lab · Ch14 order-service · Ch17 security-lab · Ch18
├── 06-database/                   ✅ Ch15 db-lab (PostgreSQL 18.6 + Flyway + Doobie)
├── 07-concurrency/                ✅ Ch16A concurrency-lab · Ch16B effects-lab (Cats Effect)
├── 08-data-processing/            ✅ Ch19 ingest-lab · Ch20 transform-lab · Ch21 formats-lab
├── 09-spark/                      Scala 2.13 · ✅ Ch22–26 (Ch26: spark-submit thật trên WSL2)
├── 10-streaming/                  ✅ Ch27 streaming-lab · Ch28 kafka-lab · Ch29 streaming-pipeline-lab · Ch30 reliability-lab
├── 11-data-platform/              ✅ Ch31 architecture-lab (+ Dagster) · Ch32 distributed-lab (Spark, Scala 2.13) · Ch33 quality-lab
├── 12-testing/                    ✅ Ch10 (code nằm trong order-platform)
├── 13-performance/                ✅ Ch34 cost-lab (đo latency/throughput/cost, mô hình sizing)
├── 14-projects/                   ✅ P1–P5
├── solutions/                     lời giải mọi chương (code + test + giải thích)
├── exercises/                     chỉ mục bài tập theo chương
├── glossary/                      thuật ngữ Anh–Việt
└── libraries/                     tra cứu thư viện/công cụ theo phase: vai trò, phiên bản, Scala 3, ví dụ, thay thế, lỗi thường gặp
```

## Chạy code

```bash
# Ví dụ bài học
cd 01-scala3-core/code
scala-cli run . --main-class scalaeng.ch03.ch03Demo
scala-cli test .
# hoặc: sbt test   |   sbt "runMain scalaeng.ch04.ch04Demo"

# Lời giải bài tập
cd solutions/code
scala-cli test .
```

Danh sách main class hiện có: `scalaeng.ch02.hello`, `scalaeng.ch03.ch03Demo`, `scalaeng.ch03.ch03Types`, `scalaeng.ch04.ch04Demo`,
`scalaeng.ch05.ch05Demo`, `scalaeng.ch06.ch06Demo`, `scalaeng.ch07.ch07Demo`, `scalaeng.ch07.ch07Purity`,
`scalaeng.ch08a.ch08aDemo`, `scalaeng.ch08b.ch08bDemo`.

Nếu chữ tiếng Việt hiển thị thành `?`, xem mục "Lỗi thường gặp" của Chương 2.

## Trạng thái kiểm chứng (đọc kỹ)

| Hạng mục | Trạng thái |
|---|---|
| Code chính Ch2–12, Ch16A, lời giải và Project 1 | ✅ Biên dịch bằng `scalac 3.9.0` (bản phân phối chính thức) + OpenJDK 21.0.10, bật `-deprecation -feature -Wunused:all`, **không có cảnh báo**; các chương trình `@main` đã chạy và output trong bài là output thật |
| Test | ✅ **Chạy lại với thư viện thật** ngày 16/10/2026 (Windows 11, JDK 21, scala-cli 1.9.1, sbt 1.12.11, munit 1.2.4): toàn bộ test Ch3–12, Ch16A, lời giải và P1 pass. Lần chạy thật đầu tiên phát hiện 1 lỗi (`JsonLogger` ghi `
` trên Windows) — đã sửa |
| `build.sbt` (`order-platform` 4 module, P1 2 module) | ✅ `sbt test` chạy thật. Plugin scalafmt và GitHub Actions: ⚠️ chưa chạy |
| Lệnh cài đặt (Coursier, SDKMAN, Homebrew, winget) | ⚠️ Lấy từ tài liệu chính thức, chưa chạy trong môi trường biên soạn |
| Thông báo lỗi compiler trích trong bài | ✅ Output thật của `scalac 3.9.0` |
| Ch12–Ch34, P2–P5 (team Claude Code + Codex) | Mỗi chương có tác giả + reviewer là agent còn lại; ✅ chỉ khi qua review chéo **và** `sbt test` thật do lead chạy. Test cần PostgreSQL/Kafka chạy với bản **nhúng** (`RUN_DB_TESTS=1`, `RUN_KAFKA_IT=1`), không cần Docker. Spark chạy local mode; `spark-submit` thật trên WSL2. Trạng thái từng chương: [PROGRESS.md](PROGRESS.md) |
| Docker / Docker Compose, MinIO, cluster nhiều máy | ⚠️ Chưa kiểm chứng (daemon Docker không chạy trên máy biên soạn) |

Nếu bạn gặp lỗi khi chạy, hãy ghi lại: hệ điều hành, `java -version`, lệnh đã chạy, toàn bộ output.

## Nguyên tắc biên soạn

- Ưu tiên tài liệu chính thức; mọi khẳng định về phiên bản có nguồn và ngày kiểm chứng.
- Không giả định thư viện hỗ trợ Scala 3 chỉ vì hỗ trợ Scala 2.
- Không trình bày thư viện ngoài như một phần của ngôn ngữ.
- Code ưu tiên dễ đọc; mỗi ví dụ quan trọng có test.
- Mỗi bài theo template 17 mục: [00-roadmap/08-lesson-template.md](00-roadmap/08-lesson-template.md).
