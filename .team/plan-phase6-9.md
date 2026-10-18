# Kế hoạch hoàn thiện Phase 5 (phần còn lại) → Phase 9

Lập ngày 17/10/2026 bởi Claude Code (lead). Nguồn: `PROMPT_PROJECT.md` PHẦN 8–10, 14; `00-roadmap/01-curriculum.md`, `06-projects-overview.md`.

## 1. Rà soát: phần nào chưa có trong kế hoạch

| Hạng mục | Trước rà soát | Sau rà soát |
|---|---|---|
| Review Ch24, Ch26, P3 | R-T14 dở (hạn mức), Ch26/P3 chưa giao | **R-T14/T16/P3** giao Codex (một lượt, `R-batch3`) |
| Ch29–30 + P4 | Brief T15 có, chưa chạy | Giữ T15 (Codex), chạy lại |
| Ch31 Data Pipeline Architecture | Chỉ có dòng trong curriculum | **T18** — Claude |
| Ch32 Advanced Distributed Processing | Chỉ có dòng trong curriculum | **T19** — Codex |
| Ch33 Data Quality & Testing | `11-data-platform/quality-lab` dựng dở, chưa brief | **T17** — Claude (tiếp tục lab) |
| Ch34 Performance & Cost | Chỉ có dòng trong curriculum, thư mục `13-performance/` chưa có | **T20** — Codex (chuyển từ Claude 17/10 để cân bằng: Claude nhận thêm sửa 17 MAJOR của Ch24/Ch26/P3) |
| Project 5 | Có thiết kế trong `06-projects-overview.md` | **T21** — Claude tác giả, Codex review |
| Ch8C (match types, inline, macros) | "để sau Ch16", chưa giao | **T22** — Codex |
| Phase 9 kiểm tra chất lượng | Chỉ có một dòng ⏳ | **T23** — chia đôi: Codex kiểm tra tự động (link, 17 mục, đầu chương, build lại toàn bộ lab bằng Scala CLI); Claude sbt toàn bộ + README/glossary/exercises/PROGRESS |

## 2. Phân công (cân bằng khối lượng)

| Codex | Claude Code |
|---|---|
| R-batch3: review Ch24, Ch26, P3 | T17 Ch33 Data Quality & Testing |
| T15: Ch29, Ch30, P4 | T18 Ch31 Data Pipeline Architecture |
| T19: Ch32 Advanced Distributed Processing (Spark 2.13) | Sửa Ch24 (4 MAJOR), Ch26 (7 MAJOR), P3 (6 MAJOR) |
| T22: Ch8C Advanced Types (Scala 3.9) + T20 Ch34 | T21 Project 5 |
| Review: Ch31, Ch33, Ch34, P5 | Review: Ch29, Ch30, P4, Ch32, Ch8C; `sbt test` cho mọi task |
| T23a: rà soát tự động toàn giáo trình | T23b: file dùng chung, commit, PROGRESS |

Mỗi bên ~4 nội dung lớn + review của bên kia. Hai tiến trình Codex chạy song song khi không đụng thư mục nhau.

## 3. Thứ tự và phụ thuộc

```
Đợt 1 (song song):  Codex#1 T15 (10-streaming, 14-projects/p4)   Codex#2 R-batch3 (chỉ ghi .team/reviews)
                    Claude T17 Ch33 → T18 Ch31
Đợt 2:              Codex#2 T19 Ch32 (sau R-batch3)               Claude sửa Ch24/26/P3 theo review; review T15
Đợt 3:              Codex T22 Ch8C; Codex review Ch31+Ch33         Claude T20 Ch34 → T21 P5 (cần P2, P3, P4)
Đợt 4:              Codex review Ch34 + P5; T23a                   Claude T23b, commit cuối
```

Quy tắc: thư mục ghi của hai bên không giao nhau; Codex không commit; lead chạy `sbt test` (và `RUN_DB_TESTS=1`, `RUN_KAFKA_IT=1`) trước khi ✅.
Khi Codex hết hạn mức: log kết thúc bằng "usage limit … try again at <giờ>", chạy lại cùng prompt sau giờ đó.

## 4. Vị trí file

- `11-data-platform/`: ch31, ch32 (+ `distributed-lab/`, Scala 2.13), ch33 (+ `quality-lab/`)
- `13-performance/`: ch34 (+ `cost-lab/`) — theo `01-curriculum.md` mục 4
- `03-type-system/`: ch08c (+ lab)
- `14-projects/p4-kafka-event-processing/`, `14-projects/p5-data-platform/`
