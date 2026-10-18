# Brief T01 — Chương 16B: Effect system với Cats Effect 3

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 16 + PHẦN 11–12), `00-roadmap/08-lesson-template.md`,
  `00-roadmap/04-compatibility-matrix.md`, `07-concurrency/ch16a-concurrency-foundations.md` (đặc biệt mục 3.11
  "Vì sao cần effect system"), code `07-concurrency/concurrency-lab/`.

## Phạm vi được phép ghi

- `07-concurrency/ch16b-cats-effect.md` (bài học, ~20–30 KB, đủ 17 mục template)
- `07-concurrency/effects-lab/` (lab mới: `project.scala`, `build.sbt`, `project/build.properties`, `src/main`, `src/test`)
- `solutions/ch16b-solutions.md` và `solutions/effects-code/` (lời giải code + test)
- `.team/handoffs/T01-ch16b-cats-effect.md`

## Nội dung bắt buộc

1. Kiểm chứng phiên bản Cats Effect 3.7.x mới nhất có artifact `_3` (Maven Central / GitHub releases), munit-cats-effect
   tương thích. Ghi link + ngày 16/10/2026 trong bài và handoff.
2. Mỗi giới hạn của `Future` đã đo ở Ch16A (eager, không hủy được, timeout không dừng công việc, retry phải tự viết,
   quản lý tài nguyên, starvation khi chặn) → cách Cats Effect giải quyết, có **thí nghiệm đối chiếu chạy thật**.
3. Chủ đề: `IO` (lazy, referential transparency), `IOApp`, `flatMap`/for-comprehension, `IO.blocking` vs `IO.delay`
   vs `IO.interruptible`, error handling (`attempt`, `handleErrorWith`, `raiseError`), `Resource` (acquire/release, thứ tự
   giải phóng), `Fiber`/`start`/`join`, `parTraverse`/`parMapN`, cancellation + `onCancel` + `uncancelable` (mức phù hợp),
   `timeout`, `Ref`, `Deferred` (giới thiệu), `Queue` hoặc `Semaphore` (một cái), retry với backoff tự viết bằng IO,
   testing với `munit-cats-effect` và `TestControl` nếu phiên bản hỗ trợ (kiểm chứng).
4. Phân biệt rõ: đâu là tính năng ngôn ngữ, đâu là thư viện (Cats Effect là thư viện ngoài).
5. Mục so sánh ngắn với ZIO 2 (chỉ khái niệm; kiểm chứng phiên bản ZIO 2.x có Scala 3, ghi nguồn; không viết lab ZIO).
6. Ví dụ thực tế nối tiếp domain: gọi song song 3 "dịch vụ" (giá, tồn kho, khuyến mãi) với timeout + fallback; pool kết nối giả
   bằng `Resource`; bộ đếm request an toàn bằng `Ref`.
7. Bài tập cơ bản → nâng cao (≥6), lời giải đầy đủ có test; câu hỏi kiểm tra; mini project; checklist.

## Tiêu chí xong

- `scala-cli.bat test 07-concurrency/effects-lab --server=false` và `... solutions/effects-code --server=false` pass, 0 cảnh báo.
- Mọi output trong mục 9 và lỗi trong mục 10 là output thật (dán từ lần chạy).
- Có `.team/handoffs/T01-ch16b-cats-effect.md` theo mẫu AGENTS.md mục 2.3.
- Không sửa file dùng chung (README, PROGRESS, roadmap, glossary) — đề xuất trong handoff.
