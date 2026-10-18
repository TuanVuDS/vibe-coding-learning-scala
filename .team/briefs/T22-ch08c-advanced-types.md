# Brief T22 — Chương 8C Lập trình kiểu nâng cao (chuyên sâu, học sau Ch16)

- **Tác giả:** Codex · **Reviewer:** Claude Code
- **Đọc trước:** `AGENTS.md`, `PROMPT_PROJECT.md` (Chương 8 + template 17 mục), `00-roadmap/01-curriculum.md` (Ch8C đánh dấu [CS] — chuyên sâu),
  `03-type-system/` (Ch8A, Ch8B và lab của chúng — giữ văn phong, nối tiếp domain đơn hàng/người dùng/sự kiện), `solutions/ch08-solutions.md`.

## Phạm vi được phép ghi
- `03-type-system/ch08c-advanced-type-programming.md`, `03-type-system/ch08c-lab/` (Scala 3.9.0, JDK 21, munit 1.2.4, `project.scala`)
- `solutions/ch08c-solutions.md`, `solutions/ch08c-code/`, `.team/handoffs/T22-ch08c.md`, `.cache/`

## Nội dung bắt buộc
Mở đầu bằng **khi nào KHÔNG cần** các kỹ thuật này (người học BE/DE có thể bỏ qua chương mà không hổng kiến thức sau).
- `inline` (inline def/val/if/match, `inline` parameter, `compiletime.error`, `constValue`, `erasedValue`, `summonInline`): khi nào giúp hiệu năng/kiểm tra lúc biên dịch, giá trị thật của việc inline (code size).
- Match types (định nghĩa, reduction, giới hạn: không reduce được khi kiểu chưa đủ cụ thể — trích lỗi thật), tuple types (`Tuple.Map`, `*:`).
- Typeclass derivation với `Mirror` (`derives` một typeclass tự viết, ví dụ `CsvEncoder`/`Show` cho case class đơn hàng) — nối Ch8B.
- Macros ở mức giới thiệu (quotes/splices `'{}`/`${}`, một macro nhỏ có ích như kiểm tra literal lúc biên dịch), hạn chế (phải tách compilation unit, độ phức tạp bảo trì, cross-version).
- So sánh: inline vs macro vs runtime reflection vs code generation; chi phí cho đồng đội đọc code.
- Thư viện thực tế dùng các kỹ thuật này (circe/derivation, Tapir, Iron/refined-style — chỉ nhắc nếu kiểm chứng được phiên bản Scala 3; không bắt buộc dùng).

Mọi lỗi trong "Lỗi thường gặp" phải là thông báo thật của scalac 3.9.0 (ví dụ match type không reduce, `compiletime.error` tự viết, macro dùng trong cùng file định nghĩa).

## Tiêu chí xong
- Đủ 17 mục; output thật; ≥6 bài tập có lời giải + test.
- `scala-cli.bat test` pass `--server=false`, 0 cảnh báo với `-deprecation -feature -Wunused:all`.
- Handoff đầy đủ (lệnh, output, phiên bản + nguồn + ngày, phần chưa chạy, đề xuất glossary).
