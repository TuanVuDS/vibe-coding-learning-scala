# Review T01 — Chương 16B Cats Effect (reviewer: Claude Code)

## Kiểm chứng của reviewer (16/10/2026, ngoài sandbox)
- `sbt.bat -batch test` trong `07-concurrency/effects-lab`: **Passed: Total 22, Failed 0** (sbt 1.12.11) → build.sbt đã kiểm chứng.
- `scala-cli test solutions/effects-code`: 12/12 pass.
- Kỹ thuật: chính xác, mọi khẳng định quan trọng có test; phát hiện munit-cats-effect 2.2.1 → munit 1.3.6 rất tốt (lead áp dụng cho cả repo).

## BLOCKER
(không có)

## MAJOR
1. **Thiếu code minh họa trong phần lý thuyết** (`ch16b-cats-effect.md` mục 3.2–3.11). Người học chưa từng thấy Cats Effect sẽ không biết
   `Resource.make(...)(...).use(...)`, `fiber <- action.start; outcome <- fiber.join`, `(a, b, c).parMapN(...)`, `timeoutTo`, `Ref.update`,
   `Deferred.get/complete`, `semaphore.permit.use`, test `CatsEffectSuite` + `TestControl.executeEmbed` trông như thế nào. Mỗi mục 3.x cần
   ít nhất một đoạn code ngắn (5–15 dòng) lấy/trích từ lab (đã biên dịch), có chú thích từng dòng quan trọng.
2. **Văn phong quá cô đặc cho người mới** — nhiều câu dạng ghi chú (vd. 3.7 dòng 87, 3.8 dòng 95–97, 3.9 dòng 109–111, mục 8). Viết lại thành
   đoạn giải thích đầy đủ chủ ngữ–vị ngữ, mỗi ý một câu; giữ nguyên nội dung kỹ thuật. Độ dài bài có thể tăng lên ~40–45 KB, chấp nhận được.
3. **Mục 10 chỉ có exception runtime của demo.** Bổ sung ≥3 lỗi BIÊN DỊCH thật hay gặp, chạy trên bản sao trong `.cache/` hoặc thư mục tạm:
   (a) dùng `parMapN`/`parTraverse` thiếu `import cats.syntax.all.*`; (b) gọi `unsafeRunSync()` thiếu `cats.effect.unsafe.implicits.global`;
   (c) bỏ quên một `IO` (giá trị bị loại) với `-Wvalue-discard` hoặc `-Wnonunit-statement` — kiểm tra cờ nào thực sự cảnh báo trong Scala 3.9.0.
   Trích nguyên văn thông báo.
4. **Mục 7 hướng dẫn người học đặt `COURSIER_CACHE`** — đó là ràng buộc của sandbox Codex, không phải của người học. Chuyển thành ghi chú nhỏ
   ("nếu môi trường không ghi được cache mặc định...") hoặc bỏ. Lệnh chạy chính nên là `cd 07-concurrency/effects-lab && scala-cli test .`
   và `sbt test` (đã pass).

## MINOR
5. Dòng 1 (khối phiên bản) đặt TRƯỚC tiêu đề `#`, khác các chương khác. Đưa tiêu đề lên dòng 1, khối `> **Phiên bản:** ... **Code:** ... **Trạng thái kiểm chứng:**` ngay sau,
   cập nhật trạng thái: "22/22 (sbt + Scala CLI) + 12/12 lời giải, munit thật; đã review chéo".
6. Thêm mục "Vị trí trong lộ trình" như Ch12/Ch13 (sau 16A, trước Ch13; Ch13–15 dùng `IO`, `Resource`, `Ref`).
7. Mục 4 thuật ngữ thiếu: `IO`, `Resource`, `Fiber`, `Outcome`, `Ref`, `Deferred`, `Semaphore`, `parMapN`, `TestControl`, `IOApp`.
8. Dòng 187: "build.sbt chưa chạy qua do sandbox" → đã chạy (22/22) bởi reviewer; sửa.
9. Mục 16 nên thêm link tài liệu chính thức dạng người đọc: https://typelevel.org/cats-effect/docs/ (IO, Resource, Concurrency, Testing).

## Phạm vi sửa được phép
Như brief T01. Viết lại `.team/handoffs/T01-ch16b-cats-effect.md` mục "Vòng 2" liệt kê từng mục đã sửa.
