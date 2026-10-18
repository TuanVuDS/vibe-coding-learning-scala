# Review T22 — Chương 8C Lập trình kiểu nâng cao (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **ĐẠT — 0 BLOCKER, 0 MAJOR, 4 MINOR** (sửa MINOR rồi lead đánh dấu ✅, không cần vòng 2).

Điểm mạnh: mở đầu nói rõ khi nào **không** cần chương này; mọi khẳng định về compile/runtime đều có test hoặc ca biên dịch âm (6/6 fixture, chạy compiler
thật, kiểm exit code + diagnostic); `twice` vs `twiceInline` chứng minh ngữ nghĩa đánh giá tham số bằng số lần gọi; match type kẹt có thông báo thật và giải
thích disjointness chính xác; derivation tách `CellEncoder` (một ô) khỏi `CsvEncoder` (một dòng) — đúng thiết kế; ranh giới cast được gom và giải thích; không
tuyên bố hiệu năng chưa đo; circe chỉ được dẫn ở mức "đã kiểm tra artifact + source", không thêm dependency.

## Kiểm chứng của reviewer (17/10/2026, Windows 11, JDK 21)

| Lệnh | Kết quả |
|---|---|
| `scala-cli.bat test 03-type-system/ch08c-lab --server=false` | `AdvancedTypesSuite` 12/12, không cảnh báo |
| `scala-cli.bat test solutions/ch08c-code --server=false` | `ExercisesSuite` 13/13 |
| Probe 1 — `case class Tagged(orderId: String, tags: List[String]) derives CsvEncoder` | lỗi biên dịch thật, xem m2 |
| Probe 2 — `CsvWriter.write` với giá trị `=HYPERLINK(...)`, `+1+1` | ghi nguyên văn, xem m3 |

## MINOR

- **m1 — Định dạng đầu chương.** Dòng 1 là blockquote phiên bản, tiêu đề `# Chương 8C` ở dòng 3. Các chương khác của giáo trình: tiêu đề trước, blockquote
  phiên bản/trạng thái sau. Đổi thứ tự cho đồng bộ (bỏ "Chờ Claude Code review chéo" khỏi dòng trạng thái khi sửa xong).
- **m2 — Thiếu ca lỗi phổ biến nhất của derivation: trường không có instance.** Người học gặp lỗi này đầu tiên khi tự `derives` cho DTO của mình. Output thật:

  ```
  -- [E172] Type Error: ...N.scala:3:81
   3 |case class Tagged(orderId: String, tags: List[String]) derives vn.scalaeng.ch08c.CsvEncoder
     |                                                                                 ^
     |No given instance of type vn.scalaeng.ch08c.CellEncoder[List[String]] was found
     |----------------------------------------------------------------------------
     |Inline stack trace
     |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
     |This location contains code that was inlined from CsvEncoder.scala:44
  44 |        writer(summonInline[CellEncoder[head]]) :: cellWriters[tail]
  ```

  Thêm vào mục 10 (fixture thứ 7 trong `negative/` + `verify-negative.ps1`) và giải thích cách đọc *inline stack trace*: dòng đầu là nguyên nhân, phần dưới
  chỉ nơi `summonInline` được mở rộng. Ba cách sửa: viết `CellEncoder[List[String]]` có chủ đích (định dạng gì cho danh sách trong một ô?), đổi DTO, hoặc
  viết encoder tay.
- **m3 — CSV injection.** Mục 6 dùng `OrderReport` cho "báo cáo đơn hàng của API nội bộ (BE) và file giao cho downstream". Probe: `CsvWriter` ghi nguyên
  `"=HYPERLINK(""http://x"",""bấm"")"` và `+1+1` — mở bằng Excel/Sheets thì thành công thức. Không phải lỗi của lab (escape theo RFC 4180 đúng), nhưng nên có
  một câu ở mục 6 hoặc 3.5: CSV cho **người** mở bằng bảng tính cần chính sách riêng cho ô bắt đầu bằng `=`, `+`, `-`, `@` (OWASP "CSV Injection"); CSV cho
  **máy** (downstream DE) thì không được sửa dữ liệu. Có thể thành một bài tập mở rộng.
- **m4 — Liên kết lộ trình.** Không có câu nối Ch8C với nơi người học sẽ gặp lại kỹ thuật (circe trong Ch13/Ch14, Tapir schema derivation ở P2). Thêm 1–2 câu
  ở mục 2 hoặc cuối mục 3.8 để người học chuyên sâu biết đọc tiếp ở đâu. (Lead sẽ cập nhật curriculum/README để trỏ tới Ch8C.)

## Đã kiểm và đạt

- 17 mục đủ nội dung; 6 bài tập + mini project có lời giải và 13 test.
- Khẳng định `Payload[List[Option[Int]]] = Option[Int]` (bóc một lớp), `Tuple1`, `erasedValue` không tạo giá trị runtime, `summonInline` không dò classpath — đúng.
- Policy tên topic (1–249 ký tự `[a-zA-Z0-9._-]`, cấm `.`/`..`) khớp giới hạn tên topic của Kafka; bài ghi rõ không thay kiểm tra broker.
- Giới hạn "gọi macro trong cùng file định nghĩa" có fixture và thông báo thật.

## Vòng 2 — Claude Code, 17/10/2026: **ĐẠT** (giữ nguyên kết luận vòng 1)

- m1–m4 đã sửa (tiêu đề, mục 10.6 với diagnostic thật thiếu `CellEncoder[List[String]]`, CSV injection, nối Ch13/Ch14/P2).
- **Kiểm chứng của lead:** `scala-cli test 03-type-system/ch08c-lab --server=false` → `12 total, 0 failed`; `solutions/ch08c-code` → `13 total, 0 failed`; `verify-negative.ps1` → `Negative compilation: 7/7 expected failures verified.` Lab không có `build.sbt` (một module, Scala CLI đủ theo AGENTS.md §4).
