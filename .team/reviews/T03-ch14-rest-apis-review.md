# Review T03 — Chương 14 REST APIs (reviewer: Claude Code)

## Kiểm chứng của reviewer (17/10/2026, ngoài sandbox)
- `sbt.bat -batch test` trong `05-backend/order-service`: domain **Passed 5**, app **Passed 20**, `[success]` → build.sbt 2 module đã kiểm chứng.
- Kỹ thuật: thiết kế tốt — trait repository độc lập HTTP/JSON/DB; mọi chuyển trạng thái trong một `Ref.modify`; Problem Details RFC 9457
  có `application/problem+json`; 415 qua `oneOfBody`; 405 + `Allow`; contract test trên HttpApp thật + socket test cổng 0. Phát hiện
  `jsonBody` không tự kiểm tra Content-Type (mục 10.6) là bài học tốt. Ghi chú nối Ch15 (adapter `ConnectionIO` → `transact`, không ghép
  nhiều method đã transact rồi gọi là một transaction) chính xác.

## BLOCKER
(không có)

## MAJOR
1. **Gần như không có code trong bài** — 48 KB nhưng chỉ 1 khối ```scala (mục 5, 2 dòng). Người học không thấy endpoint Tapir được viết thế nào.
   Bổ sung đoạn code ngắn (5–20 dòng, trích nguyên từ lab đã biên dịch, có chú thích) ít nhất cho:
   - 3.1: một endpoint đầy đủ (`createUser`: path, `oneOfBody(jsonBody[...])`, `errorOut(oneOf(...))`, `out(statusCode).out(header Location)`), và `serverLogic`.
   - 3.2: `Validation.user` (tích lũy lỗi) và cách query param `limit/offset` được khai báo.
   - 3.3: opaque `UserId`/`Money` + factory; `TransportCodecs` (codec enum chuỗi) và Schema enum.
   - 3.4: trait `OrderRepository` (đã có ở file — trích vào bài) + `OrderService.run` helper.
   - 3.5: `updateStatus` bằng một `state.modify`, đặt cạnh phiên bản SAI `get` rồi `set` để người học thấy khác biệt.
   - 3.6: `problemBody`, `decodeFailures`, exception handler, reject handler 405 — mỗi cái vài dòng.
   - 3.7: sort whitelist (`UserSort`) + `pageOf`.
   - 3.9: một test contract (gọi HttpApp như hàm) và khung `SocketSuite` (Resource server + client, cổng 0).
2. **Văn phong quá cô đặc cho người mới** ở 3.1–3.9 và mục 8: nhiều câu nén nhiều ý (vd. 3.3 dòng 74, 3.6 dòng 140, 3.7 dòng 159). Tách thành đoạn
   ngắn, mỗi ý một câu, giải thích "vì sao" trước "làm thế nào". Có thể tăng độ dài bài; giữ nguyên mọi khẳng định kỹ thuật đã kiểm chứng.

## MINOR
3. Dòng 1: khối phiên bản đặt TRƯỚC tiêu đề `#` — đưa tiêu đề lên đầu như Ch13/15/17; cập nhật trạng thái: sbt 25/25 do reviewer chạy, đã review chéo.
4. Thêm mục "Vị trí trong lộ trình" ngắn (sau Ch13/16B, trước Ch15/17/18; Project 2 ghép Ch14 + Ch15 + Ch17).
5. Mục 7: lệnh chạy chính cho người học nên là `cd 05-backend/order-service && sbt test` / `scala-cli test .`; ghi `COURSIER_CACHE`/`--server=false`
   là ghi chú cho môi trường hạn chế (như đã làm ở Ch16B vòng 2), không phải bước bắt buộc.

## Phạm vi sửa
Như brief T03. Ghi mục "Vòng 2" trong handoff liệt kê từng điểm đã sửa; chạy lại test Scala CLI.
