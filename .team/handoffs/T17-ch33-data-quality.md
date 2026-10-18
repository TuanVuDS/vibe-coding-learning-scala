# Handoff T17 — Chương 33 Data Quality và Testing (tác giả: Claude Code, reviewer: Codex)

## File đã tạo
- `11-data-platform/ch33-data-quality-testing.md` (17 mục)
- `11-data-platform/quality-lab/` — `project.scala`, `build.sbt`, `project/build.properties` (sbt 1.12.11), `contracts/orders.v1.json`, `orders.v2.json`,
  `data/customers.csv`, `data/input/ingest_date=2026-10-15|16/orders.csv`, `golden/daily_revenue.csv`, `rejected.csv`, `conflicts.csv`,
  `src/main/scala/vn/scalaeng/quality/{Pipeline,Checks,Contract,Drift,Files,QualityDemo}.scala`, `src/test/scala/vn/scalaeng/quality/{QualitySuite,PropertySuite,Golden}.scala`
- `solutions/ch33-solutions.md`, `solutions/ch33-code/` (project.scala, `Solutions.scala`, `SolutionsSuite.scala`)

## Lệnh đã chạy (17/10/2026, Windows 11, JDK 21)
- `scala-cli.bat test 11-data-platform/quality-lab --server=false` → PropertySuite 6/6, QualitySuite 10/10.
- `sbt.bat test` trong `quality-lab` → `Passed: Total 16, Failed 0`, không cảnh báo biên dịch.
- `scala-cli.bat run . --main-class vn.scalaeng.quality.QualityDemo --java-opt -Dstdout.encoding=UTF-8` → output trích nguyên văn ở mục 9.
- `scala-cli.bat test solutions/ch33-code --server=false` → 7/7.
- Golden: tính tay (bảng mục 6) **trước** khi chạy; lần chạy đầu sau khi ghi golden: xanh.
- Probe lỗi (bản sao trong scratchpad, không để lại trong repo): (1) `sortKey = (orderId, sku)` → property "đến trễ" đỏ, seed + phản ví dụ trích mục 10;
  (2) bỏ nhánh `updatedAt == t` trong `Incremental.step` → property backfill đỏ; (3) golden diff của "tối ưu" bỏ dedup phiên bản (mục 9).

## Phiên bản (tra repo1.maven.org ngày 17/10/2026)
Scala 3.9.0; circe-parser 0.14.17; munit 1.2.4; munit-scalacheck 1.2.0 (kéo munit 1.1.1 + scalacheck 1.19.0; munit 1.2.4 khai báo trực tiếp thắng — giống Ch20;
1.3.x có nhưng không dùng để giữ chuẩn munit 1.2.4).

## Đề xuất cập nhật file dùng chung
- PROGRESS: Ch33 viết xong, 16 test (sbt + CLI) + 7 lời giải, chờ review.
- Glossary: data contract, referential integrity, freshness, golden dataset, drift/profile, reconciliation, breaking/compatible change, shrinking.

## Chưa kiểm chứng
- Lỗi 6 (golden đỏ do `\r\n`) là phòng ngừa — ghi rõ trong bài là chưa tái hiện.
- Deequ / Great Expectations / Soda / dbt chỉ nhắc khái niệm, không chạy, không chốt phiên bản.

## Điểm reviewer nên soi
- Quy tắc compatibility (đặc biệt "thêm enum = Breaking", "int → long = Compatible") và cách bài nói về hướng reader/writer.
- `Incremental.step` có thật sự tương đương `Pipeline.dedup` trong mọi trường hợp (ba bản cùng `updated_at`, hai bản trùng + một xung đột, lô sớm hơn đến sau)?
- Property "đến trễ" và "trùng ở lô sau" có quá yếu không (bộ sinh), `served` có loại quá nhiều không.
- Drift: chia cho 0, segment mới xuất hiện/biến mất.
- `DataFiles.records`: dòng thiếu cột, BOM, giá trị có dấu phẩy (bài nói rõ không hỗ trợ).

## Vòng 2 (17/10/2026) — sửa theo `.team/reviews/T17-ch33-review.md`
- **M1/M2:** tách `Contract.validate` (producer conformance, nghiêm) và `Contract.read` (reader projection, bỏ qua trường lạ); `compatibility` = FORWARD
  (reader cũ ← writer mới): int → long và bắt buộc → nullable là Breaking, long → int, thêm trường, bỏ trường nullable là Compatible. Test mới
  `CAM KẾT của compatibility` sinh bản ghi biên cho 9 cặp thay đổi: Compatible ⇔ `read(old)` đọc được mọi bản ghi biên hợp lệ của `next`. Bài mục 3.4 viết lại
  (bảng FORWARD + đoạn BACKWARD đảo chiều), demo/mục 9 cập nhật (`quantity: int → long` giờ Breaking).
- **M3/m2:** `DataFiles.records` bỏ BOM, đưa giá trị thừa vào `Contract.ExtraColumns`; `RawOrder.extra` → pipeline từ chối với lý do; test BOM+CRLF+thừa cột.
  Lỗi biên dịch thật khi thêm trường thứ 10 vào khóa sắp xếp (`Ordering` chỉ tới Tuple9) → mục 10 Lỗi 7.
- **m1:** `Incremental.conflicts` + property backfill so cả conflicts, `Prop.collect` phủ ca có/không xung đột; bỏ chữ "mọi" trong bài, thêm đoạn "property không phải chứng minh".
- **m3:** mini project nêu `maxLagDays = 1` và phân biệt freshness/completeness. Thêm: Golden chuẩn hóa CRLF cả hai phía; ghi chú baseline 0 và `Profile.rows`.
- Kiểm chứng: `scala-cli test` 18/18 (PropertySuite 6, QualitySuite 12), `sbt test` `Passed: Total 18`, lời giải 7/7, demo chạy lại.

## Vòng 2b — phản hồi review vòng 2 (M3 còn hở: cột thừa rỗng), 17/10/2026

- `RawOrder.extra: Option[String]`; `DataFiles.toRaw` truyền `r.get(Contract.ExtraColumns)`; `Pipeline.validate` kiểm SỰ HIỆN DIỆN (`Some("")`, `Some("   ")`, `Some(",")` đều bị từ chối). Test thêm ba dòng `…,` / `…,   ` / `…,,`. Chương mục 10 lỗi 6 bổ sung biến thể này; số test mục 9 cập nhật 12 + 6 = 18.
- **Kiểm chứng (lead):** Scala CLI `18 total, 0 failed` (Property 6 + Quality 12), 0 cảnh báo; `sbt test` → `Passed: Total 18`.
