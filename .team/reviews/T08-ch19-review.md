# Review T08 — Chương 19 (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 7 MAJOR, 2 MINOR; không có BLOCKER.**

**Mốc phiên bản:** review vòng 1 trên nội dung ban đầu khớp các file T08 tại commit `b3c00899d5907158c716be8ea76fd4e98ff05860`. Khi kiểm tra cuối sau T10, working tree T08 đã có sửa đổi từ tiến trình khác (Normalize/Model/Aggregate/Ingest và test); các số dòng, phản ví dụ và kết quả 15+6 dưới đây thuộc bản được đọc/chạy ban đầu, **chưa phải review lại các sửa đổi mới**. Không sửa hoặc ghi đè các thay đổi đó.

Đã đọc handoff, toàn bài, toàn bộ main/test/config/data/golden của ingest-lab và code/test/lời giải Ch19. Đủ 17 mục có nội dung, tiếng Việt dễ theo dõi, ví dụ cùng domain và phần golden tính tay tốt. Các lỗi dưới đây nằm ở tính đúng, hợp đồng dữ liệu và lời giải; bộ test hiện tại xanh chưa bao phủ chúng. Không sửa file tác giả.

## Kiểm chứng chạy thật

Đặt `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`; `.cache/` đã được git ignore. Mỗi lệnh dùng `--workspace` trỏ vào `.cache/review-batch/` để output build nằm trong phạm vi cho phép.

- Trong `08-data-processing/ingest-lab`: `scala-cli.bat test . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t08-lab-build` → exit 0; NormalizeSuite **7/7**, PipelineSuite **8/8**, 0 failed, 0 ignored.
- Trong `solutions/ch19-code`: cùng lệnh với workspace `t08-sol-build` → exit 0; Ch19SolutionsSuite **6/6**, 0 failed, 0 ignored.
- `scala-cli.bat run . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t08-main-build --main-class vn.scalaeng.ingest.IngestMain -- data/events.jsonl data/campaigns.csv C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t08-out` → `Summary(17,8,7,2,1,3) trong 1301 ms`. Thời gian khác máy biên soạn là bình thường. Golden test pass; report và 7 rejection khớp nội dung mục 9.
- Probe riêng: `.cache/review-batch/t08-probe/Probe.scala`; chạy `scala-cli.bat run .cache/review-batch/t08-probe --server=false --main-class Probe` → exit 0, tái hiện các trường hợp bên dưới. Probe in hành vi thực tế, không phải regression test đã sửa lỗi.
- Không chạy sbt trong sandbox; không chạy lại thí nghiệm heap 1 triệu dòng hay mutation test lịch sử. Không xác nhận độc lập các số đo đó hoặc nguồn gốc các transcript lỗi compiler cũ. Không có compiler warning trong các lượt compile đã chạy; thông báo dependency outdated của CLI là hint, không phải yêu cầu nâng version.

## MAJOR

### M1 — Thuộc tính đảo thứ tự không đúng khi cùng event_id có nội dung mâu thuẫn

Vị trí: `08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Aggregate.scala:48`; `08-data-processing/ch19-scala-data-processing.md:179`; `08-data-processing/ingest-lab/src/test/scala/vn/scalaeng/ingest/PipelineSuite.scala:53`.

Tái hiện: hai impression hợp lệ cùng `event_id=i1`, cùng CMP-1/U-1, lần lượt có ts `2026-10-15T01:00:00Z` và `2026-10-16T01:00:00Z`. Forward trả `ReportRow(...,2026-10-15,1,0,1,0.00)`, reverse trả ngày **2026-10-16**. Bản thứ hai bị đếm là duplicate dù mâu thuẫn và quyết định giữ bản đầu phụ thuộc thứ tự. Handoff đã nhận biết nhưng bài vẫn dạy thuộc tính không điều kiện. Trường hợp khác campaign/loại sự kiện còn có thể thay đổi liên kết click.

Đề xuất: ghi hợp đồng phân biệt retry tương đương với xung đột ID. Nếu giữ first-wins, giới hạn rõ thuộc tính chỉ áp dụng cho các ID không mâu thuẫn, ghi nhận xung đột và thêm regression test khác ngày/campaign/type. Nếu muốn bất biến với mọi hoán vị, phải có quy tắc chọn/từ chối xung đột xác định và cập nhật aggregate tương ứng; không chỉ mở rộng seed trên mẫu hiện tại.

### M2 — Decoder vẫn fail-fast theo kiểu, làm sai lời hứa báo mọi lỗi

Vị trí: `08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Model.scala:30`; `08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Normalize.scala:20`; `08-data-processing/ch19-scala-data-processing.md:66`.

Input JSON hợp lệ `{"event_id":12,"type":"view","campaign_id":34,"geo":{"country":"BAD"}}` chỉ trả một rejection: `JSON không hợp lệ: DecodingFailure at .event_id: Got value '12' with wrong type, expecting string`. Các lỗi type, ts, user, campaign, country không được gom vì validate không chạy. `Option[String]` chỉ nới missing/null, không nới kiểu số/object. Ngược lại `.orElse(Right(None))` ở các trường lồng nhau nuốt cả sai kiểu, biến dữ liệu sai thành missing/default và có thể làm mất city/os mà không có chẩn đoán.

Đề xuất: raw giữ Json hoặc kết quả đọc có lỗi theo từng field, tích lũy lỗi cấu trúc/kiểu và nghiệp vụ; phân biệt JSON syntax error với decoding error. Chỉ mặc định khi thực sự missing/null theo hợp đồng. Thêm test nhiều field sai kiểu cùng lúc, `user` sai cấu trúc, `device.os`/`geo.city` là number, bên cạnh missing/null. [Circe phân biệt decoder thường với accumulating decoder](https://circe.io/circe/codecs/custom-codecs.html) (tra 17/10/2026).

### M3 — Chuẩn hóa quốc gia không kiểm tra mã ISO như mô tả

Vị trí: `08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Normalize.scala:49`; `08-data-processing/ch19-scala-data-processing.md:196`.

Probe `Normalize.country(Some("ZZ"))` → `Right(ZZ)`; `Some("ÅÅ")` cũng Right. Hai chữ Unicode không đồng nghĩa với mã ISO 3166-1 alpha-2 được cấp; dữ liệu này đi vào dạng được gọi là sạch.

Đề xuất: sau alias và chuẩn hóa với Locale.ROOT, tra tập mã ISO được chấp nhận (chẳng hạn `Locale.getISOCountries`, hoặc bảng nghiệp vụ ghi rõ version), quyết định riêng mã unknown/private-use nếu cần. Thêm test ZZ, Unicode, VN/TH/IN hợp lệ. [JDK Locale.getISOCountries](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Locale.html#getISOCountries()) (tra 17/10/2026).

### M4 — Bài 6: dòng CSV thiếu cột vẫn làm dừng cả file

Vị trí: `solutions/ch19-code/src/main/scala/vn/scalaeng/solutions/ch19/Ch19Solutions.scala:102`; `solutions/ch19-solutions.md:88`.

Probe CSV có header 4 cột, dòng PAID hợp lệ, dòng `O2,2026-10-15,2` (3 cột), rồi một dòng hợp lệ nữa. Kết quả `.attempt` là `Left(fs2.data.csv.HeaderSizeError: Headers have size 4 but row has size 3. Both numbers must match! in line 3)`; không có tổng hoặc bad count. Lỗi phát sinh trong `lowlevel.headers`, trước `CsvRowDecoder`, trái cả comment dòng 99 lẫn yêu cầu bài 6 “dòng hỏng được đếm, không làm dừng”.

Đề xuất: dùng API lenient phù hợp của fs2-data (ví dụ `lenient.attemptDecodeUsingHeaders`) để nhận lỗi số cột từng record; fold Left vào bộ đếm. Ghi rõ lỗi lexical CSV nào không thể phục hồi, không hứa phục hồi mọi lỗi dấu quote. Test thiếu/thừa cột, decode amount sai và record hợp lệ sau record lỗi. [fs2-data: Dealing with erroneous files](https://fs2-data.gnieh.org/documentation/csv/#dealing-with-erroneous-files) mô tả đúng ranh giới này (tra 17/10/2026).

### M5 — Bài 6: tổng tiền tràn Long âm, không báo lỗi

Vị trí: `solutions/ch19-code/src/main/scala/vn/scalaeng/solutions/ch19/Ch19Solutions.scala:107`.

Hai đơn PAID cùng ngày, amount lần lượt `9223372036854775807` và `1`, đều decode được Long. Kết quả chạy thật: `(Map(2026-10-15 -> -9223372036854775808),0)`. Sai doanh thu và không đếm lỗi.

Đề xuất: cộng bằng BigInt/BigDecimal, hoặc `Math.addExact` với chính sách lỗi rõ ràng; nếu giữ giới hạn nghiệp vụ phải validate đầu vào và cả tổng. Thêm regression tại Long.MaxValue và kiểm tra amount âm theo quy tắc đơn hàng. Không thể giải quyết tràn tổng chỉ bằng giới hạn mỗi dòng là Long.

### M6 — Chuẩn hóa phụ thuộc locale mặc định, thay máy có thể thay kết quả

Vị trí: `08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Normalize.scala:46` và `:55`.

Đặt `Locale.setDefault(Locale.forLanguageTag("tr-TR"))`: `"in"` thành `İN` (U+0130), `"IMPRESSION"` hạ chữ thành `ımpressıon` và bị báo type không hỗ trợ. Cùng JSON hợp lệ trên locale thông thường lại pass. Code đã tránh systemDefault cho múi giờ nhưng bỏ sót locale khi chuẩn hóa định danh.

Đề xuất: dùng `toUpperCase(Locale.ROOT)` / `toLowerCase(Locale.ROOT)` cho các mã/type không phụ thuộc ngôn ngữ, áp dụng nhất quán cả bảng campaign. Test locale Thổ Nhĩ Kỳ và khôi phục default trong finally, tránh race với test song song. [JDK String](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/String.html#toUpperCase()) nêu rõ case conversion mặc định phụ thuộc locale (tra 17/10/2026).

### M7 — Bài 1: ngày không tồn tại được tự sửa rồi coi là hợp lệ

Vị trí: `solutions/ch19-code/src/main/scala/vn/scalaeng/solutions/ch19/Ch19Solutions.scala:20`.

Probe `LegacyTimestamp.parse(Some(Json.fromString("2026-02-30 08:00:00")))` → **`Right(2026-02-28T01:00:00Z)`**. `ofPattern` dùng SMART, nên không chỉ thêm hỗ trợ format mà còn âm thầm đổi ngày; pipeline sạch không được báo lỗi dữ liệu gốc.

Đề xuất: `DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT)`; dùng uuuu để strict resolution không đòi era của yyyy. Test 30/2, 31/4, năm nhuận, ngày hợp lệ và định dạng có offset vẫn được ưu tiên. [DateTimeFormatter](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html), [ResolverStyle](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/ResolverStyle.html) (tra 17/10/2026).

## MINOR

### m1 — Cửa sổ thời gian không bảo đảm một trần bộ nhớ cố định

Vị trí: `08-data-processing/ch19-scala-data-processing.md:13` và `:390`; `solutions/ch19-code/src/main/scala/vn/scalaeng/solutions/ch19/Ch19Solutions.scala:51`; `solutions/ch19-solutions.md:57`.

Probe 10.000 ID có cùng ts → `maxSize=10000`; tăng số ID thì state tiếp tục tăng dù window chỉ 10 phút. `run` còn giữ mọi event được giữ lại trong Vector. Con số ≤11 chỉ đúng với dataset một event/phút, không phải giới hạn thuật toán. Phần 3.5 giải thích state khá tốt nhưng câu mở bài “để bộ nhớ không phụ thuộc kích thước file” lại tuyệt đối hóa.

Đề xuất: diễn đạt O(số ID trong cửa sổ), phụ thuộc tốc độ/độ trễ event; phân biệt state dedup với bộ nhớ output của helper chạy batch. Muốn trần cứng cần cap hoặc state ngoài RAM và chính sách khi vượt cap. Ghi giả định thời gian tiến triển; thêm ví dụ burst.

### m2 — Handoff thiếu nguồn phiên bản đủ cụ thể để kiểm toán

Vị trí: `.team/handoffs/T08-ch19-data-processing.md:16`; `08-data-processing/ch19-scala-data-processing.md:441`.

Handoff chỉ liệt kê version, không kèm link/ngày ngay phần phiên bản; bài dẫn root Maven Central. Build đã resolve được các artifact nên không nghi ngờ chúng tồn tại, nhưng bàn giao chưa đủ mục 2.3 AGENTS.md về version + link nguồn đã kiểm chứng + ngày.

Đề xuất: bổ sung URL artifact POM/release cụ thể, ngày tra và giới hạn kiểm chứng. Không cần đổi version chỉ vì CLI có hint phiên bản mới.

## Điều kiện review lại

Sửa M1–M7 trong code/bài/lời giải tương ứng; thêm regression với input nêu trên, chạy lại 15 test lab + 6 test lời giải và các test mới. Lead kiểm chứng sbt cuối. Chưa đánh dấu chương hoàn tất chỉ dựa trên golden mẫu hiện có.
