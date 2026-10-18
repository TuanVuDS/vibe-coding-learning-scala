# Review T17 — Chương 33 Data Quality và Testing

Reviewer: Codex · Ngày: 17/10/2026 · Tác giả: Claude Code.

**Kết luận: CHƯA ĐẠT — 0 BLOCKER, 3 MAJOR, 3 MINOR.** Test gốc 16/16 và lời giải 7/7 pass; chưa thể duyệt phần compatibility vì kết luận Compatible hiện không bảo đảm consumer cũ nhận được dữ liệu mới. Không sửa code/tài liệu của tác giả, không commit.

## Phạm vi và kiểm chứng thật

Đã đọc handoff T17, chương, toàn bộ source/test/config/fixture/golden của `quality-lab`, lời giải Markdown và `ch33-code`, AGENTS.md mục 2.4/5, template 17 mục và PROMPT_PROJECT.md phần 11–14. Không tìm thấy brief riêng mang tên T17 trong `.team/briefs/`; dùng phạm vi review được user giao trực tiếp.

Chạy trên **bản sao** giữ nguyên cấu trúc tương đối tại `.cache/review-t17/`, vì Scala CLI tạo build files và regression test ghi/xóa `.actual`. Nguồn tác giả không bị sửa. `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`; test có tạo thư mục tạm nên TEMP/TMP được chuyển vào `.cache/review-t17`.

| Lệnh (tại thư mục bản sao tương ứng) | Kết quả thật |
|---|---|
| `scala-cli.bat test . --server=false` tại `11-data-platform/quality-lab` | PropertySuite: 0 failed, 0 ignored, 6 total; QualitySuite: 0 failed, 0 ignored, 10 total |
| `scala-cli.bat test . --server=false` tại `solutions/ch33-code` | SolutionsSuite: 0 failed, 0 ignored, 7 total |
| `scala-cli.bat run . --server=false --main-class vn.scalaeng.quality.QualityDemo --java-opt '-Dstdout.encoding=UTF-8'` | Exit 0; fact=6, rejected=3, conflicts=2, trùng=1, thay-thế=1; daily và toàn bộ báo cáo khớp mục 9 |
| `scala-cli.bat run . --server=false --main-class vn.scalaeng.quality.Probe33 --java-opt '-Dstdout.encoding=UTF-8'` tại `probes/` | Exit 0; 20.926 kiểm tra incremental/latest/conflicts/replay pass; các phản ví dụ contract/file dưới đây tái hiện được |

Log: `.cache/review-t17/{test-lab.log,test-solutions.log,demo.log,probes.log}`; probe: `.cache/review-t17/probes/Probe33.scala`. Không có compiler warning; có hint nâng phiên bản munit/munit-scalacheck của Scala CLI (không phải warning biên dịch). Không chạy sbt trong sandbox theo AGENTS.md; sbt do lead kiểm chứng lại.

## MAJOR

### M1 — `int → long` bị duyệt Compatible ở hướng reader cũ / writer mới

- **Vị trí:** `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Contract.scala:81`; `11-data-platform/ch33-data-quality-testing.md:114`; test khẳng định kết luận sai tại `quality-lab/src/test/scala/vn/scalaeng/quality/QualitySuite.scala:60`.
- Hàm được mô tả là hỏi consumer theo `old` có đọc được dữ liệu của `next` hay không. Probe contract một trường `x`: old=int, next=long; bản ghi `x=2147483648` qua next, trượt old, nhưng compatibility trả `Change(x,int → long (nới rộng),Compatible)`. Contract không có thông tin chứng minh consumer chấp nhận long; chú thích có điều kiện trong bài không được thực thi trong code.
- **Hướng chuẩn:** đây là FORWARD của Confluent (reader cũ, writer mới). Avro cho phép **writer int → reader long**, không cho writer long → reader int. Đừng lẫn việc mở rộng reader với việc mở rộng miền dữ liệu writer.
- **Đề xuất:** với hướng đã chọn, mặc định Breaking cho int→long; hoặc thêm khả năng/ràng buộc có thể kiểm tra của reader và chỉ cho Compatible khi điều kiện đó được chứng minh. Bổ sung regression với `Int.MaxValue + 1`, sửa bảng lý thuyết, test, output demo và lời giải liên quan. Ghi trực tiếp ánh xạ FORWARD/BACKWARD; enum thêm/bớt hiện đang đúng hướng khi reader không có fallback/default.

### M2 — Thêm trường được gọi Compatible trong khi chính consumer validator cũ từ chối trường thừa

- **Vị trí:** `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Contract.scala:44`, `Contract.scala:76`; `11-data-platform/ch33-data-quality-testing.md:117`; `solutions/ch33-solutions.md:55` (phần Bài 3 mô tả v2 vi phạm v1 vì `channel`).
- Probe chỉ thêm trường string nullable `new`: compatibility trả Compatible; bản ghi `{x:1,new:value}` qua next nhưng old báo `new: trường không có trong contract x v1`. Lỗi này độc lập M1 và không cần thay enum/nullability. Bài vừa nói consumer cũ bỏ qua trường mới, vừa triển khai validator cũ theo closed-world (cấm trường thừa).
- Avro reader bỏ qua trường chỉ có ở writer nên thêm trường có thể FORWARD compatible; validator của lab chưa có phép chiếu/tùy chọn tương đương. Có thể cố ý tách producer conformance khỏi consumer decoding, nhưng phải có hai API/chính sách và phát biểu rõ compatibility đang bảo đảm API nào; hiện chưa có.
- **Đề xuất:** chọn chính sách nhất quán: (a) compatibility theo strict validator thì thêm trường là Breaking; hoặc (b) giữ kiểm tra producer nghiêm, thêm reader projection/allowUnknownFields và kiểm thử reader cũ đọc record mới. Test nên chứng minh `validate(next, record)` đạt ⇒ reader cũ nhận được record với từng thay đổi được đánh Compatible. Đồng bộ phần lý thuyết và lời giải.

### M3 — Parser âm thầm bỏ giá trị vượt số cột, che mất dữ liệu sai trước cả contract

- **Vị trí:** `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Files.scala:14`.
- Với header 8 cột hiện tại, thêm `,UNDECLARED` cuối một dòng hợp lệ. `zipAll` gán tên cột thừa thành chuỗi rỗng rồi `filter(_._1.nonEmpty)` xóa nó. Probe trả bản ghi như cũ và `Pipeline.validate` vẫn `Right(Order(...))`; contract cũng không còn thấy dữ liệu thừa để báo. Đây là input không có quote/dấu phẩy trong giá trị, nằm ngay trong tập CSV đơn giản của lab.
- **Đề xuất:** kiểm tra số cột trước khi tạo Map; từ chối/quarantine dòng thừa cột với số dòng và lý do. Giữ hoặc ghi rõ chính sách điền rỗng khi thiếu cột. Thêm regression qua đường `DataFiles → Contract/Pipeline`, tránh chỉ test Map tự dựng.

## MINOR

### m1 — Property có ích nhưng chưa bảo đảm gặp các tổ hợp khó mà bài gọi là “mọi cách chia lô”

- **Vị trí:** `11-data-platform/quality-lab/src/test/scala/vn/scalaeng/quality/PropertySuite.scala:15`, `PropertySuite.scala:64`, `PropertySuite.scala:66`; `11-data-platform/ch33-data-quality-testing.md:159`, `ch33-data-quality-testing.md:484`.
- Bộ sinh hẹp về khóa/thời gian là đúng; property trùng/trễ chủ động thêm dữ liệu nên không chỉ kiểm “không exception”. `served` giữ facts/daily/conflicts, bỏ số đếm thay đổi khi replay là hợp lý. Tuy nhiên payload còn nhiều chiều độc lập, không chủ động sinh ba payload hòa timestamp, duplicate cùng payload ở các lô và conflict đi kèm. Backfill chỉ chọn một kích thước nhóm rồi shuffle các nhóm, không duyệt mọi ranh giới; chỉ so latest, không so toàn bộ conflict state. Khi latest rỗng, mất một phần payload xung đột có thể chưa bị bắt.
- **Đề xuất:** thêm generator theo scenario và regression cố định cho các ca nêu trên; dùng classify/collect hoặc coverage statistics để thấy ca khó thực sự xuất hiện; nếu muốn cam kết đầy đủ conflict state thì so cả tập đó. Viết “kiểm tra nhiều cách chia/thứ tự sinh ngẫu nhiên”, tránh gọi 100 mẫu là chứng minh với mọi input.

### m2 — UTF-8 BOM làm mọi dòng mất khóa cột đầu

- **Vị trí:** `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Files.scala:13`.
- Thêm BOM U+FEFF trước header `order_id`: `trim` không bỏ BOM, Map có khóa `\uFEFForder_id`; `toRaw` nhận orderId rỗng và trả `order_id: bắt buộc` cho dòng vốn hợp lệ. Nếu BOM ở customers, customer_id cũng không được nhận diện. CRLF đơn thuần không lỗi.
- **Đề xuất:** strip BOM chỉ ở đầu file/header và thêm test BOM+CRLF; hoặc ghi rõ BOM không được hỗ trợ và fail sớm với lỗi header cụ thể. Không đổ lỗi dữ liệu nghiệp vụ cho vấn đề encoding.

### m3 — Kỳ vọng freshness đỏ khi xóa lô 16/10 không đúng cấu hình hiện có

- **Vị trí:** `11-data-platform/ch33-data-quality-testing.md:491`; `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Checks.scala:51`.
- Giữ nguyên asOf=17/10 trong demo và maxLagDays=2, xóa lô 16/10 thì newest=15/10 vẫn nằm đúng biên cho phép. Probe thật: `CheckResult(fact: có dữ liệu trong 2 ngày gần nhất,Error,4,0,List())`. Mini project yêu cầu freshness đỏ nhưng chưa nói đổi tham số.
- **Đề xuất:** nêu maxLagDays=1/asOf phù hợp cho ca thử, hoặc dùng check đủ lô/ngày được kỳ vọng. Phân biệt freshness với completeness của batch.

## Các probe còn lại và đánh giá nội dung

- **Incremental:** duyệt mọi chuỗi độ dài 0..4 từ 7 biến thể cùng key (ba payload hòa timestamp, trùng khác lô sớm/muộn, timestamp cũ/mới), mọi ranh giới chia lô của từng chuỗi; thêm hoán vị scenario ba payload và duplicate+conflict+lô sớm đến sau. Tổng **20.926** lần: latest bằng `Pipeline.dedup.latest`, toàn bộ conflict của state bằng `Pipeline.dedup.conflicts`, replay không đổi state. Không tìm thấy lỗi logic trong `Incremental.step` ở phạm vi đã thử; đây không phải chứng minh cho mọi input.
- **Drift:** 0→0 không cảnh báo/không chia 0; 0→dương dùng quy ước 100% và cảnh báo ở ngưỡng 1%; dương→0 cảnh báo; đổi Retail→Wholesale cùng tổng tiền bắt cả segment mất và segment mới. Nên ghi quy ước baseline=0 là quyết định chính sách; không phát hiện lỗi ở các probe này. `Profile.rows` được ghi nhận nhưng chưa được `compare` so ngưỡng; nếu muốn dạy drift số dòng như mục 3.7 thì bổ sung hoặc nói rõ hiện chỉ theo dõi trong profile.
- **DataFiles:** CRLF pass; thiếu cột cuối được điền rỗng và pipeline reject với lỗi `updated_at` thật; BOM và thừa cột có vấn đề như trên. Không coi việc chưa hỗ trợ quoted CSV là lỗi vì code/bài đã giới hạn rõ.
- **Golden:** golden checkout CRLF + actual LF trả `Right(())`, đúng đường `Csv.*` hiện luôn sinh LF. Actual CRLF vẫn fail (chỉ expected được normalize), diff có thể rỗng vì linesIterator bỏ xuống dòng. Đây là giới hạn helper, chưa chặn luồng lab; có thể normalize cả hai phía nếu mở rộng cho output Windows từ writer khác.
- **17 mục:** đủ 1–17, không mục rỗng; mục vị trí trong lộ trình là phần bổ sung. Tiếng Việt và domain đơn hàng nhất quán, ví dụ tính tay dễ đối soát; phân biệt contract/type và validation nghiệp vụ tốt. Cần sửa phần compatibility trước khi dùng làm tài liệu học. Các mục lý thuyết chưa trình bày lựa chọn thay thế/ưu nhược đồng đều như yêu cầu đầy đủ của template, nhưng đã có tradeoff ở các chủ đề chính.
- **Output:** demo khớp phần trích mục 9, golden cả 3 file pass, đối soát 13=3+1+1+2+6; không xác nhận được lịch sử “golden được tính trước lần chạy đầu” chỉ bằng trạng thái repo hiện tại. Số test khớp handoff. Không tái chạy mutation dùng để tạo mọi seed/thông báo lỗi lịch sử của tác giả; không coi đó là đã độc lập tái hiện.
- **dbt/Deequ/Great Expectations/Soda:** chỉ được mô tả ở mức khái niệm, không có dependency/API/version triển khai hay suy diễn hỗ trợ Scala 3. Phát biểu dbt có bốn generic data tests và Deequ dựa trên Spark phù hợp nguồn chính thức. Không cần bổ sung framework này vào lab.

## Nguồn đối chiếu (17/10/2026)

- [Avro 1.12.0 — Schema Resolution](https://avro.apache.org/docs/1.12.0/specification/#schema-resolution): hướng promotion của writer sang reader, trường writer thừa bị bỏ qua, enum lạ có thể dùng reader default. Quy tắc lab không phải bộ kiểm tra Avro đầy đủ.
- [Confluent — Schema evolution/compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html): FORWARD = reader cũ đọc writer mới; BACKWARD = reader mới đọc writer cũ.
- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests), [Deequ](https://github.com/awslabs/deequ), [Great Expectations](https://docs.greatexpectations.io/docs/home/): đối chiếu phạm vi khái niệm, không kiểm chứng runtime/version của các công cụ này.
- Maven Central POM trả HTTP 200: [circe-parser_3 0.14.17](https://repo.maven.apache.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom), [munit_3 1.2.4](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom), [munit-scalacheck_3 1.2.0](https://repo.maven.apache.org/maven2/org/scalameta/munit-scalacheck_3/1.2.0/munit-scalacheck_3-1.2.0.pom). POM cuối kéo ScalaCheck 1.19.0, munit 1.1.1 như handoff; lab khai báo trực tiếp munit 1.2.4 và test thật pass. Truy cập repo1 bằng web/PowerShell lỗi kết nối; đã dùng endpoint Maven Central `repo.maven.apache.org` để xác minh, bản POM lưu trong `.cache/review-t17/`.

**Điều kiện review tiếp:** tác giả sửa M1–M3, bổ sung regression tương ứng, cập nhật output/tài liệu; lead chạy lại test theo quy trình. Chưa đánh dấu chương hoàn tất.


## V?ng 2

Reviewer: Codex ? Ng?y: 17/10/2026 ? **CH?A ??T ? c?n 1 MAJOR (M3, ca c?t th?a r?ng), 0 BLOCKER, 0 MINOR.**

?? ??c handoff V?ng 2, source/test m?i, b?ng FORWARD, test ?CAM K?T c?a compatibility?, ph?n property/freshness v? l?i gi?i li?n quan. Gi? nguy?n review v?ng 1. Ch? ghi review v? `.cache/`; kh?ng s?a source/b?i, kh?ng sbt/commit.

### Ki?m ch?ng l?i

Scala **3.9.0**, JDK **21.0.12.1**, dependency pin gi? nguy?n. Ch?y tr?n **b?n sao source hi?n t?i** t?i `.cache/review-t17/v2/11-data-platform/quality-lab` v? Golden.check c? ghi/x?a `.actual`. Probe33 d?ng b?n sao source c?p nh?t t?i `.cache/review-t17/11-data-platform/quality-lab`; kh?ng d?ng code v?ng 1. `Probe33.scala.v1` gi? b?n probe tr??c s?a; probe hi?n t?i th?m ki?m `Contract.read`, c?t th?a r?ng/kho?ng tr?ng v? so k?t qu? contract/pipeline.

Runner `.cache/run-review.ps1 -Part t17`, `--server=false`; `COURSIER_CACHE=.cache/coursier`, workspace/TEMP/TMP/SPARK_LOCAL_DIRS/warehouse d??i `.cache/review-t17/` nh? c?u h?nh v?ng 1.

| L?nh (cwd n?u tr?n) | K?t qu? th?t | Log |
|---|---|---|
| `scala-cli.bat test . --server=false` t?i b?n sao lab | exit 0; **PropertySuite 6/6**, **QualitySuite 12/12**; 0 fail, 0 ignored | `.cache/review-t17/v2-lab.log` |
| `scala-cli.bat run . --server=false --main-class vn.scalaeng.quality.Probe33` t?i `.cache/review-t17/probes` | exit 0; **20.926** ki?m tra incremental/latest/to?n b? conflicts/replay pass; c?c probe contract/parser/golden/drift ch?y ?? | `.cache/review-t17/v2-probes.log` |

Kh?ng ph?t hi?n compiler warning; hint n?ng dependency c?a Scala CLI kh?ng ph?i warning bi?n d?ch. Log Java c? ch? Vi?t l?i encoding; d?ng s? li?u/gi? tr? c?u tr?c v? ??i chi?u source cho l? do ti?ng Vi?t. Kh?ng ch?y l?i demo v? suite l?i gi?i ? v?ng n?y.

### C?c m?c ?? kh?p l?i

- **M1:** FORWARD ???c ??nh ngh?a r? l? reader c? ? writer m?i. int?long nay Breaking; writer nh?n 2147483648 nh?ng reader int kh?ng nh?n. long?int Compatible ??ng theo mi?n d? li?u; b?t bu?c?nullable Breaking, chi?u ng??c l?i Compatible.
- **M2:** ?? c? hai API v?i hai m?c ??ch. Th?m tr??ng nullable tr? Compatible; producer validator c? t? ch?i tr??ng l? nh? thi?t k? nghi?m, c?n `Contract.read(old,record)` tr? Nil. L? thuy?t, v? d? v? l?i gi?i ?? gi?i th?ch kh?c bi?t. Quy t?c n?y l? m? h?nh contract chu?i c?a lab, kh?ng ph?i b? ph?n gi?i Avro ??y ??.
- **Test CAM K?T:** ch?n c?p thay ??i c? d? li?u bi?n th?c t? v? ??i chi?u `validate(next)` v?i `read(old)`; verdict ??ng `false,true,true,true,false,false,true,false,true`. Test ?? ch?y trong QualitySuite 12/12; kh?ng ch? assert nh?n do h?m compatibility tr? ra. T?p m?u n?y l? b?ng ch?ng cho ch?n scenario, kh?ng l? ch?ng minh t??ng th?ch tr?n m?i contract/record. C?c quy t?c ??nh Compatible hi?n t?i ? ch?n c?p kh?ng c?n ph?n v? d? v?ng 1.
- **M3, ca g?c:** `,UNDECLARED` ???c gi? qua DataFiles?RawOrder v? pipeline reject; contract c?ng b?o c?t th?a. C?n ca r?ng b?n d??i.
- **m1:** backfill property nay so c? latest v? conflicts, c? collect nh?n; b?i ph?n bi?t ki?m tra ng?u nhi?n v?i ch?ng minh. Probe exhaustive ??c l?p v?n **20926 checks PASS** cho kh?ng gian h?u h?n v?ng 1.
- **m2:** BOM+CRLF ??c ra ??ng order_id=O1, contract kh?ng l?i, pipeline Right. C?t thi?u updated_at v?n b? reject c? l? do.
- **m3:** mini project ?? n?u maxLagDays=1 v? ph?n bi?t freshness/completeness. Probe c? ? ng??ng 2 v?n kh?ng ??, ??ng nh? ch? th?ch m?i; kh?ng c?n kh?ng ??nh sai ca ?? ph?i ?? v?i c?u h?nh demo.
- Golden normalize CRLF c? hai ph?a: expected CRLF/actual LF v? actual CRLF ??u Right. Drift 0?0, 0?d??ng, d??ng?0 v? segment m?i/m?t gi? k?t qu? ??ng v?ng 1; quy ??c baseline=0 v? gi?i h?n Profile.rows ?? ???c ghi r?.

??i chi?u l?i ??nh ngh?a h??ng v?i [Confluent ? Schema Evolution](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html), truy c?p 17/10/2026: FORWARD d?ng reader c? ?? ??c d? li?u writer m?i; BACKWARD ??o vai reader/writer. Kh?ng suy ra t??ng th?ch Avro wire format t? vi?c hai h?m parse Map[String,String] ??c ???c c?ng d? li?u.

### MAJOR ? M3 c?n h?: c?t th?a r?ng/kho?ng tr?ng m?t d?u hi?u l?i khi sang RawOrder

V? tr?: `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Files.scala:22`, `:35`; `11-data-platform/quality-lab/src/main/scala/vn/scalaeng/quality/Pipeline.scala:10`, `:63`; `11-data-platform/quality-lab/src/test/scala/vn/scalaeng/quality/QualitySuite.scala:155`; `11-data-platform/ch33-data-quality-testing.md:326`.

Gi? header 8 c?t v? d?ng h?p l? v?ng 1, ch? th?m cu?i d?ng **m?t d?u ph?y**, ho?c `,   `:

```text
DataFiles extra-empty: _extra_columns -> ""; Pipeline.validate = Right(Order(...)); Contract.validate = List(... ngo?i header: '')
DataFiles extra-space: _extra_columns -> "   "; Pipeline.validate = Right(Order(...)); Contract.validate = List(... ngo?i header: '   ')
```

??y l? t?m t?t gi? tr? th?t t? `v2-probes.log`, kh?ng ph?i tr?ch nguy?n v?n log ??y ??. Parser ?? ph?t hi?n sai s? c?t v? ??t key ??c bi?t, nh?ng `toRaw` d?ng `getOrElse(..., "")` n?n ?kh?ng c? c?t th?a? v? ?c? c?t th?a r?ng? tr? th?nh c?ng d? li?u. `Pipeline.validate` ti?p t?c d?ng `text(r.extra).nonEmpty`, b? c? tr??ng h?p c?t th?a ch? c? kho?ng tr?ng. ???ng pipeline kh?ng g?i Contract.validate tr??c ??, n?n record h?ng v?n th?nh fact; tr?i policy m?i ?t? ch?i d?ng th?a c?t?. Regression hi?n ch? d?ng UNDECLARED n?n kh?ng b?t ???c.

**?? xu?t:** mang theo s? c?t th?a ho?c `Option[Vector[String]]`/c? c?u tr?c ri?ng; ki?m tra s? hi?n di?n/s? l??ng, kh?ng suy t? n?i dung ?? trim. Th?m test end-to-end cho c?t th?a c? gi? tr?, r?ng, kho?ng tr?ng v? nhi?u c?t r?ng, ??ng th?i gi? thi?u c?t ?i qua validation nghi?p v? nh? policy ?? ch?t. N?u ch? ? cho trailing delimiter th? ph?i c?ng b? ri?ng v? l?m contract/pipeline nh?t qu?n; hi?n m?t ???ng reject, ???ng kia nh?n.

### Gi?i h?n

Kh?ng ch?y sbt, mutation ?? t?o l?i c?c seed l?i l?ch s?, framework ngo?i, ho?c m?i mi?n input. C?c M1/M2 ?? s?a ? m? h?nh contract hi?n t?i; ch? c?n M3 c?n t?c gi? x? l? v? lead ki?m ch?ng cu?i. Kh?ng ??nh d?u ch??ng ho?n t?t t? suite xanh khi probe c?u tr?c CSV c?n l?t.
