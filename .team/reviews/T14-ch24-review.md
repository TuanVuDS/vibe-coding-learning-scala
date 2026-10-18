# Review T14 — Chương 24: Batch Data Pipeline (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 4 MAJOR, 3 MINOR; không có BLOCKER.**

**Mốc source vòng 1:** kiểm chứng và chốt review khoảng 09:52–10:00 (UTC+7), trước các sửa đổi đồng thời sau đó. Khi kiểm tra workspace cuối lúc 10:21, Pipeline.scala/PipelineDemo.scala/PipelineSuite.scala đã được tiến trình khác sửa lúc 10:16–10:17; reviewer không ghi các file đó. File:dòng và kết quả bên dưới thuộc bản trước sửa (Pipeline.scala blob `d06fd31`), **chưa phải review lại bản mới**. Không áp kết luận test/probe cũ cho bản vừa sửa.

Đã đọc handoff T14, chương, toàn bộ source/test/build/data của lab và lời giải/code/test Ch24; đối chiếu PHẦN 11–14 và template. Chương đủ **17 mục có nội dung**, tiếng Việt rõ, ví dụ tính tay nối tiếp domain đơn hàng. Bài có giải thích đánh đổi, lỗi thật, bài tập tăng dần và liên kết lời giải. Những lỗi dưới đây nằm ở hợp đồng chạy lại/đối soát và validation; test mẫu xanh chưa đủ để chốt chương.

## Kiểm chứng thực tế

Chỉ ghi báo cáo này và `.cache/`; không sửa bài/code, không chạy sbt, không commit. Môi trường probe: **Spark 4.2.0, Scala 2.13.18, JDK 21.0.12.1, ANSI=true, session timezone UTC**, Windows local[2]. Build Scala 2.13 tách riêng phù hợp compatibility matrix; dependency và sbt pin khớp quy ước repo.

Runner `.cache/run-review.ps1` đặt `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`, `SPARK_LOCAL_DIRS=C:\Users\VuTT\zero_to_hero_scala\.cache\review-t14\local`, TEMP/TMP trong `review-t14/tmp`. Mỗi lượt thêm:

```text
--server=false
--workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t14/<name>-build
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/review-t14/tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/review-t14/warehouse
```

Các lệnh chạy từ thư mục source tương ứng:

| Lệnh | Kết quả thật | Log trong `.cache/review-t14/` |
|---|---|---|
| `scala-cli.bat test .` tại `09-spark/batch-pipeline-lab` + cờ trên | exit 0; **14 pass, 0 fail, 0 ignored**, 74.666s | `lab-rerun.log` |
| `scala-cli.bat test .` tại `solutions/ch24-code` + cờ trên | exit 0; **6 pass, 0 fail, 0 ignored**, 33.095s | `sol-rerun.log` |
| `scala-cli.bat run . --main-class Probe` tại `.cache/review-t14/probe` + cờ trên | exit 0; ca đối kháng chi tiết bên dưới | `probe-rerun2.log` |

Probe tái sử dụng file từ lượt trước. Lượt đầu không compile vì BOM trong **project.scala của probe** làm Scala CLI không nhận directives; đã bỏ BOM trong `.cache/` rồi chạy thành công. Hai cảnh báo unused import thuộc probe; không phải cảnh báo code tác giả. WARN winutils/native-hadoop không cản ghi/đọc Parquet. Không dùng PowerShell `NativeCommandError` bọc stderr để suy ra test fail; dùng exit code và summary thật.

`scala-cli.bat run . --main-class vn.scalaeng.batch.PipelineDemo` tại lab + các cờ trên → **exit 0**, log `demo-rerun.log`. Output mục 9 khớp: lần 1 doanh thu 21.600.000/fact 4; lần 2 và 3 doanh thu 21.900.000/fact 6, cả ba OK; các hàng daily khớp bảng tính tay, O5 có 6 lý do, O7 có 2 conflict; 8→1 file/ngày, ba ModeResult và pruning/broadcast khớp. Bảng trích trong bài có rút gọn như đã ghi. Console Java có một số chữ tiếng Việt hiển thị sai encoding trong log; số, schema, plan và error code vẫn đối chiếu được.

## MAJOR

### M1 — Output rỗng hợp lệ làm bước verify ném lỗi thay vì trả báo cáo

Vị trí: `09-spark/batch-pipeline-lab/src/main/scala/vn/scalaeng/batch/Pipeline.scala:214`, `:215`, `:244`, `:247`; `09-spark/ch24-batch-data-pipeline.md:53`.

Chạy vào output mới với từng input sau đều tái hiện:

- CSV chỉ có header.
- Một dòng có NULL `updated_at`: validation loại đúng, nhưng không còn fact.
- Một dòng hợp lệ trạng thái REFUNDED: có fact, daily không có dòng PAID.
- `upTo` nhỏ hơn tất cả các lô hiện có.

```text
AnalysisException: [UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Parquet.
It must be specified manually. SQLSTATE: 42KD9
```

Ghi partitioned DataFrame rỗng không cung cấp file dữ liệu để `read.parquet` tự suy schema. Kết quả: không trả được RunReport dù hợp đồng các ngăn hoàn toàn tính được; trong ca REFUNDED, fact đã được ghi trước khi job thất bại.

Đề xuất: định nghĩa rõ empty batch/no PAID/all rejected, đọc lại với schema tường minh và xử lý tập partition rỗng; hoặc xuất manifest/schema cho bảng rỗng. Không bỏ qua verify chỉ vì ít dữ liệu. Thêm regression độc lập cho bốn ca trên, cả output mới lẫn output đã có.

### M2 — Recompute + dynamic overwrite chưa tạo snapshot đúng; verify có thể báo OK với doanh thu cũ

Vị trí: `Pipeline.scala:15`, `:214`, `:215`, `:216`, `:231`, `:243`; `09-spark/ch24-batch-data-pipeline.md:125`, `:174`, `:176`; `solutions/ch24-code/src/main/scala/vn/scalaeng/solutions/ch24/Ch24Solutions.scala:95`.

Bài đã **nêu đúng** giới hạn partition mồ côi ở mục 3.7 và có bài 5. Tuy nhiên `Pipeline.run` vẫn mắc giới hạn đó và còn tự báo thành công:

1. Ghi một dòng PAID X=100; thêm lô có X CANCELLED **cùng timestamp**, làm cả hai thành conflict. Probe trả:

   ```text
   RunReport(2,0,2,0,0,2,0,0,List())
   diskFacts=1 diskRevenue=[100]
   ```

   `ok=true` dù report fact=0/doanh thu=0 và người đọc vẫn thấy fact/doanh thu cũ. `days` lấy từ facts mới là rỗng, nên verify lọc bỏ toàn bộ dữ liệu cũ.

2. Ghi PAID=100 rồi thêm bản REFUNDED muộn hơn. Report fact=1/doanh thu=0; disk daily vẫn 100. Verify bắt lệch tổng, nhưng output sai **đã phục vụ**, chạy lại y hệt vẫn không sửa được partition daily rỗng.
3. Dataset mẫu: chạy `upTo=16` rồi `upTo=15` cùng output. Report lần sau doanh thu **21.600.000**, `ok=true`; đọc toàn daily ra **22.350.000**, còn partition ngày 16 cũ. Chạy `upTo=15` vào thư mục mới cho snapshot khác.

Đề xuất: chốt semantics `upTo` (snapshot toàn bộ hay cập nhật phạm vi), tính scope từ cả trạng thái trước và input hiện tại, dọn partition biến mất cho **từng bảng** facts/daily/rejected trong scope; verify cả partition thừa/thiếu. Nếu không hỗ trợ giảm upTo, từ chối rõ trước ghi và hướng dẫn output riêng. Đưa giải pháp bài 5 vào đường chạy chính hoặc thu hẹp lời hứa về pipeline. Chỉ thêm chú thích không đủ chữa ca `ok=true` khi output còn số liệu sai. Việc sửa nguồn khiến một lô không còn rejected cũng cần regression vì rejected dùng cùng dynamic overwrite.

Đối chiếu [Spark 4.2.0 configuration — partitionOverwriteMode](https://spark.apache.org/docs/4.2.0/configuration.html): dynamic chỉ thay partition có dữ liệu ghi trong lượt chạy (tra 17/10/2026).

### M3 — Verify chỉ so tổng daily và số fact, bỏ qua tính đúng của nội dung và các bảng cách ly

Vị trí: `Pipeline.scala:242`, `:245`, `:247`, `:250`; `09-spark/ch24-batch-data-pipeline.md:182`.

Probe chạy golden trước, rồi sửa output qua staging để tránh đọc/ghi đè cùng đường dẫn:

```text
verify altered facts prices: List()
verify altered daily keys counts: List()
verify tamper and rejected missing: List()
```

- Đổi **mọi** `unit_price_vnd` fact thành 1 và `revenue_vnd` thành 0: số dòng và khóa vẫn như cũ → verify qua.
- Tiếp tục đổi **mọi** segment daily thành WRONG, orders/order_lines thành 999: giữ nguyên tổng revenue → vẫn qua, cả grain daily có thể trùng.
- Di chuyển toàn thư mục rejected ra ngoài output: verify vẫn qua vì không đọc rejected; conflicts cũng không được đọc lại.

Đây không phải yêu cầu chống mọi lỗi lưu trữ: chính bước 10 dạy đọc lại và đối soát nhưng hai phương trình đầu chỉ so **các số trong bộ nhớ**, chưa chứng minh bản ghi cách ly được ghi đủ; tổng daily cũng không được đối chiếu lại với fact đã lưu.

Đề xuất: đối soát fact đã lưu với fact kỳ vọng bằng multiset/khóa+nội dung; tính lại daily từ fact đọc lại và so theo `(day,segment)` cả revenue/order_lines/orders; kiểm tra uniqueness grain daily và đối soát rejected/conflicts đọc từ đĩa trong scope. Thêm các probe trên thành regression. Nếu giữ verify tối thiểu, phải nêu chính xác những lỗi nó **không** phát hiện và không gọi nó là chứng minh output đúng toàn pipeline.

### M4 — `try_cast` không thực thi hợp đồng định dạng ngày/timestamp; timezone có thể đổi phiên bản thắng

Vị trí: `Pipeline.scala:77`, `:82`, `:89`, `:94`; `09-spark/ch24-batch-data-pipeline.md:73`; `solutions/ch24-solutions.md:17`.

Probe `day="2026"`, `updated_at="2026"` được nhận, errors rỗng; day thành `2026-01-01`. Timestamp không có offset cũng qua. Cùng chuỗi `2026-10-15T00:00:00`, đổi session UTC sang Asia/Ho_Chi_Minh cho:

```text
UTC=1792022400000000 Vietnam=1791997200000000 deltaHours=7
```

Vì timestamp dùng để chọn phiên bản, chấp nhận giờ thiếu offset mà không công bố policy có thể đổi dedup/xung đột. Demo có chốt UTC, nhưng API `Pipeline.validate/run` nhận session từ ngoài; thông báo lỗi hứa ngày `yyyy-MM-dd` và timestamp ISO-8601 không mô tả cú pháp thực tế. Offset +07:00 và Z cùng instant được dedup đúng trong probe riêng.

Đề xuất: chốt contract (ngày đủ năm/tháng/ngày; timestamp đầy đủ và có offset, hoặc giờ địa phương với timezone cố định), kiểm tra cú pháp **và** parse hợp lệ; dùng TIMESTAMP_LTZ rõ ràng nếu cần instant, kiểm tra/thiết lập cấu hình phiên ở entrypoint. Nếu chủ ý chấp nhận cú pháp CAST rộng, sửa bài/lý do lỗi và thêm test timezone, ngày rút gọn, timestamp chỉ ngày/giờ. [SparkDateTimeUtils 4.2.0](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala) cho thấy parser chấp nhận ngày rút gọn và dùng timezone phiên khi thiếu zone; [Data types](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html) phân biệt LTZ/NTZ (tra 17/10/2026).

## MINOR

### m1 — `raw` không giữ nguyên văn CSV; hợp đồng cần phân biệt record với dòng vật lý

Vị trí: `09-spark/ch24-batch-data-pipeline.md:19`, `:112`, `:260`; `Pipeline.scala:74`, `:105`.

Probe UTF-8 BOM + CRLF + dòng rỗng + dòng chỉ khoảng trắng + một record có quote đọc được **1 record**. `"X","C1"` thành `X,C1` trong struct raw; quote, dấu phân cách và line ending không còn. Dòng `,,,,,,,` được giữ và nhận đủ 8 lỗi — khác với dòng vật lý rỗng được parser bỏ qua.

Đề xuất: gọi raw là “giá trị trường trước chuẩn hóa”, nói inputRows đếm **record parser trả ra**, loại header/dòng trắng. Nếu cần nguyên văn để truy vết, giữ source file/record identity và raw text riêng với policy multiline rõ ràng. Không gọi struct đã parse là nguyên văn byte/dòng gốc. [CSV options](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html) có quote/escape/lineSep và parser column pruning (tra 17/10/2026).

### m2 — `maxRecordsPerFile` không chữa một ngày bị dồn vào một task

Vị trí: `09-spark/ch24-batch-data-pipeline.md:151`, `:159`; `Pipeline.scala:168`; `Experiments.scala:25`.

Probe 100 dòng cùng ngày, repartition(day), maxRecordsPerFile=10: **10 file nhưng vẫn 1 partition/task ghi**. Cấu hình này giới hạn số record/file, không chia công việc của task nóng. “Mỗi task một file mỗi partition” cũng cần điều kiện không rollover file.

Đề xuất: tách vấn đề kích thước file khỏi parallelism/skew; để chia ngày lớn dùng khóa phụ/bucket và số partition phù hợp. Nêu giả định maxRecordsPerFile=0 cho các số 8→1. [Spark configuration](https://spark.apache.org/docs/4.2.0/configuration.html) chỉ định nghĩa giới hạn record/file (tra 17/10/2026).

### m3 — Tỷ lệ miền DECIMAL so với Long tính nhầm

Vị trí: `09-spark/ch24-batch-data-pipeline.md:132`.

`(10^38 - 1) / Long.MaxValue ≈ 1,084 × 10^19`, không phải `4 × 10^19`. Test vượt Long pass và probe vượt Decimal(38,0) ném đúng `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`; không phát hiện wrap-around ở đường SQL này. Đề xuất sửa tỷ lệ hoặc chỉ ghi “tối đa 38 chữ số”.

## Các ca đã qua và giới hạn

- NULL order_id/sku/updated_at bị loại trước dedup: 5 record → rejected 4 + latest 1; không phát hiện bản ghi biến mất do so sánh NULL trong đường chạy đã validate.
- Cùng instant khác offset: accepted 4 = exact 1 + superseded 2 + latest 1; bản REFUNDED mới hơn giải quyết xung đột cũ đúng.
- Partition `ingest_date=oops` làm kiểu suy ra String và checkSchema trả Left đúng; có thể đổi nhãn “Chưa kiểm chứng” của câu này sau khi tác giả tự chạy regression.
- Các lỗi cast, corrupt-only query, path exists, thiếu partition column và decimal overflow đã tái hiện; thông báo/error code khớp phần trích dẫn. Riêng tài liệu CSV tổng quát mô tả token count có khác hành vi lab: ưu tiên regression trên đúng build, không suy rộng mọi parser/query.
- Sáu bài tập có code/test tương ứng và pass; bài 5 chứng minh cách dọn orphan riêng, chưa được nối vào run (M2). WAP local trong bài 4 có khoảng trống giữa hai rename như lời giải đã nói; chưa kiểm chứng crash recovery/lỗi rename thứ hai, concurrent writer hay filesystem khác. Không coi đây là transaction nhiều bảng.
- Không kiểm chứng cluster/object storage, sbt và tải lớn; không dùng số thời gian local làm kết luận hiệu năng production. Lead cần sửa M1–M4, chạy regression và kiểm chứng cuối trước khi đánh dấu hoàn thành.


## V?ng 2

Reviewer: Codex ? Ng?y: 17/10/2026 ? **CH?A ??T ? c?n 1 MAJOR (M4), 0 BLOCKER, 0 MINOR.**

?? ??c handoff V?ng 2, ph?n s?a trong b?i, source v? regression test m?i. Gi? nguy?n to?n b? review v?ng 1. Ch? ghi file review v? `.cache/`; kh?ng s?a source, kh?ng ch?y sbt, kh?ng commit.

### Ki?m ch?ng l?i

D?ng `.cache/run-review.ps1`, `--server=false`, `COURSIER_CACHE=.cache/coursier`, workspace build/TEMP/TMP/SPARK_LOCAL_DIRS/warehouse d??i `.cache/review-t14/` nh? v?ng 1. Spark **4.2.0**, Scala **2.13.18**, JDK **21.0.12.1**, ANSI=true, session UTC; `PipelineDemo.session()` ?? t?t artifact isolation. Probe c? ???c gi? t?i `probe/Probe.scala.v1`; b?n `ProbeV2.scala` ch? th?ch nghi ch? k? `verify` m?i, gi? expected fact ??c l?p tr??c khi s?a output v? ??c b?ng r?ng b?ng schema/ki?m tra file.

| L?nh qua runner | K?t qu? | Log |
|---|---|---|
| `scala-cli.bat test .` t?i `09-spark/batch-pipeline-lab` | exit 0; **18 pass, 0 fail, 0 ignored** (129,804 gi?y) | `.cache/review-t14/v2-lab.log` |
| `scala-cli.bat run . --main-class ProbeV2` t?i `.cache/review-t14/probe` | exit 0; ch?y ?? nh?m probe M1?M4 | `.cache/review-t14/v2-probe.log` |
| `scala-cli.bat run . --main-class TypeProbe` t?i c?ng th? m?c | exit 0; x?c nh?n ?i?m M4 d??i ??y | `.cache/review-t14/v2-type.log` |

Runner t??ng ?ng: `powershell -NoProfile -ExecutionPolicy Bypass -File .cache/run-review.ps1 -Part t14 -Source <source> -Name <v2-lab|v2-probe|v2-type>`; th?m `-Mode run -Main <main>` cho probe. M?t warning unused import thu?c TypeProbe; kh?ng thu?c source t?c gi?. WARN native-hadoop/winutils kh?ng c?n ??c/ghi. Log Java v?n c? ch? Vi?t l?i encoding; ??i chi?u s? li?u v? th?ng b?o v?i source, kh?ng coi b?n hi?n th? l?i d?u l? output chu?n ?? ??a v?o b?i.

### ?? kh?p l?i t? v?ng 1

- **M1:** b?n ca header-only, NULL timestamp, ch? REFUNDED v? `upTo` tr??c t?t c? l? ??u tr? `RunReport(...,List())`, kh?ng c?n `UNABLE_TO_INFER_SCHEMA`; doanh thu ??u 0.
- **M2:** gi?m upTo 16?15 cho `diskTotal=[21600000]`, ch? c?n ng?y 15; report n?u partition ng?y 16 b? d?n ? fact/daily/rejected. L? m?i g?y to?n b? fact th?nh conflict: `diskFacts=0`, daily r?ng, conflictRows=2, report OK. Ho?n ti?n d?ng PAID cu?i: factLines=1, daily r?ng, report OK. Kh?ng c?n gi? doanh thu 100 c?.
- **M3:** ??i gi?/revenue fact gi? s? d?ng b? b?t v?i thi?u 6/th?a 6; ??i segment/orders/order_lines daily b? b?t v?i 8 d?ng l?ch v? grain tr?ng; di chuy?n rejected kh?i output b? b?t v?i `0 != 3`. `verify` so to?n b?ng v? d?ng `exceptAll` hai chi?u; b?i m?c 3.8 m? t? ??ng gi?i h?n ch? ??m s? d?ng c?a b?ng c?ch ly.
- **M4, ph?n c? ph?p:** ng?y/timestamp `2026` b? lo?i; timestamp kh?ng offset b? lo?i ? c? UTC v? Asia/Ho_Chi_Minh. Z v? +07:00 c?ng instant c? exactDuplicateRows=1 trong c?u h?nh LTZ m?c ??nh. Tuy nhi?n c?n ph? thu?c `timestampType`, xem d??i.
- **m1?m3:** c?ch g?i gi? tr? tr??ng tr??c chu?n h?a, ph?n bi?t file v?i task v? t? l? DECIMAL/Long ?? s?a ??ng; kh?ng c?n ph?t hi?n MINOR t? ba m?c c?.

### MAJOR ? M4 c?n h?: `TIMESTAMP` v?n l? alias ph? thu?c c?u h?nh phi?n

V? tr?: `09-spark/batch-pipeline-lab/src/main/scala/vn/scalaeng/batch/Pipeline.scala:92` (cast `AS TIMESTAMP`); `09-spark/batch-pipeline-lab/src/main/scala/vn/scalaeng/batch/PipelineDemo.scala:13`; `09-spark/ch24-batch-data-pipeline.md:82`.

`Pipeline.validate` nh?n DataFrame t? phi?n b?n ngo?i; regex b?t c? offset nh?ng ph?p cast ch?a bu?c ki?u instant. Probe gi? nguy?n hai record c?ng kh?a/n?i dung, ch? kh?c bi?u di?n `2026-10-15T00:00:00Z` v? `2026-10-15T07:00:00+07:00`, r?i ??i `spark.sql.timestampType`:

```text
TYPE TIMESTAMP_LTZ schema=TimestampType exact=1 latest=List([2026-10-15 07:00:00.0]) errors=List([ArraySeq()], [ArraySeq()])
TYPE TIMESTAMP_NTZ schema=TimestampNTZType exact=0 latest=List([2026-10-15T07:00]) errors=List([ArraySeq()], [ArraySeq()])
```

? NTZ, offset b? b? qua: hai c?ch vi?t c?a c?ng instant tr? th?nh 00:00 v? 07:00, b?n 07:00 th?ng nh? m?t b?n c?p nh?t. Regex v?n ch?p nh?n c? hai, kh?ng b?o c?u h?nh kh?ng h? tr?. Nh? v?y ch? ??i c?u h?nh c? th? ??i exact/superseded v? phi?n b?n th?ng; l?i h?a ?khi c? offset, instant kh?ng ph? thu?c m?i gi? phi?n? c?n th?m ?i?u ki?n LTZ. Demo kh?ng pin ho?c ki?m tra `spark.sql.timestampType`.

**?? xu?t:** cast tr?c ti?p `try_cast(... AS TIMESTAMP_LTZ)` t?i validation (?u ti?n v? API nh?n session t? ngo?i), ho?c t? ch?i c?u h?nh NTZ r? r?ng tr??c x? l?. Th?m regression ch?y c?ng c?c offset d??i c? hai gi? tr? timestampType; ph?i c?ng instant, c?ng k?t qu? dedup, ho?c l?i c?u h?nh ???c c?ng b?. Kh?ng ch? s?a timezone UTC v? UTC kh?ng thay alias NTZ th?nh LTZ. [Spark 4.2.0 Data Types](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html) x?c nh?n TIMESTAMP l? alias c?u h?nh ???c gi?a LTZ/NTZ (??i chi?u 17/10/2026).

### Gi?i h?n

V?ng n?y kh?ng ch?y l?i sbt, l?i gi?i Ch24, demo, cluster/object storage hay crash gi?a c?c l?n ghi; kh?ng m? r?ng k?t lu?n cho c?c ???ng ch?y ?y. V?n ?? nguy?n t? nhi?u b?ng ???c b?i th?a nh?n v? chuy?n sang WAP/table format, kh?ng t?nh th?nh l?i m?i c?a lab. Lead c?n x? l? M4 c?n l?i v? ch?y ki?m ch?ng cu?i tr??c khi ??nh d?u ho?n th?nh.
