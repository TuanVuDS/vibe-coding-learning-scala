# Review Project 3 — Batch Data Pipeline (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 6 MAJOR, 6 MINOR; không có BLOCKER.**

Đã đọc README, toàn bộ source/test/build/deploy/data P3, phần liên quan trong handoff T14/T16 và khuôn project tại `14-projects/README.md`. README đủ mục tiêu, tiên quyết, đặc tả, thiết kế/trade-off, cách chạy, output, kiểm thử, lỗi thường gặp, bài tập mở rộng, checklist và nguồn. P3 dùng **khuôn project**, không yêu cầu áp 17 mục chương. Tiếng Việt rõ; bài tính tay assisted revenue và phân biệt tổng assisted với doanh thu thật có giá trị sư phạm. Không sửa code tác giả, file chung, T15/P4 hay 11-data-platform; không commit.

## Kiểm chứng thực tế

Dùng runner `.cache/run-review.ps1`, Windows, Scala **2.13.18**, Spark **4.2.0**, JDK **21.0.12.1**, munit **1.2.4**, local[2], timezone UTC mặc định của Job.session, ANSI bật. COURSIER_CACHE ở `<repo>/.cache/coursier`; SPARK_LOCAL_DIRS, TEMP/TMP, java.io.tmpdir, warehouse và workspace dưới `.cache/review-p3/`. Lệnh nào cũng có **`--server=false`**, thêm:

```text
--workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-p3/<name>-build
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/review-p3/tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/review-p3/warehouse
```

- `scala-cli.bat test .` từ `14-projects/p3-batch-pipeline` + cờ trên: lượt 1 **exit 1, 9 pass / 2 fail / 0 ignored**, tổng 143.645s. Hai lỗi đều `TimeoutException: test timed out after 30 seconds`: e2e 35.986s và dữ liệu ngẫu nhiên 32.289s. Không thấy assertion dữ liệu sai. Log `project-test.log`. Probe của reviewer chỉ bắt đầu cuối suite; không quy toàn bộ hai timeout cho nó, cũng không khẳng định toàn máy rảnh vì các agent khác đang làm việc.
- Chạy lại nguyên suite riêng, không sửa source và không còn Spark JVM/probe khác của reviewer: **exit 0, 11 pass / 0 fail / 0 ignored**, tổng **126.573s** (`project-test-isolated.log`). Không khẳng định toàn máy rảnh. Kết quả xác nhận timeout lượt đầu phụ thuộc điều kiện chạy; không phải assertion chứng minh dữ liệu golden sai.
- `scala-cli.bat run . --main-class Probe` từ `.cache/review-p3/probe` + cờ trên: **exit 0**, fault injection thật, log `attack.log`; kết quả dưới đây. Cảnh báo unused của probe thuộc reviewer, không phải warning của source tác giả.
- Golden trong suite khớp bảng nhỏ: daily 700.000/450.000/20.000.000/750.000; campaign NEWBIE 2/2/1/700.000/0,5000, SALE10 ngày 15 3/2/2/20.700.000/1,0000, ngày 16 2/2/1/750.000/0,5000. Plan cuối in SortMergeJoin click–revenue cùng BroadcastHashJoin customers như README (ID/codegen number thay đổi bình thường). Generator nhỏ in `Summary(3,1533,6025,200)`.
- Đã tái sinh **3 × 100.000 đơn/ngày + 300.000 click/ngày, seed 2026**, rồi chạy Job.run tới ngày 17 với ngưỡng 0.02 trong `.cache/`: `Summary(3,305299,904521,2000)`, job **SUCCEEDED / code 0 / 76.805 ms**, log `large-check.log`. Manifest khớp **từng count và doanh thu** trong README: ordersInput=305299, rejected=2964, exactDup=1497, superseded=3788, conflict=0, fact=297050; clicksInput=904521, rejected=4545, dup=4493, conflict=0, kept=895483; paidRevenueVnd=1189620280000. Đây là một lần publish đầy đủ trên Windows, không phải benchmark so với WSL hay chạy lại toàn bộ bốn lệnh lớn của README.

Driver `LargeCheck` cuối cùng exit **1** vì reviewer còn ghép phép thử lexical sau job lớn: thay mọi cột RAW bằng literal làm optimizer chỉ đọc `_corrupt_record`, bị guard QUERY_ONLY_CORRUPT_RECORD_COLUMN. Job lớn trước đó đã trả 0 và ghi manifest như trên; không tính lỗi probe này thành lỗi P3. Đã tách phép thử lexical sang fixture DataFrame trong bộ nhớ: `scala-cli.bat run . --main-class LexicalCheck` + cờ trên → **exit 0**, log `lexical-check.log`, không cần chạy lại job lớn chỉ để sửa probe. Cả Orders.validate và Clicks.validate chấp nhận chuỗi chỉ có năm `2026`, errors rỗng, day=2026-01-01.

WARN thiếu winutils/native-hadoop không cản test Parquet qua BareLocalFileSystem. Không chạy sbt/assembly hay WSL: ở lượt Ch26, `wsl --list --quiet` đã bị sandbox từ chối E_ACCESSDENIED; không gọi lại một đường bị chặn tương tự. Chưa tái xác nhận thời gian WSL, kích thước jar hay SHA-512 trong lần review P3 này.

## MAJOR

### M1 — Audit chấp nhận dữ liệu sai rồi publish thành công

Vị trí: `14-projects/p3-batch-pipeline/src/main/scala/vn/scalaeng/p3/Job.scala:52`, `:57`, `:59`, `:61`, `:130`; `14-projects/p3-batch-pipeline/README.md:17`, `:78`.

Probe dùng chính callback tamper của Job.run, sửa staging qua đường dẫn tạm khác để không đọc/ghi đè cùng lineage:

- Fact: mọi unit_price_vnd=1, revenue_vnd=0; giữ count và khóa.
- Daily: segment=WRONG, orders/order_lines=999; giữ nguyên tổng revenue.
- Campaign: campaign=WRONG, converted_users=-999, assisted_revenue=0; giữ tổng clicks.
- Rejected orders: thay bằng bảng cùng schema nhưng **0 dòng**, trong khi counts vẫn ghi 3 rejected.

Kết quả thật:

```text
audit bypass published: code=0 factRevenue=[0] dailyRevenue=[21900000]
rejectedOrders=0 publishedLogs=3
```

Audit chỉ kiểm tra tổng daily, tổng clicks, fact count/uniqueness và một chiều converted≤users. Nó không so nội dung fact, không tính daily từ fact đọc lại, không kiểm tra grain daily/campaign, số âm/rate và không đọc lại bảng rejected/conflicts. Hai phương trình đối soát so các **số tính trong bộ nhớ**, không chứng minh dữ liệu cách ly đã lưu đủ. Do đó write–audit–publish hiện chặn được một số lỗi (append trong suite), chưa thực hiện đủ hợp đồng data quality mà người học được yêu cầu tin.

Đề xuất: đối soát multiset fact kỳ vọng–staging hoặc digest có hợp đồng rõ; tính lại daily từ fact staging và so theo grain đầy đủ; kiểm tra campaign theo khách-ngày-chiến dịch với click đã dedup, uniqueness/range; đọc lại rejected/conflicts và so số dòng/nội dung. Thêm chính các mutation giữ nguyên tổng trên thành regression. Không thay bằng nhiều kiểm tra tổng tương tự vì chúng vẫn có cùng điểm mù.

### M2 — Publish thất bại để lại trạng thái trộn sau job, xóa staging cần phục hồi

Vị trí: `src/main/scala/vn/scalaeng/p3/Publish.scala:38`, `:39`, `:40`; `Job.scala:138`, `:140`, `:142`, `:156`; `README.md:79`, `:84`.

README đã công bố **cả lần publish không nguyên tử**; đây không phải yêu cầu biến local rename thành transaction. Lỗi là code/README chỉ mô tả cửa sổ “vài ms”, không có đường xử lý khi một move thất bại giữa chừng.

Probe: publish mẫu tới ngày 15 thành công; lần ngày 16 tạo sẵn đích trash của daily là thư mục không rỗng để mô phỏng xung đột đích. Audit staging vẫn qua, fact được thay trước, daily lỗi AccessDeniedException:

```text
code=1 O1fact=List([REFUNDED]) dailyTotal=[21600000] stagingExists=false
```

Fact là bản ngày 16, daily vẫn là bản ngày 15 (lẽ ra tổng mới 21.900.000). Staging bị `finally deleteTree` xóa dù publish mới làm một phần. Không có journal ghi partition nào đã thay; log published chỉ phát **sau khi cả ba bảng và các bảng lỗi đã chuyển xong**, nên lần lỗi này không có bản đồ tiến độ. Chạy lại **cùng run-id** tiếp tục code 1 vì đụng `_trash/<id>/fact_order_lines/day=...` của lần trước.

Đề xuất: preflight xung đột/path/quyền trước move; nhật ký trạng thái từng bước và rollback/roll-forward có thể chạy lại; giữ staging/trash có ownership khi publish lỗi, chỉ dọn sau khi có quyết định phục hồi. Tốt hơn là publish snapshot qua con trỏ/version/table format nếu cần consistency nhiều bảng. Nếu giữ thiết kế đơn giản, bắt buộc có runbook phục hồi và test lỗi rename ở từng bước, ID dùng lại, đọc sau failure; nói rõ trạng thái trộn có thể tồn tại tới khi sửa, không phải cam kết vài ms. Bảng lỗi hiện delete serving rồi move cũng có cửa sổ mất dữ liệu riêng.

[JDK 21 Files.move](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#move(java.nio.file.Path,java.nio.file.Path,java.nio.file.CopyOption...)) bảo đảm một move khi filesystem hỗ trợ ATOMIC_MOVE, không bảo đảm chuỗi move hay rollback (tra 17/10/2026).

### M3 — Scope lấy từ các ngày vẫn còn input hợp lệ không dọn được ngày bị rút hoàn toàn

Vị trí: `Job.scala:137`; `Publish.scala:42`; `README.md:16`, `:75`, `:80`.

Probe tối thiểu hai ngày, mỗi ngày một đơn PAID=100 và một click, publish tới ngày 16 thành công. Sau đó sửa lô ngày 15 thành CSV chỉ header và JSONL rỗng, giữ nguyên lô 16. Chạy lại cùng run-date 16:

```text
code=0
manifest: factLines=1, clicksKept=1, paidRevenueVnd=100, status=SUCCEEDED
disk: factLines=2, dailyTotal=200
```

Ngày 15 không còn trong accepted của cả hai nguồn → nằm ngoài scope → partition cũ vẫn được phục vụ. Test orphan O6 hiện tại qua vì ngày đó **vẫn có** accepted orders đang conflict và accepted clicks; chưa bao phủ retract/delete, sửa ngày hay toàn bộ ngày chuyển sang rejected. Đây là trường hợp cần biết trạng thái cũ, không thể suy chỉ từ tập ngày mới không rỗng.

Đề xuất: scope explicit theo hợp đồng recompute (khoảng ngày, manifest partition của lần trước + dữ liệu mới, tombstone/retract), tính riêng từng bảng khi cần. Audit sau publish hoặc manifest snapshot phải phát hiện thừa/thiếu partition trong phạm vi. Quy định giảm run-date/backfill: snapshot tới ngày D hay cập nhật phạm vi D, tránh report một tổng nhưng người đọc toàn bảng thấy tổng khác. Thêm regression rút trọn một ngày và đổi ngày của tất cả record.

### M4 — Fact/daily/campaign rỗng hợp lệ làm audit lỗi suy schema

Vị trí: `Publish.scala:26`; `Job.scala:53`, `:54`, `:55`; `README.md:54`, `:57`.

Probe với đủ lô ngày chạy, max-reject-rate=1 và output mới:

| Input | Kết quả thật |
|---|---|
| Một đơn REFUNDED, một click hợp lệ | code 1, daily staging không có file để suy schema |
| Một đơn quantity=0, một click hợp lệ | code 1, fact staging rỗng |
| Một đơn PAID, hai click cùng event_id nhưng khác campaign (toàn click conflict) | code 1, campaign staging rỗng |

Cả ba báo `[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Parquet. It must be specified manually. SQLSTATE: 42KD9`; serving chưa xuất hiện. Dữ liệu không lộ ra như Ch24, nhưng policy vận hành bị sai: nguồn có record, gate qua, các ngăn đều đếm được, pipeline vẫn thất bại bất ngờ chỉ vì tập kết quả rỗng.

Đề xuất: schema tường minh cho audit/bảng rỗng và policy empty result; kiểm tra được số 0, xóa partition cũ trong scope nếu kết quả mới rỗng. Không coi header-only thiếu lô giống mọi trường hợp không có PAID/click kept. Test cả output mới và output đang phục vụ.

### M5 — Hợp đồng timestamp/ngày rộng hơn đặc tả, ngày “UTC” phụ thuộc session ngoài

Vị trí: `src/main/scala/vn/scalaeng/p3/Sources.scala:46`, `:51`, `:102`, `:103`; `README.md:39`, `:41`, `:43`.

`try_cast(... AS DATE/TIMESTAMP)` kiểm tra parse được, không kiểm tra cú pháp đầy đủ mà lý do lỗi và đặc tả hứa. Probe P3 `LexicalCheck` chạy riêng xác nhận cả order day/updated_at và click ts chỉ có năm `2026` đều được nhận, errors rỗng, day=2026-01-01. Probe timezone độc lập với `ts=2026-10-15T23:30:00Z`, qua validate nhận session từ ngoài:

```text
UTC=2026-10-15 Vietnam=2026-10-16
```

`to_date(ts)` dùng timezone phiên, không tự UTC. Main qua Job.session hiện chốt UTC đúng, nên ca chạy chính trong README không bị lệch; API nhận SparkSession bên ngoài chưa đặt/kiểm tra tiền điều kiện này. Timestamp thiếu offset còn có thể đổi thứ tự phiên bản theo timezone.

Đề xuất: contract cú pháp timestamp đầy đủ/offset hoặc policy giờ địa phương cố định; kiểm tra cú pháp lẫn parse, chốt TIMESTAMP_LTZ nếu cần instant. Đặt/kiểm tra UTC tại job boundary hoặc ghi tiền điều kiện rõ; thêm regression near-midnight, offsets tương đương, timestamp/ngày rút gọn. [Spark data types](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html), [SparkDateTimeUtils 4.2.0](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/SparkDateTimeUtils.scala) và [JSON datasource](https://spark.apache.org/docs/4.2.0/sql-data-sources-json.html) phân biệt parsing/timezone/schema (tra 17/10/2026).

### M6 — Main tạo Spark trước validate; finalization có thể ghi đè manifest/thoát ngoài mã lỗi

Vị trí: `Job.scala:72`, `:76`, `:156`, `:158`, `:177`; `README.md:54`.

Giống Ch26 M1/M4, main khởi tạo session trước Config.parse nên lời hứa “cấu hình sai, không chạm Spark” chưa đúng ở entrypoint production. Manifest dùng run-id do người dùng nhập và REPLACE_EXISTING; cùng ID thay mất lịch sử. Lỗi dọn staging hoặc ghi manifest nằm ngoài khối catch ánh xạ mã 1/3/4; có thể khiến Job.run ném exception sau khi serving đã thay xong. Probe M2 đã cho thấy ID dùng lại còn đụng trash và tiếp tục thất bại; không có ownership/checkpoint để biết khôi phục từ đâu.

Đề xuất: parse trước session factory; execution ID duy nhất với policy tái chạy rõ; finalization có error/status riêng và không phá dữ liệu cần phục hồi. Thêm test main bằng session factory/subprocess, collision manifest/staging/trash và lỗi telemetry sau data commit. Bằng chứng Windows spark-submit khởi tạo trước parse đã có ở review T16 trên Job tương ứng; với P3 đây là phát hiện từ source, chưa chạy lại subprocess P3.

## MINOR

### m1 — Generator sinh lại trên thư mục cũ giữ lô ngoài Summary, ID dòng lỗi đụng dòng khác

Vị trí: `src/main/scala/vn/scalaeng/p3/Generator.scala:17`, `:29`, `:47`, `:78`; `README.md:114`, `:134`.

Probe generate 3 ngày rồi generate 1 ngày cùng seed vào cùng đường dẫn:

```text
first=Summary(3,610,906,50) second=Summary(1,202,302,50)
dirs=List(ingest_date=2026-10-15, ingest_date=2026-10-16, ingest_date=2026-10-17)
```

Thư mục vẫn ba lô; pipeline có thể đọc dữ liệu cũ dù Summary nói một ngày. Trong dữ liệu sinh, raw key `(O406,KB-01)` có hai giá trị quantity khác nhau vì nhánh dòng hỏng dùng `orderSeq+1` mà không tăng counter. Dòng hỏng bị validation loại nên chưa làm sai tổng accepted hiện tại, nhưng vô tình tạo collision ngoài các lỗi generator đã công bố.

Đề xuất: yêu cầu output mới/rỗng và từ chối ghi đè, hoặc thiết kế regenerate nguyên snapshot có manifest/phạm vi rõ (không xóa tùy tiện thư mục người dùng); cấp ID trước khi chọn valid/invalid branch. Nêu byte-for-byte determinism cho output mới. `clicksPerDay=0` được API cho phép nhưng không thể qua gate hai nguồn — ghi rõ đó là fixture âm hoặc giới hạn tham số.

### m2 — Click conflict chỉ được đếm, không có bảng để kiểm tra/sửa

Vị trí: `Job.scala:112`, `:120`, `:127`; `README.md:42`, `:69`.

Clicks.dedup trả DataFrame conflict nhưng run chỉ `.count()`, không stage/publish payload nào, trong khi order_conflicts được lưu. Manifest đủ đối soát **số**, nhưng không hỗ trợ truy vết nội dung click conflict như order conflict. Đề xuất lưu click_conflicts hoặc nói rõ đây là ngăn chỉ đếm, nguồn thô phải giữ để điều tra; thêm vào sơ đồ/audit/test nếu chọn lưu.

### m3 — Suite integration dùng timeout 30 giây cho nhiều lần chạy Spark

Vị trí: `src/test/scala/vn/scalaeng/p3/PipelineSuite.scala:106`, `:176`; `README.md:5`, `:191`.

Lượt đầu có hai timeout thật dù chưa phát hiện assertion sai; đây là độ mong manh của kiểm chứng, không tự chứng minh logic pipeline sai. Đề xuất timeout integration phù hợp và/hoặc tách các lượt e2e thành ca độc lập, tránh để kết quả phụ thuộc máy nhanh hơn 30 giây; ghi kết quả reviewer riêng với lần 11/11 của tác giả. Kết quả chạy lại được bổ sung ở phần kiểm chứng.

### m4 — Khẳng định SortMergeJoin và test nhận diện join cần giới hạn phạm vi

Vị trí: `src/main/scala/vn/scalaeng/p3/Reports.scala:37`; `src/test/scala/vn/scalaeng/p3/PipelineSuite.scala:91`; `README.md:96`.

Code comment “hai bảng lớn không broadcast → SortMergeJoin” quá rộng: AQE có thể chọn ShuffledHashJoin theo cấu hình. Test nhánh dữ liệu nhỏ chỉ assert toàn plan contains BroadcastHashJoin, trong khi join customers **luôn** có loại này, nên chưa chứng minh join click–revenue broadcast. Plan sort-merge in ra trong lượt hiện tại đúng; không coi đó là luật của mọi cấu hình/kích thước.

Đề xuất: gọi đây là plan quan sát dưới cấu hình đã ghi; tìm đúng join theo key/customer-day và build side trong final executed plan, tách khỏi join customers; thêm kiểm tra AQE/hint có điều kiện. [Spark 4.2.0 performance tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html) mô tả các chiến lược này (tra 17/10/2026).

### m5 — Bài tập last-click cần policy cho đơn không có click trước đó

Vị trí: `README.md:220`.

Nếu chỉ gán cho click gần nhất trước đơn trong ngày, đơn không có click phù hợp không thể tự có campaign. Sample có doanh thu C9 ngày 15=450.000 nhưng click C9 ở ngày 16, nên không thể khẳng định tổng doanh thu gán **cho click/campaign có thật** luôn bằng doanh thu thật. Đề xuất thêm bucket UNATTRIBUTED/no-click hoặc đổi invariant thành attributed + unattributed = actual; chốt tie-break click cùng instant và thời điểm đặt hàng thay vì dùng updated_at.

### m6 — Handoff chưa phản ánh P3 và một số nhãn sư phạm quá rộng

Vị trí: `.team/handoffs/T14-ch24-batch-pipeline.md:18`; `.team/handoffs/T16-ch26-spark-production.md:3`; `README.md:39`, `:205`; `Sources.scala:44`.

T14 vẫn ghi “Chưa làm Project 3”, T16 không liệt kê P3 hay bàn giao build/test/version của nó. Lead nên bổ sung phần bàn giao P3 với lệnh/nguồn/giới hạn thật trước chốt. Ngoài ra `raw` là struct giá trị CSV/JSON đã parse, không phải nguyên văn byte/quote/line ending; và một seed ngẫu nhiên 3 ngày không chứng minh “bất biến đúng với input bất kỳ” (các phản ví dụ M1–M4 đã bác phạm vi đó). Đề xuất gọi đây là regression trên fixture/seed cụ thể và phân biệt record parser trả ra với dòng vật lý.

## Các phần đã kiểm tra đúng và giới hạn

- NULL key/timestamp: probe orders có 1 accepted + 2 rejected, clicks 1 accepted + 1 rejected; không mất dòng do NULL equality trong dedup sau validate. Dimension C1 trùng bị require từ chối trước staging. Join clickers đã aggregate theo khách-ngày-campaign, paid theo khách-ngày, nên nhiều dòng hàng không nhân số click; golden test xác nhận multi-line O1 và multi-campaign C1.
- Test audit fail bằng append thêm fact trả 4 và daily phục vụ cũ giữ nguyên; orphan O6 do xung đột được dọn đúng trong ca test, campaign ngày đó giữ vì còn click. Không phủ nhận các ca đã qua này; vấn đề là contract rộng hơn coverage.
- [Spark CSV](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html), [JSON](https://spark.apache.org/docs/4.2.0/sql-data-sources-json.html), [JOIN](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-join.html), [ANSI](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html) khớp RAW schema/null/left join/decimal ở miền được test. Bản ghi lỗi parser/try_cast được tách có lý do; decimal vượt Long được chốt bằng thiết kế giống Ch24, không xem DECIMAL là vô hạn. Ngày tra: 17/10/2026.
- Script submit đúng thứ tự option/jar/application args, Spark Provided và loại Scala khỏi jar phù hợp [Submitting applications 4.2.0](https://spark.apache.org/docs/4.2.0/submitting-applications.html). P3 chủ động giới hạn local file publish và để S3 thành bài mở rộng, phù hợp phạm vi đã nói; chưa xác minh MinIO/S3, catalog/table transaction, concurrent writer, crash vật lý hoặc cluster nhiều máy.
- Bài tập mở rộng là yêu cầu thiết kế, P3 không có bộ lời giải riêng được giao; không đánh dấu chúng đã chạy. Hai lỗi compiler mục 9 chỉ có message rút gọn, chưa tái hiện riêng trong review này. Sau sửa cần chạy lại regression mutation/publish failure/retract/empty và lead kiểm chứng triển khai cuối.


## V?ng 2

Reviewer: Codex ? Ng?y: 17/10/2026 ? **CH?A ??T ? c?n 2 MAJOR (M2, M5), 0 BLOCKER, 0 MINOR.**

?? ??c handoff P3 V?ng 2, README (??c bi?t runbook m?c 9), source/test m?i v? ch?y l?i c?c nh?m probe ???c giao. Gi? nguy?n v?ng 1. Ch? ghi review v? `.cache/`, kh?ng s?a project/file chung, kh?ng sbt/commit.

### Ki?m ch?ng l?i

Runner `.cache/run-review.ps1 -Part p3`; m?i l?nh d?ng **`--server=false`**, `COURSIER_CACHE=.cache/coursier`; workspace, TEMP/TMP, java.io.tmpdir, Spark local v? warehouse d??i `.cache/review-p3/`, nh? v?ng 1. Spark **4.2.0**, Scala **2.13.18**, JDK **21.0.12.1**, munit **1.2.4**, local[2], ANSI=true. Probe d?ng `Job.session()` ?? t?t artifact isolation; kh?ng c?n JVM Spark c?a reviewer ch?y c?c ph?n A?C khi b?t ??u P3.

| L?nh Scala CLI qua runner | K?t qu? th?t | Log trong `.cache/review-p3/` |
|---|---|---|
| `test .` t?i `14-projects/p3-batch-pipeline` | exit 0; **18 pass, 0 fail, 0 ignored**, 331,385s | `v2-project.log` |
| `run . --main-class ProbeV2` t?i `.cache/review-p3/probe` | exit 0; ch?y l?i audit bypass, publish l?i b??c 3, c?ng run-id, r?t ng?y, ba k?t qu? r?ng, NULL/dimension/timezone/generator | `v2-attack.log` |
| `run . --main-class LexicalCheck` c?ng cwd | exit 0; ng?y/timestamp ch? n?m b? lo?i ? c? hai ngu?n | `v2-lexical.log` |
| `run . --main-class TypeCheck` c?ng cwd | exit 0; LTZ/NTZ thay ??i ng?y click v? dedup | `v2-type.log` |
| `run . --main-class TypeJobProbe` c?ng cwd | exit 0; Job.run tr? 0 nh?ng publish sai ng?y/conversion d??i NTZ | `v2-type-job.log` |
| `run . --main-class JournalProbe` c?ng cwd | exit 0; l?i ghi journal sau move kh?ng th?nh PublishFailed/kh?ng rollback | `v2-journal.log` |
| `run . --main-class JournalJobProbe` c?ng cwd | exit 0; Job.run th?c t? tr? 1, serving tr?n v? staging b? x?a | `v2-journal-job.log` |

V? d?: `powershell -NoProfile -ExecutionPolicy Bypass -File .cache/run-review.ps1 -Part p3 -Source .cache/review-p3/probe -Mode run -Main ProbeV2 -Name v2-attack`. Probe g?c gi? t?i `Probe.scala.v1`. B?n V2 th?ch nghi execution_id/manifest m?i, kh?ng ??c serving khi audit ?? ch?n, v? d?ng beforeMove ?? ti?m l?i b??c 3 v? ???ng trash c? theo run-id kh?ng c?n ??ng. M?t unused parameter warning thu?c probe reviewer (c?ng th? m?c compile), kh?ng thu?c source t?c gi?. WARN native-hadoop/winutils kh?ng c?n ghi/??c Parquet; ch? Vi?t ? m?t s? log Java l?i encoding.

### C?c s?a ??i ?? ???c x?c nh?n

- **M1:** mutation gi? t?ng v?ng 1 nay tr? **4**, `servingExists=false`. Audit b?o fact l?ch 12 d?ng, daily l?ch 8 d?ng/grain tr?ng, campaign l?ch 6 d?ng/grain tr?ng/3 d?ng ngo?i mi?n v? rejected_orders 0 != 3. So fact v?i expected ??c l?p, t?nh l?i aggregate t? fact ??c l?i, ki?m ?? b?n b?ng c?ch ly b?ng count ??ng h?p ??ng m?i. Suite c?n ki?m t?ng mutation ri?ng.
- **M2, l?i move th?ng th??ng:** baseline ng?y 15 tr? 0; l?i tr??c move th? 3 tr? 1 v?i `publish_failed_rolled_back`, O1 v?n PAID v? daily v?n **21.600.000** (??ng b?n tr??c). Ch?y l?i **c?ng run-id** tr? 0. Nh?n probe c? ?leaves mixed data? ???c gi? ?? ??i chi?u l?ch s?; gi? tr? hi?n t?i cho th?y ca n?y ?? s?a, kh?ng c?n l? b?ng ch?ng l?i.
- **M3:** r?t tr?n ng?y 15 kh?i c? orders/clicks r?i ch?y ng?y 16 tr? 0; **diskTotal=100, factLines=1**, manifest c?ng factLines=1/clicksKept=1/paidRevenueVnd=100. Kh?ng c?n partition/doanh thu ng?y 15 c? trong ba b?ng partitioned.
- **M4:** ch? REFUNDED, m?i orders b? lo?i, m?i clicks conflict ??u **code=0**, kh?ng l?i suy schema. Empty staging ???c audit v?i schema t??ng minh.
- **M5, ph?n ?? s?a:** LexicalCheck cho day/updated_at/ts r?t g?n th?nh null k?m l?i; test suite ki?m timestamp thi?u offset. `Job.runConfigured` ??t UTC t?i boundary n?n session ngo?i Asia/Ho_Chi_Minh ???c chu?n h?a. Probe g?i ri?ng `Clicks.validate` v?n c? UTC=15/Vietnam=16 l? h?nh vi timezone c?a h?m c?p th?p, kh?ng d?ng n? ?? k?t lu?n boundary job ch?a s?a. C?n alias timestampType b?n d??i.
- **M6:** main d?ng launch parse tr??c session; suite c? factory fail n?u b? g?i; execution_id t?ch kh?i run-id cho staging/trash/journal/manifest, suite v? probe c?ng-ID qua. Finalization ghi manifest ???c b?c mapping l?i. Kh?ng ch?y l?i spark-submit ? v?ng n?y.
- **m1?m6:** generator t? ch?i th? m?c c? (probe l?n hai nh?n IllegalArgumentException), kh?ng c?n raw key v?i quantity kh?c do ID d?ng h?ng trong fixture ?? d?; click_conflicts ?? stage/audit/publish; timeout 300s; test t?m join theo customer_id/day, README g?i plan quan s?t; last-click c? UNATTRIBUTED; handoff P3 ?? ?? v? nh?n raw/seed ???c thu h?p. Kh?ng c?n finding MINOR t? s?u m?c v?ng 1.

### MAJOR

**R2-M2 ? L?i ghi journal sau move b? qua rollback v? l?m Job x?a staging c?n ph?c h?i.**

V? tr?: `14-projects/p3-batch-pipeline/src/main/scala/vn/scalaeng/p3/Publish.scala:71`, `:72`, `:73`, `:76`, `:84`; `src/main/scala/vn/scalaeng/p3/Job.scala:218`, `:232`, `:235`; `README.md:255`, `:259`.

Th? t? hi?n t?i: `Files.move` ? ghi DONE ? th?m move v?o `done`. N?u ghi DONE l?i, move v?a th?nh c?ng ch?a c? trong danh s?ch rollback. H?n n?a catch ghi FAILED **tr??c** khi v?o rollback; n?u journal ti?p t?c kh?ng ghi ???c, ch?nh record(FAILED) n?m exception th??ng v? tho?t kh?i Publish.execute, kh?ng t?o PublishFailed.

Probe filesystem ch?n l?i journal ? b??c 2 b?ng c?ch gi? b?n journal tr??c ?? d??i t?n `.saved`, thay ???ng journal b?ng th? m?c ?? Files.write nh?n AccessDeniedException. C?c move d? li?u v?n l? Files.move th?t; kh?ng mock k?t qu?:

```text
JOURNAL failure=AccessDeniedException publishFailed=false
JOURNAL serving=NEW trash=OLD stagedExists=false
```

Probe qua **Job.run** v?i d? li?u ??n/click th?t c?ng t?i hi?n:

```text
JOB-JOURNAL baseline=0
JOB-JOURNAL code=1 facts=List([X,REFUNDED]) daily=[100] staging=List() trash=List(<execution_id>)
```

Ch? c? event `job_failed`, kh?ng `publish_partial`. Serving fact ?? l? b?n REFUNDED m?i, daily v?n 100 c?; staging ch?a fact Y ng?y 16 v? c?c b?ng ch?a chuy?n b? finally x?a. Source ch? b?t keepStaging khi b?t ???c PublishFailed(rolledBack=false), n?n l?i journal r?i ra ngo?i ???ng b?o to?n ??. B?n `.saved` ch? ghi move 1, kh?ng c? DONE cho move 2 ?? x?y ra; n? ???c reviewer gi? ?? quan s?t, kh?ng ph?i kh? n?ng ph?c h?i c?a code.

**??nh gi? runbook m?c 9:** d?ng writer kh?c v? ??o c?c DONE ch?a UNDONE theo th? t? ng??c l? h?p l? **khi journal ??y ??** v? nh?nh PUBLISH_PARTIAL gi? artifacts. Nh?ng ca tr?n kh?ng c? log k?ch ho?t runbook, kh?ng c? DONE cho move cu?i v? staging ?? m?t. V? v?y ch?a th? d?ng runbook hi?n t?i ?? b?o ??m ph?c h?i m?i l?i publish nh? m? t?.

**?? xu?t:** ghi intent b?n tr??c khi move v? k?t qu? sau move; c?p nh?t tr?ng th?i rollback trong b? nh? ngay sau move, tr??c c?c thao t?c journal c? th? n?m l?i. Ghi FAILED/UNDONE l?i kh?ng ???c ng?n n? l?c rollback; m?i failure sau khi publish b?t ??u ph?i tr? tr?ng th?i c? c?u tr?c, gi? staging/trash khi ch?a ch?ng minh ph?c h?i xong. COMMITTED hi?n c?ng n?m ngo?i catch rollback, c?n policy r? n?u ghi marker cu?i l?i. Runbook c?n h??ng d?n reconcile intent/result v? ki?m filesystem khi journal ??t, s?a nguy?n nh?n tr??c khi l?p move. Th?m regression l?i ghi DONE, FAILED, UNDONE v? COMMITTED qua Job.run, kh?ng ch? l?i callback tr??c move.

**R2-M5 ? ??t UTC ch?a kh?a ki?u timestamp instant; NTZ v?n l?m sai ng?y/phi?n b?n.**

V? tr?: `src/main/scala/vn/scalaeng/p3/Sources.scala:27`, `:111`; `src/main/scala/vn/scalaeng/p3/Job.scala:146`; `README.md:43`.

Common.instantOf v?n cast `AS TIMESTAMP`; alias ph? thu?c `spark.sql.timestampType`. Khi timestampType=NTZ, offset h?p l? qua regex nh?ng b? b? qua. C?ng UTC v? c?ng input:

```text
TYPE TIMESTAMP_LTZ clicks=List([2026-10-16 00:30:00.0,2026-10-15,ArraySeq()])
TYPE TIMESTAMP_LTZ ordersExact=1 superseded=0
TYPE TIMESTAMP_NTZ clicks=List([2026-10-16T00:30,2026-10-16,ArraySeq()])
TYPE TIMESTAMP_NTZ ordersExact=0 superseded=1
```

Click l? `2026-10-16T00:30:00+07:00` = **17:30 UTC ng?y 15**. Hai order timestamp `00:00Z` v? `07:00+07:00` c?ng instant b? coi l? hai phi?n b?n khi NTZ. Probe ??y ?? qua Job.run v?i session ban ??u Asia/Ho_Chi_Minh + NTZ cho `TYPE-JOB code=0 zone=UTC type=TIMESTAMP_NTZ campaign=List([2026-10-16,0,0])`: boundary ?? ??i UTC, audit v?n qua, nh?ng click ??ng l? ng?y 15 (c?ng kh?ch/ng?y v?i ??n PAID=100) b? publish sang ng?y 16 v?i converted_users=0 v? assisted_revenue=0.

**?? xu?t:** cast t??ng minh `TIMESTAMP_LTZ` trong Common.instantOf ho?c t? ch?i NTZ r? r?ng ? boundary tr??c x? l?; UTC ??n l? kh?ng ??i alias NTZ. Test c? orders/clicks d??i hai gi? tr? timestampType, offset qua n?a ??m v? hai bi?u di?n c?ng instant, c? assertion ? output job. [Spark 4.2.0 Data Types](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html) x?c nh?n TIMESTAMP l? alias LTZ/NTZ c?u h?nh ???c (??i chi?u 17/10/2026).

### Gi?i h?n

Kh?ng ch?y l?i sbt/assembly, WSL/spark-submit, b?n l??t d? li?u 1,2 tri?u d?ng, cluster, S3/MinIO, hai writer ??ng th?i hay crash v?t l?. L?i journal ? ??y l? fault injection b?ng exception filesystem th?t, kh?ng ph?i ki?m ch?ng power-loss durability; ?? ?? b?c nh?nh cleanup/rollback hi?n t?i. Lead c?n s?a M2/M5, ch?y fault injection v? ki?m ch?ng tri?n khai cu?i tr??c khi ch?t P3.
