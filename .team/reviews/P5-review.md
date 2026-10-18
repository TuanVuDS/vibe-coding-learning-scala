# Review Project 5 — Production-Oriented Data Platform

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 6 MAJOR, 4 MINOR; không có BLOCKER.**

Đã đọc `.team/handoffs/P5-data-platform.md`, toàn bộ **24 file** không bị ignore trong `14-projects/p5-data-platform/`, AGENTS.md, PROMPT_PROJECT.md phần 11–14, template bài học, khuôn project và thiết kế P5 trong roadmap. README đủ các mục của **khuôn project** tại `14-projects/README.md`, cộng runbook và nguồn; không áp máy móc 17 mục của một chương. Tiếng Việt rõ, domain nối tiếp đơn hàng. Bảng trách nhiệm Scala/SQL/PostgreSQL/Dagster và phần giải thích dữ liệu muộn có giá trị sư phạm; các thay đổi so với thiết kế MinIO/Spark/P2 được công khai ở mục 4.5.

Chỉ ghi báo cáo này và `.cache/`; không sửa source/build/data của tác giả, file dùng chung hay commit git. Đã đối chiếu SHA-256 **24/24 file** giữa source gốc và bản sao dùng test: giống nhau. Các thay đổi khác đang có trong working tree không thuộc lần review này.

## Kiểm chứng thực tế và giới hạn

Chạy từ bản sao `.cache/p5-review/source/p5-data-platform/` để mọi build artifact nằm trong `.cache/`. Scala **3.9.0**, JDK **21**, munit **1.2.4**, munit-cats-effect **2.2.0**, các dependency giữ nguyên `project.scala`. Môi trường:

```powershell
$env:COURSIER_CACHE = 'C:\Users\VuTT\zero_to_hero_scala\.cache\coursier'
$env:TEMP = 'C:\Users\VuTT\zero_to_hero_scala\.cache\p5-review\tmp'
$env:TMP = $env:TEMP
$env:JAVA_TOOL_OPTIONS = '-Djava.io.tmpdir=C:\Users\VuTT\zero_to_hero_scala\.cache\p5-review\tmp'
```

| Lượt chạy | Kết quả thật | Log dưới `.cache/p5-review/` |
|---|---|---|
| `RUN_DB_TESTS=1`, `RUN_KAFKA_IT=1`; `scala-cli.bat test project.scala core ingest serving app --server=false` | **exit 1**; Core 5/5, Kafka 2/2 pass; fixture PostgreSQL thất bại trước bốn test Platform | `baseline-all.log` |
| Bỏ `RUN_DB_TESTS`, giữ `RUN_KAFKA_IT=1`; cùng lệnh trên | **exit 0**, **7 pass, 4 skipped** | `baseline-kafka.log` |
| Thêm `ReviewerCoreProbe.test.scala`, `--test-only vn.scalaeng.p5.ReviewerCoreProbe --server=false` | **exit 0, 8/8 probe pass** | `core-probes.log` |
| Thêm `ReviewerKafkaProbe.test.scala`, `--test-only vn.scalaeng.p5.ReviewerKafkaProbe --server=false` | **exit 0, 2/2 probe pass**, broker KRaft thật | `kafka-probes.log` |
| Bash harness source `orchestration/run-platform.sh`, giả lập mọi lệnh Dagster trả 23 | Script vẫn **exit 0**, ba partition đều exit 23 | `demo-failure-probe.log` |

Các probe pass nghĩa là đã tái hiện và assert hành vi ghi bên dưới, **không** có nghĩa các lỗi đã được sửa. File probe nằm cạnh `project.scala` trong bản sao, không được đưa vào source tác giả. Một lượt biên dịch probe đầu tiên bị `-Werror` vì unused import do reviewer viết; đã bỏ import và chạy lại 8/8. Source tác giả biên dịch không có compiler warning; hai hint nâng munit của Scala CLI không phải compiler warning và không cần đổi version chuẩn repo.

Output gốc đáng chú ý:

```text
Test run vn.scalaeng.p5.CoreSuite finished: 0 failed, 0 ignored, 5 total
Test run vn.scalaeng.p5.KafkaLandingSuite finished: 0 failed, 0 ignored, 2 total
java.io.IOException: Gave up waiting for server to start after 10000ms
Caused by: java.sql.SQLException: connect failed
Caused by: java.net.ConnectException: Connection refused: getsockopt
```

Lượt DB tắt có bốn dòng `skipped`; summary munit vẫn ghi `0 ignored, 4 total`, nên không tính thành bốn test pass. PostgreSQL nhúng **không khởi động được trong sandbox này**; lỗi quan sát được là timeout/connect refused, chưa có bằng chứng đủ để khẳng định cơ chế sandbox cụ thể nào chặn. Không quy lỗi fixture này thành bug của P5, cũng không xác nhận lại tuyên bố 11/11 của tác giả.

**Chưa kiểm chứng runtime:** `ServingDb.load` với PostgreSQL thật, rollback khi insert lỗi, nạp cạnh tranh/advisory lock, ngày rỗng trong DB, view tháng và API qua HTTP với DB, sbt/assembly, toàn luồng Dagster, Docker/MinIO/Spark, nhiều consumer/rebalance và kill JVM thật. Đã review tĩnh toàn bộ các nhánh DB: lock, kiểm hash, DELETE/INSERT và ghi marker đều nằm trong một `ConnectionIO.transact`; ngày rỗng vẫn đi tới ghi `serving_loads`; chưa thấy lỗi tuần tự trong thiết kế này. Lead cần chạy lại các kiểm chứng DB sau khi sửa các MAJOR.

Harness Bash chỉ dùng dịch vụ giả, mock `rm`/`kill`/`wait`, không khởi động PostgreSQL/API hay xóa thư mục. Lượt đầu harness gặp vấn đề đường dẫn Windows tuyệt đối; kết quả nêu trên là lượt sửa đường dẫn sang tương đối và chạy xong.

## BLOCKER

Không có.

## MAJOR

### M1 — CSV không escape làm mất doanh thu của sự kiện hợp lệ, quality gate vẫn pass

Vị trí: `14-projects/p5-data-platform/core/src/main/scala/vn/scalaeng/p5/Assets.scala:52`, `:76`; `core/src/main/scala/vn/scalaeng/p5/Orchestrator.scala:92`, `:99`; `core/src/main/scala/vn/scalaeng/p5/Model.scala:49` (các đường dẫn rút gọn trong mục này cùng gốc P5).

Probe gửi một sự kiện PAID, `order_id="O,comma"`, C1, quantity=1, price=100. Contract và validate đều chấp nhận. Staging nội suy trực tiếp ID vào CSV nên tạo tám cột; `DailyRevenue.compute.collect` âm thầm bỏ dòng không khớp đúng bảy cột. Quality gate dùng lại chính phép tính có lỗi và chỉ kiểm uniqueness, không đối chiếu số PAID bị rơi:

```text
staging row: O,comma,C1,PAID,SKU,1,100,2026-10-15T01:00:00Z
daily=Vector() gate=List()
counts: rows=1 kept=1 rejected=0 duplicate_events=0 superseded=0 conflicts=0
```

Đây là mất doanh thu ngay trên đường vào được hỗ trợ, không cần sửa tay artifact. Các field chuỗi khác có dấu phẩy/xuống dòng cũng cùng rủi ro.

**Đề xuất:** dùng JSONL hoặc CSV codec có quoting/escaping cho mọi bước ghi/đọc; nếu domain chủ ý cấm ký tự đó thì validate và tài liệu hóa tại cửa ngõ. Thay `collect` bỏ schema sai bằng lỗi có cấu trúc, đồng thời đối soát PAID/aggregate độc lập. Thêm regression cho dấu phẩy, quote và newline; gate phải chặn hoặc giữ đúng 100 VND.

### M2 — Cùng event_id nhưng khác payload bị coi là duplicate, che mất xung đột

Vị trí: `14-projects/p5-data-platform/core/src/main/scala/vn/scalaeng/p5/Assets.scala:41`, `:42`; `14-projects/p5-data-platform/README.md:49`, `:50`.

Probe dùng hai sự kiện cùng event_id/order_id/sku/updated_at, chỉ khác giá 100 và 900. `byEvent.map(_.head)` loại một bản trước khi kiểm xung đột phiên bản. Đảo thứ tự hai dòng trong cùng file:

```text
forward: daily=Vector(Retail,100,1,1)
reverse: daily=Vector(Retail,900,1,1)
cả hai: rows=2 duplicate_events=1 conflicts=0 kept=1 gate=List()
```

Vì thế payload lỗi của producer có thể quyết định doanh thu tùy thứ tự file/lô; quy tắc “nhiều bản mới nhất khác nội dung → xung đột” không còn bảo vệ được. Contract hiện chỉ kiểm shape/type, chưa thực thi giả định event_id luôn đại diện cùng nội dung.

**Đề xuất:** chỉ dedup các bản cùng event_id có cùng toàn bộ nội dung sự kiện chuẩn hóa, kể cả business key và timestamp. Nếu một ID ánh xạ nhiều nội dung, đưa cả nhóm vào nhánh conflict/quarantine có bộ đếm đối soát, không chọn `.head`. Test hoán vị thứ tự, trùng ID khác key và cùng ID khác timestamp.

### M3 — Quy tắc ngày của unassigned và staging khác nhau, một số dòng biến mất khỏi mọi đối soát

Vị trí: `14-projects/p5-data-platform/core/src/main/scala/vn/scalaeng/p5/Assets.scala:32`, `:88`; `core/src/main/scala/vn/scalaeng/p5/Model.scala:40`, `:44`.

`rowsOf` so chuỗi với `day.toString`; validate yêu cầu đúng bốn chữ số năm; `unassigned` chỉ dùng `LocalDate.parse`. Probe gửi `day="+02026-10-15"`, shape hợp lệ:

```text
LocalDate.parse: Success(2026-10-15)
Order.validate: Left(day: phải là ngày yyyy-MM-dd)
staging counts: rows=0 rejected=0 kept=0
unassigned=Vector() gate=List()
```

Dòng không được đưa vào staging ngày 15 để bị reject, cũng không vào unassigned vì Java parse thành công. Đối soát tổng raw bị thủng ngay cả khi gọi utility thủ công.

**Đề xuất:** dùng chung một bộ phân loại ngày theo contract cho validate, partition routing và unassigned. Mỗi raw row phải thuộc đúng một ngày chuẩn hoặc một nhóm unassigned có lý do. Thêm phép kiểm tổng raw = tổng rows đã gán + unassigned, với ngày sai định dạng nhưng Java parse được, ngày không tồn tại và chuỗi rỗng.

### M4 — unassigned chỉ xuất hiện trong test; luồng vận hành không báo các dòng ngày sai

Vị trí: `14-projects/p5-data-platform/core/src/main/scala/vn/scalaeng/p5/Assets.scala:85`; `app/src/main/scala/vn/scalaeng/p5/Main.scala:46`; `serving/src/main/scala/vn/scalaeng/p5/Serving.scala:77`; `README.md:53`, `:244`.

`rg` toàn bộ Scala/Python P5 chỉ tìm thấy **một caller** của `Reconcile.unassigned`: `CoreSuite.scala:49`. Không có CLI, asset, quality report hay metric gọi nó. Fixture thật chứa E1506, ngày `2026-13-01`; test biết có một dòng unassigned nhưng quy trình Dagster/CLI vẫn thành công và không công bố dòng này ở bất kỳ báo cáo chất lượng nào. Một utility tồn tại trong source chưa đáp ứng tuyên bố “đếm riêng” trong vận hành.

**Đề xuất:** thêm một bước reconcile cấp raw hoặc tích hợp vào report/gate hiện có, xuất số lượng và vị trí/lý do; nêu rõ policy cho phép hay chặn publish khi unassigned > 0. Không nhất thiết phải chặn mọi ngày, nhưng cần bằng chứng vận hành quan sát được. Test qua entry point chạy thật rằng fixture báo một unassigned, không chỉ gọi helper trong test.

### M5 — Script demo xóa đệ quy WORK do người dùng truyền mà không kiểm tra phạm vi

Vị trí: `14-projects/p5-data-platform/orchestration/run-platform.sh:7`, `:10`; `14-projects/p5-data-platform/README.md:122`.

Kiểm `${WORK:?đặt WORK}` chỉ loại chuỗi rỗng. Ngay sau đó script chạy `rm -rf "$WORK"` trước khi kiểm dependency hay artifact. Nếu WORK trỏ vào repo, thư mục dữ liệu hoặc một thư mục có sẵn, nội dung đó bị xóa mà không có kiểm tra ownership/phạm vi. Đây là đường lệnh trực tiếp, không phải lỗi giả định trong thư viện. **Không chạy phép xóa thật**; harness đã thay `rm` bằng lệnh chỉ ghi log.

**Đề xuất:** tạo thư mục con riêng bằng `mktemp -d` dưới WORK, chỉ dọn đúng thư mục do lần chạy đó tạo. Nếu cần tái sử dụng workspace, yêu cầu marker ownership và kiểm tra đường dẫn đã canonicalize, từ chối root/home/repo và thư mục không thuộc script. Không dùng `rm -rf` với chính đường dẫn đầu vào tùy ý.

### M6 — Demo trả exit 0 dù mọi lần materialize Dagster đều thất bại

Vị trí: `14-projects/p5-data-platform/orchestration/run-platform.sh:6`, `:22`, `:36`, `:47`.

`set -uo pipefail` không có `-e`; script in mã lỗi rồi tiếp tục, cuối cùng trả mã của cleanup/wait. Harness source **nguyên script**, mock Dagster trả 23 cho mọi lần gọi, mock API để không mở dịch vụ, cho kết quả:

```text
partition 2026-10-15: exit=23 0 asset được tính
partition 2026-10-16: exit=23 0 asset được tính
partition 2026-10-17: exit=23 0 asset được tính
MOCK_SCRIPT_EXIT=0 (all Dagster calls return 23)
```

Một lệnh dùng làm “kiểm chứng toàn luồng” có thể báo thành công cho caller/CI khi pipeline không tạo asset nào.

**Đề xuất:** kiểm mã trả về từng bước quan trọng, dừng hoặc tích lũy lỗi rồi trả nonzero; dùng cleanup trap giữ nguyên exit status và xử lý đúng process chưa được tạo. Kiểm HTTP readiness bằng status thành công (`curl -f` hoặc tương đương), không chỉ kết nối được. Thêm kiểm chứng giả lập một asset và một lệnh API thất bại.

## MINOR

### N1 — API thiếu tham số trả 404; giới hạn “366 ngày” thực tế nhận 367 ngày dữ liệu

Vị trí: `14-projects/p5-data-platform/serving/src/main/scala/vn/scalaeng/p5/Serving.scala:88`, `:90`, `:92`.

Probe gọi `Api.routes.orNotFound` trực tiếp, dùng DB cố ý không hoạt động để phân biệt validate với truy vấn:

```text
/api/v1/revenue/daily                                      => 404
/api/v1/revenue/daily?from=2026-10-15                      => 404
?from=bad&to=2026-10-15                                   => 400
?from=2026-01-01&to=2027-01-02                            => đi tới DB query
?from=2026-01-01&to=2027-01-03                            => 400
```

Khoảng đầu có hiệu 366 ngày, nhưng SQL `BETWEEN` bao gồm cả hai đầu nên có 367 ngày. Query lặp cũng lấy giá trị đầu (`to=2026-10-15&to=bad` vẫn tới DB); hiện chưa có policy ghi rõ cho trường hợp này.

**Đề xuất:** bắt route trước rồi validate required/optional params để thiếu tham số trả 400 có JSON error. Chốt giới hạn là độ dài khoảng bao gồm hai đầu hay hiệu ngày, sửa thông báo/code thống nhất. Quy định rõ query param lặp và test các biên. Readiness khi DB không hoạt động đã trả đúng **503** trong probe.

### N2 — Quality gate ném exception trước khi trả lỗi thiếu/malformed artifact

Vị trí: `14-projects/p5-data-platform/core/src/main/scala/vn/scalaeng/p5/Orchestrator.scala:87`, `:94`, `:99`, `:101`; `app/src/main/scala/vn/scalaeng/p5/Main.scala:33`, `:72`.

Lake chưa materialize: `Quality.check` ném `NoSuchFileException` trước khi tạo thông báo “thiếu lineage staging/daily”. Daily có header đúng nhưng dòng chỉ chứa `Retail`: ném `ArrayIndexOutOfBoundsException` thay vì trả danh sách vi phạm. Vì `Main.publish` không chuyển các lỗi này thành quality result, nhánh mã 4 không được dùng cho các trường hợp đó. Vẫn fail closed nên chưa có publish dữ liệu lỗi.

**Đề xuất:** kiểm tồn tại/schema trước khi truy cập cột và parse số an toàn; trả lỗi chất lượng có file/dòng. Phân biệt rõ lỗi dữ liệu/artifact chưa đủ với IOException hạ tầng, rồi test mã thoát tương ứng.

### N3 — Runbook schema/quarantine mô tả đường file nhưng áp cho cả Kafka

Vị trí: `14-projects/p5-data-platform/ingest/src/main/scala/vn/scalaeng/p5/Ingest.scala:35`, `:46`; `README.md:47`, `:223`, `:227`.

Probe Kafka thật xác nhận null và record sai contract được giữ trước commit, nhưng nội dung quarantine là TSV trong file mang đuôi `.jsonl`:

```text
1\t<null>
2\t{"order_id":"bad"}
```

Không có `.reason.txt` hoặc reason trong record; `.reason.txt` chỉ do FileLanding tạo. Quarantine Kafka không thể đưa nguyên file vào `land-file` như bước phục hồi chỉ dẫn, vì tiền tố offset làm nó không còn JSON; KafkaLanding cũng không tự trả mã CLI 3. Runbook hiện khiến người trực tìm artifact/mã lỗi không tồn tại trên đường Kafka.

**Đề xuất:** tách rõ recovery file/Kafka, lưu reason cùng topic/partition/offset/value theo envelope có codec, cung cấp cách sửa/extract/replay an toàn và test recovery. Hoặc giới hạn hướng dẫn `.reason.txt`/mã 3/`land-file` cho đường file và bổ sung hướng dẫn Kafka riêng.

### N4 — Handoff chưa đủ dữ liệu bàn giao theo AGENTS.md

Vị trí: `.team/handoffs/P5-data-platform.md:14`, `:15`, `:17`.

Handoff ghi “Như P2/P4” và “repo1.maven.org” nhưng không có link version cụ thể cho dependency mới; thiếu phần đề xuất cập nhật PROGRESS/glossary/compatibility-matrix (có thể ghi “không cần cập nhật”). AGENTS.md mục 2.3 yêu cầu các thông tin này. Dependency trong Scala CLI/sbt/README khớp nhau, nhưng audit nguồn sẽ dễ hơn nếu handoff tự đủ thông tin.

**Đề xuất:** thêm link POM/release/nguồn phiên bản + ngày xác minh, dẫn chính xác handoff P2/P4 nếu kế thừa; ghi đề xuất cho ba file dùng chung, không để reviewer tự suy ra.

## Kết quả các điểm handoff yêu cầu soi

- **Superseded nhiều bản cùng payload:** probe năm sự kiện khác ID, hai bản cũ + ba bản mới nhất cùng payload: `rows=5, superseded=4, kept=1`, các count khác 0, gate pass. Phép đếm này đúng. M2 là lỗi khác: collision ở bước dedup event_id trước bước này.
- **Landing/commit:** null bị quarantine; lỗi sau khi ghi nhưng trước commit có `committed=null, lag=3, raw=1`; restart nhận đủ 3 record, quarantine=2, lag=0. Lỗi ghi raw có `committed=null, lag=1`; consumer mới nhận lại record và commit được. Hai probe Kafka bổ sung pass, cộng hai test Kafka gốc pass. Đây là exception injection trong JVM và restart consumer, chưa phải kill JVM/OS crash thật.
- **Dimension đổi sau daily:** status daily thành stale; gate trả hai vi phạm (partition cũ và khác bảng tính lại), rebuild daily chuyển sang segment mới và gate pass. Không tái hiện được bỏ sót dimension ở tình huống tuần tự này. Chưa kiểm snapshot/concurrency giữa audit và đọc để load.
- **Load idempotent/ngày rỗng:** review tĩnh phù hợp README; bốn integration test DB chưa chạy do PostgreSQL nhúng không khởi động, cần lead xác nhận runtime. Không coi việc đọc test source là bằng chứng test pass.
- **Metrics:** probe không cần DB kiểm tra lake chưa load cho `days_loaded=0`, hai gauge còn lại -1; reload ngày 15 mới hơn lần load ngày 17 vẫn giữ `last_day_epoch_day=20743`, age=5 giây theo timestamp load mới nhất. Đúng với ý nghĩa hai gauge riêng biệt. Chưa gọi endpoint `/metrics` với PostgreSQL thật ở lượt review này.
- **Output README:** golden CoreSuite khớp các daily cuối cùng và đối soát 8 dòng ngày 15; test Kafka khớp doanh thu ngày 15 trước hoàn tiền (1.150.000/450.000/20.000.000). Cấu trúc JSON của API và tổng SQL tháng phù hợp code khi dữ liệu đầu vào sạch. Không tái xác nhận output HTTP/thời gian age=6/12 asset Dagster/sbt trong sandbox này; giữ chúng là bằng chứng tác giả, không nâng thành bằng chứng reviewer.
- **Bảng 4.1:** phân công công cụ hợp lý; SQL view thực sự nằm trong `migrate`, Dagster gọi lệnh Scala chứ không tự tính doanh thu. Nên viết “Spark dùng Scala 2.13”, tránh “Spark 2.13” dễ bị hiểu là version Spark; thu hẹp câu “không có sẵn trong công cụ” thành trách nhiệm của consumer tự viết trong P5. `defs.py` hiện minh họa asset/deps/partition; schedule/sensor/retry tự động chưa được cấu hình, không nên hiểu mô tả năng lực Dagster là đã triển khai các cấu hình đó.

## Nguồn và kiểm tra phiên bản

Ngày đối chiếu: **17/10/2026**. Manual offset control/at-least-once phù hợp [KafkaConsumer Javadoc chính thức](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html); trang truy cập được là bản 4.2, còn khả năng chạy client/broker **4.4.0** được xác nhận trực tiếp bởi hai suite Kafka nói trên. Phạm vi transaction-level lock khớp [PostgreSQL 18 — advisory locks](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS). [Metadata Dagster 1.13.26 trên PyPI](https://pypi.org/pypi/dagster/1.13.26/json) truy cập được qua web; không chạy Dagster trong lượt này.

Không lấy trang Maven lỗi làm bằng chứng HTTP 200: web không mở được POM http4s/Kafka, PowerShell HTTPS gặp `The underlying connection was closed`, curl gặp `SEC_E_NO_CREDENTIALS`. Các kết quả thất bại được ghi ở `.cache/p5-review/dependency-check.json`. Version Scala/dependency ở source đã resolve/compile/run qua Scala CLI; chưa tái chứng thực riêng mọi POM/release HTTP của handoff. Không có căn cứ yêu cầu nâng munit ngoài chuẩn repo.

Sau khi tác giả sửa các MAJOR, cần review lại các regression dữ liệu/đối soát và script; lead chạy lại cả Scala CLI và sbt, đặc biệt nhóm PostgreSQL/load/API/backfill, trước khi đánh dấu hoàn thành theo AGENTS.md.

## Vòng 2

Reviewer: Codex · 17/10/2026 · **CHƯA ĐẠT — 2 MAJOR, 2 MINOR; không có BLOCKER.** Đã đọc phản hồi Vòng 2/Vòng 3 trong handoff và source hiện tại. Tiếp tục checkpoint T25, giữ nguyên kết quả chạy đã có, không chạy lại 11 lab đầu hay probe đã hoàn tất. SHA-256 của 844 file trong snapshot `.cache/t25/snapshot.json` vẫn khớp source tại thời điểm tiếp tục.

### Đối chiếu từng vấn đề vòng 1

| ID | Kết quả vòng 2 | Bằng chứng và giới hạn |
|---|---|---|
| M1 | Đã sửa | `core/src/main/scala/vn/scalaeng/p5/Assets.scala:76` ghi JSONL; `Orchestrator.scala:134` đối soát PAID độc lập. Probe dấu phẩy/quote/newline giữ đủ `Retail,300,3,3`, gate rỗng; CoreSuite kiểm thêm daily mất dòng bị chặn. |
| M2 | Sửa đúng trong cùng partition, chưa đủ trên toàn raw | `Assets.scala:64` cách ly cả nhóm khác nội dung, hoán vị 3 dòng luôn cho `(event_conflicts,duplicate_events,kept)=(3,0,0)`. Tuy nhiên lọc ngày trước groupBy làm lọt xung đột khác ngày: R2-M1 bên dưới. |
| M3 | Đã sửa | `Model.scala:46` dùng `Order.dayOf`; staging/reconcile cùng gọi quy tắc này. Probe 7 dòng: 2 assigned, 5 unassigned, tổng cân bằng; kiểm cả null, ngày không tồn tại, năm mở rộng, newline, khoảng trắng. |
| M4 | Đã sửa | `app/src/main/scala/vn/scalaeng/p5/Main.scala:63` có CLI reconcile; `orchestration/defs.py:61` công bố metadata/cảnh báo. Test tác giả và probe gọi thật `Main.run`: ghi JSON báo cáo; malformed raw được đếm unassigned. Chính sách cho phép publish ngày hợp lệ được ghi rõ. Dagster thật trên bản Vòng 3 chưa chạy được trong sandbox. |
| M5 | Đã sửa | `orchestration/run-platform.sh:14` tạo thư mục con bằng mktemp; không còn xóa WORK. Bốn lượt harness giữ nguyên sentinel của caller. |
| M6 | Đã sửa phần truyền mã lỗi | Harness source script Vòng 3: success exit 0; tất cả Dagster lỗi, chỉ partition giữa lỗi (partition cuối thành công), hoặc API lỗi đều exit 1. Cleanup giữ mã lỗi. Harness mock dịch vụ, không chứng minh vòng đời PostgreSQL/API thật. |
| N1 | Đã sửa lỗi cũ | `serving/src/main/scala/vn/scalaeng/p5/Serving.scala:79`: thiếu/lặp/sai/range 367 ngày đều 400; 366 ngày được nhận; readiness DB down trả 503. Còn lỏng định dạng ngày: R2-N1. |
| N2 | Đã sửa các ca cũ | `Orchestrator.scala:111` kiểm artifact trước đọc; dòng daily chỉ có `Retail` trả vi phạm có dòng, không exception. Probe và CoreSuite pass. |
| N3 | Đã sửa | `ingest/src/main/scala/vn/scalaeng/p5/Ingest.scala:39` ghi envelope JSON có topic/partition/offset/reason/value. Runbook 3a/3b tách file/Kafka, hướng dẫn extract `.value`. Broker thật xác nhận null và JSON sai contract đều giữ được trước commit. |
| N4 | Sửa một phần | Handoff đã ghi version/ngày và đề xuất file dùng chung, nhưng chỉ có URL POM Kafka mẫu, các dependency khác yêu cầu người đọc tự dựng link: R2-N2. |

Đường dẫn rút gọn trong bảng và các mục dưới cùng gốc `14-projects/p5-data-platform/`, trừ khi ghi rõ `.team/`.

### MAJOR

**R2-M1 — Xung đột event_id khác ngày vẫn được giữ và publish qua gate (M2 chưa khép).** Vị trí: `core/src/main/scala/vn/scalaeng/p5/Assets.scala:56`, `:59`, `:64`; `README.md:50`.

Lọc theo ngày diễn ra trước khi phát hiện event_id trùng khác nội dung. Probe đưa hai sự kiện hợp lệ cùng ID/key, ngày 15 giá 100 và ngày 16 giá 900; chạy backfill cả hai ngày cho kết quả thật:

```text
event_conflicts ngày 15 = 0; ngày 16 = 0
daily15=Vector(Retail,100,1,1) daily16=Vector(Retail,900,1,1)
Quality.check cả hai ngày = Nil
```

Probe thứ hai materialize ngày 15 trước, rồi land một file mới chỉ có bản trùng ID ngày 16: staging ngày 15 vẫn `Fresh`, vì `relevant` bỏ file chỉ chứa ngày 16. Đây là hai đường vào được hỗ trợ; không sửa artifact thủ công. Quy tắc README “cùng event_id khác nội dung → cách ly cả nhóm” không giới hạn theo ngày nên đang bị vi phạm và hai phiên bản cùng sự kiện đều có thể đóng góp doanh thu.

Đề xuất: phát hiện collision trên snapshot raw trước khi chọn partition; liên kết fingerprint/ID index vào dependency của mọi ngày bị ảnh hưởng, để sự kiện đến sau có thể làm cũ cả ngày cũ. Giữ phương trình đối soát theo ngày rõ ràng; thêm regression khác ngày trong cùng file, hai file/lô đến lần lượt và đảo thứ tự. Nếu chủ ý ID chỉ unique theo ngày, phải đổi contract/khóa định danh và mô tả rõ; không giữ tuyên bố global hiện tại.

**R2-M2 — Mất materializationId của đúng upstream đã đọc khi upstream đổi giữa snapshot và refs.** Vị trí: `core/src/main/scala/vn/scalaeng/p5/Orchestrator.scala:43`, `:45`, `:48`, `:78`; `core/src/main/scala/vn/scalaeng/p5/Assets.scala:12`.

Snapshot đã giữ đúng byte: probe đổi raw trong `compute` vẫn xuất giá 100, lineage hash cũ và status stale. Tuy nhiên `refs` chỉ tìm `latestIn` rồi so hash. Probe dùng hook `Asset.relevant` để xen kẽ rematerialize staging sau khi đọc file, trước `refs` (một lịch xen kẽ xác định của thay đổi đồng thời): downstream vẫn tính trên staging giá 100 và giữ đúng hash cũ, nhưng `InputRef.materializationId=None`, dù bản ghi cũ với hash/ID tương ứng vẫn còn trong lineage. Log thật: `PROVENANCE ... recorded=List(InputRef(asset,staging_orders@2026-10-15,67135d8a0fb23438,None), ...)`.

Đề xuất: chụp byte, hash và định danh phiên bản upstream trong cùng cơ chế nhất quán/khóa root như Ch31; không chỉ tra latest sau snapshot. Ít nhất phải truy được bản lịch sử khớp hash, đồng thời xử lý trường hợp nhiều lần materialize cùng byte nếu cần identity chính xác. Khi không xác định được provenance thì fail/retry rõ ràng, không âm thầm ghi None cho input asset đã materialize. Thêm regression lịch xen kẽ trên; probe hiện tại xác nhận lỗi chứ chưa phải test sửa lỗi thành công. Chưa suy rộng probe này thành bằng chứng đã test đầy đủ nhiều writer hay toàn giao dịch publish.

### MINOR

**R2-N1 — API vẫn nhận ngày không đúng định dạng được hứa.** Vị trí: `serving/src/main/scala/vn/scalaeng/p5/Serving.scala:81`. `Api.range` dùng trực tiếp `LocalDate.parse`, trong khi thông báo đòi `yyyy-MM-dd`; probe `+02026-10-15` cho `Right((2026-10-15,2026-10-15))`. Staging đã từ chối kiểu ngày này bằng `Order.dayOf`. Đề xuất dùng parser/ràng buộc định dạng chung hoặc ghi rõ API nhận ISO expanded-year; test trực tiếp tham số đã URL-encode dấu `+`. Không ảnh hưởng bản sửa giới hạn 366 ngày.

**R2-N2 — Nguồn version trong handoff vẫn chưa tự đủ để audit.** Vị trí: `.team/handoffs/P5-data-platform.md:46`, `:49`. Đã có ngày, bảng version và một URL Kafka nhưng chưa có link chính xác cho từng dependency (đặc biệt http4s-circe mới). Đề xuất liệt kê URL POM/release chính xác hoặc link tới bảng nguồn cụ thể trong handoff P2/P4. Không yêu cầu đổi pin; resolve/compile thành công không thay thế việc ghi đủ nguồn được yêu cầu trong AGENTS.md.

### Kiểm chứng vòng 2 và giới hạn

- Baseline snapshot P5: `scala-cli.bat test project.scala core ingest serving app --server=false`, cache Coursier và TEMP/TMP dưới `.cache/`: **exit 0, 11 pass, 6 skipped, 0 compiler warning** (Core 9, Platform 2 không cần DB; Kafka 2 và Platform 4 cần dịch vụ bỏ qua). Log `.cache/t25/logs/14-projects__p5-data-platform.log`.
- Probe core/API/snapshot: cùng nhóm source + `.cache/t25/ReviewerRound2.test.scala`, `--test-only vn.scalaeng.p5.ReviewerRound2 --server=false`: **exit 0, 13/13 pass**; trong đó các assert R2-M1/R2-M2 xác nhận lỗi còn tồn tại. Log `.cache/t25/p5-round2-probes.log`.
- `RUN_KAFKA_IT=1`, test-only `*Kafka*` + `.cache/t25/ReviewerKafkaRound2.test.scala`: **exit 0, 5/5 pass** (2 gốc + 3 bổ sung), **0 compiler warning**, broker KRaft thật. Trước commit: `committed=null`, lag 3, raw 1, quarantine 2 JSON envelope; restart nhận lại đủ 3, lag 0. Lỗi ghi quarantine không commit; restart giữ raw value và hoàn tất. Lỗi ghi raw cũng không commit, lag 1. Log `.cache/t25/p5-kafka-round2.log`. Đây là exception injection/restart consumer, chưa phải kill JVM hay OS crash thật.
- Bash syntax exit 0; harness bốn mode exit **0/1/1/1**, sentinel caller nguyên vẹn. Log `.cache/t25/p5-script-{success,dagster,middle,api}.log`; mock SCALA_CLI/Dagster/Java/curl và socket probe. Không coi output API giả `[]` hay marker pg.stopped giả là bằng chứng runtime PostgreSQL.
- Lượt opt-in DB trước checkpoint chỉ có log chưa hoàn tất, không có exit/result nên không được tính thành pass. Lượt tiếp tục có thời hạn đã kết thúc: `RUN_DB_TESTS=1 RUN_KAFKA_IT=1 scala-cli.bat test project.scala core ingest serving app --server=false` **exit 1**, Core **9 pass**, Kafka **2 pass**, Platform **1 failed ở beforeAll(postgres)**, **0 compiler warning**. Log `.cache/t25/p5-db-round2.log`: `Gave up waiting for server to start after 10000ms` → `SQLException: connect failed` → `ConnectException: Connection refused: getsockopt`. Cả 6 test PlatformSuite không được thực thi vì fixture toàn suite thất bại, gồm cả 2 test logic thường chạy khi tắt opt-in. Đây là lỗi fixture khởi động trong sandbox, chưa chứng minh bug nghiệp vụ hay cơ chế chặn cụ thể. T24 còn có `toRealPath` AccessDenied trên đường script; không đánh đồng hai triệu chứng.
- Không chạy sbt/assembly, Dagster thật, Docker hoặc fallback pg_ctl/taskkill thật. Vòng đời stop-file Vòng 3 vẫn cần lead chạy ngoài sandbox theo handoff. Giữ giới hạn của T24; harness không thay thế các ca runtime đó.

Chỉ ghi review/progress và `.cache/`; không sửa source hay file dùng chung, không commit. Lead cần sửa hai MAJOR, hoàn tất nguồn và kiểm chứng runtime DB/vòng đời dịch vụ trước khi chốt P5.
