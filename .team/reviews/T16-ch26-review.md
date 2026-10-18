# Review T16 — Chương 26: Spark Production Engineering (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 7 MAJOR, 4 MINOR; không có BLOCKER.**

**Mốc source vòng 1:** test/probe Ch26 thực hiện khoảng 10:01–10:07 (UTC+7). Source Ch24 được build dùng chung lúc đó là bản trước sửa; cuối lượt phát hiện Pipeline.scala/PipelineDemo.scala/PipelineSuite.scala được tiến trình khác sửa lúc 10:16–10:17. Reviewer không sửa source và chưa chạy lại Ch26 với dependency Ch24 mới; kết quả này không xác nhận các thay đổi đồng thời đó.

Đã đọc handoff T16, đủ chương/source/test/build/deploy/probe và lời giải/code/test Ch26; đối chiếu compatibility matrix, template và PHẦN 11–14. Chương đủ **17 mục có nội dung**, tiếng Việt rõ, bài tập nối tiếp lab và có lời giải tương ứng. Build Scala 2.13, Spark Provided, loại Scala khỏi assembly và lệnh spark-submit local có cơ sở đúng. Các bảo đảm về lifecycle, metric, lịch sử manifest và Scala 3 cần sửa trước khi chốt.

## Chạy lại và bằng chứng

Chỉ ghi review và `.cache/`; không sửa tác giả, không sbt/commit. Dùng `.cache/run-review.ps1`, `COURSIER_CACHE=<repo>/.cache/coursier`; SPARK_LOCAL_DIRS, TEMP/TMP, java.io.tmpdir, warehouse và workspace riêng dưới `.cache/review-t16/`, giống cấu hình review T14. Mọi lệnh Scala CLI đều **`--server=false`**, thêm:

```text
--workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-t16/<name>-build
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/review-t16/tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/review-t16/warehouse
```

| Lượt chạy | Kết quả thật | Log dưới `.cache/review-t16/` |
|---|---|---|
| `scala-cli.bat test .` từ `09-spark/spark-production-lab` | exit 0; **9 pass / 0 fail / 0 ignored**, 52.041s | `lab-test.log` |
| `scala-cli.bat test .` từ `solutions/ch26-code` | exit 0; **6 pass / 0 fail / 0 ignored**, 18.056s | `sol-test.log` |
| `scala-cli.bat run . --main-class Probe` từ `.cache/review-t16/probe` | exit 0; fault injection + kiểm tra JSON/config/lời giải/metric/manifest | `attack.log` |
| Probe gốc `09-spark/scala3-spark-probe`, `--scala 3.9.0` và `--scala 3.8.4` | đều exit 1, đúng lỗi Cannot find a SparkSession implementation | `scala390.log`, `scala384.log` |
| Probe bổ sung `.cache/review-t16/scala-probe`, `--scala 3.7.3` và `--scala 3.9.0` | đều exit 0; direct classic builder + DataFrame + Encoder số chạy thật | `scala373-extra2.log`, `scala390-extra.log` |

Lượt compile đầu của probe bổ sung dùng macro Scala 2 `currentMirror` nên lỗi; đã chuyển sang `universe.runtimeMirror(classLoader)` rồi chạy lại. Cảnh báo unused import/inferred Any chỉ thuộc probe tấn công, không thuộc lab/lời giải. Một số JVM compile/probe độc lập chạy đồng thời; không suy tỷ lệ hiệu năng từ thời gian này.

Checkpoint tái hiện `Run(60,122,80,559500)` và `Run(60,1,1,559500)`: số node/kết quả khớp bài, planningMs phụ thuộc lượt chạy. JSON log/manifest từ fault injection được parse bằng Jackson; chuỗi có quote, backslash, CR/LF/tab/backspace/formfeed, toàn bộ control U+0000..001F và tiếng Việt round-trip **true**. Không phát hiện lỗi escape JSON; NaN/Infinity được ghi thành chuỗi hợp lệ theo policy code.

Đã gọi **spark-submit.cmd thật** từ bản phân phối cache 4.2.0, dùng jar tác giả đã build 110.004 byte (chỉ đọc, chưa rebuild): không có application args vẫn vào `Job.main → session → SparkContext` rồi lỗi `Executor.updateDependencies → FileUtil.chmod`, thiếu winutils; **exit 1**, chưa có `config_invalid`. Log `submit-invalid-uri.log`. Lượt đầu của runner dùng `C:/...jar` bị Hadoop hiểu scheme C; đã đổi thành file URI, không tính lỗi runner đó cho tác giả. `wsl --list --quiet` bị sandbox từ chối `Wsl/EnumerateDistros/Service/E_ACCESSDENIED`; chưa chạy lại Linux/WSL, mã thoát 0/3 và SHA-512 tải phân phối trong handoff. Không xác nhận độc lập sbt assembly hay cluster.

Probe gốc với `--scala 3.3.8` → **exit 0**, `0=334, 1=333, 2=333` (`scala338.log`), khớp bài. Dòng in `scala.util.Properties` báo 2.13.18 là phiên bản thư viện runtime, không phải compiler Scala 3.3.8; bài đã phân biệt cột này. Các lượt probe bổ sung có workspace riêng nên không ghi build vào source.

## MAJOR

### M1 — Điểm vào production khởi tạo Spark trước kiểm tra cấu hình

Vị trí: `09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala:148`, `:149`; `src/test/scala/vn/scalaeng/sparkprod/JobSuite.scala:57`; `09-spark/ch26-spark-production-engineering.md:59`, `:167`.

`main` gọi session trước run/parse. Test “không chạm Spark/đĩa” dùng session đã tạo trong beforeAll, chỉ chứng minh nhánh `Job.run` sau đó không dùng Spark. Spark-submit không truyền tham số ở lượt reviewer vẫn tạo SparkContext và chết vì winutils trước khi trả lỗi thiếu tham số. Trên môi trường chạy được, vẫn tốn khởi tạo Spark và có side effect filesystem/network cho cấu hình sai.

Đề xuất: parse/validate trước session trong main, tách hàm chạy cấu hình đã kiểm tra, giữ run testable; thêm test lifecycle/subprocess chứng minh cấu hình sai trả 2 mà không gọi session factory. Phân biệt Job.run integration test với kiểm thử main/launcher.

### M2 — Phân loại retry không nhất quán, gate nằm ngoài retry, và retry bao cả side effect sau commit

Vị trí: `Job.scala:28`, `:87`, `:95`, `:100`, `:103`; `solutions/ch26-code/src/main/scala/vn/scalaeng/solutions/ch26/Ch26Solutions.scala:24`; `09-spark/ch26-spark-production-engineering.md:96`, `:105`.

Probe thật:

```text
IllegalArgumentException(cause IOException): job=true, solution=Data
FileAlreadyExistsException:                 job=false, solution=Transient
ZipException("invalid header"):             job=true
```

“Có bất kỳ IOException” không đủ suy ra tạm thời; dữ liệu/ZIP hỏng và lỗi hợp đồng bọc I/O có thể bị thử lại. Lời giải và job còn khác ưu tiên Permanent/Data/Transient. Đồng thời gate đọc storage ngoài `attempt`: I/O tạm thời ở chính bước liệt kê/đọc gate không được retry dù bài dùng liệt kê file làm ví dụ.

Fault injection vào sink: nhận `pipeline_succeeded` (output đã ghi và verify xong), tăng side-effect counter rồi ném IOException **một lần**. Kết quả:

```text
code=0 effects=2 attempts=2 recordsWritten=30 committedFacts=6
```

Sink lỗi khiến toàn bộ pipeline chạy lại; side effect quan sát được nhân đôi. Bộ output Parquet này không bị nhân đôi dòng nhờ overwrite, nhưng điều đó không chứng minh mọi side effect hay mọi input đều an toàn. Đường chạy còn kế thừa orphan/verify của Ch24 (review T14 M1–M4): “output idempotent” hiện chưa là tiền đề đã giữ được.

Đề xuất: một classifier chung có thứ tự ưu tiên, allowlist transient theo nguyên nhân/storage code đã hiểu; retry đúng phạm vi đọc + xử lý cần làm lại, tách logging/finalization khỏi transaction dữ liệu. Gate và pipeline phải dùng cùng snapshot input hoặc có điều kiện bất biến rõ. Thêm fault injection **sau ghi**, lúc gate, và mixed cause; không chỉ tiêm lỗi trước Pipeline.run.

### M3 — Lỗi chốt metric bị nuốt, số ghi là chi phí các attempt chứ không phải số dòng đã commit

Vị trí: `Job.scala:122`; `src/main/scala/vn/scalaeng/sparkprod/Observability.scala:76`, `:95`; `09-spark/ch26-spark-production-engineering.md:78`, `:90`.

Probe dừng SparkContext ngay khi sink nhận pipeline_succeeded để buộc job rào chắn không chạy được. `metrics.close()` thất bại nhưng Job.run vẫn trả **0**, manifest **SUCCEEDED**; không có trường nào báo metric không đầy đủ. Đây là lỗi xác định, không phụ thuộc chờ 60 giây. `Try(...).getOrElse(metrics.asMap)` trái với lời bài “hết hạn thì báo lỗi thay vì số dở dang”. Nếu lỗi xảy ra ngay ở action rào chắn, close cũng chưa tới finally remove listener.

Ở probe retry sau commit (M2), metric **30** trong khi bộ output cuối có **15 record** (6 fact + 4 daily + 3 rejected + 2 conflict). Listener cộng metric mọi task attempt, kể cả attempt thất bại/ghi lại; không thể dùng làm đối soát committed rows. Scope job group hoạt động cho ca độc lập đã thử: action 10 record trong group + 10.000 record ngoài group → recordsRead=10, tasksSucceeded=1.

Đề xuất: policy rõ khi metric unavailable (mã lỗi hoặc cờ completeness + log lỗi), luôn gỡ listener/khôi phục job group trong finally. Tách metric physical attempted work khỏi committed business counts; giới hạn phép bằng 15 cho thí nghiệm một attempt không lỗi. Thêm regression listener chậm/barrier lỗi/retry sau ghi; dùng thông số timeout có thể tiêm để test không phải chờ lâu.

### M4 — Manifest có thể mất lịch sử và lỗi finalization thoát khỏi hợp đồng mã thoát

Vị trí: `Job.scala:59`, `:63`, `:124`, `:126`, `:132`; `09-spark/ch26-spark-production-engineering.md:130`.

Chạy thành công `run-id=same`, sau đó chạy gate fail cùng run-id/output:

```text
secondCode=3 status="DATA_REJECTED" jsonFiles=1
```

Manifest SUCCEEDED bị `REPLACE_EXISTING` thay mất. Đây không còn “một file mỗi lần chạy”; lịch sử dùng cho SLA/lineage thay đổi theo ID người dùng nhập. File tmp cũng dùng cùng tên, không phù hợp concurrent run cùng ID.

Probe khác tạo thư mục `_runs/m.json`, chạy gate fail `run-id=m`: `AccessDeniedException` từ rename **thoát ra khỏi Job.run**, không trả exit code theo bảng và không có job_finished. Ghi manifest/prom nằm ngoài khối try phân loại; pipeline có thể đã thành công trước lỗi finalization tương tự. Hai file được rename riêng, không phải một commit chung.

Đề xuất: định danh execution duy nhất, không ghi đè manifest lịch sử; nếu run-id là khóa idempotency thì công bố policy và từ chối/đọc lại kết quả cũ trước side effect. Dùng tmp duy nhất, xử lý finalization với trạng thái/exit code rõ, tách data committed khỏi telemetry failed. Bổ sung test collision, target không ghi được và lỗi giữa JSON/prom. `Paths.get(output,...)` là filesystem cục bộ: nếu output có thể là HDFS/S3 thì cần abstraction phù hợp hoặc giới hạn rõ CLI chỉ local/shared filesystem; ATOMIC_MOVE không tự bảo đảm cho mọi storage.

### M5 — File Prometheus theo run gây trùng series khi scrape thư mục lịch sử

Vị trí: `Job.scala:132`, `:134`; `Observability.scala:115`; `09-spark/ch26-spark-production-engineering.md:78`, `:90`.

Suite tạo `_runs/a.prom` và `_runs/b.prom` cho cùng ngày. Reviewer đọc hai file thật; cả hai chứa:

```text
job_exit_code{job="ch26_orders",run_date="2026-10-16"} 0
```

Nếu trỏ textfile collector vào `_runs/` như cách dùng được gợi ý, collector đọc cả hai, sinh duplicate metric-name+labels; các TYPE cũng lặp lại. Nhãn ngày tích lũy theo backfill còn tăng mãi nếu không retention. Probe helper với label chứa newline cũng cho LF literal trong quoted value; chỉ escape backslash/quote, thiếu escape `\n` (nhãn hiện tại của Job bị giới hạn nên chưa kích hoạt lỗi newline).

Đề xuất: giữ lịch sử JSON riêng; export một file ổn định cho trạng thái hiện tại (hoặc một file/ngày có retention/không trùng series), ghi atomic. Nếu dùng Pushgateway, nêu grouping key/replace/delete và đưa đường chạy thật; escape LF trong helper. Kiểm tra nội dung exposition gộp sau rerun, không chỉ từng file có substring. Chưa chạy node_exporter service; duplicate được xác nhận trên file, hệ quả đối chiếu [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/) và [node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector) (tra 17/10/2026).

### M6 — Scala 3: suy rộng từ probe builder và case-class encoder thành bất khả thi với mọi API

Vị trí: `09-spark/ch26-spark-production-engineering.md:210`, `:219`, `:221`, `:222`, `:418`; `09-spark/scala3-spark-probe/project.scala:8`; `00-roadmap/04-compatibility-matrix.md:97` (lead sửa).

Probe gốc 3.8.4/3.9.0 lỗi đúng như bài. Nhưng probe bổ sung trên **cả Scala 3.7.3 và 3.9.0**, cùng Spark 4.2.0, gọi trực tiếp `org.apache.spark.sql.classic.SparkSession.builder()`:

```text
toDS=List(1, 2) asLong=List(0, 1, 2) grouped=List([0,334], [1,333], [2,333])
```

Code dùng `import spark.implicits.*`, `Seq(1L,2L).toDS()` và `.as[java.lang.Long](using Encoders.LONG)`. Vậy các câu “mọi phiên bản Scala 3: import/toDS/as[T] không dùng được”, “Encoder không có ở Scala 3”, “Scala 3.8+ không chạy Spark 4.2” đều quá rộng.

Probe 3.9 tái hiện phần bị builder che:

```text
reflection underlying: scala.reflect.internal.FatalError: class Array does not have a member apply
API builder: IllegalStateException: Cannot find a SparkSession implementation on the Classpath.
```

[SparkSession 4.2.0 source](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala) dùng Scala reflection tìm companion, bọc hai Try rồi thay lỗi bằng thông báo chung. Bypass builder chứng minh không thể suy lỗi này là mọi DataFrame API/stdlib binary đều không dùng được. [Scala 3.8 release](https://www.scala-lang.org/news/3.8/) nói stdlib được kiểm tra binary compatibility; [Encoders 4.2.0](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Encoders$.html) có encoder tường minh, không phải mọi encoder đều đòi TypeTag case class (tra 17/10/2026).

Đề xuất: giữ **đúng** quyết định giáo trình dùng job Scala 2.13, vì [Spark overview](https://spark.apache.org/docs/4.2.0/) chỉ hỗ trợ Scala 2.13. Giới hạn bảng probe theo exact entrypoint/classpath; phân biệt automatic case-class encoder dựa reflection Scala 2 với encoder scalar/explicit. Không hướng dẫn bypass như giải pháp production được Apache hỗ trợ. Đồng bộ phần kết luận và compatibility matrix qua lead; reviewer không sửa file chung.

### M7 — Lời giải VolumeCheck tràn Long khi tính trung vị, bỏ sót cảnh báo

Vị trí: `solutions/ch26-code/src/main/scala/vn/scalaeng/solutions/ch26/Ch26Solutions.scala:78`; `solutions/ch26-solutions.md:42`.

Probe `history=Seq.fill(4)(Long.MaxValue), today=1, minRatio=0.5` trả **None**. Cộng hai giá trị giữa bằng Long tràn thành -2, median=-1, làm ngày tụt về 1 record không còn bất thường.

Đề xuất: tính tổng/median bằng BigInt/BigDecimal hoặc chuyển miền trước phép cộng với sai số được công bố; validate số đếm không âm. Thêm test lịch sử chẵn sát biên và không che lỗi chỉ bằng test `delay >= 0`/số nhỏ.

## MINOR

### m1 — Tham số có mặt nhưng rỗng bị coi như vắng; tham số trùng âm thầm lấy cuối

Vị trí: `src/main/scala/vn/scalaeng/sparkprod/JobConfig.scala:27`, `:30`, `:35`.

Probe `--max-reject-rate= --max-attempts= --run-id=` được Right với 0.05/3/ID tự sinh. `--max-attempts=BAD --max-attempts=2` cũng Right=2. Không phải mọi giá trị sai đều được gom lỗi như cách mô tả dễ khiến người vận hành hiểu. Đề xuất phân biệt absent/empty, chốt policy duplicate flag, thêm test; nếu chủ ý dùng last-wins/default-on-empty thì ghi rõ.

### m2 — Backoff clamp số mũ làm sai trần từ attempt 64 với base nhỏ

Vị trí: `solutions/ch26-code/src/main/scala/vn/scalaeng/solutions/ch26/Ch26Solutions.scala:42`; `solutions/ch26-solutions.md:23`.

Với attempt=64, base=1, cap=Long.MaxValue và Random trả 0.5: thực tế **2305843009213693952**, do ceiling bị giữ ở 2^62. Công thức bài yêu cầu min(cap,2^63)=Long.MaxValue, jitter 0.5 phải khoảng **4611686018427387904**. Mặc định base=100/cap=30000 đã bão hòa sớm nên test không thấy. Đề xuất saturation theo ngưỡng thay vì clamp exp=62 vô điều kiện; test exact ceiling bằng random xác định.

### m3 — RunHistory đọc regex không nhận JSON hợp lệ có whitespace

Vị trí: `Ch26Solutions.scala:50`, `:51`, `:59`; `solutions/ch26-solutions.md:37`.

Probe manifest SUCCEEDED pretty-print có khoảng trắng sau dấu `:` trả Set rỗng. Lời giải đã công bố regex cho bài tập nên đây là giới hạn hơn là lỗi production giấu kín; tuy vậy nó có thể báo sai SLA chỉ vì format JSON đổi. Đề xuất dùng JSON parser sẵn trên classpath hoặc giới hạn rõ chỉ nhận compact format do chính Job hiện tại sinh, tránh gọi đây là bộ đọc manifest JSON tổng quát.

### m4 — Ví dụ đơn giản dùng đường dẫn của Ch24 khi đang ở lab Ch26

Vị trí: `09-spark/ch26-spark-production-engineering.md:256`.

`--input=data/input --customers=data/customers.csv` không tồn tại trong spark-production-lab, trong khi phần chạy chương đặt cwd ở đó. Comment `code == 0` không đúng nếu người học copy nguyên ví dụ. Đề xuất dùng `../batch-pipeline-lab/data/...` như submit.sh và test; ghi cwd tường minh.

## Đối chiếu triển khai và phạm vi chưa xác minh

- [Submitting applications 4.2.0](https://spark.apache.org/docs/4.2.0/submitting-applications.html) khớp thứ tự Spark options → jar → application arguments, Spark/Hadoop Provided và việc đường dẫn trên cluster phải truy cập được. submit.sh dùng exec nên không tự nuốt mã thoát. Script mặc định relative file local, chưa là cấu hình YARN/K8s đầy đủ; “cùng jar chạy mọi nơi” cần điều kiện storage/dependency.
- [Monitoring 4.2.0](https://spark.apache.org/docs/4.2.0/monitoring.html) và code listener phù hợp việc gom task metrics bất đồng bộ; số physical task output không đồng nghĩa committed business rows. JDK 17/21/25 và Scala 2.13 được overview xác nhận. [sbt-assembly 2.3.1 release](https://github.com/sbt/sbt-assembly/releases/tag/v2.3.1) tồn tại. Ngày tra các nguồn: 17/10/2026.
- Thông báo Windows winutils đã tái hiện trên distribution thật. Chưa chạy độc lập transcript lỗi Scaladoc và automatic case-class encoder; counterexample scalar không phủ nhận lỗi case class đã trích.
- Main API chỉ kiểm thử local; không kiểm chứng driver/executor nhiều máy, timeout listener thật 60 giây, node_exporter/Pushgateway service, Hadoop/object storage hay crash durability của ATOMIC_MOVE. Sau sửa, lead cần chạy lại test + fault injection và spark-submit Linux cuối; không đánh dấu hoàn thành chỉ từ 9+6 test xanh hiện tại.


## V?ng 2

Reviewer: Codex ? Ng?y: 17/10/2026 ? **CH?A ??T ? c?n 2 MAJOR, 2 MINOR; 0 BLOCKER.**

?? ??c handoff V?ng 2 v? c?c thay ??i ? ch??ng, lab, l?i gi?i, Scala 3 probe. Gi? nguy?n v?ng 1; ch? ghi review v? `.cache/`, kh?ng s?a code/b?i, kh?ng sbt/commit.

### Ki?m ch?ng l?i

Runner `.cache/run-review.ps1` v?i `-Part t16`, `--server=false`; cache Coursier chung `.cache/coursier`; TEMP/TMP, workspace, Spark local v? warehouse trong `.cache/review-t16/`. Spark 4.2.0, Scala 2.13.18, JDK 21.0.12.1. Session c?a lab v? probe classic ?? ??t artifact isolation=false.

| L?nh Scala CLI (qua runner, cwd t??ng ?ng) | K?t qu? th?t | Log trong `.cache/review-t16/` |
|---|---|---|
| `test .` t?i `09-spark/spark-production-lab` | exit 0; **12 pass, 0 fail, 0 ignored**, 92,813s | `v2-lab.log` |
| `test .` t?i `solutions/ch26-code` | exit 0; **6 pass, 0 fail, 0 ignored**, 20,694s | `v2-sol.log` |
| `run . --main-class ProbeV2` t?i `.cache/review-t16/probe` | exit 0; fault injection v? helper probes | `v2-attack.log` |
| `run . --scala 3.9.0` t?i `09-spark/scala3-spark-probe` | exit 1; builder chung l?i nh? b?i | `v2-builder390.log` |
| `run . --scala 3.9.0 --main-class testScalaSpark` t?i `.cache/review-t16/scala-probe` | exit 0; classic builder + scalar encoder/DataFrame ch?y | `v2-scala390.log` |

V? d? runner: `powershell -NoProfile -ExecutionPolicy Bypass -File .cache/run-review.ps1 -Part t16 -Source .cache/review-t16/probe -Mode run -Main ProbeV2 -Name v2-attack`. Probe v?ng 1 gi? t?i `Probe.scala.v1`; b?n m?i th?ch nghi t?n manifest execution v? tr??ng metric m?i, gi? c?c th?i ?i?m fault injection c?, th?m l?i gi?a manifest v? Prometheus. WARN native-hadoop/winutils kh?ng c?n ki?m ch?ng; ch? Vi?t ? m?t s? log Java l?i encoding. Kh?ng ph?t hi?n compiler warning trong hai suite t?c gi?.

### C?c s?a ??i ?? ???c x?c nh?n

- **M1:** `Job.launch` c?u h?nh thi?u tr? 2, session factory kh?ng ???c g?i; main d?ng launch ??ng. Kh?ng ch?y l?i spark-submit/WSL ? v?ng n?y.
- **M2, ???ng retry ch?nh:** l?i sink ? `pipeline_succeeded` nay `code=1 effects=1`, fact cu?i=6, kh?ng ch?y l?i d? li?u. Data b?c IOException kh?ng transient; FileAlreadyExists/ZIP ???c x?p ??ng; gate ?? n?m trong attempt. C?n kh?c bi?t nh? gi?a hai classifier, xem MINOR.
- **M3, t?nh ??y ??:** d?ng SparkContext l?c `pipeline_succeeded` nay tr? 0 v?i manifest `SUCCEEDED, metrics_complete=false`; kh?ng c?n che vi?c metric thi?u. ??y l? policy ???c c?ng b? trong b?i; kh?ng y?u c?u metric h?ng ph?i ??i k?t qu? d? li?u th?nh th?t b?i. Scope metric v?n ??ng: action 10 trong group v? 10.000 ngo?i group cho recordsRead=10, task th?nh c?ng=1. Tuy nhi?n cleanup ch?a bao h?t nh?nh sink l?i.
- **M4, l?ch s?/catch:** ch?y c?ng run-id l?n ??u th?nh c?ng, l?n sau gate fail gi? **2** manifest v?i hai tr?ng th?i; `_runs` l? file tr? m? 1, kh?ng c?n exception tho?t kh?i Job.run. C?n tr?ng th?i kh?ng nh?t qu?n n?u l?i ? b??c ti?p theo, xem MAJOR.
- **M5:** `_metrics` ch? c? `ch26_orders.prom`; nh?n kh?ng c?n run_date; LF ???c escape. Kh?ng ch?y node_exporter service.
- **M6:** m?c 3.8 v? comment probe ?? ph?n bi?t builder chung, classic builder, encoder scalar/t??ng minh v? case class t? ??ng; kh?ng c?n suy r?ng th?nh m?i API Scala 3 ??u kh?ng ch?y. Probe 3.9 x?c nh?n l?i reflection g?c `class Array does not have a member apply`, l?i builder chung, v? `toDS=List(1, 2) asLong=List(0, 1, 2) grouped=List([0,334], [1,333], [2,333])`. Quy?t ??nh production d?ng Scala 2.13 v?n ??ng. Kh?ng ch?y l?i m?i phi?n b?n/case-class compiler probe ? v?ng n?y.
- **M7:** history b?n Long.MaxValue, today=1 nay tr? Some c?nh b?o, median=9223372036854775807.0; kh?ng c?n overflow.
- **m1:** flag r?ng/l?p b? t? ch?i. **m2 ? code:** attempt=64/base=1/cap=Long.MaxValue/random=0.5 nay ra 4611686018427387904. **m3:** pretty JSON history tr? Set(2026-10-16); gi?i h?n regex ?? ghi r?. **m4:** v? d? ?? d?ng ???ng d?n `../batch-pipeline-lab/data`.

### MAJOR

**R2-M3 ? Sink l?i tr??c `metrics.close()` v?n b? l?i listener v? job group.**

V? tr?: `09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala:174`, `:175`, `:182`, `:208`; `09-spark/ch26-spark-production-engineering.md:91`.

Probe n?m IOException ??ng l?c nh?n `pipeline_succeeded`:

```text
code=1 effects=1 groupAfter=ch26-same-ee533f13 committedFacts=6 manifestsExist=false
```

?? s?a vi?c retry, nh?ng `outcome.finalEvent` ch?y tr??c `metrics.close()` trong c?ng Try. Sink n?m l?i th? nh?y th?ng v?o fold tr? 1; kh?ng g?i close, n?n finally b?n trong close kh?ng ch?y. Job group tr?n thread c?n nguy?n (b?ng ch?ng ? tr?n); listener ?? add t?i d?ng 129 kh?ng c? ???ng remove kh?c. V?i `Job.run` d?ng session c? s?n, l?n ch?y l?i c? th? ti?p t?c gi? listener v? thu nh?m action sau ??; tr?i l?i b?i ?lu?n ???c g??. Main d?ng session kh?ng thay th? ???c h?p ??ng c?a API d?ng session ngo?i.

**?? xu?t:** ??t lifecycle metric trong try/finally bao to?n b? ph?n sau `JobMetrics.start`, b?o ??m cleanup ngay c? khi log/finalization th?t b?i; tr?nh ch?y barrier hai l?n n?u close ?? ???c g?i. Test sink l?i ph?i ki?m job group ???c d?n v? listener kh?ng ti?p t?c thu action sau khi Job.run tr? v?, ngo?i ki?m m? 1/s? l?n ch?y.

**R2-M4 ? L?i gi?a manifest v? metric ?? l?i b?ng ch?ng th?nh c?ng v?i m? tho?t sai.**

V? tr?: `Job.scala:191`, `:200`, `:208`; `solutions/ch26-code/src/main/scala/vn/scalaeng/solutions/ch26/Ch26Solutions.scala:63`; `09-spark/ch26-spark-production-engineering.md:167`.

Probe t?o `_metrics` l? file, ?? pipeline v? manifest th?nh c?ng r?i b??c ghi Prometheus th?t b?i:

```text
code=1 manifest=List(("SUCCEEDED",0)) succeededDates=Set(2026-10-16)
```

Job tr? 1 ??ng policy finalization, nh?ng manifest ???c ghi tr??c ?? v?nh vi?n m? t? l?n th?c thi l? SUCCEEDED/exit_code=0; l?i gi?i RunHistory t?nh ng?y ?? th?nh c?ng. Kh?ng ph?i ch? thi?u m?t file metric: hai giao di?n v? tr?ng th?i/m? tho?t c?a c?ng execution m?u thu?n. B?i hi?n g?i manifest l? tr?ng th?i/m? tho?t c?a l?n th?c thi, ch?a t?ch pipeline outcome v? finalization outcome. Sink l?i t?i `job_finished` sau c? hai file c?ng c? c?ng c?u tr?c v?n ??.

**?? xu?t:** ph?n bi?t r? `data_status` v? `execution_status/finalization_status`, c? b?ng ch?ng l?i cu?i (event/record b? sung g?n execution_id ho?c giao th?c publish ph? h?p), v? s?a RunHistory ch?n ??ng ??nh ngh?a th?nh c?ng ???c c?ng b?. N?u ch? ? manifest ch? m? t? k?t qu? d? li?u th? ??i t?n tr??ng/gi?i th?ch r?, kh?ng g?i `exit_code=0` l? m? tho?t to?n job; runbook ph?i bi?t reconciliate finalization fail. Th?m test l?i sau JSON/tr??c prom v? sink l?i ? job_finished.

### MINOR

**R2-m1 ? Handoff n?i d?ng m?t classifier chung nh?ng l?i gi?i v?n c? b?n kh?c.**

V? tr?: `solutions/ch26-code/src/main/scala/vn/scalaeng/solutions/ch26/Ch26Solutions.scala:16`, `:34`; `09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala:48`.

Probe `TransientStorageException("503")` cho `job=true solution=Unknown`. C?c l?i c? ?? s?a nh?t qu?n, nh?ng marker storage m?i ch? c? trong lab. **?? xu?t:** l?i gi?i g?i classifier chung ho?c c?ng b? r? t?p l?i c?a b?i t?p h?p h?n; th?m marker v? mixed cause v?o b?ng regression. Kh?ng c?n coi ??y l? m?t b? ph?n lo?i duy nh?t nh? handoff n?u.

**R2-m2 ? ?o?n code l?i gi?i Markdown v?n gi? c?ng th?c clamp sai ?? s?a trong source.**

V? tr?: `solutions/ch26-solutions.md:23`, `:24`; ??i chi?u `Ch26Solutions.scala:51`.

B?i v?n in `math.min(attempt - 1, 62)` v? kh?ng c? nh?nh saturation exp?63; copy ?o?n n?y v?n cho k?t qu? sai v?ng 1 ? attempt=64 d? code ch?y th?t ?? ??ng. **?? xu?t:** ??ng b? snippet v? gi?i th?ch v?i source m?i, k?m ca random=0.5 ? tr?n.

### Gi?i h?n v? ph? thu?c

Kh?ng ki?m ch?ng l?i sbt assembly, jar m?i/spark-submit, WSL/Linux, cluster, storage t? xa, collector service hay timeout listener 60 gi?y th?t. Ch26 d?ng Pipeline Ch24; v?n ?? cast `TIMESTAMP` d??i c?u h?nh NTZ ?? ghi ? review T14 V?ng 2, kh?ng nh?n th?nh finding m?i ? ??y. Lead c?n s?a c?c MAJOR, ch?y l?i fault injection v? ki?m ch?ng tri?n khai cu?i tr??c khi ch?t.
