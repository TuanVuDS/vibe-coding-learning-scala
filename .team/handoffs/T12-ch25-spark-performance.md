# Handoff T12 — Chương 25 Spark Performance

Tác giả: Codex · Reviewer được giao: Claude Code · Ngày: **17/10/2026**.

Trạng thái tác giả: **hoàn tất nội dung + code + chạy thật, sẵn sàng review chéo**. Chưa đánh dấu chương hoàn thành sau review; chưa chạy sbt. Không commit git; không sửa file dùng chung hoặc file Ch22/Ch23/Ch24 của agent khác. Mọi ghi của task nằm trong phạm vi T12 và `.cache/`.

## 1. File đã tạo

- `09-spark/ch25-spark-performance.md`: đủ 17 mục; giải thích theo vấn đề/nguyên nhân/bằng chứng/giải pháp/khi không áp dụng; 6 bài tập, link lời giải/test, số đo local và giới hạn.
- `09-spark/spark-performance-lab/project.scala`
- `09-spark/spark-performance-lab/build.sbt`
- `09-spark/spark-performance-lab/project/build.properties`
- `09-spark/spark-performance-lab/verify.ps1`
- `09-spark/spark-performance-lab/src/main/resources/log4j2.properties`
- `09-spark/spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/Measurement.scala`
- `09-spark/spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/PerformanceLab.scala`
- `09-spark/spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/PerformanceDemo.scala` (ba main: PerformanceDemo, MemoryDemo, SerializationDemo)
- `09-spark/spark-performance-lab/src/test/scala/vn/scalaeng/sparkperf/PerformanceSuite.scala` (13 test)
- `09-spark/spark-performance-lab/evidence/README.md`
- `09-spark/spark-performance-lab/evidence/performance-demo.txt`
- `09-spark/spark-performance-lab/evidence/lab-test.txt`
- `09-spark/spark-performance-lab/evidence/solutions-test.txt`
- `09-spark/spark-performance-lab/evidence/memory.txt`
- `09-spark/spark-performance-lab/evidence/serializers.txt`
- `09-spark/spark-performance-lab/evidence/dependency-poms.json`
- `solutions/ch25-solutions.md`
- `solutions/ch25-code/project.scala`
- `solutions/ch25-code/src/main/scala/vn/scalaeng/solutions/ch25/Ch25Solutions.scala`
- `solutions/ch25-code/src/test/scala/vn/scalaeng/solutions/ch25/Ch25SolutionsSuite.scala` (6 test)
- `.team/handoffs/T12-ch25-spark-performance.md` (file này).

Build/log/temp/Parquet/local shuffle trong `.cache/t12/`; dependency cache trong `.cache/coursier/` theo `.gitignore` có sẵn, không sửa `.gitignore`. Evidence nhỏ lưu trong lab để review không phụ thuộc cache bị dọn.

## 2. Lệnh và kết quả chạy THẬT

Working directory: `C:/Users/VuTT/zero_to_hero_scala`. Môi trường JVM thực: Windows 11 amd64, JDK **21.0.12.1**, `availableProcessors=12`, local master **`local[2,3]`**, một JVM có hai task thread. PerformanceDemo heap tối đa 8.547.991.552 byte; không kiểm soát tải nền toàn máy. Không phải benchmark cluster hoặc median nhiều lượt.

### Lượt test cuối — exit 0, 19 pass, 0 fail, 0 ignored

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode tests
```

Script chạy đúng **hai lệnh Scala CLI test**, tuần tự (không dùng Bloop):

```text
scala-cli.bat test C:\Users\VuTT\zero_to_hero_scala\09-spark\spark-performance-lab --server=false --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t12/lab-test-build --java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t12/tmp --java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t12/warehouse
scala-cli.bat test C:\Users\VuTT\zero_to_hero_scala\solutions\ch25-code --server=false --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t12/solutions-test-build --java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t12/tmp --java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t12/warehouse
```

`COURSIER_CACHE=<repo>/.cache/coursier`, `SPARK_LOCAL_DIRS=<repo>\.cache\t12\local`; script tạo `.cache/t12/tmp` và local trước. Output gốc được trích:

```text
Test run vn.scalaeng.sparkperf.PerformanceSuite started
vn.scalaeng.sparkperf.PerformanceSuite: finished 39.191s
Test run vn.scalaeng.sparkperf.PerformanceSuite finished: 0 failed, 0 ignored, 13 total 39.196s

Test run vn.scalaeng.solutions.ch25.Ch25SolutionsSuite started
vn.scalaeng.solutions.ch25.Ch25SolutionsSuite: finished 46.076s
Test run vn.scalaeng.solutions.ch25.Ch25SolutionsSuite finished: 0 failed, 0 ignored, 6 total 46.082s
```

Hai log cuối **0 compiler warning**, đã kiểm tra không có `warning:`, `warnings found`, `Compilation failed`. Có hint Scala CLI gợi ý munit mới hơn và WARN Hadoop thiếu winutils/native library; không thay munit chuẩn repo, không phải compiler warning. Ghi/đọc Parquet đã pass thật qua BareLocalFileSystem. Retry test có ERROR task exception chủ động tiêm; suite vẫn pass và đếm đúng một failed attempt.

13 test lab bao phủ: lazy action; barrier listener chậm; loại group không liên quan và restore properties khi lỗi; ba final join strategy + cùng dữ liệu; AQE initial SMJ→final SHJ khi tắt broadcast; 200/8/AQE coalesce cùng kết quả; key nóng và hash salt; multiset/revenue của salted INNER join; cache default/cleanup; số file Parquet + đọc lại; serializer payload size; builtin/UDF Parquet pushdown; retry attempt và sum.

6 test lời giải bao phủ 6 bài, gồm plan chưa final, stage toàn lỗi/median=0, byte-size Long.MaxValue, dimension duplicate/null/empty, DECIMAL tổng Long.MaxValue+1/null/empty, và cache cleanup trên consumer lỗi sau materialization.

### PerformanceDemo — exit 0

Đã chạy trực tiếp, trước các lượt test cuối:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:SPARK_LOCAL_DIRS = "$PWD\.cache\t12\local"
scala-cli.bat run 09-spark/spark-performance-lab --server=false --workspace .cache/t12/lab-build --main-class vn.scalaeng.sparkperf.PerformanceDemo --java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t12/tmp --java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t12/warehouse
```

Log `.cache/t12/demo.log`; bản từ ENV trở đi ở `evidence/performance-demo.txt`. Mode `demo` trong verify.ps1 là wrapper tương ứng, chưa gọi riêng mode đó. Kết quả chính:

- Lazy trước=0, sum=10100, sau=100.
- Ba hint cho final BHJ / SMJ / SHJ, cùng 100.000 dòng; thời gian 1108 / 2033 / 696 ms. BHJ shuffle read/write=0 nhưng vẫn có BroadcastExchange, **không tuyên bố byte broadcast=0**.
- Aggregate 200 AQE off → 200 partition, 208 task, 1882 ms; 8 off → 8/16/380 ms; 200 AQE on → 1/9/401 ms.
- Hash key nóng: `1000,2000,2000,0,0,92000,1000,2000`; salt: `1508,1229,881,12067,1263,24899,35174,22979`. Salt không bảo đảm đều.
- SMJ skew off / salted / AQE skew: 701 / 891 / 473 ms. Stage cuối max/upper median: 192/40, 120/59, 94/18 ms; task cuối 8/8/15. Final AQE có **`SortMergeJoin(skew=true)` + `AQEShuffleRead skewed`**. Ngưỡng factor=2, byte=1024, advisory=4096, coalesce=false, forceOptimize=true chỉ để tái hiện trên local; không đề xuất production.
- Cache dùng một lần: không 582 vs có 1477 ms; hai lần: 609 vs 772 ms, tính cả materialization. Kết quả hai bên bằng nhau; không hứa cache dùng lại hai lần sẽ nhanh hơn.
- MEMORY_AND_DISK default Dataset thể hiện `StorageLevel(disk, memory, deserialized, 1 replicas)`.
- Repartition 8 vs 2 → 8 vs 2 file Parquet trong điều kiện writer cụ thể; không suy cho mọi partitionBy/writer.
- Payload array EventRecord 1000 phần tử: Java 27156 byte, Kryo 20004 byte; không biến thành tuyên bố tăng tốc RDD mọi trường hợp.
- Parquet filter builtin/UDF cùng 100 dòng; builtin PushedFilters có LessThan, UDF rỗng. UDF 214 ms, builtin 417 ms trong lượt nhỏ này; bài ghi nguyên, không sửa thành thứ hạng mong muốn.
- Retry: sum=5050, tasks=3, failed=1, attempt 1 thành công; lỗi thật `IllegalStateException: CH25_TRANSIENT_TASK_FAILURE`.

### MemoryDemo — exit 0, bounded JVM, không OOM

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode memory
```

Thực thi `scala-cli.bat run <lab> --main-class vn.scalaeng.sparkperf.MemoryDemo --java-opt=-Xmx768m` cùng `--server=false`, workspace `memory-build`, tmp/warehouse và biến env như trên. Trong context: fraction=0.2, force spill threshold=10000. Output thật:

```text
MEMORY heap=805306368 rows=200000 ms=3096 tasks=4 failed=0 taskMedianMs=1475 taskMaxMs=1480 gcMs=60 memorySpill=40514368 diskSpill=209862 shuffleRead=1037666 shuffleWrite=1037666
```

Log `.cache/t12/memory.log`, trích `evidence/memory.txt`. **Spill cưỡng bức, không phải chứng minh thiếu heap tự nhiên.** GC cộng theo task có thể chồng pause trong cùng JVM; không lấy gcMs / wall-clock làm tỷ lệ GC độc quyền.

### Java/Kryo RDD — hai JVM riêng, đều exit 0

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode serializers
```

Thực thi hai `scala-cli.bat run <lab> --main-class vn.scalaeng.sparkperf.SerializationDemo --java-opt=-Dch25.serializer=Java` và `...=Kryo`, mỗi lần context mới và common options như trên. Output thật:

```text
RDD serializer=Java rows=10000 ms=1927 tasks=8 failed=0 taskMedianMs=276 taskMaxMs=658 gcMs=0 memorySpill=0 diskSpill=0 shuffleRead=75996 shuffleWrite=75996
RDD serializer=Kryo rows=10000 ms=1734 tasks=8 failed=0 taskMedianMs=247 taskMaxMs=493 gcMs=24 memorySpill=0 diskSpill=0 shuffleRead=72960 shuffleWrite=72960
```

Log `.cache/t12/serializer-{Java,Kryo}.log`, trích `evidence/serializers.txt`. Không kết luận tỷ lệ tốc độ ổn định từ hai lượt. Spark SQL dùng Encoder/biểu diễn nội bộ, không hứa spark.serializer thay mọi SQL operator.

### Các lượt trước và kiểm tra tĩnh

- Lần đầu dùng `-Wunused:all` đúng chuỗi chuẩn Scala 3 bị Scala 2.13 từ chối: `scalac error: 'all' is not a valid choice for '-Wunused'`. Đã dùng **`-Wunused:_`** (mọi nhóm trên compiler Scala 2.13) ở CLI, sbt và lời giải, thêm `-deprecation -feature -Xlint:_`.
- Compile test ban đầu gặp Compare giữa boxed java.lang.Long của accumulator và Scala Long: sửa `.longValue()` trong assert.
- Lượt 12 test đầu pass nhưng có deprecation warning Array→varargs; sửa `.toIndexedSeq`, thêm test UDF, chạy cuối 13+6 như trên không cảnh báo.
- Chạy script trực tiếp gặp execution policy Windows; dùng `powershell.exe -NoProfile -ExecutionPolicy Bypass -File ...`, chỉ áp dụng tiến trình con, không đổi policy máy.
- Script Python đọc POM XML (kể cả version kế thừa từ parent), tính SHA-256, kiểm tra 17 số mục liên tiếp, local links và log test không warning → pass. Log PowerShell con UTF-16 được decode đúng trước khi xuất evidence UTF-8.
- `git diff --check` trong phạm vi không báo lỗi (các file T12 đang untracked); không stage/commit và không chỉnh thay đổi concurrent của agent khác.

## 3. Phiên bản và nguồn đã kiểm chứng — ngày 17/10/2026

| Thành phần | Chốt | Nguồn và cách kiểm chứng |
|---|---|---|
| Scala | 2.13.18 | [Release chính thức](https://www.scala-lang.org/news/2.13.18/), POM scala-library trong Coursier cache, compiler chạy thật |
| JDK | baseline 21; runtime 21.0.12.1 | Directives jvm 21 và ENV từ JVM chạy; không đổi chuẩn repo |
| Spark SQL/Core | 4.2.0 `_2.13` | [Spark overview](https://spark.apache.org/docs/4.2.0/) xác nhận Scala 2.13/JDK 21; [POM](https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/4.2.0/spark-sql_2.13-4.2.0.pom) đọc bản cached, compile/run SQL/RDD thật |
| munit | 1.2.4 `_2.13` | [POM](https://repo1.maven.org/maven2/org/scalameta/munit_2.13/1.2.4/munit_2.13-1.2.4.pom) đọc cached, chạy framework thật 13+6 test |
| hadoop-bare-naked-local-fs | 0.1.0 | [Source/README chính thức](https://github.com/globalmentor/hadoop-bare-naked-local-fs), [POM](https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom) cached; cấu hình + ghi/đọc Parquet thật trên Windows |
| sbt | 1.12.11 | [GitHub release](https://github.com/sbt/sbt/releases/tag/v1.12.11) đã mở kiểm tra; chưa chạy launcher/test trong task này |

Không suy hỗ trợ Scala 3 từ artifact Scala 2.13. HTTP trực tiếp tới Maven ở phiên này lỗi kết nối hoặc web fetch không truy cập được; đã đọc POM **trong cache**, không nhận là tải mới. `evidence/dependency-poms.json` lưu link, version, SHA và cách kiểm chứng. Không thêm thư viện ngoài stack đã dùng ở Ch23.

Nguồn cơ chế đã mở: [SQL tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html), [hints](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-hints.html), [tuning serialization/memory](https://spark.apache.org/docs/4.2.0/tuning.html), [Dataset API](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html), [SQL guide](https://spark.apache.org/docs/4.2.0/sql-programming-guide.html), [monitoring](https://spark.apache.org/docs/4.2.0/monitoring.html), [configuration](https://spark.apache.org/docs/4.2.0/configuration.html), [AsyncEventQueue v4.2.0](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/AsyncEventQueue.scala).

## 4. Đề xuất cho lead — KHÔNG tự sửa file dùng chung

- **PROGRESS.md:** thêm Ch25, Codex/Claude, bài+lab+6 lời giải, Scala CLI 13+6 pass, local experiments chạy thật; trạng thái chờ review chéo và sbt, chưa ✅.
- **Glossary:** final executed plan, build/probe side, skew/straggler, salting, coalesced shuffle read, materialization/eviction, spill, task attempt/recomputation. Chương đã có bảng thuật ngữ; lead chọn phần chưa trùng.
- **Compatibility matrix:** nhánh Spark giữ Scala 2.13.18 / Spark 4.2.0 `_2.13` / JDK 21 / munit 1.2.4 / sbt 1.12.11. Ghi ngoại lệ cú pháp compiler `-Wunused:_` cho 2.13 thay `-Wunused:all` (Scala 3); 15 add-opens và BareLocalFileSystem đã kiểm chứng CLI/Parquet trên Windows. Không đánh dấu sbt đã chạy từ kết quả CLI.

## 5. Chưa kiểm chứng và giới hạn

- **sbt.bat test:** chưa chạy trong task này; AGENTS đã ghi launcher sandbox lỗi BootServerSocket AccessDenied, dùng CLI và để Claude chạy sbt ngoài sandbox. Đã cung cấp build.sbt + properties, không giả vờ có sbt summary.
- Review chéo chưa thực hiện; không tự gắn ✅.
- Không cluster nhiều máy, không executor/process death, FetchFailed recovery, mất cache do mất máy, dynamic allocation; retry chỉ task exception trong local JVM. Không OOM, không log GC/JFR chi tiết.
- Không mở Spark UI/History Server; hướng dẫn theo docs, bằng chứng từ listener. Không chạy Linux/macOS.
- Các cấu hình chỉ một lượt, có warm-up nhỏ ở PerformanceDemo nhưng không đảo thứ tự/lặp statistically; không kiểm soát tải nền. Thời gian chỉ minh họa cơ chế, không khuyến nghị thứ hạng production.
- Helper đo chỉ action đồng bộ trong thread gọi; không đo Future chưa join hoặc stage chia sẻ giữa job concurrent phức tạp. Marker JobEnd là rào FIFO trên shared queue, timeout báo lỗi; không sleep. Với event drop phải coi lượt đo không hợp lệ.
- `saltedJoin` chỉ INNER equijoin, dimension unique/non-null. Lời giải validation dimension có chi phí riêng và giả định snapshot ổn định. SUM Decimal yêu cầu tổng/partial vừa 38 chữ số, giữ ANSI.

## 6. Những điểm cần reviewer soi kỹ

1. **Listener barrier:** group→stage mapping, marker cùng shared queue, timeout/cleanup/properties và test listener chậm. Khác lỗi M3 Ch22: không dùng sleep và có loại job khác group. Xem giới hạn concurrent/shared-stage đã ghi, không diễn giải thành helper tổng quát cho mọi async action.
2. **Final plan:** `collect` trên chính DataFrame được đọc plan; classifier bỏ initial section; ba hint kiểm tra final operator. BroadcastExchange không bị tính là shuffle. Test AQE chuyển SMJ→SHJ dù broadcast tắt.
3. **Skew và số đo:** 90% hot key, partition sau salt vẫn lệch, AQE thật có skew=true; so max/median theo stage thay vì tổng job. Salt tổng chậm hơn dù max stage cuối giảm; bài giữ nguyên.
4. **Hợp đồng dữ liệu:** multiset hai chiều và doanh thu cho salted INNER join; không hứa outer join; Decimal cast trước cộng, boundary Long.MaxValue+1/null/empty.
5. **Memory/GC/serialization:** spill được ép có chủ đích, không nhận là OOM; GC sum theo task có thể đếm lặp pause; payload byte không đồng nhất với byte shuffle; Java/Kryo RDD chạy JVM riêng; không quy mọi SQL operator về Kryo.
6. **Cache/UDF:** default Dataset MEMORY_AND_DISK, ghi rõ SQL cache nội bộ; đo cả materialization, cleanup finally; UDF cản pushdown của biểu thức cụ thể, không mọi tối ưu, và không hứa luôn chậm hơn.

Đề nghị lead review và chạy `sbt.bat test` lab, sau đó cập nhật file chung/commit theo AGENTS. Tác giả đã hoàn tất phạm vi được giao và giữ riêng các phần chưa kiểm chứng.
