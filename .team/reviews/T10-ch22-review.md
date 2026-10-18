# Review T10 — Chương 22 (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 5 MAJOR, 3 MINOR; không có BLOCKER.**

Đã đọc handoff, toàn bộ bài, source/test/config/resources của lab và code/test/lời giải Ch22. Bài đủ 17 mục cộng mục 0 chuyển cú pháp Scala 3 → 2.13; giải thích tiếng Việt và các thí nghiệm lazy/cache/retry dễ theo dõi. Build riêng Scala 2.13 đúng quy ước repo. Không sửa file tác giả.

## Kiểm chứng chạy thật — Spark chạy được trong sandbox này

Môi trường: Windows, Scala CLI, Scala 2.13.18, JDK 21, Spark **4.2.0**, munit 1.2.4. Đặt:

```powershell
$env:COURSIER_CACHE = 'C:/Users/VuTT/zero_to_hero_scala/.cache/coursier'
$env:SPARK_LOCAL_DIRS = 'C:\Users\VuTT\zero_to_hero_scala\.cache\review-batch\t10-local'
```

Các lệnh đều thêm:

```text
--server=false
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t10-tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t10-warehouse
```

`--workspace` cũng trỏ vào `.cache/review-batch/` để build/temp/local shuffle/output Parquet nằm trong phạm vi được phép. `.cache/` được ignore.

1. Trong `09-spark/spark-fundamentals-lab`: `scala-cli.bat test . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t10-lab-build` + hai java-opt trên → exit 0. Output thật:

   ```text
   FundamentalsSuite: finished 24.488s
   FundamentalsSuite finished: 0 failed, 0 ignored, 8 total 24.499s
   ```

2. Trong `solutions/ch22-code`: lệnh test tương tự, workspace `t10-sol-build` → exit 0. Output thật:

   ```text
   [bài 2] stage: write=1, distinct+count=3, sort-merge join+count=4
   [bài 4] shuffle.partitions=200 -> 200 partition, 3871 ms
   [bài 4] shuffle.partitions=4 -> 4 partition, 517 ms
   [bài 5] Left(Job aborted due to stage failure: Task 0 in stage 33.0 failed 3 times, ...); số lần thử = 3
   Ch22SolutionsSuite finished: 0 failed, 0 ignored, 6 total 34.384s
   ```

   Ghi Parquet trong test đã chạy thật. Cảnh báo thiếu winutils/native-hadoop không làm test thất bại. Lượt lab ban đầu có cảnh báo Spark nghi đường dẫn local dạng `C:/...` là URI; đổi biến SPARK_LOCAL_DIRS sang đường dẫn Windows có backslash ở các lượt sau. Đây là cấu hình reviewer, không phải lỗi bài.

3. Chạy `FundamentalsDemo` bằng `scala-cli.bat run . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t10-demo-build --main-class vn.scalaeng.spark.FundamentalsDemo` + hai java-opt trên → exit 0. Khớp mục 9: map 0→1.000.000; rows=333334; cache 2.000.000/1.000.000; stage 1/2/2; partition 4/8/2; AQE 200→1; sum=1001000, failedAttempts=1. Kế hoạch narrow/wide khớp cả cấu trúc. RDD ID thay đổi 95/94 → 99/98 là bình thường, bài đã cảnh báo ID không cố định.
4. Probe `.cache/review-batch/t10-probe/Probe.scala`: chạy `scala-cli.bat run .cache/review-batch/t10-probe --server=false --main-class Probe` + hai java-opt trên. Lượt hoàn chỉnh cuối exit 0; log `t10-probe-round2.log`. Đã thử narrow nhiều-parent, một partition, broadcast, skew, overflow, null/empty, listener chậm và thay đổi chiến lược join bằng AQE. Một lượt bổ sung probe từng compile lỗi do reviewer truy cập rpcEnv nội bộ; đã thay bằng SparkConf public rồi chạy lại thành công, không phải lỗi lab.
5. Log lưu tại `.cache/review-batch/t10-lab-test.log`, `t10-sol-test.log`, `t10-demo.log`, `t10-probe.log`, `t10-probe-round2.log`. Một số dòng tiếng Việt bị runner hiển thị sai dấu; các số/plan/error code vẫn kiểm tra được. PowerShell ghi stderr thành NativeCommandError trong log kể cả hint/WARN; kết luận dựa vào exit code và test summary.

Không chạy sbt hay cluster nhiều máy; không tái hiện độc lập các transcript cú pháp sai/thiếu add-opens từ lần biên soạn. Không có compiler warning của lab/lời giải trong các lượt compile; warning gom directives của probe riêng không thuộc code tác giả.

## MAJOR

### M1 — Định nghĩa narrow/wide và suy ra shuffle từ tên phép toán không chính xác

Vị trí: `09-spark/ch22-spark-fundamentals.md:124`, `:125`, `:256`, `:410`.

Bài định nghĩa narrow là “mỗi partition đầu ra chỉ cần một partition đầu vào”, wide là “đầu ra cần nhiều đầu vào”. Probe `parallelize(...,4).coalesce(1,shuffle=false)` có **NarrowDependency** với `getParents(0).size=4`. Đây chính là coalesce được bài dạy không shuffle, nhưng lại bị định nghĩa trên xếp vào wide.

Các khẳng định đi kèm cũng cần giới hạn theo kế hoạch vật lý:

- `range(...,numPartitions=1).groupBy(id % 3).count()` có hai HashAggregate nhưng **không có Exchange**.
- `range(...,numPartitions=1).count()` chỉ **1 stage**, không phải mọi count đều có 2 stage như cách hỏi/đáp hiện tại dễ khiến người mới hiểu.
- Broadcast join có `BroadcastExchange`, **không có shuffle Exchange**. Chỉ tìm substring “Exchange” không phân biệt được broadcast với shuffle.

Đề xuất: định nghĩa dependency qua quan hệ phân phối partition cha/con; narrow có thể nhiều-cha→một-con (coalesce), không có sự phân tán lại bản ghi theo khóa sang nhiều partition con. Phân biệt logical groupBy/join với ShuffleExchangeExec được planner chèn khi partitioning chưa đáp ứng. Gọi các số stage 1/2/2 là kết quả **của query/cấu hình lab**. Thêm phản ví dụ coalesce, một partition và broadcast vào bài/test; khi đọc plan phân biệt BroadcastExchange với Exchange hashpartitioning/SinglePartition.

Nguồn tra 17/10/2026: [Dataset.coalesce](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html) nêu rõ narrow và ví dụ 1000→100 partition, mỗi con nhận 10 cha; [Performance tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html) mô tả join strategies và tránh shuffle.

### M2 — Repartition theo hash không bảo đảm phân bố đều

Vị trí: `09-spark/ch22-spark-fundamentals.md:172` và `:173`.

Bảng trả lời “Đều (theo hash/round-robin)” rồi khuyến nghị cả `repartition($"day")`. Probe 100 dòng có cùng `k="X"`, `repartition(8,col("k"))` trả số dòng từng partition:

```text
List((0,0), (1,100), (2,0), (3,0), (4,0), (5,0), (6,0), (7,0))
```

Hash đưa mọi bản ghi cùng key về một partition; key nóng làm skew, không được chữa chỉ bằng tăng n.

Đề xuất: tách `repartition(n)` round-robin (cân bằng tương đối số record, không bảo đảm byte/chi phí) với `repartition(n,cols...)` hash (phụ thuộc phân bố key). Thêm ví dụ một ngày/campaign chiếm đa số và nối sang skew ở chương tối ưu. [Dataset.repartition](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html) chỉ cam kết hash partitioning cho overload theo biểu thức (tra 17/10/2026).

### M3 — stagesOf có race: chờ 500 ms không có nghĩa listener đã xử lý xong

Vị trí: `09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/Experiments.scala:59` và `:65`; `09-spark/ch22-spark-fundamentals.md:337`.

Probe đăng ký một SparkListener đứng trước helper trên hàng đợi shared, chặn callback onStageCompleted bằng latch. Action `range(...,1 partition).collect()` **đã hoàn thành**, callback blocker cũng đã vào; `Experiments.stagesOf` vẫn trả **0** sau 500 ms. Release latch sau khi nhận số đo để kết thúc sạch. Trước đó cùng query count một partition đo đúng 1 khi listener không bị chậm. Đây là counterexample xác định, không phải đoán máy chậm. Helper còn đếm mọi stage trên SparkContext, kể cả job không thuộc action đang đo.

Đề xuất: gắn job group/local property duy nhất, theo dõi jobIds/stageIds tương ứng, chờ sự kiện kết thúc của các job liên quan bằng latch/promise với timeout có báo lỗi. Không trả một con số hợp lệ nếu chưa đồng bộ xong; không tăng sleep như cách chữa chính. Thêm regression listener chậm và job khác chạy đồng thời. [Spark AsyncEventQueue 4.2.0](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/AsyncEventQueue.scala) cho thấy dispatch bất đồng bộ và cơ chế đếm event đang xử lý (tra 17/10/2026).

### M4 — Bài 6: RDD và DataFrame khác hành vi overflow; RDD trả doanh thu âm

Vị trí: `solutions/ch22-code/src/main/scala/vn/scalaeng/solutions/ch22/Ch22Solutions.scala:110` và `:116`; `solutions/ch22-solutions.md:82`.

Input cùng SKU A, amount Long.MaxValue và 1:

```text
RDD:       Map(A -> -9223372036854775808)
DataFrame: org.apache.spark.SparkArithmeticException: [ARITHMETIC_OVERFLOW] overflow. ... SQLSTATE: 22003
```

Probe xác nhận `spark.sql.ansi.enabled=true`. Cộng Scala `_ + _` trong RDD wrap-around; SQL sum kiểm tra overflow theo ANSI. Vì vậy “kết quả bằng groupBy của DataFrame” chỉ đúng trên miền tổng vừa Long, chưa phải hợp đồng chung của hai implementation.

Đề xuất: định nghĩa miền số tiền và policy overflow, dùng BigInt/Decimal phù hợp hoặc checked addition + lỗi có cấu trúc nhất quán. Không tắt ANSI để làm cả hai cùng trả số sai. Test sát Long.MaxValue, tổng vượt biên, nhiều partition và cancellation. Với minh họa Monoid ở mục 6, cũng giới hạn theo kiểu số: sum của số floating-point không kết hợp chính xác theo số học máy, không thể hứa bit-for-bit giống nhau ở mọi cách chia.

Nguồn tra 17/10/2026: [Spark ANSI Compliance](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html), mục Arithmetic Operations/try_sum. Probe null key và input rỗng ở cả hai implementation **đều khớp** (Map có key null và Map rỗng); không ghi chúng thành lỗi.

### M5 — Tắt broadcast không “buộc sort-merge join”

Vị trí: `solutions/ch22-code/src/main/scala/vn/scalaeng/solutions/ch22/Ch22Solutions.scala:42`; `solutions/ch22-solutions.md:33`.

`autoBroadcastJoinThreshold=-1` chỉ tắt automatic broadcast ở đường cấu hình đó, không loại shuffled hash join. Probe đúng hình dạng hai bảng của StageCounts, giữ broadcast=-1 nhưng đặt `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=134217728`: initial plan là SortMergeJoin, **final plan sau collect là ShuffledHashJoin**. Code hiện chỉ đếm stage, không kiểm tra loại join, nên không chứng minh nhãn “sort-merge” của thí nghiệm.

Đề xuất: diễn đạt “tắt automatic broadcast; với cấu hình mặc định của thí nghiệm này planner chọn sort-merge”, hoặc cố định các cấu hình/hint cần thiết và kiểm tra **final executed plan**. Nêu AQE có thể thay chiến lược. Test số stage dùng bất đẳng thức có thể vẫn xanh dù chiến lược đã khác. [Spark: Converting sort-merge join to shuffled hash join](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html#converting-sort-merge-join-to-shuffled-hash-join) (tra 17/10/2026).

## MINOR

### m1 — shuffle.partitions không điều khiển mọi shuffle

Vị trí: `09-spark/ch22-spark-fundamentals.md:177`.

“Số partition sau mỗi shuffle của DataFrame” quá rộng: chính demo có repartition(8), còn tổng hợp toàn cục cần SinglePartition=1. AQE có thể đổi số sau chạy; bài đã giải thích phần này tốt.

Đề xuất: nói đây là mặc định dùng cho shuffle join/aggregation khi không có chỉ định khác; phân biệt partition đích explicit, số partition shuffle ban đầu và số partition sau AQE. [Spark configuration](https://spark.apache.org/docs/4.2.0/configuration.html), [SQL performance tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html) (tra 17/10/2026).

### m2 — Local mode vẫn dùng socket; một SparkContext có thể có nhiều SparkSession

Vị trí: `09-spark/ch22-spark-fundamentals.md:94` và `:245`.

Không có mạng **giữa các máy** trong local lab không có nghĩa không cần socket: probe SparkConf có driver host/port thật (`nitro-5:51868`, port thay đổi mỗi lượt). Quy tắc “một JVM chỉ nên có một SparkSession” nên phân biệt với yêu cầu một SparkContext hoạt động; `newSession()` tạo session SQL độc lập dùng chung context là API hợp lệ. Tắt parallelExecution cho suite vẫn là lựa chọn hợp lý để tránh cấu hình/cache dùng chung.

Đề xuất: sửa thành “driver/executor cùng JVM, không có executor trên máy khác, vẫn khởi tạo dịch vụ mạng cục bộ”; dùng thuật ngữ SparkContext đúng ở quy tắc lifecycle và giải thích vì sao suite dùng một session. [Spark RDD guide](https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html#initializing-spark), [Spark configuration](https://spark.apache.org/docs/4.2.0/configuration.html) (tra 17/10/2026).

### m3 — Nên diễn đạt cache/persist theo storage level và chi phí

Vị trí: `09-spark/ch22-spark-fundamentals.md:207` và `:215`.

“Mỗi action tính lại từ đầu” và “action sau đọc từ bộ nhớ” đúng với phép đo hiện tại nhưng không phải bảo đảm chung. Dataset.cache mặc định MEMORY_AND_DISK; cache có thể bị evict/mất và tính lại, còn dữ liệu lớn hơn RAM vẫn có thể đáng persist nếu tính lại đắt. Bài có nhắc đẩy ra đĩa nhưng chưa nêu default/storage level dù mục tiêu có persist.

Đề xuất: bổ sung 2–3 câu về mặc định Dataset so với RDD, đọc cả RAM/đĩa, eviction và lựa chọn dựa trên đo chi phí; giữ các số 2N/N là quan sát của thí nghiệm. Có thể đặt unpersist trong finally ở helper để dọn cả khi action lỗi. [Dataset.cache/persist](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html) (tra 17/10/2026).

## Nguồn và giới hạn còn lại

Đã kiểm tra [Spark 4.2.0 overview](https://spark.apache.org/docs/4.2.0/) xác nhận Scala 2.13 và JDK 21; build/test thực tế xác nhận artifact 4.2.0 hoạt động trong môi trường này. [GlobalMentor README](https://github.com/globalmentor/hadoop-bare-naked-local-fs) khớp version 0.1.0 và cách cấu hình BareLocalFileSystem; test ghi Parquet đã pass. Phần accumulator action/transformation của bài khớp [RDD guide](https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html#accumulators). Ngày tra: 17/10/2026.

Để review lại: sửa M1–M5 ở bài/code/lời giải liên quan, thêm regression cho listener chậm và arithmetic/plan edge cases, chạy lại 8+6 test cùng test mới; lead chạy sbt cuối. Sandbox không phải trở ngại ở lượt review này.
