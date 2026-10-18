> Phiên bản: Scala 2.13.18 · JDK 21 (chạy 21.0.12.1) · Spark SQL/Core 4.2.0 `_2.13`, munit 1.2.4, hadoop-bare-naked-local-fs 0.1.0 · Trạng thái: Đã biên dịch & chạy bằng Scala CLI ngày 17/10/2026; chờ Claude review chéo và chạy sbt 1.12.11.

# Chương 25 — Spark Performance: từ triệu chứng đến bằng chứng

## 1. Mục tiêu học tập

Sau bài này, bạn có thể giải thích một job chậm bằng dữ liệu: công việc nào chưa chạy vì lazy evaluation, operator nào gây shuffle, partition nào giữ key nóng, và vì sao cache hoặc broadcast có thể làm tình hình tệ hơn. Bạn biết đọc **final executed plan** (kế hoạch thực thi cuối cùng) khi AQE thay chiến lược; đo task theo từng stage; đối chiếu kết quả trước/sau tối ưu để tránh một job nhanh nhưng sai doanh thu.

Đích đến là một quy trình: đặt giả thuyết → giữ nguyên dữ liệu và hợp đồng → đổi một yếu tố → đo → kiểm tra tính đúng → quyết định. Không có bộ cấu hình tốt cho mọi đơn hàng, mọi executor hay mọi cluster.

## 2. Kiến thức tiên quyết

Đọc [Ch22](ch22-spark-fundamentals.md) về job/stage/task, lineage và local mode, [Ch23](ch23-spark-sql-dataframes.md) về SQL/null/plan, và Ch24 về đầu vào/đầu ra pipeline batch. Ôn JVM/GC ở Ch11 và schema/Parquet ở Ch21. Trong bài, **fact** là dòng đơn hàng; **dimension** là bảng khách hàng dùng để bổ sung segment.

Nhánh Spark dùng Scala **2.13**, build riêng với backend Scala 3. Viết `import ..._`, `{ ... }`, `object ... { def main(args: Array[String]): Unit = ... }`. Không đổi directives sang Scala 3. Spark 4.2.0 công bố Java 17/21/25 và Scala 2.13; bài chọn JDK 21 theo repo. [Spark overview](https://spark.apache.org/docs/4.2.0/).

## 3. Lý thuyết

### 3.1. Lazy evaluation và query plans: đo đúng công việc

**Vấn đề và nguyên nhân.** `filter`, `select`, `join` xây mô tả; đo thời gian tạo `val df = ...` chủ yếu đo việc dựng plan. Action như `collect`, `count`, ghi file mới yêu cầu kết quả. Catalyst có thể loại cột không cần thiết: `df.count()` không nhất thiết tính một biểu thức đắt chỉ có trong cột đầu ra. Vì vậy đo `count` rồi tuyên bố tốc độ cho `write` là lỗi thiết kế thí nghiệm.

**Cách xác định.** Xem logical plan (mô tả quan hệ), optimized logical plan (đã biến đổi), physical plan (cách chạy) bằng `df.explain("extended")`; dùng `formatted` để đọc operator/thuộc tính. AQE — Adaptive Query Execution, tối ưu thích nghi — dùng thống kê khi chạy nên plan trước action chưa đủ. Lab lấy `df.queryExecution.executedPlan` trước và sau **`df.collect()` trên chính df đó**. Tìm `isFinalPlan=true`, đọc phần `== Final Plan ==`, không tìm tên join trên cả chuỗi vì phần initial có thể còn tên cũ.

**Giải pháp và lựa chọn khác.** Chọn action đại diện cho sản phẩm thật, giới hạn dữ liệu khi thu về driver. `execute` trong lab chỉ collect 100.000 dòng hẹp để đọc plan và so kết quả; production nên đo action ghi dữ liệu hoặc tổng hợp đủ nhỏ. Dùng RDD/accumulator trong ví dụ lazy để quan sát 0→100 lần gọi; đây là quan sát của một lần chạy không lỗi, không phải giao kèo exactly-once cho side effect trong transformation.

**Khi không áp dụng.** Không thêm `collect` toàn bảng production để “lấy plan cuối”; nó có thể gây driver OOM. Không chèn action giữa mỗi transformation: bạn vừa thay đổi workload cần đo, vừa có thể tính lại nhiều lần. Xem thêm [Dataset API](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html).

### 3.2. Shuffle, partition sizing và small files

**Vấn đề và nguyên nhân.** Quá nhiều partition nhỏ làm chi phí lập lịch, mở file và lưu metadata lấn át công việc hữu ích. Quá ít partition làm task giữ quá nhiều dữ liệu, tăng sort/hash state, spill và đuôi thời gian chạy. Shuffle phân phối lại bản ghi theo yêu cầu của operator; nó tốn serialize, ghi/đọc và truyền dữ liệu. Local mode vẫn có I/O và dịch vụ socket, nhưng không đo mạng giữa các máy.

**Cách xác định.** Đọc `Exchange hashpartitioning(...)` hoặc `Exchange SinglePartition` để tìm shuffle; `BroadcastExchange` có mục đích khác. `groupBy` là phép toán logical, không bảo đảm lúc nào cũng xuất hiện shuffle nếu phân phối đầu vào đã đáp ứng. Narrow dependency có thể gom nhiều partition cha vào một con như `coalesce` không shuffle; không phân loại chỉ bằng “con cần nhiều cha”.

Đối chiếu số task, byte shuffle và phân bố kích thước/thời gian từng task trong stage. Thử nghiệm 100.000 dòng của bài: cấu hình 200, AQE tắt có 208 task; 8 có 16; 200 với AQE có 9. Đây là số task của query cụ thể, gồm map và reduce, không phải quy luật số stage/task của mọi `count`.

**Giải pháp.** Ước lượng ban đầu `ceil(shuffleBytes / targetBytes)` rồi kiểm tra độ lệch, CPU và bộ nhớ. Đây là ước lượng theo byte, không phải hằng số “128 MB luôn tốt”. `spark.sql.shuffle.partitions` là mặc định cho một số shuffle SQL như join/aggregation; `repartition(n)` explicit, global aggregate và AQE có thể cho số khác. AQE coalesce gộp các partition shuffle nhỏ dựa trên runtime statistics. Quan sát `AQEShuffleRead coalesced` và số partition thực sau action.

`repartition(n)` dùng round-robin để phân phối tương đối theo số dòng; `repartition(n, col("customer_id"))` dùng hash theo key. Hash không bảo đảm cân bằng byte hoặc số dòng: mọi bản ghi cùng key vẫn về cùng partition. `coalesce(n)` giảm partition với narrow dependency, ít chi phí hơn nhưng có thể gom thành ít task rất nặng. Tăng n không chữa được một key chiếm 90%.

**Đầu ra.** Lab ghi bảng không `partitionBy`, AQE tắt, không `maxRecordsPerFile`; 8 task ghi ra 8 file Parquet, 2 ra 2. `partitionBy(day)`, task rỗng, giới hạn record/file và chế độ writer khác có thể thay quan hệ này. Chọn số file theo reader downstream và dung lượng thật; giảm số file có thể phải shuffle thêm. Không dùng `coalesce(1)` cho mọi output lớn: bạn tạo nút thắt một task ghi.

**Khi không áp dụng.** Không giảm partition chỉ vì bảng nhỏ sau aggregate nếu các task trước đó đang spill; phân biệt partition đầu vào, partition shuffle và partition ghi. Không bật/tắt AQE mà không kiểm tra final plan. Các tham số coalesce và join thích nghi ở [SQL tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html).

### 3.3. Join strategies và broadcast

**Vấn đề và nguyên nhân.** Hai bảng cần ghép theo `customer_id`; chi phí phụ thuộc kích thước sau filter/project, phân bố key, join type và thống kê, không chỉ số dòng. Bảng ít dòng nhưng mỗi dòng có payload rất lớn vẫn nguy hiểm để broadcast.

| Chiến lược | Cách hoạt động | Ưu điểm / chi phí | Khi không chọn |
|---|---|---|---|
| BroadcastHashJoin | Đưa build side đến các executor, lập hash table, probe bên còn lại | Có thể tránh shuffle của probe side; tốn bộ nhớ bản sao, thời gian broadcast | Build side không đủ nhỏ, thiếu RAM hoặc kiểu join không hỗ trợ build side đó |
| SortMergeJoin | Căn partition theo key rồi sort hai phía, merge các luồng có thứ tự | Phù hợp equijoin lớn, sort có thể spill; tốn shuffle/sort khi đầu vào chưa đáp ứng | Bảng dimension thật sự nhỏ và broadcast có lợi; key nóng vẫn tạo task dài |
| ShuffledHashJoin | Căn partition theo key, lập hash table build side trong từng partition | Tránh sort; cần hash table cục bộ vừa bộ nhớ | Partition build side quá lớn hoặc skew làm state phình lên |

**Cách xác định.** Lab bật AQE, đặt cả ngưỡng broadcast thường và adaptive bằng `-1`, ngưỡng adaptive shuffled hash bằng `0`, rồi dùng lần lượt hint `broadcast`, `merge`, `shuffle_hash` trên dimension. Hint broadcast vẫn có thể được dùng dù automatic broadcast bị tắt; hint nói ưu tiên và còn chịu điều kiện hỗ trợ của join. Test kiểm tra operator trong **phần final** và so nội dung kết quả cả ba cách.

**Giải pháp và thay thế.** Project/filter sớm để giảm build side; cập nhật thống kê của bảng khi dùng catalog; để planner/AQE chọn trước khi ép hint lâu dài. Lab có regression không hint: giữ automatic broadcast tắt nhưng đặt `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=134217728`; initial SMJ chuyển thành final shuffled hash. Chỉ đặt `autoBroadcastJoinThreshold=-1` không buộc SMJ.

**Khi không áp dụng.** Không sao chép hint chỉ vì lượt lab shuffled hash chạy nhanh nhất. Không tăng broadcast timeout để chữa thiếu heap. Không so byte shuffle=0 với “không truyền dữ liệu”: BroadcastExchange vẫn phân phối bảng; TaskMetrics shuffle không tính byte broadcast. Cơ chế và giới hạn hint được mô tả ở [Join hints](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-hints.html#join-hints).

### 3.4. Data skew: key nóng, AQE và salting

**Vấn đề và nguyên nhân.** Trong bộ đơn hàng, 90.000/100.000 dòng có `customer_id=0`. Cùng một hash key dồn vào một reducer; 2.000 dòng của key khác còn va vào partition đó, nên partition lớn nhất có 92.000 dòng. Dù thêm executor, task giữ key nóng vẫn có thể quyết định thời điểm job kết thúc.

**Cách xác định.** Đếm key, đếm dòng mỗi partition và xem **max/trung vị trong cùng một stage**, cộng byte read và spill theo task. Trung vị toàn job trộn map/reduce không có cùng công việc chỉ là thống kê mô tả, không dùng để kết luận skew. Một task chậm cũng có thể do GC, I/O, retry hay máy lỗi; số dòng skew chưa tự chứng minh thời gian skew.

**Giải pháp thứ nhất — AQE skew join.** Khi partition vượt cả hệ số so với trung vị lẫn ngưỡng byte, AQE có thể chia partition lệch và nhân phần dữ liệu đối ứng. Lab cố tình hạ factor xuống 2, threshold xuống 1.024 byte, advisory size 4.096 byte, tắt coalesce để quan sát rõ, bật `forceOptimizeSkewedJoin`. Final plan thật có `SortMergeJoin(skew=true)` và `AQEShuffleRead skewed`, stage cuối chạy 15 task thay vì 8. Các ngưỡng nhỏ này **chỉ để tái hiện** trên một máy; không chép sang production. Chia theo shuffle map block cũng không bảo đảm đạt đúng advisory size.

**Giải pháp thứ hai — salting (thêm khóa phụ).** Gán mỗi fact **một** salt xác định từ `order_id`, nhân dimension nhỏ thành 8 bản với salt 0…7, join theo `(customer_id,salt)`, rồi bỏ salt. Trên domain INNER equijoin và dimension có key duy nhất/không null, mỗi dòng fact vẫn ghép đúng một khách; test `exceptAll` hai chiều bảo vệ cả số bản sao, đồng thời so doanh thu theo segment. Phân bố sau salt vẫn không đều: 8 salt cùng key có thể hash va nhau vào một reducer.

**Ưu/nhược và lựa chọn khác.** AQE giữ SQL đơn giản nhưng chỉ áp dụng khi operator/điều kiện phù hợp; salting cho phép chủ động phân tải nhưng tăng dữ liệu dimension và độ phức tạp. Broadcast dimension nhỏ có thể tránh nút thắt shuffle join ngay từ đầu; pre-aggregate fact cũng có thể giảm byte nếu hợp đồng cho phép. Đo lại toàn job, gồm chi phí nhân dimension và validation, không chỉ task cuối.

**Khi không áp dụng.** Không dùng helper này như công thức cho outer join: các bản sao dimension không được match có thể làm sai kết quả. Không đổi `avg` thành trung bình của các trung bình; `countDistinct` cũng không cộng từng salt tùy ý. SUM hai tầng cần kiểu số và chính sách overflow: lời giải cast tiền sang DECIMAL(38,0) **trước** cộng, yêu cầu tổng vừa 38 chữ số và giữ ANSI bật. Không tắt ANSI để che tràn Long. Với workload lab, salting làm max task cuối giảm 192→120 ms nhưng tổng thời gian tăng 701→891 ms; đây là lý do không coi mọi cân bằng tốt hơn là job nhanh hơn.

### 3.5. Cache và storage level

**Vấn đề và nguyên nhân.** Nhiều action tái dùng một kết quả đắt có thể lặp tính toán, nhưng materialize cache cũng tốn CPU, bộ nhớ, có thể I/O và cạnh tranh với execution memory của join/sort. Cache một nguồn `range` rẻ chỉ đọc một lần thường không có phần tính lại để tiết kiệm.

**Cách hoạt động và xác định.** `Dataset.cache()` tương đương persist mặc định MEMORY_AND_DISK; cache vẫn lazy. Lần action đầu cần tạo cache; các lần sau có thể đọc RAM/đĩa, hoặc tính lại partition đã mất/evict. Lab in `StorageLevel(disk, memory, deserialized, 1 replicas)`. Đây là cách API biểu diễn level; SQL cache sử dụng cấu trúc nội bộ theo cột, không có nghĩa giữ nguyên mọi object case class của người dùng. RDD `cache()` mặc định MEMORY_ONLY, khác Dataset. [Dataset API](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html).

Xem Storage tab: số partition đã materialize, kích thước, level; xem SQL plan có `InMemoryTableScan`. Lab tính SHA-256 cho 200.000 order ID rồi sum độ dài payload; có/không cache cùng consumers, tính cả lần materialize. Với một consumer, 582 ms không cache và 1.477 ms có cache. Hai consumer trong lượt này vẫn chậm hơn: 609 so với 772 ms. Không có kết luận “hai lần dùng là hòa vốn”; JIT và thứ tự chạy cũng ảnh hưởng số đo.

**Giải pháp và thay thế.** Cache đúng phần đắt dùng lại, sau filter/project khi có thể; chọn level theo chi phí tính lại và dung lượng. Dữ liệu lớn hơn RAM vẫn có thể đáng persist nếu đọc đĩa rẻ hơn tính lại. So với materialize bảng trung gian bền vững, cache tiện và ngắn hạn nhưng có thể bị mất; nó không thay checkpoint/reliable storage.

**Khi không áp dụng.** Không cache mọi DataFrame; không bỏ `unpersist` trong nhánh lỗi. Dùng `try/finally`, unpersist khi consumer cuối xong. Helper bài tập yêu cầu quyền sở hữu cache độc lập để không gỡ cache mà caller khác đang dùng. Không đồng nhất “action sau” với “chắc chắn đọc RAM”.

### 3.6. Serialization, Encoder và UDF

**Vấn đề và nguyên nhân.** Đưa object qua shuffle hoặc lưu RDD serialized cần chuyển thành byte. Object graph nhiều trường/chuỗi có overhead bộ nhớ và GC. Java serializer tiện vì hỗ trợ Serializable; Kryo có thể gọn hơn và cần kiểm tra/đăng ký kiểu tùy biến. Đổi serializer là cấu hình SparkContext lúc khởi tạo, không chỉ `spark.conf.set` giữa hai query.

**Cách xác định.** Lab đo cùng mảng 1.000 `EventRecord`: Java 27.156 byte, Kryo 20.004 byte (đăng ký EventRecord và array). Đó là kích thước payload, không chứng minh một tỷ lệ tăng tốc cho mọi RDD. `SerializationDemo` chạy hai tiến trình mới, một Java và một Kryo, shuffle RDD chứa EventRecord; so row count, byte shuffle và task metrics thực tế ở evidence. Spark có các đường dùng Kryo nội bộ cho kiểu đơn giản dù serializer cấu hình là Java. [Spark tuning](https://spark.apache.org/docs/4.2.0/tuning.html).

**SQL khác RDD object.** Encoder (bộ mã hóa Dataset) chuyển object sang biểu diễn nội bộ của Spark SQL; các operator SQL dùng hàng/cột nội bộ và sinh mã trong hệ thực thi thường gọi là Tungsten. Đổi `spark.serializer` không tự biến mọi SQL expression thành Kryo hoặc chữa một UDF khó tối ưu.

**Giải pháp.** Giữ biểu thức SQL dựng sẵn khi diễn đạt được nghiệp vụ. Với Parquet, so `col("order_id") < 100L` với Scala UDF có cùng điều kiện: plan thứ nhất đẩy `LessThan(order_id,100)` xuống nguồn; plan UDF có `PushedFilters: []`. Catalyst không nhìn được phép so sánh bên trong hàm Scala. UDF không vô hiệu hóa mọi tối ưu của cả query: projection và các điều kiện độc lập khác vẫn có thể được tối ưu. Typed Dataset lambda cũng cần được xem plan, không mặc định tương đương biểu thức cột. [SQL overview](https://spark.apache.org/docs/4.2.0/sql-programming-guide.html).

**Khi không áp dụng.** Không thay mọi hàm tùy biến bằng chuỗi SQL khó kiểm thử; khi nghiệp vụ không có biểu thức tương đương, giữ UDF và đo phạm vi mất tối ưu. Không suy ra UDF luôn chậm hơn từ plan: lượt nhỏ của bài UDF 214 ms, builtin 417 ms do nhiều yếu tố, trong khi bằng chứng về pushdown vẫn rõ ràng. Không so microbenchmark serialize byte với tốc độ cluster. Đăng ký Kryo sai/thiếu kiểu cũng cần test round-trip với domain thật.

### 3.7. Memory pressure, spill và GC

**Vấn đề và nguyên nhân.** Sort/hash cần execution memory, cache giữ storage memory, object ngắn hạn tạo áp lực GC. Nhiều task đồng thời chia cùng heap; byte Parquet nén không đại diện lượng RAM sau giải mã. Driver `collect` lớn là vấn đề khác với executor sort thiếu bộ nhớ.

**Cách xác định.** Trong Spark UI, xem peak execution memory, memory/disk spill, GC time và task dài; ghép thời điểm với log JVM và dữ liệu partition. Lab trả `memoryBytesSpilled`, `diskBytesSpilled`, `jvmGCTime` qua TaskMetrics. Memory-spill là ước lượng dữ liệu trong bộ nhớ bị spill; disk-spill là byte ghi thực tế sau biểu diễn/nén, không đòi hai số bằng nhau. GC tính theo khoảng chạy task; cộng các task chồng nhau trong cùng JVM có thể đếm lại cùng pause, không phải tổng thời gian GC độc quyền hay tỷ lệ GC/wall-clock. [Monitoring](https://spark.apache.org/docs/4.2.0/monitoring.html).

**Thí nghiệm có giới hạn.** `MemoryDemo` chạy JVM riêng `-Xmx768m`, `spark.memory.fraction=0.2`, ép ExternalSorter spill mỗi 10.000 phần tử; 200.000 record, hai partition. Nó cho phép quan sát spill mà không cố đánh sập tiến trình. Đây là **spill cưỡng bức**, không chứng minh heap đã cạn. Không có OOM trong bài; cơ chế executor chết vì OOM trên cluster chưa tái hiện. Kết quả thật nằm trong mục 9/evidence.

**Giải pháp và lựa chọn khác.** Giảm dữ liệu trước sort/join, giảm payload và object graph, tăng partition khi state mỗi task quá lớn, sửa key nóng nếu tăng partition không chia được state. Cân nhắc bỏ cache ít giá trị, serialized RDD storage, hoặc tăng bộ nhớ sau khi đã ước lượng theo số task đồng thời. Spill là cơ chế hoàn thành công việc khi RAM hạn chế; một ít spill không tự động là lỗi cần xóa bỏ.

**Khi không áp dụng.** Không tăng heap vô hạn, tắt GC, hay chỉnh `spark.memory.fraction` thành công thức mặc định. Không tăng executor cores nếu mỗi task đang giữ state lớn: số task đồng thời có thể tăng áp lực. Với local mode, driver và executor cùng JVM; đặt `spark.executor.memory` lúc session đã chạy không thay `-Xmx` hiện có. Để phân tích pause thật, bật GC log từ lúc khởi động và đối chiếu JDK/collector, không suy từ một lần task chậm.

### 3.8. Task retry và executor failures

**Vấn đề và nguyên nhân.** Task có thể lỗi tạm thời, mất shuffle block hoặc mất executor. Spark thử lại/tính lại lineage khi có thể; lặp lại đọc/serialize/shuffle làm chậm job, và side effect ghi ngoài Spark có thể bị nhân đôi.

**Cách xác định.** So task index với attempt number, đọc **lỗi đầu tiên** trong executor log, xem failed/killed task và executor removed, phân biệt lỗi dữ liệu lặp lại với lỗi hạ tầng. Chỉ xem action cuối thành công sẽ bỏ sót chi phí retry. Retry không chữa một phép parse hỏng giống nhau mọi lần, cũng không chữa aggregate tràn số.

**Thí nghiệm.** Master `local[2,3]`: hai thread task, failure limit 3 (tối đa ba lần thất bại cho task trong local scheduler). Partition 0 đọc dữ liệu vào vector, chủ động ném `CH25_TRANSIENT_TASK_FAILURE` ở attempt 0; attempt 1 xử lý lại partition. Có 3 attempt, 1 failed, sum vẫn 5.050. `local[N]` thông thường chỉ cho một failure; đừng nhầm với retry mặc định cluster. [Configuration](https://spark.apache.org/docs/4.2.0/configuration.html).

**Giải pháp.** Bảo đảm output idempotent/transactional khi ghi ra ngoài; điều tra thất bại trước khi tăng giới hạn retry. Cache có thể giúp giảm tính lại khi partition còn tồn tại, nhưng mất executor có thể mất cả cache/shuffle trên máy đó. Replication/checkpoint/durable intermediate là các đánh đổi tài nguyên và độ bền, không cùng mục đích với retry.

**Khi không áp dụng.** Lab này chỉ tiêm **task exception trong một JVM**, không giết executor, không đo fetch-failure recovery, mất máy hay dynamic allocation. Muốn kết luận về cluster phải thử có kiểm soát trên cluster và đo phần việc lặp lại. Accumulator/transformation không phải nhật ký nghiệp vụ exactly-once.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong bài |
|---|---|
| Lazy evaluation | Trì hoãn thực thi đến khi action cần kết quả |
| Query plan / operator | Kế hoạch truy vấn / bước thực thi trong kế hoạch |
| AQE | Tối ưu kế hoạch theo thống kê trong lúc chạy |
| Shuffle / exchange | Phân phối lại dữ liệu / operator đáp ứng yêu cầu phân phối (cần phân biệt broadcast) |
| Partition sizing | Chọn quy mô công việc mỗi partition |
| Build side / probe side | Bên lập cấu trúc tra cứu / bên đem dòng đi tra khi hash join |
| Data skew / hot key | Dữ liệu lệch / key tập trung phần lớn dữ liệu hoặc chi phí |
| Straggler | Task về cuối do chạy lâu; cần điều tra trước khi quy cho skew |
| Salting | Thêm khóa phụ để chia tải của key |
| Materialization / eviction | Tạo dữ liệu thật từ mô tả / loại block khỏi cache |
| Spill / GC | Đẩy dữ liệu tạm xuống đĩa / thu gom rác JVM |
| Serialization / Encoder | Chuyển object thành byte / chuyển kiểu Dataset vào biểu diễn Spark SQL |
| Attempt / recomputation | Một lần chạy task / tính lại dữ liệu từ lineage |

## 5. Ví dụ đơn giản

Chạy phần lazy trong `PerformanceDemo`: một RDD chứa 1…100, `map` nhân đôi và tăng LongAccumulator. Sau khi tạo transformation accumulator vẫn 0; action `sum` cho 10.100 và accumulator 100. Cặp số cho thấy thời điểm công việc diễn ra, không dùng accumulator để làm nguồn dữ liệu đơn hàng.

Sau đó xem aggregate theo `customer_id`. Khi AQE tắt, đổi `shuffle.partitions` từ 200 sang 8 giảm số task của query từ 208 xuống 16 trên local[2,3]. Bật AQE lại với 200 ban đầu, final plan có coalesced shuffle read và chỉ một partition đầu ra. Ba trường hợp phải cho cùng map `(customer_id, count)`, và test kiểm tra điều này độc lập với thời gian chạy.

## 6. Ví dụ thực tế (Data Engineering)

Giả sử đơn hàng của một khách tổng tập trung 90% lưu lượng, còn bảng khách nhỏ có 101 dòng. `orders` sinh `order_id` duy nhất, `customer_id`, tiền nguyên `amount=100`; `customers` có key 0…100 duy nhất và segment `S0/S1/S2`. Chúng ta giữ nguyên dữ liệu khi thử ba chiến lược join, sau đó thử SMJ lệch, SMJ thêm salt, SMJ có AQE skew join.

Quy trình nghiệp vụ vẫn là bổ sung segment và tính doanh thu. Với dữ liệu ngoài lab, cần xác nhận quy tắc duplicate/null/unmatched trước khi tối ưu. Helper salting chỉ có hợp đồng INNER join với dimension unique/non-null; không tự động dùng cho left join của Ch23. Lời giải có validation để lỗi dimension không bị che bởi một kết quả nhanh. Test so `exceptAll` hai chiều bảo vệ multiplicity, rồi so tổng tiền, không chỉ kiểm tra số dòng.

Ở production hãy chốt snapshot input, ghi cấu hình, hardware, kích thước sau filter, loại join cuối, số task, byte shuffle, spill, GC, lỗi/retry và chi phí output. Sau warm-up, chạy lặp nhiều lượt theo thứ tự hoán đổi, báo phân bố thời gian thay vì chỉ một số tốt nhất. Lab dưới đây là thí nghiệm quan sát cơ chế, **mỗi cấu hình một lượt**, không có kiểm soát tải nền toàn máy và không suy thời gian tuyệt đối hay thứ hạng sang cluster.

## 7. Code đầy đủ

Mã nguồn được build riêng trong [spark-performance-lab](spark-performance-lab/):

- [project.scala](spark-performance-lab/project.scala), [build.sbt](spark-performance-lab/build.sbt), [build.properties](spark-performance-lab/project/build.properties): Scala/JDK/dependencies, 15 cờ `--add-opens`, tắt test song song với sbt.
- [PerformanceLab.scala](spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/PerformanceLab.scala): dataset, chạy query, cache, salting, serializer và retry.
- [Measurement.scala](spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/Measurement.scala): listener và rào đồng bộ.
- [PerformanceDemo.scala](spark-performance-lab/src/main/scala/vn/scalaeng/sparkperf/PerformanceDemo.scala): ba entry point PerformanceDemo, MemoryDemo, SerializationDemo.
- [PerformanceSuite.scala](spark-performance-lab/src/test/scala/vn/scalaeng/sparkperf/PerformanceSuite.scala): test thật với munit.
- [verify.ps1](spark-performance-lab/verify.ps1): đưa build, temp, local shuffle, Parquet và log vào `.cache/t12/`.

Từ **gốc repo**, dùng các lệnh sau để tái hiện trên Windows. Các mode tests/memory/serializers đã chạy qua script; PerformanceDemo đã chạy trực tiếp bằng Scala CLI với cùng tập option, mode demo là wrapper tương ứng:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode tests
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode demo
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode memory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode serializers
```

`Bypass` chỉ áp dụng tiến trình PowerShell này, không đổi policy toàn máy. Script dùng `scala-cli.bat test` cho cả lab và `solutions/ch25-code`, luôn `--server=false`, `COURSIER_CACHE=<repo>/.cache/coursier`, `SPARK_LOCAL_DIRS=<repo>\.cache\t12\local`; `--workspace` cùng `java.io.tmpdir`/warehouse đều trong `.cache/t12`. Tải dependency lần đầu cần mạng; test đã có dependency không cần Docker, PostgreSQL, Kafka hay dịch vụ bên ngoài. Spark vẫn dùng socket cục bộ.

Nếu chạy tay, cùng các biến môi trường và java-opt của script:

```powershell
scala-cli.bat test 09-spark/spark-performance-lab --server=false --workspace .cache/t12/lab-build --java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t12/tmp --java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t12/warehouse
```

Đổi đường dẫn tuyệt đối theo máy. Trên Linux/macOS dùng `scala-cli`, đường dẫn tương ứng; lệnh trên các hệ đó **chưa kiểm chứng**. Scala 2.13 nhận `-Wunused:_` để bật mọi nhóm unused; `-Wunused:all` của Scala 3 bị compiler này từ chối. Có thêm `-deprecation -feature -Xlint:_`. BareLocalFileSystem cho phép ghi Parquet trên Windows thiếu winutils; 15 `--add-opens` theo launcher Spark, không sửa code framework để né Java module access. sbt build đã cấu hình nhưng lượt này chưa chạy; Claude kiểm chứng `sbt.bat test` cuối.

## 8. Giải thích code

`withSql` lưu và khôi phục từng SQLConf trong `finally`, tránh cấu hình thí nghiệm trước rò sang thí nghiệm sau. Cấu hình SparkContext như serializer và memory fraction cần JVM/context mới, nên MemoryDemo và từng SerializationDemo chạy riêng. Lab quản lý một SparkContext trong một tiến trình; nhiều SparkSession có thể chia sẻ context, vì vậy không chạy suite/benchmark đồng thời trên cùng context này.

`Measurement.measure` gắn UUID job group; listener ghi `stageIds` của `JobStart` đúng group rồi mới giữ TaskEnd tương ứng. Khi action đồng bộ đã trả về, helper gửi một job marker riêng trên cùng thread. Callback JobEnd của marker là rào FIFO trên shared listener queue, bảo đảm các TaskEnd trước đó đã được xử lý; `CountDownLatch` có timeout 60 giây rồi báo lỗi nếu metrics chưa hoàn chỉnh. Không dùng `Thread.sleep` để đoán hàng đợi đã xong. Task marker bị loại khỏi số đo và thời gian marker không cộng vào elapsedMs; thời gian action vẫn có lập lịch, lập plan còn lại, collect và truyền về driver.

Test cố tình giữ listener bằng latch, xác nhận phép đo chưa trả về rồi release; test khác chạy job đổi sang group không liên quan để bảo đảm không đếm task đó. Helper chỉ dành cho action **đồng bộ trong thread gọi**, không cho Future chưa join; không nhận đo đúng một stage được chia sẻ giữa những action concurrent phức tạp. Listener phải nhẹ, queue không được drop event; event bị drop/timeout cần coi lượt đo không hợp lệ. Cuối cùng helper gỡ listener và trả lại local properties ngay cả khi action lỗi.

`summary` là tổng quan toàn action, `stageSummary` tách từng stage và dùng upper median (với n chẵn lấy phần tử n/2 khi đánh chỉ số từ 0), chỉ dùng attempt thành công cho thời gian phân bố. Byte shuffle/spill/GC trong summary cộng mọi attempt có metrics, gồm cả lần thất bại. Vì thế retry và pause JVM có thể tác động thống kê; không coi tổng task-ms bằng wall-clock. `QueryRun` giữ cả initial lẫn final plan để reviewer kiểm tra các khẳng định về AQE.

## 9. Output mong đợi

Đây là **output đã chạy**, ngày 17/10/2026 trên Windows 11 amd64, JDK 21.0.12.1, JVM thấy 12 CPU, `maxHeap=8547991552` byte, master `local[2,3]`: một máy, driver/executor cùng JVM, hai task thread. PerformanceDemo warm-up một `range(10000).count`, không cố định CPU/tải nền. Dòng số và full plan được giữ trong [performance-demo.txt](spark-performance-lab/evidence/performance-demo.txt). ID stage/plan, đường dẫn temp và thời gian có thể thay đổi khi chạy lại; test kiểm tra nội dung/cấu trúc, không assert thứ hạng thời gian.

```text
LAZY before=0
LAZY sum=10100 after=100
SKEW hash=1000,2000,2000,0,0,92000,1000,2000
SKEW salted=1508,1229,881,12067,1263,24899,35174,22979
CACHE storage=StorageLevel(disk, memory, deserialized, 1 replicas)
FILES partitions=8 parquet=8
FILES partitions=2 parquet=2
SERIALIZE javaBytes=27156 kryoBytes=20004
```

Bảng dưới chép các trường từ cùng transcript; byte shuffle là TaskMetrics, không gồm byte broadcast:

| Thí nghiệm | ms | Task attempt | Shuffle read/write (byte) | Kết quả |
|---|---:|---:|---:|---|
| Hint broadcast | 1108 | 12 | 0 / 0 | 100.000 dòng, final BroadcastHashJoin |
| Hint merge | 2033 | 13 | 550845 / 550845 | 100.000 dòng, final SortMergeJoin |
| Hint shuffle_hash | 696 | 13 | 550845 / 550845 | 100.000 dòng, final ShuffledHashJoin |
| 200 partition, AQE tắt | 1882 | 208 | 5112 / 5112 | 200 partition đầu ra |
| 8 partition, AQE tắt | 380 | 16 | 3504 / 3504 | 8 partition đầu ra |
| 200 partition ban đầu, AQE bật | 401 | 9 | 5112 / 5112 | 1 partition đầu ra |
| SMJ skew, AQE tắt | 701 | 20 | 550845 / 550845 | 100.000 dòng |
| SMJ salted | 891 | 20 | 748129 / 748129 | 100.000 dòng |
| SMJ, AQE skew bật | 473 | 27 | 553099 / 550845 | 100.000 dòng, skew=true |

Stage cuối của ba thí nghiệm skew, trích nguyên dòng:

```text
SKEWSTAGES stage=34 tasks=4 medianMs=113 maxMs=117; stage=35 tasks=8 medianMs=37 maxMs=44; stage=36 tasks=8 medianMs=40 maxMs=192
SALTSTAGES stage=38 tasks=4 medianMs=103 maxMs=116; stage=39 tasks=8 medianMs=77 maxMs=163; stage=40 tasks=8 medianMs=59 maxMs=120
SKEWSTAGES stage=42 tasks=4 medianMs=39 maxMs=41; stage=43 tasks=8 medianMs=35 maxMs=45; stage=46 tasks=15 medianMs=18 maxMs=94
```

AQE tăng số task cuối, max giảm nhưng median cũng giảm; không chỉ nhìn tỷ số max/median. Kết hợp row distribution và dấu `skew=true` mới giải thích được điều gì đã thay đổi. Hai dòng operator dưới là **hai đoạn trích rời** từ final plan, không ghép thành cây giả:

```text
      +- *(5) SortMergeJoin(skew=true) [customer_id#182L], [customer_id#185L], Inner
         :  +- AQEShuffleRead skewed
```

Cache gồm materialization; mỗi consumer trả 12.800.000 (tổng chiều dài chuỗi SHA-256, không phải doanh thu):

| Consumer | Không cache (ms) | Có cache (ms) | Tổng kết quả hai bên |
|---:|---:|---:|---:|
| 1 | 582 | 1477 | 12800000 |
| 2 | 609 | 772 | 25600000 |

Builtin và UDF cùng trả 100 dòng. Trích thuộc tính scan thực tế: builtin `PushedFilters: [IsNotNull(order_id), LessThan(order_id,100)]`; UDF `PushedFilters: []`. Lượt này builtin 417 ms, UDF 214 ms, không dùng thời gian nhỏ này để phủ nhận khác biệt pushdown.

MemoryDemo và hai SerializerDemo có transcript riêng trong thư mục [evidence](spark-performance-lab/evidence/). Cần đọc kèm cấu hình cưỡng bức spill ở mục 3.7; không coi đó là benchmark memory tự nhiên.

MemoryDemo chạy riêng với heap 768 MiB, execution fraction 0.2 và ngưỡng ép spill 10.000 phần tử, output thật:

```text
MEMORY heap=805306368 rows=200000 ms=3096 tasks=4 failed=0 taskMedianMs=1475 taskMaxMs=1480 gcMs=60 memorySpill=40514368 diskSpill=209862 shuffleRead=1037666 shuffleWrite=1037666
```

Hai reduce task và hai map task cùng ở một JVM, nên `gcMs=60` là tổng metric theo task có khả năng chồng lặp pause. Spill khác 0 mà query vẫn hoàn thành; không có kết luận OOM từ output này.

Hai SerializerDemo chạy context mới cho mỗi cấu hình, byte shuffle đo trên RDD có EventRecord; Java và Kryo đều giữ đủ 10.000 dòng:

```text
RDD serializer=Java rows=10000 ms=1927 tasks=8 failed=0 taskMedianMs=276 taskMaxMs=658 gcMs=0 memorySpill=0 diskSpill=0 shuffleRead=75996 shuffleWrite=75996
RDD serializer=Kryo rows=10000 ms=1734 tasks=8 failed=0 taskMedianMs=247 taskMaxMs=493 gcMs=24 memorySpill=0 diskSpill=0 shuffleRead=72960 shuffleWrite=72960
```

Không suy một tỷ lệ tăng tốc ổn định từ hai lượt này. Retry cuối PerformanceDemo:

```text
RETRY sum=5050 ms=60 tasks=3 failed=1 taskMedianMs=4 taskMaxMs=4 gcMs=9 memorySpill=0 diskSpill=0 shuffleRead=0 shuffleWrite=0
```

Kiểm chứng cuối bằng munit thật: lab **13 pass, 0 fail, 0 ignored**; lời giải **6 pass, 0 fail, 0 ignored**. Các summary gốc nằm trong [lab-test.txt](spark-performance-lab/evidence/lab-test.txt) và [solutions-test.txt](spark-performance-lab/evidence/solutions-test.txt). Không có cảnh báo compiler ở hai lượt cuối.

## 10. Lỗi thường gặp

Các thông báo sau đều đã gặp trong lúc build/run lab này; chúng không phải dự đoán error message:

| Thông báo thật | Nguyên nhân / cách xử lý |
|---|---|
| `scalac error: 'all' is not a valid choice for '-Wunused'` | Dùng cờ Scala 3 cho Scala 2.13; đổi thành `-Wunused:_` |
| `Can't compare these two types:` rồi `First type: Long`, `Second type: Long` | munit gặp `java.lang.Long` từ `LongAccumulator.value` và Scala Long; dùng `.longValue()` khi assert |
| `Passing an explicit array value to a Scala varargs method is deprecated (since 2.13.0)` | `select(array: _*)` sinh cảnh báo; đổi `array.toIndexedSeq` trước khi truyền varargs |
| `java.lang.IllegalStateException: CH25_TRANSIENT_TASK_FAILURE` | Lỗi được chủ động tiêm vào attempt 0; xác nhận attempt 1 thành công và tổng đúng, không xóa log rồi bỏ qua |
| `HADOOP_HOME and hadoop.home.dir are unset.` | Hadoop cảnh báo thiếu winutils; lab dùng BareLocalFileSystem và test ghi/đọc Parquet thật vẫn pass |

OOM, executor lost, FetchFailed và Kryo buffer overflow **chưa tái hiện** trong lượt này; không có transcript để trích. Khi gặp trên cluster, lưu stacktrace và cấu hình thực, xác định driver hay executor, task/attempt nào và dữ liệu partition nào trước khi đổi tham số. Một dòng WARN không tự có nghĩa test fail; xem exit code và munit summary.

## 11. Cách debug

1. **Chốt hợp đồng và baseline:** cùng snapshot, schema, null/duplicate policy, miền số tiền; xác nhận kết quả đối chiếu bằng `exceptAll`/aggregate. Giữ log gốc và cấu hình.
2. **Chọn action:** đo đúng công việc ghi hoặc tổng hợp; không lấy thời gian dựng DataFrame. Ghi startup/warm-up tách biệt.
3. **Đọc plan cuối:** xem join, sort, shuffle, broadcast, pushdown, cache scan và AQE thay đổi. Đừng tìm `Exchange` rồi kết luận mọi exchange là shuffle.
4. **Khoanh stage:** chọn stage chiếm đuôi thời gian, so phân bố task trong stage đó: input/shuffle byte, spill, GC, attempt. Stage có task nặng đều khác stage chỉ có một key nóng.
5. **Đặt một giả thuyết:** overhead partition nhỏ, state quá lớn, skew, UDF cản pushdown, cache đắt, hay retry? Đổi một yếu tố và ghi cả chi phí tạo dữ liệu/caching/output mới.
6. **Đo lại nhiều lượt:** xác nhận final plan mới, cùng kết quả, rồi so phân bố thời gian và tài nguyên. Không giữ cấu hình chỉ vì một lượt thắng.

Lab mặc định tắt UI cho test. Để quan sát trực tiếp khi chạy app lâu hơn, khởi tạo session với `extra=Map("spark.ui.enabled" -> "true")` rồi xem URL UI thực mà Spark báo; Spark thường bắt đầu ở cổng 4040 và có thể đổi khi cổng bận. SQL tab nối query với stage, Stages xem task distributions/spill, Storage xem cache, Executors xem GC/failure, Environment xem cấu hình. Muốn xem sau khi app dừng, bật event log và tạo thư mục đích trước lúc khởi tạo context, rồi dùng History Server. Phần mở UI/History Server **chưa chạy** trong bài; bằng chứng đo ở đây đến từ listener, không từ ảnh UI. [Monitoring và History Server](https://spark.apache.org/docs/4.2.0/monitoring.html).

## 12. Bài tập (cơ bản → nâng cao)

1. **Đọc chiến lược join cuối.** Viết `joinStrategy(plan)` chỉ nhận final AQE plan (hoặc plan không adaptive), trả một trong ba tên join. Dùng query không hint với adaptive shuffled-hash threshold 128 MiB: chứng minh initial SMJ, final SHJ. Từ chối plan chưa final; chạy thêm cả ba hint.
2. **Báo cáo theo stage.** Từ `Vector[TaskSample]`, tính số attempt thành công/thất bại, upper median, max và tỷ số max/median theo stage. Không chia 0, không lấy failed attempt vào phân bố thời gian. Test stage rỗng, toàn lỗi, duration 0 và một stage có đuôi dài; ghi vì sao tỷ số không tự chẩn đoán skew.
3. **Ước lượng partition ban đầu.** Viết hàm từ tổng byte và target byte, ceil, tối thiểu 1, có trần maxPartitions; tránh tràn `Long.MaxValue`. Từ chối input âm/target 0. Giải thích vì sao hàm không chữa hot key, không dự báo số partition sau AQE.
4. **Salting có hợp đồng.** Trước khi dùng `saltedJoin`, từ chối dimension duplicate key hoặc null key. Tính doanh thu theo segment và so với INNER join gốc trên dữ liệu hợp lệ; thêm empty facts. Ghi giới hạn với outer join và chi phí validation.
5. **SUM hai tầng an toàn tiền nguyên.** Group theo `(customer_id,salt)` rồi cộng về customer. Cast DECIMAL(38,0) trước lần cộng đầu; giữ ANSI và hợp đồng tổng vừa 38 chữ số. Test `Long.MaxValue + 1`, empty và amount toàn null. Không thay bằng cộng Long hoặc trung bình của các trung bình.
6. **Quản lý vòng đời cache.** Viết `withCache(df)(consume)` cho DataFrame chưa cache, giải phóng khi consumer thành công hoặc ném lỗi sau materialization. Từ chối đầu vào đã cache để bảo vệ ownership. Test kết quả hai consumer và kiểm tra persistent RDDs trước/sau; đo cache dùng một lần mà không assert thời gian.

## 13. Đáp án và lời giải

Xem [ch25-solutions.md](../solutions/ch25-solutions.md), [Ch25Solutions.scala](../solutions/ch25-code/src/main/scala/vn/scalaeng/solutions/ch25/Ch25Solutions.scala) và [suite sáu bài](../solutions/ch25-code/src/test/scala/vn/scalaeng/solutions/ch25/Ch25SolutionsSuite.scala). `verify.ps1 -Mode tests` chạy độc lập cả lab lẫn lời giải; lời giải dùng lại source main của lab, không sao chép một phiên bản thuật toán khác.

## 14. Câu hỏi kiểm tra

1. Vì sao `df.count()` nhanh không đủ kết luận `df.write.parquet` nhanh? → Cột có thể bị prune; action và chi phí output khác nhau.
2. Broadcast có exchange nhưng shuffle bytes bằng 0 có mâu thuẫn? → Không; BroadcastExchange không phải shuffle exchange, metric shuffle không đo broadcast.
3. Tắt automatic broadcast có buộc SMJ? → Không; planner/AQE vẫn có thể chọn SHJ, hint cũng có thể yêu cầu broadcast.
4. Tăng hash partition lên nhiều hơn có chia key nóng? → Không; một key vẫn vào một partition nếu không đổi khóa/cơ chế xử lý.
5. Max task giảm sau salting thì job phải nhanh hơn? → Không; thêm dữ liệu, shuffle và overhead có thể làm tổng chậm hơn.
6. MEMORY_AND_DISK có bảo đảm action sau không tính lại? → Không; block có thể mất/evict, cache không phải storage bền vững.
7. `diskBytesSpilled > 0` có chứng minh job lỗi bộ nhớ? → Không; spill giúp hoàn thành trong giới hạn RAM; lab còn cố ý ép spill.
8. Lỗi attempt 0 rồi job thành công có cho phép ghi thanh toán tùy ý trong map? → Không; công việc có thể lặp, side effect cần idempotency và thiết kế giao dịch.

## 15. Mini project

Lập hồ sơ tối ưu pipeline đơn hàng của Ch24 trên một snapshot nhỏ có thể chạy local. Giữ một baseline không salt, dimension unique và số tiền có miền rõ ràng; tạo hai phân bố cùng schema: đều và 90% hot key. Lưu input count, kết quả doanh thu, số file/kích thước output cùng initial/final plan, metrics theo stage và cấu hình cho mỗi lượt.

Thử ít nhất ba thay đổi riêng lẻ: partition/AQE, join strategy, cache hoặc salting. Mỗi thay đổi phải kèm giả thuyết, bằng chứng và điều kiện không áp dụng; giữ lại cả thay đổi làm chậm hơn. Kiểm chứng kết quả bằng multiset và tổng tiền, không chỉ count. Nếu có cluster, đo lại cùng snapshot và mô tả khác biệt tài nguyên/mạng; không suy thời gian cluster từ local.

Tiêu chí nhận bài: có lệnh chạy lặp được, không phụ thuộc timing assertion, bảng so sánh có ít nhất một cấu hình bị loại bằng bằng chứng, và ghi trung thực phần executor loss/GC/UI nào chưa đo. Đây là bài thực hành mở rộng cho người học; không đánh dấu dự án đó đã chạy chỉ vì lab trong chương đã pass.

## 16. Nguồn tài liệu chính thức

Tra ngày **17/10/2026**, gắn với Spark 4.2.0 để tránh vô tình đọc `latest` khác phiên bản:

- [Spark overview](https://spark.apache.org/docs/4.2.0/): Scala/JDK được hỗ trợ.
- [SQL performance tuning](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html): AQE coalesce, chuyển chiến lược và skew join.
- [Join hints](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-hints.html#join-hints): ưu tiên và điều kiện hỗ trợ hint.
- [Tuning Spark](https://spark.apache.org/docs/4.2.0/tuning.html): serialization, memory, GC và parallelism.
- [Dataset ScalaDoc](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html): cache/persist/repartition/coalesce và actions.
- [SQL guide](https://spark.apache.org/docs/4.2.0/sql-programming-guide.html): Dataset/Encoder và thực thi SQL.
- [Monitoring](https://spark.apache.org/docs/4.2.0/monitoring.html), [configuration](https://spark.apache.org/docs/4.2.0/configuration.html): TaskMetrics, UI/event log, task failure và cấu hình.
- [AsyncEventQueue.scala v4.2.0](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/AsyncEventQueue.scala): shared listener xử lý sự kiện bất đồng bộ, lý do cần barrier.
- [Scala 2.13.18 release](https://www.scala-lang.org/news/2.13.18/), [sbt 1.12.11 release](https://github.com/sbt/sbt/releases/tag/v1.12.11).
- Maven Central POM: [spark-sql_2.13 4.2.0](https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/4.2.0/spark-sql_2.13-4.2.0.pom), [munit_2.13 1.2.4](https://repo1.maven.org/maven2/org/scalameta/munit_2.13/1.2.4/munit_2.13-1.2.4.pom), [BareLocalFileSystem 0.1.0](https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom); đã đọc bản POM trong Coursier cache và dùng artifact thật để compile/run. Lượt HTTP trực tiếp tới Maven trong phiên này không thành công; không tuyên bố đã tải mới POM. [GlobalMentor source/README](https://github.com/globalmentor/hadoop-bare-naked-local-fs) xác nhận cách cấu hình filesystem trên Windows.

## 17. Checklist tự đánh giá

- [ ] Tôi đo action đại diện và biết công việc nào optimizer có thể loại bỏ.
- [ ] Tôi đọc phần final AQE plan, phân biệt shuffle exchange với broadcast exchange.
- [ ] Tôi phân biệt số partition ban đầu, explicit repartition và số sau AQE.
- [ ] Tôi dùng phân bố theo stage cùng byte/GC/retry để điều tra task về cuối.
- [ ] Tôi chứng minh salting không đổi multiplicity/doanh thu trong hợp đồng đã nêu.
- [ ] Tôi tính cả materialization khi đo cache và dọn cache trong nhánh lỗi.
- [ ] Tôi phân biệt serializer RDD với biểu diễn nội bộ Dataset và pushdown của SQL.
- [ ] Tôi giải thích được spill mà không kết luận OOM; phân biệt task retry với executor chết.
- [ ] Tôi chạy được lab + sáu lời giải và có log thực; ghi riêng phần chưa chạy trên cluster.
