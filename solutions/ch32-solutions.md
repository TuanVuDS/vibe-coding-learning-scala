# Lời giải Chương 32 — Advanced Distributed Processing

Scala 2.13.18 · JDK 21 · Spark 4.2.0 `_2.13` · munit 1.2.4 · BareLocalFileSystem 0.1.0. Code dùng lại source main của [distributed-lab](../11-data-platform/distributed-lab/) qua [project.scala](ch32-code/project.scala). Không trộn Scala 3 vào nhánh Spark. Kết quả chạy thật và nguồn phiên bản ở [evidence](../11-data-platform/distributed-lab/evidence/README.md).

Từ gốc repo chạy `powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode tests`; script gọi Scala CLI `--server=false` cho cả lab/lời giải, COURSIER_CACHE/tmp/workspace/local shuffle trong `.cache/`. Test dùng Spark thật, không cần Docker hay dịch vụ ngoài. Lần đầu tải dependency cần mạng.

Mã đầy đủ: [Exercises.scala](ch32-code/src/main/scala/vn/scalaeng/distributed/solutions/Exercises.scala); bảy test theo E1–E7: [ExercisesSuite.scala](ch32-code/src/test/scala/vn/scalaeng/distributed/solutions/ExercisesSuite.scala). Test kiểm nội dung và cơ chế, không assert cấu hình nào chạy nhanh hơn.

## Bài 1 — Giữ partitioner bằng mapValues

`doubleValues` dùng `input.mapValues(_ * 2L)`. Vì không đổi key nên Spark giữ partitioner; test xác nhận cả metadata lẫn 20 cặp kết quả và input rỗng. Phép nhân dùng cho fixture value=1; nếu áp dụng số tiền ngoài miền này cần checked arithmetic/BigInt. `map(identity)` bỏ metadata theo API thông thường, dù kết quả key không đổi.

Không dùng `preservesPartitioning=true` sau khi đổi key: giả sử key=1 ở partition 1 chuyển thành key=2 nhưng vẫn ở partition cũ, partitioner khai báo key=2 phải ở partition 2; join không shuffle có thể bỏ lỡ cặp khớp. Tiết kiệm shuffle không cho phép hứa sai metadata.

## Bài 2 — Prepare, reuse và cardinality

`prepare` kiểm n>0 rồi `partitionBy(new HashPartitioner(n))`. Test persist và count trước khi đo join, nhánh thứ hai dùng doubleValues. Mỗi key có năm dòng ở mỗi phía; inner join có 5×5=25 cặp/key, 20 key thành **500 dòng**. Không phải 100 dòng ban đầu. Đây là golden cardinality nghiệp vụ riêng, không chỉ so hai implementation cùng sai.

Byte shuffle mới bằng 0 trong **phần consumer**; preparation shuffle và storage vẫn có giá. Với dimension thực tế cần key duy nhất nếu hợp đồng là many-to-one. `finally` unpersist dữ liệu dù test/action lỗi. Demo lab đối chiếu thêm trường hợp metadata bị mất để thấy shuffle quay lại.

## Bài 3 — Combine với BigInt

`totals` đổi Long → BigInt **trước** reduceByKey rồi gộp `_ + _`. Tổng key=1 vượt Long.MaxValue vẫn đúng; key=2 có MinValue+MaxValue+1=0; input rỗng trả rỗng. Phép cộng BigInt kết hợp được về giá trị, không wrap-around như Long và không có sai số làm tròn Double.

Đổi sang groupByKey rồi sum cùng kiểu có thể cùng kết quả, nhưng phải mang các giá trị qua shuffle và giữ/duyệt nhóm. Với sum, map-side combine chỉ cần trạng thái tổng mỗi key mỗi map partition. So writeRecords và bytes ở demo, không hứa tốc độ hay tỷ lệ byte cố định vì còn nén. BigInt cũng có chi phí allocation/serialization; production có thể dùng Decimal với precision/overflow policy tường minh.

## Bài 4 — Retry audit không phải idempotent sink

`DistributedLab.retry` append trước rồi ném lỗi ở attempt0 của partition0. Output thành công là hai ID 100,200; journal có `100,0`, `100,1`, `200,0`. `duplicateIds` tách business ID trước dấu phẩy, groupMapReduce đếm rồi lấy IDs xuất hiện nhiều lần; test chạy retry thật và input journal rỗng.

Để ghi DB an toàn, transaction có thể kiểm/chèn event_id có unique constraint rồi cập nhật số dư trong **cùng transaction**, xử lý duplicate theo hợp đồng đã chốt. Transaction phải gắn dedup với business write, không chỉ kiểm ở memory trước khi gọi DB. Audit phát hiện sau sự kiện không ngăn nhân đôi; lời giải không có DB sink đã kiểm chứng. Không dùng stage/taskAttempt ID làm event ID vì mỗi lần thử khác nhau. Journal chỉ hoạt động local một máy trong thí nghiệm.

## Bài 5 — Checkpoint trước job đầu tiên

Caller đặt `setCheckpointDir` trên đường phù hợp. `materializeCheckpoint` nhận **RDD mới chưa chạy job**, persist → checkpoint request → count → kiểm `isCheckpointed` → unpersist trong finally. Persist tránh tính lại upstream chỉ để ghi checkpoint. Return là cùng RDD đã có reliable checkpoint, đọc sau khi bỏ cache đi qua ReliableCheckpointRDD.

Test dùng accumulator chỉ để đo upstream trong trường hợp không retry, đối chiếu vector 0–29 và số calls không tăng. Không suy accumulator bảo đảm exactly-once khi task retry. Cluster phải dùng storage bền truy cập từ các executor; local temp của test không có bảo đảm ấy. `localCheckpoint` cắt lineage nhưng dùng storage executor nên không chịu được mất storage đó như reliable checkpoint.

## Bài 6 — Exact và approximate cardinality

`cardinality(false)` dùng count_distinct, `true` dùng approx_count_distinct(...,0.05). Fixture 1,1,2,NULL có exact=2, sketch của fixture nhỏ này cũng=2; input rỗng trả 0 ở cả hai nhánh. Đây là output kiểm chứng của fixture, không chứng minh sketch luôn chính xác. Cả hai hàm bỏ NULL theo semantics Spark.

Nguồn 120.000 dòng / 20.000 customer_id khác nhau nằm trong DistributedDemo, có kết quả/sai số, metrics và plan thật tại evidence; không đặt benchmark lớn vào test correctness. RSD là độ lệch chuẩn tương đối thống kê, không là max error tuyệt đối cho mọi tập khóa. Dashboard có ngân sách sai số có thể dùng sketch; số lượng để lập hóa đơn/đối soát dùng exact. So plan: exact distinct có các bước loại trùng và aggregate, approx gộp trạng thái sketch; không suy một lần timing thành bảo đảm.

## Bài 7 — Đơn vị throughput và phần độ trễ còn thiếu

`throughput(rows,millis)` trả Some(rows×1000/millis), None cho 0ms vì độ phân giải không đủ để chia; từ chối số âm. Fixture 2.500 dòng / 500ms = **5.000 dòng/s**. `batches(total=25,batch=12,partitions=2)` phải xử lý 12+12+1, ba job, đủ 25 dòng; `firstMs<=totalMs` là quan hệ ngữ nghĩa, không phải assertion hiệu năng.

Chạy `-Mode demo` cho bảng bốn cấu hình. `firstMs` là thời gian hoàn tất batch đầu từ lúc gửi; total là processing wall-clock. Chưa có thời gian chờ trigger/gom lô, input backlog/event-time delay, sink commit, network nhiều máy hay p99 giữa các record; vì vậy không dùng bảng đó để tuyên bố streaming latency. Muốn so production cần lặp/hoán đổi thứ tự, input snapshot và SLO cụ thể.

## Giới hạn kiểm chứng

Local[2,3] chỉ mô phỏng task exception + retry trong một JVM. Chưa chạy executor death, FetchFailed/stage recovery, network partition, locality wait trên nhiều node, DB transaction hay speculation. sbt và review chéo do Claude thực hiện sau bàn giao. Không có lời giải nào tuyên bố các tình huống này đã được test.
