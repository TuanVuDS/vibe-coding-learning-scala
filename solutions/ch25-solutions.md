> Scala 2.13.18 · JDK 21 · Spark 4.2.0 `_2.13` · munit 1.2.4 · Đã biên dịch & chạy Scala CLI ngày 17/10/2026; chờ review chéo và sbt của lead.

# Lời giải Chương 25 — Spark Performance

Mã [Ch25Solutions.scala](ch25-code/src/main/scala/vn/scalaeng/solutions/ch25/Ch25Solutions.scala), [Ch25SolutionsSuite.scala](ch25-code/src/test/scala/vn/scalaeng/solutions/ch25/Ch25SolutionsSuite.scala) dùng lại source chính của [lab](../09-spark/spark-performance-lab/). Các bài kiểm tra nội dung và cơ chế, không assert thời gian. Đọc [bài học](../09-spark/ch25-spark-performance.md) để hiểu hợp đồng và các số đo local[2,3].

Từ gốc repo:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode tests
```

Script chạy hai lệnh `scala-cli.bat test ... --server=false`, đặt cache/build/temp/warehouse/local dirs trong `.cache/`. Có thể chạy riêng lời giải với các biến/java-opt tương tự script:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:SPARK_LOCAL_DIRS = "$PWD\.cache\t12\local"
scala-cli.bat test solutions/ch25-code --server=false --workspace .cache/t12/solutions-build --java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t12/tmp --java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t12/warehouse
```

Tạo thư mục temp trước (script làm tự động), đổi đường dẫn theo máy. Output thật của lượt cuối:

```text
Test run vn.scalaeng.solutions.ch25.Ch25SolutionsSuite started
vn.scalaeng.solutions.ch25.Ch25SolutionsSuite: finished 46.076s
Test run vn.scalaeng.solutions.ch25.Ch25SolutionsSuite finished: 0 failed, 0 ignored, 6 total 46.082s
```

Không có cảnh báo compiler; hint munit mới hơn và WARN Hadoop là log công cụ/runtime, không đổi phiên bản chuẩn repo. Unit test không cần Docker hay dịch vụ bên ngoài sau khi đã tải dependency.

## Bài 1 — Phân loại final join strategy

`joinStrategy` từ chối adaptive plan chưa `isFinalPlan=true`, dùng `PerformanceLab.finalSection` bỏ phần `== Initial Plan ==`, rồi chỉ chấp nhận đúng một trong BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin. Với query nhiều join cần duyệt cây operator/trả danh sách; helper này cố ý giới hạn một join để không trả nhầm tên đầu tiên.

Test lấy plan từ query thật: tắt automatic broadcast ở cả ngưỡng thường/adaptive, đặt adaptive shuffled-hash threshold 134.217.728 byte. Initial plan là SMJ, final là SHJ; tìm substring trên cả plan có thể thấy cả hai. Test cũng chạy ba hint trên dimension và xác nhận đúng tên cuối. Không kết luận `-1` buộc sort-merge; không gọi BroadcastExchange là shuffle.

## Bài 2 — Phân bố thời gian theo stage

`stages` group theo `TaskSample.stage`, đếm failed attempt riêng, sắp duration của attempt thành công và lấy **upper median**. Stage không có success nhận median/max 0 và ratio=None; median=0 cũng không chia. Vector rỗng cho vector rỗng. Test dùng dữ liệu chủ động `(10,20,100)` và failed=300: median 20, max 100, tỷ số 5; không để failed attempt đẩy max lên 300.

Các mẫu trong test này là **fixture để kiểm tra phép tính**, không phải số đo Spark giả. Số đo thật phải đến từ `Measurement.measure`, được đồng bộ qua JobEnd marker. Khi chẩn đoán, ghép tỷ số với input/shuffle bytes, spill/GC và vị trí stage: task về cuối chưa chứng minh data skew, và max/median có thể tăng khi median giảm nhanh hơn max.

## Bài 3 — Ước lượng partition theo byte

`initialPartitions` tính ceil bằng `BigInt` để biểu thức `bytes + targetBytes - 1` không tràn ở gần Long.MaxValue; clamp vào `[1,maxPartitions]`, từ chối input âm/target/trần không dương. Đây chỉ là điểm xuất phát cho cấu hình, không phải bảo đảm task vừa bộ nhớ.

Ví dụ fixture 129 byte, target 64 byte cho 3 partition; input rỗng vẫn trả 1. Test sát Long.MaxValue xác nhận bị chặn ở trần 200 mà không wrap. Byte nén không bằng state giải mã, key nóng không được chia chỉ nhờ tăng số partition; AQE có thể gộp/chia tiếp khi chạy. Chọn target theo tài nguyên và workload, không đưa con số fixture vào production.

## Bài 4 — Salting và hợp đồng dimension

`checkedSaltedRevenue` kiểm tra null và duplicate key trước, sau đó dùng salted INNER join rồi aggregate theo segment. Việc validation có action/shuffle, phải cộng chi phí đó khi đánh giá production; không phải một phép kiểm tra miễn phí. Đầu vào dimension cần đủ nhỏ cho chiến lược nhân salt. Snapshot dimension phải ổn định giữa validation và action để tránh dữ liệu đổi sau kiểm tra.

Test so doanh thu với INNER join gốc, thêm empty facts, duplicate và null dimension. Lab còn so `exceptAll` hai chiều giữa join gốc và salted join để kiểm tra cả multiplicity. Không mặc định áp dụng cho outer join hoặc dimension nhiều bản theo thời gian: cần định nghĩa quy tắc chọn phiên bản trước. Helper gán mỗi fact đúng một salt xác định từ order_id và nhân dimension qua mọi salt; nó không nhân tiền fact tám lần.

## Bài 5 — SUM hai tầng và overflow

`saltedTotals` group `(customer_id,salt)` và SUM amount đã cast DECIMAL(38,0), rồi SUM partial theo customer. Hợp đồng: tổng cuối và partial vừa 38 chữ số, ANSI vẫn bật. Với workload thuần cộng tiền nguyên dương, tổng vượt Long nhưng vừa decimal là hợp lệ; không dùng `_ + _` trên Scala Long rồi cast sau vì giá trị đã có thể wrap.

Test thật tạo hai khoản `Long.MaxValue` và 1 cho cùng khách, chạy nhiều partition; tổng đọc được là `9223372036854775808`. So multiset với SUM Decimal một tầng theo cả hai chiều, test empty và amount toàn null (SUM vẫn null). Không đổi all-null thành 0 nếu nghiệp vụ chưa quy định. Với số thập phân khác scale phải thiết kế DECIMAL(p,s) phù hợp; với floating-point, cách chia có thể làm đổi sai số làm tròn.

Chỉ SUM có thể cộng partial theo cách này. AVG cần partial sum và count rồi chia; COUNT DISTINCT không thể cộng số distinct từng salt khi cùng giá trị có thể xuất hiện ở nhiều salt.

## Bài 6 — Cache theo quyền sở hữu

`withCache` yêu cầu input ở StorageLevel.NONE, persist MEMORY_AND_DISK rồi `try consume(cached) finally unpersist(blocking=true)`. Callback có thể thực hiện nhiều action, kết quả được trả nguyên vẹn. Không gọi count để materialize vô điều kiện trước callback vì như vậy thay hợp đồng chi phí; consumer đầu tiên quyết định khi nào cần dữ liệu.

Test chạy hai count, xác nhận 2.000 dòng tổng cộng, kiểm tra storage level và tập persistent RDD trước/sau. Nhánh lỗi tạo cache thật bằng count trước khi ném lỗi ở consumer, rồi kiểm tra vẫn dọn sạch. Nhánh input đã cache bị từ chối và caller tự dọn trong finally; helper không giành quyền gỡ cache của thành phần khác. Đây là ownership theo phạm vi, không an toàn cho nhiều luồng đồng thời dùng cùng cache/query tương đương.

Đo cache dùng một lần bằng `PerformanceLab.cacheTrial`, tính cả materialization. Lượt thật trong bài: 582 ms không cache, 1.477 ms có cache; kết quả đều 12.800.000. Đây là bằng chứng phản bác việc cache mọi DataFrame; không là timing assertion trong test và không phải ngưỡng hòa vốn chung.

## Kiểm tra lại sáu câu hỏi thiết kế

- Tối ưu vẫn đúng với null/duplicate/overflow trong hợp đồng chưa? Xem bài 4–5; helper không giả vờ hỗ trợ mọi join type.
- Có đang đo action khác sản phẩm thật không? `execute` chỉ dành cho kết quả nhỏ trong lab; production đo writer tương ứng.
- Có đọc nhầm initial plan không? Bài 1 từ chối plan adaptive chưa final.
- Có nhầm stage/task hoặc coi fixture thành metric thật không? Bài 2 tách rõ nguồn dữ liệu.
- Có bỏ chi phí validation/materialization không? Bài 4 và bài 6 ghi rõ vị trí chi phí.
- Có kết luận quá phạm vi local không? Không bài nào mô phỏng được executor process death hoặc mạng nhiều máy; các phần đó chờ thí nghiệm cluster.
