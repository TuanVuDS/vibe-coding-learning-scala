> Scala **3.9.0** · JDK **21** · munit **1.2.4** · Trạng thái: **7 test lời giải pass**, Scala CLI `--server=false`, compiler 0 warning; chờ Claude review chéo. Code tái sử dụng `Models.scala`, không cần circe/Avro/DB/Kafka.

# Lời giải Chương 34 — Performance và Cost

Đề bài ở [Ch34 mục 12](../13-performance/ch34-performance-cost.md#12-bài-tập-cơ-bản--nâng-cao). Mọi input bài tập bên dưới là **giả định/tổng hợp** để kiểm logic; số đo máy thật nằm ở [Ch34 mục 9](../13-performance/ch34-performance-cost.md#9-output-mong-đợi). Không biến các kỳ vọng logic thành assertion số ms chạy chương trình.

Code đầy đủ: [Exercises.scala](ch34-code/Exercises.scala), [ExercisesSuite.test.scala](ch34-code/ExercisesSuite.test.scala), [project.scala](ch34-code/project.scala). Từ gốc repo, chạy hai suite **tuần tự** qua verifier:

```powershell
$env:PYTHONIOENCODING = 'utf-8'
python 13-performance/cost-lab/verify.py test
```

Verifier đặt cache/tmp/build trong `.cache/`, dùng Scala CLI `--server=false`, `-deprecation -feature -Wunused:all -Werror`. Test lab có **15 pass**, lời giải **7 pass**, cả hai **0 fail / 0 ignored**; transcript thật: [lab-test.txt](../13-performance/cost-lab/verification/lab-test.txt), [solutions-test.txt](../13-performance/cost-lab/verification/solutions-test.txt). Lần đầu cần mạng tải dependency; logic test không gọi socket/Docker hay dịch vụ ngoài.

## Bài 1 — Đọc phân phối, không che phần đuôi

`distribution` gọi `Models.percentile` với p=0,50/0,95/0,99. Nearest-rank lấy mẫu thứ `ceil(p*n)` sau sắp xếp theo thứ tự tăng; chỉ số mảng trừ 1. Mean là tổng chia số mẫu, có ý nghĩa khác percentile.

Với **99 mẫu 1 ms +1 mẫu 1000 ms**:

- Mean `(99+1000)/100 = 10,99 ms`.
- p50, p95, p99 đều **1 ms** vì mẫu thứ 99 vẫn là 1.
- Max **1000 ms**. Nếu có 98 mẫu 1 ms và 2 mẫu 1000 ms thì p99=1000 ms.

Vì vậy p99 cũng không kể hết những outlier hiếm hơn 1%. Báo count, max và histogram cùng percentile; không suy tail từ 21 sample nhỏ. Hàm từ chối vector rỗng/âm/NaN/vô hạn qua validation của Models; không điền latency 0 khi thiếu dữ liệu. `Distribution` trong bài giữ bốn trường theo đề; trong báo cáo thực tế hãy thêm max/histogram và phân biệt event với run.

Test `B1` kiểm cả hai phân phối trên và input rỗng. Suite lab kiểm validation samples và nearest-rank của tập ngắn; không kiểm runtime phải chạy dưới một thời gian nhất định.

## Bài 2 — Little's law và headroom có ranh giới

`requiredConsumers` dùng **mean** từ `distribution`, đổi ms sang giây, rồi gọi `Models.concurrency`:

```scala
ceil(ratePerSecond * meanResidenceMs / 1000 / utilization)
```

Input giả định rate **100/s**, residence `[10,30] ms`, utilization **0,5** cho mean 20 ms, L=2 và 4 slot sau headroom. Dùng p95=30 thay mean sẽ trả 6 slot, nhưng đó không còn là phép áp dụng định luật cho trung bình. Headroom là policy của mô hình, không bảo đảm p99.

Nếu residence chỉ đo thời gian chiếm consumer/connection thì L tương ứng số tài nguyên bận trung bình; nếu gồm cả queue, L đếm cả phần chờ. Không lấy residence qua queue của consumer cũ để tuyên bố chính xác phải mua bao nhiêu consumer mới. Cần điều kiện ổn định, cùng ranh giới, burst/quota/backpressure được kiểm thêm. Rate=0 hoặc residence=0 cho 0; utilization ngoài (0,1] bị từ chối, tràn Int cũng bị chặn.

Test `B2` kiểm mean/headroom/ceil, thời gian 0 và utilization >1; suite lab kiểm utilization 0 và input dẫn đến overflow. Dữ liệu này không là latency P4 hay thời gian giữ connection thực.

## Bài 3 — Partition count và thời gian kết thúc task cuối

`layout` ghép hai mô hình **độc lập**:

1. Số partition khởi điểm: `max(1, ceil(bytes/targetBytes))`, tính bằng BigInt.
2. `makespan`: task đến lần lượt theo vector, giao cho slot rảnh sớm nhất; mỗi task trả thêm `overheadMs`.

Ví dụ giả định **401 byte /100 byte →5 partition khởi điểm**. Vector task bốn phần tử là một input giả định riêng để minh họa scheduling, **không phải metrics của chính layout 5 partition đó**. Khi ra quyết định thật, phải dùng task vector của layout đã đo; `layout` không tự suy duration từng task từ byte.

Hai slot, overhead 1 ms:

| Task work (ms) | Makespan (ms) | Vì sao |
|---|---:|---|
| `[10,10,10,10]` | 22 | Mỗi slot làm hai task 11 ms |
| `[37,1,1,1]` | 38 | Task đầu một mình đã 38 ms, slot kia không cứu được |
| `[]` | 0 | Không task để chạy, dù mô hình partition rỗng quy ước tối thiểu một |

`B3` kiểm hai lịch và empty task. Suite lab kiểm byte rỗng, chia hết, phần dư, BigInt vượt Long và target=0. Nếu hot key vẫn vào một partition, chỉ tăng số partition tổng không tự chia task nóng. Chọn pre-aggregation/salting/AQE theo semantics rồi đo lại task tail, shuffle và peak memory; không mua slot chỉ từ tổng byte.

## Bài 4 — Tổng byte và chi phí truyền

`wireCost` nhận payload byte và row count của một phép đo. Tổng byte mục tiêu dùng **ceil một lần**:

```scala
val bytes = (payloadBytes * targetRows + measuredRows - 1) / measuredRows
```

Input giả định **10 byte/3 row**, mục tiêu một triệu row cho **3.333.334 byte**, không phải làm tròn 3,333... byte/event thành 4 rồi nhân một triệu. Với effective bandwidth giả định **1.000.000 byte/s**, thời gian truyền là **3,333334 s**. Giá tính theo `bytes / 1024^3 * pricePerGiB`; giá dùng BigDecimal, byte dùng BigInt.

Test `B4` kiểm làm tròn tổng, conversion thời gian, targetRows=0 và measuredRows=0 bị từ chối. Models còn từ chối bandwidth=0; wireCost từ chối byte/target/giá âm. Đo payload thật của Ch34 rồi thay các tham số vào chỉ cho **mô hình truyền**, không tạo ra phép đo network.

Chưa gồm schema ID, framing ngoài payload, compression, TCP/TLS, retransmit, replication, fan-out và lưu trữ. Đơn giá/GiB **giả định** không được gắn tên nhà cung cấp hay USD. Nếu encode-transfer-decode không pipeline, cộng thời gian; khi pipeline, capacity do bước chậm nhất quyết định và latency vẫn cần đo cả đường.

## Bài 5 — Cache phải tính cả materialization

`cacheSaving(B,U,R,k)` trả `k*U - (B + k*R)`. B là build **chưa gồm read**, U là recompute mỗi consumer, R là read cache. Dùng `Models.cacheBreakEven` để validate input, rồi tính savings có dấu; k phải >0.

Input giả định B=150/U=90/R=20 ms:

| Consumer k | Không cache kU | Có cache B+kR | Lợi ích |
|---:|---:|---:|---:|
| 2 | 180 | 190 | −10 ms |
| 3 | 270 | 210 | +60 ms |

`cacheBreakEven` giải **bất đẳng thức nghiêm** `k > B/(U−R)`. Nếu B=140 thì k=2 chỉ hòa vốn, vẫn phải k=3 mới lợi. Nếu U≤R, không có số lần dùng nào làm cache nhanh hơn với B≥0, trả `None`. `cacheSaving` vẫn trả số âm để giải thích lỗ chứ không che bằng 0.

Test `B5` kiểm hai consumer, ba consumer, R>U và k=0; suite lab kiểm điểm bằng nhau, B=0 và B âm. Không assert cache phải nhanh hơn trong runtime test.

Tổng thời gian nhanh hơn chưa đồng nghĩa chi phí rẻ hơn: cache chiếm memory có thể buộc từ một node lên hai node. Tính lại `nodes * hours * price`, thêm cache miss, rebuild, eviction và invalidation. Số đo Ch34 cho build+1 read sát recompute nên chưa đủ kết luận; cần đo nhiều consumer cùng một lượt trước khi chọn.

## Bài 6 — Loại ứng viên không đạt SLO trước khi tối thiểu hóa chi phí

`Candidate` có tên, nodes, **tổng fleetRate**, p99Ms và hourlyPrice/node. `cheapest` validate mọi candidate, lọc `fleetRate >= requiredRate && p99Ms <= maxP99Ms`, rồi sắp theo `Models.costPerMillion` và trả `headOption`. Nếu không có phương án hợp lệ thì `None`; không tự nới SLO.

Test `B6` dùng các candidate **giả định**: một phương án rẻ nhưng thiếu rate, một rẻ nhưng p99 vượt SLO, hai phương án cùng rate/p99/giá node nhưng dùng một hoặc hai node. Với yêu cầu rate=500/s, p99≤5 ms, chọn phương án **một node** có fleetRate=1000/s, p99=2 ms, giá=0,12/node-hour; hai node với cùng tổng rate tốn gấp đôi. Không nhân throughput tổng thêm node lần nữa.

Test còn kiểm tất cả bị loại, vector rỗng và p99 NaN. Ch34 có ví dụ **đo thật** tất cả cap đều vượt p99≤6 ms nên chưa chọn được. Không dùng capacity service và p99 của một workload khác arrival/sink/schema làm một candidate hợp lệ. Hàm chọn theo giá capacity trong đề; với service luôn bật tải thấp, hãy dùng tổng chi phí thời gian cấp máy thực và headroom tối thiểu thay vì rate tối đa làm hóa đơn.

## Bài 7 — Memory và CPU cùng giới hạn concurrency

`feasibleSlots` lấy:

```scala
min(cpuSlots, max(0, heap - cache - reserve) / perTask)
```

Các byte cùng đơn vị và dùng BigInt. Input **giả định** heap=1024, cache=256, reserve=256, perTask=128 (có thể tất cả cùng tính theo MiB) cho 4 slot theo memory. CPU có 8 slot thì còn 4; CPU chỉ 2 thì còn 2. Cache lớn hơn heap cho 0, không trả slot âm.

Test `B7` kiểm cả hai giới hạn, thiếu memory và byte vượt Long; suite lab kiểm perTask=0. `cpuSlots` âm bị từ chối. Per-task phải là peak phù hợp dưới concurrency dự kiến, không dùng allocation/event hay used heap **sau GC** làm peak/task. Reserve trong heap chưa chứa stack/metaspace/native/direct memory ngoài heap; phải dự toán phần đó riêng trong giới hạn tiến trình/container.

## Gợi ý mini project

Giữ [bảng decision record của chương](../13-performance/ch34-performance-cost.md#6-ví-dụ-thực-tế-be-và-de), chạy một thay đổi mỗi lần và lưu raw. Dùng `cheapest` cho các candidate cùng ranh giới đã đo; dùng `wireCost`, `cacheSaving`, `feasibleSlots` để giải thích các ràng buộc. Với phần chưa đo (NIC/cluster/recovery), ghi **Chưa kiểm chứng**, nêu metric cần bổ sung và ngưỡng khiến quyết định đổi. Reviewer cần tái lập được test logic và lần theo mọi số timing về CSV, không cần thứ hạng tốc độ giống máy tác giả.
