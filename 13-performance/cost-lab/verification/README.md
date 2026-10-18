# Bằng chứng T20 — Ch34

Ngày biên soạn/kiểm chứng: **17/10/2026**, Windows 11, Intel Core i7-9750H @2,60 GHz, JVM thấy 12 logical processor, RAM 34.188.873.728 byte, Eclipse Adoptium JDK 21.0.12.1, Scala 3.9.0. CPU model trong `sources.json`; `ENV`/`JVM` trong các transcript benchmark ghi RAM, heap và cờ thực tế.

## Nguồn gốc các lượt chạy

Phiên T20 trước bị dừng giữa chừng **sau khi đã chạy** local/cpu/batch/traps và test. Phiên tiếp tục đọc code và giữ nguyên bốn snapshot benchmark đó, không chọn lại lượt đẹp. Chương và lời giải được hoàn thiện dựa trên raw đã có. Phiên tiếp tục chạy lại hai suite sau sửa memory probe, chạy memory probe mới, kiểm lại toàn bộ URL/POM và tính lại bảng CSV.

`memory-before-frame-fix.txt` là transcript cũ khi bỏ static field nhưng heap vẫn còn cao. Code hiện tại tách materialize/sum ra helper `@noinline`; `memory.txt` và `memory-gc.log` là **lượt mới sau sửa**, không ghép GC log mới với transcript cũ. Chưa heap dump/JIT assembly để khẳng định root cũ; chương ghi giả thuyết và giới hạn.

Vòng 2 sửa theo review Claude ngày 17/10/2026: giữ nguyên CSV và snapshot local/cpu/batch/traps/memory; chạy lại `verify.py test` và `demo`. ModelDemo thêm `COST_IDLE` để minh họa dùng arrival thực. `analyze.py` thêm thống kê bước CPU trên 420 mẫu: 176 mẫu 0, delta dương nhỏ nhất và ước chung lớn nhất đều 15.625.000 ns (15,625 ms). Đây là bước quan sát trên Windows/JDK của snapshot, không là hằng số API. Bộ đếm dùng `com.sun.management.OperatingSystemMXBean.getProcessCpuTime()`; manifest bổ sung tài liệu JDK 21 tương ứng.

Máy dùng chung với team; không có nhật ký job nền tại thời điểm các snapshot nên không xác nhận được job nào chạy đồng thời. Verifier T20 gọi JVM tuần tự, chưa cô lập mọi tải nền. Không dùng p99 ở đây làm cam kết production.

## Tái lập từ gốc repository

```powershell
$env:PYTHONIOENCODING = 'utf-8'
python 13-performance/cost-lab/verify_sources.py
python 13-performance/cost-lab/verify.py all
python 13-performance/cost-lab/analyze.py
```

`verify.py all` **ghi đè** transcript và CSV của lượt hiện hành; copy cả thư mục verification nếu cần giữ snapshot trước khi tự chạy. Có thể chọn `test`, `demo`, `local`, `cpu`, `batch`, `traps`, `memory` thay `all`. Các JVM của verifier chạy tuần tự, không song song benchmark với test khác. Cache tại `.cache/coursier`, tmp/build tại `.cache/t20`; Scala CLI luôn `--server=false`, compiler bật `-deprecation -feature -Wunused:all -Werror`. Source verification là HTTP đọc tài liệu/artifact, không phải network benchmark.

## Bản đồ bằng chứng

| File | Nội dung / ranh giới |
|---|---|
| `lab-test.txt` | 15 test munit pass, 0 fail, 0 ignored, EXIT 0 |
| `solutions-test.txt` | 7 test lời giải pass, 0 fail, 0 ignored, EXIT 0 |
| `model-demo.txt` | Công thức với input giả định và bốn lỗi validation runtime thật |
| `local.txt` | ENV/JVM, FIRST, summary codec/query/cache, byte payload và checksum |
| `serialization.csv`, `query.csv`, `cache.csv` | 21 sample/case, wall/CPU/main-thread allocation/GC delta; warm-up không ghi vào CSV |
| `cpu.txt`, `cpu.csv` | 1/2/4 worker, 2/8/128 task, skew 80%; overhead worker **và** phần tính lại ở driver đều nằm trong wall |
| `batch.txt`, `batch-service.csv` | Service của 2.000 event đã có sẵn; checksum gồm overhead theo biên batch nên khác cap |
| `event-latency.csv` | 3 cap ×9 round ×2.000 event; scheduled arrival → completion, lịch không dời khi JVM thức muộn |
| `event-totals.csv` | Elapsed tới khi hoàn tất mọi event trong một round; dùng kiểm phần đuôi sau EOS |
| `traps.txt`, `traps.csv` | Kết quả bỏ đi so với checksum tiêu thụ; tốc độ discarded không là capacity nghiệp vụ |
| `memory.txt`, `memory-gc.log` | JVM riêng heap 256 MiB, live heap sau GC và sum; không đo RSS/peak streaming |
| `memory-before-frame-fix.txt` | Transcript trước sửa frame, chỉ để giải thích lỗi harness; không đại diện code hiện tại |
| `sources.json`, sáu file `*.pom` | URL, ngày UTC, HTTP 200 cho 6 pin +9 tài liệu, SHA-256; POM có artifact/version đã parse |
| `analysis.md` | Bảng tính lại từ raw bằng `analyze.py`, gồm bước quan sát CPU; mọi giá compute/bandwidth/egress đều **giả định** |
| `round2-checks.txt` | Đối chiếu 17 mục, 9 mục 3.x, code trích/output, local link, test, hash nguồn và snapshot giữ nguyên sau sửa review |

Phân vị của microbench là **run**, của event-latency là **event**. p99 của 21 run bằng max; event cùng batch/round có tương quan. Không có timing assertion trong test. FIRST không tách JIT/class loading; codec/CPU đã chạy validate trước FIRST. Counter CPU Windows thô, median=0 không có nghĩa không dùng CPU; main allocation không gồm worker.

## Trạng thái và giới hạn

- **Đã chạy:** Scala CLI lab/lời giải, ModelDemo, local/cpu/batch/traps, MemoryProbe +GC log, kiểm nguồn HTTP/POM và phân tích raw. Tổng **22 pass / 0 fail / 0 ignored**, compiler 0 warning.
- **Quyết định chưa được phép vượt bằng chứng:** cả ba cap đều không đạt p99 giả định 6 ms; cache một consumer chưa đủ lợi so với biến thiên; không biến tốc độ discarded-result thành giá nghiệp vụ.
- **Review vòng 1 đã có:** `.team/reviews/T20-ch34-review.md` ghi Claude chạy sbt: **15 pass / 0 fail**. Bản sửa vòng 2 được bàn giao riêng; Codex không chạy lại sbt hay tự đánh dấu chương hoàn tất review.
- **Chưa chạy trong T20:** JMH fork/assembly/JFR riêng cho Ch34, peak/RSS, benchmark mạng/cluster/cloud hay sink bền end-to-end. Ch25/Ch32/P4 chỉ được dẫn nguồn lại, không chạy lại trong T20.

Đơn giá 0,12/node-hour và 0,05/GiB, bandwidth 50.000.000 byte/s, mục tiêu rate/SLO trong bài là tham số học tập. Không có báo giá cloud đã kiểm chứng. Không suy capacity một JVM thành capacity/node của cluster.
