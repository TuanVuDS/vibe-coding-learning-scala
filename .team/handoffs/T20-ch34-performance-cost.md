# Handoff T20 — Ch34 Performance và Cost

Tác giả: **Codex** · Reviewer theo brief T20: **Claude Code** · Ngày: **17/10/2026 (UTC+7)**.

**Sẵn sàng review chéo:** đủ 17 mục, 7 bài tập có lời giải và test, Scala CLI **22 pass / 0 fail / 0 ignored**, compiler **0 warning**. Chưa đánh dấu hoàn tất review/sbt. Không commit; chỉ ghi trong phạm vi brief (`13-performance/ch34-performance-cost.md`, `13-performance/cost-lab/`, `solutions/ch34-*`, handoff này, `.cache/`). Không sửa file dùng chung, `09-spark`, `11-data-platform` hay P3/P5.

## 1. Tiếp tục từ phiên bị ngắt

Đã đọc AGENTS.md, kế hoạch phase6–9, brief T20, PROMPT_PROJECT phần Ch34/11–14, template 17 mục và nội dung tiên quyết Ch11/JVM lab, Ch21, Ch25, Ch29/P4, Ch32. Đã đọc lại chương/code/test/script/transcript của T20 trước khi sửa.

Giữ nguyên lab và các snapshot local/cpu/batch/traps đã chạy ở phiên trước. Hoàn thiện mục 9 bằng bảng tính từ raw, thay phần chờ bổ sung ở mục 10 bằng lỗi runtime/đo đạc thật, viết lời giải văn bản, thêm analyzer và bản đồ evidence. Sửa probe memory sau khi thấy bỏ static reference mà used heap chưa giảm: tách materialize/iterator sum sang helper trả primitive trước khi gọi GC. Lưu transcript cũ riêng, chạy lại memory probe và cả hai suite. Không làm lại các chương/project của agent khác.

## 2. File bàn giao

Đường dẫn dưới đây tính từ gốc repo. Các file đã có được tiếp tục, không thay toàn bộ lab bằng thiết kế mới.

| Nhóm | File |
|---|---|
| Bài học | `13-performance/ch34-performance-cost.md` — hoàn thiện 17 mục, bảng số đo/cost/quyết định và giới hạn |
| Build lab đã có | `13-performance/cost-lab/project.scala`, `build.sbt`, `project/build.properties` |
| Code lab đã có | `src/main/scala/vn/scalaeng/cost/Models.scala`, `Orders.scala`, `CostBench.scala`, `Probes.scala`; đường dẫn gốc nhóm này là `13-performance/cost-lab/` |
| Sửa code ở lượt tiếp tục | `13-performance/cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala` — tách frame materialize/stream để tránh local tạm ảnh hưởng probe GC |
| Test lab đã có | `13-performance/cost-lab/src/test/scala/vn/scalaeng/cost/CostSuite.scala` — 15 test |
| Công cụ | `13-performance/cost-lab/verify.py`, `verify_sources.py` (tiếp tục dùng); `analyze.py` (mới: tính phân vị, rate, cost, CPU/wall, kiểm sample/round/event) |
| Lời giải | `solutions/ch34-solutions.md` (mới), `solutions/ch34-code/project.scala`, `Exercises.scala`, `ExercisesSuite.test.scala` (đọc và giữ code đã có, 7 test) |
| Bản đồ/chỉ số evidence | `13-performance/cost-lab/verification/README.md`, `analysis.md` (mới) |
| Transcript | Trong `13-performance/cost-lab/verification/`: `lab-test.txt`, `solutions-test.txt`, `model-demo.txt`, `local.txt`, `cpu.txt`, `batch.txt`, `traps.txt`, `memory.txt`, `memory-before-frame-fix.txt`, `memory-gc.log` |
| Raw sample | Cùng thư mục verification: `serialization.csv`, `query.csv`, `cache.csv`, `cpu.csv`, `batch-service.csv`, `event-latency.csv`, `event-totals.csv`, `traps.csv` |
| Nguồn phiên bản | Cùng verification: `sources.json`, `scala3-compiler_3-3.9.0.pom`, `munit_3-1.2.4.pom`, `circe-parser_3-0.14.17.pom`, `avro-1.12.2.pom`, `slf4j-nop-2.0.17.pom`, `sbt-1.12.11.pom` |
| Handoff | `.team/handoffs/T20-ch34-performance-cost.md` |

## 3. Lệnh và kết quả kiểm chứng thật

Tất cả lệnh từ gốc repo, Windows PowerShell. Verifier đặt `COURSIER_CACHE=<repo>/.cache/coursier`, TEMP/TMP/java.io.tmpdir/build workspace trong `.cache/t20`. Tất cả Scala CLI dùng `--server=false`. Compiler bật `-deprecation -feature -Wunused:all -Werror`; suppress chỉ tắt hint cập nhật dependency.

```powershell
$env:PYTHONIOENCODING = 'utf-8'
$env:PATH = 'C:\Users\VuTT\AppData\Local\Coursier\data\bin;' + $env:PATH
python 13-performance/cost-lab/verify.py test
python 13-performance/cost-lab/verify.py memory
python 13-performance/cost-lab/verify_sources.py
python 13-performance/cost-lab/analyze.py
```

`memory` được chạy trước `test` ở lượt tiếp tục; các lệnh trên liệt kê theo nhóm để tái lập, không mô tả chạy song song. Trong phiên trước, `verify.py demo`, `local`, `cpu`, `batch`, `traps` đã có transcript EXIT 0; giữ nguyên chúng. `verify.py all` có thể tái lập toàn bộ **tuần tự** nhưng sẽ ghi đè snapshot hiện hành; reviewer cần giữ bản evidence trước nếu muốn so sánh.

Hai lệnh test thực tế do verifier gọi (đầy đủ cờ Java/workspace ở transcript):

```text
scala-cli.bat test 13-performance/cost-lab --server=false --suppress-outdated-dependency-warning --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t20\build-lab ...
scala-cli.bat test solutions/ch34-code --server=false --suppress-outdated-dependency-warning --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t20\build-solutions ...
```

Output cuối nguyên văn từ [lab-test.txt](../../13-performance/cost-lab/verification/lab-test.txt) và [solutions-test.txt](../../13-performance/cost-lab/verification/solutions-test.txt):

```text
Test run vn.scalaeng.cost.CostSuite started
vn.scalaeng.cost.CostSuite: finished 0.276s
Test run vn.scalaeng.cost.CostSuite finished: 0 failed, 0 ignored, 15 total 0.286s
EXIT 0

Test run vn.scalaeng.cost.solutions.ExercisesSuite started
vn.scalaeng.cost.solutions.ExercisesSuite: finished 0.069s
Test run vn.scalaeng.cost.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 0.082s
EXIT 0
```

Không có compiler warning trong hai transcript cuối. Unit test kiểm công thức, validation, overflow/count, schedule/skew, round-trip Unicode/duplicates/Long biên, query rewrite và batching; **không assert thời gian tuyệt đối hay thứ hạng tốc độ**. Không cần DB/Kafka/Docker/socket của ứng dụng.

Memory probe cuối, JVM `-Xmx256m` ([memory.txt](../../13-performance/cost-lab/verification/memory.txt)):

```text
MEMORY rows=300000 beforeBytes=3778424 heldBytes=80312088 deltaBytes=76533664 bytesPerRow=255.11221333333333 sum=1529850000
STREAM releasedBytes=3920168 afterGcBytes=3922312 sum=1529850000 heapMaxBytes=268435456
```

`analyze.py` đọc toàn bộ raw và in:

```text
RAW CHECKS PASS: 21 rounds/case; stable checksums; 9 x 2000 scheduled events/cap, no missing event.
```

Đã kiểm thêm bằng Python read-only: đủ **17 mục có nội dung**, local link của chương/lời giải/evidence README đều tồn tại; **20 case** có summary p50/p95/p99/rate khớp CSV; **14 URL HTTP 200**, SHA-256 sáu POM lưu khớp manifest. Không chạy lại benchmark chỉ để chọn timing đẹp; chưa sửa code benchmark sau các snapshot, ngoại trừ MemoryProbe đã có lượt mới tương ứng.

## 4. Số liệu và quyết định cần review

Môi trường: Intel Core i7-9750H @2,60 GHz, 12 logical processor JVM thấy, RAM **34.188.873.728 byte**, Windows 11 amd64, Eclipse Adoptium **21.0.12.1**, G1. Heap benchmark 512 MiB; memory probe 256 MiB. Không kiểm soát hoàn toàn tải nền máy dùng chung.

- JSONL/Avro raw cùng 12.000 Order: **1.402.760 /845.226 byte**. p50 encode **18,5849 /4,4641 ms**, decode **17,4150 /6,1722 ms**. Schema Avro ngoài payload, count 4 byte; không phải Avro container/Schema Registry format. Network dùng byte thật nhưng bandwidth/giá **giả định**, không mở socket.
- Query filter-late/early cùng XOR: p50 **11,8074 /0,4954 ms**; không suy thành năng lực sink bền. Có dẫn bằng chứng plan/pushdown Ch25 kèm cảnh báo một lượt nhỏ không quyết định thứ hạng.
- CPU 4 worker/8 task đều: p50 **7,0561 ms/60.000 input**; skew 80% về task đầu **20,0935 ms**. Overhead là **12.000 bước ở worker +12.000 bước/task tính lại ở driver**, cùng nằm trong timing. Bài giải thích phần tuần tự này, không gọi đó là pure scheduler overhead Spark.
- Cache B/U/R median **11,8804 /12,1390 /0,1199 ms**; công thức cho k=1 nhưng lợi một consumer chỉ 0,1387 ms, **chưa đủ kết luận hơn nhiễu**. Dự toán hai consumer được gắn nhãn phép cộng median, chưa đo cả pipeline hai consumer.
- Batch cap1/16/128, nguồn đều 10.000/s: event p99 **13,1805 /16,7132 /19,0510 ms**; **cả ba không đạt SLO giả định 6 ms**, chưa chọn candidate. Service capacity cao không biến thành rate tính hóa đơn của service đang chờ nguồn. Cap128 ở lịch paced thực tế flush tối đa 51 vì timeout.
- Memory giữ 300.000 row tăng ~76,53 MB thập phân; sau release/stream heap sau GC gần baseline và sum khớp. **Không phải peak/RSS**, không đưa 255 byte/row làm hằng số object layout production.
- Trap discarded-result p50 **0,0063 ms**, consumed **8,4872 ms**. Chưa xem assembly nên chỉ nói phù hợp giả thuyết DCE; rate discarded không là capacity nghiệp vụ. FIRST batch1 **353,8310 ms**, median nóng **41,6982 ms**; chưa tách JIT/class loading khỏi tải nền.
- Bảng cost/million dùng **0,12 đơn vị tiền/node-hour giả định**; egress **0,05/GiB**, effective bandwidth **50.000.000 byte/s giả định**. Không trích giá cloud/không gọi tiền giả định là USD. Có phân biệt capacity, arrival thực, startup/idle và chi phí RAM làm tăng node count.

## 5. Phiên bản và nguồn đã kiểm chứng

Kiểm lại ngày **17/10/2026**, manifest timestamp **2026-10-17T07:54:59.610517+00:00**; cả sáu POM HTTP 200, parse đúng artifact/version, giữ file và SHA-256 trong [sources.json](../../13-performance/cost-lab/verification/sources.json).

| Thành phần | Pin | Nguồn chính |
|---|---|---|
| Scala | 3.9.0 | [scala3-compiler_3 POM](https://repo.maven.apache.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom) |
| JDK | target 21, thực tế Adoptium 21.0.12.1 | `ENV`/GC log runtime; [JDK 21 java](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html), [MemoryMXBean](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/MemoryMXBean.html) HTTP 200 |
| munit | 1.2.4 | [munit_3 POM](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom) |
| circe-parser | 0.14.17 | [circe-parser_3 POM](https://repo.maven.apache.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom) |
| Apache Avro | 1.12.2 | [Avro POM](https://repo.maven.apache.org/maven2/org/apache/avro/avro/1.12.2/avro-1.12.2.pom) |
| slf4j-nop | 2.0.17 | [slf4j-nop POM](https://repo.maven.apache.org/maven2/org/slf4j/slf4j-nop/2.0.17/slf4j-nop-2.0.17.pom) |
| sbt | 1.12.11 | [sbt POM](https://repo.maven.apache.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom), pin build.properties; **chưa chạy sbt** |

Tám tài liệu khác trong manifest gồm JMH DeadCode/Forking, hai trang JDK trên, Avro spec **1.12.0** (tài liệu encoding, runtime pin riêng **1.12.2**), Spark SQL performance/monitoring **4.2.0**, MIT queueing model assumptions; tất cả HTTP 200 ngày kiểm lại. `_3` và build/test thật kiểm Scala 3, không suy từ hỗ trợ Scala 2. **Không thêm module Spark hay dependency Spark vào T20.**

## 6. Chưa kiểm chứng và việc Claude cần làm

- **Review chéo chưa thực hiện.** Tập trung ranh giới metric, hai phần overhead CPU, checksum batch có overhead, cache gần hòa vốn, counter CPU thô, quantile 21 run và scheduled-arrival latency. Mục 10 giữ cả quan sát cũ và giả thuyết; không nâng giả thuyết GC root/DCE thành kết luận profiler.
- **sbt chưa chạy.** Theo AGENTS.md, sandbox trước đó chặn launcher BootServerSocket; lượt này dùng Scala CLI, không dựng một log lỗi sbt mới. Claude chạy `sbt.bat test` trong `13-performance/cost-lab` với JDK 21, kỳ vọng logic suite **15 test**, rồi chạy lại `verify.py test` nếu muốn kiểm cả 7 lời giải.
- **Network thật/cluster/cloud chưa chạy:** không đo NIC, remote bandwidth/TLS/egress, autoscaling, executor loss, remote shuffle hay giá nhà cung cấp. Không chạy benchmark mạng song song test/JVM khác. Ch25/Ch32/P4 chỉ là nguồn đã có, T20 không chạy lại các lab đó.
- **JMH forks/assembly, JFR/heap dump, RSS/peak streaming, CPU stability khi cô lập tải nền chưa chạy.** Wall-clock lặp lại, GC log và counter JVM phục vụ bài học; không có CI timing threshold hay cam kết nanosecond.
- **Pipeline bền end-to-end chưa đo:** checksum/codec không bao transaction/force/commit/ack; số snapshot P4 dẫn lại từ review T15 có nêu là trước sửa, không đánh đồng với measurement mới hoặc capacity P5.

## 7. Đề xuất cho file dùng chung — lead áp dụng

- **PROGRESS.md:** ghi “T20 tác giả hoàn thành; Ch34 đủ 17 mục, 7 bài tập, Scala CLI 22 pass, 0 fail/ignored, 0 compiler warning; có raw số đo/cost và handoff; chờ Claude review + sbt”. Chưa dùng nhãn hoàn tất toàn quy trình trước hai bước đó.
- **Glossary:** thêm/đối chiếu resource sizing, CPU service demand, process CPU vs wall-clock, live heap vs allocation vs RSS, effective bandwidth, open-loop, coordinated omission, tail latency/nearest-rank, cache materialization/break-even, headroom/utilization, node-hour/cost per million, sensitivity analysis. Phần giải thích tiếng Việt nằm ở mục 3–4 chương.
- **Compatibility matrix:** thêm lab Ch34 Scala **3.9.0**, JDK **21**, munit **1.2.4**, circe-parser_3 **0.14.17**, Avro JVM **1.12.2**, slf4j-nop **2.0.17**; Scala CLI test/run verified **17/10/2026 Windows**, sbt **1.12.11 pinned/chờ chạy**. Không ghi Spark hỗ trợ Scala 3 dựa trên T20; chương chỉ tham chiếu nhánh Spark riêng.

Không sửa trực tiếp các file dùng chung kể trên và không commit git.

## Vòng 2

Ngày **17/10/2026 (UTC+7)**. Sửa theo [review vòng 1 của Claude](../reviews/T20-ch34-review.md): **M1 và m1–m4**. Phần này cập nhật trạng thái bàn giao; các mục phía trên giữ lịch sử lượt đầu. Claude đã review và ghi `sbt.bat test`: **15 pass / 0 fail** trong review vòng 1. Codex chưa chạy lại sbt cho bản sửa này và không tự đánh dấu chương hoàn tất review.

### 1. Xử lý từng góp ý

| Góp ý | Đã sửa | Phần không sửa / chưa kiểm chứng và lý do |
|---|---|---|
| **M1** | Tiêu đề lên dòng đầu, blockquote phiên bản ngay sau; thêm **Vị trí trong lộ trình** Ch11 → Ch25 → Ch32 → Ch34 → P5. Viết gọn lại mục 3, giữ vấn đề, mô hình, lựa chọn và giới hạn. **9/9 mục 3.x** có code cốt lõi và **2–3 dòng output thật**: tổng **14 trích đoạn code / 21 dòng output**, đều được đối chiếu với source/transcript. Code bao warm-up/lặp/phân vị, checksum qua volatile sink, CPU vs wall-clock, GC, scheduling, payload/network, query, cache, scheduled latency và cost/million. | Không chép cả lab vào chương. Trích đoạn nằm trong ngữ cảnh hàm gốc, có link source; `requiredConsumers` của lời giải lấy mean rồi gọi `Models.concurrency`, được giải thích và dẫn link ở 3.2. |
| **m1** | Dịch ba docstring Python sang tiếng Việt; đổi comment mô tả greedy scheduling và framing sang câu tiếng Việt. Các comment còn lại đã dùng tiếng Việt kèm thuật ngữ kỹ thuật. | Giữ identifier, Scala CLI directives, tên test và literal thông báo lỗi/output tiếng Anh: chúng không phải comment và đang được dùng để đối chiếu kết quả thật. |
| **m2** | Mục 10.2 ghi đúng API `com.sun.management.OperatingSystemMXBean.getProcessCpuTime()`, đơn vị ns không bảo đảm độ chính xác ns; đối chiếu JDK 21 API. Analyzer tính trên **420 delta**, có **176 mẫu 0**; delta dương nhỏ nhất và ước chung lớn nhất đều **15.625.000 ns =15,625 ms**. Ghi cách đọc mẫu ngắn và giới hạn của tổng CPU/wall. | Không khẳng định mọi Windows/JDK có cùng độ phân giải; chỉ có bước quan sát của snapshot này. Chưa đo API trên máy khác hoặc cô lập CPU nghiệp vụ khỏi JIT/thread nền. |
| **m3** | Ghi i7-9750H, **12 CPU logic**, RAM, Windows/JDK và tải nền chưa cô lập cạnh **từng bảng số đo/ngoại suy ở mục 9 và bảng DCE ở 10.2**, cùng bối cảnh chung cho output 3.x. Analyzer cũng gắn bối cảnh cạnh từng bảng sinh ra. | **Chưa bổ sung danh sách job team chạy đồng thời lúc đo** vì không có nhật ký lịch sử. Ghi rõ không xác nhận được tác vụ nào chạy đồng thời; không suy đoán rồi gắn thành dữ kiện. Không chạy lại benchmark để thay snapshot khi chỉ sửa trình bày. |
| **m4** | Mục 3.1 dẫn JMH như công cụ microbenchmark JVM khi cần kết luận khác biệt nhỏ, giải thích fork/warm-up/tiêu thụ kết quả và dẫn mẫu OpenJDK. Ghi rõ **JMH chưa chạy trong T20**. | Không thêm dependency, tích hợp hay chạy JMH: review yêu cầu giới thiệu ở mức khái niệm. Lab vẫn là wall-clock lặp với giới hạn được nêu rõ. |

Không bỏ qua toàn bộ MINOR nào. Phần chưa thể xác minh của m3 và các giới hạn m2/m4 được ghi cụ thể ở bảng trên.

### 2. File tạo/sửa trong vòng này

Đường dẫn từ gốc repo; tất cả nằm trong phạm vi brief T20:

- `13-performance/ch34-performance-cost.md` — sửa M1, m2–m4; đồng bộ output test/demo với lượt mới.
- `13-performance/cost-lab/src/main/scala/vn/scalaeng/cost/Models.scala`, `Orders.scala` — chỉ sửa comment.
- `13-performance/cost-lab/src/main/scala/vn/scalaeng/cost/Probes.scala` — ModelDemo thêm một dòng `COST_IDLE`; **không đổi MemoryProbe**.
- `13-performance/cost-lab/verify.py` — chỉ dịch docstring; `verify_sources.py` — dịch docstring, thêm URL API CPU; `analyze.py` — dịch docstring, thêm bối cảnh máy và thống kê bước CPU từ raw.
- `13-performance/cost-lab/verification/README.md`, `analysis.md`, `sources.json`, `lab-test.txt`, `solutions-test.txt`, `model-demo.txt` — cập nhật bản đồ evidence, phân tích, nguồn và transcript chạy lại.
- `13-performance/cost-lab/verification/round2-checks.txt` — mới, kết quả đối chiếu chương/source/output và evidence.
- `.team/handoffs/T20-ch34-performance-cost.md` — thêm mục **Vòng 2** này. Script/bản đối chiếu tạm nằm trong `.cache/` đã được ignore.

**Giữ nguyên:** `CostBench.scala`, logic Models/Orders, toàn bộ test, lời giải, dependency/build pin và **15 file raw CSV/transcript benchmark/memory/GC**. Có đối chiếu byte với bản trước sửa trong `.cache/t20-r2-before`. Không ghi file dùng chung, file review hay ngoài brief; không commit git.

### 3. Lệnh và kết quả kiểm chứng chạy lại

Từ gốc repo, PowerShell; verifier đặt `COURSIER_CACHE`, TEMP/TMP/java.io.tmpdir và build workspace trong `.cache/`. Các JVM test/demo chạy **tuần tự**, luôn có `--server=false`; compiler dùng `-deprecation -feature -Wunused:all -Werror`.

```powershell
$env:PYTHONIOENCODING = 'utf-8'
$env:PATH = 'C:\Users\VuTT\AppData\Local\Coursier\data\bin;' + $env:PATH
python 13-performance/cost-lab/verify.py test
python 13-performance/cost-lab/verify.py demo
python 13-performance/cost-lab/verify_sources.py
python 13-performance/cost-lab/analyze.py
python .cache/t20-r2-check.py
```

Hai lệnh Scala CLI test thực tế (đầy đủ cờ JVM/workspace ở transcript):

```text
scala-cli.bat test 13-performance/cost-lab --server=false --suppress-outdated-dependency-warning --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t20\build-lab ...
scala-cli.bat test solutions/ch34-code --server=false --suppress-outdated-dependency-warning --workspace C:\Users\VuTT\zero_to_hero_scala\.cache\t20\build-solutions ...
```

Output thật từ [lab-test.txt](../../13-performance/cost-lab/verification/lab-test.txt) và [solutions-test.txt](../../13-performance/cost-lab/verification/solutions-test.txt):

```text
Test run vn.scalaeng.cost.CostSuite finished: 0 failed, 0 ignored, 15 total 0.527s
EXIT 0
Test run vn.scalaeng.cost.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 0.18s
EXIT 0
```

Tổng **22 pass / 0 fail / 0 ignored, 0 compiler warning**. Không thêm test timing; các test hiện có kiểm công thức và hợp đồng dữ liệu. ModelDemo chạy Scala CLI `run --server=false --main-class vn.scalaeng.cost.ModelDemo`, **EXIT 0**; hai dòng mới dùng ở 3.9 được lấy nguyên văn từ [model-demo.txt](../../13-performance/cost-lab/verification/model-demo.txt):

```text
COST 0.001333333333333333333333333333333333
COST_IDLE 0.003333333333333333333333333333333334
```

Giá/rate/node count ở hai dòng trên là input **giả định**, không phải phép đo fleet. Kết quả analyzer từ CSV giữ nguyên:

```text
CPU_COUNTER samples=420 zero=176 minPositiveNs=15625000 gcdPositiveNs=15625000 observedStepMs=15.625
RAW CHECKS PASS: 21 rounds/case; stable checksums; 9 x 2000 scheduled events/cap, no missing event.
```

[round2-checks.txt](../../13-performance/cost-lab/verification/round2-checks.txt) ghi: **17 mục có nội dung; 9/9 mục 3.x có code/output; 14 đoạn code khớp source; mọi dòng output trong text fence của chương khớp transcript/GC log; 88 local link tồn tại; 15 file snapshot không đổi; logic benchmark/model/codec và MemoryProbe không đổi**. Đây là kiểm tra tài liệu/evidence, không phải test tốc độ mới.

### 4. Phiên bản, nguồn và trạng thái còn lại

Giữ các pin tại mục 5 của handoff: **Scala 3.9.0, JDK 21** (snapshot Adoptium 21.0.12.1), **munit 1.2.4, circe-parser 0.14.17, Avro 1.12.2, slf4j-nop 2.0.17, sbt 1.12.11**. Không thêm thư viện. Kiểm lại nguồn ngày **17/10/2026**; [sources.json](../../13-performance/cost-lab/verification/sources.json) có timestamp **2026-10-17T08:34:33.309928+00:00**, **6 POM +9 tài liệu, tất cả HTTP 200**; SHA-256 của sáu POM lưu khớp manifest. URL pin/version chính thức nằm trong mục 5 và chương mục 16.

Nguồn mới: [OperatingSystemMXBean JDK 21 — getProcessCpuTime](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html#getProcessCpuTime()). Tài liệu chỉ bảo đảm đơn vị trả về, không bảo đảm độ chính xác ns. Con số 15,625 ms là phép tính từ CSV local, không suy từ tài liệu thành thuộc tính chung của Windows.

**Chưa chạy trong vòng 2:** sbt, JMH/fork/assembly/JFR, benchmark CPU/codec/batch/memory mới, mạng thật/cluster/cloud và sink bền end-to-end. Giữ snapshot vì các sửa đổi không đổi logic đo; không tuyên bố đã kiểm chứng các phần đó. Review vòng 1/sbt của Claude là bằng chứng đã có, khác lượt Scala CLI của Codex ở đây.

**Đề xuất lead áp dụng, Codex không sửa file dùng chung:** cập nhật PROGRESS thành “T20 đã sửa M1 và m1–m4; Scala CLI chạy lại 22 pass, 0 warning; review vòng 1 có sbt 15 pass; bản sửa có handoff Vòng 2”. Không tự nâng nhãn hoàn tất review. Glossary có thể thêm *precision vs accuracy* và *bước quan sát của bộ đếm CPU*. Compatibility matrix giữ nguyên version, cập nhật ngày chạy lại Scala CLI **17/10/2026** và dẫn review cho sbt của Claude.
