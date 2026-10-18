# Evidence Chương 32

Ngày kiểm chứng: **17/10/2026**. Windows amd64, Scala CLI chạy Scala 2.13.18 + Temurin JDK 21.0.12.1, Spark 4.2.0. JVM thấy **12 CPU**, master `local[2,3]`, hai task thread cùng driver/executor JVM. Demo ghi max heap; MemoryDemo chạy `-Xmx768m`. Không kiểm soát tải nền toàn máy; thời gian là quan sát một lượt mỗi cấu hình, không là benchmark cluster hay bảo đảm thứ hạng.

Từ gốc repo:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode tests
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode demo
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode memory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode serializers
```

`-Mode all` chạy các bước tuần tự. Tất cả Scala CLI dùng `--server=false`; cache dependency ở `.cache/coursier`, build/tmp/TEMP/TMP/SPARK_LOCAL_DIRS/warehouse ở `.cache/t19`. Script để lại log gốc và exit code trong lần chạy mới. Các file `.txt` dưới đây chuyển encoding của log PowerShell thành UTF-8, bỏ mã màu ANSI và chuẩn hóa newline; không thay số, plan hay error. PowerShell có thể bọc stderr thành `NativeCommandError` ngay khi chỉ có WARN/hint; kết luận dùng process exit code và test summary.

| File | Nội dung |
|---|---|
| [lab-test.txt](lab-test.txt) | Suite lab: 11 test, retry exception cố ý; kết luận theo dòng summary cuối |
| [solutions-test.txt](solutions-test.txt) | Bảy test E1–E7 bằng Spark thật |
| [distributed-demo.txt](distributed-demo.txt) | ENV, partitioner/range, join/bucket plan, combine/group, exact/approx, payload/closure, task warning, cache/checkpoint, retry journal, batch, snapshot file shuffle |
| [memory.txt](memory.txt) | Forced spill 1.000 phần tử, heap 768 MiB; không mô phỏng OOM |
| [serializer-Java.txt](serializer-Java.txt), [serializer-Kryo.txt](serializer-Kryo.txt) | RDD groupByKey trong hai JVM/context; không phải SQL serializer benchmark |
| [dependency-poms.json](dependency-poms.json) | POM Spark/Scala/munit/BareLocalFileSystem/sbt, HTTP 200, URL và ngày tra |
| [official-sources.json](official-sources.json) | Nguồn API/mã Apache Spark + GlobalMentor đã tải được, SHA-256 và dòng nguồn liên quan |

`groupByKey` của SerializerDemo có chủ đích giữ payload OrderEvent để quan sát serializer, không khuyến nghị cho sum. Java và Kryo dùng cùng key/value/partition và kiểm đủ 20.000 record. Phép đo object payload thô của DistributedDemo không tương đương bytes shuffle đã nén/framing, nên hai phép đo có thể cho thứ hạng ngược nhau.

Plan approx có đoạn `... 27 more fields` do Spark rút ngắn chuỗi output, giữ nguyên trong transcript; các operator, partial/final và distribution vẫn được in. Snapshot `SHUFFLE_FILES` chỉ đếm file còn giữ lúc demo gần kết thúc; cleanup có thể xóa output cũ nên không coi đó là tổng số file đã tạo. ANY cùng remote=0 là quan sát hợp lệ trong local mode, không suy thành remote fetch trên cluster.

Warm-up: Spark count riêng; batch còn warm-up checksum 2.000 dòng. Bốn cấu hình dùng cùng 60.000 dòng, checksum CPU nhân tạo 2.048 bước/record; checksum XOR và số dòng được đối chiếu, không assert timing. firstMs = hoàn tất batch đầu kể từ submission; chưa gồm chờ nguồn/trigger hay event-time delay.

Giới hạn: tác giả chưa chạy sbt (Claude đã ghi 11/11 pass ở review vòng 1, cần chạy lại bản vòng 2), cluster nhiều máy, executor death, FetchFailed recovery, network partition, speculation, thay locality wait trên nhiều host, DB/HTTP sink hay Linux/macOS. RDD retry chỉ tiêm task exception trong một JVM. Không có test dịch vụ cần assume vì lab không phụ thuộc dịch vụ ngoài.

## Vòng 2 — sửa review T19 (17/10/2026)

Sáu transcript `.txt` ở trên đã được cập nhật bằng lượt vòng 2. `distributed-demo.txt` có thêm `artifactIsolation=false` và `BUCKET_PATH`: đọc trực tiếp cùng layout bucket qua Parquet vẫn đúng 200 dòng nhưng có hai `Exchange hashpartitioning`. Lab test kiểm kết quả multiset bằng nhau, catalog không Exchange, nhánh path có shuffle bytes. `batches` dùng `foldLeft` ở driver; vòng `while` chỉ còn trong checksum CPU nhân tạo.

Đã chạy `verify.ps1 -Mode all`, rồi chạy `-Mode tests` sau lần tách helper CPU cuối. Cả sáu entry point exit **0**; test cuối **11+7 pass / 0 fail / 0 ignored**, **0 compiler warning**. Chỉ comment lời giải được chỉnh encoding tiếng Việt sau đó; không đổi logic đã test. Mỗi lệnh Scala CLI vẫn có `--server=false`, COURSIER_CACHE/tmp/TEMP/TMP/SPARK_LOCAL_DIRS/workspace/warehouse giữ trong `.cache/` như vòng 1.

```text
Test run vn.scalaeng.distributed.DistributedSuite finished: 0 failed, 0 ignored, 11 total 27.308s
Test run vn.scalaeng.distributed.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 19.044s
```

[verification-round2.json](verification-round2.json) lưu lệnh wrapper, exit code/thời điểm cuối, test summary và SHA-256 source/transcript. Log gốc cùng toàn bộ lịch sử exit nằm trong `.cache/t19/`; transcript vòng 1 được giữ tại `.cache/t19/round1-evidence/`. `official-sources.json` bổ sung SQLConf 4.2.0, HTTP 200, SHA-256 và định nghĩa artifact isolation; dependency/version không đổi.

Vòng này chưa đo lại socket TIME_WAIT hay gây cạn cổng Windows. Giới hạn cluster, external sink và benchmark một lượt ở trên vẫn áp dụng.
