# Bằng chứng chạy Chương 25

Ngày: 17/10/2026. Windows 11 amd64, JDK 21.0.12.1, Scala 2.13.18, Spark 4.2.0, master `local[2,3]`. Tất cả là một máy, driver và executor cùng JVM. Thời gian là một lượt quan sát mỗi cấu hình, không kiểm soát tải nền toàn máy; không suy thứ hạng hay thời gian sang cluster.

| File | Nguồn / ý nghĩa |
|---|---|
| `performance-demo.txt` | Output PerformanceDemo từ dòng ENV trở đi, gồm initial/final plan, số đo, stacktrace task exception chủ động tiêm; bỏ phần wrapper PowerShell và WARN khởi động trước ENV |
| `lab-test.txt` | Ba dòng munit summary cuối từ `.cache/t12/lab-test.log`: 13 test pass |
| `solutions-test.txt` | Ba dòng munit summary cuối từ `.cache/t12/solutions-test.log`: 6 test pass |
| `memory.txt` | Dòng MEMORY từ `.cache/t12/memory.log`; JVM 768 MiB, fraction 0.2, ép spill mỗi 10.000 phần tử; không phải OOM |
| `serializers.txt` | Dòng kết quả từ mỗi `.cache/t12/serializer-{Java,Kryo}.log`; mỗi serializer dùng một tiến trình mới |
| `dependency-poms.json` | Version đọc từ POM Maven Central trong Coursier cache và SHA-256 của POM; HTTP tải mới trong phiên này không thành công |

Log đầy đủ vẫn ở `.cache/t12/` (đã ignore), có thể được ghi lại khi chạy `verify.ps1`. Các file evidence này giữ lượt đã trích trong bài; thời gian/ID có thể khác khi chạy lại. Không sửa số đo để làm phương án tối ưu trông nhanh hơn.

Lệnh đã chạy và giới hạn kiểm chứng được ghi tại [handoff T12](../../../.team/handoffs/T12-ch25-spark-performance.md). Từ gốc repo, tái hiện bằng `powershell.exe -NoProfile -ExecutionPolicy Bypass -File 09-spark/spark-performance-lab/verify.ps1 -Mode tests` và các mode `demo`, `memory`, `serializers`. Mode tests/memory/serializers đã chạy qua script; PerformanceDemo chạy trực tiếp Scala CLI cùng options. sbt, UI/History Server và cluster nhiều máy chưa chạy.

Không có cảnh báo compiler trong hai log test cuối. Hint đề xuất munit 1.3.6 không phải compiler warning; vẫn giữ munit 1.2.4 theo chuẩn repo. Hadoop WARN thiếu winutils/native library không chặn các test ghi/đọc Parquet dùng BareLocalFileSystem. Task retry cố ý có một ERROR log nhưng action và suite thành công; xem failed attempt và sum thay vì coi mọi stderr là fail.
