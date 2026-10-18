# Lời giải Ch29 — Streaming Pipeline

Scala 3.9.0, JDK 21, munit 1.2.4; dependency khác pin trong [project.scala](ch29-code/project.scala). Code dùng model thật của P4, không mô phỏng thư viện test. Trạng thái chạy và output: [sol29.txt](../14-projects/p4-kafka-event-processing/verification/sol29.txt); chờ review chéo.

Chạy từ repo root:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test solutions/ch29-code --server=false
```

[Exercises.scala](ch29-code/src/main/scala/vn/scalaeng/solutions/ch29/Exercises.scala) chứa sáu hàm; [ExercisesSuite.scala](ch29-code/src/test/scala/vn/scalaeng/solutions/ch29/ExercisesSuite.scala) có sáu test có tên bắt đầu bằng số bài. Đây là helper mở rộng, không tự thay đổi P4 Store.

## Bài 1 — Đối soát biên nhận

`reconcile` kiểm tra expected không âm, kích thước bằng expected, source ID không trùng, kind thuộc tập đóng và reason có đúng trên nhóm lỗi. Sau đó `groupMapReduce` đếm theo kind. Chỉ kiểm tổng counts là chưa đủ: một source xuất hiện hai lần trong khi một source mất vẫn có thể giữ nguyên tổng.

Test đưa cùng receipt hai lần và receipt invalid thiếu reason để buộc hàm từ chối. `expected` là số nguồn đã hoàn tất trong tập cần đối soát, không phải số lần poll và cũng không phải end offset. Kafka log có thể có lỗ. Reason hiện là Option; production nên kiểm cả chuỗi không trắng ở biên tạo receipt.

## Bài 2 — Watermark nhiều partition

`globalWatermark` bỏ những partition thuộc idle set đã được operator xác định, rồi lấy min các mốc active. Active chưa có mốc → `None`; không còn active → `None`. Kiểm idle set không chứa partition lạ để tránh typo làm âm thầm bỏ nguồn.

Test có mốc 100, unknown, 50: khi chưa đánh idle thì unknown; partition unknown idle thì mốc chung 50; tất cả idle thì unknown. Helper trả candidate, chưa bảo đảm watermark phát ra không lùi khi partition resume. Engine/operator phải giữ watermark đã phát đơn điệu và quyết định resumed events quá cũ đi late hay side output. Không lấy candidate mới thấp hơn làm watermark lùi.

## Bài 3 — Chặn timestamp quá xa tương lai

`futureAllowed` chuyển sang BigInt **trước** phép cộng now+skew; eventTime tại biên được nhận, biên+1 bị chặn. Test sát Long.MaxValue chứng minh cộng không quay thành âm. Now truyền làm tham số nên test độc lập đồng hồ máy.

P4 đã tích hợp cùng quy tắc vào Store.process trước dedup/event time, với clock truyền được, skew mặc định 300 giây và reason `eventTime in the future`. CoreSuite kiểm DLT, timestamp mili-giây, đúng biên, overflow, policy mismatch và mở lại nhận event đúng giờ. Hàm bài tập vẫn độc lập để học phép tính, không được Store gọi trực tiếp. Cần chọn clock skew cho domain; không tự sửa eventTime sang now rồi làm mất bằng chứng. Guard này không giải quyết partition chậm.

## Bài 4 — DLT publish trùng

`uniqueDead` chỉ nhận DLT receipts, group theo `input.id`, yêu cầu các bản sao bằng nhau rồi lấy một. Sắp source ID cho output ổn định; đây là thứ tự chuỗi, không phải thứ tự offset số để xử lý nghiệp vụ.

Test hai bản giống nhau thu về một; thay attempts trên cùng source thì từ chối vì đây là dữ liệu audit mâu thuẫn. P4 persist receipt trước publish nên replay bình thường tạo envelope giống nhau. Helper giữ cả batch trong RAM, chưa phải consumer DLT bền; muốn chạy dài cần unique index/storage và namespace incarnation/topic/cluster phù hợp.

## Bài 5 — Budget bộ đệm

`fitsMemory` dùng `BigInt(partitions) * queuedEach * maxRecordBytes`. Không để Int×Int tràn trước khi đổi sang Long. Test nhỏ đạt đúng budget; test cấu hình rất lớn vượt Long cho kết quả false thay vì overflow.

Đây chỉ là payload estimate của queue, chưa gồm object overhead, fetch buffers, decoder, snapshot cũ/mới, seen/bucket/receipt hay copy JSON. Khi lập budget thật phải đo cả heap và native/direct memory, thêm headroom. Queue bằng 0 hợp lệ trong phép tính này nhưng không tự triển khai rendezvous queue.

## Bài 6 — Điều kiện đóng cửa sổ

`closed` kiểm start/watermark không âm, width dương, tính end bằng BigInt rồi so `end <= watermark`. Cửa sổ start 100 width 100 đóng ở watermark 200; chưa đóng ở 199. Test start sát Long.MaxValue chặn lỗi tràn end.

Helper chỉ trả trạng thái đóng; chưa phát kết quả hay xóa bucket/seen. Muốn cleanup phải tách retention của bucket, event ID và receipt theo replay horizon. Xóa seen cùng lúc đóng cửa sổ có thể nhận lại ID được dùng với eventTime mới, chính là giới hạn đã bổ sung ở Ch27 mục 10.
