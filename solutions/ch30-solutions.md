# Lời giải Ch30 — Streaming Reliability

Scala 3.9.0, JDK 21, munit 1.2.4; dependency pin tại [project.scala](ch30-code/project.scala). Đã có sáu test cho sáu bài, output thật tại [sol30.txt](../14-projects/p4-kafka-event-processing/verification/sol30.txt); chưa review chéo. Những helper này không phải thí nghiệm crash; thí nghiệm kill KRaft nằm trong P4 BrokerSuite.

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test solutions/ch30-code --server=false
```

[Code](ch30-code/src/main/scala/vn/scalaeng/solutions/ch30/Exercises.scala) và [test](ch30-code/src/test/scala/vn/scalaeng/solutions/ch30/ExercisesSuite.scala) dùng model và Policy của lab thật.

## Bài 1 — Commit frontier

Lời giải gọi lại `Policy.frontier`, không tạo một thuật toán thứ hai. Validation đòi offsets tăng chặt và không âm. `takeWhile(_._2)` lấy prefix hoàn tất; phần tử cuối quyết định offset+1, dùng `Math.addExact` để không wrap ở Long.MaxValue.

Test `[0 done, 2 pending, 5 done]` chỉ cho commit 1; khi cả ba xong cho commit 6. Empty trả None; offset giảm bị chặn. Ở runtime thật, map frontier tách theo TopicPartition và assignment epoch; helper chưa quản lý rebalance hay callback futures. Không commit max của mọi task done.

## Bài 2 — Backoff có cap

Với maxAttempts N có N-1 khoảng nghỉ. `Vector.tabulate` tính từng `initial * 2^n` bằng BigInt, lấy min cap rồi đổi Long. Cấu hình giới hạn 1..10 attempts, initial không âm, cap >= initial.

Test 4 attempts initial 10 cap 25 cho vector 10,20,25; 1 attempt không có khoảng nghỉ. Sát Long.MaxValue không tràn. Production có thể thêm jitter với nguồn ngẫu nhiên truyền vào để test; helper này chưa jitter và chưa gắn vào P4 Retry, vốn dùng pause 10/20 ms.

## Bài 3 — Sequence theo key

`ordered` group sequence theo key, mỗi vector phải không giảm; key khác xen kẽ không ảnh hưởng nhau. Test a1,b99,a2,a2 đúng, a2,b1,a1 sai. Duplicate sequence cho phép ở mức kiểm thứ tự nhưng cùng sequence khác payload vẫn có thể là conflict; cần fingerprint/version nghiệp vụ riêng.

Chỉ kiểm trace hữu hạn đã có trong RAM, không giữ state qua restart hay chứng minh Kafka chia partition đúng. Nếu domain yêu cầu không có lỗ thì còn cần phát hiện sequence thiếu và quy trình đợi/bù; không tự bỏ event vì `seq > last+1` trong một helper kiểm thứ tự đơn giản.

## Bài 4 — Budget poll chứa retry

`admitted` kiểm số waits đúng attempts-1, mỗi khoảng không âm. Worst duration là `attempts * oneAttemptMillis + sum(waits)` bằng BigInt. Nếu chưa vừa Long thì trả false; nếu vừa, gọi `Policy.withinBudget` để nhân cả batch rồi cộng margin.

Test một record có 3×1000+10+20 ms và margin 1000 vừa poll 10000; ba record không vừa; cấu hình cực lớn không wrap. Đây là kiểm tra giả định latency upper bound, không đảm bảo file/network thực tế hoàn tất đúng hạn. Cần timeout ở adapter và đo tail latency; policy không hủy task đang chạy.

## Bài 5 — Tổng committed lag

Mỗi cặp là end offset và Option committed. Validation yêu cầu 0 <= committed <= end; một committed unknown hoặc vector không có partition thì tổng unknown. Khi đầy đủ mới cộng bằng BigInt; hai lag Long.MaxValue không tràn.

Nếu committed > end sau truncate/tạo lại topic thì phải điều tra incarnation/reset policy, không âm thầm clamp 0. Lag là chênh vị trí offset, không phải count tuyệt đối trong topic compacted/transactional. Không lấy position mới poll làm committed.

## Bài 6 — Redrive có validation

`redrive` chặn receipt không phải DLT, decode correctedRaw bằng chính Json.event rồi so orderId với key gốc. Trả nguyên Event hợp lệ, không tự đổi ID. Test JSON vẫn sai, key sai và non-DLT bị chặn; raw đã sửa hợp lệ giữ ID stable.

Sau khi operator duyệt, producer phải tạo record Kafka mới và lưu audit nối provenance gốc với offset mới; **helper chưa gửi**. Nếu event đã accepted thì ID cũ bảo vệ không cộng lại. Nếu conflict với event đã accepted, không đổi ID để né dedup rồi charge thêm: thiết kế correction event riêng. Với late, publish lại cùng eventTime có thể vẫn late; cần backfill hoặc policy riêng, không xóa watermark để ép nhận.
