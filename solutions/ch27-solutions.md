# Lời giải Chương 27 — Streaming Fundamentals

Scala 3.9.0 · JDK 21 · fs2-core 3.13.0 · Cats Effect 3.7.1 · munit 1.2.4 · munit-cats-effect 2.2.0. Đã chạy **7/7 test**, 0 fail, `-Werror`, ngày 17/10/2026; chờ review chéo. [Log thật](ch27-code/tests.txt).

[Đề bài](../10-streaming/ch27-streaming-fundamentals.md#12-bài-tập-cơ-bản--nâng-cao), [code đầy đủ](ch27-code/src/main/scala/vn/scalaeng/solutions/ch27/Exercises.scala), [test](ch27-code/src/test/scala/vn/scalaeng/solutions/ch27/ExercisesSuite.scala). Chạy từ repo root:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test solutions/ch27-code --server=false
```

## 1. Hai đồng hồ

`clocks(event, receivedAt, size)` dùng cùng hàm tumbling cho hai timestamp khác nhau. Thời điểm nhận được truyền vào, không gọi `System.currentTimeMillis` trong core. Nhờ vậy test giả lập replay bằng cách đổi tham số, không cần ngủ.

Test cố định event time 8, nhận lúc 100 và replay lúc 200. Event window vẫn `[0,10)`; processing window chuyển từ `[100,110)` sang `[200,210)`. Cả ba cửa sổ được so đầy đủ. Nếu tính báo cáo thời điểm nghiệp vụ bằng receivedAt, test sẽ bắt được việc replay đổi ngày/cửa sổ.

Chọn processing time khi câu hỏi là tải xử lý tại hệ thống; chọn event time khi câu hỏi là lúc người dùng hành động. Event time vẫn cần validation timestamp và chính sách late; đổi đồng hồ không tự giải quyết dữ liệu sai.

## 2. Sliding doanh thu

Mỗi sự kiện được gán vào **mọi** cửa sổ mà `Windows.sliding` trả về, sau đó cộng nguyên amount vào từng `(key,window)`. Không chia amount theo số cửa sổ: mỗi cửa sổ là một truy vấn tổng độc lập.

Golden dùng sự kiện tại 12 có amount `Long.MaxValue`, và tại 20 có amount 5. `[0,20)` chỉ chứa sự kiện đầu; `[10,30)` chứa cả hai; `[20,40)` chỉ chứa sự kiện sau. Giá trị lớn được đổi sang `BigInt` trước cộng. Test còn đảo thứ tự để kiểm tra phép cộng ở phạm vi **không có watermark/loại late** độc lập thứ tự.

Gọi số sự kiện là `n`, fan-out là `size/slide`, chi phí gán/cộng tỷ lệ `n * fan-out`; state phụ thuộc số cặp key/window. Không lấy tổng ba cửa sổ làm tổng toàn bộ dữ liệu vì cửa sổ chồng nhau. Lời giải giữ map hữu hạn trong RAM, chưa có eviction theo watermark.

## 3. Watermark nhiều nguồn

Đầu vào `(idle, Option[watermark])` biểu diễn hai vấn đề riêng: nguồn đã bị đánh dấu idle và nguồn chưa báo tiến độ. Lời giải:

1. Loại nguồn idle khỏi tập nguồn đang chờ.
2. Tập còn lại rỗng hoặc chứa `None` → trả `None`, chưa phát tiến độ mới.
3. Còn lại lấy watermark nhỏ nhất.

Nếu A báo 10 còn B báo 4, không chốt theo 10 vì B có thể còn gửi dữ liệu giữa 4 và 10. Nếu B chưa báo tiến độ và vẫn active, cũng không giả định B đã xong.

`None` khi tất cả idle **không có nghĩa reset watermark đã phát**. Runtime phải giữ watermark trước đó và chỉ cho tiến lên. Hàm này mới tính ứng viên; chưa quản lý timeout idle, trạng thái source hồi phục hay chống watermark từng source giảm. Những bước đó cần state và test riêng trước khi dùng trong engine thật.

## 4. Bridge session

`sessionPreviews` lấy từng prefix đã nhận, gọi bản tham chiếu batch `Windows.sessions`. Với thứ tự 0,18,9 và gap 10:

- Sau 0 có một phiên.
- Sau 18 có hai phiên chưa nối.
- Sau 9, sort lại thành 0,9,18; khoảng cách 9 và 9 đều nhỏ hơn 10, tạo một phiên `[0,28)` có count/total 3.

Test so cả số phiên qua ba bước và toàn bộ kết quả cuối. Nếu sink chỉ append ba preview mà không có khả năng thay thế/upsert, sẽ giữ các phiên cũ sai. Runtime cần đợi chốt theo watermark hoặc có định danh/cơ chế sửa kết quả tạm.

Đây là lời giải minh họa hữu hạn: giữ mọi input, sort lại nhiều lần, còn trả mọi preview. Tổng chi phí và bộ nhớ cao hơn một bộ xử lý session online chuyên dụng; không dùng để tuyên bố bounded-memory hay xử lý nguồn vô hạn.

## 5. Audit trạng thái

`Audit` nằm ở harness, giữ state hiện tại, bảng đếm quyết định, các result đã phát và peak số cửa sổ mở. Mỗi input luôn tăng đúng một loại quyết định. Test gồm accepted, duplicate, conflict và late, rồi so **toàn bộ** bảng đếm và result đã đóng.

Điểm khác với core: harness giữ `emitted` để assertion, còn `EventTime.State` chỉ giữ cửa sổ mở. Chép nguyên harness vào nguồn vô hạn sẽ lại giữ toàn bộ output trong RAM. Production cần đẩy output tới sink và chỉ giữ counter có kích thước cố định, hoặc metric có kiểm soát cardinality.

Test thứ hai tạo 1.000 ID/key cùng timestamp. Watermark không tiến vượt timestamp đó, nên cả 1.000 cửa sổ và fingerprint còn mở. Điều này chứng minh giới hạn thời gian chưa đủ để có trần bộ nhớ: cần thêm giả định tốc độ/key cardinality hoặc state store/backpressure/admission policy.

## 6. Backlog và backpressure

Với tốc độ cố định, backlog tăng là `max(arrival - service, 0) * seconds`, tính bằng `BigInt`. Test chốt nguồn 1.000/s, sink 800/s trong 60 giây có backlog 12.000; sink nhanh hơn nguồn cho backlog 0. Giá trị cực biên kiểm tra phép nhân không tràn, và cấu hình âm bị từ chối.

Mô hình này bỏ qua burst, dung lượng byte, thời gian khởi động và backlog ban đầu; chỉ dùng cho lý luận sơ bộ. Queue capacity 2 không đủ chứa backlog dài hạn nếu nguồn cứ giữ tốc độ cao. Khi nguồn có thể chờ, bounded queue làm tốc độ thực tế bị giới hạn bởi sink; khi nguồn không thể chờ, phải có nơi lưu hoặc chính sách drop/reject.

Test IO chạy `Backpressure.measure` thật: offer thứ ba chưa hoàn tất trước khi consumer bắt đầu; peak quan sát là 2; tiêu thụ đủ 12. Không assert đúng số mili giây của output demo vì scheduler thay đổi giữa các lần chạy. Đọc [log phép đo](../10-streaming/streaming-lab/verification/demo.txt) để phân biệt số đo thật với mô hình tốc độ giả định.
