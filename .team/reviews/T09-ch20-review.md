# Review T09 — Chương 20 (vòng 1)

Reviewer: Codex · Ngày: 17/10/2026 · Kết luận: **CHƯA ĐẠT — 6 MAJOR, 4 MINOR; không có BLOCKER.**

Đã đọc handoff, đủ bài học/lab/test/config/data và lời giải/code/test Ch20. Đủ 17 mục, tiếng Việt có tính sư phạm, giải thích golden/mutation và phân biệt mô phỏng trong một JVM với Spark tốt. Bảng “phân tán cần kết hợp + giao hoán” đúng với phép reduce không giữ thứ tự; tuy nhiên API thực tế chưa thể hiện yêu cầu đó. Không sửa mã tác giả.

## Kiểm chứng chạy thật

`COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`, `.cache/` được ignore. Các build dùng `--workspace` trong `.cache/review-batch/`.

- Trong `08-data-processing/transform-lab`: `scala-cli.bat test . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t09-lab-build` → exit 0; PropertySuite **4/4**, TransformSuite **8/8**, 0 failed/ignored.
- Trong `solutions/ch20-code`: tương tự, workspace `t09-sol-build` → exit 0; Ch20SolutionsSuite **6/6**, 0 failed/ignored.
- Chạy `TransformDemo` bằng `scala-cli.bat run . --server=false --workspace C:/Users/VuTT/zero_to_hero_scala/.cache/review-batch/t09-demo-build --main-class vn.scalaeng.transform.TransformDemo --java-opt=-Dfile.encoding=UTF-8`: số lượng valid/rejected, các tỷ lệ quality, ba dòng Stats và trung bình 550/280 khớp mục 9. Console qua runner có lỗi hiển thị dấu tiếng Việt; không quy lỗi encoding này cho file UTF-8 của bài.
- Probe riêng `.cache/review-batch/t09-probe/Probe.scala`: `scala-cli.bat run .cache/review-batch/t09-probe --server=false --main-class Probe --java-opt=-Dfile.encoding=UTF-8` → exit 0. Đã thử overflow, hoán vị, Monoid không giao hoán, null, NaN và sai đơn vị tính.
- Không chạy sbt, benchmark triệu dòng hay mutation lịch sử; không xác nhận độc lập các số đo/seed lỗi cũ. Không có compiler warning trong các lượt compile đã chạy; dependency hints không phải compiler warning.

## MAJOR

### M1 — Phép nhân tiền và lời giải TopK vẫn tràn số âm lặng lẽ

Vị trí: `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Model.scala:19` và `:59`; `solutions/ch20-code/src/main/scala/vn/scalaeng/solutions/ch20/Ch20Solutions.scala:59`; `08-data-processing/ch20-functional-data-transformation.md:140`.

`Validate.order` chấp nhận quantity=2, unit_price_vnd=Long.MaxValue. `revenueVnd` chạy thật trả **-2**, `Reports.revenueBySegmentDay` trả `Stats(1,-2,-2,-2)`. `Math.addExact` ở combine không bắt được vì giá trị đã sai trước khi vào Stats. Với quantity=1, hai dòng cùng SKU giá Long.MaxValue và 1, `TopK.right` trả `List((A,-9223372036854775808))`: lời giải dùng cộng Long thường ở cả từng phần và Monoid Map.

Đề xuất: kiểm tra cả phép nhân lẫn phép cộng; dùng BigInt cho tiền nguyên hoặc `multiplyExact/addExact` với kết quả lỗi có cấu trúc. Đặt bất biến tại khâu dựng Order nếu muốn mọi Order hợp lệ luôn tính được revenue. Áp dụng chính sách nhất quán ở TopK và ví dụ, thêm test qua **Validate → Reports**, không chỉ test `Stats.combine` trực tiếp.

### M2 — Monoid chưa đủ cho partition không giữ thứ tự

Vị trí: `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Execution.scala:26` và `:28`; `08-data-processing/ch20-functional-data-transformation.md:377`.

`Execution.partitioned[A,K,V: Monoid]` nhận cả Monoid nối chuỗi hợp lệ. Probe cùng key=0, dữ liệu `List("a","b","c","d","e","f")`: inMemory trả `abcdef`; n=3, seed 1/2/3 trả **adbcfe / befadc / cefdab**. Nối chuỗi kết hợp và có trung hòa, nhưng không giao hoán. Dòng “khác nhau nghĩa là phép gộp không phải Monoid” trong debug vì vậy sai. Bảng lý thuyết biết cần giao hoán nhưng hàm/comment/test chưa đồng bộ.

Đề xuất: yêu cầu `CommutativeMonoid[V]` cho phiên bản chia ngẫu nhiên, cung cấp instance/law test thích hợp cho Stats, hoặc giữ thứ tự gốc bằng thiết kế khác. Giữ Monoid thường cho inMemory/streaming có thứ tự. Thêm phản ví dụ String/List và sửa debug để phân biệt lỗi kết hợp với lỗi giao hoán. [Cats Monoid](https://typelevel.org/cats/typeclasses/monoid.html) có String là ví dụ Monoid, không đòi giao hoán; [Spark PairRDDFunctions](https://spark.apache.org/docs/4.1.2/api/scala/org/apache/spark/rdd/PairRDDFunctions.html) quy định associativity/commutativity cho reduce theo khóa (tra 17/10/2026).

### M3 — Stats với checked Long không phải Monoid toàn phần trên miền đang dạy

Vị trí: `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Aggregate.scala:17`; `08-data-processing/transform-lab/src/test/scala/vn/scalaeng/transform/PropertySuite.scala:17`; `08-data-processing/ch20-functional-data-transformation.md:168`.

Cho a=Stats.of(Long.MaxValue), b=Stats.of(1), c=Stats.of(-1). Probe `(a |+| b) |+| c` → `Failure(java.lang.ArithmeticException: long overflow)`, còn `a |+| (b |+| c)` → `Success(Stats(3,9223372036854775807,-1,9223372036854775807))`. Cả ba là Stats dựng bằng factory, tổng cuối vừa Long. Cách đặt ngoặc/chia phần làm thay đổi cả khả năng thành công. Property hiện cố ý chỉ sinh ±1.000.000 nên không chứng minh phát biểu “mọi danh sách/mọi cách chia”. Đếm count cũng cộng unchecked.

Đề xuất: dùng BigInt cho trạng thái cộng để bảo toàn luật và chỉ kiểm tra/chuyển về Long ở ranh giới xuất kết quả. Hoặc ghi đây là phép gộp có thể thất bại, nêu miền bảo đảm mọi tổng trung gian biểu diễn được, không quảng bá là Monoid toàn phần. Test cancellation ở biên, nhiều phần/chunk và miền của count. Đổi “property test khẳng định với mọi...” thành phạm vi generator và số lượt thử thực tế.

### M4 — Ngưỡng NaN vô hiệu hóa quality gate

Vị trí: `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Quality.scala:35`; `solutions/ch20-code/src/main/scala/vn/scalaeng/solutions/ch20/Ch20Solutions.scala:76`.

Probe một đơn hợp lệ về trường nhưng customer C9 không tồn tại; gọi `GatedPipeline.run(..., Double.NaN)` → **Published(Map(), QualityReport(... customer_id tồn tại: failed=1, checked=1 ...),0)**. So sánh `failRate > NaN` luôn false; +Infinity/>1 cũng bỏ mọi chặn. Không có validate cấu hình ở API gate.

Đề xuất: kiểm tra ngưỡng hữu hạn trong [0,1] tại ranh giới; từ chối cấu hình sai bằng lỗi có cấu trúc hoặc require rõ ràng. Áp dụng cả `RefundRateRule(maxRate)`. Test NaN, ±Infinity, âm, >1 và biên đúng bằng ngưỡng. [JDK Double.NaN](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Double.html#NaN) (tra 17/10/2026); kết quả bypass ở đây đã chạy thật, không chỉ suy luận từ tài liệu.

### M5 — Bài 1 và nhãn báo cáo nhầm dòng hàng với đơn hàng

Vị trí: `solutions/ch20-code/src/main/scala/vn/scalaeng/solutions/ch20/Ch20Solutions.scala:14`; `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/TransformDemo.scala:45`; `08-data-processing/ch20-functional-data-transformation.md:253`.

Model là **order line**, khóa `(order_id,sku)`; O1 mẫu có hai SKU. Nhưng RefundRateRule đếm phần tử List làm mẫu số “tỷ lệ đơn Refunded”. Probe O1 Paid có 10 SKU và O2 Refunded có 1 SKU cùng ngày: code trả `RuleResult(...,checked=1,failed=0,...)` vì 1/11≈9,09% <20%; tỷ lệ theo **đơn** thực tế 1/2=50%, ngày phải vi phạm. Ngay bảng mẫu, Retail ngày 15 ghi “Đơn=2” dù chỉ có **một đơn O1**, với hai dòng hàng; min/max cũng là giá trị dòng hàng.

Đề xuất: xác định rõ grain của từng metric. Nếu yêu cầu tỷ lệ đơn, gom/dedup theo order_id (và ngày theo hợp đồng), xử lý trạng thái không nhất quán giữa các dòng, rồi tính tỷ lệ. Với doanh thu Stats hiện tại, đổi nhãn count/min/max thành dòng hàng, hoặc tổng hợp theo đơn trước nếu muốn thống kê đơn. Thêm test một đơn nhiều SKU và một đơn Refunded; test hiện mỗi đơn chỉ một SKU nên che lỗi.

### M6 — Bài 3 chưa đáp ứng bất biến thứ tự khi phiên bản bằng nhau

Vị trí: `solutions/ch20-code/src/main/scala/vn/scalaeng/solutions/ch20/Ch20Solutions.scala:38`; `08-data-processing/ch20-functional-data-transformation.md:384`; `solutions/ch20-solutions.md:45`.

Hai update hợp lệ `CustomerUpdate("C1","HN",1)` và `CustomerUpdate("C1","Hue",1)`: forward giữ HN, reverse giữ Hue. Lời giải có cảnh báo tie, nhưng code/test và yêu cầu bài vẫn khẳng định “không phụ thuộc thứ tự đến” không kèm tiền điều kiện; CDC timestamp mili giây không bảo đảm duy nhất.

Đề xuất: mở rộng Versioned với sequence/tie-breaker xác định, hoặc trả lỗi conflict cho cùng key/version khác payload; cũng có thể giới hạn đặc tả chỉ cho version duy nhất và kiểm tra điều kiện đó. Thêm test tie tương đương và tie mâu thuẫn ở mọi hoán vị. Không dùng “chọn bản đầu” làm cách phá hòa nếu vẫn giữ bất biến thứ tự.

## MINOR

### m1 — Null tại raw boundary không được xử lý nhất quán

Vị trí: `08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Model.scala:40`, `:43`, `:46`.

`nonEmpty` chủ động dùng Option(s), nhưng status=null/quantity=null làm `Validate.order` ném NullPointerException ở `s.trim`, thay vì trả lỗi tích lũy. Probe đã chạy cả hai. Reader CSV mẫu không sinh null nên đây chưa phải lỗi đường chạy demo.

Đề xuất: hoặc ghi rõ và bảo vệ ranh giới non-null khi tích hợp Java/nguồn ngoài, hoặc chuẩn hóa null thành lỗi DataError cho mọi field. Test raw có nhiều field null để việc “gom mọi lỗi” không bị exception cắt ngang.

### m2 — Validation bị loại không tham gia quality gate; cần nêu chính sách

Vị trí: `solutions/ch20-code/src/main/scala/vn/scalaeng/solutions/ch20/Ch20Solutions.scala:77`; `solutions/ch20-solutions.md:92`.

Probe input duy nhất có ngày sai → `Published(Map(), QualityReport(... checked=0,failed=0 ...),1)`, dù 100% input bị loại. Đây có thể là chính sách cho phép xuất dữ liệu sạch còn lại, nhưng chương không giải thích gate chỉ đo **sau validation**. Demo còn in “CHẶN” rồi tiếp tục tính doanh thu để minh họa.

Đề xuất: nêu rõ phạm vi gate và hành vi all-rejected/empty; thêm rule tỷ lệ lỗi validation nếu mục tiêu là chặn nguồn dữ liệu hỏng. Gắn nhãn phần doanh thu demo là minh họa trên dữ liệu lọc, hoặc chỉ chạy khi gate qua để tránh người mới sao chép nhầm.

### m3 — Thuộc tính/generator/shrinking bị mô tả như chứng minh tuyệt đối

Vị trí: `08-data-processing/ch20-functional-data-transformation.md:168`, `:376`, `:383`; `08-data-processing/transform-lab/src/test/scala/vn/scalaeng/transform/PropertySuite.scala:17`; `solutions/ch20-code/src/test/scala/vn/scalaeng/solutions/ch20/Ch20SolutionsSuite.scala:23`.

Các property chỉ thử miền hữu hạn được sinh (chunk 1..64, n 1..8, số tiền nhỏ). Test Mean chỉ sinh số nguyên -1000..1000, không phải mọi BigDecimal/MathContext. Custom Gen[Stats] không tự cung cấp một shrinker theo cấu trúc Stats; ScalaCheck không bảo đảm tìm phản ví dụ nhỏ nhất toàn cục. Probe cộng Mean với 1e34,-1e34,1 **đúng bằng 1 ở cả hai cách gộp**; không có bằng chứng để quy thêm lỗi làm tròn cho phép cộng Scala BigDecimal trong lời giải.

Đề xuất: dùng “kiểm tra thuộc tính trên nhiều mẫu”, công bố miền, thêm boundary generator và shrinker nếu cần; phân biệt kiểm thử với chứng minh toán học. [ScalaCheck User Guide](https://www.scalacheck.org/documentation.html), [Scala BigDecimal](https://www.scala-lang.org/api/2.13.18/scala/math/BigDecimal.html) (tra 17/10/2026).

### m4 — Bàn giao thiếu nguồn phiên bản cụ thể; phản ví dụ TopK có một câu sai

Vị trí: `.team/handoffs/T09-ch20-transformation.md:15`; `08-data-processing/ch20-functional-data-transformation.md:434`; `solutions/ch20-code/src/test/scala/vn/scalaeng/solutions/ch20/Ch20SolutionsSuite.scala:37`; câu tương tự ở `08-data-processing/ch20-functional-data-transformation.md:366`.

Handoff có version nhưng không kèm link artifact/release cụ thể theo AGENTS.md 2.3; root Maven trong bài khó kiểm toán. Comment nói C “không đứng đầu ở partition nào”, nhưng p3 có C40>A10, nên C chính là top-1 p3. **Phản ví dụ và kết quả test vẫn đúng**: C mất đóng góp p1/p2.

Đề xuất: bổ sung POM/release URL và ngày tra; sửa câu thành “không đứng đầu ở mọi partition chứa nó, nên bị mất một phần doanh thu”.

## Điều kiện review lại

Sửa M1–M6, bổ sung regression/properties biên nêu trên, chạy lại lab và lời giải bằng Scala CLI; lead chạy sbt cuối. Các kết quả xanh hiện tại xác nhận đường chạy mẫu, chưa đủ xác nhận các khẳng định tổng quát.
