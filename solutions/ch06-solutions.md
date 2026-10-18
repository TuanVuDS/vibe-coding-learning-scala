# Lời giải — Chương 6: Collections

> **Code:** [`code/src/main/scala/scalaeng/solutions/Ch06Solutions.scala`](code/src/main/scala/scalaeng/solutions/Ch06Solutions.scala) ·
> **Test:** [`Ch06SolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch06SolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 5/5 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).

## Bài 1 — `wordFrequency`

```scala
text.toLowerCase.split("[^\\p{L}\\p{N}]+").filter(_.nonEmpty).toList
  .groupMapReduce(identity)(_ => 1)(_ + _)
```

- `\p{L}` khớp **mọi chữ cái Unicode**, nên `đơn`, `hàng` được giữ nguyên. Regex `[^a-zA-Z]` sẽ cắt `đơn` thành `n` — lỗi phổ biến với dữ liệu tiếng Việt.
- `filter(_.nonEmpty)`: chuỗi bắt đầu bằng dấu câu tạo ra phần tử rỗng đầu tiên.
- Lưu ý chuẩn hóa Unicode: cùng chữ "ệ" có thể được mã hóa dựng sẵn (NFC) hoặc tổ hợp (NFD). Dữ liệu từ nhiều nguồn nên chuẩn hóa bằng
  `java.text.Normalizer.normalize(s, Normalizer.Form.NFC)` trước khi đếm (Chương 19).

## Bài 2 — `topN`

`sortBy((name, score) => (-score, name))` — tiêu chí phụ `name` làm kết quả xác định khi hòa điểm.

## Bài 3 — `mergeInventories`

```scala
(a.keySet ++ b.keySet).iterator.map(k => k -> (a.getOrElse(k, 0) + b.getOrElse(k, 0))).toMap
```

Cách sai thường gặp: `a ++ b` — với key trùng, giá trị của `b` **ghi đè** chứ không cộng. `.iterator` tránh tạo `Set` trung gian của các cặp.

## Bài 4 — `dedupLatest`

```scala
records.groupMapReduce(_._1)(identity)((x, y) => if y._2 > x._2 then y else x)
  .values.toList.sortBy(_._1)
```

- Một lượt duyệt, mỗi id chỉ giữ một bản ghi "đang thắng" — không giữ cả nhóm như `groupBy(...).map(_.maxBy(...))`.
- Khi hai bản có cùng `updatedAt`, hàm giữ bản gặp trước (`>` chứ không `>=`). Đây là quyết định phải ghi lại: trong CDC thật thường cần thêm
  tiêu chí phụ (offset Kafka, sequence number) để chọn **xác định**.
- Trên Spark (Chương 24) cùng bài toán được viết bằng window function `row_number() over (partition by id order by updated_at desc)` —
  **logic** giống, **thực thi** khác hẳn (shuffle theo id).

## Bài 5 — `errorBurstStarts`

```scala
if statuses.size < window then Nil
else statuses.sliding(window).zipWithIndex.collect {
  case (w, idx) if w.count(_ >= 500) >= threshold => idx
}.toList
```

Vì sao kiểm tra `size < window`: `sliding(n)` trên collection ngắn hơn `n` vẫn trả **một** cửa sổ (chứa toàn bộ phần tử), có thể gây cảnh báo sai.
Tự kiểm tra trong REPL: `List(1).sliding(3).toList`.

## Câu hỏi kiểm tra — gợi ý

1. `List` là danh sách liên kết: thêm cuối phải đi qua toàn bộ và sao chép; thêm đầu chỉ tạo một node mới trỏ vào list cũ.
2. Khi cần truy cập theo chỉ số, thêm cuối, hoặc collection lớn mà cách truy cập không thuần head/tail.
3. `map` nhận `A => B`; `flatMap` nhận `A => IterableOnce[B]` (hoặc `Option[B]`...) rồi làm phẳng.
4. `filter` + `map` (với pattern matching).
5. `filter`: `xs.foldLeft(List.empty[A])((acc, x) => if p(x) then x :: acc else acc).reverse`; `map` tương tự với `f(x) :: acc`.
6. `groupBy` giữ mọi phần tử trong List con; `groupMapReduce` chỉ giữ một giá trị gộp mỗi key.
7. `view` tránh tạo collection trung gian trong một tiến trình; Spark lazy để tối ưu toàn bộ kế hoạch và phân phối thực thi trên cluster.
8. Khi là builder cục bộ không thoát khỏi hàm, hoặc trạng thái được đóng gói kỹ trong một class (và đã cân nhắc đa luồng).
