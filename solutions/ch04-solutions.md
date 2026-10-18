# Lời giải — Chương 4: Functions và Methods

> **Code:** [`code/src/main/scala/scalaeng/solutions/Ch04Solutions.scala`](code/src/main/scala/scalaeng/solutions/Ch04Solutions.scala)
> **Test:** [`code/src/test/scala/scalaeng/solutions/Ch04SolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch04SolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 5/5 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).

## Bài 1 — `compose3`

```scala
def compose3[A, B, C, D](f: A => B, g: B => C, h: C => D): A => D =
  f.andThen(g).andThen(h)
```

Các tham số kiểu `[A, B, C, D]` đảm bảo đầu ra của hàm trước khớp đầu vào của hàm sau — ghép sai thứ tự sẽ **không biên dịch**.
Khi gọi với lambda dạng `_`, compiler đôi khi không đủ thông tin để suy luận kiểu; test ghi rõ `compose3[String, String, Int, Boolean](...)`.

## Bài 2 — `countWhere` đệ quy đuôi

```scala
def countWhere[A](xs: List[A])(p: A => Boolean): Int =
  @tailrec
  def loop(rest: List[A], acc: Int): Int = rest match
    case Nil            => acc
    case h :: t if p(h) => loop(t, acc + 1)
    case _ :: t         => loop(t, acc)
  loop(xs, 0)
```

- Cả hai nhánh đều gọi `loop` ở vị trí cuối → tail-recursive; `@tailrec` xác nhận điều đó.
- Tách `(p: A => Boolean)` thành danh sách tham số riêng giúp gọi gọn: `countWhere(xs)(_ % 2 == 0)` — kiểu `A` đã biết từ `xs` nên lambda `_` được suy luận đúng.
- Trong thực tế bạn sẽ viết `xs.count(p)`. Bài tập để luyện kỹ thuật accumulator.

## Bài 3 — `paginateSafe`

```scala
def paginateSafe(total: Int, pageSize: Int = 20, page: Int = 1): (Int, Int) =
  require(pageSize > 0, "pageSize phải > 0")
  val pages = math.max(1, (math.max(total, 0) + pageSize - 1) / pageSize)
  val current = math.min(math.max(page, 1), pages)
  (current, pages)
```

- `require` ném `IllegalArgumentException` — đúng ngữ nghĩa "người gọi truyền tham số sai".
- **Kẹp giá trị (clamp)** `min(max(page, 1), pages)` thay vì báo lỗi: một lựa chọn thiết kế API. Lựa chọn khác: trả lỗi 400 cho client.
  REST API công khai thường trả về trang rỗng hoặc lỗi rõ ràng; hãy thống nhất trong team và ghi vào tài liệu API (Chương 14).
- `pages` tối thiểu 1 để "trang 1 của 1" luôn hợp lệ kể cả khi không có dữ liệu.

## Bài 4 — `averageDurationByUser`

```scala
def averageDurationByUser(events: List[(String, Long)]): Map[String, Double] =
  events.groupMap(_._1)(_._2).view
    .mapValues(ds => ds.sum.toDouble / ds.size)
    .toMap
```

- `groupMap(key)(value)` → `Map[String, List[Long]]`.
- `.view.mapValues(...)` tạo view lười; `.toMap` cụ thể hóa thành `Map` thật. Thiếu `.toMap` sẽ trả về một `MapView` — tính lại mỗi lần truy cập.
- `ds.size` không bao giờ là 0 vì mỗi nhóm có ít nhất 1 phần tử — không có lỗi chia cho 0.
- `.toDouble` **trước** phép chia; nếu không, `Long / Int` là chia nguyên.

## Bài 5 — `countSessions`

```scala
def countSessions(sortedTimestamps: List[Long], gapMs: Long): Int =
  @tailrec
  def loop(rest: List[Long], prev: Long, sessions: Int): Int = rest match
    case Nil => sessions
    case t :: tail =>
      if t - prev > gapMs then loop(tail, t, sessions + 1)
      else loop(tail, t, sessions)
  sortedTimestamps match
    case Nil           => 0
    case first :: tail => loop(tail, first, 1)
```

- Trạng thái cần mang theo: timestamp trước đó và số session — đó chính là hai accumulator.
- Khoảng cách **đúng bằng** gap vẫn cùng session (dùng `>` chứ không phải `>=`). Biên này phải khớp định nghĩa nghiệp vụ và có test riêng.
- Liên hệ DE: đây là phiên bản in-memory của **sessionization**. Trong streaming (Chương 27) bài toán khó hơn nhiều: sự kiện đến trễ, không theo thứ tự,
  và session có thể kéo dài qua nhiều lô dữ liệu — vì thế các streaming engine có khái niệm *session window* và *watermark*.
- Giả định "đã sắp xếp" được ghi trong tên tham số. Hàm không tự sắp xếp để tránh chi phí `O(n log n)` khi dữ liệu đã có thứ tự.

## Câu hỏi kiểm tra — gợi ý trả lời

1. `f.andThen(g)(x) == g(f(x))`; `f.compose(g)(x) == f(g(x))`.
2. `@tailrec` chỉ là yêu cầu kiểm tra; việc tối ưu thành vòng lặp compiler làm cho mọi hàm tail-recursive (tự gọi chính nó, không thể bị override). Annotation đảm bảo bạn không vô tình phá vỡ tính chất đó khi sửa code.
3. Với `msg: => String`, chuỗi (có thể ghép tốn kém) không được tạo nếu hàm không dùng `msg` khi log tắt. Với `msg: String`, chuỗi luôn được tạo trước khi gọi hàm.
4. Mỗi `_` là một tham số riêng: `_ * _` là hàm hai tham số `(a, b) => a * b`, trong khi `map` cần hàm một tham số.
5. Test có thể truyền đồng hồ giả trả giá trị xác định, nên kết quả test luôn giống nhau và không phụ thuộc tốc độ máy.
6. Closure là hàm "giữ" biến từ phạm vi nơi nó được tạo. Ví dụ: `multiplier(3)` trả về `x => x * factor` với `factor = 3` được giữ lại.
