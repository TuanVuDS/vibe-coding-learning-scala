# Lời giải — Chương 7: Functional Programming

> **Code:** [`code/src/main/scala/scalaeng/solutions/Ch07Solutions.scala`](code/src/main/scala/scalaeng/solutions/Ch07Solutions.scala) ·
> **Test:** [`Ch07SolutionsSuite.scala`](code/src/test/scala/scalaeng/solutions/Ch07SolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `scalac 3.9.0`; 5/5 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21).

## Bài 1 — `parsePositiveInt`

```scala
def parsePositiveInt(s: String): Option[Int] = s.trim.toIntOption.filter(_ > 0)
```

`Option.filter` biến `Some(0)` thành `None` — không cần `if`.

## Bài 2 — `managerEmail`

```scala
for
  user      <- db.get(userId)
  managerId <- user.managerId
  manager   <- db.get(managerId)
  email     <- manager.email
yield email
```

Bốn chỗ có thể "không có" (user không tồn tại, không có quản lý, quản lý đã bị xóa, quản lý không có email) — không một `if` hay `null` nào.
Nhược điểm: `None` không cho biết **vì sao**. Nếu API cần trả thông báo cụ thể, đổi từng bước sang `Either` bằng `.toRight(...)`.

## Bài 3 — `parseBirthDate`

```scala
for
  nonEmpty <- Either.cond(s.nonEmpty, s, DateError.Empty)
  date     <- Try(LocalDate.parse(nonEmpty)).toEither.left.map(_ => DateError.BadFormat(s))
  _        <- Either.cond(!date.isAfter(today), (), DateError.InFuture(date))
yield date
```

**Vì sao `today` là tham số:** nếu gọi `LocalDate.now()` bên trong, hàm không thuần — test "ngày trong tương lai" sẽ hỏng khi thời gian trôi,
và xử lý lại dữ liệu cũ dùng sai ngày tham chiếu. Shell (HTTP handler, job) lấy ngày hiện tại hoặc ngày xử lý rồi truyền vào.
Test cũng kiểm tra biên: đúng `today` là hợp lệ.

## Bài 4 — tích lũy lỗi

```scala
(nameE, emailE, ageE) match
  case (Right(n), Right(e), Right(a)) => Right(Signup(n, e, a))
  case _ => Left(List(nameE, emailE, ageE).collect { case Left(err) => err })
```

- Ba kiểm tra **độc lập** được tính riêng, rồi gộp: tất cả `Right` → thành công; ngược lại → thu thập mọi `Left`.
- Bên trong mỗi field vẫn fail-fast hợp lý: tuổi không phải số thì không cần kiểm tra "< 13".
- Cách này dài dòng khi có 10 field. `cats.data.Validated` với cú pháp `(a, b, c).mapN(Signup.apply)` giải quyết gọn — Chương 8B (thư viện ngoài, không phải standard library).

## Bài 5 — tồn kho theo sự kiện

```scala
events.zipWithIndex.foldLeft[Either[StockError, Map[String, Int]]](Right(initial)):
  case (acc, (event, idx)) => acc.flatMap(step(_, event, idx))
```

- `foldLeft[Either[...]]` — phải ghi tham số kiểu, nếu không compiler suy ra kiểu `Right[...]` từ giá trị khởi đầu và báo lỗi ở `flatMap`.
- Sau khi gặp `Left`, `flatMap` không gọi `step` nữa — sự kiện thứ 3 (`Received("A", -1)`) trong test không bao giờ được xét, nên lỗi báo về là `Insufficient(1, ...)`.
  Lưu ý: `foldLeft` vẫn **duyệt** hết list (chỉ không làm gì). Với hàng triệu sự kiện, dùng đệ quy đuôi để dừng thật sự.
- Các guard `q <= 0` đặt **trước** các case chính — thứ tự case quan trọng.
- Liên hệ: đây là hình mẫu của event sourcing (BE) và state trong stream processing (DE, Chương 27).

## Câu hỏi kiểm tra — gợi ý

1. Cùng input → cùng output; không có side effect quan sát được.
2. Backfill ngày 1/10 vào ngày 14/10 sẽ dùng "hôm nay" là 14/10 → lọc/tính sai. Ngày xử lý phải là tham số của job.
3. Exception: lỗi lập trình, lỗi không phục hồi được. `Either`: lỗi nghiệp vụ, input sai — dự đoán được và cần xử lý.
4. Bọc API ném exception ở biên; chuyển ngay sang `Either` với lỗi domain, không để `Throwable` lan vào core.
5. Chuỗi không thể phân loại, đếm, hay xử lý khác nhau theo loại lỗi; dễ sai chính tả, không được compiler kiểm tra.
6. Fail-fast. Cần tích lũy khi các kiểm tra độc lập và người dùng cần thấy mọi lỗi cùng lúc (form, validate bản ghi).
7. Shell: poll Kafka, commit offset, ghi DB, log. Core: `decode(bytes): Either[DecodeError, Event]`, `validate`, `toRows(event): List[Row]` — thuần, test không cần Kafka/DB.
8. Phát hiện bản ghi "biến mất" giữa các bước — loại lỗi khó thấy nhất trong pipeline dữ liệu.
