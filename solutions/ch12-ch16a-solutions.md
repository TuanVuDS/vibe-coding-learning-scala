# Lời giải — Chương 12 và 16A

> **Code:** [`backend-code/`](backend-code/) — [`Ch12Solutions.scala`](backend-code/src/main/scala/vn/scalaeng/solutions/Ch12Solutions.scala),
> [`Ch16aSolutions.scala`](backend-code/src/main/scala/vn/scalaeng/solutions/Ch16aSolutions.scala), test [`BackendSolutionsSuite.scala`](backend-code/src/test/scala/vn/scalaeng/solutions/BackendSolutionsSuite.scala)
> **Trạng thái kiểm chứng:** đã biên dịch với `-Werror`; 7/7 test pass (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21). Ch12 bài 1, 4, 5 và Ch16A bài 5b là hướng dẫn (không có code chạy sẵn).

## Chương 12

**Bài 1 — `PUT`.** Thêm `ProductStore.replace(id, name, price): Option[Product]` dùng `modify` (CAS) — trả `None` nếu không có id. Route `PUT /api/v1/products/:id` bọc `requireRole(Role.Admin)`,
kiểm tra `Content-Type` (415), validate body (chỉ `name`, `price`; `sku` có mặt mà khác giá trị cũ → 422). Test idempotent: gửi cùng body 2 lần, so sánh hai response và trạng thái store — phải giống hệt.
Nhớ cập nhật router: `PUT` trên `/products/:id` sẽ xuất hiện trong header `Allow` của 405.

**Bài 2 — `sort`.**

```scala
def sortProducts(ps: List[Product], sort: Option[String]): Either[Response, List[Product]] =
  sort.getOrElse("id") match
    case "id"         => Right(ps.sortBy(_.id))
    case "price_asc"  => Right(ps.sortBy(p => (p.price, p.id)))
    case "price_desc" => Right(ps.sortBy(p => (-p.price, p.id)))
    case other        => Left(Response.problem(400, "Bad Request", s"sort phải là id|price_asc|price_desc, nhận '$other'"))
```

Tie-break theo `id` bắt buộc: không có nó, hai sản phẩm cùng giá có thể đổi chỗ giữa hai lần gọi và xuất hiện ở cả trang 1 lẫn trang 2. Áp dụng **trước** `slice` phân trang.

**Bài 3 — ETag / 304** (viết thành middleware dùng chung cho mọi GET):

```scala
val conditionalGet: Middleware = next => req =>
  val res = next(req)
  if req.method != Method.GET || res.status != 200 then res
  else
    val tag = etag(res.body)   // 8 byte đầu của SHA-256, dạng "\"...\""
    val matches = req.header("If-None-Match").exists(_.split(",").map(_.trim).exists(t => t == tag || t == "*"))
    if matches then Response(304, Map("ETag" -> tag)) else res.withHeader("ETag", tag)
```

Lưu ý: middleware này vẫn **tính toàn bộ response** rồi mới so ETag — tiết kiệm băng thông nhưng không tiết kiệm CPU/DB. Muốn tiết kiệm cả hai cần ETag từ **phiên bản** của bản ghi (cột `version`/`updated_at`, Chương 15).
Không dùng `String.hashCode` làm ETag: chỉ 32 bit, dễ trùng.

**Bài 4 — live/ready.** `live` luôn 200 nếu tiến trình còn phản hồi. `ready` đọc một `AtomicBoolean ready`; shutdown hook đặt `ready = false`, **chờ vài giây** (để load balancer kịp ngừng gửi traffic), rồi mới `stop`.
Không tách hai loại: Kubernetes khởi động lại container (do liveness fail) khi service chỉ đang tạm quá tải, hoặc gửi traffic vào instance đang tắt (do readiness không phản ánh shutdown).

**Bài 5 — `If-Match` → 412.** `PUT` đọc `If-Match`; thiếu → 428 Precondition Required (tùy chính sách); khác ETag hiện tại → 412. Kiểm tra và ghi phải **nguyên tử**: thực hiện so sánh **bên trong** `modify` của store
(so phiên bản trong trạng thái hiện tại), không phải đọc-so-rồi-ghi ở hai bước — nếu không, race condition quay lại. Test: hai client đọc cùng ETag, cùng PUT; một 200, một 412.

## Chương 16A

**Bài 1 — `BankAccount`.**

```scala
@tailrec final def withdraw(amount: Long): Boolean =
  val b = balance.get()
  if b < amount then false
  else if balance.compareAndSet(b, b - amount) then true
  else withdraw(amount)
```

Test: số dư 1.000, 200 lần rút 7 đồng trên 8 thread → **đúng 142 lần thành công**, còn 6 đồng (142 × 7 = 994). Kiểm tra số dư và trừ tiền nằm trong **một** CAS — nếu tách thành `if get() >= amount then set(get() - amount)`, hai thread cùng thấy đủ tiền và số dư âm.

**Bài 2 — `firstSuccessful`.** Đệ quy qua `recoverWith`; nguồn sau chỉ được gọi khi nguồn trước thất bại (test đếm số lần gọi = 1). Nguồn cuối không bọc `recoverWith` để lỗi của nó lan ra.

**Bài 3 — `traverseLimited`.** Mô hình "N worker cùng lấy việc từ hàng đợi":

```scala
val next = AtomicInteger(0)
def worker(acc: List[(Int, B)]): Future[List[(Int, B)]] =
  val i = next.getAndIncrement()
  if i >= items.size then Future.successful(acc)
  else f(items(i)).flatMap(b => worker((i, b) :: acc))
Future.sequence(List.fill(math.min(maxConcurrent, items.size))(worker(Nil))).map(_.flatten.sortBy(_._1).map(_._2))
```

Test đo được số tác vụ đồng thời tối đa **đúng bằng 3** với 20 phần tử. Vì sao không chia lô (`grouped(3)`)? Chia lô phải chờ phần tử **chậm nhất** của mỗi lô mới bắt đầu lô sau; worker lấy việc mới ngay khi rảnh.
Trạng thái chia sẻ duy nhất là bộ đếm nguyên tử; kết quả mang theo chỉ số rồi sắp xếp lại — không cần mảng mutable dùng chung.

**Bài 4 — circuit breaker.** Máy trạng thái thuần `Closed(n) → Open(until) → HalfOpen → Closed | Open`; mọi quyết định nhận `now` làm tham số → test toàn bộ chuyển trạng thái **không cần chờ** (biên `now = 999` vs `1000`).
Khi dùng thật, trạng thái được giữ trong `AtomicReference` và cập nhật bằng CAS như `ProductStore`. Ở `HalfOpen` chỉ cho **một** lời gọi thử; các lời gọi khác bị từ chối cho đến khi biết kết quả.

**Bài 5.** (a) `Future(1).flatMap(_ => Future(42))` chạy được trên pool **1 thread** (test) — thread không bị giữ trong lúc chờ, nên Future trong được chạy.
(b) Giữ `Await` nhưng chạy Future trong trên pool khác: hết treo trong thí nghiệm, nhưng chỉ **dời vấn đề** — khi pool thứ hai cũng cạn (tải cao), treo quay lại; và mỗi `Await` vẫn lãng phí một thread.

## Ghi chú kiểm chứng (phát hiện khi biên dịch lời giải)

`NoSuchElementException` **không có sẵn** trong phạm vi mặc định của Scala 3.9.0:

```
-- [E006] Not Found Error
2 |  val e = NoSuchElementException("x")
  |          Not found: NoSuchElementException
```

Cần `import java.util.NoSuchElementException`. Nhiều ví dụ cũ (viết cho Scala 2) dùng tên này không cần import — một khác biệt nữa cần lưu ý khi đọc tài liệu cũ.
