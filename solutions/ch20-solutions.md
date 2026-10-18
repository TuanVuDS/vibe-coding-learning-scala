# Lời giải — Chương 20: Functional Data Transformation

> Code: [`ch20-code/`](ch20-code/) — dùng lại mã nguồn `08-data-processing/transform-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch20-code && scala-cli test .` → **9/9 test pass** (1 property ScalaCheck), 17/10/2026.
> Đã sửa sau review chéo (Codex): đếm theo đơn (bài 1), xung đột khi hòa phiên bản (bài 3), tổng `BigInt` (bài 4), phạm vi gate (bài 6).

## Bài 1 — Quy tắc trên tổng hợp: tỷ lệ hoàn tiền theo ngày

```scala
QualityReport.requireRate(maxRate)                      // NaN / ngoài [0, 1] → lỗi cấu hình ngay
val byDay = lines.groupBy(_.day).view.mapValues { ls =>
  val orders = ls.groupBy(_.orderId).values               // GRAIN: đếm ĐƠN, không đếm dòng hàng
  orders.count(_.exists(_.status == Status.Refunded)).toDouble / orders.size
}.toMap
val bad   = byDay.filter(_._2 > maxRate)
RuleResult(name, Severity.Error, checked = byDay.size, failed = bad.size, examples = ...)
```

- `checked` là **số ngày**, không phải số dòng: đơn vị kiểm tra của quy tắc này là ngày.
- **Grain:** `Order` của lab là một **dòng hàng** `(order_id, sku)`. Bản đầu chia theo số dòng — review chéo chỉ ra: O1 có 10 sku Paid, O2 có 1 sku Refunded →
  theo dòng 1/11 = 9% (qua ngưỡng 20%), theo đơn 1/2 = **50%** (vi phạm). Câu hỏi nghiệp vụ là "bao nhiêu **đơn** bị hoàn", nên nhóm theo `orderId` trước.
  Quy ước: đơn có **ít nhất một** dòng Refunded là đơn hoàn tiền — ghi rõ vì dữ liệu thật có thể hoàn một phần.
- Ngưỡng `NaN` làm `rate > maxRate` luôn `false` → quy tắc không bao giờ vi phạm. Test kiểm tra `NaN` và `2.0` bị từ chối.
- Một đơn hoàn tiền không có gì sai; **ngày** có 33% hoàn tiền mới bất thường (lỗi thanh toán, sản phẩm lỗi hàng loạt).
- Đặt ngưỡng cần dữ liệu lịch sử: tỷ lệ "bình thường" của shop là bao nhiêu? Quy tắc cứng 20% chỉ là điểm bắt đầu.

## Bài 2 — `Monoid[Mean]`

```scala
final case class Mean(sum: BigDecimal, count: Long):
  def value: Option[BigDecimal] = Option.when(count > 0)(sum / count)

given Monoid[Mean] with
  def empty: Mean                     = Mean(0, 0)
  def combine(a: Mean, b: Mean): Mean = Mean(a.sum + b.sum, a.count + b.count)
```

- Gộp **tổng và số lượng**, chia ở **bước cuối** (`value`). Phép cộng kết hợp và giao hoán nên chia dữ liệu thế nào cũng được.
- `value` trả `Option`: trung bình của tập rỗng không tồn tại — không trả `0` (một con số sai trông hợp lệ).
- Property: với mọi danh sách và mọi điểm cắt, `combineAll` cả danh sách == `combineAll` của hai phần; và luật trung hòa.

## Bài 3 — Dedup tổng quát bằng type class

```scala
trait Versioned[A]:
  type Key
  def key(a: A): Key
  def version(a: A): Long

final case class Result[A, K](latest: List[A], conflicts: List[K])

def dedupLatest[A](xs: List[A])(using v: Versioned[A]): Result[A, v.Key] =
  val decided = xs.groupBy(v.key).toList.map { (k, versions) =>
    val top  = versions.map(v.version).max
    val best = versions.filter(a => v.version(a) == top).distinct
    if best.size == 1 then Right(best.head) else Left(k)       // hòa mà khác nội dung → xung đột
  }
  Result(decided.collect { case Right(a) => a }, decided.collect { case Left(k) => k })
```

- `type Key` là **kiểu thành viên** (type member): mỗi kiểu bản ghi tự chọn kiểu khóa (`String`, `(String, LocalDate)`...), người dùng `dedupLatest` không cần biết.
- Giữ bản có `version` lớn nhất → **không phụ thuộc thứ tự đến** (test chạy cả `updates` và `updates.reverse`). Khác Chương 19 (giữ bản *đến trước*): với dữ liệu
  cập nhật (CDC), bản mới nhất mới là sự thật.
- Hai bản cùng `version`: bản đầu dùng `maxBy`, chọn bản **gặp trước** → lại phụ thuộc thứ tự (review chéo). Giờ: giống hệt nhau → giữ một bản (`distinct`);
  khác nhau → **xung đột**, trả riêng trong `conflicts`, không đoán. Test chạy **mọi hoán vị** của 4 bản ghi. Bản thật có thể phá hòa bằng `sequence`/offset của nguồn —
  nhưng đó là quyết định nghiệp vụ phải ghi rõ, không phải hành vi ngẫu nhiên của `maxBy`.
- `Result[A, v.Key]` là **kiểu phụ thuộc đường dẫn** (path-dependent): kiểu khóa xung đột lấy từ chính instance `v`.

## Bài 4 — Top-K khi dữ liệu chia partition

```scala
def wrong(...) = partitions.flatMap(p => topK(p)).merge.topK     // cắt TRƯỚC khi gộp
def right(...) = partitions.map(sumByKey).combineAll.topK        // gộp theo khóa TRƯỚC, cắt SAU
```

Phản ví dụ trong test, K = 1:

| Partition | Doanh thu | Top-1 của partition |
|---|---|---|
| p1 | A 100, C 40 | A 100 |
| p2 | B 90, C 40 | B 90 |
| p3 | A 10, C 40 | C 40 |

- **Đúng:** tổng theo khóa A 110, B 90, **C 120** → top-1 là **C**.
- **Sai:** gộp các top-1 (A 100, B 90, C 40) → **A 100**. C bị cắt ở p1 và p2 (không đứng đầu ở **mọi** partition chứa nó) dù tổng cao nhất; A cũng mất 10 ở p3.
- Tổng là `BigInt`: với `Long`, hai partition mỗi bên ~9×10¹⁸ cộng lại tràn thành số **âm** và rơi xuống cuối bảng xếp hạng (review chéo; có test hồi quy).
- Top-K **không** phải Monoid theo nghĩa cần thiết: "top-K của các top-K" ≠ "top-K của tổng" khi một khóa trải trên nhiều partition. Cái gộp được là `Map[sku, tổng]`
  (Cats có `Monoid[Map[K, V]]` khi `V` có `Semigroup`, cộng theo khóa — `combineAll`). Spark làm đúng việc này: `reduceByKey`/`groupBy().sum()` trước, `orderBy().limit()` sau.

## Bài 5 — Số user khác nhau mỗi ngày

```scala
partitions.map(p => Aggregate.by(p)(_.day)(o => Set(o.customerId)))   // Monoid[Set]: hợp tập
  .foldLeft(Map.empty[LocalDate, Set[String]])(Aggregate.merge)
  .view.mapValues(_.size)
```

- `Set` với phép hợp là Monoid (`empty = Set()`), kết hợp + giao hoán → tính theo partition được. C1 xuất hiện ở hai partition vẫn chỉ đếm một lần.
- **Không** gộp được bằng cách cộng `size` từng partition (C1 bị đếm hai lần) — giống lỗi trung bình của trung bình.
- Bộ nhớ ~ **số user khác nhau**: 10 triệu user × vài chục byte = hàng trăm MB mỗi ngày. Khi quá lớn, dùng **HyperLogLog** (ước lượng, sai số ~1–2%, bộ nhớ vài KB,
  vẫn là Monoid). Spark có `approx_count_distinct` dựa trên ý tưởng này.

## Bài 6 — Pipeline có quality gate

```scala
enum Outcome:
  case Blocked(report: QualityReport, rejected: Int)
  case Published(revenue: Map[(Segment, LocalDate), Stats], report: QualityReport, rejected: Int)
```

- Kết quả là **ADT**: người gọi buộc phải xử lý cả hai trường hợp (compiler cảnh báo match thiếu). Không trả `null` hay ném exception khi gate chặn — "dữ liệu không đạt
  chuẩn" là kết quả **dự kiến** của pipeline, không phải lỗi chương trình.
- `Blocked` vẫn mang `QualityReport` để người vận hành biết vì sao; `Published` mang cả báo cáo chất lượng để lưu cùng số liệu (ngày này dữ liệu có 12% cảnh báo...).
- Test: một khách hàng không tồn tại trong 3 dòng hợp lệ = 33% → chặn ở ngưỡng 10%, xuất bản ở ngưỡng 50%; dòng ngày sai bị loại ở validation (đếm `rejected = 1`).
- **Phạm vi gate:** quy tắc chỉ chạy trên dòng **đã qua** validation. Nếu **mọi** dòng bị loại, `orders` rỗng → mọi `failRate = 0` → gate "qua" với báo cáo rỗng.
  Bản thật nên thêm điều kiện trên `rejected / raw.size` (hoặc "ít nhất N dòng hợp lệ"). Ngưỡng được kiểm tra bởi `QualityReport.blocking` (`NaN` → lỗi).
- Ở production, gate thường là một **bước riêng** của orchestration (Chương 31): job ghi dữ liệu vào vùng staging, gate kiểm tra, chỉ khi qua mới "công bố" sang vùng serving.
