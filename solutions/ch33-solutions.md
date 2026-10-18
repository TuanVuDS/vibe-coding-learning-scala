# Lời giải — Chương 33: Data Quality và Testing

> Code: [`ch33-code/`](ch33-code/) — Scala 3.9.0, dùng lại `11-data-platform/quality-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch33-code && scala-cli test .` → **7/7 test pass** (17/10/2026, Windows 11 + JDK 21, 0 cảnh báo). Test đọc dữ liệu mẫu của lab qua
> đường dẫn tương đối `../../11-data-platform/quality-lab`, nên phải chạy từ thư mục `ch33-code`.

## Bài 1 — Check theo tỷ lệ

```scala
def maxShare[A](name: String, sev: Severity, maxRatio: BigDecimal)(bad: A => Boolean): Check[A] =
  Check(name, sev, xs => {
    val n     = xs.count(bad)
    val ratio = if xs.isEmpty then BigDecimal(0) else BigDecimal(n) / xs.size
    ...
```

- Dữ liệu mẫu: 1/6 fact là UNKNOWN = **16,67%**. Ngưỡng 20% → đạt; ngưỡng 10% → đỏ với mẫu `1/6 = 16.67% > 10.00%` (output thật của test).
- Vì sao tỷ lệ chứ không phải "không có UNKNOWN nào": khách mới chưa đồng bộ là bình thường; **tăng vọt** mới là sự cố (dimension không cập nhật, khóa đổi định dạng).
- `failed` là 1 (một vi phạm cấp tập), không phải số dòng UNKNOWN — người đọc báo cáo không nhầm "1 dòng lỗi" với "1 lần vượt ngưỡng".
- Tập rỗng → tỷ lệ 0, không chia cho 0; còn "tập rỗng có phải lỗi không" là việc của `rowCount`.

## Bài 2 — Accepted values

```scala
def acceptedValues[A](name: String, sev: Severity, allowed: Set[String])(field: A => String): Check[A] =
  Check.row[A](name, sev)(a => !allowed(field(a)))(field)
```

- Đổi `Retail` → `retail` ở dimension: check bắt **3** fact (O1/KB-01 REFUNDED, O1/MS-01, O8/KB-01). Lần đầu viết test, kỳ vọng là 2 — nhìn bảng daily (chỉ PAID)
  thay vì bảng fact (mọi trạng thái). Đây là Lỗi 4 trong bài: khi test đỏ, kiểm tra **đặc tả** trước.
- Không có check này, `retail` tạo một segment mới trong daily, tổng doanh thu không đổi — drift theo tổng không thấy; drift theo tỷ trọng segment mới thấy.

## Bài 3 — Sinh dữ liệu từ contract

```scala
def sampleRecord(c: Contract, i: Int): Map[String, String] =
  c.fields.map { f => f.name -> (f.`type` match
    case "int"  => (i % 1000).toString
    case "enum" => f.values.fold("")(vs => vs(i % vs.size))
    ...
```

- Contract là **nguồn sự thật** cho cả validator lẫn dữ liệu test: thêm trường vào contract thì bộ sinh tự sinh trường đó — test không bị lỗi thời.
- 50 bản ghi × 2 phiên bản đều qua `Contract.validate` của chính phiên bản đó.
- Bản ghi v2 vi phạm v1 **theo producer conformance** (`validate`, nghiêm) vì trường `channel` thừa: `channel: trường không có trong contract orders v1`. Đây là
  tình huống thật khi **producer nâng cấp trước consumer** — kiểm tra trường thừa ở biên phát hiện ngay. Nhưng consumer v1 **đọc** (`Contract.read`, bỏ qua trường
  lạ) bản ghi đó được — trừ khi `quantity` vượt `Int.MaxValue`, đúng lý do `int → long` là Breaking theo hướng FORWARD (bài, mục 3.4). Phân biệt hai câu hỏi:
  "producer có giữ lời?" và "consumer có đọc được?".
- Kiểu lạ → `sys.error`: bộ sinh phải hỏng to khi contract có kiểu mới, không âm thầm sinh chuỗi rỗng.

## Bài 4 — Đối soát trong production

- Dữ liệu mẫu: 13 = 3 + 1 + 1 + 2 + 6 ✔. Bỏ một fact → đỏ với mẫu `input=13, ngăn=3+1+1+2+5`.
- Property "bảo toàn" chứng minh **code** giữ tính chất này; check chạy trong production phát hiện khi **môi trường** phá nó (một bước bị bỏ qua do cấu hình,
  output ghi dở). Cùng một quy tắc, hai mục đích (mục 3.1).

## Bài 5 — Drift trên tỷ lệ bị loại

- Dữ liệu mẫu: 3/13 = **23,08%**. Thêm 10 dòng có `day = 17/10/2026` (định dạng `dd/MM/yyyy`) → 13/23 = **56,52%**; ngưỡng +5% → cảnh báo
  `tỷ lệ bị loại tăng 23.0800% → 56.5200% (ngưỡng +5.00%)`.
- Tỷ lệ bị loại thường là **tín hiệu sớm nhất** khi nguồn đổi định dạng: doanh thu chỉ giảm nhẹ (dòng hỏng không vào), nhưng rejected tăng vọt.
- So với **tỷ lệ cơ sở** chứ không với 0: nguồn này vốn có 23% dòng hỏng — đặt ngưỡng tuyệt đối 5% sẽ đỏ mỗi ngày.

## Bài 6 — Incremental idempotent theo lô

```scala
forAll(Gen.listOf(genOrder), Gen.listOf(genOrder)) { (history, batch) =>
  val s = Incremental.step(Map.empty, history)
  Incremental.step(Incremental.step(s, batch), batch) == Incremental.step(s, batch)
}
```

- Replay (Chương 30) đưa lại cùng lô; tính chất này bảo đảm replay **không đổi trạng thái**.
- Vì sao đúng: mỗi bản ghi của lô lần hai hoặc cũ hơn (bỏ qua), hoặc cùng `updated_at` và cùng nội dung với bản đã giữ (trùng tuyệt đối: giữ bản lô sớm nhất —
  lô lần hai không sớm hơn), hoặc cùng `updated_at` khác nội dung đã có trong tập (Set không đổi).
- Bộ sinh hẹp (4 mã đơn, 2 mốc thời gian, 2 lô) để cùng `updated_at` xảy ra thường xuyên.

## Bài 7 — Backfill theo ngày

```scala
def backfill(serving: Vector[DailyRow], recomputed: Vector[DailyRow], days: Set[LocalDate]): Vector[DailyRow] =
  (serving.filterNot(r => days(r.day)) ++ recomputed.filter(r => days(r.day))).sortBy(r => (r.day, r.segment))
```

- Bảng đang phục vụ được tính khi chỉ có lô 15/10: Retail 15/10 = 1.150.000 (O1/KB-01 còn PAID, chưa có O8). Lô 16/10 mang hoàn tiền O1 và O8 đến trễ
  **cho ngày 15** — job hằng ngày chỉ ghi partition 16/10 nên ngày 15 sai.
- Backfill ngày 15 từ **toàn bộ input** rồi thay đúng partition đó (overwrite động, Chương 24) → bằng tính lại toàn bộ; ngày 16 giữ nguyên.
- Bài học thiết kế: dữ liệu trễ sửa **ngày cũ**. Pipeline phải biết lô mới chạm những ngày nào (`facts.map(_.order.day).distinct`) và tính lại **các ngày đó**,
  không chỉ "ngày hôm nay".
