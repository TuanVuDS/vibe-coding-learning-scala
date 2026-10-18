# Lời giải — Chương 31: Data Pipeline Architecture

> Code: [`ch31-code/`](ch31-code/) — Scala 3.9.0, dùng lại `11-data-platform/architecture-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch31-code && scala-cli.bat test . --server=false` → **10/10 test pass** (17/10/2026, Windows 11 + JDK 21, 0 cảnh báo). Test đọc dữ liệu mẫu của lab qua đường
> dẫn tương đối nên phải chạy từ thư mục `ch31-code`; mỗi test làm việc trên bản sao tạm.

## Bài 1 — Asset many-to-one

```scala
final class SegmentTotals(from: LocalDate, to: LocalDate) extends Asset:
  val upstream = List(DailyRevenue.name)
  def sources(p: Platform, day: LocalDate): List[Source] =
    days(from, to).toList.map(d => Source("asset", s"${DailyRevenue.name}@$d", p.partitionFile(DailyRevenue.layer, DailyRevenue.name, d)))
  def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int) = ... // cộng theo segment trên snapshot
```

- Kết quả trên 3 ngày mẫu: Retail **2.590.000** (700.000 + 1.890.000), UNKNOWN 450.000, Wholesale 20.000.000 — khớp cộng tay từ bảng mục 6 của bài.
- Lineage của nó là **mọi** partition daily trong khoảng. Thêm C9 vào `customers.csv` → daily ngày 15 đổi → `segment_totals` báo
  `stale: input đổi daily_revenue@2026-10-15` (test).
- Ghi chú thiết kế: asset không phân vùng được lưu như một "partition" ở ngày `to`. Hệ thống thật (Dagster) có khái niệm partition mapping riêng cho quan
  hệ nhiều-một; ý tưởng giống nhau — phải khai báo rõ partition nào của upstream được dùng.

## Bài 2 — Provenance theo phiên bản

- Đi ngược lineage: input `kind = "raw"` là lá; input `kind = "asset"` thì đi tới **đúng lần tính upstream** có ID ghi trong `materializationId` — không tới bản
  mới nhất của `staging_orders@D`. Lineage cũ không rõ phiên bản (`materializationId = None`) được in
  thẳng `? staging_orders@2026-10-15 (không rõ phiên bản upstream)`. Materialize mới từ chối downstream nếu upstream chưa có ID khớp, trước khi publish.
  Status đánh dấu downstream có ID thiếu/sai là stale để backfill sửa sau khi phục hồi upstream. Impact fail-closed nếu đồ thị hiện hành chưa đầy đủ.
- `daily_revenue@2026-10-16` ← `customers.csv`, `raw/.../ingest_date=2026-10-16/orders.csv`, `raw/.../ingest_date=2026-10-17/orders.csv`. File lô 15 **không** có mặt
  — lô 15 không chứa dòng nào của ngày 16. Khi nghi ngờ số ngày 16, chỉ cần đọc hai file.
- Vì sao phải theo phiên bản (test thứ hai): một lô muộn chạm ngày 15 đến, **chỉ** staging tính lại. Provenance của `staging_orders@15` có lô muộn; provenance
  của `daily_revenue@15` **không đổi** — serving chưa từng đọc lô đó. Nối theo bản mới nhất sẽ "nhận vơ" lô muộn cho con số đang phục vụ.

## Bài 3 — SLA

- Ba loại vi phạm: `chưa tính` (Missing), `cũ: <lý do>` (Stale), `tính lúc <at>, quá N giờ` (lần tính gần nhất quá cũ dù input không đổi — ví dụ job ngừng
  chạy nhưng không ai thấy vì không có dữ liệu mới).
- Dùng **freshness tận nguồn** (`Orchestrator.freshness`), không phải trạng thái cục bộ: lô 17 đến mà chưa backfill thì `daily_revenue@16` vẫn khớp input trực
  tiếp nhưng là vi phạm SLA — `cũ: upstream staging_orders@2026-10-16 stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv` (test).
- `now` được truyền vào để test xác định được: dịch `now` thêm 30 giờ thì cả hai partition ngày 15 vi phạm SLA 24 giờ.
- Freshness của **serving** là chỉ số người dùng cảm nhận; đặt cảnh báo ở đây, không chỉ ở "job có lỗi không".

## Bài 4 — Cửa sổ tính lại

- Cửa sổ 1 ngày tại 17/10: chỉ tính ngày 17. Lô 17 cũng chạm ngày 16 (hoàn tiền O6) — ngày 16 nằm ngoài cửa sổ nên **không** được tính, nhưng được trả về trong
  `lateOutsideWindow`: `2026-10-16 staging_orders: input mới raw/orders/ingest_date=2026-10-17/orders.csv`.
- Đây là điểm mấu chốt: cửa sổ là quyết định chi phí, nhưng dữ liệu muộn hơn cửa sổ phải được **nhìn thấy** (bảng `late_arrivals`, cảnh báo, backfill có chủ
  đích) — không được biến mất khỏi báo cáo một cách im lặng.

## Bài 5 — Hợp đồng ở cửa ngõ

- File có header `customer_id,order_id,day` (đổi thứ tự, thiếu cột) bị chuyển sang `raw_quarantine/orders/ingest_date=2026-10-18/orders.csv`.
- Raw vẫn append-only: ta không **sửa** file, chỉ không **nhận** nó vào raw. Sau khi cách ly, backfill 15..17 toàn `skip` — không partition nào bị làm cũ bởi file hỏng.
- Ở hệ thống thật, kiểm tra này là contract của Chương 33 (`Contract.validate`), chạy trong bước ingestion trước khi file được ghi vào raw.

## Bài 6 — Replay từ raw và đối chiếu

- Copy raw + `customers.csv` **hiện tại** sang root mới, `backfill(force = true)`, so hash từng partition với serving. Ghi đè tay `daily_revenue` ngày 15 →
  `2026-10-15 daily_revenue: serving khác bản tính lại từ raw`.
- Đây là phép kiểm tra nhất quán với **một snapshot input + code hiện tại**, không phải chứng minh tính tất định: một lần bằng nhau chỉ là bằng chứng cho lần
  so đó (pipeline có yếu tố ngẫu nhiên vẫn có thể trùng một lần; serving bị sửa rồi khôi phục cũng trùng). Ngược lại, lệch không tự nói "sửa tay": raw/dimension
  hoặc code đã đổi hợp lệ mà chưa backfill cũng gây lệch, và hàm copy dimension **hiện tại** — không nhất thiết là bản serving đã dùng. Khi lệch: xem `status`
  /lineage trước, rồi mới kết luận.
- Đây là ý tưởng Kappa hướng tới (dùng lại cùng logic khi phát lại), chưa phải Kappa: lab không có stream processor, state, offset hay bước chuyển phục vụ
  sang kết quả replay.

## Bài 7 — Dry-run

- Duyệt theo thứ tự topo, mang theo tập `willRun`, chia ba loại:
  - `chắc chạy — <lý do>`: missing, hoặc stale khi không có upstream còn được tính trước nó;
  - `có thể chạy — nếu output của staging_orders khác hash partition này đã dùng`: upstream sẽ (hoặc có thể) chạy, kể cả downstream **đang stale**.
    Nếu chỉ lệch input upstream, runtime chạy khi hash sau tính lại khác hash downstream đã tiêu thụ. Một lý do stale độc lập khác vẫn có thể buộc chạy;
    nhãn này là phân loại bảo thủ khi chưa biết output upstream;
  - còn lại skip.
- Test thứ hai cho thấy "có thể" là thật: lô muộn chỉ có một dòng 9 cột (bị loại) → staging tính lại ra **đúng các byte cũ**, `backfill` thật skip
  `daily_revenue`. Vì vậy ước lượng chi phí từ kế hoạch (số partition × thời gian trung bình) là **cận trên**.
- Regression R2-MAJOR-04: thêm O99 PAID, chỉ staging tính lại → daily stale; thêm REFUNDED cùng khóa/timestamp gây xung đột, staging bỏ O99 và trở về
  hash daily đã dùng. Plan phải ghi daily `có thể chạy`; backfill thật chạy staging rồi skip daily, dù daily stale khi lập kế hoạch. So với hash hiện tại
  của staging sẽ dự đoán sai trường hợp này.
- Giới hạn: `willRun` chỉ lưu tên asset trong **cùng ngày** — đủ cho DAG một-một theo ngày của lab. Asset many-to-one hay khác ngày (bài 1) cần lan theo partition
  cụ thể ghi trong `InputRef`.
- `Lineage.all(p).size` vẫn là 4 sau khi lập kế hoạch — dry-run không ghi gì.
