# Chương 31 — Data Pipeline Architecture

> **Phiên bản:** Scala **3.9.0** · JDK 21 · circe-parser/circe-generic 0.14.17 · munit 1.2.4 · sbt 1.12.11 · **Dagster 1.13.26** (Python 3.12.10 — công cụ riêng,
> chỉ dùng làm ví dụ orchestration)
> **Code:** [`architecture-lab/`](architecture-lab/) — nền tảng thu nhỏ: lớp raw → staging → serving, asset phân vùng theo ngày, lineage có hash, phát hiện
> partition cũ, backfill idempotent, impact analysis, speed layer; [`architecture-lab/dagster/`](architecture-lab/dagster/) — Dagster gọi job Scala
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` và PyPI ngày 17/10/2026. **13/13 test pass** (`scala-cli test .` và `sbt test`, 0 cảnh báo), lời
> giải 9/9 (sửa theo review chéo của Codex, 17/10/2026). Dagster 1.13.26 cài thật trong venv, điều phối job Scala cho 3 partition — exit 0. Mọi output và thông báo lỗi trong bài là output thật
> (Windows 11 + JDK 21). Kết quả doanh thu khớp **tính tay** (mục 6).

---

## Vị trí trong lộ trình

Chương 24 xây **một** pipeline batch; Chương 26 biến nó thành **một** job chạy được trong production; Chương 29–30 làm điều tương tự cho streaming; Chương 33
dạy kiểm tra dữ liệu. Chương này lùi lại một bước và hỏi: khi có **nhiều** nguồn, **nhiều** bảng, **nhiều** người đọc và dữ liệu đến muộn, ta tổ chức chúng
thế nào để luôn trả lời được *"con số này từ đâu ra, còn đúng không, và nếu sai thì tính lại cái gì?"*. Chương 32 nhìn vào bên trong một job phân tán; Chương 34
nói về chi phí; **Project 5** ghép tất cả thành một nền tảng.

## 1. Mục tiêu học tập

1. Mô tả các **lớp** của một nền tảng dữ liệu — ingestion, raw, staging, transformation, serving — và quy tắc ghi của từng lớp.
2. Phân biệt **orchestration** (chạy gì, khi nào, theo thứ tự nào) với **processing** (tính một phần dữ liệu như thế nào); đặt Dagster/Airflow và Scala/Spark/SQL
   đúng chỗ.
3. Thiết kế **asset phân vùng** có hàm tính tất định, ghi nguyên tử, chạy lại được (idempotent).
4. Ghi **lineage** đủ để trả lời: partition này dùng input nào, code phiên bản nào — và dùng nó để biết partition nào **cũ** (stale).
5. Thực hiện **backfill** và **reprocessing** (đổi code) mà chỉ tính lại đúng phần cần tính; làm **impact analysis** khi một file nguồn hỏng.
6. So sánh tích hợp batch + streaming kiểu **Lambda** và **Kappa**, và biết vì sao cần **đối soát** giữa hai đường.
7. Đặt data contract, schema evolution và data quality vào đúng **ranh giới** của kiến trúc; biết đo **observability** của nền tảng.

## 2. Kiến thức tiên quyết

Chương 24 (pipeline 10 bước, snapshot, write–audit–publish), Chương 26 (job production, mã thoát, manifest), Chương 21 (định dạng, schema evolution),
Chương 27–30 (event time, replay, idempotency), Chương 33 (data contract FORWARD, golden, drift). Lab dùng Scala thuần (không Spark) để mọi khái niệm chạy
trong mili giây; phần Dagster cần Python 3.10–3.14 (Dagster 1.13.26 yêu cầu `>=3.10,<3.15`, tra PyPI).

## 3. Lý thuyết

### 3.1. Các lớp của một nền tảng dữ liệu

**Vấn đề.** Pipeline đầu tiên của một team thường là một script đọc nguồn, biến đổi và ghi thẳng ra bảng báo cáo. Ba tháng sau: nguồn gửi lại dữ liệu sửa sai
cho tuần trước, một bug trong phép tính bị phát hiện, và một team khác muốn dùng dữ liệu đã làm sạch. Script không trả lời được câu nào, vì nó không giữ lại
**dữ liệu gốc**, không tách **dữ liệu đã làm sạch** khỏi **báo cáo**, và không ghi **cái gì được tính từ cái gì**.

**Cách hoạt động.** Chia lưu trữ thành các lớp, mỗi lớp có **quy tắc ghi** riêng:

```
 nguồn ──ingestion──► RAW ──────────► STAGING ───────────► SERVING ──► dashboard, API, team khác
                      append-only     đã parse/validate/     bảng cho người đọc,
                      đúng như nguồn  dedup, theo ngày       ghi đè nguyên tử
                      gửi             NGHIỆP VỤ              từng partition
                         ▲
                         └── tính lại MỌI THỨ từ đây khi sửa bug, đổi logic, backfill
```

| Lớp | Lab | Quy tắc ghi | Ai đọc |
|---|---|---|---|
| Ingestion | file thả vào `raw/orders/ingest_date=D/` | chỉ thêm file mới; kiểm hợp đồng ở cửa (bài 5) | — |
| Raw | `raw/...` | **append-only**, không sửa, không xóa (trừ chính sách retention) | chỉ pipeline |
| Staging | `staging/staging_orders/day=D/part.csv` | tính lại được bất cứ lúc nào từ raw; validate + dedup + theo **ngày nghiệp vụ** | pipeline, kỹ sư dữ liệu |
| Serving | `serving/daily_revenue/day=D/part.csv` | ghi đè nguyên tử từng partition; có kiểm tra trước khi publish (Ch24, P3) | người dùng cuối |
| Metadata | `_lineage/materializations.jsonl` | chỉ thêm; mỗi lần tính một partition một dòng | orchestrator, người vận hành |

Tên gọi thay đổi theo nơi (bronze/silver/gold, landing/curated/mart) — **quy tắc ghi** mới là thứ quan trọng. Raw là "nguồn sự thật": giữ nó thì mọi lớp sau
đều tính lại được; mất nó thì một bug ở staging là mất dữ liệu vĩnh viễn.

**Thay thế.** Một lớp duy nhất (ghi thẳng ra báo cáo): ít lưu trữ, nhưng không sửa được quá khứ. ELT trong warehouse (raw đổ vào bảng SQL, biến đổi bằng
SQL/dbt): cùng mô hình lớp, khác công cụ. **Ưu/nhược:** nhiều lớp tốn lưu trữ (raw thường chiếm nhiều nhất) và tốn thời gian tính lại; đổi lại sửa được sai
lầm. **Lỗi thiết kế:** sửa file raw "cho đúng"; staging theo **ngày nhận** thay vì ngày nghiệp vụ (hoàn tiền đến hôm sau rơi vào ngày sai — mục 6); để người
dùng cuối đọc thẳng staging.

### 3.2. Orchestration khác processing

**Vấn đề.** "Chạy job doanh thu lúc 2 giờ sáng, nếu lỗi thì thử lại, nếu bên gửi trễ thì đợi, khi sửa bug thì chạy lại 30 ngày" — những việc này **không**
phải là tính doanh thu. Nhét chúng vào code tính toán làm code đó khó test, khó chạy lại bằng tay, và gắn chặt với một lịch cụ thể.

**Cách hoạt động.** Tách hai vai trò:

| | Orchestration | Processing |
|---|---|---|
| Trả lời | chạy **gì**, **khi nào**, thứ tự nào, thử lại ra sao, đã chạy chưa | tính **một** partition như thế nào |
| Ví dụ công cụ | Dagster, Airflow, cron, Kubernetes CronJob | Scala, Spark, SQL/dbt, Flink |
| Trong lab | `Orchestrator.backfill` (Scala, cho học) và `dagster/defs.py` (Dagster thật) | `Asset.compute` + CLI `materialize` |
| Hợp đồng giữa hai bên | CLI có tham số rõ (`--asset --day`), **mã thoát** có nghĩa, kết quả in ra là dòng lineage JSON | |

Job Scala **không biết** nó được gọi bởi Dagster, bởi cron hay bởi người vận hành gõ lệnh; Dagster **không biết** doanh thu tính thế nào. Đổi Dagster sang
Airflow không sửa một dòng Scala; sửa logic Scala không sửa lịch.

**Dagster** (Python, mã nguồn mở) là một orchestrator lấy **asset** làm trung tâm: ta khai báo bảng nào phụ thuộc bảng nào, phân vùng thế nào; Dagster tự
suy ra thứ tự, lưu lịch sử chạy, cho giao diện xem/backfill. Nó là **công cụ riêng**, không phải một phần của Scala — giáo trình dùng nó làm ví dụ vì mô hình
asset khớp với lab. Đoạn định nghĩa thật ([`defs.py`](architecture-lab/dagster/defs.py)):

```python
daily = dg.DailyPartitionsDefinition(start_date="2026-10-15", end_date="2026-10-18", end_offset=1, timezone="Asia/Ho_Chi_Minh")

def run_scala(context: dg.AssetExecutionContext, asset: str) -> dg.MaterializeResult:
    cmd = [JAVA, "-Dstdout.encoding=UTF-8", "-jar", JAR, "materialize", f"--root={ROOT}", f"--asset={asset}",
           f"--day={context.partition_key}", f"--run-id=dagster-{context.run_id[:8]}"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        raise dg.Failure(description=f"job Scala trả mã {result.returncode}: {(result.stderr or result.stdout).strip()}")
    lineage = json.loads(result.stdout.strip().splitlines()[-1])  # dòng lineage do job Scala in ra
    return dg.MaterializeResult(metadata={"rows": lineage["rows"], "rejected": lineage["rejected"], ...})

@dg.asset(partitions_def=daily, deps=[staging_orders], code_version="v1", group_name="orders")
def daily_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_scala(context, "daily_revenue")
```

**Thay thế:** Airflow (DAG của task, lâu đời, hệ sinh thái lớn), cron + script (đơn giản, không có lịch sử/backfill), orchestrator gắn với nền tảng cloud.
**Khi nào cần orchestrator thật:** nhiều job phụ thuộc nhau, cần backfill, cần lịch sử và cảnh báo. Một job mỗi ngày không phụ thuộc gì — cron là đủ.
**Lỗi thiết kế:** viết logic biến đổi bên trong DAG Python (orchestrator thành nơi xử lý dữ liệu); job chỉ chạy được khi có orchestrator; mã thoát luôn 0 nên
orchestrator không biết job lỗi (mục 10, lỗi 2).

### 3.3. Asset phân vùng và tính idempotent

**Vấn đề.** "Chạy lại" phải an toàn: sau sự cố, sau khi sửa bug, khi orchestrator thử lại. Nếu chạy hai lần cho ra kết quả khác (thêm dòng trùng, thứ tự
khác, chứa thời điểm chạy), không ai dám bấm chạy lại.

**Cách hoạt động.** Một **asset** là một bảng có định nghĩa; chia theo **partition** (ở đây: một ngày nghiệp vụ). Hàm tính một partition phải:

1. **Tất định:** cùng input + cùng code → cùng output từng byte (sắp xếp, không có `now()` trong dữ liệu).
2. **Ghi nguyên tử:** file tạm rồi `ATOMIC_MOVE` — người đọc thấy bản cũ hoặc bản mới.
3. **Thay thế, không cộng dồn:** ghi đè partition, không append.

```scala
trait Asset:
  def name: String
  def layer: String
  def codeVersion: String                                                      // tăng khi đổi logic → reprocessing
  def upstream: List[String]
  def sources(p: Platform, day: LocalDate): List[Source]                       // các file CÓ THỂ là input
  def relevant(day: LocalDate, files: List[InputFile]): List[InputFile] = files // input THỰC SỰ thuộc partition
  def compute(day: LocalDate, inputs: List[InputFile]): (Vector[String], Int)  // tính CHỈ từ snapshot: output + số dòng bị loại
```

`compute` không nhận `Platform` và không tự đọc đĩa: orchestrator đọc mỗi file trong `sources` **đúng một lần** thành `InputFile` (một **snapshot**),
ghi hash của **chính các byte đó** vào lineage, rồi đưa snapshot cho `compute`. Nhờ vậy lineage luôn mô tả đúng dữ liệu đã tính, kể cả khi một lô raw đến
giữa chừng hay dimension bị đổi rồi đổi lại (mục 10, lỗi 4). Cái giá: mọi file asset phụ thuộc **phải** nằm trong `sources` — file đọc lén ngoài danh sách thì
đổi mà partition vẫn báo fresh.

Mọi file được khai báo trong `sources` phải đọc được, gồm dimension và partition upstream. Mất file hoặc lỗi I/O làm dừng trước publish; không biến lỗi đọc
thành input rỗng/`UNKNOWN`. Không có lô raw nào là tập input rỗng hợp lệ, nhưng một file raw đã được liệt kê rồi biến mất vẫn là lỗi đọc.
Snapshot ở đây bảo đảm hash và compute dùng cùng byte, không phải snapshot đồng thời nhiều file hoặc bảo đảm một lô đã ghi xong. Ingestion phải ghi file tạm
ngoài tập `rawFiles`, đóng và kiểm tra rồi atomic rename vào raw; dimension cũng publish nguyên tử hoặc dùng phiên bản bất biến.

`StagingOrders.compute` đọc **mọi** lô raw và lấy các dòng của ngày nghiệp vụ `day` — vì vậy lô 17/10 chứa hoàn tiền của đơn ngày 16 sửa đúng partition ngày 16.
Với ba điều kiện trên, "chạy hằng ngày", "chạy lại" và "backfill 30 ngày" là **cùng một thao tác**, chỉ khác khoảng ngày.

**Lỗi thiết kế:** partition theo ngày nhận (`ingest_date`) cho bảng nghiệp vụ; hàm tính đọc "lô mới nhất" thay vì mọi lô liên quan; output chứa thời điểm chạy
(không bao giờ so được hai lần chạy).

### 3.4. Lineage và phát hiện partition cũ

**Vấn đề.** Có 3 năm × 365 ngày × 20 bảng partition. Một lô raw mới đến, một file dimension đổi, một bug được sửa. Partition nào **cần** tính lại? Tính lại
tất cả: tốn kém. Đoán ("chạy lại 7 ngày gần nhất"): bỏ sót dữ liệu đến muộn hơn 7 ngày, và tính lại vô ích những ngày không đổi.

**Cách hoạt động.** Mỗi lần tính một partition, ghi một dòng **lineage** — dùng input nào (kèm **hash nội dung**), code phiên bản nào, ra output hash nào:

```json
{"asset":"daily_revenue","day":"2026-10-15","codeVersion":"v1","inputs":[{"kind":"asset","ref":"staging_orders@2026-10-15","hash":"1e944db75a43c867",
 "materializationId":"<id lần tính staging đã sinh ra đúng các byte này>"},{"kind":"raw","ref":"customers.csv",...}],...,"id":"<id của lần tính này>"}
```

Có **hai** câu hỏi khác nhau, và nhầm chúng là lỗi vận hành thật:

- **Khớp cục bộ** (`Orchestrator.status`): code version không đổi, tập input **trực tiếp** hiện tại có cùng hash với lần tính gần nhất, và output trên đĩa đúng
  là output đã ghi. Khác bất kỳ điều nào → **cũ** (stale), kèm **lý do** (đoạn code dưới). Backfill dùng câu hỏi này để quyết định chạy.
- **Fresh tận nguồn** (`Orchestrator.freshness`): khớp cục bộ **và** mọi partition upstream nó đã đọc (đúng partition ghi trong `InputRef`, có chống chu trình)
  cũng fresh tận nguồn. Giám sát, SLA và CLI `status` dùng câu hỏi này. Ví dụ thật ở mục 9: lô 17 vừa đến, `daily_revenue@16` vẫn khớp cục bộ (staging
  ngày 16 chưa tính lại nên chưa đổi) nhưng **không** fresh tận nguồn — dashboard đang hiển thị doanh thu của một đơn đã hoàn tiền.

```scala
val reasons =
  Option.when(m.codeVersion != a.codeVersion)(s"code ${m.codeVersion} → ${a.codeVersion}").toList ++
    (now.keySet -- before.keySet).toList.sorted.map(r => s"input mới $r") ++
    (before.keySet -- now.keySet).toList.sorted.map(r => s"input mất $r") ++
    (now.keySet & before.keySet).toList.sorted.filter(r => now(r) != before(r)).map(r => s"input đổi $r") ++
    Option.when(Files2.hash(p.partitionFile(a.layer, a.name, day)) != m.outputHash)("output trên đĩa khác lần tính gần nhất").toList
```

**Lineage chính xác** là chìa khóa: `staging_orders@D` chỉ khai báo input là các file raw **có dòng của ngày D**. Nhờ vậy lô 17 (chứa ngày 16 và 17) làm cũ đúng
ngày 16 và 17, không đụng ngày 15 (output thật, mục 9). Lineage thô ("mọi file raw") đúng nhưng làm mọi ngày cũ mỗi khi có lô mới.

**Impact analysis** dùng lineage theo chiều xuôi: một file raw bị phát hiện hỏng → những partition nào (trực tiếp và bắc cầu) đã dùng nó → ai đang đọc số sai.
**Provenance** theo chiều ngược (bài 2): partition này được tính từ những file raw nào. Cả hai phải đi theo **phiên bản** đã được dùng (`materializationId`),
không theo "bản mới nhất" của upstream: nếu staging ngày 15 đã tính lại (bỏ một file bị cách ly, thêm một lô muộn) mà serving chưa tính lại, thì serving vẫn chứa
đóng góp của file cũ và **không** chứa lô muộn. Nối theo bản mới nhất sẽ cho kết quả sai cả hai chiều (test `impact theo PHIÊN BẢN`).

**Thay thế:** lineage cấp bảng (bảng A từ bảng B) — đủ vẽ sơ đồ, không đủ quyết định tính lại partition nào. Lineage cấp cột (OpenLineage và các catalog) — sâu
hơn, phức tạp hơn (khái niệm; giáo trình chưa kiểm chứng công cụ cụ thể). **Lỗi thiết kế:** lineage chỉ là tài liệu vẽ tay; dựa vào thời gian sửa file
(mtime) thay vì hash — copy file làm đổi mtime mà không đổi nội dung, và ngược lại.

### 3.5. Backfill và reprocessing

**Backfill** = tính các partition của quá khứ (lần đầu, hoặc sau khi dữ liệu muộn đến). **Reprocessing** = tính lại vì **code** đổi (sửa bug, đổi định nghĩa).
Với asset tất định + lineage, cả hai là cùng một vòng lặp: với mỗi ngày trong khoảng, theo thứ tự **topo** (upstream trước downstream), chỉ tính partition
**thiếu hoặc cũ**:

```scala
def backfill(p: Platform, assets: List[Asset], from: LocalDate, to: LocalDate, runId: String, force: Boolean = false): Vector[Step] =
  Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).toVector.flatMap { day =>
    assets.map { a =>
      status(p, a, day) match
        case Status.Fresh if !force => Step(a.name, day, "skip", "fresh")
        case s =>
          val m = materialize(p, a, day, runId)
          Step(a.name, day, "materialize", s"${describe(s)} → rows=${m.rows} rejected=${m.rejected}")
    }
  }
```

Thứ tự topo quan trọng: trước khi chạy, `daily_revenue@16` còn **khớp cục bộ** (staging ngày 16 chưa đổi); ngay sau khi staging được tính lại, hash của nó
đổi và `daily_revenue@16` thành **cũ** — được tính trong cùng lượt. Vì vậy backfill chỉ cần trạng thái cục bộ: upstream tính lại ra byte khác thì downstream
thấy "input đổi"; ra byte y hệt (ví dụ lô muộn toàn dòng bị loại) thì downstream được skip đúng. Đổi `codeVersion` của `daily_revenue` thành `v2` → mọi partition daily cũ, staging không bị đụng
(test `reprocessing`).

**Cửa sổ tính lại.** Tính lại toàn bộ lịch sử mỗi lần là đúng nhưng đắt. Giới hạn cửa sổ N ngày thì phải **liệt kê** những gì đến muộn hơn cửa sổ, đừng bỏ
qua âm thầm (bài 4). **Lỗi thiết kế:** backfill bằng cách xóa cả bảng rồi chạy lại từ đầu (người đọc thấy bảng trống); chạy song song nhiều ngày khi các ngày
phụ thuộc nhau; tin rằng "chạy lại hôm qua" bắt được mọi dữ liệu muộn.

### 3.6. Tích hợp batch và streaming

**Vấn đề.** Dashboard muốn doanh thu **hôm nay** từng phút; báo cáo tài chính muốn con số **đúng** sau khi mọi hoàn tiền, trùng lặp, xung đột được xử lý.

| Kiến trúc | Cách làm | Ưu | Nhược |
|---|---|---|---|
| **Lambda** | hai đường: speed layer (stream, tạm) + batch layer (tính lại từ raw, đúng); phục vụ gộp hai bên | nhanh + đúng | **hai** cài đặt cùng một logic, dễ lệch nhau |
| **Kappa** | một đường: mọi thứ là stream; tính lại = phát lại (replay) raw/log qua **cùng** code | một logic | cần log/raw giữ đủ lâu; replay lớn tốn thời gian |
| Batch nhỏ thường xuyên | chạy batch mỗi 5–15 phút | đơn giản | độ trễ = chu kỳ |

Lab có một speed layer ngây thơ: cộng PAID ngay khi event đến, không dedup, không xử lý cập nhật. Đối soát với batch (output thật):

```
2026-10-15: speed=42050000 batch=21150000 lệch=20900000
2026-10-16: speed=1740000 batch=0 lệch=1740000
```

Ngày 15: speed layer cộng **hai lần** O2 (20 triệu, bản gửi lại) và vẫn tính O1/KB-01 (900.000) đã hoàn tiền. Ngày 16: O6 đã hoàn tiền ở lô 17, O7 đang xung đột.
Speed layer của lab **cố ý** bỏ dedup/cập nhật/hoàn tiền để thấy rõ con số "tạm" có thể sai **gấp đôi**; một speed layer thật có thể xử lý những việc đó.
Cái giá cố hữu của Lambda là **hai đường logic** phải được duy trì và **đối soát** — mọi quy tắc nghiệp vụ (dedup, phiên bản, xung đột) cài hai lần, hoặc
chấp nhận lệch. Lab **không** triển khai Kappa (không có stream processor giữ state, offset, replay qua cùng processor rồi chuyển phục vụ sang kết quả replay);
nó chỉ minh họa ý tưởng Kappa hướng tới: cùng `StagingOrders.compute` + `DailyRevenue.compute` dùng lại khi phát lại toàn bộ raw (bài 6 đối chiếu bản replay với
bảng đang phục vụ). Lập luận gốc so sánh hai kiến trúc: Jay Kreps, *Questioning the Lambda Architecture* (mục 16). Chương 29–30 cho thấy consumer streaming thật xử lý dedup/phiên bản thế nào; Spark Structured Streaming/Flink có
state và watermark — nhưng **semantics khác nhau**, đừng mặc định con số stream bằng con số batch.

**Lỗi thiết kế:** hiển thị con số speed layer như con số cuối cùng; không có đối soát định kỳ; viết logic streaming và batch ở hai team khác nhau.

### 3.7. Data contract, schema evolution và data quality — đặt ở đâu

Mỗi loại kiểm tra có **chỗ** của nó trong kiến trúc:

| Ranh giới | Kiểm tra | Hỏng thì | Chương |
|---|---|---|---|
| Ingestion (vào raw) | **data contract**: header/schema, kiểu, trường bắt buộc; tương thích FORWARD khi producer đổi schema | cách ly file (`raw_quarantine/`), báo producer | 33 (bài 5 ở đây) |
| Raw → staging | validate từng dòng, dedup, phiên bản, xung đột; đếm bị loại | dòng vào bảng rejected kèm lý do | 19, 24 |
| Staging → serving | check trên tập: uniqueness, toàn vẹn tham chiếu, đối soát, drift | không publish (write–audit–publish) | 24, 33, P3 |
| Serving (sau publish) | freshness, khối lượng, SLA | cảnh báo người trực | 26, 33 (bài 3 ở đây) |

**Schema evolution** chạm cả hai đầu: raw giữ **mọi phiên bản** schema producer từng gửi, nên staging phải đọc được cả cũ lẫn mới (hướng BACKWARD của reader
mới — Chương 21, 33); serving đổi schema là đổi hợp đồng với người đọc. **Table format** (Delta Lake, Apache Iceberg) thêm schema evolution có kiểm soát,
commit nguyên tử nhiều file và time travel cho lớp lưu trữ — ở mức khái niệm, giáo trình chưa kiểm chứng chúng.

### 3.8. Observability của nền tảng

Một job xanh không có nghĩa nền tảng khỏe. Bốn câu hỏi người trực cần trả lời được **không cần đọc code**:

1. **Freshness:** partition mới nhất của mỗi bảng phục vụ là ngày nào, tính lúc nào? (bài 3: SLA)
2. **Completeness:** có partition nào thiếu hoặc cũ? (`status`, dry-run ở bài 7)
3. **Chất lượng:** tỷ lệ bị loại mỗi lần tính (`rejected` trong lineage), drift (Ch33)
4. **Nguồn gốc:** số này từ file nào, code nào? (provenance, impact)

Lab ghi tất cả vào lineage và in ra qua CLI `status`; Dagster hiển thị cùng metadata (`rows`, `rejected`, `output_hash`) cho mỗi lần materialize.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong bài |
|---|---|
| Ingestion | đưa dữ liệu từ nguồn vào nền tảng |
| Raw / staging / serving layer | lớp gốc (append-only) / lớp đã làm sạch / lớp phục vụ người đọc |
| Medallion (bronze/silver/gold) | một cách đặt tên ba lớp |
| Orchestration vs processing | quyết định chạy gì khi nào / tính dữ liệu như thế nào |
| Asset, partition, materialization | bảng có định nghĩa / một phần của bảng (một ngày) / một lần tính partition |
| Lineage, provenance, impact analysis | quan hệ input–output / truy ngược nguồn / truy xuôi ảnh hưởng |
| Stale / fresh | partition cũ (input hoặc code đã đổi) / còn đúng — phân biệt "khớp cục bộ" (input trực tiếp) với "fresh tận nguồn" (cả upstream) |
| Snapshot (input) | bản đọc một lần của mọi input; hash ghi vào lineage và phép tính dùng cùng các byte đó |
| Backfill, reprocessing | tính quá khứ / tính lại vì code đổi |
| Idempotent | chạy N lần = chạy 1 lần |
| Lambda / Kappa architecture | hai đường batch + stream / một đường, tính lại bằng replay |
| Quarantine | cách ly dữ liệu vi phạm hợp đồng |
| Table format | lớp metadata trên file (Delta, Iceberg) cho commit nguyên tử, time travel |

## 5. Ví dụ đơn giản

Câu hỏi "partition này còn đúng không?" bằng ba dòng:

```scala
val p = Platform(Paths.get("/tmp/platform"))
Orchestrator.backfill(p, Assets.all, LocalDate.parse("2026-10-15"), LocalDate.parse("2026-10-16"), "run-1")
println(Orchestrator.describe(Orchestrator.status(p, StagingOrders, LocalDate.parse("2026-10-16"))))
// fresh — rồi thả lô 2026-10-17 vào raw/, hỏi lại:
// stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv
```

Không ai phải nhớ "lô 17 có chứa dữ liệu ngày 16 không" — lineage trả lời.

## 6. Ví dụ thực tế — ba lô đơn hàng, tính tay

Input: lô 15 và 16 giống Chương 24/33; lô **17** mới:

```
O6,C2,2026-10-16,REFUNDED,MS-01,3,250000,2026-10-17T05:00:00Z     ← hoàn tiền đơn ngày 16
O10,C1,2026-10-17,PAID,HP-01,1,990000,2026-10-17T06:00:00Z
O11,C3,2026-10-17,PAID,KB-01,2,450000,2026-10-17T07:00:00Z
```

Tính tay `daily_revenue` (chỉ PAID, segment từ `customers.csv`: C1, C3 Retail; C2 Wholesale; C9 không có → UNKNOWN):

| Ngày | Đơn PAID còn lại sau dedup | Kết quả |
|---|---|---|
| 15 | O1/MS-01 250.000 (C1), O8 450.000 (C1, đến ở lô 16), O2 20.000.000 (C2, gửi 2 lần), O4 450.000 (C9). O1/KB-01 đã REFUNDED | Retail 700.000 (2 dòng, 2 đơn); UNKNOWN 450.000; Wholesale 20.000.000 |
| 16 | O6 bị REFUNDED ở lô 17; O7 PAID và CANCELLED cùng `updated_at` → xung đột, không tính | **rỗng** |
| 17 | O10 990.000 (C1) + O11 2 × 450.000 (C3) | Retail 1.890.000 (2 dòng, 2 đơn) |

Trước khi lô 17 đến, ngày 16 là Wholesale 750.000 (O6 còn PAID). Lô 17 phải sửa ngày 16 — và **chỉ** ngày 16 với 17, không đụng ngày 15. Bảng này được tính
trước khi chạy và khớp với output thật mục 9 cùng test `golden tính tay sau 3 lô`.

## 7. Code đầy đủ

```
architecture-lab/
├── project.scala, build.sbt, project/build.properties
├── data/customers.csv, data/raw/orders/ingest_date=2026-10-15|16|17/orders.csv
├── dagster/defs.py, dagster/run-dagster.sh        # orchestration bằng Dagster (Python), gọi job Scala
└── src/
    ├── main/scala/vn/scalaeng/platform/
    │   ├── Platform.scala        # bố cục lớp, hash nội dung, ghi nguyên tử, parse dòng raw
    │   ├── Assets.scala          # Asset, Source, InputFile (snapshot), InputRef, StagingOrders, DailyRevenue
    │   ├── Orchestrator.scala    # Lineage (JSONL), Status, status/freshness, materialize, backfill, impact
    │   ├── SpeedLayer.scala      # speed layer ngây thơ + đối soát với batch
    │   ├── Main.scala            # CLI: materialize | status | backfill | impact (mã thoát 0/1/2)
    │   └── ArchitectureDemo.scala
    └── test/scala/vn/scalaeng/platform/PlatformSuite.scala   # 13 test
```

```
cd 11-data-platform/architecture-lab
scala-cli test .                                                        # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.platform.ArchitectureDemo     # kịch bản mục 9, trên bản sao tạm của data/
scala-cli --power package . --assembly --main-class vn.scalaeng.platform.Main -o <jar>
DAGSTER=<venv>/Scripts/dagster CH31_JAR=<jar> WORK=<thư mục đã có> dagster/run-dagster.sh   # tạo thư mục con mới trong WORK, không xóa gì
```

## 8. Giải thích code

- **`Platform`** — mọi đường dẫn đi qua một chỗ; `partitionFile(layer, asset, day)` là quy ước duy nhất về bố cục. `Files2.hash` = 16 ký tự hex đầu của
  SHA-256 — đủ phát hiện thay đổi, không dùng cho bảo mật. `writeAtomically` dùng `Files.createTempFile` trong **cùng thư mục** (move nguyên tử chỉ đúng trên
  cùng filesystem).
- **`StagingOrders.sources`** liệt kê mọi file raw; **`relevant`** chỉ giữ file (trong snapshot) có dòng khai báo ngày `day` — lineage chính xác (mục 3.4).
  `compute` dùng `distinct` cho trùng tuyệt đối, giữ bản `updated_at` lớn nhất, loại xung đột.
- **`Orchestrator.materialize`** giữ khóa file độc quyền **toàn root** từ snapshot đến append lineage; run trùng bị từ chối ngay, cần chạy lại sau. Khóa thô
  giảm khả năng song song, nhưng ngăn downstream đọc giữa publish/append của upstream. Hash output tính từ serialization riêng của run trước rename, cùng
  cách ghi CSV; không đọc lại target có thể bị thay bởi run khác. Upstream phải có ID hợp lệ khớp partition/hash, nếu thiếu phải phục hồi upstream trước.
  Hai bước ghi output nguyên tử → thêm dòng lineage **không** là một giao dịch: process chết vẫn có thể để output mới với lineage cũ; khóa không làm hai
  file thành transaction. `status` phát hiện mismatch → backfill theo topo. Status không lấy khóa và có thể thấy trạng thái chuyển tiếp; đây không phải cam kết
  giám sát có snapshot đồng thời với writer. Hệ thống lớn có thể dùng khóa theo partition cùng giao thức đọc phiên bản bất biến.
- **`status` / `freshness`** — mục 3.4. `freshness` đệ quy theo đúng partition trong `InputRef`, mang theo tập `seen` để không lặp vô hạn nếu đồ thị có chu trình.
- **`Lineage`** — circe `deriveEncoder/deriveDecoder`; file chỉ thêm, ghi với `DSYNC`. Dòng hỏng làm đọc lineage **ném lỗi** (không bỏ qua): lineage sai còn
  nguy hiểm hơn không có lineage.
- **`impact`** — bắt đầu từ **mọi** lần tính từng đọc file raw, lan theo các lần tính downstream đã đọc chúng (qua `materializationId`), rồi giữ những partition mà
  lần tính **hiện hành** nằm trong tập đó. Nếu đồ thị phục vụ còn cạnh thiếu/sai ID từ lineage cũ, API dừng với `impact không đầy đủ`, không trả danh sách
  có vẻ đầy đủ. `status` đánh dấu downstream đó stale kể cả upstream đã phục hồi cùng byte; backfill topo sẽ ghi lại ID hợp lệ. Provenance (bài 2) vẫn in
  `? staging_orders@...` cho dữ liệu cũ thiếu nguồn. Không suy ID lịch sử chỉ từ hash: cùng byte có thể có nguồn gốc khác.
- **`Main.run`** trả mã thoát thay vì gọi `sys.exit` — test gọi thẳng `run`; chỉ `main` gọi `sys.exit` (mục 10, lỗi 2).

## 9. Output mong đợi (output thật)

`scala-cli.bat test . --server=false` (Vòng 3; sbt do lead kiểm chứng riêng):

```
Test run vn.scalaeng.platform.PlatformSuite finished: 0 failed, 0 ignored, 17 total 11.059s
```

`ArchitectureDemo` (lô 17 được thả vào **giữa** kịch bản):

```
== Lần 1: backfill 15..16 trên hai lô đầu
   2026-10-15 staging_orders: materialize (missing → rows=5 rejected=1)
   2026-10-15 daily_revenue: materialize (missing → rows=3 rejected=0)
   2026-10-16 staging_orders: materialize (missing → rows=1 rejected=2)
   2026-10-16 daily_revenue: materialize (missing → rows=1 rejected=0)
== Lần 2: chạy lại y hệt (idempotent)
   2026-10-15 staging_orders: skip (fresh)
   2026-10-15 daily_revenue: skip (fresh)
   2026-10-16 staging_orders: skip (fresh)
   2026-10-16 daily_revenue: skip (fresh)
== Lô 2026-10-17 đến (hoàn tiền O6 của ngày 16 + đơn ngày 17). Trạng thái TRƯỚC khi chạy:
   2026-10-15 staging_orders: fresh
   2026-10-15 daily_revenue: fresh
   2026-10-16 staging_orders: stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv
   2026-10-16 daily_revenue: stale: upstream staging_orders@2026-10-16 stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv
   2026-10-17 staging_orders: missing
   2026-10-17 daily_revenue: missing
== Lần 3: backfill 15..17
   2026-10-15 staging_orders: skip (fresh)
   2026-10-15 daily_revenue: skip (fresh)
   2026-10-16 staging_orders: materialize (stale: input mới raw/orders/ingest_date=2026-10-17/orders.csv → rows=1 rejected=2)
   2026-10-16 daily_revenue: materialize (stale: input đổi staging_orders@2026-10-16 → rows=0 rejected=0)
   2026-10-17 staging_orders: materialize (missing → rows=2 rejected=0)
   2026-10-17 daily_revenue: materialize (missing → rows=1 rejected=0)
== Impact của raw/orders/ingest_date=2026-10-17/orders.csv:
   daily_revenue@2026-10-16
   daily_revenue@2026-10-17
   staging_orders@2026-10-16
   staging_orders@2026-10-17
== serving/daily_revenue:
   2026-10-15 | segment,revenue_vnd,order_lines,orders
   2026-10-15 | Retail,700000,2,2
   2026-10-15 | UNKNOWN,450000,1,1
   2026-10-15 | Wholesale,20000000,1,1
   2026-10-16 | segment,revenue_vnd,order_lines,orders
   2026-10-17 | segment,revenue_vnd,order_lines,orders
   2026-10-17 | Retail,1890000,2,2
== Đối soát speed layer (cộng PAID khi đến) với batch:
   2026-10-15: speed=42050000 batch=21150000 lệch=20900000
   2026-10-16: speed=1740000 batch=0 lệch=1740000
== Lineage: 8 dòng trong _lineage/materializations.jsonl
```

Đọc kỹ: `rejected=1` ngày 15 là O3 (`quantity = 0`); `rejected=2` ngày 16 là O9 (`quantity = abc`) + một dòng hàng xung đột (O7). Dòng
`2026-10-16 daily_revenue` **trước** khi chạy là freshness tận nguồn: input trực tiếp (staging ngày 16) chưa đổi nên nó **khớp cục bộ**, nhưng staging ngày 16
đã cũ nên số đang phục vụ không còn đúng. Lần 3 vẫn ghi lý do cục bộ `input đổi staging_orders@2026-10-16` — đó là lý do backfill thực sự chạy nó.

**Dagster** điều phối cùng job (`dagster/run-dagster.sh`, mỗi partition một lệnh `dagster asset materialize --partition`):

```
partition 2026-10-15: exit=0 2 materialization
partition 2026-10-16: exit=0 2 materialization
partition 2026-10-17: exit=0 2 materialization
```

Log của Dagster cho partition 16 (trích):

```
2026-10-17 11:59:35 +0700 - dagster - DEBUG - __ASSET_JOB - 2589fc0e-... - 3100 - staging_orders - ASSET_MATERIALIZATION - Materialized value staging_orders.
2026-10-17 11:59:35 +0700 - dagster - DEBUG - __ASSET_JOB - 2589fc0e-... - 3100 - staging_orders - STEP_SUCCESS - Finished execution of step "staging_orders" in 1.1s.
2026-10-17 11:59:39 +0700 - dagster - DEBUG - __ASSET_JOB - 2589fc0e-... - 20220 - daily_revenue - ASSET_MATERIALIZATION - Materialized value daily_revenue.
2026-10-17 11:59:41 +0700 - dagster - DEBUG - __ASSET_JOB - 2589fc0e-... - 23360 - RUN_SUCCESS - Finished execution of run for "__ASSET_JOB".
```

Dagster tự xếp `staging_orders` trước `daily_revenue` (khai báo `deps`), và file lineage do **job Scala** ghi có 6 dòng — hai hệ thống, mỗi bên giữ đúng phần
của mình. Mỗi bước ~1 giây chủ yếu là khởi động JVM: gọi job qua subprocess cho mỗi partition là đơn giản nhưng đắt khi có hàng nghìn partition nhỏ.

## 10. Lỗi thường gặp (thật)

**Lỗi 1 — Partition của ngày hôm nay "không tồn tại" trong Dagster.** Lần chạy đầu (ngày 17/10, `DailyPartitionsDefinition` không có `end_offset`):

```
dagster._core.errors.DagsterUnknownPartitionError: Could not find a partition with key `2026-10-17`.
...
dagster._core.errors.DagsterInvalidSubsetError: All selected assets must have a PartitionsDefinition containing the passed partition key `2026-10-17` or have no PartitionsDefinition.
```

Mặc định Dagster chỉ tạo partition cho khoảng thời gian **đã kết thúc**: ngày 17 chưa hết thì chưa có partition 17. Docstring của
`DailyPartitionsDefinition` trong Dagster 1.13.26 nói đúng điều này: *"If end_offset is 0 (the default), the last partition ends before the current time."* `end_offset=1` cho phép partition của
ngày đang diễn ra — một **quyết định nghiệp vụ** (số liệu hôm nay chưa đủ, sẽ được tính lại khi lô muộn đến), không phải sửa lỗi kỹ thuật cho xong.
"Ngày" ở đây cắt theo `timezone` của partition — mặc định **UTC**. Reviewer kiểm tra lúc 00:30 giờ Việt Nam ngày 17: với `end_offset=1` chỉ có key 15 và 16
(vẫn là ngày 16 theo UTC). Lab khai báo `timezone="Asia/Ho_Chi_Minh"` để "hôm nay" khớp lịch vận hành.

**Lỗi 2 — Mã thoát luôn là 1 dù job trả 2 — không có cảnh báo nào.** Bản đầu của `Main.main`:

```scala
val code =
  try run(args.toSeq, println)
  catch case e: Exception => System.err.println(s"lỗi: $e"); 1
sys.exit(code)
```

`java -jar architecture-lab.jar status` (thiếu `--root`) in đúng thông báo hướng dẫn nhưng `echo $?` ra **1** thay vì **2**. Nguyên nhân: trong khối thụt lề
của `val code =`, `; 1` là **câu lệnh thứ hai của khối**, không thuộc nhánh `catch` — giá trị của khối luôn là `1`, kết quả của `try` bị bỏ. Tái hiện tối thiểu:

```
-- [E198] Unused Symbol Warning: ...T.scala:6:15
6 |    catch case e: Exception => println("l?i"); 1
  |               ^
  |               unused pattern variable
1 warning found
code=1
```

Ở bản tối thiểu còn có cảnh báo "unused pattern variable"; trong `Main` thật `e` **có** được dùng (`$e`) nên **không có cảnh báo nào**. Orchestrator nhận mã
sai thì ra quyết định sai (Chương 26: mã 2 = cấu hình sai, chạy lại vô ích). Sửa: `Try(run(...)).fold(e => { ...; 1 }, identity)`, và luôn kiểm mã thoát
qua **đúng** công cụ chạy production.

**Lỗi 3 — `/*` trong Scaladoc mở comment lồng nhau.** Viết bố cục `raw/<nguồn>/ingest_date=D/*.csv` trong `/** ... */`:

```
-- Error: ...Platform.scala:10:0
   |^
   |unclosed comment
```

Lần thứ **ba** lỗi này xuất hiện trong giáo trình (Chương 26, Project 3, và đây): Scala cho comment lồng nhau, `/*` bên trong mở comment mới. Viết khác đi
(`ingest_date=D/` + "các file .csv").

**Lỗi 4 — Input đổi trong lúc tính.** Bản đầu của `materialize` đọc hash input, tính (hàm tính **tự đọc đĩa**), ghi output, rồi đọc hash **lần nữa**; khác
nhau thì ném:

```
java.lang.IllegalStateException: input của racy@2026-10-15 đổi trong lúc tính — chạy lại
```

Review chéo chỉ ra hai lỗ hổng của cách "so hai đầu": (1) lỗi được ném **sau** khi output đã thay bản đang phục vụ — một lần tính bị từ chối vẫn publish dữ liệu;
(2) dimension đổi A → B → A trong lúc tính thì hai lần đọc đều thấy A, lineage ghi A, `status` báo fresh, nhưng output tính từ B — race **ABA**. So hai đầu
không chứng minh được input nào **thực sự** đã được dùng. Bản hiện tại chụp **snapshot**: đọc mỗi input một lần, hash và tính trên **cùng** các byte đó (mục
3.3). Lô đến giữa chừng không lọt vào output lẫn lineage, và lần `status` sau báo `input mới` (test `snapshot: lô raw đến TRONG lúc tính` và
`snapshot: dimension đổi rồi đổi lại`).

**Lỗi 5 — Partition theo ngày nhận.** Nếu staging partition theo `ingest_date`, hoàn tiền O6 (lô 17) nằm ở partition 17 và ngày 16 vẫn báo 750.000 — không có
exception nào, chỉ có số sai. Bảng nghiệp vụ partition theo ngày **nghiệp vụ**; ngày nhận chỉ dùng cho lớp raw.

## 11. Cách debug

1. **Số trên dashboard sai:** `status` cho ngày đó (freshness **tận nguồn**: một upstream cũ cũng hiện ra) → nếu fresh: provenance (bài 2) → đọc đúng các file
   raw đó, tính tay. Đừng dựa vào trạng thái cục bộ của riêng bảng serving — nó chỉ nói input trực tiếp chưa đổi.
2. **Một file nguồn bị phát hiện hỏng:** `impact --raw=<file>` → danh sách partition bị ảnh hưởng → cách ly file (bài 5) → backfill đúng các ngày đó.
   Nếu báo `impact không đầy đủ`: giữ log/lineage, phục hồi upstream theo topo rồi backfill downstream thiếu ID trước khi hỏi lại impact. Không coi kết quả
   chưa truy được là tập ảnh hưởng rỗng. Khi upstream lịch sử cũng thiếu nguồn, dựng root sạch theo bước 6 để phục hồi cả đồ thị.
3. **Backfill chạy lại quá nhiều:** đọc lý do `stale:` — "input đổi customers.csv" (dimension đổi làm cũ mọi ngày) là đúng; "output trên đĩa khác" nghĩa là
   ai đó sửa tay serving.
4. **Orchestrator báo thành công nhưng bảng không đổi:** kiểm mã thoát thật của job (lỗi 2) và dòng lineage mới có xuất hiện không.
5. **Lineage và output mâu thuẫn:** replay từ raw trên root tạm (bài 6). Bằng nhau chỉ là bằng chứng cho **snapshot input + code hiện tại**; khác nhau thì
   trước hết xem partition có stale không (input/code đã đổi mà chưa backfill, hoặc dimension hiện tại khác bản serving đã dùng), rồi mới kết luận sửa tay hay
   pipeline không tất định.
6. **Lineage hỏng** (`status` ném `IllegalStateException: lineage hỏng: ...`): đây là **fail-closed** có chủ đích, và `backfill --force=true` cũng không vượt qua —
   `force` chỉ bỏ quyết định skip, lineage vẫn phải đọc được. Phục hồi: giữ nguyên file hỏng làm bằng chứng (copy sang chỗ khác, không sửa tại chỗ); xem dòng
   lỗi và bản sao lưu; dựng lại trên **root sạch** từ raw + đúng bản dimension đã chọn (bài 6), đối chiếu từng partition với serving hiện tại, rồi mới thay
   root. Không tự bỏ qua dòng lỗi — một dòng lineage mất là một partition bị báo sai trạng thái.

## 12. Bài tập

1. **(Cơ bản)** Asset `segment_totals` **không phân vùng** phụ thuộc mọi partition `daily_revenue` trong một khoảng ngày (many-to-one). Chứng minh một ngày
   daily đổi làm nó cũ.
2. **(Cơ bản)** `provenance(asset, day)`: truy ngược tới các file raw, bắc cầu qua upstream.
3. **(Trung bình)** Kiểm tra **SLA**: liệt kê partition thiếu, cũ, hoặc tính đã quá `maxAge`.
4. **(Trung bình)** **Cửa sổ tính lại** N ngày; liệt kê partition cũ nằm **ngoài** cửa sổ thay vì bỏ qua âm thầm.
5. **(Trung bình)** **Hợp đồng ở cửa ngõ:** cách ly file raw sai header sang `raw_quarantine/` trước khi pipeline thấy nó.
6. **(Nâng cao)** **Replay từ raw + đối chiếu** (ý tưởng Kappa): tính lại toàn bộ từ raw trên root mới, so từng partition với serving.
7. **(Nâng cao)** **Dry-run:** kế hoạch chạy mà không thực thi, chia "chắc chạy" / "có thể chạy" / skip. Downstream đang stale vẫn có thể skip nếu upstream
   tính lại trở về hash downstream đã dùng; xem lời giải bài 7 về cận trên và giới hạn DAG cùng ngày.

## 13. Đáp án và lời giải

[`solutions/ch31-solutions.md`](../solutions/ch31-solutions.md), code [`solutions/ch31-code/`](../solutions/ch31-code/) — 10/10 test pass (Vòng 3).

## 14. Câu hỏi kiểm tra

1. Vì sao lớp raw phải append-only? Điều gì mất đi nếu ta "sửa" file raw cho đúng?
2. Phân biệt orchestration và processing. Kể một việc thuộc mỗi bên trong lab.
3. Ba điều kiện để một asset chạy lại được an toàn là gì?
4. Vì sao lineage chính xác (theo file raw chứa ngày D) tốt hơn "mọi file raw"?
5. Trước lần 3 của demo, `daily_revenue@16` khớp cục bộ nhưng không fresh tận nguồn. Vì sao, và backfill dùng câu hỏi nào để quyết định chạy nó?
6. Backfill và reprocessing khác nhau ở nguyên nhân, giống nhau ở cơ chế nào?
7. Speed layer của lab lệch batch bao nhiêu ở ngày 15, vì những đơn nào?
8. Data contract nên kiểm tra ở ranh giới nào, data quality trên tập ở ranh giới nào?
9. Vì sao Dagster không tìm thấy partition `2026-10-17` khi chạy trong ngày 17?
10. Vì sao `; 1` trong `catch case e => ...; 1` làm mã thoát luôn là 1?

<details><summary>Gợi ý đáp án</summary>

1. Raw là nguồn để tính lại mọi lớp sau; sửa nó là mất khả năng tái tạo quá khứ và mất bằng chứng nguồn đã gửi gì.
2. Orchestration: chọn ngày/partition, thứ tự, thử lại (`backfill`, Dagster). Processing: `StagingOrders.compute`.
3. Tất định, ghi nguyên tử, thay thế (không append).
4. Lô mới chỉ làm cũ đúng các ngày nó chạm; không tính lại vô ích.
5. Input trực tiếp (staging ngày 16) chưa đổi cho tới khi staging được tính lại, nhưng staging đã cũ vì lô 17. Backfill dùng trạng thái cục bộ theo thứ
   tự topo: staging tính lại trước, hash đổi, daily thành "input đổi" và được tính trong cùng lượt.
6. Nguyên nhân: dữ liệu (thiếu/muộn) vs code đổi. Cơ chế: cùng vòng "chỉ tính partition thiếu hoặc cũ".
7. 20.900.000: O2 bị cộng hai lần (20.000.000) và O1/KB-01 đã hoàn tiền (900.000).
8. Contract ở ingestion; check trên tập giữa staging và serving (write–audit–publish).
9. Mặc định chỉ có partition của ngày đã kết thúc; cần `end_offset=1`. Và "ngày" cắt theo `timezone` của partition (mặc định UTC).
10. `; 1` là câu lệnh thứ hai của khối `val code =`, không thuộc `catch`; giá trị khối là `1`.
</details>

## 15. Mini project

Thêm nguồn **click** (JSON Lines, như Project 3) vào lab: asset `staging_clicks` và `campaign_daily` phụ thuộc cả `staging_clicks` và `staging_orders` (một asset hai
upstream). Yêu cầu: lineage chính xác cho cả hai nguồn; một lô click muộn chỉ làm cũ `campaign_daily` của đúng ngày đó, không đụng `daily_revenue`; thêm hai
asset vào `defs.py` và chạy bằng Dagster cho 3 partition. Viết một trang README: sơ đồ asset, lớp lưu trữ, ai sở hữu từng bảng, SLA của từng bảng phục vụ.

## 16. Nguồn tài liệu chính thức

- Dagster docs — software-defined assets, partitions, `DailyPartitionsDefinition` (`end_offset`), CLI `dagster asset materialize`: https://docs.dagster.io/
- Dagster trên PyPI (phiên bản 1.13.26, yêu cầu Python `>=3.10,<3.15`, tra 17/10/2026): https://pypi.org/project/dagster/
- Apache Airflow (orchestrator thay thế): https://airflow.apache.org/docs/
- Jay Kreps, *Questioning the Lambda Architecture* (O'Reilly Radar, 2014): https://www.oreilly.com/radar/questioning-the-lambda-architecture/
- circe: https://circe.github.io/circe/ · circe-generic 0.14.17: https://repo1.maven.org/maven2/io/circe/circe-generic_3/0.14.17/
- Delta Lake, Apache Iceberg (table format — khái niệm, chưa kiểm chứng trong giáo trình): https://docs.delta.io/ , https://iceberg.apache.org/docs/latest/
- OpenLineage (chuẩn mở cho lineage — khái niệm): https://openlineage.io/docs/
- Java `Files.move` với `ATOMIC_MOVE`: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html
- Java 21 `Files.readAllBytes` (lỗi đọc được truyền ra), `FileChannel.tryLock` (khóa độc quyền, từ chối ngay khi đang bận), đối chiếu 17/10/2026:
  https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#readAllBytes(java.nio.file.Path)
  và https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html#tryLock()

## 17. Checklist tự đánh giá

- [ ] Tôi vẽ được các lớp raw/staging/serving và nói quy tắc ghi của từng lớp.
- [ ] Tôi tách được orchestration khỏi processing; job của tôi chạy được bằng tay, bằng cron hay bằng Dagster như nhau.
- [ ] Hàm tính partition của tôi tất định, ghi nguyên tử, thay thế chứ không append.
- [ ] Tôi ghi lineage có hash và dùng nó để biết partition nào cũ, vì sao.
- [ ] Backfill và reprocessing của tôi chỉ tính lại đúng phần cần, theo thứ tự topo, chạy lại được.
- [ ] Tôi giải thích được Lambda vs Kappa và đối soát được speed layer với batch.
- [ ] Tôi đặt contract, validation, audit, freshness đúng ranh giới.
- [ ] Tôi kiểm mã thoát của job qua đúng công cụ gọi nó.
