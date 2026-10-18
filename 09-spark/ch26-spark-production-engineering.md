# Chương 26 — Spark Production Engineering

> **Phiên bản:** **Scala 2.13.18** · JDK 21 · Apache Spark **4.2.0** (`spark-sql_2.13`, scope `Provided`) · sbt 1.12.11 + sbt-assembly 2.3.1 ·
> hadoop-bare-naked-local-fs 0.1.0 (chỉ Windows) · munit 1.2.4
> **Code:** [`spark-production-lab/`](spark-production-lab/) — bọc pipeline Chương 24 thành job production: cấu hình có kiểm tra, log JSON, metric từ
> SparkListener, retry có phân loại, quality gate theo lô, manifest lần chạy, mã thoát có nghĩa, checkpoint; [`scala3-spark-probe/`](scala3-spark-probe/) —
> thí nghiệm Scala 3 với Spark
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` và `dlcdn.apache.org` ngày 17/10/2026. **13/13 test pass** (`scala-cli test .` và `sbt test`), lời giải 6/6.
> `sbt assembly` + **`spark-submit` thật** (bản phân phối `spark-4.2.0-bin-hadoop3`, SHA-512 đã đối chiếu) trên **WSL2 Ubuntu 24.04** + Temurin 21.0.12.1. Mọi log,
> manifest, số đo và thông báo lỗi trong bài là output thật.

---

## Vị trí trong lộ trình

Chương 24 xây pipeline đúng và chạy lại được; Chương 25 làm nó nhanh hơn có bằng chứng. Chương này trả lời câu hỏi của người **vận hành**: job chạy lúc 2 giờ sáng
không ai nhìn — làm sao biết nó chạy với cấu hình gì, có thành công không, nếu lỗi thì có nên chạy lại tự động không, và dữ liệu nào do lần chạy nào tạo ra?
Project 3 dùng toàn bộ kỹ thuật này; Chương 31 đặt job vào một nền tảng dữ liệu (orchestration, lineage).

## 1. Mục tiêu học tập

1. Thiết kế **cấu hình** của job: nguồn ưu tiên (tham số > biến môi trường > mặc định an toàn), kiểm tra lúc khởi động, gom mọi lỗi.
2. Viết **log có cấu trúc** (JSON, có `run_id`) và **metric** của job từ `SparkListener`, xuất cho Prometheus.
3. Phân biệt lỗi **tạm thời** (retry) với lỗi **vĩnh viễn** và lỗi **dữ liệu** (không retry); dùng **mã thoát** có nghĩa cho scheduler.
4. Đặt **quality gate** đúng chỗ (lô của ngày chạy) và **manifest** cho mỗi lần chạy; kết hợp với idempotency của Chương 24.
5. Biết khi nào dùng **checkpoint** (cắt lineage) và khác gì `localCheckpoint`, `cache`.
6. Viết **job test** và **integration test** gọi đúng điểm vào của job.
7. **Triển khai**: fat jar với Spark `Provided`, `spark-submit`; giải thích các bẫy Windows.
8. Nắm **tương thích phiên bản**: Spark 4.2 ↔ Scala 2.13 ↔ JDK; vì sao không ép Scala 3 vào Spark.

## 2. Kiến thức tiên quyết

Chương 24 (pipeline, `RunReport`, rerun safety), Chương 22 (listener bất đồng bộ + job rào chắn, lineage, task retry), Chương 25 (đo bằng metric), Chương 17
(log không chứa bí mật, health/metric), Chương 9–10 (sbt, test).

## 3. Lý thuyết

### 3.1. Cấu hình: thiếu là lỗi khởi động

**Vấn đề.** Job đọc `input` mặc định `/data/orders` — một ngày ai đó quên truyền tham số, job đọc nhầm thư mục cũ và ghi đè báo cáo hôm nay bằng số liệu hôm qua,
**thành công**. Lỗi cấu hình là lỗi đắt nhất vì nó **không ném exception**.

**Cách hoạt động.** `JobConfig.parse(args, env, newRunId)`:

- Nguồn: `--key=value` > biến môi trường `CH26_KEY` > mặc định — **chỉ** cho tham số an toàn (`max-reject-rate=0.05`, `shuffle-partitions=4`, `max-attempts=3`,
  `run-id` = UUID mới). **Đường dẫn và ngày chạy không có mặc định.**
- Kiểm tra từng giá trị (ngày hợp lệ, tỷ lệ trong [0, 1] — không NaN, số nguyên trong khoảng), tham số lạ, tham số sai dạng — và **gom hết** (output thật của test):

```
tham số 'lạc' phải có dạng --key=value
tham số lạ --nope
thiếu --input (hoặc CH26_INPUT)
thiếu --customers (hoặc CH26_CUSTOMERS)
thiếu --output (hoặc CH26_OUTPUT)
--run-date='2026-13-01': ngày dạng yyyy-MM-dd
--max-reject-rate='NaN': số thực trong [0, 1]
```

Cấu hình sai → mã thoát **2**, chưa chạm Spark hay đĩa. Điều này phải đúng ở **điểm vào thật** (`main`), không chỉ trong hàm được test: phiên bản đầu
của lab tạo `SparkSession` trong `main` **trước** khi parse — reviewer chạy `spark-submit` không tham số và job chết vì lỗi khởi tạo Spark trước khi kịp báo
thiếu tham số. Giờ `main` gọi `Job.launch(args, env, sink, newSession, ...)`: parse trước, chỉ gọi `newSession()` khi cấu hình hợp lệ; test truyền một
`newSession` gọi `fail(...)` để chứng minh. Output thật khi chạy `--input=x` — không có một dòng log Spark nào:

```
{"ts":"2026-10-17T04:06:50.106016900Z","level":"ERROR","run_id":"-","event":"config_invalid","errors":["thiếu --customers (hoặc CH26_CUSTOMERS)","thiếu --output (hoặc CH26_OUTPUT)","thiếu --run-date"]}
```

Tham số **có mặt nhưng rỗng** (`--max-attempts=`) và tham số **lặp lại** là lỗi, không âm thầm dùng mặc định hay lấy bản cuối — người gõ đã có ý định khác. **`run-date` là tham số, không phải `LocalDate.now()`**: job chạy lại cho ngày 16 vào ngày 20 (backfill) phải
cho kết quả của ngày 16. **Lựa chọn thay thế:** file cấu hình (HOCON, YAML) cho nhiều tham số; `--conf` của Spark cho cấu hình **engine** — đừng trộn cấu hình
nghiệp vụ vào `SparkConf`. **Bí mật** (mật khẩu DB) lấy từ biến môi trường/secret manager, không bao giờ in ra log hay manifest.

### 3.2. Log có cấu trúc và metric

**Log.** Mỗi dòng một JSON có `run_id` (output thật của một lần chạy):

```
{"ts":"2026-10-17T04:05:58.175824Z","level":"INFO","run_id":"demo-2026-10-16","event":"job_started","execution_id":"2026-10-17T040558052457700Z-6157be60","run_date":"2026-10-16",...,"spark":"4.2.0","scala":"2.13.18","java":"21.0.12.1"}
{"ts":"2026-10-17T04:06:04.940209600Z","level":"INFO","run_id":"demo-2026-10-16","event":"quality_gate_passed","rows":6,"rejected":1}
{"ts":"2026-10-17T04:06:16.553234400Z","level":"INFO","run_id":"demo-2026-10-16","event":"pipeline_succeeded","input_rows":13,"removed_partitions":[],"fact_lines":6,"gate_rejected":1,"rejected_rows":3,"paid_revenue_vnd":21900000,"failed_checks":[],"conflict_rows":2,"gate_rows":6}
{"ts":"2026-10-17T04:06:16.575777400Z","level":"INFO","run_id":"demo-2026-10-16","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":18516,"attempts":1,"execution_id":"2026-10-17T040558052457700Z-6157be60"}
```

`run_id` do người vận hành đặt (thường là ngày nghiệp vụ, để chạy lại cùng ngày dễ tìm); `execution_id` do job sinh, **duy nhất** cho mỗi lần thực thi.

Hệ thống log (Loki, Elasticsearch, CloudWatch) lọc `run_id="..." AND level="ERROR"` thay vì grep chuỗi. Log ghi **số đếm, đường dẫn, mã lỗi** — không ghi dữ liệu khách
hàng. Đây là log của **driver**; log của executor (ở cluster nằm trên máy khác) thu qua YARN/Kubernetes log aggregation.

**Metric.** `JobMetrics` là một `SparkListener` gắn với **job group** của lần chạy (chỉ đếm việc của job này), cộng `TaskMetrics`; khi kết thúc chạy một job
**rào chắn** để chắc hàng đợi sự kiện đã xử lý xong (bài học Chương 22 mục 3.4). `close()` trả `Either`: rào chắn lỗi hoặc hết hạn chờ → `Left(lý do)`, job log
`metrics_incomplete` và ghi `metrics_complete: false` vào manifest — **không** nuốt lỗi rồi trả số dở dang như số thật (phiên bản đầu dùng
`Try(close()).getOrElse(asMap)`, reviewer dừng SparkContext giữa chừng và job vẫn báo SUCCEEDED với metric thiếu). Listener và job group luôn được gỡ trong `finally`.

**Hai loại số, đừng trộn.** Metric của listener là **chi phí vật lý**: mọi task của mọi lần thử, kể cả task thất bại, lần ghi bị retry, và các lượt verify đọc lại.
Số **nghiệp vụ đã commit** (bao nhiêu fact, bao nhiêu dòng bị loại) nằm trong `details`, lấy từ `RunReport` sau đối soát. Ở lần chạy sạch một lần thử, hai số trùng
nhau tình cờ: `spark_records_written = 15` = 6 fact + 4 dòng doanh thu + 3 rejected + 2 conflict. Reviewer tiêm lỗi làm pipeline chạy hai lần → metric **30**,
output vẫn 15 dòng. Dùng metric cho chi phí và xu hướng; dùng `details` cho đối soát.

**Prometheus.** Job batch **không có endpoint để Prometheus scrape** (nó chết sau vài phút) → ghi file text cho node_exporter textfile collector hoặc đẩy lên
Pushgateway. Lab ghi **một** file ổn định `output/_metrics/ch26_orders.prom` (ghi đè nguyên tử mỗi lần chạy) — trạng thái **hiện tại** của job. Phiên bản đầu ghi
một file `.prom` mỗi lần chạy vào `_runs/`: collector đọc cả thư mục sẽ thấy nhiều file cùng tên metric + cùng nhãn → series **trùng** (exposition format không cho
phép). Lịch sử thuộc về manifest JSON, không thuộc về metric. Output thật sau lần chạy bị gate chặn (rút gọn):

```
# TYPE job_exit_code gauge
job_exit_code{job="ch26_orders"} 3
# TYPE job_last_run_date_epoch_day gauge
job_last_run_date_epoch_day{job="ch26_orders"} 20742
# TYPE job_metrics_complete gauge
job_metrics_complete{job="ch26_orders"} 1
# TYPE spark_tasks_failed gauge
spark_tasks_failed{job="ch26_orders"} 0
```

Ngày chạy là **giá trị** (`job_last_run_date_epoch_day`, 20742 = 16/10/2026), không phải nhãn: nhãn `run_date` sinh một series mới mỗi ngày (backfill một năm = 365
series không bao giờ hết hạn). Nhãn chỉ dùng giá trị **ít** (`job`); nhãn `run_id` hay `order_id` làm bùng nổ số chuỗi thời gian. Giá trị nhãn escape `\`, `"` và
xuống dòng. **Cảnh báo nên đặt trên:** job không có lần chạy `SUCCEEDED` cho ngày X trước giờ Y (lời giải bài 3),
`spark_tasks_failed` tăng, khối lượng tụt bất thường (lời giải bài 4) — không chỉ "job lỗi".

### 3.3. Retry: chỉ cho lỗi tạm thời

Spark đã retry **task** (`spark.task.maxFailures`, Chương 22). Retry ở mức **job** dành cho lỗi mà cả job phải làm lại: storage chập chờn lúc liệt kê file, mất kết nối
metastore. Bộ phân loại `Failures.classify` đi qua **toàn bộ** chuỗi `getCause` (Spark bọc lỗi gốc trong `SparkException` nhiều tầng) và áp **thứ tự ưu tiên** khi
chuỗi chứa nhiều loại — dữ liệu > vĩnh viễn > tạm thời > chưa biết:

| Lỗi (ở bất kỳ tầng nào của chuỗi cause) | Nhóm | Hành động |
|---|---|---|
| `IllegalArgumentException` (vi phạm hợp đồng: khóa trùng), `AnalysisException` | dữ liệu | mã 3, báo bên gửi, **không retry** |
| `NoSuchFileException`, `AccessDeniedException`, `FileAlreadyExistsException`, `FileNotFoundException`, `ZipException`, `EOFException` | vĩnh viễn | dừng — lần sau vẫn hỏng y hệt |
| `SocketTimeoutException`, `ConnectException`, `SocketException`, `TransientStorageException` | tạm thời | retry có giới hạn + backoff |
| còn lại — **kể cả `IOException` trơn** | chưa biết | mã 1, cần người xem, **không retry** |

Tạm thời là **danh sách cho phép** (allowlist), không phải "mọi `IOException` trừ vài loại". Phiên bản đầu làm ngược lại và reviewer chỉ ra ba ca sai thật:
`IllegalArgumentException` bọc `IOException` bị coi là tạm thời; `ZipException("invalid header")` (file nén/Parquet hỏng) bị retry; `FileAlreadyExistsException`
bị lời giải xếp tạm thời trong khi job xếp vĩnh viễn. `TransientStorageException` là lớp lab tự định nghĩa: adapter storage ném nó khi nhận mã lỗi mà nhà cung cấp
tài liệu hóa là tạm thời (HTTP 503 SlowDown, throttling) — nơi duy nhất biết điều đó.

**Retry đúng phạm vi.** Một lần thử = đọc + gate + tính + ghi + verify (gate cũng đọc storage, nên cũng được retry). Log "thành công" và mọi việc **sau** khi dữ liệu
đã ghi (manifest, metric) nằm **ngoài** vòng retry: reviewer tiêm lỗi vào sink log đúng lúc `pipeline_succeeded` và phiên bản đầu chạy lại **toàn bộ** pipeline
(`effects=2 attempts=2`). Giờ lỗi đó là lỗi finalization → mã 1, pipeline chạy đúng một lần (test). **Retry an toàn vì output idempotent** (snapshot + dọn
partition mồ côi, Chương 24). Không có idempotency thì retry job = ghi đôi. Test: tiêm 2 lỗi tạm thời → lần 3 thành công, manifest ghi `"attempts":3`; tiêm 2 lỗi
với `max-attempts=2` → mã 5; customers trùng khóa → chạy **đúng 1 lần**, mã 3. Production thêm **jitter** vào backoff (lời giải bài 2)
để trăm job lỗi cùng lúc không cùng thử lại cùng lúc.

### 3.4. Quality gate, mã thoát, manifest

**Gate theo lô của ngày chạy.** Gate xét lô `ingest_date = run-date`: lô cũ từng tệ không được chặn mọi lần chạy sau. Không có lô cho ngày chạy = dữ liệu **chưa đến**
→ lỗi, không phải "thành công với 0 dòng". Gate chạy **trước** khi ghi: bị chặn thì không có output nào (test kiểm tra thư mục không tồn tại). Output thật với ngưỡng
mặc định 5%:

```
{"level":"ERROR","run_id":"demo-gate","event":"quality_gate_blocked","reason":"lô 2026-10-16: 1/6 dòng bị loại (16.7%) > ngưỡng 5.0%"}
{"level":"INFO","run_id":"demo-gate","event":"job_finished","status":"DATA_REJECTED","exit_code":3,"duration_ms":6846,"attempts":1,"execution_id":"2026-10-17T040632522770Z-3f49873b"}
```

**Mã thoát** là giao diện giữa job và scheduler (Airflow, cron, Kubernetes Job):

| Mã | Ý nghĩa | Scheduler nên |
|---|---|---|
| 0 | thành công | chạy bước sau |
| 1 | lỗi bất ngờ, hoặc ghi manifest/metric thất bại (dữ liệu có thể đã ghi) | báo người |
| 2 | cấu hình sai | báo người; chạy lại vô ích |
| 3 | dữ liệu bị từ chối (gate, khóa trùng, thiếu lô) | báo bên gửi dữ liệu |
| 4 | đối soát thất bại | báo người; không publish |
| 5 | hết lượt retry lỗi tạm thời | có thể chạy lại sau |

**Manifest** — một file JSON cho **mỗi lần thực thi** trong `output/_runs/<run_id>--<execution_id>.json`, ghi nguyên tử (file tạm tên duy nhất + `ATOMIC_MOVE`,
**không** ghi đè): `run_id`, `execution_id`, trạng thái, mã thoát, số lần thử, thời lượng, **toàn bộ cấu hình**, phiên bản Spark/Scala/Java, số đếm đối soát,
metric vật lý và cờ `metrics_complete`. Trả lời được "dữ liệu ngày 16 do lần chạy nào tạo ra, với ngưỡng nào?" — nền của data lineage (Chương 31). Phiên bản
đầu đặt tên file theo `run_id` và `REPLACE_EXISTING`: chạy lại cùng `run_id` bị gate chặn **xóa mất** manifest SUCCEEDED trước đó — lịch sử thay đổi theo ý người
gõ lệnh. Output thật (rút gọn):

```
{"run_id":"demo-2026-10-16","execution_id":"2026-10-17T040558052457700Z-6157be60","status":"SUCCEEDED","exit_code":0,"attempts":1,"duration_ms":18516,
 "config":{"max_reject_rate":0.2,"shuffle_partitions":4,"run_date":"2026-10-16",...,"max_attempts":3,...},
 "versions":{"spark":"4.2.0","scala":"2.13.18","java":"21.0.12.1"},
 "details":{"input_rows":13,"removed_partitions":[],"fact_lines":6,"gate_rejected":1,"rejected_rows":3,"paid_revenue_vnd":21900000,"failed_checks":[],...},
 "metrics_complete":true,
 "spark_physical_metrics":{"spark_records_written":15,"spark_bytes_written":18367,"spark_tasks_failed":0,"spark_stages_completed":77,"spark_tasks_succeeded":129,...}}
```

**Lỗi khi ghi bằng chứng.** Ghi manifest/metric có thể hỏng **sau** khi dữ liệu đã ghi xong (đĩa đầy, thư mục sai quyền). Phiên bản đầu để exception thoát ra khỏi
`Job.run` — không có mã thoát theo bảng, không có `job_finished`. Review vòng 2 còn tìm ra hai lỗi thứ tự: sink log hỏng **trước** `metrics.close()` để lại
listener và job group trên SparkContext; và manifest ghi SUCCEEDED/`exit_code: 0` **trước** bước ghi file metric — bước sau hỏng thì job trả 1 nhưng bằng chứng
nói 0. Thứ tự hiện tại: `metrics.close()` chạy **đầu tiên** và luôn gỡ listener + job group; log và file metric mỗi bước đi qua `step(...)` riêng, gom lỗi; manifest
ghi **cuối cùng** với mã thoát **thật** và danh sách `finalization_errors`. Có bất kỳ lỗi nào → log `finalization_failed`, mã **1** — và manifest nói đúng như
vậy (test: `_metrics` là file → manifest `"status":"SUCCEEDED"`, `"exit_code":1`, `finalization_errors` có `metrics_file`). Ghi
manifest dùng `java.nio` — chỉ cho filesystem cục bộ/chia sẻ; output trên HDFS/S3 cần Hadoop `FileSystem` API và ngữ nghĩa rename của storage đó (rename trên S3
không nguyên tử).

### 3.5. Checkpointing

**Vấn đề.** Vòng lặp (cập nhật trạng thái theo từng ngày, thuật toán lặp) làm plan dài thêm mỗi vòng. Thí nghiệm thật: 60 vòng `withColumn` + `filter`:

| | Số node plan logic | Thời gian lập plan | Kết quả |
|---|---|---|---|
| Không checkpoint | **122** | 63 ms | 559500 |
| `checkpoint()` mỗi 10 vòng | **1** | 1 ms | 559500 |

Plan dài → Catalyst phân tích/tối ưu lâu hơn mỗi vòng (tăng theo vòng, có thể thành phút với hàng trăm vòng), và mất một partition thì phải tính lại cả chuỗi.

**Cách hoạt động.** `df.checkpoint(eager = true)` ghi dữ liệu ra `setCheckpointDir` (lưu trữ **bền**: HDFS/S3 trên cluster) và trả DataFrame mới mà plan **bắt đầu từ
dữ liệu đã ghi**. `localCheckpoint()` cắt plan nhưng lưu ở bộ nhớ/đĩa của executor — mất executor là mất dữ liệu (lời giải bài 6: cả hai cắt plan còn 1 node; chỉ
checkpoint tạo file). `cache()` **không** cắt lineage.

**Khi nào không dùng:** pipeline tuyến tính như Chương 24 (plan ngắn, không vòng lặp) — checkpoint chỉ thêm một lượt ghi. "Checkpoint" của **Structured Streaming**
(lưu offset + state) là khái niệm khác, cùng tên (Chương 29–30).

### 3.6. Kiểm thử job

| Mức | Thứ được test | Ở lab |
|---|---|---|
| Unit | Hàm biến đổi, quy tắc, phân loại lỗi | Chương 24 `PipelineSuite`; `isTransient`, `JobConfig.parse`, `prometheus` |
| Job / integration | **Đúng điểm vào** `Job.run(args)` như production, dữ liệu thật trên đĩa | `JobSuite`: mã thoát, output, manifest, metric, chuỗi sự kiện log |
| Triển khai | Jar thật trên `spark-submit` thật | mục 9 (chạy tay, không trong unit test) |

Integration test kiểm **hành vi vận hành**: gate chặn thì **không có** output; retry đếm đúng; chạy lại cùng ngày cho cùng bảng; log là JSON hợp lệ có `run_id` với
đúng chuỗi sự kiện `job_started → quality_gate_passed → pipeline_succeeded → job_finished`. `Job.run` nhận `SparkSession` từ ngoài (test dùng chung một session,
`main` tự tạo và dừng) và `faults` để tiêm lỗi — thiết kế cho test được ngay từ đầu.

### 3.7. Triển khai: fat jar và spark-submit

**Spark là `Provided`.** Cụm (hay bản phân phối Spark) đã có Spark + Scala; jar của job chỉ chứa code của ta và dependency **không** có sẵn:

```scala
"org.apache.spark" %% "spark-sql" % sparkV % Provided
assembly / assemblyOption ~= (_.withIncludeScala(false))   // Spark đã có scala-library 2.13.18
```

Số đo thật: jar ban đầu (quên loại Scala) **6.246.223 byte**; loại `scala-library` → **110.004 byte** (bản đầu; sau hai vòng sửa theo review chéo, code nhiều hơn → **136.974 byte**). Đóng Spark/Scala vào jar không chỉ làm jar to: hai bản khác phiên
bản trên classpath là nguồn `NoSuchMethodError` khó hiểu nhất. `Provided` không có trên classpath của `sbt run` → build thêm lại (`Defaults.runTask(Compile /
fullClasspath, ...)`).

**`master` không viết cứng.** `Job.session()` chỉ đặt `local[2]` khi **không** có `spark.master` (chạy trực tiếp); dưới `spark-submit`, `--master` quyết định (local,
`yarn`, `k8s://...`). Cùng một jar chạy được ở mọi nơi.

**spark-submit thật** (`deploy/submit.sh`, WSL2 Ubuntu 24.04, Spark 4.2.0 tải từ `dlcdn.apache.org`, SHA-512 khớp file `.sha512` chính thức):

```bash
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --conf spark.ui.enabled=false \
  --class vn.scalaeng.sparkprod.Job target/scala-2.13/spark-production-job.jar \
  --input=../batch-pipeline-lab/data/input --customers=../batch-pipeline-lab/data/customers.csv --output="$OUT" --run-date=2026-10-16 --max-reject-rate=0.2
```

→ chuỗi log `job_started … job_finished` như mục 3.2, **`exit=0`**; chạy với ngưỡng mặc định → **`exit=3`**. Mã thoát của job đến được shell/scheduler.

**Windows: `spark-submit` cần `winutils.exe`.** Cùng lệnh trên Windows lỗi khi khởi tạo `SparkContext` (mục 10). `BareLocalFileSystem` của các lab không giúp được ở
đây: khi tạo executor, Spark **tải jar ứng dụng** và gọi `FileUtil.chmod` của Hadoop — đường đó luôn dùng `winutils`. Trên Windows: chạy job trong **WSL2/Linux/
container**, hoặc cài `winutils` khớp phiên bản Hadoop (bản dựng cộng đồng, không do Apache phát hành). Cluster production gần như luôn là Linux.

### 3.8. Spark 4.2 và Scala 3: phân biệt rõ hai thế giới

Giáo trình dùng **Scala 3.9.0** cho backend và **Scala 2.13.18** cho Spark. Đây không phải sở thích:

- Apache Spark 4.2.0 chỉ phát hành artifact **`_2.13`** (không có `spark-sql_3`, tra Maven Central 17/10/2026). Tài liệu Spark: ứng dụng phải dùng **cùng phiên bản
  Scala** mà Spark được biên dịch. Hỗ trợ Scala 3 chính thức chưa có.
- Scala 3 *có thể* gọi thư viện 2.13 (TASTy/binary compat của stdlib 2.13). Thí nghiệm thật (`scala3-spark-probe/`, DataFrame API, gọi
  `SparkSession.builder()` — điểm vào chuẩn mà tài liệu Spark hướng dẫn):

| Scala | `scala-library` trên classpath | `SparkSession.builder()` |
|---|---|---|
| 3.3.8 (LTS) | 2.13.18 | chạy: `0=334, 1=333, 2=333` |
| 3.7.3 | 2.13.18 | chạy |
| 3.8.4 | **3.8.4** | `IllegalStateException: Cannot find a SparkSession implementation on the Classpath.` |
| 3.9.0 | **3.9.0** | như 3.8.4 |

  Từ Scala 3.8, thư viện chuẩn là `scala-library` **3.x** (biên dịch bằng Scala 3) thay cho bản 2.13 mà Spark được build cùng. Thông báo "không tìm thấy
  implementation" **che** lỗi thật: `SparkSession.builder()` dùng **reflection của Scala 2** (`scala-reflect`) để tìm companion của lớp
  `org.apache.spark.sql.classic.SparkSession`, bọc trong `Try` rồi thay bằng thông báo chung. Gọi trực tiếp đoạn reflection đó trên 3.9.0 (review chéo, tác giả
  chạy lại độc lập) cho lỗi gốc: `scala.reflect.internal.FatalError: class Array does not have a member apply` — `scala-reflect` 2.13 không đọc được stdlib 3.x.
- **Nhưng đó là lỗi của một điểm vào, không phải của mọi API.** Bỏ qua builder chung và gọi thẳng `org.apache.spark.sql.classic.SparkSession.builder()`, cùng
  Scala **3.9.0** + Spark 4.2.0 chạy được DataFrame, `import spark.implicits.*` + `Seq(1L, 2L).toDS()` và `.as[java.lang.Long](using Encoders.LONG)` (output thật:
  `toDS=List(1, 2) asLong=List(0, 1, 2) grouped=List([0,334], [1,333], [2,333])`). Encoder cho **kiểu nguyên thủy/tường minh** không cần `TypeTag`.
- Encoder **case class tự động** thì không có ở **mọi** phiên bản Scala 3: nó cần `TypeTag` của Scala 2 (lỗi thật, Scala 3.9.0:
  `[E008] value toDS is not a member of Seq[Order]`). Thư viện cộng đồng bù phần này; không được Apache hỗ trợ.
- Kết luận cho production: không có tổ hợp Scala 3 + Spark 4.2 nào được Apache hỗ trợ (tài liệu Spark 4.2: Scala 2.13). Đường "gọi thẳng lớp `classic`" là
  phát hiện thí nghiệm, phụ thuộc chi tiết nội bộ có thể đổi ở bản vá sau — **không** dùng nó làm nền cho job thật.

**Phương án thực tế:** job Spark là **build riêng Scala 2.13** (như mọi lab 09-spark); logic nghiệp vụ dùng chung giữa backend Scala 3 và job Spark thì đặt ở dạng
**dữ liệu/hợp đồng** (schema Avro/JSON, Chương 21) hoặc module cross-build 2.13 + 3 không phụ thuộc Spark. **Không** ép Scala 3 vào Spark trong production.

| Thành phần | Phiên bản | Ràng buộc |
|---|---|---|
| Spark | 4.2.0 | chỉ `_2.13` |
| Scala cho job Spark | 2.13.18 | trùng bản trong `spark-4.2.0-bin-hadoop3/jars` |
| JDK | 21 | Spark 4.2: Java 17/21/25 |
| Scala backend | 3.9.0 | không trộn vào classpath của job Spark |

Lời giải bài 5 kiểm tra các ràng buộc này **lúc khởi động** job, dừng sớm với thông báo rõ thay vì `NoSuchMethodError` giữa chừng.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Structured logging | Log có cấu trúc (JSON), lọc theo trường |
| Run ID, run manifest | Định danh một lần chạy / bản ghi mô tả lần chạy |
| Exit code | Mã thoát tiến trình, giao diện với scheduler |
| Transient / permanent failure | Lỗi tạm thời / vĩnh viễn |
| Exponential backoff, jitter | Chờ tăng lũy thừa giữa các lần thử / ngẫu nhiên hóa thời gian chờ |
| Pushgateway, textfile collector | Cách đưa metric của job ngắn hạn vào Prometheus |
| Checkpoint / local checkpoint | Ghi ra lưu trữ bền để cắt lineage / cắt lineage, lưu ở executor |
| Fat (uber) jar, `Provided` | Jar gồm dependency / phụ thuộc do môi trường chạy cung cấp |
| `spark-submit`, driver class path | Công cụ gửi job / classpath của tiến trình driver |
| Backfill | Chạy lại job cho ngày quá khứ |

## 5. Ví dụ đơn giản

```scala
val spark = Job.session()
// cwd = 09-spark/spark-production-lab; dữ liệu mẫu nằm ở lab Chương 24
val code  = Job.run(spark, Seq("--input=../batch-pipeline-lab/data/input", "--customers=../batch-pipeline-lab/data/customers.csv",
  "--output=/tmp/out", "--run-date=2026-10-16", "--max-reject-rate=0.2"), sys.env, println)
// code == 0; /tmp/out/_runs/<run_id>--<execution_id>.json mô tả lần chạy; /tmp/out/_metrics/ch26_orders.prom là trạng thái hiện tại
```

Một lời gọi chứa cả vòng đời: cấu hình → gate → pipeline → đối soát → manifest → mã thoát.

## 6. Ví dụ thực tế — một đêm của job

| Giờ | Sự kiện | Job làm gì | Mã |
|---|---|---|---|
| 02:00 | Lô 16/10 về đủ | gate qua, ghi, đối soát OK, manifest SUCCEEDED | 0 |
| 02:00 (giả sử) | Storage chập chờn 2 lần | log `attempt_failed_transient` ×2, lần 3 thành công, manifest `attempts: 3` | 0 |
| 02:00 (giả sử) | Bên gửi gửi file 1/6 dòng hỏng, ngưỡng 5% | `quality_gate_blocked`, **không ghi gì**, báo bên gửi | 3 |
| 02:00 (giả sử) | `customers.csv` có hai dòng C1 | `data_contract_violation`, không retry | 3 |
| 02:00 (giả sử) | Collector log mất kết nối đúng lúc ghi `pipeline_succeeded` | **không** chạy lại pipeline; log `finalization_failed` | 1 |
| 09:00 | Bên gửi sửa file, chạy lại cùng `run-date` | cùng output như chưa từng lỗi (idempotent), manifest mới, manifest cũ còn nguyên | 0 |

Mỗi dòng của bảng là một test trong `JobSuite`.

## 7. Code đầy đủ

```
09-spark/spark-production-lab/            ← BUILD RIÊNG, Scala 2.13.18; dùng lại src của batch-pipeline-lab
├── project.scala / build.sbt (unmanagedSourceDirectories → ../batch-pipeline-lab, Spark Provided, assembly)
├── project/build.properties, project/plugins.sbt (sbt-assembly 2.3.1)
├── deploy/submit.sh                       ← spark-submit trên Linux/WSL2
├── src/main/scala/vn/scalaeng/sparkprod/
│   ├── JobConfig.scala      ← parse + kiểm tra, gom lỗi
│   ├── Observability.scala  ← Json, JobLog (JSON lines), JobMetrics (SparkListener + rào chắn, Prometheus text)
│   ├── Job.scala            ← ExitCode, Failures (phân loại lỗi), gate, retry, finalization, manifest, session, launch/main
│   └── Checkpointing.scala  ← thí nghiệm cắt lineage
└── src/test/scala/vn/scalaeng/sparkprod/JobSuite.scala   ← 12 test (3 regression từ review chéo)
09-spark/scala3-spark-probe/               ← thí nghiệm Scala 3 + Spark (mục 3.8), ngoài lab 2.13
```

```bash
cd 09-spark/spark-production-lab
scala-cli test .                          # hoặc: sbt test
sbt assembly                              # → target/scala-2.13/spark-production-job.jar (136 KB)
OUT=/tmp/ch26 deploy/submit.sh --run-date=2026-10-16 --max-reject-rate=0.2   # Linux/WSL2, cần SPARK_HOME
```

## 8. Giải thích code

- **`JobConfig.parse`** — `args.partition(...)` tách tham số đúng/sai dạng; mỗi trường là một `Either[String, A]`; `collect { case Left(e) => e }` gom lỗi.
- **`JobLog`** — nhận `sink: String => Unit` (production: `println`; test: buffer) — log test được mà không bắt stdout.
- **`JobMetrics`** — `LongAdder` (cộng từ thread listener, đọc từ driver); `stageIds` lấy từ `onJobStart` của đúng group; `close()` chạy job rào chắn trên group
  riêng rồi `await` có hạn.
- **`Job.launch` / `Job.run`** — `launch` parse trước, tạo session sau; `run` là cùng đường đó với session có sẵn (cho test). `JobMetrics.start` đặt job group cho
  thread hiện tại: **mọi** action trong `Pipeline.run` (kể cả `collect` kiểm tra khóa trùng) thuộc lần chạy này. `attempt(n)` đệ quy, catch có guard
  `if isTransient(e) && n < maxAttempts`; lỗi không khớp guard rơi ra `catch` ngoài, `Failures.classify` quyết định mã 5/3/1. Kết quả là `Outcome` chứa sự kiện
  log cuối — log **sau** vòng retry. Finalization: đóng metrics trước, rồi log, `.prom`, cuối cùng manifest mang mã thoát thật + `finalization_errors`; hỏng bất
  kỳ bước nào thì mã 1.
- **`writeAtomically`** — file tạm tên duy nhất (`Files.createTempFile` cùng thư mục) rồi `Files.move(..., ATOMIC_MOVE)`; manifest **không** `REPLACE_EXISTING`
  (đích tồn tại là lỗi), file `.prom` thì có (trạng thái hiện tại).
- **`Job.session`** — `sys.props.contains("spark.master")`: `spark-submit` truyền master qua system property; cấu hình `fs.file.impl` chỉ đặt trên Windows.

## 9. Output mong đợi (output thật)

Log, manifest, metric: mục 3.2 và 3.4. Checkpoint (test):

```
[checkpoint] không: Run(60,122,63,559500) | mỗi 10 vòng: Run(60,1,1,559500)
```

`sbt test assembly`:

```
[info] Passed: Total 13, Failed 0, Errors 0, Passed 13
[info] Built: ...\spark-production-lab\target\scala-2.13\spark-production-job.jar
```

`spark-submit` trên WSL2 (lần 1 ngưỡng 0,2; lần 2 ngưỡng mặc định; lần 3 **không** tham số nào — chạy lại 17/10/2026 với jar sau review vòng 2):

```
{"ts":"2026-10-17T08:29:31.049978501Z","level":"INFO","run_id":"submit-1","event":"job_started","execution_id":"2026-10-17T082930871281028Z-54a5dd01",...,"output":"/tmp/tmp.jYO35l024M","spark":"4.2.0","scala":"2.13.18","java":"21.0.12.1"}
{"ts":"2026-10-17T08:29:40.045208207Z","level":"INFO","run_id":"submit-1","event":"quality_gate_passed","rows":6,"rejected":1}
{"ts":"2026-10-17T08:29:55.031648872Z","level":"INFO","run_id":"submit-1","event":"pipeline_succeeded","input_rows":13,"removed_partitions":[],"fact_lines":6,"gate_rejected":1,"rejected_rows":3,"paid_revenue_vnd":21900000,"failed_checks":[],"conflict_rows":2,"gate_rows":6}
{"ts":"2026-10-17T08:29:55.042529913Z","level":"INFO","run_id":"submit-1","event":"job_finished","status":"SUCCEEDED","exit_code":0,"duration_ms":24157,"attempts":1,"execution_id":"2026-10-17T082930871281028Z-54a5dd01"}
exit=0
{"ts":"2026-10-17T08:30:08.833677139Z","level":"ERROR","run_id":"submit-gate","event":"quality_gate_blocked","reason":"lô 2026-10-16: 1/6 dòng bị loại (16.7%) > ngưỡng 5.0%"}
{"ts":"2026-10-17T08:30:08.890297945Z","level":"INFO","run_id":"submit-gate","event":"job_finished","status":"DATA_REJECTED","exit_code":3,"duration_ms":6588,"attempts":1,"execution_id":"2026-10-17T083002290938340Z-73bea28f"}
exit=3
noargs exit=2
{"ts":"2026-10-17T08:30:11.212216464Z","level":"ERROR","run_id":"-","event":"config_invalid","errors":["thiếu --input (hoặc CH26_INPUT)","thiếu --customers (hoặc CH26_CUSTOMERS)","thiếu --output (hoặc CH26_OUTPUT)","thiếu --run-date"]}
dòng log chứa SparkContext khi thiếu tham số: 0
/tmp/tmp.jYO35l024M: _metrics _runs conflicts daily_revenue fact_order_lines rejected
_runs: submit-1--2026-10-17T082930871281028Z-54a5dd01.json submit-gate--2026-10-17T083002290938340Z-73bea28f.json
_metrics: ch26_orders.prom
.../submit-1--...54a5dd01.json:"status":"SUCCEEDED","exit_code":0
.../submit-gate--...73bea28f.json:"status":"DATA_REJECTED","exit_code":3
.../submit-1--...54a5dd01.json:"finalization_errors":[]
.../submit-gate--...73bea28f.json:"finalization_errors":[]
```

Lần 3 là bằng chứng cho mục 3.1 trên đúng công cụ triển khai: `spark-submit` không tham số trả **2** và không khởi tạo `SparkContext` (phiên bản đầu khởi tạo Spark
trước, trên Windows chết ở `FileUtil.chmod` với mã 1 trước khi kịp báo thiếu tham số).

**Test:** `JobSuite 13/13` (`scala-cli test .` và `sbt test`); lời giải `Ch26SolutionsSuite 6/6`.

## 10. Lỗi thường gặp (thật)

**1. `spark-submit` trên Windows không có winutils** — job chưa kịp chạy dòng code nào:

```
ERROR SparkContext: Error initializing SparkContext.
java.lang.RuntimeException: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
  at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:790)
  at org.apache.hadoop.fs.FileUtil.chmod(FileUtil.java:1291)
  at org.apache.spark.util.Utils$.fetchFile(Utils.scala:432)
  at org.apache.spark.executor.Executor.updateDependencies(Executor.scala:1480)
  ...
  at org.apache.spark.SparkContext.<init>(SparkContext.scala:629)
```

Thêm `--conf spark.hadoop.fs.file.impl=...BareLocalFileSystem --driver-class-path <jar>` vẫn lỗi y hệt: stack trace cho thấy lỗi ở `Executor.updateDependencies`
→ `FileUtil.chmod`, không qua `FileSystem`. Đọc stack trace trước khi thử cấu hình tiếp. Cách xử lý: mục 3.7.

**2. Scala 3.8+ với Spark:** `java.lang.IllegalStateException: Cannot find a SparkSession implementation on the Classpath.` — lỗi gốc bị che là
`scala.reflect.internal.FatalError: class Array does not have a member apply` (mục 3.8). Đừng dừng ở thông báo bề mặt: khi một thư viện bọc lỗi trong `Try` rồi
thay bằng thông báo chung, gọi lại chính đoạn mã đó để thấy nguyên nhân.

**3. Encoder case class trong Scala 3:** `[E008] value toDS is not a member of Seq[Order]` — mục 3.8.

**4. `/*` trong Scaladoc mở comment lồng nhau.** Viết đường dẫn `_runs/*.json` trong `/** ... */`:

```
Ch26Solutions.scala:48: error: unclosed comment
```

Scala (khác Java) cho phép comment lồng nhau: `/*` bên trong mở một comment mới chưa đóng. Viết khác đi (`các file .json trong _runs`).

**5. Mã thoát bị wrapper "nuốt".** Chạy job qua `scala-cli run` với cổng gate bị chặn: log ghi `exit_code: 3` nhưng tiến trình `scala-cli` trả **1**. Qua `spark-submit`
trên Linux: **3**. Luôn kiểm tra mã thoát qua **đúng** công cụ chạy production.

**6. Quên loại Scala khỏi fat jar** — 6,2 MB thay vì 136 KB và hai `scala-library` trên classpath (mục 3.7).

**7. Kiểm tra pass nhưng hợp đồng vận hành sai** — không có exception nào, chỉ test tấn công của review chéo mới thấy: retry lại toàn bộ pipeline vì sink log
lỗi **sau** khi dữ liệu đã ghi; nuốt lỗi metric rồi báo SUCCEEDED; manifest bị ghi đè khi chạy lại cùng `run_id`; nhiều file `.prom` cùng series. Mỗi lỗi giờ có
một test trong `JobSuite` (mục 3.2–3.4).

**8. Windows: `Unable to establish loopback connection` khi chạy nhiều truy vấn** — cô lập artifact theo session của Spark 4 làm executor tải class qua RPC, mỗi lần
một cặp socket loopback. `Job.session()` đặt `spark.sql.artifact.isolation.enabled=false`. Chi tiết và số đo: Chương 24 mục 10.

## 11. Cách debug

- **Job lỗi đêm qua:** tìm manifest theo `run_date` trong `_runs/` → `status`, `details`, `config`; lọc log theo `run_id`.
- **Job "thành công" nhưng số sai:** so `config` của manifest (ngưỡng, input) với lần chạy đúng; `details` cho số đếm từng ngăn.
- **Retry liên tục:** log `attempt_failed_transient` có `error` — lỗi đó có thật sự tạm thời? Lỗi lặp y hệt là vĩnh viễn → sửa `Failures.classify`.
- **Mã 1 dù dữ liệu đúng:** tìm `finalization_failed` hoặc `metrics_incomplete` — dữ liệu có thể đã ghi; kiểm tra quyền/dung lượng thư mục `_runs`, `_metrics`.
- **Lỗi lúc khởi động trên cụm:** đọc stack trace tới frame đầu tiên của Spark/Hadoop (`Executor.updateDependencies`, `SparkContext.<init>`); kiểm tra phiên bản
  Spark/Scala/Java (lời giải bài 5); kiểm tra jar có chứa Spark/Scala không (`unzip -l`).
- **Metric = 0:** action có chạy trên **cùng thread** đã `setJobGroup` không?

## 12. Bài tập

1. **(Cơ bản)** Phân loại lỗi thành Transient/Permanent/Data/Unknown qua toàn bộ chuỗi cause; test với `SparkException` bọc `SocketTimeoutException`.
2. **(Cơ bản)** Backoff lũy thừa có trần và full jitter; không tràn `Long` ở lần thử 64; xác định được với seed.
3. **(Trung bình)** Từ thư mục manifest, liệt kê các ngày trong khoảng **chưa có** lần chạy SUCCEEDED (lần lỗi không tính).
4. **(Trung bình)** Kiểm tra khối lượng: cảnh báo khi số dòng hôm nay < X% trung vị các lần chạy trước; chưa đủ lịch sử thì im lặng có chủ đích.
5. **(Nâng cao)** Kiểm tra tương thích Spark/Scala/Java lúc khởi động; dừng sớm với thông báo rõ.
6. **(Nâng cao)** So `checkpoint` và `localCheckpoint`: số node plan, file trong thư mục checkpoint, kết quả.

## 13. Đáp án và lời giải

- Giải thích: [`solutions/ch26-solutions.md`](../solutions/ch26-solutions.md)
- Code + test (6 test): [`solutions/ch26-code/`](../solutions/ch26-code/)

## 14. Câu hỏi kiểm tra

1. Vì sao đường dẫn input/output không có giá trị mặc định?
2. Vì sao `run-date` là tham số chứ không phải ngày hôm nay?
3. Lỗi nào nên retry ở mức job? Vì sao retry an toàn ở lab này?
4. Gate xét lô nào? Vì sao không có lô cho ngày chạy là lỗi?
5. Manifest trả lời được những câu hỏi nào?
6. Khi nào checkpoint có ích? `cache` có cắt lineage không?
7. Vì sao Spark là `Provided` và loại Scala khỏi fat jar?
8. Vì sao giáo trình không viết job Spark bằng Scala 3.9?

<details><summary>Gợi ý đáp án</summary>

1. Quên tham số phải là lỗi to, không phải đọc/ghi nhầm chỗ một cách "thành công".
2. Backfill/chạy lại phải cho kết quả của ngày đó, không phụ thuộc lúc chạy.
3. Lỗi nằm trong danh sách cho phép (timeout, mất kết nối, mã lỗi tạm thời của storage) — không phải mọi `IOException`; vì output idempotent (snapshot,
   Chương 24) và việc sau commit nằm ngoài vòng retry.
4. Lô `ingest_date = run-date`; dữ liệu chưa đến không được trông như thành công rỗng.
5. Lần chạy nào, cấu hình gì, phiên bản gì, bao nhiêu dòng mỗi ngăn, metric, kết quả.
6. Vòng lặp làm plan dài; không — cache giữ lineage.
7. Cụm đã có; hai bản khác phiên bản trên classpath gây lỗi nhị phân; jar nhỏ.
8. Spark 4.2 chỉ được hỗ trợ trên Scala 2.13; từ Scala 3.8 builder chuẩn hỏng (reflection Scala 2 không đọc được stdlib 3.x); encoder case class không có ở
   Scala 3; đường vòng qua lớp `classic` chạy được nhưng không được hỗ trợ.
</details>

## 15. Mini project

Viết **DAG** (Airflow hoặc chỉ một script bash) chạy job cho 3 ngày liên tiếp (backfill), dừng ở mã 2/3/4, chạy lại tối đa 2 lần ở mã 5. Sau đó dùng lời giải bài 3 để in
ngày nào còn thiếu. Ghi mọi quyết định "mã nào → làm gì" thành bảng trong README. **Project 3** đóng gói pipeline + job này thành ứng dụng hoàn chỉnh.

## 16. Nguồn tài liệu chính thức

- Spark 4.2.0 overview (Scala 2.13, Java 17/21/25): https://spark.apache.org/docs/4.2.0/
- Submitting applications (`spark-submit`, `--master`, fat jar, `provided`): https://spark.apache.org/docs/4.2.0/submitting-applications.html
- Configuration (`spark.task.maxFailures`, `spark.hadoop.*`): https://spark.apache.org/docs/4.2.0/configuration.html
- Monitoring (listener, metrics): https://spark.apache.org/docs/4.2.0/monitoring.html
- `Dataset.checkpoint` / `localCheckpoint`: https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html
- Bản phân phối + SHA-512: https://dlcdn.apache.org/spark/spark-4.2.0/
- sbt-assembly 2.3.1: https://github.com/sbt/sbt-assembly
- Prometheus Pushgateway / textfile collector: https://prometheus.io/docs/practices/pushing/
- Hadoop trên Windows (winutils): https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems

## 17. Checklist tự đánh giá

- [ ] Cấu hình job không có mặc định nguy hiểm, gom mọi lỗi, `run-date` là tham số.
- [ ] Log JSON có `run_id`; metric đúng phạm vi lần chạy; đẩy được vào Prometheus.
- [ ] Phân loại lỗi đúng; retry chỉ lỗi tạm thời và chỉ khi output idempotent.
- [ ] Mã thoát có nghĩa; manifest mỗi lần chạy, ghi nguyên tử.
- [ ] Biết khi nào checkpoint; phân biệt checkpoint/localCheckpoint/cache.
- [ ] Integration test gọi đúng điểm vào của job.
- [ ] Build fat jar với Spark `Provided`, chạy `spark-submit`; biết bẫy winutils.
- [ ] Giải thích được ma trận Spark/Scala/JDK và vì sao không dùng Scala 3 cho job Spark.
