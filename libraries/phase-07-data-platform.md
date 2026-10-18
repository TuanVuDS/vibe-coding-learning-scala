# Thư viện — Phase 7: Data platform, quality và performance

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| Dagster | 1.13.26 | Python, ngoài JVM | Điều phối asset/partition | Ch31, P5 |
| Circe parser / generic | 0.14.17 | `_3` | JSON/lineage/contract | Ch31/33, Ch34 (parser) |
| Spark SQL | 4.2.0 | **Chỉ `_2.13`** | Thực thi phân tán | Ch32 |
| BareLocalFileSystem | 0.1.0 | Java | File local Windows | Ch32 |
| munit | 1.2.4 | `_3` / `_2.13` riêng | Unit test | Ch31–34 |
| munit-scalacheck | 1.2.0 | `_3` | Property/contract | Ch33 |
| Avro | 1.12.2 | Java | Benchmark serialization | Ch34 |
| slf4j-nop | 2.0.17 | Java | Giảm log lab benchmark | Ch34 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

## Thư viện dùng lại theo lab

- **Circe parser 0.14.17** — [phase 4](phase-04-backend.md#circe); architecture/quality/cost dùng để đọc JSON thay vì tự parse chuỗi.
- **Circe generic 0.14.17** — [phase 6](phase-06-streaming.md#circe-generic); architecture-lab derive codec lineage/metadata.
- **Spark SQL 4.2.0 (`spark-sql_2.13`)** — [phase 5](phase-05-data-spark.md#spark); distributed-lab giữ Scala **2.13.18**, không đưa artifact `_3` của quality-lab vào cùng build.
- **hadoop-bare-naked-local-fs 0.1.0** — [phase 5](phase-05-data-spark.md#bare-local-fs); cấu hình local Windows của distributed-lab.
- **Avro 1.12.2** — [phase 5](phase-05-data-spark.md#avro); cost-lab đo encode/decode, không thêm một engine phân tán.
- **slf4j-nop 2.0.17** — [phase 5](phase-05-data-spark.md#slf4j-nop); giữ đúng bản cost-lab, khác 2.0.20 ở Kafka.
- **munit 1.2.4 (`_3` hoặc `_2.13` theo build)** — [tooling](tooling.md#munit); assertion/golden độc lập với pipeline.
- **munit-scalacheck 1.2.0** — [tooling](tooling.md#scalacheck); quality-lab có generator cho contract và dữ liệu, không thay quality gate runtime bằng property test.

Toàn bộ dependency Maven của phase đã xuất hiện ở phần trước; chỉ Dagster cần mô tả mới. [architecture build](../11-data-platform/architecture-lab/build.sbt), [distributed build](../11-data-platform/distributed-lab/build.sbt), [quality build](../11-data-platform/quality-lab/build.sbt), [cost build](../13-performance/cost-lab/build.sbt) cho thấy ranh giới thực tế. JMH được nhắc như lựa chọn benchmark trong chương nhưng **không có dependency/plugin JMH**; cost-lab dùng harness tự viết, không ghi số đo đó là JMH.

Bằng chứng đã có: [T18/Ch31](../.team/handoffs/T18-ch31-architecture.md), [T19/Ch32](../.team/handoffs/T19-ch32-distributed.md), [T17/Ch33](../.team/handoffs/T17-ch33-data-quality.md), [T20/Ch34](../.team/handoffs/T20-ch34-performance-cost.md). Đọc cả [review Ch31](../.team/reviews/T18-ch31-review.md) về snapshot/ABA và mã thoát wrapper.

<a id="dagster"></a>
## Dagster

### 1. Là gì / giải quyết vấn đề gì

Dagster là công cụ điều phối (orchestrator) Python, quyết định asset nào chạy, phụ thuộc nào phải có trước và ghi lại lần materialize. Data platform cần lịch sử partition/backfill và quan sát trạng thái chạy; một script tính đúng dữ liệu chưa giải quyết được toàn bộ các việc này. Trong giáo trình, Dagster gọi CLI Scala qua subprocess, còn logic transform/quality/publish giữ trong Scala.

### 2. Phiên bản trong giáo trình

**dagster 1.13.26**, package **Python**, không có artifact Maven hay binary version Scala. Nguồn chính xác: [PyPI 1.13.26](https://pypi.org/project/dagster/1.13.26/), [metadata JSON phiên bản](https://pypi.org/pypi/dagster/1.13.26/json), [GitHub release](https://github.com/dagster-io/dagster/releases/tag/1.13.26). Ngày đối chiếu **18/10/2026**: metadata PyPI trả HTTP 200, `version=1.13.26`, `requires_python=<3.15,>=3.10`, khớp Python **3.10–3.14** ở P5; không liên quan phiên bản JDK của job Scala.

Ch31/P5 có bằng chứng chạy Dagster thật trong handoff; **T27 không cài package, không materialize lại asset**. Đây là version giáo trình dùng, không khẳng định mới nhất.

### 3. Khái niệm cốt lõi

Asset là sản phẩm dữ liệu có tên; dependency/deps mô tả thứ tự, partition tách công việc theo miền như ngày. `@dg.asset`, `DailyPartitionsDefinition`, `MaterializeResult` và `Definitions` nối hàm Python với graph. Materialization là một lần tính/ghi kết quả; freshness của dữ liệu tận nguồn vẫn cần lineage/input hash và chính sách nghiệp vụ, không chỉ nhìn run xanh.

Wrapper dùng `subprocess.run(..., capture_output=True, encoding="utf-8")`, kiểm returncode rồi chuyển metadata JSON. Retry cấp run có thể gọi lại cả job, nên job Scala phải idempotent và giữ mã thoát thật. `end_offset=1` cùng `timezone="Asia/Ho_Chi_Minh"` là quyết định cho partition ngày đang diễn ra của lab, không phải mặc định nên áp cho mọi pipeline.

### 4. Ví dụ từ lab đã kiểm chứng

Dagster có nguồn Python, nên ví dụ dưới trích nguyên văn file điều phối đã chạy theo bằng chứng Ch31, không viết thêm code hay đưa Python vào compiler Scala.

Nguồn: [11-data-platform/architecture-lab/dagster/defs.py:45](../11-data-platform/architecture-lab/dagster/defs.py#L45). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 11-data-platform/architecture-lab/dagster/defs.py:45-55 -->
```python
@dg.asset(partitions_def=daily, code_version="v1", group_name="orders")
def staging_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_scala(context, "staging_orders")


@dg.asset(partitions_def=daily, deps=[staging_orders], code_version="v1", group_name="orders")
def daily_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_scala(context, "daily_revenue")


defs = dg.Definitions(assets=[staging_orders, daily_revenue])
```

Hai asset gọi cùng adapter `run_scala`; xem [hàm wrapper](../11-data-platform/architecture-lab/dagster/defs.py#L31) để hiểu mã thoát và metadata. Ví dụ Scala tương ứng được test riêng ở [PlatformSuite](../11-data-platform/architecture-lab/src/test/scala/vn/scalaeng/platform/PlatformSuite.scala), không dùng test munit để khẳng định package Python đã chạy.

### 5. Lựa chọn thay thế

cron phù hợp tác vụ đơn ít phụ thuộc, rẻ về vận hành nhưng thiếu mô hình asset/backfill. Airflow phù hợp đội đã chuẩn hóa DAG/task; Dagster làm rõ sản phẩm dữ liệu, partition và metadata. Chọn theo vận hành/quan sát cần có và kỹ năng đội; không chuyển logic parse, join hay quality vào orchestrator chỉ vì có thể viết Python.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “dagster._core.errors.DagsterUnknownPartitionError: Could not find a partition with key `2026-10-17`.” — [11-data-platform/ch31-data-pipeline-architecture.md:459](../11-data-platform/ch31-data-pipeline-architecture.md#L459). Mặc định chỉ có ngày đã kết thúc theo timezone partition. Đọc lịch nghiệp vụ trước khi thêm end_offset; kết quả hôm nay chưa đủ dữ liệu vẫn cần tính lại.

Trích nguyên văn mục 10: “java.lang.IllegalStateException: input của racy@2026-10-15 đổi trong lúc tính — chạy lại” — [11-data-platform/ch31-data-pipeline-architecture.md:510](../11-data-platform/ch31-data-pipeline-architecture.md#L510). Đây là lỗi của bản Scala trước review, không phải lỗi Dagster. So hash hai đầu không chặn được ABA; bản hiện tại tính/hash cùng snapshot byte. Orchestrator không tự sửa race bên trong job.

### 7. Dùng ở đâu trong giáo trình

[Ch31](../11-data-platform/ch31-data-pipeline-architecture.md), [defs.py Ch31](../11-data-platform/architecture-lab/dagster/defs.py), [Project 5](../14-projects/p5-data-platform/README.md), [defs.py P5](../14-projects/p5-data-platform/orchestration/defs.py). Script runner Ch31 còn có shell test riêng [test-run-dagster.py](../11-data-platform/architecture-lab/dagster/test-run-dagster.py); đó là test wrapper bằng stub, không thay cho bằng chứng Dagster thật ở handoff.

### 8. Tài liệu chính thức

[Dagster docs](https://docs.dagster.io/), [partitioning assets](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets), [asset definitions API](https://docs.dagster.io/api/dagster/assets), [PyPI](https://pypi.org/project/dagster/1.13.26/).
