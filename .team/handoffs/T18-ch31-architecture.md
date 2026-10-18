# Handoff T18 — Chương 31 Data Pipeline Architecture (tác giả: Claude Code, reviewer: Codex)

## File
- `11-data-platform/ch31-data-pipeline-architecture.md` (17 mục + Vị trí trong lộ trình)
- `11-data-platform/architecture-lab/`: `project.scala`, `build.sbt`, `project/build.properties`, `data/` (3 lô raw + customers), `dagster/defs.py`,
  `dagster/run-dagster.sh`, `src/main/scala/vn/scalaeng/platform/{Platform,Assets,Orchestrator,SpeedLayer,Main,ArchitectureDemo}.scala`,
  `src/test/scala/vn/scalaeng/platform/PlatformSuite.scala`
- `solutions/ch31-solutions.md`, `solutions/ch31-code/` (7 bài)

## Kiểm chứng (17/10/2026, Windows 11, JDK 21)
- `scala-cli test .` → 10/10; `sbt test` → `Passed: Total 10`; 0 cảnh báo (`-Wunused:all -Wvalue-discard`).
- `ArchitectureDemo` → output trích nguyên văn mục 9; doanh thu khớp bảng tính tay mục 6 (tính trước khi chạy).
- Lời giải `scala-cli test` 7/7.
- Dagster **1.13.26** cài trong venv `.cache/dagster-venv` (Python 3.12.10); `scala-cli --power package --assembly` → jar; `dagster/run-dagster.sh` chạy
  `dagster asset materialize --partition` cho 15, 16, 17 → exit 0, 2 materialization mỗi partition; lineage do job Scala ghi 6 dòng.
- Lỗi thật thu được khi viết: `DagsterUnknownPartitionError` (thiếu `end_offset`, xác nhận bằng docstring), mã thoát sai do `; 1` trong `catch` (tái hiện tối
  thiểu), `unclosed comment` do `/*` trong Scaladoc.

## Phiên bản
Scala 3.9.0, circe-parser/circe-generic 0.14.17, munit 1.2.4, sbt 1.12.11 (repo1.maven.org); Dagster 1.13.26, `requires_python >=3.10,<3.15` (PyPI JSON API).

## Đề xuất file dùng chung
Glossary: raw/staging/serving layer, orchestration vs processing, asset/partition/materialization, lineage/provenance/impact analysis, stale/fresh, backfill
vs reprocessing, Lambda/Kappa, quarantine, table format. Compatibility matrix: dòng Dagster 1.13.26 (Python, công cụ riêng, đã chạy).

## Chưa kiểm chứng
Dagster UI/daemon/schedule (chỉ CLI), `--partition-range` (yêu cầu `BackfillPolicy.single_run()` theo help của CLI — không dùng), Airflow, Delta/Iceberg,
OpenLineage (chỉ khái niệm).

## Điểm reviewer nên soi
- `Orchestrator.status`: còn trường hợp nào partition thực ra cũ mà báo fresh? (dimension không khai báo trong `currentInputs`, file raw bị xóa, đổi tên file).
- `StagingOrders.currentInputs` dựa vào cột thứ 3 "khai báo" — dòng hỏng có ngày hợp lệ nhưng sai số cột thì sao?
- `impact` dùng lần tính gần nhất — có bỏ sót partition đã tính bằng input cũ không?
- Kiểm tra "input đổi trong lúc tính" có race nào còn lọt (file đến sau lần đọc thứ hai)?
- Speed layer so sánh có công bằng; mục 3.6 về Lambda/Kappa và Spark/Flink có khẳng định quá mức không.

## Vòng 2 — phản hồi review Codex (`.team/reviews/T18-ch31-review.md`, 5 MAJOR + 5 MINOR), 17/10/2026

| ID | Sửa | Bằng chứng |
|---|---|---|
| M1 | Tách `status` (khớp cục bộ — backfill dùng) và `freshness` (tận nguồn, đệ quy theo partition trong `InputRef`, có `seen` chống chu trình). CLI `status`, demo và SLA (bài 3) dùng `freshness`. Định nghĩa thuật ngữ, mục 3.4, debug bước 1 sửa theo. | test `freshness tận nguồn ...`, bài 3 (lô 17 chưa backfill → `cũ: upstream staging_orders@2026-10-16 stale: ...`); demo mục 9 dòng daily@16. |
| M2 | `Materialization.id` (UUID) + `InputRef.materializationId` (ID lần tính upstream sinh ra đúng các byte đã đọc; None nếu hash không khớp). `impact` lan theo ID rồi giữ partition có lần tính HIỆN HÀNH trong tập; provenance (bài 2) đi theo ID, upstream không rõ phiên bản in `? ...`. | test `impact theo PHIÊN BẢN` (raw cách ly + chỉ staging tính lại → vẫn liệt kê daily@15; lô muộn chỉ staging → không liệt kê daily); bài 2 test thứ hai. |
| M3 | Asset API đổi: `sources` / `relevant` / `compute(day, inputs: List[InputFile])` — orchestrator đọc mỗi input MỘT lần (snapshot), hash và tính trên cùng byte; bỏ phép "so hai đầu" (vốn publish trước khi kiểm và không bắt ABA). Ghi rõ giới hạn: output + lineage không là một giao dịch; cùng partition song song cần khóa. | test `snapshot: lô raw đến TRONG lúc tính ...`, `snapshot: dimension đổi rồi đổi lại (A → B → A)`; mục 10 lỗi 4 viết lại. |
| M4 | `run-dagster.sh` không xóa gì: `WORK` phải tồn tại, mỗi lần chạy `mktemp -d` thư mục con, giữ log; mã thoát Dagster được bắt đúng (bản cũ `set -e` làm `exit=$?` luôn 0). | chạy lại thật: `partition 2026-10-15/16/17: exit=0 2 materialization`, lineage có `materializationId`. |
| M5 | Dry-run (bài 7) chia `chắc chạy` / `có thể chạy — nếu output của X đổi hash` / skip; ghi rõ ước lượng chi phí là cận trên và giới hạn `willRun` theo tên + cùng ngày. | test mới: lô muộn toàn dòng 9 cột → staging chạy, daily `skip`. |
| m1 | Đổi tên bài 6 "Replay từ raw + đối chiếu", lab không triển khai Kappa; Lambda: cái giá là hai đường logic + đối soát, speed layer lab cố ý ngây thơ; thêm nguồn Jay Kreps. | mục 3.6, SpeedLayer.scala, mục 16. |
| m2 | `timezone="Asia/Ho_Chi_Minh"` trong `defs.py`; mục 10 lỗi 1 giải thích UTC mặc định. | Dagster chạy lại exit 0. |
| m3 | Runbook "lineage hỏng" (mục 11 bước 6); `force` không vượt lineage hỏng — ghi trong Scaladoc `backfill`. | — |
| m4 | Ví dụ CLI trong `project.scala` dùng `--k=v`. | — |
| m5 | Bài 6: "kiểm tra nhất quán với một snapshot input + code", bỏ `⇔`; nêu dimension hiện tại có thể khác bản serving đã dùng. | `solutions/ch31-solutions.md`. |

**Kiểm chứng (lead, 17/10/2026):** `scala-cli test 11-data-platform/architecture-lab --server=false` → 13 total, 0 failed, 0 cảnh báo (sau khi thêm `@unused` cho tham số mặc định của `relevant`); `sbt test` → `Passed: Total 13`; `solutions/ch31-code` → 9 total, 0 failed; `ArchitectureDemo` chạy lại, output mục 9 cập nhật (chỉ dòng daily@16 trước lần 3 đổi); `scala-cli --power package --assembly` + `dagster/run-dagster.sh` với Dagster 1.13.26 → 3 partition exit 0.

## Vòng 3 (Codex sửa thay)

Ngày **17/10/2026**, theo giao việc T24 của lead; sửa toàn bộ R2-MAJOR-01..05 và R2-MINOR-01. Không sửa file dùng chung, không commit, không chạy sbt.

**File đã sửa/tạo:**

- `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Assets.scala`: không nuốt lỗi I/O; mọi source đã khai báo phải đọc được, kể cả dimension/upstream. Missing hoặc IOException dừng trước publish.
- `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Platform.scala`: serialization CSV chung; khóa file độc quyền toàn root bằng `FileChannel.tryLock`, nhả trong finally. Từ chối ngay run đồng thời cả trong JVM và giữa JVM.
- `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala`: hash đúng serialization riêng của run, không hash lại target; khóa từ snapshot đến append lineage; chặn downstream khi thiếu/sai ID upstream; status đánh dấu lineage None cũ stale để backfill sửa; impact fail-closed nếu đồ thị phục vụ thiếu nguồn; parser ref kiểm ngày hợp lệ.
- `11-data-platform/architecture-lab/src/test/scala/vn/scalaeng/platform/PlatformSuite.scala`: thêm 4 regression cho R2-MAJOR-01/02/03 và MINOR-01; giữ 13 test cũ. Test đồng thời đặt B trên thread riêng ngay trong compute của A, trước publish; B phải bị từ chối, A giữ đúng output từ snapshot A, khóa được nhả để retry.
- `11-data-platform/architecture-lab/dagster/run-dagster.sh`: gom lỗi mọi partition và phần tổng kết; exit 1 nếu có lỗi, exit 0 khi tất cả thành công; giữ thư mục run/log và WORK của caller.
- `11-data-platform/architecture-lab/dagster/test-run-dagster.py` (mới): 2 regression shell dùng Dagster giả lập: tất cả thành công, partition giữa exit 42 nhưng partition cuối thành công; assert exit toàn script 0/1 và sentinel caller nguyên vẹn.
- `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala` và `src/test/scala/vn/scalaeng/platform/solutions/SolutionsSuite.scala`: dry-run hạ downstream stale thành có điều kiện nếu upstream còn chạy; so với hash downstream đã tiêu thụ; thêm regression xung đột PAID/REFUNDED append-only khôi phục hash ban đầu. Bài 2 chuyển test sửa tay staging sang từ chối publish downstream.
- `11-data-platform/ch31-data-pipeline-architecture.md`, `solutions/ch31-solutions.md`: cập nhật cơ chế, runbook, số test, giới hạn snapshot/transaction và output thật mục 9.

**Lệnh và kết quả thật:** Windows, Java 21, Scala 3.9.0; `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`, TEMP/TMP/`java.io.tmpdir` trong `.cache/t24/tmp`.

| Lệnh (cwd tương ứng) | Kết quả | Bằng chứng |
|---|---|---|
| `scala-cli.bat test . --server=false --java-opt=-Dstdout.encoding=UTF-8 --java-opt=-Djava.io.tmpdir=<cache-tmp>` (architecture-lab) | **17 total, 0 failed, 0 ignored**, exit 0 | `.cache/t24/lab-test.log`: `Test run vn.scalaeng.platform.PlatformSuite finished: 0 failed, 0 ignored, 17 total 11.059s` |
| Cùng lệnh (solutions/ch31-code) | **10 total, 0 failed, 0 ignored**, exit 0 | `.cache/t24/solutions-test.log`: `Test run vn.scalaeng.platform.solutions.SolutionsSuite finished: 0 failed, 0 ignored, 10 total 9.911s` |
| `python 11-data-platform/architecture-lab/dagster/test-run-dagster.py` (root, Git Bash tường minh) | **Ran 2 tests ... OK**, exit 0; giả lập, không phải Dagster thật | `.cache/t24/shell-test.log`, `.cache/t24/ch31-script-*/{stdout,stderr}.txt` |
| `bash -n 11-data-platform/architecture-lab/dagster/run-dagster.sh` | exit 0 | Kiểm tra cú pháp |
| `scala-cli.bat run . --server=false --main-class vn.scalaeng.platform.ArchitectureDemo --java-opt=-Dstdout.encoding=UTF-8 --java-opt=-Djava.io.tmpdir=<cache-tmp>` (lab) | exit 0; **41/41 dòng** mục 9 khớp, đủ **17/17 mục** | `.cache/t24/demo.utf8.txt`, `verify_ch31.py --reuse` |

Hai suite không có cảnh báo compiler; hint munit có bản 1.3.6 không đổi pin 1.2.4. Lần đầu regression thread dùng latch bị timeout (16/17 pass), đã đổi điểm phối hợp bằng cách khởi chạy B trong compute A và chạy lại suite đạt 17/17. Driver shell ban đầu chọn nhầm WSL rồi gặp `mkdir -p` đi qua cha ngoài sandbox; chuyển Git Bash tường minh và WORK tương đối trong cache, source script giữ nguyên. Driver đối chiếu demo được sửa chỉ số sau khi đổi header/newline và stdout cp1252; dùng lại stdout từ demo exit 0 để assert, không bịa output. Tiến độ từng bước: `.team/progress/T24.md`.

**Phiên bản/nguồn:** không đổi dependency so với Vòng 2. Đối chiếu Java 21 ngày 17/10/2026: [Files.readAllBytes](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#readAllBytes(java.nio.file.Path)) (IOException truyền ra), [FileChannel.tryLock](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html#tryLock()) (khóa không chờ, overlap trong JVM phải từ chối). Thư viện Scala/circe/munit được compile/test thật theo pins hiện có.

**Chưa kiểm chứng:** sbt (lead chạy riêng); không chạy lại Dagster thật/UI/daemon/schedule trong Vòng 3, shell regression chỉ dùng wrapper giả lập. Khóa file được regression hai thread cùng JVM; chưa thử riêng hai JVM/host/filesystem mạng. Khóa root và rename từng file không biến output + lineage thành transaction, cũng không tạo snapshot đồng thời raw/dimension. Producer phải publish file hoàn chỉnh bằng rename. Giữ thư mục `.cache` rỗng tạo nhầm trong lab do công cụ từ chối lệnh dọn; không có source hoặc dữ liệu bị xóa.

**Đề xuất lead:** review lại các thay đổi và chạy sbt/Dagster cuối trước khi cập nhật trạng thái chung; không đổi dependency/glossary/compatibility matrix trong task này.
