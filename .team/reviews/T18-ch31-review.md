# Review chéo T18 — Chương 31: Data Pipeline Architecture

Reviewer: **Codex** · Tác giả: Claude Code · Ngày: **17/10/2026**.

**Kết luận: CHƯA ĐẠT — 0 BLOCKER, 5 MAJOR, 5 MINOR.** Test và output mẫu đúng; còn lỗi ảnh hưởng đến kết luận freshness, truy vết nguồn và an toàn chạy lab. Cần tác giả xử lý các MAJOR rồi lead kiểm chứng lại trước khi đánh dấu hoàn tất.

## Phạm vi và cách kiểm chứng

Đã đọc handoff `.team/handoffs/T18-ch31-architecture.md`, toàn bộ chương, source/test/data/build của `11-data-platform/architecture-lab/` (bao gồm `dagster/defs.py`, `dagster/run-dagster.sh`), `solutions/ch31-solutions.md` và `solutions/ch31-code/`; đối chiếu template, AGENTS.md và PHẦN 11–14 của PROMPT_PROJECT.md.

Chỉ ghi review này và `.cache/`. Build/test thực hiện trên bản sao giữ nguyên cấu trúc tương đối tại `.cache/ch31-review/`, để Scala CLI không tạo build artifact trong source của tác giả. Các file Scala/data/build/script được dùng nguyên bản; probe riêng nằm trong `.cache/ch31-review/probes/ReviewProbes.scala`. Không commit, không sửa file dùng chung hay source của tác giả.

Môi trường: Windows, Java **21.0.7**, Scala **3.9.0**, Python **3.12.10**, venv `.cache/dagster-venv` có Dagster **1.13.26**. Các lệnh Scala CLI đều có `--server=false`; đặt `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`. TEMP/TMP của test và probe đặt trong `.cache/ch31-review/tmp`.

| Kiểm chứng đã chạy | Kết quả thực | Bằng chứng trong `.cache/ch31-review/` |
|---|---|---|
| `scala-cli.bat test . --server=false`, cwd bản sao `11-data-platform/architecture-lab` | **10 total, 0 failed, 0 ignored**, exit 0 | `lab-test.log` |
| Cùng lệnh, cwd bản sao `solutions/ch31-code` | **7 total, 0 failed, 0 ignored**, exit 0 | `solutions-test.log` |
| `scala-cli.bat --power package . --assembly --server=false --main-class vn.scalaeng.platform.Main -o ../../architecture-lab.jar` | exit 0 | `package.stdout.txt`, `package.stderr.txt` |
| Chạy `ArchitectureDemo` từ assembly bằng JDK 21, ép stdout UTF-8; Python đối chiếu từng dòng với mục 9 | **41/41 dòng khớp nguyên văn** | `demo.utf8.txt`, `verify_outputs.py`, `verify-outputs.log` |
| Chạy Main từ jar, `status` thiếu `--root` | **exit 2**, đúng hướng dẫn CLI | `verify-outputs.log` |
| `scala-cli.bat run . --server=false --java-opt=-Dstdout.encoding=UTF-8 --main-class vn.scalaeng.platform.review.ReviewProbes`, cwd `probes` | **12/12 probe tái hiện đúng quan sát được assert**; trong đó có quan sát chứng minh lỗi, không có nghĩa hệ thống qua 12 yêu cầu correctness | `probes.stdout.txt`, `probes.stderr.txt` |
| `.cache/dagster-venv/Scripts/python.exe -B .cache/ch31-review/dagster_check.py` | Ba lệnh `dagster asset materialize ... --select staging_orders,daily_revenue --partition <15/16/17>` đều **exit 0**, mỗi lệnh **2 ASSET_MATERIALIZATION**, Scala lineage **6 dòng** | `dagster-check.log`, `dagster-2026-10-{15,16,17}.log`, `dagster-platform/` |
| Tái hiện mẫu `catch ...; 1` với Scala 3.9.0 | `code=1`, có `unused pattern variable` ở mẫu không dùng `e` | `errors/Semicolon.scala`, `semicolon.stdout.txt`, `semicolon.stderr.txt` |
| Compile Scaladoc chứa glob `/*.csv` | exit 1, **`unclosed comment`** | `errors/BadComment.scala`, `badcomment.stderr.txt` |
| `bash -n .../dagster/run-dagster.sh` | exit 0 | Kiểm tra cú pháp; không chạy phần xóa thư mục của script |

Hai suite gốc không có cảnh báo compiler; Scala CLI in **hint** munit 1.2.4 có bản mới 1.3.6. Giữ pin munit theo chuẩn repo, không coi hint là cảnh báo compiler. Lỗi PowerShell `NativeCommandError` trong một số log là cách PowerShell biểu diễn stderr của native command; exit code và dòng kết quả test ở trên là kết quả tiến trình thật.

Không chạy lại sbt do giới hạn sandbox đã nêu trong AGENTS.md. Không khẳng định độc lập xác nhận dòng `sbt test` trong handoff. Không chạy UI/daemon/schedule, Airflow, Delta/Iceberg hoặc OpenLineage.

## BLOCKER

Không có.

## MAJOR

### MAJOR-01 — `Fresh` chỉ kiểm tra input trực tiếp, nhưng được dùng như freshness của serving

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:49`, `:56`; `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:43`; `11-data-platform/ch31-data-pipeline-architecture.md:263`, `:486`.
- **Tái hiện:** sau khi build ngày 15, xóa raw lô 15: staging báo `stale: input mất raw/orders/ingest_date=2026-10-15/orders.csv`, nhưng `daily_revenue@2026-10-15` vẫn **fresh**. Với lô muộn append-only, chính demo cũng cho `daily_revenue@16: fresh` dù raw đã có hoàn tiền cần sửa doanh thu. Đây là semantics được tác giả chủ động chọn, không phải output trích sai.
- **Ảnh hưởng:** định nghĩa thuật ngữ “fresh = còn đúng”, hướng dẫn debug “nếu fresh thì truy provenance”, và SLA chỉ theo dõi `DailyRevenue` có thể trấn an người vận hành khi upstream đã cũ. Backfill đủ danh sách topo sẽ sửa được, nhưng `status`/giám sát không phải lúc nào cũng chạy backfill trước.
- **Đề xuất:** phân biệt rõ “khớp materialization/input trực tiếp” với “fresh đến tận nguồn”; thêm trạng thái/lý do `upstream stale/missing` cho API giám sát, truy các **partition cụ thể trong InputRef**, có chống chu trình. Có thể giữ phép so cục bộ cho vòng backfill để tránh chạy downstream nếu upstream tái tính ra cùng byte. Sửa định nghĩa, debug và SLA theo semantics đã chọn. Bổ sung test chỉ hỏi trạng thái/SLA serving sau raw muộn hoặc mất, trước backfill.

### MAJOR-02 — Impact/provenance nối nhầm phiên bản upstream, bỏ sót nguồn vẫn còn trong output đang phục vụ

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:95`; `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:28`, `:32`; `11-data-platform/ch31-data-pipeline-architecture.md:168`; `solutions/ch31-solutions.md:24`.
- **Tái hiện bỏ sót:** build ngày 15, xóa raw lô 15, chỉ materialize lại staging. Serving giữ nguyên hash cũ, vẫn chứa đóng góp từ raw đã xóa. `impact(raw_lô_15)` trả **`Vector()`**; provenance của serving cũng mất raw lô 15. Vì lineage staging **mới nhất** không dùng file đó, đường đi tới serving cũ bị cắt.
- **Tái hiện không cần vi phạm append-only:** build serving, thêm `late.csv` có đơn X ngày 15, chỉ materialize lại staging. Provenance của serving lập tức liệt kê `late.csv`, dù hash serving chưa đổi và lần tính serving chưa từng dùng nó.
- **Nguyên nhân:** InputRef có hash phiên bản upstream đã đọc, nhưng cả hai phép truy vết bỏ qua hash và nối tới `.lastOption`/`.last` của `asset@day` hiện tại. Đây là đồ thị trạng thái mới nhất, không phải đồ thị nguồn gốc của output đang được hỏi.
- **Đề xuất:** gắn tham chiếu tới **materialization ID** upstream thực sự được tiêu thụ; khi duyệt phải dùng đúng phiên bản đó. Với dữ liệu hiện có, có thể tra lineage lịch sử theo `asset@day + outputHash`, nhưng phải xử lý trường hợp nhiều run có cùng hash và provenance khác nhau; ID bất biến rõ ràng hơn. Impact khởi đầu từ các materialization có dùng rawRef rồi đi tới những phiên bản còn được serving hiện hành tham chiếu. Thêm cả hai regression test, không chỉ test khi mọi asset vừa được backfill đồng bộ.

### MAJOR-03 — Kiểm tra race sau khi publish; so hash hai đầu cũng không bảo đảm snapshot đã dùng

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:61`, `:64`, `:66`; `11-data-platform/ch31-data-pipeline-architecture.md:339`, `:474`.
- **Tái hiện 1, raw vẫn append-only:** Asset wrapper giữ logic staging, thêm một file raw muộn ngay khi bắt đầu `compute`. `materialize` ném đúng lỗi `input ... đổi trong lúc tính — chạy lại`, nhưng **file output đã bị thay thế**; lineage vẫn là bản cũ. Một lần tính bị từ chối đã publish dữ liệu trước khi validation hoàn tất.
- **Tái hiện 2, không sửa raw:** wrapper của `DailyRevenue` đọc dimension tạm có C9=Retail rồi dimension được trả về nội dung cũ trước lần kiểm tra cuối. Output phản ánh dimension tạm, lineage ghi hash dimension cũ, `status` báo **fresh**, và tính lại trên input hiện tại cho kết quả khác. Đây là race ABA (A → B → A) với dimension mà lab cho phép thay đổi. Probe raw ABA riêng cũng tái hiện được, nhưng trường hợp raw đó vi phạm giả định append-only và không được dùng làm bằng chứng duy nhất.
- **Phân biệt:** file mới tới **sau** lần đọc input thứ hai không tự tạo fresh sai lâu dài: probe cho lần `status` kế tiếp báo `stale: input mới ...`. Không nên gọi riêng cửa sổ đó là lỗi false-fresh.
- **Đề xuất:** freeze manifest và **nội dung/phiên bản** input cho cả compute, kể cả dimension; compute trên snapshot đó. Ghi output vào vùng tạm, kiểm tra điều kiện chấp nhận trước khi publish; lần bị phát hiện race không được thay output đang phục vụ. Nêu rõ atomic rename một file khác với transaction output + lineage, và mô tả khoảng hở khi process chết. Thêm test giữ output cũ khi run bị từ chối và test dimension ABA. Nếu chỉ giữ biện pháp so hai đầu cho lab, phải hạ rõ mức bảo đảm và không diễn đạt nó như bằng chứng input thực sự đã dùng.

### MAJOR-04 — Script xóa đệ quy toàn bộ đường dẫn do người học cung cấp

- **Vị trí:** `11-data-platform/architecture-lab/dagster/run-dagster.sh:8`, `:9`; `11-data-platform/ch31-data-pipeline-architecture.md:328`.
- **Bằng chứng tĩnh:** `${WORK:?...}` chỉ bắt biến trống/không đặt; đường dẫn tuyệt đối tới một thư mục đã có dữ liệu vẫn hợp lệ. Lệnh tiếp theo là **`rm -rf "$WORK"`**. Đặt WORK thành đường dẫn tuyệt đối tới architecture-lab sẽ nhắm toàn bộ lab; đặt nhầm một thư mục tạm đang chứa dữ liệu khác cũng mất hết nội dung. Quoting không giới hạn phạm vi xóa.
- **Đề xuất:** tạo **thư mục con mới, duy nhất** bên trong WORK bằng `mktemp -d`, và không xóa WORK do caller sở hữu. Nếu cần cleanup, chỉ cleanup thư mục con do chính lần chạy tạo, có kiểm tra đường dẫn đã resolve và marker sở hữu; mặc định giữ log cho người học. Không dùng xóa đệ quy như bước mở đầu của ví dụ.
- Reviewer chỉ kiểm tra cú pháp và chạy lại ba lệnh Dagster tương đương bằng driver Python, không thực hiện lệnh xóa này hay thử xóa dữ liệu để chứng minh.

### MAJOR-05 — Dry-run khẳng định downstream sẽ chạy dù runtime có thể skip

- **Vị trí:** `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:83`, `:89`; `solutions/ch31-solutions.md:56`; `11-data-platform/ch31-data-pipeline-architecture.md:503`.
- **Tái hiện:** thêm `late.csv` với ngày 15 hợp lệ nhưng **9 cột**. Staging stale vì có input mới, tái tính và tăng rejected, nhưng output CSV vẫn giống hệt trước. `Solutions.plan` liệt kê cả staging và daily với lời “upstream ... sẽ được tính lại”; `Orchestrator.backfill` thật materialize staging rồi **skip daily, fresh**.
- **Ảnh hưởng:** kế hoạch được trình bày như quyết định chạy thực tế trong khi chỉ là tập ứng viên bảo thủ; việc upstream chạy lại không có nghĩa data version của nó đổi.
- **Đề xuất:** trả ba loại “chắc chạy”, “có thể chạy nếu hash upstream đổi”, “skip”, và diễn giải ước lượng chi phí là cận trên. Nếu muốn kế hoạch chính xác, cần biết trước output hash qua cache/versioning hoặc tính thử trên scratch; không thể suy ra chỉ từ tập tên `willRun`. Thêm regression test với input mới bị loại và reprocessing upstream không đổi output. Với many-to-one/cross-day, cũng phải mô tả giới hạn: `willRun` hiện reset mỗi ngày và chỉ lưu tên asset.

## MINOR

### MINOR-01 — Batch replay được gọi là Kappa, và lỗi speed layer bị quy quá mạnh cho Lambda

- **Vị trí:** `11-data-platform/ch31-data-pipeline-architecture.md:217`, `:219`; `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/SpeedLayer.scala:6`; `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:70`; `solutions/ch31-solutions.md:48`.
- Lab có hàm cộng PAID trên toàn bộ Seq và batch recompute, chưa có đường stream xử lý state, checkpoint/offset, replay cùng stream processor hay chuyển kết quả replay sang phục vụ. Tái sử dụng cùng code batch khi replay là đặc tính hữu ích, chưa chứng minh đã triển khai Kappa. Lệch 20.900.000 là do speed layer cố ý bỏ dedup/update/refund; Lambda không bắt buộc chấp nhận sai số này.
- **Đề xuất:** gọi bài 6 là “replay từ raw/đối chiếu batch”, giải thích nó minh họa khả năng tái sử dụng logic hướng tới Kappa. Nêu chi phí của Lambda là duy trì/đối chiếu hai đường logic; một speed layer đầy đủ cũng có thể xử lý dedup và cập nhật. Bảng phân biệt kiến trúc và câu lưu ý Spark/Flink không mặc định có cùng semantics là hợp lý. Có thể thêm [bài gốc của Jay Kreps](https://www.oreilly.com/radar/questioning-the-lambda-architecture/) làm nguồn cho phần so sánh.

### MINOR-02 — “Ngày hôm nay” trong partition Dagster đang là UTC

- **Vị trí:** `11-data-platform/architecture-lab/dagster/defs.py:22`, `:26`; `11-data-platform/ch31-data-pipeline-architecture.md:434`, `:532`.
- Đối chiếu Dagster 1.13.26: `daily.timezone` là **UTC**. Probe tại `2026-10-17T00:30:00+07:00`, `end_offset=1` chỉ trả keys 15 và 16; tới trưa cùng ngày mới có key 17. Vì vậy “ngày đang diễn ra” cần gắn với timezone partition, không phải mặc nhiên giờ Việt Nam của máy.
- **Đề xuất:** đặt `timezone="Asia/Bangkok"`/`"Asia/Ho_Chi_Minh"` nếu ngày nghiệp vụ theo UTC+7, hoặc nói rõ lab dùng UTC và kiểm thử ở biên nửa đêm. Quy tắc `end_date` loại trừ và `end_offset` trong bài đúng với [API partitions chính thức](https://docs.dagster.io/api/dagster/partitions) và docstring bản cài; không yêu cầu bỏ `end_offset=1`.

### MINOR-03 — Chưa có hướng phục hồi khi lineage hỏng; `--force=true` cũng không vượt qua được

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:30`, `:75`, `:80`; `11-data-platform/ch31-data-pipeline-architecture.md:341`, `:491`.
- Thêm một dòng `{broken` cuối JSONL: `status` ném `IllegalStateException: lineage hỏng: ParsingFailure...` đúng thiết kế fail-closed. Nhưng `backfill(force=true)` vẫn gọi `status` trước, nên cũng ném lỗi. Kappa replay có thể dựng root khác, nhưng bài chưa nối nó thành quy trình phục hồi metadata hỏng.
- **Đề xuất:** bổ sung debug/runbook: giữ nguyên bản hỏng làm bằng chứng, kiểm tra dòng lỗi và backup; tái dựng trên root sạch từ raw + dimension snapshot đã chọn, đối chiếu trước khi thay thế. Nói rõ force chỉ bỏ qua quyết định skip, không sửa/vượt lỗi parse lineage; không tự bỏ qua dòng lỗi.

### MINOR-04 — Lệnh mẫu trong project.scala dùng cú pháp CLI mà parser không nhận

- **Vị trí:** `11-data-platform/architecture-lab/project.scala:4`; đối chiếu `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Main.scala:12`.
- Comment chỉ `--root <thư mục> --from 2026-10-15 --to 2026-10-17`; parser chỉ thu token có dấu `=`. Chạy các tham số cách nhau bằng space từ jar cho **exit 2**, không thực hiện status.
- **Đề xuất:** đổi ví dụ thành `--root=<thư-mục> --from=2026-10-15 --to=2026-10-17`, khớp các ví dụ CLI khác. Trong hướng dẫn dành Codex trên Windows, luôn thêm `--server=false` theo AGENTS.md.

### MINOR-05 — Một lần replay bằng nhau không chứng minh mệnh đề tương đương về tính tất định

- **Vị trí:** `solutions/ch31-solutions.md:51`; `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:70`; `11-data-platform/ch31-data-pipeline-architecture.md:491`.
- Câu “Bằng nhau ⇔ pipeline tất định và serving không bị ai sửa” quá mạnh. Pipeline có yếu tố ngẫu nhiên vẫn có thể trùng ở một lần thử; serving có thể đã được sửa rồi khôi phục. Ngược lại, raw/dimension hoặc code đã đổi hợp lệ nhưng chưa backfill cũng làm replay lệch dù compute tất định và serving không bị sửa tay. Hàm hiện copy dimension **hiện tại**, không nhất thiết dimension phiên bản mà serving đã dùng.
- **Đề xuất:** gọi đây là phép kiểm tra nhất quán của **snapshot input + code cụ thể**; một lần bằng nhau chỉ là bằng chứng cho lần so đó. Khi lệch, kiểm tra code/input versions và trạng thái stale trước khi kết luận nondeterminism hay sửa tay. Đổi `⇔` thành diễn đạt có điều kiện, không cần thêm framework để chứng minh hình thức.

## Tổng hợp các probe được yêu cầu

| Probe | Quan sát | Đánh giá |
|---|---|---|
| Raw bị xóa | Staging stale vì `input mất`; daily vẫn fresh khi staging chưa tái tính | Phát hiện mất input trực tiếp đúng; vấn đề downstream ở MAJOR-01 |
| Raw đổi tên | Staging stale cả `input mới` và `input mất` | Qua kiểm tra; không báo sai thành lỗi bỏ sót của staging |
| Dimension thay đổi, khai báo đúng | Daily stale; staging fresh | `DailyRevenue` đã khai báo customers.csv đúng |
| Asset cố ý không khai báo dimension | Output phụ thuộc dimension nhưng status fresh sau dimension đổi | Giới hạn hợp đồng `currentInputs`, không gán lỗi bỏ khai báo cho DailyRevenue đang có trong lab; cần nhắc người mở rộng asset phải khai báo mọi input đọc |
| Dòng sai số cột, cột ngày hợp lệ | File mới được theo dõi; materialize tăng rejected lên 2; dòng không vào output | Qua kiểm tra lineage trực tiếp, dùng tiếp để bắt sai khác dry-run |
| Recompute staging, giữ serving cũ | Impact bỏ sót raw cũ; provenance nối nhầm upstream mới | MAJOR-02 |
| Lô muộn append-only, chỉ chạy staging | Provenance nhận vơ raw mới cho serving cũ | MAJOR-02, không phụ thuộc thao tác sửa/xóa raw |
| Raw ABA | Status fresh dù output khác tính lại từ raw hiện tại | Stress ngoài hợp đồng append-only; không dùng làm lỗi độc lập |
| Dimension ABA | Status fresh sai, không sửa raw | MAJOR-03 |
| File đến sau lần đọc input thứ hai | Materialize trả về, nhưng lần status tiếp theo stale | Giới hạn thời điểm quan sát; không phải false-fresh bền vững |
| Race được guard phát hiện | Ném lỗi nhưng output đã publish, lineage cũ | MAJOR-03 |
| Lineage JSONL hỏng | Fail-closed, kể cả backfill force | Đúng lựa chọn an toàn; thiếu hướng phục hồi ở MINOR-03 |

Probe dùng Asset wrapper để đặt thay đổi input chính xác vào các điểm xen kẽ, không dựa vào sleep hay xác suất scheduler. Các wrapper giữ compute gốc, trừ asset cố ý bỏ dimension dùng làm negative control.

## Cấu trúc, sư phạm và nguồn phiên bản

- **17/17 mục có nội dung**, mục 13 dẫn sang solutions, 7 bài tập có code/test tương ứng; tiếng Việt có dấu, domain đơn hàng nối tiếp, có tính tay, thuật ngữ và câu hỏi tự kiểm tra. Số liệu Retail/Wholesale/UNKNOWN và đối soát speed/batch khớp dữ liệu thật. Vấn đề chính là những kết luận mạnh hơn mức code bảo đảm đã nêu phía trên.
- Phân tách **orchestration/processing** đúng: Dagster chọn partition/thứ tự, subprocess Scala tính partition và trả exit/metadata. `deps` xếp staging trước daily trong lần chạy đã kiểm chứng. `defs.py` hiện không định nghĩa schedule/sensor/automation/retry policy; ví dụ không tự theo dõi hash raw hay tự biến metadata `output_hash` thành `data_version` của Dagster. Không suy rộng lần chạy CLI thành đã kiểm chứng UI/lịch/daemon.
- Phiên bản **Dagster 1.13.26** và `Requires-Python <3.15,>=3.10` khớp cả metadata package cài sẵn lẫn [PyPI JSON riêng phiên bản](https://pypi.org/pypi/dagster/1.13.26/json), kiểm tra 17/10/2026. `daily-docstring.txt` lưu docstring của **chính bản 1.13.26**, không chỉ dựa vào docs latest. Source CLI cài sẵn `_cli/asset.py:43` và `:169` cũng xác nhận `--partition-range` cần `BackfillPolicy.single_run()` như handoff; reviewer không chạy partition-range.
- Scala/circe/munit/sbt pins đều có POM HTTP 200 ở Maven Central, ngày 17/10/2026: [Scala compiler 3.9.0](https://repo1.maven.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom), [circe-parser 0.14.17](https://repo1.maven.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom), [circe-generic 0.14.17](https://repo1.maven.org/maven2/io/circe/circe-generic_3/0.14.17/circe-generic_3-0.14.17.pom), [munit 1.2.4](https://repo1.maven.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom), [sbt 1.12.11](https://repo1.maven.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom). Xác nhận artifact tồn tại không thay thế cho việc chạy sbt; Scala CLI đã compile/test các dependency Scala thật.

## Điều kiện review lại

Tác giả xử lý MAJOR-01..05, thêm regression test phân biệt consistency cục bộ/freshness tận nguồn, lineage theo phiên bản, snapshot/race và kế hoạch chạy có điều kiện. Sửa các diễn giải MINOR tương ứng, chạy lại hai suite và output demo; nếu semantics output đổi thì cập nhật trích dẫn bằng output mới thật. Lead chạy kiểm chứng cuối và cập nhật file dùng chung theo AGENTS.md. Reviewer chưa đánh dấu chương hoàn tất.

## Vòng 2

Reviewer: **Codex** · Tác giả: Claude Code · Ngày: **17/10/2026**.

**Kết luận: CHƯA ĐẠT — 0 BLOCKER, 5 MAJOR, 1 MINOR còn lại/phát hiện thêm.** Hai suite gốc qua **13/13 + 9/9**, demo khớp **41/41 dòng**. Bản sửa xử lý đúng nhiều phản ví dụ vòng 1, nhưng chưa đủ để đóng MAJOR-02, MAJOR-03 và MAJOR-05; các probe mới còn chứng minh mất thông tin impact, nuốt lỗi đọc input, lineage gán hash output của run khác và dry-run khẳng định sai. Script hết nguy cơ xóa WORK nhưng còn trả thành công khi một partition thất bại.

### Đối chiếu từng vấn đề vòng 1

| ID vòng 1 | Đánh giá vòng 2 | Bằng chứng và phần còn lại |
|---|---|---|
| **MAJOR-01** | **Đã sửa đúng tình huống vòng 1** | `status` giữ semantics cục bộ; `freshness` truy đúng partition upstream; CLI, demo và SLA dùng `freshness`. Raw xóa/đổi tên/lô muộn đều làm serving stale trước khi staging tính lại (probe 1, 2, 4). Chu trình hai nút, tự tham chiếu và asset không biết tên đều trả Stale (probe 11, 12). Không còn trấn an serving là fresh trong các tình huống gốc. Trường hợp ngày trong ref sai định dạng được ghi riêng ở R2-MINOR-01. |
| **MAJOR-02** | **Sửa đúng đường có ID; CHƯA đóng hoàn toàn** | UUID và truy theo `materializationId` sửa cả hai phản ví dụ: xóa raw rồi chỉ staging tính lại vẫn impact daily; thêm raw muộn rồi chỉ staging tính lại không nhận vơ nguồn mới cho daily (probe 3, 4). Tuy nhiên `None` cắt đồ thị mà `impact` không báo kết quả thiếu; phục hồi upstream cũng không chữa lineage downstream (probe 14, R2-MAJOR-03). |
| **MAJOR-03** | **Snapshot sửa đúng race trong compute; CHƯA đóng hoàn toàn** | Compute dùng đúng byte đã hash: dimension ABA, raw ABA, raw đến trong compute đều qua (probe 6–8). Không còn mô hình publish rồi ném lỗi guard như vòng 1. Tuy nhiên snapshot nuốt IOException (probe 16, R2-MAJOR-01), và đọc lại hash target sau publish có thể nhận hash của run khác, dẫn tới false-fresh (probe 18, R2-MAJOR-02). Bài đã nêu giới hạn output/lineage không transactional và thiếu khóa; vẫn cần sửa điểm gán sai hash, không chỉ mô tả là hai run “đè nhau”. |
| **MAJOR-04** | **Đã sửa đúng nguy cơ xóa dữ liệu** | Script tạo thư mục con mới bằng `mktemp -d`, không còn `rm -rf`; WORK phải tồn tại. Chạy thật trên bản sao: sentinel của caller còn nguyên, log và hai thư mục run được giữ. Lỗi mã thoát mới tách riêng ở R2-MAJOR-05. |
| **MAJOR-05** | **Sửa đúng probe 9 cột; CHƯA đóng hoàn toàn** | Input mới toàn dòng bị loại: plan ghi daily “có thể chạy”, runtime skip, dry-run không ghi lineage (probe 5). Nhưng daily đang stale vì upstream có thể trở về hash từng tiêu thụ: plan vẫn ghi “chắc chạy”, runtime skip (probe 17, R2-MAJOR-04). |
| **MINOR-01** | **Đã sửa đúng** | Chương mục 3.6, `SpeedLayer.scala`, bài 6 và lời giải phân biệt replay batch với Kappa; nói rõ speed layer của lab cố ý ngây thơ, Lambda không bắt buộc bỏ dedup/refund. Có nguồn bài gốc Jay Kreps. Tên một test còn chữ “kappa replay” không thay đổi semantics đã giải thích đúng, không mở lại issue. |
| **MINOR-02** | **Đã sửa đúng và kiểm chứng biên ngày** | `defs.py:28` đặt `Asia/Ho_Chi_Minh`. Với Dagster 1.13.26, tại `2026-10-17T00:30:00+07:00`, keys thực là **15, 16, 17**; không chỉ suy luận từ một lần chạy buổi chiều. |
| **MINOR-03** | **Đã sửa đúng** | Runbook mục 11 bước 6 giữ file hỏng, đối chiếu backup, dựng root sạch từ raw + dimension đã chọn rồi đối chiếu. Nêu force không vượt lỗi parse. Probe 10 xác nhận status và backfill force vẫn fail-closed. |
| **MINOR-04** | **Đã sửa đúng cú pháp parser** | `project.scala:4` dùng `--root=... --from=... --to=...`. Chạy Main từ assembly với cú pháp này exit **0**; thiếu root vẫn exit **2**. Reviewer luôn thêm `--server=false` khi gọi Scala CLI theo AGENTS.md. |
| **MINOR-05** | **Đã sửa đúng** | Lời giải bài 6 và debug bước 5 bỏ mệnh đề `⇔`, giới hạn kết luận ở lần so snapshot/code cụ thể; nhắc dimension hiện tại có thể khác phiên bản serving đã dùng và kiểm tra stale trước khi quy nondeterminism/sửa tay. |

### Kiểm chứng độc lập vòng 2

Đọc lại review vòng 1, mục Vòng 2 của handoff, toàn bộ chương, source/test/build/data/script của lab và lời giải; đối chiếu AGENTS.md, template và PHẦN 11–14 của đề bài. **Chỉ ghi file review này và `.cache/`**; mọi build/probe chạy trên bản sao giữ cấu trúc tương đối tại `.cache/ch31-review-r2/`. Không sửa source tác giả, không commit. Bản vòng 1 được lưu byte-for-byte trước khi nối phần này; phần nối ghi UTF-8.

Môi trường: Windows, Java **21.0.7**, Scala **3.9.0**, Python **3.12.10**, Dagster **1.13.26** từ `.cache/dagster-venv`. Đặt `COURSIER_CACHE=C:/Users/VuTT/zero_to_hero_scala/.cache/coursier`; TEMP/TMP và `java.io.tmpdir` của test/probe/demo trong `.cache/ch31-review-r2/tmp`.

| Lệnh/kiểm tra thực tế | Kết quả | Bằng chứng trong `.cache/ch31-review-r2/` |
|---|---|---|
| `scala-cli.bat test . --server=false --java-opt=-Djava.io.tmpdir=<cache-tmp>` tại bản sao `11-data-platform/architecture-lab` | **0 failed, 0 ignored, 13 total**, exit 0 | `lab-test.stdout.txt`, `lab-test.stderr.txt` |
| Cùng lệnh tại bản sao `solutions/ch31-code` | **0 failed, 0 ignored, 9 total**, exit 0 | `solutions-test.stdout.txt`, `solutions-test.stderr.txt` |
| `scala-cli.bat --power package . --assembly --server=false --main-class vn.scalaeng.platform.Main -o <cache>/architecture-lab.jar` | exit **0** | `package.stdout.txt`, `package.stderr.txt` |
| `python .cache/ch31-review-r2/verify_outputs.py` — chạy demo từ assembly, ép stdout UTF-8; kiểm tra CLI | Demo **41/41 dòng khớp nguyên văn** mục 9; đủ 17 mục; `status` thiếu root exit **2**, `--key=value` exit **0** | `verify_outputs.py`, `demo.stdout.txt`, `demo.stderr.txt` |
| `scala-cli.bat run . --server=false --main-class vn.scalaeng.platform.review.ReviewProbesR2 --java-opt=-Dstdout.encoding=UTF-8 --java-opt=-Djava.security.manager=allow --java-opt=-Djava.io.tmpdir=<cache-tmp>` tại `probes/` | **18/18 quan sát được assert tái hiện**, exit 0; gồm quan sát chứng minh lỗi, KHÔNG có nghĩa hệ thống đạt 18 yêu cầu correctness | `probes/ReviewProbesR2.scala`, `probes.stdout.txt`, `probes.stderr.txt` |
| `bash -n dagster/run-dagster.sh`; chạy script thật với Dagster, jar mới, WORK trong cache | Ba partition **exit=0**, mỗi partition **2 materialization**; Scala lineage **6 dòng**, daily có upstream ID; sentinel còn nguyên | `dagster-script.stdout.txt`, `dagster-script.stderr.txt`, `script-work-relative/ch31-dagster.JFoV5U/` |
| Chạy cùng script với DAGSTER wrapper chỉ trả 42 ở partition 16, thực thi Dagster thật ở 15 và 17 | Partition 16 in **exit=42, 0 materialization**, nhưng **toàn script exit 0** | `dagster-failure.stdout.txt`, `dagster-failure.stderr.txt`, `dagster-verify.log` |
| Nạp `defs.py` mới và gọi `daily.get_partition_keys` tại 00:30 UTC+7 | timezone **Asia/Ho_Chi_Minh**; keys `['2026-10-15', '2026-10-16', '2026-10-17']` | `dagster_verify.py`, `dagster-verify.log` |

Hai suite source không có cảnh báo compiler; hint munit 1.2.4 → 1.3.6 không phải cảnh báo và không đổi pin repo. Probe concurrency dùng **SecurityManager của JDK 21 chỉ làm điểm dừng kiểm soát lịch xen kẽ**, không đổi kết quả đọc/ghi hoặc source; JDK in cảnh báo runtime API này deprecated. Đây là cảnh báo của instrumentation reviewer, không phải của lab. Các probe race dùng latch/điểm xen kẽ cụ thể, không dựa vào sleep hay xác suất.

Chi tiết môi trường Dagster: `mkdir -p` với WORK tuyệt đối bị sandbox Windows từ chối tại thư mục cha, nên lần chạy thành công dùng WORK tương đối trong cache và wrapper DAGSTER chuẩn hóa `DAGSTER_HOME`/`CH31_ROOT` thành đường dẫn tuyệt đối trước khi gọi **dagster.exe thật**. Script và `defs.py` bản sao giữ nguyên byte. Driver Python đầu tiên đã chạy xong ba partition (script exit 0, stdout/log đã lưu), rồi lỗi encode cp1252 khi in tiếng Việt; sửa encoding ở **driver reviewer**, xác nhận lại bằng log đã lưu và tiếp tục lượt chèn lỗi, không tính lỗi driver là lỗi source. Không khẳng định đã chạy script nguyên trạng với WORK tuyệt đối trong sandbox này.

Không chạy lại sbt theo giới hạn BootServerSocket đã ghi ở AGENTS.md, không xác nhận độc lập kết quả sbt trong handoff. Không chạy UI/daemon/schedule, Airflow, Delta/Iceberg, OpenLineage hay Project 5. Phiên bản Dagster và `Requires-Python <3.15,>=3.10` khớp metadata cài thật và [PyPI JSON riêng bản 1.13.26](https://pypi.org/pypi/dagster/1.13.26/json), kiểm tra **17/10/2026**; semantics timezone đối chiếu thêm [API partitions chính thức](https://dagster.io/docs/api/dagster/partitions). Không có thay đổi dependency so với vòng 1; các dependency Scala được compile/test thật ở hai suite trên.

### BLOCKER vòng 2

Không có.

### MAJOR còn lại/phát hiện thêm

#### R2-MAJOR-01 — Snapshot nuốt IOException thành “missing”, publish dữ liệu sai rồi báo fresh

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Assets.scala:19`, `:26`, `:99`; `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:53`, `:83`.
- **Tái hiện, probe 16:** trên root sao chép, chuyển `customers.csv` sang file dự phòng rồi tạo **thư mục** tên `customers.csv`. `Files.exists` là true; `Files.readAllBytes` thật ném **AccessDeniedException** (IOException). `InputFile.read` bắt lỗi qua `Try(...).toOption` rồi trả `bytes=None`, `hash="missing"`. Materialize daily vẫn thành công, output là `UNKNOWN,21150000,4,4`; **freshness tận nguồn = fresh**. Thay vì báo nguồn không đọc được, pipeline âm thầm coi dimension không có dữ liệu. Đây là hồi quy so với việc để IOException nổi lên; Scaladoc “None ⇔ file không tồn tại” cũng sai.
- **Ảnh hưởng:** lỗi quyền đọc/đường dẫn/IO khi chụp snapshot có thể biến thành lần chạy xanh với dữ liệu thiếu, thay output đang phục vụ. Với raw, `relevant` còn có thể loại input đọc lỗi khỏi manifest như thể nó không chạm ngày nào.
- **Đề xuất:** phân biệt file thật sự không tồn tại với **đọc thất bại**; không chuyển mọi IOException thành None. Đọc trực tiếp, chỉ xử lý ngoại lệ mất file theo chính sách đã định, propagate lỗi khác và dừng **trước publish**. Nêu rõ input nào bắt buộc (dimension/upstream) và input nào được phép vắng. Thêm regression kiểm tra lỗi đọc giữ nguyên output + lineage, trạng thái giám sát không báo fresh như một lần đọc thành công. [Java Files.readAllBytes](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#readAllBytes(java.nio.file.Path)) quy định IOException khi đọc lỗi; không nên làm mất tín hiệu này.

#### R2-MAJOR-02 — Hai materialize cùng partition có thể gán outputHash của run B cho input của run A

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:119`, `:120`, `:122`; `11-data-platform/ch31-data-pipeline-architecture.md:356`.
- **Tái hiện, probe 18:** A chụp dimension A, tính và rename output A; dừng **ngay trước** `Files2.hash(target)`. Thêm C9=Retail vào dimension rồi B materialize **cùng daily@15**, ghi output B và lineage B. Khôi phục dimension A; cho A tiếp tục hash và append lineage. A ghi hash **`f7d62677df205b69` của B**, trong khi hash đúng của output do A tính là **`584acb109416c72d`**. Dòng lineage mới nhất là A, input hash khớp dimension A hiện tại, output hash khớp file B → cả `status` và **`freshness` đều fresh**, nhưng serving có `Retail,1150000,3,3` từ dimension B thay vì kết quả đúng của A. Không sửa raw.
- **Ảnh hưởng:** đây không chỉ là last-writer-wins hoặc mismatch sẽ bị status phát hiện. Một materialization ghi input của mình và output hash của run khác, che mất mismatch; chạy backfill thường sẽ skip partition sai. Bài đã cảnh báo thiếu khóa, nhưng chưa chỉ ra hoặc ngăn được kiểu false-fresh này.
- **Đề xuất:** hash **đúng byte output do chính run tạo**, cùng serialization được publish (hoặc hash file tạm riêng trước rename), không đọc lại mutable target để xây record. Khóa theo partition để bao trùm phần publish + append lineage hoặc từ chối chạy trùng; mô tả chính xác bảo đảm khi chưa có transaction. Chỉ sửa cách hash chưa làm commit transactional, nhưng ít nhất không nhận output của run khác làm output của mình. Thêm regression lịch xen kẽ trên: phải serialize/từ chối, hoặc phát hiện stale/mismatch, không được fresh với dữ liệu B và input A.

#### R2-MAJOR-03 — `materializationId=None` làm impact thiếu downstream mà không báo không đầy đủ

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:61`, `:75`, `:92`, `:156`; `11-data-platform/ch31-data-pipeline-architecture.md:363`, `:519`.
- **Tái hiện, probe 14:** thêm lô muộn O99 giá 1 đồng (append-only). Tính/rename staging mới nhưng chưa append lineage, mô phỏng đúng cửa sổ chết giữa hai bước đã mô tả trong bài. Cho daily materialize: staging bytes không khớp lineage cũ nên input ID **None**; daily thực sự chứa đóng góp O99 (`Retail,700001,...`). Materialize staging bình thường để phục hồi, ra **cùng bytes** daily đã dùng. Sau đó daily **fresh**, backfill thường **skip cả hai**, provenance in `? staging_orders@...` đúng chủ đích. Nhưng `impact(late.csv)` chỉ trả **`Vector(staging_orders@2026-10-15)`**, bỏ daily đang phục vụ 1 đồng từ chính file này, không có cờ incomplete/unknown.
- **Phân biệt:** không khẳng định kết quả doanh thu sau phục hồi là sai hoặc Fresh bắt buộc chứng minh lineage đầy đủ. Lỗi là API impact trả một danh sách có vẻ đầy đủ để quyết định cách ly/backfill, trong khi biết chắc còn cạnh không rõ nguồn gốc; cảnh báo chỉ có ở provenance không giúp caller đang dùng `impact`. Đây là đường thiếu nguồn còn lại của MAJOR-02, không phải phản bác việc truy ID khi ID hợp lệ.
- **Đề xuất:** khi materialize downstream, từ chối upstream chưa có ID hợp lệ và yêu cầu phục hồi upstream trước; hoặc nếu hỗ trợ lineage không rõ thì trả **kết quả impact có trạng thái incomplete/unknown** và tập partition không thể loại trừ, nối nó vào CLI/runbook. Có cách phát hiện và xử lý downstream đã lưu None (normal backfill hiện không tự sửa). Không đoán một historical ID chỉ từ hash vì cùng byte có thể có provenance khác. Giữ regression về phục hồi upstream cùng hash, không chỉ test provenance in dấu `?`.

#### R2-MAJOR-04 — Dry-run vẫn gọi downstream “chắc chạy” dù upstream có thể khôi phục hash cũ

- **Vị trí:** `solutions/ch31-code/src/main/scala/vn/scalaeng/platform/solutions/Solutions.scala:90`, `:98`, `:102`; `solutions/ch31-solutions.md:69`.
- **Tái hiện, probe 17, hoàn toàn append-only:** build serving ban đầu; thêm lô O99 PAID rồi **chỉ** materialize staging → daily stale vì input đổi. Thêm file raw thứ hai có O99 REFUNDED, cùng sku và cùng `updated_at`: theo logic xung đột của lab, staging sẽ loại O99, khôi phục **đúng output hash ban đầu**. Trước backfill, plan in `daily_revenue: chắc chạy — stale: input đổi staging_orders@2026-10-15`. Backfill thật materialize staging, hash trở về phiên bản daily đã tiêu thụ, rồi **daily skip fresh**. Không xóa/sửa file raw hay sửa code.
- **Ảnh hưởng:** chia ba loại đã sửa probe vòng 1 nhưng nhánh “mọi status khác Fresh đều chắc chạy” vẫn không đúng. Stale do input upstream có thể biến mất sau khi upstream tính lại. Diễn giải chi phí là cận trên đã đúng, song không làm nhãn “chắc” trở thành đúng.
- **Đề xuất:** khi upstream còn có thể thay output, phân loại cả downstream **đang stale vì input đó** là có điều kiện; so với **hash downstream đã tiêu thụ**, không chỉ nói hash upstream có đổi so với hiện tại. Chỉ ghi “chắc chạy” khi có lý do không thể được upstream xử lý trước đó làm hết hiệu lực (hoặc đổi nhãn thành ứng viên cận trên, bỏ hẳn bảo đảm chắc chắn). Thêm regression khôi phục hash bằng xung đột append-only này cùng probe toàn dòng bị loại đã có.

#### R2-MAJOR-05 — Script Dagster in đúng lỗi partition nhưng trả exit 0 cho toàn lần chạy

- **Vị trí:** `11-data-platform/architecture-lab/dagster/run-dagster.sh:17`, `:18`, `:20`, `:22`; đối chiếu mục mã thoát `11-data-platform/ch31-data-pipeline-architecture.md:112`, `:463`.
- **Tái hiện thật:** wrapper DAGSTER gọi Dagster thật cho partition 15 và 17, chỉ trả **42** cho partition 16. Script in `partition 2026-10-16: exit=42 0 materialization`, tiếp tục chạy 17 thành công, in lineage và serving 17, rồi **exit 0** (mã thoát của `cat` cuối cùng). Bản sửa đã bắt được `$?` để hiển thị, nhưng không propagate thất bại cho caller.
- **Ảnh hưởng:** CI/người học chỉ kiểm mã thoát toàn script sẽ coi kiểm chứng thành công dù thiếu một partition; trái với bài học về mã thoát orchestration. MAJOR-04 về xóa dữ liệu đã được sửa, issue này là về báo kết quả.
- **Đề xuất:** hoặc fail-fast sau khi in vị trí log và trả nonzero của partition; hoặc giữ chạy đủ để thu log, cộng dồn trạng thái lỗi rồi `exit` nonzero sau phần tổng kết. Không để `cut`/`cat` cuối script quyết định thành bại của ba run. Thêm kiểm tra một partition giữa thất bại và partition cuối thành công; sentinel WORK phải vẫn còn.

### MINOR còn lại/phát hiện thêm

#### R2-MINOR-01 — Ref có dấu @ nhưng ngày không hợp lệ làm freshness ném lỗi thay vì trả lý do

- **Vị trí:** `11-data-platform/architecture-lab/src/main/scala/vn/scalaeng/platform/Orchestrator.scala:48`, `:98`.
- **Tái hiện, probe 13:** custom asset khai báo `Source("asset", "staging_orders@not-a-date", <file có thật>)`; materialize thành công, status cục bộ Fresh. `freshness` nhận biết được tên asset rồi ném **`DateTimeParseException: Text 'not-a-date' could not be parsed at index 0`**, trong khi ref thiếu dấu @ và tên asset không biết đều được đổi thành Stale có lý do (probe 12).
- **Đánh giá:** fail-closed, không false-fresh; không có ref lỗi này trong hai asset chuẩn nên xếp MINOR về hợp đồng API mở rộng/diagnostics.
- **Đề xuất:** validate cả tên/ngày trong parser ref, hoặc bắt parse date thành `Stale("tham chiếu không hợp lệ ...")`; có thể từ chối sớm ở materialize để không publish output với ref sai. Giữ test chu trình/tên lạ đã qua.

### Tổng hợp các probe tấn công và giới hạn snapshot

| Probe | Quan sát thật | Đánh giá |
|---|---|---|
| 1 — raw bị xóa | Staging stale, daily local fresh nhưng freshness stale upstream | Đã sửa MAJOR-01 |
| 2 — raw đổi tên | Có cả input mới và input mất, lan tới freshness daily | Đã sửa MAJOR-01 |
| 3 — chỉ staging tính lại sau raw bị xóa | Impact vẫn gồm daily cũ; provenance daily không đổi | ID hợp lệ sửa được MAJOR-02 |
| 4 — lô muộn append-only | SLA bắt daily cũ trước backfill; sau chỉ staging tính lại, daily không nhận vơ nguồn mới | Đã sửa đường gốc của MAJOR-01/02 |
| 5 — input mới toàn dòng bị loại | Daily “có thể chạy”, runtime skip, dry-run không ghi | Đã sửa phản ví dụ gốc MAJOR-05 |
| 6 — dimension ABA trong compute | Output từ A, hash input A, freshness fresh đúng | Snapshot sửa được race vòng 1 |
| 7 — raw đến trong compute | Output và lineage cùng snapshot cũ, không ném lỗi; status sau stale input mới | Chấp nhận snapshot cũ có chủ đích, không publish một run bị guard từ chối |
| 8 — raw ABA | Output vẫn từ snapshot gốc, status fresh đúng | Stress ngoài hợp đồng raw immutable, không dùng để cáo buộc lỗi độc lập |
| 9 — dimension khai báo/đọc lén | Khai báo đúng bắt stale; đọc lén ngoài sources không bị theo dõi | Giới hạn hợp đồng Asset, đã được bài nói rõ |
| 10 — JSONL hỏng | Status và backfill force đều dừng | Đúng fail-closed, runbook mới đã có |
| 11 — cycle hai nút/tự tham chiếu | Stale chu trình, không overflow/lặp vô hạn | Chống chu trình hoạt động |
| 12 — asset tên lạ/ref thiếu @ | Stale kèm lý do rõ | Hoạt động đúng |
| 13 — ngày trong ref sai | DateTimeParseException | R2-MINOR-01 |
| 14 — upstream ID None, rồi phục hồi cùng bytes | Provenance có `?`, nhưng impact thiếu daily; normal backfill skip | R2-MAJOR-03 |
| 15 — snapshot file đang ghi | Đọc được prefix hợp lệ khi producer giữ FileChannel mở; output có O99, chưa có O100; lineage hash đúng prefix; sau producer hoàn tất, status stale input đổi | Bảo đảm **cùng byte hash/compute** đúng; không phải snapshot của cả lô đã ghi xong |
| 16 — lỗi đọc snapshot | AccessDeniedException bị đổi thành missing, daily publish UNKNOWN và fresh | R2-MAJOR-01 |
| 17 — upstream trở về hash cũ | Plan “chắc chạy”, daily thực tế skip | R2-MAJOR-04 |
| 18 — hai materialize cùng partition | A lưu input A + outputHash B; serving B, dimension A, freshness fresh | R2-MAJOR-02 |

Với probe 15, reviewer **không** gán việc nhìn thấy prefix thành sai lineage: hash và compute thật sự dùng cùng byte đã đọc. Nhưng `readAllBytes` không thay thế giao thức publish input hoàn chỉnh hoặc snapshot đồng thời nhiều file. Writer đang hoàn thành file hiển thị trong raw nằm ngoài giả định “raw đã nhận thì bất biến”; nên nói rõ ingestion ghi file tạm **ngoài tập `rawFiles`**, đóng/kiểm tra rồi atomic rename vào raw, dimension cũng publish nguyên tử hoặc dùng phiên bản bất biến. Status có thể nhìn thấy prefix trước khi writer hoàn tất; sau hoàn tất probe đã thấy stale đúng, không phải false-fresh bền vững. Không yêu cầu triển khai distributed snapshot cho lab này.

### Điều kiện đóng vòng 2

Tác giả xử lý R2-MAJOR-01..05 và thêm regression tương ứng; sửa/giới hạn hợp đồng ref theo R2-MINOR-01. Giữ các test và diễn giải đã sửa đúng ở vòng 1, không bỏ snapshot hay quay lại so hash hai đầu. Chạy lại hai suite, demo và kiểm chứng failure exit của script; lead chạy sbt và kiểm chứng cuối trước khi cập nhật file dùng chung. Reviewer **chưa đánh dấu chương hoàn tất**.

