# Handoff T06 — Chương 21: Data Formats và Serialization

Tác giả: **Codex** · Reviewer: **Claude Code** · Ngày kiểm chứng: **17/10/2026 (UTC+7)**.

**Lịch sử bàn giao:** mục 1–5 ghi nhận vòng 1. Mục **6. Vòng 2** ở cuối cập nhật kết quả sửa review, test và trạng thái sbt/review hiện tại; các ghi chú “chưa review/chưa chạy sbt” ở vòng 1 đã được thay thế bằng thông tin vòng 2.

Đã hoàn thành phần tác giả của brief: bài tiếng Việt đủ 17 mục, JSONL/CSV/Avro/Parquet chạy file thật, schema evolution ghi/đọc cả hai chiều, benchmark cùng một triệu đơn hàng, sáu bài tập có lời giải và test. **25 test lab + 12 test lời giải pass, 0 fail, 0 ignored, 0 cảnh báo compiler**. Chưa đánh dấu hoàn tất review chéo; sbt cần lead chạy theo AGENTS.md mục 5. Không commit, không sửa file dùng chung hay chương 13/15/17/19.

## 1. File đã tạo

Tất cả source/tài liệu bàn giao là file mới trong phạm vi T06:

- `08-data-processing/ch21-data-formats.md` — đủ 17 mục; kiến thức nền, bảng lựa chọn, compatibility hai chiều, nullable, tiền/decimal/timezone, benchmark và giới hạn.
- `08-data-processing/formats-lab/project.scala`, `build.sbt`, `project/build.properties`, `.gitignore` — Scala CLI/sbt một module, sbt 1.12.11, ignore output lớn.
- `08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/Order.scala` — JSON codec, validation tích lũy, conversion, generator.
- Cùng package: `TextOrders.scala`, `AvroOrders.scala`, `ParquetOrders.scala` — file I/O, codec nén, projection, quản lý resource.
- Cùng package: `Evolution.scala` — tám schema v2 độc lập so với cùng v1; compatibility checker và file reader/writer thật.
- Cùng package: `Benchmark.scala` — `Format`, `Digest`, `Demo`, `Benchmark`; tám biến thể, warm-up, ba lượt đo, CSV raw và median.
- Cùng package: `ProjectionProbe.scala` — bọc InputFile, đếm logical API bytes ở full/projection, không gọi đó là physical disk I/O.
- `08-data-processing/formats-lab/src/test/scala/vn/scalaeng/formats/FormatsSuite.scala` — 25 test.
- `08-data-processing/formats-lab/verify-sources.py`, `verify-docs.py` — kiểm tra Maven metadata/POM và link tài liệu.
- `08-data-processing/formats-lab/verification/` — `tests.txt`, `demo.txt`, `evolution.txt`, `benchmark.txt`, `benchmark-raw.csv`, `benchmark-summary.md`, `projection.txt`, `projection-plain.txt`, `environment.txt`, `sources.txt`, `doc-sources.txt`.
- Cùng `verification/`: tám cặp snapshot metadata XML + POM cho `avro`, `parquet-avro`, `hadoop-client-api`, `hadoop-client-runtime`, `commons-csv`, `circe-parser_3`, `munit_3`, `slf4j-nop`; tên POM chứa version tương ứng. Snapshot giữ nội dung từ upstream, gồm whitespace gốc.
- `solutions/ch21-solutions.md` — giải thích sáu bài, giới hạn, gợi ý tám câu hỏi kiểm tra.
- `solutions/ch21-code/project.scala` — chỉ import source production lab, directive/dependency tập trung ở project lời giải.
- `solutions/ch21-code/src/main/scala/vn/scalaeng/solutions/ch21/Exercises.scala` — Money, Presence, DecimalWire, TimestampWire, RenameBridge, Quarantine.
- `solutions/ch21-code/src/test/scala/vn/scalaeng/solutions/ch21/ExercisesSuite.scala`, `solutions/ch21-code/test-output.txt` — 12 test và output thật.
- `.team/handoffs/T06-ch21-data-formats.md` — file này.

`.cache/coursier` tại gốc repo là cache build đã được ignore theo AGENTS.md; `.scala-build`/`.bsp` là output tự sinh đã ignore. `formats-lab/out/` chứa dữ liệu benchmark, demo và evolution tái tạo được, được ignore riêng; không bàn giao binary lớn vào git. Không xóa hay hoàn tác thay đổi đồng thời của Claude.

## 2. Lệnh và kết quả kiểm chứng thật

Các lệnh sau chạy từ **gốc repository**, dùng JVM Scala CLI chọn là **OpenJDK 21.0.12.1+1-LTS**, khác JDK 21.0.7 trên PATH. Scala 3.9.0/JDK 21 được pin trong directives; cả hai project bật `-deprecation -feature -Wunused:all -Werror`.

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning
scala-cli.bat test solutions/ch21-code --server=false --suppress-outdated-dependency-warning
```

Hai lệnh exit **0**; `test` đã compile source production/test và dùng munit thật. Suppress chỉ tắt gợi ý nâng dependency, không tắt cảnh báo compiler. Trích nguyên văn lần test cuối:

```text
Test run vn.scalaeng.formats.FormatsSuite finished: 0 failed, 0 ignored, 25 total 9.326s
Test run vn.scalaeng.solutions.ch21.ExercisesSuite finished: 0 failed, 0 ignored, 12 total 8.639s
```

Nguồn: [lab tests](../../08-data-processing/formats-lab/verification/tests.txt), [solution tests](../../solutions/ch21-code/test-output.txt).

Phạm vi lab test:

- Tám round-trip (bốn format × hai codec), so từng Order: None, chuỗi rỗng, sentinel `\N`, tiếng Việt, quote, CRLF, Long.MaxValue, Instant trước epoch.
- Tổng một cột đúng ở mọi format/codec.
- Tám biến thể evolution: vừa check schema compatibility, vừa tạo file v1/v2 và đọc schema đối diện; kiểm tra default, alias, kiểu Long thực và các exception thật.
- Validation tích lũy năm lỗi, exact money/overflow, timezone/ngày báo cáo, missing/null/sai kiểu JSON, CSV header đảo.
- Projection đọc ít API bytes trên cột note rộng, mọi format đọc dataset rỗng, mọi writer từ chối timestamp sub-millisecond.

Phạm vi solution test: hai test cho mỗi bài tiền, tri-state JSON, Avro decimal bytes, offset/millis/DST, bridge rename ghi/đọc file thật, streaming JSONL→Parquet/quarantine (có zero accepted và path input trùng output).

Các main được chạy lại trên source cuối, đều exit **0**:

```powershell
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --main-class vn.scalaeng.formats.Demo -- 08-data-processing/formats-lab/out/demo
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --main-class vn.scalaeng.formats.EvolutionMain -- 08-data-processing/formats-lab/out/evolution
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --java-opt -Xmx512m --main-class vn.scalaeng.formats.Benchmark -- 08-data-processing/formats-lab/out/benchmark 1000000 3
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --main-class vn.scalaeng.formats.ProjectionProbe -- 08-data-processing/formats-lab/out/benchmark/orders.parquet
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --main-class vn.scalaeng.formats.ProjectionProbe -- 08-data-processing/formats-lab/out/benchmark/orders.gzip.parquet
```

Demo: cả tám biến thể `roundTrip=true amountMinor=306`. Evolution có output/lỗi nguyên văn trong [evolution.txt](../../08-data-processing/formats-lab/verification/evolution.txt), bài mục 9–10 trích đúng những đoạn đó, không bịa compiler/runtime output.

### Benchmark

Máy: Intel Core i7-9750H @ 2.60 GHz, 12 logical processors, Windows 11 build 26200, amd64, heap JVM 512 MiB. Không xác minh được RAM vật lý/loại ổ đĩa vì CIM AccessDenied; ghi rõ trong bài và `environment.txt`.

Cùng một triệu Order cho mọi biến thể; warm-up 10.000 hàng, ba lượt đo thứ tự trộn seed cố định. **24 mẫu đo thật**, mỗi full scan khớp `Digest(1000000,50099500000,7270001777214734632)`, projection khớp tổng 50099500000. Thời gian bên trong JVM, gồm đóng writer, không tính compile/startup CLI. Median ba mẫu; file byte từ lượt cuối:

| Biến thể | Byte | Ghi ms | Full read ms | Amount read ms |
|---|---:|---:|---:|---:|
| JSONL plain | 130390896 | 1688.645 | 2362.087 | 1136.395 |
| JSONL gzip | 13156809 | 2603.211 | 2423.098 | 1314.029 |
| CSV plain | 67790956 | 1519.403 | 2075.567 | 1149.925 |
| CSV gzip | 10738389 | 2703.487 | 2205.475 | 1126.027 |
| Avro null codec | 37332472 | 596.233 | 440.063 | 139.657 |
| Avro deflate 6 | 9732283 | 1964.789 | 576.301 | 263.426 |
| Parquet UNCOMPRESSED | 34794470 | 759.499 | 445.263 | 66.632 |
| Parquet GZIP | 7867325 | 2347.409 | 494.407 | 74.063 |

Bằng chứng: [transcript](../../08-data-processing/formats-lab/verification/benchmark.txt), [raw CSV](../../08-data-processing/formats-lab/verification/benchmark-raw.csv), [summary](../../08-data-processing/formats-lab/verification/benchmark-summary.md). `out/benchmark/raw.csv` và `summary.md` là bản gốc chương trình sinh, đã copy nguyên nội dung sang `verification/`.

Probe byte riêng, cùng file cuối benchmark:

```text
rows=1000000 amountMinor=50099500000 fullApiBytes=34758415 projectedApiBytes=8007812
rows=1000000 amountMinor=50099500000 fullApiBytes=7832135 projectedApiBytes=1522363
```

Dòng đầu UNCOMPRESSED, dòng sau GZIP. Đây là API byte, không phải physical disk byte. Không suy kết quả thành tốc độ mọi hệ thống: dữ liệu lặp dễ nén, đọc sau ghi có OS cache, warm-up ngắn, không CPU isolation/JMH, không fsync, đường full read còn tạo object/validate mà projection không làm.

### Nguồn và kiểm tra tài liệu

```powershell
python 08-data-processing/formats-lab/verify-sources.py
python 08-data-processing/formats-lab/verify-docs.py
```

Đã chạy thành công; script nguồn kiểm tra version có trong metadata và tải POM tương ứng; script docs ghi HTTP **200 cho cả 13 link tài liệu** hiện có trong bài. [sources.txt](../../08-data-processing/formats-lab/verification/sources.txt) ghi thời gian UTC 16/10/2026 17:58:49, tương ứng **17/10/2026 UTC+7**. Python TLS được dùng vì Invoke-WebRequest/curl Schannel sandbox không có credential. Không dùng search.maven.org.

## 3. Phiên bản đã chốt và nguồn kiểm chứng

Tất cả kiểm tra ngày **17/10/2026 UTC+7**; pin đã test, không khẳng định tất cả là latest. Link POM bên dưới đi cùng snapshot metadata trong `verification/`:

| Dependency | Pin | Nguồn chính thức |
|---|---|---|
| Scala / JDK / sbt | 3.9.0 / 21 / 1.12.11 | Theo chuẩn repo; Scala/JDK đã chạy, sbt chỉ cấu hình |
| circe-parser_3 | 0.14.17 | [POM](https://repo1.maven.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom) |
| Commons CSV | 1.14.1 | [POM](https://repo1.maven.org/maven2/org/apache/commons/commons-csv/1.14.1/commons-csv-1.14.1.pom) |
| org.apache.avro:avro | 1.12.2 | [metadata](https://repo1.maven.org/maven2/org/apache/avro/avro/maven-metadata.xml), [POM](https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.2/avro-1.12.2.pom) |
| org.apache.parquet:parquet-avro | 1.18.1 | [metadata](https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/maven-metadata.xml), [POM](https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.18.1/parquet-avro-1.18.1.pom) |
| hadoop-client-api | 3.4.2 | [POM](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.pom) |
| hadoop-client-runtime | 3.4.2 | [POM](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.pom) |
| slf4j-nop | 2.0.17 | [POM](https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/2.0.17/slf4j-nop-2.0.17.pom) |
| munit_3 | 1.2.4 | [POM](https://repo1.maven.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom) |

Avro và Parquet **Java API**, không artifact Scala `_2.13`. circe/munit có `_3` riêng đã xác minh. Không dùng hay tuyên bố đã kiểm chứng vulcan/Scala-native wrapper.

### Parquet trên Windows

API trực tiếp chạy thành công: `AvroParquetWriter`, `AvroParquetReader`, NIO `LocalInputFile`/`LocalOutputFile`, `PlainParquetConfiguration`, GenericRecord, GZIP/UNCOMPRESSED, projection. Không cần DuckDB fallback hoặc winutils trong đường chạy local đã thử.

Vẫn có `parquet-hadoop` và cần Hadoop API/runtime trên classpath cho chữ ký lớp/codec/reflection. Hai JAR Hadoop lần lượt **19.665.963 / 30.502.934 byte** chưa kể dependency khác; bài nói rõ tải/classpath nặng. Không suy từ local NIO sang HDFS/S3/Hadoop FileSystem. Avro 1.12.2/Hadoop 3.4.2 là pin trực tiếp; kết quả kiểm chứng áp dụng đúng phối hợp pin này.

## 4. Đề xuất cập nhật file dùng chung — chỉ lead áp dụng

- **PROGRESS.md:** thêm Ch21 với bài đủ 17 mục, formats-lab, 25 test lab + 12 lời giải, benchmark một triệu × tám biến thể × ba lượt, schema evolution hai chiều. Tạm ghi “đã chạy Scala CLI; chờ review chéo và sbt lead”, chưa đánh dấu chương hoàn tất.
- **Compatibility matrix:** thay dòng Avro/Parquet `❓` bằng hai dòng: Avro **1.12.2**, Java API gọi trực tiếp Scala **3.9.0/JDK 21**, file/codec/schema resolution đã test; parquet-avro **1.18.1**, Java API qua NIO+PlainParquetConfiguration trên Windows đã test, **có điều kiện** vì Hadoop client API/runtime **3.4.2**, dependency nặng, không kiểm chứng HDFS/S3/winutils path. Ghi ngày17/10/2026, link metadata/POM từ bảng trên. Đánh dấu việc nghiên cứu Ch21 đã thực hiện sau khi lead review, không suy ra hỗ trợ Scala-native wrappers.
- **Glossary:** serialization/deserialization, writer schema, reader schema, schema evolution, backward/forward/full và transitive compatibility, nullable/default, logical type, precision/scale, row group/column chunk/page, projection, quarantine, dual-write, minor unit. Có thể lấy định nghĩa ngắn từ mục4 chương; đặc biệt full ở lab là pairwise, không tự transitive.
- **exercises/README.md:** nếu lead cập nhật chỉ mục, dẫn sáu bài Ch21 và `solutions/ch21-solutions.md`.

## 5. Chưa kiểm chứng và giới hạn cần giữ khi review

- **sbt 1.12.11 chưa chạy**: theo AGENTS.md mục5, sandbox chặn BootServerSocket; Codex dùng Scala CLI bắt buộc `--server=false`. Lead chạy `sbt.bat test` trong `08-data-processing/formats-lab` và chạy lại hai lệnh Scala CLI khi review. Build là một module, không cần module wiring phức tạp.
- **Chưa review chéo**: Claude cần ghi `.team/reviews/T06-ch21-data-formats-review.md` theo quy trình; Codex chưa tạo thay reviewer hoặc gắn nhãn hoàn tất.
- Không chạy Spark/Scala2.13, HDFS, S3, cluster, Hadoop filesystem Windows, mọi codec khác ngoài các codec nêu trên. Không cài winutils; không suy ra các đường đó hoạt động từ kết quả NIO.
- Bài3 lời giải kiểm chứng Avro decimal **conversion bytes**, không phải file decimal end-to-end. File lab dùng amountMinor Long + currency, có file round-trip thật; bài nói rõ ranh giới này.
- Migration quarantine là bài tập streaming, chưa có atomic publication đa file, schema registry hoặc xử lý symlink/hardlink path aliases; mini project giao thêm staging/manifest. Không trình bày bài giải như hệ thống production đã hoàn chỉnh.
- Số đo không phải cold disk/durability/JMH; không xác minh physical RAM/disk; dữ liệu sinh dễ nén; checksum có thể collision. Đã ghi rõ và giữ 24 mẫu thật, không chọn riêng lượt nhanh nhất.
- Các trang Avro spec/Java guide truy cập được là bản **1.12.0**; artifact chốt **1.12.2** được kiểm chứng bằng metadata/POM và test thật. Website docs circe trả403 qua Python; bài dùng link **source Decoder tag v0.14.17 chính thức**, đã check200, thay vì khẳng định đã tải website đó.

Không có BLOCKER đã biết trong phần tác giả tại thời điểm bàn giao. Các giới hạn trên là phạm vi kiểm chứng đã công khai, cần giữ khi lead cập nhật trạng thái chung.

## 6. Vòng 2 — Sửa review của Claude (17/10/2026, UTC+7)

Đã xử lý **cả 2 MAJOR và 4 MINOR** trong [review T06](../reviews/T06-ch21-data-formats-review.md). Chờ lead kiểm tra lại phần sửa; không tự đánh dấu chương hoàn tất hoặc sửa file dùng chung. Reviewer đã chạy sbt vòng 1 **25/25 pass**; Codex chạy Scala CLI vòng 2 **27/27 lab + 12/12 lời giải**, không fail/ignored/cảnh báo compiler.

### Đối chiếu từng mục review

1. **MAJOR 1 — Code trong bài:** bổ sung **20 khối Scala**, mỗi khối 5–20 dòng, là đoạn liên tục từ source có link và giải thích trước/sau. Các đoạn bao phủ JSONL encode/decode; một Order JSONL/CSV với None/rỗng/chuỗi thật `\N`; CSV writer/reader và tag hiện diện; Avro writer/reader/codec; Parquet NIO writer/reader/codec; compatibility checker và file v1 → reader v2; circe decoder và ADT ba trạng thái; Money/decimal; schema timestamp-millis và timezone; projection truyền vào reader; khung benchmark warm-up/lượt đo/median. Bốn schema JSON đầy đủ (v1, v2 default, v2 alias, v2 union null) được xuất từ đối tượng Schema thực sự dùng trong chương trình, không dựng schema khác chỉ để minh họa. Script kiểm tra source và schema pass, chi tiết bên dưới.
2. **MAJOR 2 — Văn phong:** viết lại 3.1–3.7 và mục 8 theo vấn đề → lý do → cơ chế, chia câu/đoạn theo một ý, giải thích vai trò của mỗi API trước khi đọc code. Giữ bảng compatibility 8 biến thể, dữ liệu/số đo gốc, lỗi runtime thật, giới hạn decimal conversion bytes, khác biệt logical API bytes với physical I/O và các giới hạn benchmark.
3. **MINOR 3 — Tiêu đề/trạng thái:** đưa tiêu đề `#` lên đầu; dòng phiên bản ngay sau. Ghi rõ “đã review chéo bởi Claude”, sbt **25/25 do reviewer chạy trước vòng sửa**, Scala CLI vòng 2 **27/27 + 12/12**. Hai test mới chưa được chạy bằng sbt; cần lead chạy lại, không gán kết quả sbt cũ cho 27 test mới.
4. **MINOR 4 — Lộ trình:** thêm “Vị trí trong lộ trình” sau tiên quyết, nối Ch19 JSONL/CSV với Ch22 Spark Scala 2.13. Ranh giới là **file Parquet/Avro**, không chia sẻ class/artifact qua classpath; dẫn compatibility matrix mục 5. Chưa tuyên bố đã chạy Spark đọc các file này.
5. **MINOR 5 — Lệnh cho người học:** đầu mục 7 dùng `cd 08-data-processing/formats-lab` rồi `scala-cli test .`, hoặc `sbt test` trong lab. Cache và cờ sandbox đặt ở ghi chú phụ với lệnh Codex đã thực sự chạy. Mục 5 chạy main ví dụ mới.
6. **MINOR 6 — Windows/Hadoop:** đã chạy lại trên **Windows 11**, `HADOOP_HOME=<unset>`, thuộc tính JVM `hadoop.home.dir=<unset>`, không có `winutils.exe` trên PATH. Cả Avro, Avro deflate, Parquet UNCOMPRESSED/GZIP round-trip thành công. Đường kiểm chứng dùng NIO `LocalInputFile`/`LocalOutputFile` + `PlainParquetConfiguration`; không cần đặt `HADOOP_HOME`, không cài winutils cho lab. Bằng chứng: [examples-round2.txt](../../08-data-processing/formats-lab/verification/examples-round2.txt), [environment-round2.txt](../../08-data-processing/formats-lab/verification/environment-round2.txt). Không suy kết quả này sang Hadoop FileSystem/HDFS/S3 hoặc Spark.

### File sửa/thêm trong vòng 2

- Sửa `08-data-processing/ch21-data-formats.md`: nội dung, code trích, lộ trình, lệnh, trạng thái và kết quả test mới; vẫn đủ 17 mục.
- Thêm `08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ChapterExamples.scala`: ví dụ chạy được, xuất schema đang dùng và in cấu hình môi trường.
- Sửa `08-data-processing/formats-lab/src/test/scala/vn/scalaeng/formats/FormatsSuite.scala`: thêm hai test cho round-trip ba trạng thái note và đọc v1 bằng reader v2 có default, nâng tổng 25 → 27.
- Thêm `08-data-processing/formats-lab/verify-chapter-snippets.py`: kiểm tra mỗi khối Scala có link nguồn, khớp một đoạn source liên tục (chỉ bỏ thụt đầu dòng chung), dài 5–20 dòng; đối chiếu JSON theo cấu trúc với bản xuất runtime; kiểm tra tiêu đề đầu file và 17 mục đúng thứ tự.
- Thêm `08-data-processing/formats-lab/verification/tests-round2.txt`, `examples-round2.txt`, `environment-round2.txt`, `snippets-round2.txt`, `schemas/v1.avsc`, `schemas/v2-default.avsc`, `schemas/v2-alias.avsc`, `schemas/v2-nullable.avsc`.
- Sửa `solutions/ch21-solutions.md`: cập nhật trạng thái review và link transcript vòng 2. Thêm `solutions/ch21-code/test-output-round2.txt`; không đổi source lời giải.
- Cập nhật chính handoff này. `formats-lab/out/chapter-examples/` là dữ liệu tái tạo được, được ignore. Không sửa file của T07, review Ch19/20/22, file dùng chung hay review của Claude; không commit.

### Lệnh và output thật vòng 2

Các lệnh Scala CLI chạy từ **gốc repo**, pin Scala **3.9.0**, JDK **21**, munit **1.2.4**, giữ `-deprecation -feature -Wunused:all -Werror`. Tất cả lệnh dưới đây exit **0**:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning
scala-cli.bat test solutions/ch21-code --server=false --suppress-outdated-dependency-warning
scala-cli.bat run 08-data-processing/formats-lab --server=false --suppress-outdated-dependency-warning --main-class vn.scalaeng.formats.ChapterExamples -- 08-data-processing/formats-lab/out/chapter-examples
python 08-data-processing/formats-lab/verify-chapter-snippets.py
```

Trích nguyên văn tổng kết từ [tests-round2.txt](../../08-data-processing/formats-lab/verification/tests-round2.txt) và [test-output-round2.txt](../../solutions/ch21-code/test-output-round2.txt):

```text
Test run vn.scalaeng.formats.FormatsSuite finished: 0 failed, 0 ignored, 27 total 10.561s
Test run vn.scalaeng.solutions.ch21.ExercisesSuite finished: 0 failed, 0 ignored, 12 total 9.61s
```

Trích phần đầu của main ví dụ (phần còn lại là đủ tám dòng `roundTrip=true amountMinor=306`):

```text
os=Windows 11
HADOOP_HOME=<unset>
hadoop.home.dir=<unset>
JSONL/CSV: absent, empty and literal \N preserved
v1->v2 {"id": 7, "label": "paid", "source": "web"}
```

Main xuất bốn file `.avsc` vào `out/chapter-examples/`; đã sao chép nguyên nội dung sang `verification/schemas/` trước khi chạy script đối chiếu. Script này kiểm tra nguồn trích; **hai lệnh Scala CLI test mới là bằng chứng biên dịch source**. Tổng kết script:

```text
PASS: 20 Scala excerpts; 4 runtime schemas; 17 sections
```

### Phiên bản, đề xuất cho lead và phần chưa kiểm chứng

Không đổi dependency hoặc thuật toán benchmark/codec hiện có. Giữ pin và nguồn Maven/POM đã kiểm chứng **17/10/2026** ở mục 3 của handoff; trong vòng này đã mở lại [Avro specification 1.12.0](https://avro.apache.org/docs/1.12.0/specification/) và [Parquet file format](https://parquet.apache.org/docs/file-format/). Sự khác biệt bản docs Avro 1.12.0 với artifact pin 1.12.2 vẫn được nêu rõ trong bài.

- **Compatibility matrix, lead áp dụng:** Avro **1.12.2** Java API từ Scala **3.9.0/JDK 21**, file/codec/schema resolution đã test trên Windows. Parquet **parquet-avro 1.18.1** + **hadoop-client-api/runtime 3.4.2 (shaded runtime)** chạy local NIO trên Windows 11 với `HADOOP_HOME` và `hadoop.home.dir` unset, không cần winutils trong đường này. Vẫn cần Hadoop JAR trên classpath; không chứng minh Hadoop FileSystem, HDFS, S3 hoặc Spark. Dùng link metadata/POM ở mục 3; ngày xác nhận **17/10/2026**. Không sửa matrix trực tiếp.
- **PROGRESS.md, lead áp dụng:** thay trạng thái vòng 1 bằng “đã review chéo; đã sửa MAJOR/MINOR; Scala CLI 27 + 12 test pass, 20 trích đoạn/4 schema đã đối chiếu; sbt reviewer vòng 1 25 pass, chờ chạy lại sau sửa”. Không tự gắn ✅ trước khi lead kiểm tra vòng này.
- **Glossary/exercises:** giữ đề xuất mục 4; không có thuật ngữ hoặc bài tập mới cần bổ sung ngoài danh sách đã giao.
- **Chưa chạy lại sbt sau vòng sửa:** Codex vẫn tuân thủ giới hạn BootServerSocket ở AGENTS.md; reviewer đã xác nhận build.sbt vòng 1. Lead cần chạy lại 27 test lab bằng sbt.
- **Không đo lại benchmark một triệu hàng:** các hàm codec, evolution, generator, benchmark, projection và bộ dữ liệu không thay đổi. Bảng/raw/output benchmark vòng 1 giữ nguyên nguồn và thời điểm; vòng 2 không gán số đo mới cho chúng. Chỉ thêm main ví dụ và hai test.
- Các giới hạn còn lại của mục 5 vẫn giữ: decimal mới kiểm chứng conversion bytes, chưa chạy Spark/HDFS/S3/cluster, chưa có publication nguyên tử/quarantine production, chưa đo I/O đĩa vật lý hoặc cold cache.
