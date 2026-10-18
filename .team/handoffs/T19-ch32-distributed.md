# Handoff T19 — Chương 32 Advanced Distributed Processing

**Tác giả:** Codex · **Reviewer được giao:** Claude Code · **Ngày:** 17/10/2026.

**Trạng thái tác giả — vòng 1 (lịch sử):** hoàn tất nội dung, lab, bảy bài tập có lời giải/test và evidence. Scala CLI kiểm chứng cuối **18 pass / 0 fail / 0 ignored**, **0 compiler warning**. Demo chính, forced spill và hai JVM RDD serializer đều exit 0. **Chờ Claude review chéo và chạy sbt**, chưa đánh dấu hoàn thành cấp giáo trình, không sửa file dùng chung và không commit.

## 1. Phạm vi và danh sách file

Đã đọc AGENTS.md, `.team/plan-phase6-9.md`, brief T19, PROMPT_PROJECT phần Ch32/11–14, template, compatibility matrix, Ch22–26 và review T10/T11/T12. Ch32 đi vào chi phí di chuyển dữ liệu/mô hình lỗi/throughput-latency, chỉ tóm tắt skew và dẫn Ch25. Không sửa T15, ch31/ch33, quality-lab, 13-performance hoặc file của tác giả khác.

File tạo mới:

- `11-data-platform/ch32-advanced-distributed-processing.md`: đủ 17 mục, metadata phiên bản/trạng thái dòng đầu; lý thuyết từng chủ đề có vấn đề, cơ chế, đo, lựa chọn, ưu/nhược, khi dùng và lỗi thiết kế; số liệu/plan thật, bảy bài tập, checklist.
- `11-data-platform/distributed-lab/project.scala`
- `11-data-platform/distributed-lab/build.sbt`
- `11-data-platform/distributed-lab/project/build.properties`
- `11-data-platform/distributed-lab/verify.ps1`
- `11-data-platform/distributed-lab/src/main/resources/log4j2.properties`
- `11-data-platform/distributed-lab/src/main/scala/vn/scalaeng/distributed/Measurement.scala`
- `11-data-platform/distributed-lab/src/main/scala/vn/scalaeng/distributed/DistributedLab.scala`
- `11-data-platform/distributed-lab/src/main/scala/vn/scalaeng/distributed/DistributedDemo.scala` (DistributedDemo, MemoryDemo, SerializerDemo)
- `11-data-platform/distributed-lab/src/test/scala/vn/scalaeng/distributed/DistributedSuite.scala`
- `11-data-platform/distributed-lab/evidence/README.md`
- `11-data-platform/distributed-lab/evidence/dependency-poms.json`
- `11-data-platform/distributed-lab/evidence/official-sources.json`
- `11-data-platform/distributed-lab/evidence/lab-test.txt`
- `11-data-platform/distributed-lab/evidence/solutions-test.txt`
- `11-data-platform/distributed-lab/evidence/distributed-demo.txt`
- `11-data-platform/distributed-lab/evidence/memory.txt`
- `11-data-platform/distributed-lab/evidence/serializer-Java.txt`
- `11-data-platform/distributed-lab/evidence/serializer-Kryo.txt`
- `solutions/ch32-solutions.md`
- `solutions/ch32-code/project.scala`
- `solutions/ch32-code/src/main/scala/vn/scalaeng/distributed/solutions/Exercises.scala`
- `solutions/ch32-code/src/test/scala/vn/scalaeng/distributed/solutions/ExercisesSuite.scala`
- `.team/handoffs/T19-ch32-distributed.md` (file này).

Dữ liệu build/cache/log tạm nằm trong `.cache/t19/` và dependency trong `.cache/coursier/`, đã được `.gitignore` hiện có phủ. Không thêm ignore ngoài brief. Evidence `.txt` chỉ đổi encoding UTF-16 của PowerShell sang UTF-8, chuẩn hóa newline và bỏ ANSI; giữ nguyên số/plan/error và cả wrapper NativeCommandError.

## 2. Lệnh thực sự đã chạy và kết quả

Từ gốc repo trên Windows:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode tests
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode all
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode tests
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode demo
```

Lần `tests` đầu: **10 pass / 1 fail** do tác giả request RDD checkpoint sau các job đã chạy. Log gốc còn tại `.cache/t19/lab-test-initial-failure.log`. Đã sửa thành RDD mới, request trước job đầu tiên, materialize rồi kiểm reliable lineage; bản cuối pass. `-Mode all` sau sửa exit **0**, gồm lab/lời giải/demo/MemoryDemo/SerializerDemo Java và Kryo. Sau đó bổ sung checksum CPU thực sự được fold ở batch, test equality qua batch/partition khác nhau và in nguyên nhân capture; chạy lại `tests` và `demo` trên bản cuối đều exit **0**. Logic MemoryDemo/SerializerDemo không đổi sau số đo đã chạy.

Các lời gọi dưới script:

```text
scala-cli.bat test <repo>/11-data-platform/distributed-lab --server=false ...
scala-cli.bat test <repo>/solutions/ch32-code --server=false ...
scala-cli.bat run <repo>/11-data-platform/distributed-lab --server=false --main-class vn.scalaeng.distributed.DistributedDemo ...
scala-cli.bat run <repo>/11-data-platform/distributed-lab --server=false --main-class vn.scalaeng.distributed.MemoryDemo --java-opt=-Xmx768m ...
scala-cli.bat run <repo>/11-data-platform/distributed-lab --server=false --main-class vn.scalaeng.distributed.SerializerDemo --java-opt=-Dch32.serializer=Java ...
scala-cli.bat run <repo>/11-data-platform/distributed-lab --server=false --main-class vn.scalaeng.distributed.SerializerDemo --java-opt=-Dch32.serializer=Kryo ...
```

`...` ở đây là tập option chung, có thể kiểm trực tiếp trong `verify.ps1`:

```text
--workspace <repo>/.cache/t19/<label>-build
--java-opt=-Djava.io.tmpdir=C:/Users/VuTT/zero_to_hero_scala/.cache/t19/tmp
--java-opt=-Dspark.sql.warehouse.dir=file:///C:/Users/VuTT/zero_to_hero_scala/.cache/t19/warehouse
```

Script đặt `COURSIER_CACHE=<repo>/.cache/coursier`, `SPARK_LOCAL_DIRS=<repo>\.cache\t19\local`, TEMP/TMP=`<repo>\.cache\t19\tmp`. JVM JDK 21 do Scala CLI chọn là **Temurin 21.0.12.1** (khác `java -version` mặc định của shell 21.0.7). JVM thấy **12 CPU**, master **local[2,3]**, heap demo **8547991552 byte**, chỉ hai task thread và một JVM. Không cần Docker/mạng ngoài sau khi đã tải dependency; Spark có socket cục bộ. Scala CLI có thể tra metadata update; unit test không dùng dịch vụ ngoài.

Output tóm tắt **thật của lượt cuối**:

```text
Test run vn.scalaeng.distributed.DistributedSuite finished: 0 failed, 0 ignored, 11 total 36.039s
Test run vn.scalaeng.distributed.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 19.406s
```

Compiler bật `-deprecation -feature -Xlint:_ -Wunused:_` (cú pháp Scala 2.13 để bật mọi nhóm unused). Không có compiler warning. Hint nâng munit lên 1.3.6 vẫn giữ nguyên phiên bản 1.2.4 theo repo; WARN native Hadoop, cảnh báo task quá lớn/plan bị rút gọn và ERROR `CH32_AFTER_EXTERNAL_APPEND` đều là log runtime đã giải thích. Suite dùng retry lỗi cố ý rồi kiểm thành công, không có ignored test.

Raw log cuối: `.cache/t19/lab-test.log`, `solutions-test.log`, `demo.log`, `memory.log`, `serializer-Java.log`, `serializer-Kryo.log`. Exit code của lần tests/demo cuối được script ghi `.cache/t19/executions.jsonl`; Mode all trước đó kết thúc exit 0 và dừng ngay nếu bất kỳ entry point lỗi. Bản evidence UTF-8 giữ các output cần review trong phạm vi lab.

QA tĩnh cuối: **17/17 mục có nội dung**, **36 liên kết local hợp lệ, 0 hỏng**, **24 file bàn giao không rỗng và UTF-8 đúng**; từng dòng trong các code block output ở mục 9 đều đối chiếu được với transcript. Báo cáo máy đọc: `.cache/t19/final-audit.json`. `.cache/t19/final-audit.json` được `git check-ignore` xác nhận là ignored. Không chạy lại benchmark sau khi đã đủ số đo và chỉ còn chỉnh tài liệu.

## 3. Những điều đã đo và kiểm được

- Hash/RangePartitioner; mapValues/filter giữ, map(identity) mất metadata; range ordered và giữ đủ record.
- Hai RDD join cùng golden 2.000 dòng: consumer giữ metadata **0 read/write**, 1 stage/4 attempts; mất metadata **868/868 byte**, 2 stage/8 attempts. Setup shuffle/materialization nằm ngoài consumer và được nói rõ.
- SQL bucket join: 200 dòng, SortMergeJoin, hai scan Bucketed:true/4 buckets, **không Exchange**, 0 shuffle bytes; vẫn có Sort trong partition. Dùng external paths + catalog tạm, drop metadata trong finally.
- 60.000 record/20 key: reduceByKey **80 shuffle records, 1264 byte**; groupByKey **60000 shuffle records, 7608 byte**; tổng mỗi key được đối chiếu golden, không assert tốc độ.
- SQL partial/final HashAggregate được collect và lấy plan của cùng DataFrame. Exact distinct=**20000**, approx=**19522**, sai số quan sát **2,39%**, shuffle write **412138** vs **1872 byte**; RSD không được diễn giải thành hard bound.
- Payload object Java/Kryo=**27653/20504 byte**. RDD shuffle Java/Kryo=**152536/167527 byte**; workload sau nén cho thứ hạng ngược phép đo payload, được ghi trung thực. Closure 16 byte/1 MiB giữ trạng thái: serialized **649/1049209 byte**; là closure thô, không giả làm task size sau ClosureCleaner. Root cause capture thực tế là NonSerializableOwner.
- Cảnh báo task size thật **1415 KiB**, maximum recommended **1000 KiB**, tạo từ collection Long lớn ở một partition. Không nhầm với large task binary warning.
- Cache calls **100**, sau unpersist/count **200**; reliable checkpoint trên RDD mới rồi bỏ cache có upstreamDelta **0**, lineage ReliableCheckpointRDD.
- Retry: `collect` **100,200**, journal **100,0;100,1;200,0**; một failed attempt và attempt1 thành công, tất cả stageAttempt=0. Không gọi đây là executor death/stage retry.
- MemoryDemo forced threshold 1.000, heap 768 MiB: memory spill **3756672**, disk spill **387454 byte**, đủ 50.000 dòng; không gọi là OOM hay bằng chứng thiếu RAM thực tế.
- Batch CPU nhân tạo 2.048 bước/record; cùng 60.000 dòng, checksum **-8654025316253842092**. Batch 6k/30k, partitions2: first **52/149 ms**, total **723/343 ms**. Partitions8: first **86/276 ms**, total **1111/514 ms**. first=hoàn tất batch đầu từ submission; chưa tính source/trigger/event delay. Bảng chỉ một lượt mỗi cấu hình, có warm-up, không kiểm soát tải nền và không suy ra cluster.
- Snapshot còn **4 data/4 index files**; không diễn giải thành tổng file đã tạo hay M×R physical files. ANY cùng remote bytes=0 được giải thích rõ.
- Regression listener: latch giữ callback, marker JobEnd làm rào FIFO, loại marker/group khác, khôi phục local properties khi thành công/lỗi. Không dùng sleep đoán hàng đợi đã xong; không hứa đo đúng shared stages concurrent phức tạp/drop events.

## 4. Phiên bản và nguồn đã kiểm chứng

Ngày **17/10/2026**; năm POM đều HTTP **200**, artifact/version được parse XML (Spark kế thừa version parent), lưu URL/status/version trong `evidence/dependency-poms.json`:

| Thành phần | Phiên bản | Nguồn |
|---|---|---|
| Scala library/compiler nhánh Spark | 2.13.18 | [Scala POM](https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.18/scala-library-2.13.18.pom), compile/run thật |
| Spark SQL `_2.13` | 4.2.0 | [Spark SQL POM](https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/4.2.0/spark-sql_2.13-4.2.0.pom), SparkSession.version=4.2.0 |
| munit `_2.13` | 1.2.4 | [munit POM](https://repo1.maven.org/maven2/org/scalameta/munit_2.13/1.2.4/munit_2.13-1.2.4.pom), 18 test thật |
| hadoop-bare-naked-local-fs | 0.1.0 | [POM](https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom), [README](https://github.com/globalmentor/hadoop-bare-naked-local-fs), ghi bucket/checkpoint thật trên Windows |
| sbt | 1.12.11 | [sbt POM](https://repo1.maven.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom), **chỉ chốt build, chưa chạy launcher** |
| JDK | 21 (runtime 21.0.12.1) | directive + ENV thực; [Spark 4.2.0](https://spark.apache.org/docs/4.2.0/) |

Đã tra Spark RDD guide/tuning/configuration/monitoring/API và mã nguồn 4.2.0. `evidence/official-sources.json` có HTTP status, SHA-256, dòng liên quan của PairRDDFunctions, TaskMetrics, DAGScheduler, AsyncEventQueue, IndexShuffleBlockResolver, JavaModuleOptions và RDD.checkpoint; GlobalMentor README cũng HTTP 200. SparkEnv xác nhận closureSerializer dùng JavaSerializer, nên đổi spark.serializer sang Kryo không sửa closure capture. Link TaskMetrics ScalaDoc trả 404 đã thay bằng mã nguồn GitHub. Một lần check TaskSetManager gặp HTTP503 tạm, retry HTTP200; không để link 404 trong bài.

Cả hai project.scala và build.sbt có **đủ 15 add-opens, tập hợp khớp JavaModuleOptions 4.2.0**; không coi đó là toàn bộ JVM flags của spark-submit. Không có thay đổi dependency dùng chung hay claim hỗ trợ Scala 3 mới.

## 5. Điểm reviewer cần soi và kiểm chứng tiếp

1. Listener barrier và job-group attribution; giới hạn concurrent shared stage được ghi trong code/bài. TaskSample lưu cả stageAttempt/taskAttempt, local/remote bytes, blocks/fetch wait, deserialize time, write records/spill.
2. Join co-partitioned **consumer** so với chi phí chuẩn bị; SQL bucket dùng catalog metadata, cùng key/type/bucket count, query thật tắt AQE/automatic broadcast; không suy từ số partition.
3. Payload/closure bytes tách khỏi TaskSetManager warning và RDD shuffle bytes; Java/Kryo không phải SQL benchmark. Raw root cause capture là owner.
4. Retry output/journal và semantics idempotency; audit duplicate chỉ phát hiện, không biến journal thành transactional sink. Bài tập self-join có golden 500 dòng để bắt cardinality many-to-many.
5. Checkpoint phải request trước job đầu, materialize trước unpersist; BigInt aggregate có test overflow/cancellation/input rỗng; approx NULL/rỗng và RSD semantics.
6. Batch checksum được sử dụng và so equality qua cấu hình; benchmark ngoài unit test, không assert thời gian. Memory spill cưỡng bức được ghi rõ.
7. Claude chạy `sbt.bat test` trong `11-data-platform/distributed-lab` với JDK21 và môi trường cache/tmp thích hợp. Review bài + lời giải và viết `.team/reviews/T19-ch32-distributed-review.md` theo quy trình lead, sau đó chạy kiểm chứng cuối.

**Chưa kiểm chứng:** sbt launcher (theo AGENTS, lead chạy); cluster nhiều node; executor/process death; FetchFailed và recompute shuffle stage; network partition; speculation; NODE_LOCAL/ANY dưới cạnh tranh nhiều máy; tác dụng chỉnh spark.locality.wait; DB/HTTP transaction hoặc exactly-once sink; Linux/macOS; mini project mở rộng production. Không giả lập các điều này rồi gắn nhãn đã chạy. Không có external-service test cần assume vì lab không phụ thuộc dịch vụ ngoài.

## 6. Đề xuất file dùng chung — chỉ lead áp dụng

- **PROGRESS:** T19/Ch32 đã biên soạn đủ 17 mục, lab Scala 2.13/Spark4.2, 11+7 test Scala CLI pass, demo/spill/serializer đã chạy; trạng thái **chờ review Claude + sbt**, chưa ✅.
- **Glossary:** co-partitioning, HashPartitioner/RangePartitioner, data locality (PROCESS_LOCAL/NODE_LOCAL/ANY/NO_PREF), closure capture/closure serializer, shuffle block vs physical file, task attempt vs stage attempt, reliable/local checkpoint, relative standard deviation (RSD), first-batch completion vs end-to-end event latency.
- **Compatibility matrix:** không đổi phiên bản đã chốt; bổ sung evidence Ch32 chạy Spark4.2 `_2.13`/Scala2.13.18/JDK21 trên Windows, RDD checkpoint và bucketed table I/O với BareLocalFileSystem0.1.0; không mở rộng claim sang spark-submit/native Hadoop hoặc Scala3.
- **Exercises index:** thêm bảy bài Ch32 và link `solutions/ch32-solutions.md`/code suite. Bài3 kiểm overflow/cancellation; bài4 retry side effect thật; bài7 có benchmark ngoài correctness test.

Không sửa các file dùng chung trên. Working tree có thay đổi đồng thời của agent khác; không commit/reset/checkout hoặc xóa file của họ.

## 7. Vòng 2 — phản hồi review T19-ch32-review.md

**Ngày:** 17/10/2026. **Trạng thái:** đã xử lý **M1 và cả năm MINOR m1–m5**; chờ Claude review lại và chạy kiểm chứng cuối/sbt trên bản sửa. Không đánh dấu ✅, không sửa file dùng chung và không commit git.

### Phản hồi từng yêu cầu

| ID | Đã sửa | Bằng chứng / vị trí |
|---|---|---|
| M1 | Tiêu đề lên dòng đầu, metadata ngay sau, thêm “Vị trí trong lộ trình”. Cả 11 mục 3.x có một khối Scala 5–15 dòng trích source lab, liên kết tới output tương ứng 9.1–9.11. Tách/rút các đoạn dày về execution, serialization, shuffle, memory, checkpoint, retry, join và batch. | `ch32-advanced-distributed-processing.md`, mục 3 và 9. Có rào listener, locality/attempt extraction, partitioner, closure/owner, append trước lỗi, bucket catalog/path, combine và batch first/total. |
| m1 | Giải thích ngay sau bảng join vì sao thứ hạng ms có thể ngược số shuffle trên 2.000 dòng. Ưu tiên bằng chứng stage/task/bytes, đề nghị lặp sau warm-up, đổi thứ tự và trung vị; nối Ch34. | Mục 9: giữ metadata 1 stage/4 attempts/0 byte; mất metadata 2/8/868 byte. Thời gian lấy lại từ lượt mới. Không khẳng định đã cô lập riêng overhead. |
| m2 | Comment trong lab main/test và lời giải main/test đều chuyển sang tiếng Việt có dấu; giữ tên API, test name và chuỗi runtime. | `Measurement.scala`, `DistributedLab.scala`, `DistributedDemo.scala`, `DistributedSuite.scala`, `Exercises.scala`, `ExercisesSuite.scala`. |
| m3 | Bỏ bốn biến tích lũy driver trong `batches`, dùng `foldLeft` với `BatchRun.copy`. Tách `cpuChecksum`; demo cũng kiểm checksum bằng vector kết quả thay biến driver. | Test lô cuối 23/10, số job, equality checksum giữa hai batch/partition count và input lỗi vẫn pass. Demo bốn cấu hình cùng 60.000 dòng/checksum. |
| m4 | Đặt `spark.sql.artifact.isolation.enabled=false` khi tạo session. Thêm giải thích phạm vi lab/classpath và dẫn Ch24; ENV in cấu hình thực tế. | `DistributedLab.session`, mục 7, dòng ENV `artifactIsolation=false`. Định nghĩa cấu hình xác minh trên SQLConf Spark 4.2.0, evidence nguồn HTTP 200 ngày 17/10/2026. |
| m5 | Thêm nhánh `bucketJoin(..., readViaPath=true)`, chạy collect rồi in executed plan. Thêm đối soát kết quả và kiểm plan/bytes trong test bucket hiện có. Mục 10 trích summary và hai dòng Exchange thật. | `BUCKET_PATH` trong `distributed-demo.txt`: 200 dòng, read/write 2.286 byte, hai `Exchange hashpartitioning`; catalog 200 dòng, 0 shuffle byte, không Exchange. Đây là triệu chứng plan, không bịa exception. |

**MINOR không sửa: không có.** Vòng `while` và hai `var` bên trong `cpuChecksum` được giữ có chủ đích để tạo tải CPU cục bộ; m3 cho phép trường hợp này. Driver đã dùng trạng thái bất biến. Không chạy phép đo socket dài hay tái hiện cạn cổng cho m4; đó là giới hạn kiểm chứng, không gắn nhãn đã đo.

### File sửa / bổ sung trong vòng 2

- `11-data-platform/ch32-advanced-distributed-processing.md`.
- `11-data-platform/distributed-lab/src/main/scala/vn/scalaeng/distributed/{DistributedLab,DistributedDemo,Measurement}.scala`.
- `11-data-platform/distributed-lab/src/test/scala/vn/scalaeng/distributed/DistributedSuite.scala`.
- `solutions/ch32-code/src/main/scala/vn/scalaeng/distributed/solutions/Exercises.scala` (comment).
- `solutions/ch32-code/src/test/scala/vn/scalaeng/distributed/solutions/ExercisesSuite.scala` (comment).
- `11-data-platform/distributed-lab/evidence/{README.md,official-sources.json,lab-test.txt,solutions-test.txt,distributed-demo.txt,memory.txt,serializer-Java.txt,serializer-Kryo.txt,verification-round2.json}`.
- `.team/handoffs/T19-ch32-distributed.md` (file này); script/log/audit tạm trong `.cache/t19/`.

Không đổi `project.scala`, `build.sbt`, phiên bản dependency hay `solutions/ch32-solutions.md`. Không ghi review của Claude, các chương/lab khác hoặc file dùng chung.

### Lệnh và kết quả thực tế

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode all
powershell.exe -NoProfile -ExecutionPolicy Bypass -File 11-data-platform/distributed-lab/verify.ps1 -Mode tests
```

`all` chạy tuần tự lab test, solutions test, DistributedDemo, MemoryDemo, Java và Kryo; sáu entry point exit **0**. Sau lần tách helper CPU cuối, chạy lại `tests` để chốt correctness. Một lần gọi `all` bổ sung trong khi lượt trước đang chạy dừng ở bước mở log bị giữ; Scala CLI chưa được gọi ở lần bổ sung đó. Đã để lượt trước hoàn tất rồi chạy kiểm chứng tiếp tuần tự.

Mọi lệnh Scala CLI dưới wrapper dùng **`--server=false`**. Môi trường giữ nguyên vòng 1: COURSIER_CACHE=`<repo>/.cache/coursier`; TEMP/TMP/java.io.tmpdir, SPARK_LOCAL_DIRS, workspace và warehouse trong `.cache/t19/`. Không chạy sbt từ Codex. Review Claude vòng 1 đã ghi `sbt.bat test`: 11 pass; kết quả đó chưa thay cho kiểm chứng cuối bản vòng 2.

Output **test cuối thật**:

```text
Test run vn.scalaeng.distributed.DistributedSuite finished: 0 failed, 0 ignored, 11 total 27.308s
Test run vn.scalaeng.distributed.solutions.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 19.044s
```

Tổng **18 pass / 0 fail / 0 ignored**, **0 compiler warning**. WARN Hadoop, task size/plan và ERROR `CH32_AFTER_EXTERNAL_APPEND` là runtime đã giải thích. Comment lời giải được sửa encoding tiếng Việt sau compile; không đổi logic sau kiểm chứng. Các transcript đã chuyển UTF-16 PowerShell sang UTF-8, bỏ ANSI/chuẩn hóa newline; không đổi số hay thông báo. Mục 9 dùng output vòng 2, không ghép thời gian cũ vào kết quả mới.

Evidence máy đọc: [verification-round2.json](../../11-data-platform/distributed-lab/evidence/verification-round2.json); nguồn mới: [official-sources.json](../../11-data-platform/distributed-lab/evidence/official-sources.json). Log gốc và exit history: `.cache/t19/*.log`, `.cache/t19/executions.jsonl`; transcript vòng 1 được giữ trong `.cache/t19/round1-evidence/`.

### Phiên bản, nguồn, phần chưa kiểm chứng và đề xuất lead

Giữ Scala **2.13.18**, Spark SQL `_2.13` **4.2.0**, JDK **21** (runtime 21.0.12.1), munit **1.2.4**, BareLocalFileSystem **0.1.0**, sbt **1.12.11**. POM/link/ngày xác minh không đổi so mục 4, [dependency-poms.json](../../11-data-platform/distributed-lab/evidence/dependency-poms.json) ngày **17/10/2026**. Nguồn thêm xác minh HTTP 200 cùng ngày: [SQLConf 4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala), `ARTIFACTS_SESSION_ISOLATION_ENABLED`; SHA-256 và dòng định nghĩa đã lưu. JVM vẫn thấy 12 CPU, `local[2,3]`, remote shuffle bytes=0.

Chưa kiểm chứng mới: số socket/TIME_WAIT khi isolation tắt, tái hiện cạn cổng và benchmark lặp kiểm soát overhead. Các giới hạn cluster/executor death/FetchFailed/network/speculation/external sink/Linux/macOS/mini project production ở mục 5 vẫn áp dụng. Bản vòng 2 cần Claude review lại nội dung và chạy sbt cuối theo quy trình; tác giả không tự đánh dấu hoàn thành.

QA tài liệu cuối: **17/17 mục có nội dung**, **11/11 mục 3.x có code 6–13 dòng** (tất cả nằm trong giới hạn 5–15), **44 dòng output trích nguyên văn** đối chiếu được transcript, **51 liên kết local hợp lệ / 0 hỏng** (kể cả anchor), UTF-8 hợp lệ. Audit lưu `.cache/t19/round2-audit.json` (đã xác nhận ignored), đồng thời nằm trong `documentAudit` của `verification-round2.json`. Đoạn code mục 5 đã thay bằng thân `joined` thật; mục 8 có thêm callback JobStart/JobEnd để thấy cách marker nhả latch. Các chỉnh cuối chỉ là tài liệu.

Đề xuất **chỉ lead áp dụng**: PROGRESS ghi T19 đã xử lý M1+m1–m5, Scala CLI vòng 2 18/18, chờ review lại/sbt cuối. Glossary giữ đề xuất mục 6, có thể thêm “catalog bucket metadata” và “first-batch completion”. Compatibility matrix không đổi phiên bản; bổ sung evidence artifact isolation tắt và bucket catalog so với đọc path nếu cần. Không có thay đổi file dùng chung trong vòng này.
