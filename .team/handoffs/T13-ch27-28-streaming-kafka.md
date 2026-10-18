# Handoff T13 — Ch27 Streaming Fundamentals + Ch28 Kafka với Scala

- **Tác giả:** Codex · **Reviewer cần thực hiện:** Claude Code.
- **Ngày:** 17/10/2026. Hoàn thành phạm vi biên soạn/lab/lời giải trong brief; **chờ review chéo và lead chạy sbt**, chưa đánh dấu chương ✅.
- **Phạm vi ghi:** hai file chương `10-streaming/ch27-streaming-fundamentals.md`, `ch28-kafka-scala.md`; hai lab tương ứng; `solutions/ch27-*`, `solutions/ch28-*`; handoff này và `.cache/`. Không sửa file dùng chung, không đụng `09-spark`, không commit git. Các thay đổi của Claude tiếp tục diễn ra trong lúc task chạy, không bị reset/checkout/xóa.

## 1. Kết quả bàn giao

Hai chương đều đủ **17 mục có nội dung**, tiếng Việt; mỗi chương có **6 bài tập**, lời giải code/giải thích và **7 test lời giải**. Output trong bài được đồng bộ từ log thật, không gõ số đo dự kiến. Bộ kiểm tra nội dung xác nhận 17 mục, các đường dẫn file nội bộ, log exit 0 và không có cảnh báo compiler.

Ch27 có đồng hồ event/processing truyền tường minh, single-source watermark, late/duplicate/conflict, tumbling/sliding, session reference hữu hạn và bridge event, eviction state, tổng `BigInt`, crash simulation và bounded-queue backpressure có số đo. Không gọi bản tham chiếu session là engine online hay khẳng định state có trần cardinality.

Ch28 chọn **Java kafka-clients 4.4.0 trực tiếp**, không nâng/ép tổ hợp fs2-kafka chưa chạy. KRaft **đã chạy thật**, không cần Docker: tạo topic, gửi/đọc offset, consumer group rebalance, lag, đóng/mở consumer tại ranh giới commit, duplicate payload, malformed JSON, transient retry hết budget, conflict ID, DLT thật và chạy cả `ProducerMain`/`ConsumerMain` trên broker nhúng.

## 2. File đã tạo

Danh mục file chính:

- Hai chương: [Ch27](../../10-streaming/ch27-streaming-fundamentals.md), [Ch28](../../10-streaming/ch28-kafka-scala.md).
- Hai lab: [streaming-lab](../../10-streaming/streaming-lab/), [kafka-lab](../../10-streaming/kafka-lab/). Mỗi lab có `project.scala`, source/test và `build.sbt` + `project/build.properties` 1.12.11.
- Lời giải: [Ch27 markdown](../../solutions/ch27-solutions.md), [Ch27 code](../../solutions/ch27-code/), [Ch28 markdown](../../solutions/ch28-solutions.md), [Ch28 code](../../solutions/ch28-code/).
- Verification: POM/metadata/URL/hash, log test/demo/broker/error, lỗi compiler bản nháp, script tra nguồn/chạy kiểm chứng/kiểm tra nội dung trong `10-streaming/kafka-lab/verification/`; log Ch27 trong `streaming-lab/verification/`.
- Danh sách **từng file**: [files.txt](../../10-streaming/kafka-lab/verification/files.txt), có cả handoff này. Cache build/dependency/temp nằm trong `.cache/` đã ignore; không tính chúng là artifact giáo trình.

Không tạo/sửa `README.md` cấp repo, `PROGRESS.md`, `00-roadmap/*`, glossary, exercises index, AGENTS.md hay review của agent khác.

## 3. Lệnh đã chạy và kết quả thật

Môi trường demo: **JDK 21.0.12.1**, 12 CPU logic (in thật từ runtime), Scala CLI directive **3.9.0**. Mọi lần Scala CLI dùng `--server=false`; `COURSIER_CACHE` trỏ `<repo>/.cache/coursier`. Runner đặt `TMP/TEMP` và `java.io.tmpdir` của broker vào `<repo>/.cache/t13-tmp`.

Đã thực hiện các tác vụ bằng runner sau (các lượt được chia ra lúc đang biên soạn; mỗi log lưu command chính xác):

```powershell
python 10-streaming/kafka-lab/verification/verify_sources.py
python 10-streaming/kafka-lab/verification/run_checks.py streaming demo
python 10-streaming/kafka-lab/verification/run_checks.py kafka
python 10-streaming/kafka-lab/verification/run_checks.py broker
python 10-streaming/kafka-lab/verification/run_checks.py ch27 ch28 errors
python 10-streaming/kafka-lab/verification/check_content.py
```

Các command Scala CLI cốt lõi được runner thực thi:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '.cache/coursier'
scala-cli.bat test 10-streaming/streaming-lab --server=false --suppress-outdated-dependency-warning
scala-cli.bat run 10-streaming/streaming-lab --main-class vn.scalaeng.streaming.StreamingDemo --server=false --suppress-outdated-dependency-warning
scala-cli.bat test 10-streaming/kafka-lab --server=false --suppress-outdated-dependency-warning
scala-cli.bat test solutions/ch27-code --server=false --suppress-outdated-dependency-warning
scala-cli.bat test solutions/ch28-code --server=false --suppress-outdated-dependency-warning
scala-cli.bat run 10-streaming/kafka-lab --main-class vn.scalaeng.kafka.ErrorDemo --server=false --suppress-outdated-dependency-warning
```

Lượt broker có `RUN_KAFKA_IT=1`, command trong [broker.txt](../../10-streaming/kafka-lab/verification/broker.txt) dùng thêm `--test-only *BrokerSuite --java-opt -Djava.io.tmpdir=<repo>/.cache/t13-tmp`. Mặc định không bật opt-in: `assume` skip đúng kịch bản broker; không đếm lượt skip là pass. Khi opt-in rồi startup lỗi, suite fail và giữ lỗi, không giả vờ skip.

| Bộ kiểm chứng | Pass | Fail | Skip có chủ đích | Log |
|---|---:|---:|---:|---|
| Ch27 lab: 9 StreamingSuite + 1 PressureSuite | 10 | 0 | 0 | [tests.txt](../../10-streaming/streaming-lab/verification/tests.txt) |
| Ch27 lời giải: 6 pure + 1 IO | 7 | 0 | 0 | [tests.txt](../../solutions/ch27-code/tests.txt) |
| Ch28 core | 7 | 0 | 1 broker trong lượt mặc định | [tests.txt](../../10-streaming/kafka-lab/verification/tests.txt) |
| Ch28 broker opt-in, nhiều assertion trong 1 scenario | 1 | 0 | 0 | [broker.txt](../../10-streaming/kafka-lab/verification/broker.txt) |
| Ch28 lời giải | 7 | 0 | 0 | [tests.txt](../../solutions/ch28-code/tests.txt) |
| **Tổng test khác nhau đã chạy thành công** | **32** | **0** | — | Không cộng các lượt chạy lại |

Trích **nguyên dòng** output tóm tắt thật (thời gian là của các lượt lưu log, không cam kết hiệu năng):

```text
Test run vn.scalaeng.streaming.StreamingSuite finished: 0 failed, 0 ignored, 9 total 0.033s
Test run vn.scalaeng.streaming.PressureSuite finished: 0 failed, 0 ignored, 1 total 0.405s
Test run vn.scalaeng.solutions.ch27.ExercisesSuite finished: 0 failed, 0 ignored, 6 total 0.414s
Test run vn.scalaeng.solutions.ch27.BackpressureExerciseSuite finished: 0 failed, 0 ignored, 1 total 0.458s
Test run vn.scalaeng.kafka.CoreSuite finished: 0 failed, 0 ignored, 7 total 0.111s
Test run vn.scalaeng.kafka.BrokerSuite finished: 0 failed, 0 ignored, 1 total 13.459s
Test run vn.scalaeng.solutions.ch28.ExercisesSuite finished: 0 failed, 0 ignored, 7 total 0.129s
```

Core munit reporter in lượt mặc định có dòng `==> s ... skipped`; dòng cuối suite vẫn ghi `0 ignored, 1 total`. Handoff phân loại theo **dòng skipped thật**, không diễn giải nhầm là broker đã chạy trong lượt mặc định.

Trích output nghiệp vụ thật:

```text
JDK=21.0.12.1 processors=12
backpressure=Pressure(2,2,true,55,12)
partitions: p0 offsets=0,1,2; p1 offsets=0,1,2
group before: A=2
group joined: A=1 B=1 revoked=1
group left: A=2
lag: end=3 committed=1 lag=2
at-most restart: first offset=1 skipped=0
at-least restart: first offset=0 sink duplicate=true total=100
pipeline: statuses=processed,processed,dead-letter,dead-letter,dead-letter attempts=3 total=100 committed=5
DLT: source_offsets=2,3,4 attempts=0,3,1
partition=0 offset=0
partition=0 offset=0 processed
handled=1 totals=Map(ORDER-1 -> 100)
```

Số đo offer 55 ms là phép thử có giữ consumer chưa chạy 40 ms, không là throughput benchmark. `Pressure` là capacity/peak quan sát/offer thứ ba bị chặn/maxOfferMs/consumed. Log đầy đủ [demo.txt](../../10-streaming/streaming-lab/verification/demo.txt).

**0 cảnh báo compiler** trong các lượt thành công, cả bốn project bật `-deprecation -feature -Wunused:all -Werror`. `--suppress-outdated-dependency-warning` chỉ tắt hint nâng dependency của Scala CLI; không tắt cảnh báo compiler. [content-check.txt](../../10-streaming/kafka-lab/verification/content-check.txt) lưu kết quả structural QA.

Bản nháp từng có `.get` sau `Using.resources` (trả Unit) và một pattern variable không dùng. Đã sửa; giữ [compiler-using-error.txt](../../10-streaming/kafka-lab/verification/compiler-using-error.txt) làm bằng chứng lỗi thật cho mục 10. Runtime error demo chủ động bắt lỗi và thoát 0; không coi đó là chương trình dịch vụ lỗi ngoài ý muốn.

## 4. Phiên bản chốt và nguồn kiểm chứng

**Ngày tra tất cả POM/metadata dưới đây: 17/10/2026.** [sources.txt](../../10-streaming/kafka-lab/verification/sources.txt) chứa URL chính xác + SHA-256; [verify_sources.py](../../10-streaming/kafka-lab/verification/verify_sources.py) tái lập việc tải. Artifact `_3` có thật, không suy từ `_2.13`.

| Thành phần | Phiên bản | Nguồn |
|---|---|---|
| Scala compiler | 3.9.0 | [POM](https://repo1.maven.org/maven2/org/scala-lang/scala3-compiler_3/3.9.0/scala3-compiler_3-3.9.0.pom) |
| fs2-core_3 (Ch27) | 3.13.0 | [POM](https://repo1.maven.org/maven2/co/fs2/fs2-core_3/3.13.0/fs2-core_3-3.13.0.pom) |
| Cats Effect_3 | 3.7.1 | [POM](https://repo1.maven.org/maven2/org/typelevel/cats-effect_3/3.7.1/cats-effect_3-3.7.1.pom) |
| munit_3 | 1.2.4 | [POM](https://repo1.maven.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom) |
| munit-cats-effect_3 | 2.2.0 | [POM](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom) |
| kafka-clients | **4.4.0** | [POM](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0.pom), [metadata latest/release 4.4.0](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml) |
| Kafka server (test) | 4.4.0, `_2.13` | [POM](https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.13/4.4.0/kafka_2.13-4.4.0.pom) |
| embedded-kafka_3 | 4.4.0 | [POM](https://repo1.maven.org/maven2/io/github/embeddedkafka/embedded-kafka_3/4.4.0/embedded-kafka_3-4.4.0.pom), [official repo](https://github.com/embeddedkafka/embedded-kafka) |
| circe-parser_3 | 0.14.17 | [POM](https://repo1.maven.org/maven2/io/circe/circe-parser_3/0.14.17/circe-parser_3-0.14.17.pom) |
| slf4j-nop | 2.0.20 | [POM](https://repo1.maven.org/maven2/org/slf4j/slf4j-nop/2.0.20/slf4j-nop-2.0.20.pom) |
| sbt (build viết, chưa chạy) | 1.12.11 | [POM](https://repo1.maven.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom) |
| fs2-kafka_3 (đối chiếu, **không dùng runtime**) | **4.1.3** dưới `org.typelevel` | [POM](https://repo1.maven.org/maven2/org/typelevel/fs2-kafka_3/4.1.3/fs2-kafka_3-4.1.3.pom), [metadata](https://repo1.maven.org/maven2/org/typelevel/fs2-kafka_3/maven-metadata.xml) |

POM fs2-kafka 4.1.3 chốt **kafka-clients 4.3.1, fs2-core 3.14.0, Cats Effect 3.7.1**, Scala 3 library 3.3.8. Không override về fs2 3.13.0 của Ch27 và không tuyên bố wrapper + kafka-clients 4.4.0 đã chạy. Tương thích **đã kiểm chứng bằng chạy** trong task là Java client 4.4.0 + server 4.4.0 + Scala 3.9.0/JDK 21.

Tài liệu Kafka URL `/44/` và JavaDoc hosted cho client 4.4.0 chưa truy cập được khi tra; chương dẫn JavaDoc/config/design **4.2** cho khái niệm, ghi rõ phiên bản, còn API/config thực dùng được compile và test trên artifact 4.4.0. Nguồn fs2-kafka POM có thay đổi coordinate nên đây là điểm reviewer cần đối chiếu kỹ, không copy ví dụ 3.x từ blog.

## 5. Đề xuất cho lead cập nhật file dùng chung

### PROGRESS.md và roadmap

- Phase 6: Ch27/Ch28 **đã biên soạn, chờ review chéo**, chưa ✅. Ch27 lab 10 + solutions 7; Ch28 core 7 + broker scenario 1 + solutions 7, đều Scala CLI pass.
- Thêm đường dẫn hai chương/lab/lời giải; thư mục thực của brief là **`10-streaming/`**, khác tên gợi ý `10-kafka-streaming/` còn ở curriculum — lead quyết định đồng bộ link dùng chung.
- Bằng chứng KRaft: Docker không cần cho lab này; vẫn không đổi yêu cầu Docker của Project 4 hay deployment khác chưa chạy.
- Reviewer/lead chạy `sbt.bat test` trên JDK 21 tại hai lab; bật `RUN_KAFKA_IT=1` cho Kafka để chạy cả broker. Sau review sửa BLOCKER/MAJOR và lead test lại mới cập nhật trạng thái hoàn tất.

### compatibility-matrix

- Dòng Kafka client đang 4.2.x: bổ sung tổ hợp **4.4.0 đã chạy ở Ch28** và ngày tra 17/10/2026; không suy rộng tới mọi broker cũ hay mọi feature.
- Dòng fs2-kafka: giữ 4.1.3/`org.typelevel`, bổ sung POM native **4.3.1 client + fs2 3.14.0 + CE 3.7.1**; wrapper chỉ tra POM, chưa chạy trong task. Không mô tả 4.1.3 tương thích fs2 3.13.0 theo suy đoán.
- Có thể thêm embedded-kafka_3 4.4.0 dùng server `_2.13` nhưng harness/client học viên Scala 3.9.0, đã chạy JDK 21; đây là nhúng để test, không là deployment production.

### Glossary đề xuất

Watermark; allowed lateness; half-open window; tumbling/sliding/session window; bridge event; processing time; state eviction; committed offset vs position vs end offset; consumer rebalance; KRaft; ISR; producer idempotence vs idempotent processing; poison message; dead-letter provenance; tombstone; commit frontier. Định nghĩa và ví dụ tiếng Việt nằm tại mục 3/4 hai chương, lead chọn mục chưa có để nhập glossary.

## 6. Chưa kiểm chứng và giới hạn phải giữ nguyên khi review

1. **sbt chưa chạy** trong task. Dựa theo AGENTS.md về `BootServerSocket` AccessDenied trong sandbox; không nói đã tái hiện lỗi sbt mới ở lượt này. Hai `build.sbt` và build.properties đã viết để Claude chạy.
2. Không chạy fs2-kafka wrapper, không thử override dependency, Schema Registry, Kafka transaction, retention expiry/compaction, TLS/SASL, multi-broker replication hoặc broker/đĩa crash. Không có claim production/exactly-once cho các phần đó.
3. Consumer restart là **đóng/mở consumer ở điểm crash mô hình**, không kill JVM. `MemoryLedger` và `Snapshot.copy()` không bền vững; chưa có DB/durable checkpoint hay test persistence. Số tổng đúng nhờ harness giữ sink qua restart consumer.
4. Producer idempotence đã kiểm tra cấu hình và chạy producer; hai app sends cùng payload thành hai offsets. Chưa fault-inject transport retry để đo dedup protocol, nên không lấy thí nghiệm app resend làm bằng chứng fault tolerance mạng.
5. Session là batch reference trên input hữu hạn; audit/preview lời giải giữ output/input, không dùng trực tiếp cho stream vô hạn. Watermark single-source không tự xử lý idle/future skew; nội dung và bài tập giải thích chỗ cần thêm policy.
6. CLI của hai main đã được gọi trong test nhúng; lệnh mẫu `localhost:9092` chưa chạy trên dịch vụ ngoài. Broker nhúng dùng port động, temp trong `.cache/`, được teardown sau test.
7. Phép đo backpressure là harness có sleep/gate, không so sánh throughput hay heap với engine phân tán. Peak queue là số đo sau offer, không đo mọi byte bộ nhớ.

## 7. Những khẳng định cần reviewer soi kỹ

- Biên `eventTime < watermark_cũ`, đóng `end <= watermark_mới`, cùng watermark vẫn nhận; không lẫn bounded-out-of-order delay với allowed lateness bổ sung. Tên/nhãn chính sách không được gán như API Flink/Spark.
- Eviction fingerprint theo event time: replay quá hạn thành late, không bảo đảm conflict detection vô hạn. Cardinality cùng timestamp vẫn tăng.
- `Using.resources` vs `Using(...)` cleanup; thread ownership của KafkaConsumer, rebalance callback và manual assign được tách rõ.
- Source commit chỉ sau sink/DLT ack, đúng partition và leader epoch; commit lỗi sau DLT có thể tạo DLT trùng. Bài tập frontier chỉ là một partition, chưa quản lý ownership generation.
- ID opaque giữ nguyên case/whitespace không rỗng; fingerprint đầy đủ, không so ID-only; amount và total đúng grain, `BigInt` trước cộng.
- Ranh giới producer idempotence, consumer delivery và durability sink; RF1 + acks all không chứng minh mất máy an toàn, schema version không tự tạo Schema Registry.
- Kafka core mặc định có một skipped test trong reporter; broker được chạy riêng thật. Tổng **32 pass** không đếm skip hoặc lượt chạy lại.

Không ghi review thay Claude và không chỉnh file dùng chung. Handoff sẵn sàng để lead review và chạy kiểm chứng cuối.
