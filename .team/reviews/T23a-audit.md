# T23a — Rà soát tự động toàn giáo trình (T25)

Codex · 17/10/2026 · **Đã hoàn tất phạm vi audit; CHƯA ĐẠT để chốt toàn giáo trình.** Đây là báo cáo phát hiện và kiểm chứng, không sửa bài/source hay đánh dấu chương hoàn thành.

Đã đối chiếu **37 chương + 5 README project**, **604 liên kết local** (kể cả fragment), và chạy đủ **56/56 thư mục có project.scala**, gồm lời giải. Giữ nguyên kết quả lab 1–11 từ checkpoint T25; tiếp tục lab 12–56. SHA-256 **844/844 file** source khớp snapshot khi kết thúc.

Kết quả lệnh: **52 exit 0**, **4 nonzero/timeout**; summary log cộng lại: **698 pass, 4 failed, 28 skipped/ignored, 1 compiler warning**. Số failed có thể là fixture trước suite; không diễn giải thành từng test nghiệp vụ hỏng. Bảng dưới ghi riêng từng trường hợp. Đây là tổng lượt thực thi theo thư mục, có thể lặp test lõi khi lab nhập lại code từ P4; không phải số test duy nhất trong repo.

## Phương pháp và giới hạn

- Mọi build chạy trên bản sao `.cache/t25/source/`; cache Coursier `.cache/coursier`, TEMP/TMP và java.io.tmpdir `.cache/t25/tmp`. Không chạy sbt, không bật RUN_DB_TESTS/RUN_KAFKA_IT trong lượt audit mặc định. Những test opt-in có thể skipped đúng thiết kế; kết quả P5 bật dịch vụ được báo riêng trong review P5.
- Lệnh chung: `scala-cli.bat test . --server=false` tại thư mục tương ứng. Riêng P5 dùng `scala-cli.bat test project.scala core ingest serving app --server=false` để không đưa công cụ dev LocalPostgres vào lab. Giữ Scala/JDK/dependency directives của từng lab, kể cả nhánh Spark Scala 2.13 và probe minh họa Scala 3.7.3.
- Phạm vi build bám đúng điều kiện có project.scala; thư mục không có file này (ví dụ P1) không được tính là đã chạy lại chỉ vì README được quét. Không xác nhận lại các thí nghiệm run, shell test, ca biên dịch âm hay spark-submit ngoài lệnh test ở trên.
- Parser đếm cả summary munit và ZIO. `5 tests passed. 0 tests failed. 0 tests ignored.` của ZIO HTTP được khôi phục từ log có sẵn: checkpoint từng ghi pass=0 vì parser chỉ nhận munit. Không chạy lại lab. JSON gốc `build-results.json` giữ nguyên; số liệu chuẩn hóa ở `verified-results.json`.
- Compiler warning tách khỏi dependency-update hint, SLF4J warning và Hadoop/Spark runtime WARN. Không đề xuất nâng munit/munit-cats-effect vì hint: giữ chuẩn repo 1.2.4/2.2.0.
- Quét Markdown bỏ fenced code/inline code khi dò link, xử lý heading Unicode, heading trùng, anchor HTML và reference link có khai báo. Không truy cập URL ngoài, không xác minh lại toàn bộ nguồn version hay output văn xuôi. File tồn tại không có nghĩa code trong file đã được chạy. Việc đếm mục không thay thế review sư phạm.
- Yêu cầu T23a về “dòng đầu” và “17 mục cho project” khác template hiện có: `00-roadmap/08-lesson-template.md:7` đặt H1 trước metadata; `14-projects/README.md:13` cho project khuôn riêng. Ghi rõ sai khác yêu cầu dưới đây, không quy máy móc thành lỗi nội dung hoặc tự sửa template.

## Phát hiện tài liệu đã xác minh

- **MINOR — metadata đúng khối mở đầu nhưng lệch dòng 1:** 38/42 tài liệu. Cả 42 có Scala/JDK/dependency/trạng thái trong khối mở đầu; Ch18, Ch25, Ch27, Ch28 có metadata ngay dòng 1. Đề xuất lead thống nhất yêu cầu T23a/AGENTS/template rồi đổi vị trí metadata đồng loạt.
- **MINOR — trạng thái cũ:** `10-streaming/ch27-streaming-fundamentals.md:1` còn “chờ review chéo của Claude; sbt chưa chạy”, trong khi `.team/reviews/T13-ch27-28-review.md:3` đã ĐẠT và các dòng 6–8 có kiểm chứng sbt. Đề xuất cập nhật đúng bằng chứng review/test của lead, không thay bằng kết quả audit này.
- **MINOR — một compiler warning Scala 2:** `solutions/ch22-code/src/main/scala/vn/scalaeng/solutions/ch22/Ch22Solutions.scala:66`: biến pattern `a` che tên `a` ở dòng 59. Log thật ghi `warning: Name a is already introduced in an enclosing scope as value a at line 59.` Đề xuất đổi tên pattern thành `adaptive` và dùng `adaptive.executedPlan`, rồi lead kiểm lại mục tiêu 0 warning. Lượt này 8/8 test vẫn pass. Parser checkpoint chỉ nhận định dạng warning Scala 3 nên từng ghi 0; số liệu chuẩn hóa đếm đúng 1 từ log có sẵn.
- **Sai khác khuôn cần lead quyết định:** 5 README project có 10 hoặc 12 mục cấp 2, không phải 17 theo brief T23a; chúng theo khuôn project riêng. Đề xuất giữ khuôn project và ghi ngoại lệ vào yêu cầu/template, hoặc yêu cầu tác giả bổ sung ánh xạ đủ 17 mục. Không tự kết luận thiếu toàn bộ nội dung chỉ từ số heading.
- **Không phát hiện:** chương thiếu/trùng mục 1–17; mục chỉ có tiêu đề; đích local/anchor hỏng; cụm “đang viết”. Chỉ có một ứng viên “chờ review” nêu trên đã đối chiếu handoff/review.

## Bảng theo chương/project

Mỗi dòng nêu kết quả cấu trúc, vị trí vấn đề và cách xử lý. Các lỗi build/lab được tách ở phần sau để không gán một lỗi fixture cho nhiều chương.

| Chương/project | Cấu trúc và liên kết | Vấn đề + file:dòng | Đề xuất |
|---|---|---|---|
| `01-scala3-core/ch01-introduction.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch01-introduction.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `01-scala3-core/ch02-environment-setup.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch02-environment-setup.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `01-scala3-core/ch03-variables-types-expressions.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch03-variables-types-expressions.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `01-scala3-core/ch04-functions-methods.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch04-functions-methods.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `01-scala3-core/ch05-oop.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch05-oop.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `01-scala3-core/ch06-collections.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`01-scala3-core/ch06-collections.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `02-functional-programming/ch07-functional-programming.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`02-functional-programming/ch07-functional-programming.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `03-type-system/ch08a-core-types.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`03-type-system/ch08a-core-types.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `03-type-system/ch08b-contextual-abstractions.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`03-type-system/ch08b-contextual-abstractions.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `03-type-system/ch08c-advanced-type-programming.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`03-type-system/ch08c-advanced-type-programming.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `04-professional-engineering/ch09-project-structure.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`04-professional-engineering/ch09-project-structure.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `04-professional-engineering/ch11-jvm-fundamentals.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`04-professional-engineering/ch11-jvm-fundamentals.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `05-backend/ch12-backend-fundamentals.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`05-backend/ch12-backend-fundamentals.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `05-backend/ch13-choosing-a-framework.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`05-backend/ch13-choosing-a-framework.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `05-backend/ch14-rest-apis.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`05-backend/ch14-rest-apis.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `05-backend/ch17-security-production.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`05-backend/ch17-security-production.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `05-backend/ch18-backend-architecture.md` | Đủ 17 mục; link/anchor đạt | Không phát hiện trong các kiểm tra tĩnh | Giữ nguyên |
| `06-database/ch15-database-engineering.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`06-database/ch15-database-engineering.md:1`); Giới hạn kiểm chứng: db-lab và lời giải Ch15 dừng ở fixture `06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala:18` | Thống nhất template rồi đưa metadata lên dòng 1; Lead chạy PostgreSQL ngoài sandbox; tách unit/integration test theo AGENTS.md |
| `07-concurrency/ch16a-concurrency-foundations.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`07-concurrency/ch16a-concurrency-foundations.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `07-concurrency/ch16b-cats-effect.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`07-concurrency/ch16b-cats-effect.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `08-data-processing/ch19-scala-data-processing.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`08-data-processing/ch19-scala-data-processing.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `08-data-processing/ch20-functional-data-transformation.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`08-data-processing/ch20-functional-data-transformation.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `08-data-processing/ch21-data-formats.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`08-data-processing/ch21-data-formats.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `09-spark/ch22-spark-fundamentals.md` | Đủ 17 mục; link/anchor đạt; thêm mục  | MINOR: metadata ở khối đầu, không ở dòng 1 (`09-spark/ch22-spark-fundamentals.md:1`); MINOR: warning che biến tại `solutions/ch22-code/src/main/scala/vn/scalaeng/solutions/ch22/Ch22Solutions.scala:66` | Thống nhất template rồi đưa metadata lên dòng 1; Đổi tên pattern a thành adaptive; giữ ý nghĩa phép khớp kiểu |
| `09-spark/ch23-spark-sql-dataframes.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`09-spark/ch23-spark-sql-dataframes.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `09-spark/ch24-batch-data-pipeline.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`09-spark/ch24-batch-data-pipeline.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `09-spark/ch25-spark-performance.md` | Đủ 17 mục; link/anchor đạt | Không phát hiện trong các kiểm tra tĩnh | Giữ nguyên |
| `09-spark/ch26-spark-production-engineering.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`09-spark/ch26-spark-production-engineering.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `10-streaming/ch27-streaming-fundamentals.md` | Đủ 17 mục; link/anchor đạt | MINOR: chờ review/sbt lỗi thời (`10-streaming/ch27-streaming-fundamentals.md:1`) | Đồng bộ review T13 và bằng chứng sbt của lead |
| `10-streaming/ch28-kafka-scala.md` | Đủ 17 mục; link/anchor đạt | Không phát hiện trong các kiểm tra tĩnh | Giữ nguyên |
| `10-streaming/ch29-streaming-pipeline.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`10-streaming/ch29-streaming-pipeline.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `10-streaming/ch30-streaming-reliability.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`10-streaming/ch30-streaming-reliability.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `11-data-platform/ch31-data-pipeline-architecture.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`11-data-platform/ch31-data-pipeline-architecture.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `11-data-platform/ch32-advanced-distributed-processing.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`11-data-platform/ch32-advanced-distributed-processing.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `11-data-platform/ch33-data-quality-testing.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`11-data-platform/ch33-data-quality-testing.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `12-testing/ch10-testing.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`12-testing/ch10-testing.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `13-performance/ch34-performance-cost.md` | Đủ 17 mục; link/anchor đạt | MINOR: metadata ở khối đầu, không ở dòng 1 (`13-performance/ch34-performance-cost.md:1`) | Thống nhất template rồi đưa metadata lên dòng 1 |
| `14-projects/p1-scala-data-processor/README.md` | 12 mục; link/anchor đạt | 12 mục thay vì 17 (`14-projects/p1-scala-data-processor/README.md:12`); MINOR: metadata ở khối đầu, không ở dòng 1 (`14-projects/p1-scala-data-processor/README.md:1`) | Chốt ngoại lệ khuôn project hoặc thêm ánh xạ 17 mục; Thống nhất template rồi đưa metadata lên dòng 1 |
| `14-projects/p2-backend-service/README.md` | 10 mục; link/anchor đạt | 10 mục thay vì 17 (`14-projects/p2-backend-service/README.md:15`); MINOR: metadata ở khối đầu, không ở dòng 1 (`14-projects/p2-backend-service/README.md:1`) | Chốt ngoại lệ khuôn project hoặc thêm ánh xạ 17 mục; Thống nhất template rồi đưa metadata lên dòng 1 |
| `14-projects/p3-batch-pipeline/README.md` | 12 mục; link/anchor đạt | 12 mục thay vì 17 (`14-projects/p3-batch-pipeline/README.md:11`); MINOR: metadata ở khối đầu, không ở dòng 1 (`14-projects/p3-batch-pipeline/README.md:1`) | Chốt ngoại lệ khuôn project hoặc thêm ánh xạ 17 mục; Thống nhất template rồi đưa metadata lên dòng 1 |
| `14-projects/p4-kafka-event-processing/README.md` | 10 mục; link/anchor đạt | 10 mục thay vì 17 (`14-projects/p4-kafka-event-processing/README.md:7`); MINOR: metadata ở khối đầu, không ở dòng 1 (`14-projects/p4-kafka-event-processing/README.md:1`) | Chốt ngoại lệ khuôn project hoặc thêm ánh xạ 17 mục; Thống nhất template rồi đưa metadata lên dòng 1 |
| `14-projects/p5-data-platform/README.md` | 12 mục; link/anchor đạt | 12 mục thay vì 17 (`14-projects/p5-data-platform/README.md:13`); MINOR: metadata ở khối đầu, không ở dòng 1 (`14-projects/p5-data-platform/README.md:1`) | Chốt ngoại lệ khuôn project hoặc thêm ánh xạ 17 mục; Thống nhất template rồi đưa metadata lên dòng 1 |

## Kết quả build/test từng lab

P/F/S = pass/failed/skipped hoặc ignored. Log dưới `.cache/t25/logs/`; mỗi link mở output thật, command đầy đủ có trong `.cache/t25/verified-results.json`. Exit nonzero với P/F/S bằng 0 không được tính là test đã pass.

| # | Lab | Exit | P/F/S | Compiler warnings | Log |
|---|---|---:|---|---:|---|
| 1 | `14-projects/p5-data-platform` | 0 | 11/0/6 | 0 | [14-projects__p5-data-platform.log](../../.cache/t25/logs/14-projects__p5-data-platform.log) |
| 2 | `01-scala3-core/code` | 0 | 75/0/0 | 0 | [01-scala3-core__code.log](../../.cache/t25/logs/01-scala3-core__code.log) |
| 3 | `03-type-system/ch08c-lab` | 0 | 12/0/0 | 0 | [03-type-system__ch08c-lab.log](../../.cache/t25/logs/03-type-system__ch08c-lab.log) |
| 4 | `04-professional-engineering/jvm-lab` | 1 | 0/0/0 | 0 | [04-professional-engineering__jvm-lab.log](../../.cache/t25/logs/04-professional-engineering__jvm-lab.log) |
| 5 | `05-backend/framework-lab/typelevel` | 0 | 13/0/0 | 0 | [05-backend__framework-lab__typelevel.log](../../.cache/t25/logs/05-backend__framework-lab__typelevel.log) |
| 6 | `05-backend/framework-lab/zio-http` | 0 | 5/0/0 | 0 | [05-backend__framework-lab__zio-http.log](../../.cache/t25/logs/05-backend__framework-lab__zio-http.log) |
| 7 | `05-backend/http-lab` | 0 | 13/0/0 | 0 | [05-backend__http-lab.log](../../.cache/t25/logs/05-backend__http-lab.log) |
| 8 | `05-backend/order-service` | 0 | 26/0/0 | 0 | [05-backend__order-service.log](../../.cache/t25/logs/05-backend__order-service.log) |
| 9 | `05-backend/security-lab` | 0 | 28/0/0 | 0 | [05-backend__security-lab.log](../../.cache/t25/logs/05-backend__security-lab.log) |
| 10 | `06-database/db-lab` | 1 | 0/3/0 | 0 | [06-database__db-lab.log](../../.cache/t25/logs/06-database__db-lab.log) |
| 11 | `07-concurrency/concurrency-lab` | 0 | 9/0/0 | 0 | [07-concurrency__concurrency-lab.log](../../.cache/t25/logs/07-concurrency__concurrency-lab.log) |
| 12 | `07-concurrency/effects-lab` | 0 | 22/0/0 | 0 | [07-concurrency__effects-lab.log](../../.cache/t25/logs/07-concurrency__effects-lab.log) |
| 13 | `08-data-processing/formats-lab` | 0 | 27/0/0 | 0 | [08-data-processing__formats-lab.log](../../.cache/t25/logs/08-data-processing__formats-lab.log) |
| 14 | `08-data-processing/ingest-lab` | 0 | 20/0/0 | 0 | [08-data-processing__ingest-lab.log](../../.cache/t25/logs/08-data-processing__ingest-lab.log) |
| 15 | `08-data-processing/transform-lab` | 0 | 16/0/0 | 0 | [08-data-processing__transform-lab.log](../../.cache/t25/logs/08-data-processing__transform-lab.log) |
| 16 | `09-spark/batch-pipeline-lab` | 0 | 18/0/0 | 0 | [09-spark__batch-pipeline-lab.log](../../.cache/t25/logs/09-spark__batch-pipeline-lab.log) |
| 17 | `09-spark/scala3-spark-probe` | 1 | 0/0/0 | 0 | [09-spark__scala3-spark-probe.log](../../.cache/t25/logs/09-spark__scala3-spark-probe.log) |
| 18 | `09-spark/spark-fundamentals-lab` | 0 | 12/0/0 | 0 | [09-spark__spark-fundamentals-lab.log](../../.cache/t25/logs/09-spark__spark-fundamentals-lab.log) |
| 19 | `09-spark/spark-performance-lab` | 0 | 13/0/0 | 0 | [09-spark__spark-performance-lab.log](../../.cache/t25/logs/09-spark__spark-performance-lab.log) |
| 20 | `09-spark/spark-production-lab` | 0 | 13/0/0 | 0 | [09-spark__spark-production-lab.log](../../.cache/t25/logs/09-spark__spark-production-lab.log) |
| 21 | `09-spark/spark-sql-lab` | 0 | 18/0/0 | 0 | [09-spark__spark-sql-lab.log](../../.cache/t25/logs/09-spark__spark-sql-lab.log) |
| 22 | `10-streaming/kafka-lab` | 0 | 7/0/1 | 0 | [10-streaming__kafka-lab.log](../../.cache/t25/logs/10-streaming__kafka-lab.log) |
| 23 | `10-streaming/reliability-lab` | 0 | 3/0/0 | 0 | [10-streaming__reliability-lab.log](../../.cache/t25/logs/10-streaming__reliability-lab.log) |
| 24 | `10-streaming/streaming-lab` | 0 | 10/0/0 | 0 | [10-streaming__streaming-lab.log](../../.cache/t25/logs/10-streaming__streaming-lab.log) |
| 25 | `10-streaming/streaming-pipeline-lab` | 0 | 2/0/0 | 0 | [10-streaming__streaming-pipeline-lab.log](../../.cache/t25/logs/10-streaming__streaming-pipeline-lab.log) |
| 26 | `11-data-platform/architecture-lab` | 0 | 17/0/0 | 0 | [11-data-platform__architecture-lab.log](../../.cache/t25/logs/11-data-platform__architecture-lab.log) |
| 27 | `11-data-platform/distributed-lab` | 0 | 11/0/0 | 0 | [11-data-platform__distributed-lab.log](../../.cache/t25/logs/11-data-platform__distributed-lab.log) |
| 28 | `11-data-platform/quality-lab` | 0 | 18/0/0 | 0 | [11-data-platform__quality-lab.log](../../.cache/t25/logs/11-data-platform__quality-lab.log) |
| 29 | `13-performance/cost-lab` | 0 | 15/0/0 | 0 | [13-performance__cost-lab.log](../../.cache/t25/logs/13-performance__cost-lab.log) |
| 30 | `14-projects/p2-backend-service` | 0 | 29/0/18 | 0 | [14-projects__p2-backend-service.log](../../.cache/t25/logs/14-projects__p2-backend-service.log) |
| 31 | `14-projects/p3-batch-pipeline` | 0 | 19/0/0 | 0 | [14-projects__p3-batch-pipeline.log](../../.cache/t25/logs/14-projects__p3-batch-pipeline.log) |
| 32 | `14-projects/p4-kafka-event-processing` | 0 | 11/0/3 | 0 | [14-projects__p4-kafka-event-processing.log](../../.cache/t25/logs/14-projects__p4-kafka-event-processing.log) |
| 33 | `solutions/ch08c-code` | 0 | 13/0/0 | 0 | [solutions__ch08c-code.log](../../.cache/t25/logs/solutions__ch08c-code.log) |
| 34 | `solutions/ch13-code` | 0 | 6/0/0 | 0 | [solutions__ch13-code.log](../../.cache/t25/logs/solutions__ch13-code.log) |
| 35 | `solutions/ch14-code` | 0 | 6/0/0 | 0 | [solutions__ch14-code.log](../../.cache/t25/logs/solutions__ch14-code.log) |
| 36 | `solutions/ch15-code` | 1 | 0/1/0 | 0 | [solutions__ch15-code.log](../../.cache/t25/logs/solutions__ch15-code.log) |
| 37 | `solutions/ch17-code` | 0 | 7/0/0 | 0 | [solutions__ch17-code.log](../../.cache/t25/logs/solutions__ch17-code.log) |
| 38 | `solutions/ch18-code` | 0 | 6/0/0 | 0 | [solutions__ch18-code.log](../../.cache/t25/logs/solutions__ch18-code.log) |
| 39 | `solutions/ch19-code` | 0 | 8/0/0 | 0 | [solutions__ch19-code.log](../../.cache/t25/logs/solutions__ch19-code.log) |
| 40 | `solutions/ch20-code` | 0 | 9/0/0 | 0 | [solutions__ch20-code.log](../../.cache/t25/logs/solutions__ch20-code.log) |
| 41 | `solutions/ch21-code` | 0 | 12/0/0 | 0 | [solutions__ch21-code.log](../../.cache/t25/logs/solutions__ch21-code.log) |
| 42 | `solutions/ch22-code` | 0 | 8/0/0 | 1 | [solutions__ch22-code.log](../../.cache/t25/logs/solutions__ch22-code.log) |
| 43 | `solutions/ch23-code` | 0 | 7/0/0 | 0 | [solutions__ch23-code.log](../../.cache/t25/logs/solutions__ch23-code.log) |
| 44 | `solutions/ch24-code` | 0 | 6/0/0 | 0 | [solutions__ch24-code.log](../../.cache/t25/logs/solutions__ch24-code.log) |
| 45 | `solutions/ch25-code` | 0 | 6/0/0 | 0 | [solutions__ch25-code.log](../../.cache/t25/logs/solutions__ch25-code.log) |
| 46 | `solutions/ch26-code` | 0 | 6/0/0 | 0 | [solutions__ch26-code.log](../../.cache/t25/logs/solutions__ch26-code.log) |
| 47 | `solutions/ch27-code` | 0 | 7/0/0 | 0 | [solutions__ch27-code.log](../../.cache/t25/logs/solutions__ch27-code.log) |
| 48 | `solutions/ch28-code` | 0 | 7/0/0 | 0 | [solutions__ch28-code.log](../../.cache/t25/logs/solutions__ch28-code.log) |
| 49 | `solutions/ch29-code` | 0 | 6/0/0 | 0 | [solutions__ch29-code.log](../../.cache/t25/logs/solutions__ch29-code.log) |
| 50 | `solutions/ch30-code` | 0 | 6/0/0 | 0 | [solutions__ch30-code.log](../../.cache/t25/logs/solutions__ch30-code.log) |
| 51 | `solutions/ch31-code` | 0 | 10/0/0 | 0 | [solutions__ch31-code.log](../../.cache/t25/logs/solutions__ch31-code.log) |
| 52 | `solutions/ch32-code` | 0 | 7/0/0 | 0 | [solutions__ch32-code.log](../../.cache/t25/logs/solutions__ch32-code.log) |
| 53 | `solutions/ch33-code` | 0 | 7/0/0 | 0 | [solutions__ch33-code.log](../../.cache/t25/logs/solutions__ch33-code.log) |
| 54 | `solutions/ch34-code` | 0 | 7/0/0 | 0 | [solutions__ch34-code.log](../../.cache/t25/logs/solutions__ch34-code.log) |
| 55 | `solutions/code` | 0 | 36/0/0 | 0 | [solutions__code.log](../../.cache/t25/logs/solutions__code.log) |
| 56 | `solutions/effects-code` | 0 | 12/0/0 | 0 | [solutions__effects-code.log](../../.cache/t25/logs/solutions__effects-code.log) |

## Phân loại trường hợp cần chú ý

- `14-projects/p5-data-platform/project.scala:1`: 6 test skipped/ignored; không tính chúng vào pass và không xác nhận integration test tương ứng.
- `04-professional-engineering/jvm-lab/project.scala:1`: **Không có test framework**: lệnh test exit 1, không phải lỗi compile/test assertion. Lab/probe dạng chương trình minh họa cần kiểm bằng lệnh run của bài; lượt audit này không thay run bằng một test giả. Không xác nhận output runtime. Đề xuất lead ghi ngoại lệ cho lab không có suite và chạy đúng thí nghiệm riêng.
- `06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala:18`: **Fixture PostgreSQL nhúng không khởi động**: timeout 10000 ms → SQLException connect failed → ConnectException connection refused; thất bại trước assertions. Phân loại giới hạn môi trường/sandbox, chưa chứng minh bug nghiệp vụ hay cơ chế sandbox cụ thể. Đề xuất lead chạy ngoài sandbox; nếu lab cần opt-in thì tách fixture và dùng assume khi thiếu môi trường theo AGENTS.md. Cụ thể là 3 lỗi beforeAll của QueryPlanSuite/RepositorySuite/TransactionSuite, không phải 3 assertion sai. Fixture gọi EmbeddedPostgres vô điều kiện nên lượt test mặc định không tự skip khi môi trường không khởi động được PostgreSQL; cần lead chốt cách tách unit/integration test, không dùng assume để nuốt lỗi DB sau khi đã bật opt-in.
- `09-spark/scala3-spark-probe/project.scala:1`: **Không có test framework**: lệnh test exit 1, không phải lỗi compile/test assertion. Lab/probe dạng chương trình minh họa cần kiểm bằng lệnh run của bài; lượt audit này không thay run bằng một test giả. Không xác nhận output runtime. Đề xuất lead ghi ngoại lệ cho lab không có suite và chạy đúng thí nghiệm riêng.
- `10-streaming/kafka-lab/project.scala:1`: 1 test skipped/ignored; không tính chúng vào pass và không xác nhận integration test tương ứng.
- `14-projects/p2-backend-service/project.scala:1`: 18 test skipped/ignored; không tính chúng vào pass và không xác nhận integration test tương ứng.
- `14-projects/p4-kafka-event-processing/project.scala:1`: 3 test skipped/ignored; không tính chúng vào pass và không xác nhận integration test tương ứng.
- `solutions/ch15-code/src/test/scala/vn/scalaeng/solutions/ch15/Ch15SolutionsSuite.scala:9`: **Fixture PostgreSQL nhúng không khởi động**: timeout 10000 ms → SQLException connect failed → ConnectException connection refused; thất bại trước assertions. Phân loại giới hạn môi trường/sandbox, chưa chứng minh bug nghiệp vụ hay cơ chế sandbox cụ thể. Đề xuất lead chạy ngoài sandbox; nếu lab cần opt-in thì tách fixture và dùng assume khi thiếu môi trường theo AGENTS.md. Suite kế thừa PostgresSuite qua `solutions/ch15-code/project.scala:8`, cùng điểm khởi động `06-database/db-lab/src/test/scala/vn/scalaeng/db/PostgresSuite.scala:18`. Summary 1 failed là lỗi beforeAll; cả 7 test lời giải không chạy, không phải một assertion nghiệp vụ sai.
- `solutions/ch22-code`: 1 compiler warning, dù exit 0 và test pass; xem phát hiện Ch22 ở trên.

## Bàn giao và bằng chứng

- `.cache/t25/snapshot.json`: checksum source; `.cache/t25/static-results.json`: 42 dòng quét tài liệu; `.cache/t25/build-results.json`: checkpoint thực thi gốc; `.cache/t25/verified-results.json`: số liệu đã đọc lại từ log, gồm ZIO.
- `.team/reviews/P5-review.md`, phần **Vòng 2**: kết luận riêng P5, 2 MAJOR + 2 MINOR; baseline và probe có DB/Kafka được phân biệt, không cộng vào tổng audit mặc định.
- `.team/progress/T25.md`: tiến độ mỗi bước; DONE chỉ có nghĩa hoàn tất tác vụ review/audit, không có nghĩa giáo trình hoặc P5 đã đạt.
- Không sửa source/bài, không sửa file dùng chung, không chạy sbt, không commit. Lead áp dụng đề xuất và chạy lại phần runtime chưa kiểm chứng trước khi cập nhật trạng thái chung.
