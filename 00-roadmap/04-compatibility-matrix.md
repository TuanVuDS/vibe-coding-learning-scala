# Đánh giá Compatibility: Scala 3 và hệ sinh thái BE/DE

> **Ngày kiểm chứng:** 14/10/2026; **cập nhật 16–17/10/2026** (tra `repo1.maven.org/.../maven-metadata.xml` và chạy code thật).
> **Lưu ý nguồn:** giao diện tìm kiếm `search.maven.org` trả dữ liệu cũ hơn hơn một năm — luôn đối chiếu `maven-metadata.xml` trên `repo1.maven.org`.
> Phiên bản phần mềm thay đổi liên tục. Trước khi bắt đầu mỗi giai đoạn, hãy kiểm tra lại
> các link nguồn trong bảng. Bảng này phân biệt rõ **điều đã kiểm chứng** và **điều cần kiểm chứng lại**.

## 1. Ký hiệu

| Ký hiệu | Ý nghĩa |
|---|---|
| ✅ | Đã kiểm chứng trên tài liệu/trang phát hành chính thức tại ngày trên |
| ⚠️ | Dùng được nhưng có điều kiện hoặc giới hạn (đọc ghi chú) |
| ❌ | Không hỗ trợ theo cách cần cho giáo trình |
| ❓ | Chưa kiểm chứng đủ — sẽ xác minh trước khi biên soạn chương liên quan |

## 2. Ngôn ngữ, JDK và build tool

| Thành phần | Phiên bản chọn | Trạng thái | Ghi chú | Nguồn |
|---|---|---|---|---|
| Scala 3 LTS | **3.9.0** (03/09/2026) | ✅ | LTS mới, kế nhiệm 3.3 LTS, cam kết bảo trì tối thiểu 3 năm. Thư viện publish bằng 3.3.x vẫn dùng được từ project 3.9 (chiều ngược lại thì không). | https://www.scala-lang.org/news/3.9/ |
| Scala 3 LTS cũ | 3.3.8 | ✅ | Vẫn bảo trì; phần lớn thư viện hiện publish bằng 3.3.x. | https://www.scala-lang.org/download/all.html |
| Scala Next | 3.10.0 (12/10/2026) | ✅ (không chọn) | Ưu tiên ổn định nên không dùng cho giáo trình. | như trên |
| Scala 2.13 | 2.13.18 | ✅ | Chỉ dùng cho **nhánh Spark** (mục 4). | như trên |
| JDK | **21 (LTS)** | ✅ | Scala 3.8+ yêu cầu tối thiểu JDK 17. Spark 4.1 chạy trên Java 17/21. Kafka clients hỗ trợ Java 11+. Chọn 21 để **một JDK dùng chung cho BE và DE**. | https://spark.apache.org/docs/4.1.1/ |
| Scala CLI | lệnh `scala` (từ Scala 3.5) hoặc bản độc lập | ✅ | Dùng cho Giai đoạn 1: file đơn, script, bài tập. | https://scala-cli.virtuslab.org/ |
| sbt | **1.12.11** | ✅ | sbt 2.0.0 đã GA (build definition dùng Scala 3, cần JDK 17). Giáo trình dùng 1.12.x vì hệ plugin trưởng thành hơn cho Spark/assembly; sẽ đánh giá lại sbt 2.x ở Giai đoạn 2. | https://eed3si9n.com/sbt-2.0.0 |
| munit | **1.2.4** | ✅ | Có artifact Scala 3. Công cụ test chính. Bản mới nhất là 1.3.6; giáo trình giữ 1.2.4 cho đồng nhất. **Toàn bộ test đã chạy với munit thật** (16/10/2026). | https://index.scala-lang.org/scalameta/munit |
| munit-cats-effect | **2.2.0** | ✅ | Dùng 2.2.0 vì POM của 2.2.1 kéo munit 1.3.6 (phát hiện ở Ch16B). | https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/ |

## 3. Backend

| Thư viện | Phiên bản | Scala 3 | Ghi chú | Nguồn |
|---|---|---|---|---|
| Cats Effect | **3.7.1** | ✅ | Runtime của stack Typelevel. Đã chạy thật (Ch16B, 22 test). testkit 3.7.1 có `TestControl.executeEmbed`. | https://typelevel.org/cats-effect/ |
| http4s | **0.23.38** | ✅ | Series 0.23 ổn định, artifact `_3`, server **Ember**. 1.0 vẫn là milestone (`1.0.0-M49`). Từ 0.23.31 `UnprocessableEntity` đổi tên `UnprocessableContent` (deprecation thật, Ch13). | https://github.com/http4s/http4s/releases |
| Tapir | **1.13.33** | ✅ | Mô tả endpoint dạng dữ liệu, sinh OpenAPI 3.1 (sttp-apispec 0.11.10), chạy trên http4s. Đã chạy thật (Ch13). | https://tapir.softwaremill.com/ |
| Doobie | **1.0.0-RC13** | ⚠️ | Có Scala 3 nhưng **vẫn là Release Candidate** (chưa có 1.0.0 final, tra 16/10/2026). Từ RC13 groupId **và package** đổi sang `org.typelevel` (`import org.typelevel.doobie.*`); `doobie.implicits.*` không còn kèm cú pháp Cats. Đã chạy thật với PostgreSQL 18.6 (Ch15). | https://github.com/typelevel/doobie/releases |
| Slick | 3.6.1 (4.0.0-RC2 đang thử) | ✅ (artifact) | Có `slick_3` từ series 3.5. API trả `Future`. Chỉ so sánh ở Ch15, không có lab. | https://scala-slick.org/ |
| Flyway | **13.10.3** | ✅ | Cần thêm module `flyway-database-postgresql`. Đã chạy thật (Ch15). | https://documentation.red-gate.com/flyway |
| PostgreSQL | **18.6** (Docker `postgres:18.6`) | ✅ | Test chạy PostgreSQL 18.6 thật qua zonky embedded-postgres 2.2.2 (không cần Docker). JDBC 42.7.14. | https://www.postgresql.org/docs/18/ |
| Play Framework | 3.0.12 (3.1.0-M9 milestone) | ✅ (artifact) | `play_3` có; Play 3.0 dùng Apache Pekko, hỗ trợ Scala 3 (Highlights30). Không có lab. | https://www.playframework.com/documentation/3.0.x/Highlights30 |
| ZIO / ZIO HTTP | ZIO 2.1.27 · ZIO HTTP 3.11.6 | ✅ | Chỉ để **so sánh** (Ch13 có lab ZIO HTTP 5 test chạy thật; Ch16B so sánh khái niệm). | https://zio.dev/ |
| Apache Pekko | 1.x | ✅ | Tài liệu chính thức: Pekko build bằng Scala 3.3 LTS và hỗ trợ Scala 3. Không phải stack chính. | https://pekko.apache.org/docs/pekko/1.3/project/scala3.html |
| Akka | — | — | Chuyển sang giấy phép BSL từ 2022; chỉ nhắc để hiểu bối cảnh của Pekko. | https://doc.akka.io/ |

## 4. Data Engineering

| Thành phần | Phiên bản | Scala 3 | Ghi chú | Nguồn |
|---|---|---|---|---|
| **Apache Spark** | **4.2.0** (GA 11/07/2026; 4.1.3 ngày 12/07/2026) | ❌ | Spark 4.x chỉ build cho **Scala 2.13**. Tài liệu chính thức: ứng dụng Scala phải dùng **cùng phiên bản Scala** mà Spark được biên dịch. Epic hỗ trợ Scala 3 (SPARK-54150) vẫn đang mở. | https://spark.apache.org/docs/4.1.1/ , https://issues.apache.org/jira/browse/SPARK-54150 |
| Spark 3.5.x | 3.5.x | ❌ | Scala 2.12/2.13. Nhiều nền tảng managed vẫn dùng. | https://spark.apache.org/ |
| Kafka clients | 4.2.x (4.2.2 phát hành 29/09/2026) | ✅* | Client là thư viện **Java**, dùng trực tiếp từ Scala 3. *Broker Kafka 4.x cần Java 17+. | https://kafka.apache.org/documentation/ |
| fs2-kafka | **4.1.3** (`org.typelevel`) / 3.9.1 (`com.github.fd4s`) | ✅ | Series 4.x **đã GA** dưới groupId `org.typelevel` (tra 16/10/2026). Chốt chi tiết ở Chương 28. | https://repo1.maven.org/maven2/org/typelevel/fs2-kafka_3/ |
| Apache Flink | 2.x | ⚠️ | **Scala API đã bị xóa từ Flink 2.0** (FLIP-265). Vẫn gọi được Java DataStream API từ Scala nhưng mất wrapper/implicit dành riêng cho Scala. Chỉ dùng để so sánh khái niệm. | https://cwiki.apache.org/confluence/display/FLINK/FLIP-265+Deprecate+and+remove+Scala+API+support |
| Avro / Parquet | ❓ | ❓ | Thư viện Java dùng được từ Scala; thư viện Scala-native sẽ kiểm chứng ở Chương 21. | — |

## 5. Kết luận kiến trúc phiên bản

```
┌──────────────────────────────── JDK 21 ────────────────────────────────┐
│                                                                        │
│  Scala 3.9 LTS                          Scala 2.13.x                   │
│  ├─ Scala Core (Ch1–11)                 └─ Spark 4.1.x jobs (Ch22–26)  │
│  ├─ Backend: CE3 + http4s + Tapir          batch + Structured          │
│  │           + Doobie + PostgreSQL         Streaming                   │
│  ├─ Data processing thuần Scala (Ch19–21)                              │
│  └─ Kafka producer/consumer (Ch28–29)                                  │
│                                                                        │
│  Ranh giới giữa hai thế giới là DỮ LIỆU (Parquet/Avro/JSON, Kafka      │
│  topic, bảng PostgreSQL) — KHÔNG chia sẻ class qua classpath.          │
└────────────────────────────────────────────────────────────────────────┘
```

**Quy tắc bắt buộc:** không trộn artifact `_3` và `_2.13` của cùng một thư viện trên một
classpath. Project Spark là một build riêng (hoặc sub-project riêng) với
`scalaVersion := "2.13.x"`.

### Về các cách "ép" Scala 3 chạy với Spark

Có dự án cộng đồng cho phép viết job Spark bằng Scala 3 qua `CrossVersion.for3Use2_13`
kèm thư viện bổ trợ encoder. Giáo trình **không** dùng cho đường học chính vì:

1. Không được Apache Spark hỗ trợ chính thức.
2. Dataset encoder của Spark dựa vào cơ chế reflection của Scala 2 (TypeTag), không tồn tại trong Scala 3.
3. Lỗi khi chạy trên cluster khó debug hơn nhiều so với lỗi compile.

**Đã kiểm chứng 17/10/2026** (`09-spark/scala3-spark-probe/`, DataFrame API, Spark 4.2.0 `_2.13`, JDK 21; hàng cuối do review chéo T16 tìm ra, Claude chạy lại độc lập):

| Scala | `scala-library` trên classpath | `SparkSession.builder()` (điểm vào chuẩn) | `classic.SparkSession.builder()` (gọi thẳng lớp nội bộ) |
|---|---|---|---|
| 3.3.8 LTS | 2.13.18 | chạy được | — |
| 3.7.3 | 2.13.18 | chạy được | chạy được |
| 3.8.4 | 3.8.4 | `IllegalStateException: Cannot find a SparkSession implementation on the Classpath.` | — |
| 3.9.0 | 3.9.0 | như 3.8.4; lỗi gốc bị che: `FatalError: class Array does not have a member apply` | chạy được: DataFrame, `toDS()` của `Long`, `Encoders.LONG` |

Từ Scala 3.8 thư viện chuẩn là `scala-library` 3.x; builder chuẩn của Spark dùng reflection Scala 2 (`scala-reflect` 2.13) để tìm lớp cài đặt và không đọc được
stdlib 3.x. Encoder **case class tự động** không có ở mọi phiên bản Scala 3 (`value toDS is not a member of Seq[Order]`); encoder kiểu nguyên thủy/tường minh thì
có. **Không tổ hợp Scala 3 nào được Apache hỗ trợ** → job Spark của giáo trình giữ Scala 2.13. Chi tiết: Chương 26 mục 3.8.

**Windows + Spark 4.2 local:** `spark.sql.artifact.isolation.enabled` (mặc định `true`) làm executor tải class qua RPC, mỗi lần một cặp socket loopback — đo
+1.650 TIME_WAIT / 60 truy vấn, suite dài cạn cổng (`Unable to establish loopback connection`). Các lab batch một session đặt `false` (Chương 24 mục 10).

**Spark 4.2 — kiểu timestamp:** `CAST(... AS TIMESTAMP)` là alias theo `spark.sql.timestampType` (mặc định `TIMESTAMP_LTZ`); phiên đặt `TIMESTAMP_NTZ` thì
offset `+07:00`/`Z` bị bỏ qua. Dữ liệu là instant (có múi giờ) → viết `TIMESTAMP_LTZ` tường minh (Chương 24 mục 3.1, Project 3; review chéo vòng 2, 17/10/2026).

**Dagster 1.13.26:** `DailyPartitionsDefinition` cắt ngày theo `timezone` (mặc định UTC) và chỉ có partition của ngày đã kết thúc trừ khi `end_offset=1`
(Chương 31 mục 10, Project 5; kiểm chứng bằng `dagster asset materialize` thật, 17/10/2026).

## 6. Danh sách cần kiểm chứng lại trước từng giai đoạn

- [x] Ch13: Play 3.x và ZIO HTTP — phiên bản, hỗ trợ Scala 3 (16/10/2026).
- [x] Ch15: Slick — hỗ trợ Scala 3; Flyway — phiên bản (16/10/2026).
- [ ] Ch21: thư viện Avro/Parquet cho Scala 3.
- [x] Ch22: Spark 4.2.0 đã GA (11/07/2026). Vẫn **chỉ** có artifact `_2.13` (không có `spark-sql_3`, tra 17/10/2026).
- [x] Spark trên **Windows** (không WSL): Spark 4.2.0 + Scala 2.13.18 + JDK 21 chạy local mode được; GHI file cần `winutils.exe`
      (`HADOOP_HOME and hadoop.home.dir are unset`). Thay thế thuần Java đã kiểm chứng: `com.globalmentor:hadoop-bare-naked-local-fs:0.1.0`
      + `spark.hadoop.fs.file.impl=com.globalmentor.apache.hadoop.fs.BareLocalFileSystem` — ghi/đọc Parquet thành công (17/10/2026).
      Linux/macOS không cần. Cần `--add-opens` cho JDK 17+.
- [x] Ch26: `spark-submit` (spark-4.2.0-bin-hadoop3, SHA-512 khớp) trên **Windows** lỗi khởi tạo SparkContext vì `Executor.updateDependencies` →
      `FileUtil.chmod` cần winutils — BareLocalFileSystem không phủ đường này. Trên **WSL2 Ubuntu 24.04** + Temurin 21.0.12.1: chạy được, mã thoát giữ nguyên.
      Fat jar: Spark `Provided` + `withIncludeScala(false)` (bản phân phối có `scala-library-2.13.18.jar`).
- [x] Ch28: chọn `kafka-clients` 4.4.0 trực tiếp (broker KRaft nhúng 4.4.0 chạy thật); fs2-kafka 4.x kiểm tra lại khi Ch29/P4 cần stream hóa.
- [ ] Doobie 1.0.0 final đã phát hành chưa (16/10/2026: chưa, RC13).
