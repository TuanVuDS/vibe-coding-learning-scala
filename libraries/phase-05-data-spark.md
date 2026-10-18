# Thư viện — Phase 5: Data processing và Apache Spark

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| fs2 core / io | 3.13.0 | `_3` | Luồng hữu hạn, file I/O | Ch19–20 |
| fs2-data CSV / generic | 1.14.1 | `_3` | CSV theo luồng, derive codec | Ch19 |
| Cats core | 2.13.0 | `_3` | Validation, phép gộp | Ch20 |
| Commons CSV | 1.14.1 | Java | CSV RFC4180 | Ch21 |
| Avro | 1.12.2 | Java | Schema và dữ liệu hàng | Ch21 |
| Parquet Avro | 1.18.1 | Java | Dữ liệu cột | Ch21 |
| Hadoop client API / runtime | 3.4.2 | Java | Classpath API/runtime cho Parquet | Ch21 |
| slf4j-nop | 2.0.17 | Java | Bỏ log trong lab format | Ch21 |
| Spark SQL | 4.2.0 | **Chỉ `_2.13`** | Xử lý phân tán | Ch22–26, P3 |
| BareLocalFileSystem | 0.1.0 | Java | Ghi file local Windows | Ch22–26, P3 |
| munit-scalacheck | 1.2.0 | `_3` | Property test | Ch20 |
| munit | 1.2.4 | `_3` / `_2.13` riêng | Unit test | Ch19–26, P3 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

Các dependency dùng lại: **Circe parser 0.14.17** — [phase 4](phase-04-backend.md#circe); **munit 1.2.4** và **munit-scalacheck 1.2.0** — [tooling](tooling.md#scalacheck); **munit-cats-effect 2.2.0** — [phase 4](phase-04-backend.md#effect-testing); **sbt-assembly 2.3.1** trong Ch26/P3 — [tooling](tooling.md#assembly). Không mặc định thư viện Scala `_3` có thể dùng trong job Spark `_2.13`.

Bằng chứng lab đã chạy test: [T08/Ch19](../.team/handoffs/T08-ch19-data-processing.md), [T09/Ch20](../.team/handoffs/T09-ch20-transformation.md), [T06/Ch21](../.team/handoffs/T06-ch21-data-formats.md), [T10/Ch22](../.team/handoffs/T10-ch22-spark-fundamentals.md), [T16/Ch26](../.team/handoffs/T16-ch26-spark-production.md), [P3](../.team/handoffs/P3-batch-pipeline.md). Các benchmark trong chương có điều kiện đo riêng; T27 không tạo số đo mới.

<a id="fs2"></a>
## fs2 core và io

### 1. Là gì / giải quyết vấn đề gì

fs2 mô tả luồng phần tử và các bước xử lý trong effect để dữ liệu có thể đi qua pipeline theo từng khối. Module core cung cấp Stream/Pipe; module io thêm đọc/ghi file, giúp xử lý file lớn mà không cần nạp toàn bộ vào List.

### 2. Phiên bản trong giáo trình

- `co.fs2:fs2-core_3:3.13.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/co/fs2/fs2-core_3/3.13.0/fs2-core_3-3.13.0.pom); HTTP 200, 18/10/2026.
- `co.fs2:fs2-io_3:3.13.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/co/fs2/fs2-io_3/3.13.0/fs2-io_3-3.13.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`Stream[F,A]`, `Pipe`, `through`, `evalMap`, `compile.drain` và `Files[IO]` tách mô tả khỏi thực thi. `compile.toList` vẫn gom toàn bộ output; dedup giữ Set không giới hạn vẫn có thể hết bộ nhớ dù nguồn đọc theo luồng. fs2 chạy trong một app/JVM, không tự biến pipeline thành Spark cluster.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala:58](../08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala#L58). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala:58-69 -->
```scala
      _ <- Files[IO].readUtf8Lines(events)
             .zipWithIndex
             .filter((text, _) => text.trim.nonEmpty)
             .evalMap { (raw, idx) =>
               state.modify { case (s, n, bad) =>
                 val (s2, rejs) = Pipeline.decide(campaigns, now)(s, raw, idx + 1)
                 ((s2, n + 1, bad + rejs.size), rejs)
               }
             }
             .flatMap(Stream.emits)
             .map(r => rejectedJson(r) + "\n")
             .through(text.utf8.encode)
```

### 5. Lựa chọn thay thế

Iterator đơn giản cho xử lý đồng bộ một lượt; fs2 phù hợp khi cần effect, tài nguyên và concurrency có giới hạn. Spark phù hợp dữ liệu cần phân tán nhưng có chi phí scheduler/shuffle và mô hình lỗi khác.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Recursion limit exceeded.” — [08-data-processing/ch19-scala-data-processing.md:375](../08-data-processing/ch19-scala-data-processing.md#L375). Nếu pipe suy luận effect Pure bị vòng, thêm covary[IO] như code lab; không mặc định đây là lỗi đệ quy dữ liệu.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch19-scala-data-processing.md](../08-data-processing/ch19-scala-data-processing.md); [08-data-processing/ch20-functional-data-transformation.md](../08-data-processing/ch20-functional-data-transformation.md); [10-streaming/ch27-streaming-fundamentals.md](../10-streaming/ch27-streaming-fundamentals.md) dùng lại core.

### 8. Tài liệu chính thức

[fs2](https://fs2.io/), [guide](https://fs2.io/#/guide).

<a id="fs2-data"></a>
## fs2-data CSV và generic

### 1. Là gì / giải quyết vấn đề gì

fs2-data bổ sung parser/encoder định dạng dữ liệu cho pipeline fs2. CSV module xử lý quote, delimiter và header; generic module derive codec case class, giúp pipeline event/report không phải tự ghép chuỗi CSV dễ sai.

### 2. Phiên bản trong giáo trình

- `org.gnieh:fs2-data-csv-generic_3:1.14.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/gnieh/fs2-data-csv-generic_3/1.14.1/fs2-data-csv-generic_3-1.14.1.pom); HTTP 200, 18/10/2026.
- `org.gnieh:fs2-data-csv_3:1.14.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/gnieh/fs2-data-csv_3/1.14.1/fs2-data-csv_3-1.14.1.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`CellDecoder` đọc một ô, `CsvRowDecoder` đọc một dòng, `decodeUsingHeaders` đọc theo header; generic semiauto cung cấp derive. `encodeUsingFirstHeaders` sinh CSV cho report. Chọn strict/lenient và đường lỗi chủ động; dòng thiếu cột không nên vô tình dừng toàn bộ batch nếu hợp đồng cho phép quarantine.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala:36](../08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala#L36). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/ingest-lab/src/main/scala/vn/scalaeng/ingest/Ingest.scala:36-48 -->
```scala
  private given CellDecoder[String] = CellDecoder.stringDecoder
  given CsvRowDecoder[Campaign, String] = deriveCsvRowDecoder[Campaign]
  given CellEncoder[LocalDate]       = CellEncoder.stringEncoder.contramap(_.toString)
  given CellEncoder[BigDecimal]      = CellEncoder.stringEncoder.contramap(_.bigDecimal.toPlainString)
  given CsvRowEncoder[ReportRow, String] = deriveCsvRowEncoder[ReportRow]

  def readCampaigns(path: Path): IO[Map[String, Campaign]] =
    Files[IO].readUtf8(path)
      .through(decodeUsingHeaders[Campaign]())
      .map(c => c.copy(campaignId = c.campaignId.trim.toUpperCase(java.util.Locale.ROOT)))
      .compile.toList
      .map(_.map(c => c.campaignId -> c).toMap)

```

### 5. Lựa chọn thay thế

Commons CSV thuận cho chương trình đồng bộ Java API; fs2-data hợp khi pipeline đã là Stream[IO,A]. Parser tự viết ở P1 để học trạng thái quote, không phải lý do tái viết parser cho mọi hệ production.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “No implicit CellDecoder found for type vn.scalaeng.solutions.ch19.OrderRow.” — [08-data-processing/ch19-scala-data-processing.md:387](../08-data-processing/ch19-scala-data-processing.md#L387). row.as[T] cần CellDecoder, không đọc cả row; dùng CsvRowDecoder hoặc decodeUsingHeaders.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch19-scala-data-processing.md](../08-data-processing/ch19-scala-data-processing.md); [solutions/ch19-code/](../solutions/ch19-code/)

### 8. Tài liệu chính thức

[fs2-data CSV](https://fs2-data.gnieh.org/documentation/csv/), [project](https://github.com/gnieh/fs2-data).

<a id="cats-core"></a>
## Cats core

### 1. Là gì / giải quyết vấn đề gì

Cats cung cấp type class và kiểu dữ liệu để viết phép biến đổi/gộp dùng chung mà vẫn nêu được luật của chúng. Ch20 dùng validation tích lũy và thống kê có thể gộp qua nhiều phần, vì dừng ở lỗi đầu hoặc lấy trung bình của các trung bình làm sai yêu cầu dữ liệu.

### 2. Phiên bản trong giáo trình

- `org.typelevel:cats-core_3:2.13.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/typelevel/cats-core_3/2.13.0/cats-core_3-2.13.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`ValidatedNec`, `NonEmptyChain`, `mapN`, `Monoid`, `CommutativeMonoid` và `|+|` biểu diễn validation độc lập và phép gộp. Kết hợp cho phép đổi ngoặc; giao hoán cho phép đổi thứ tự partition. Cats core được khai báo trực tiếp ở Ch20; Cats syntax đã xuất hiện ở backend qua dependency bắc cầu, không phải một runtime mới.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Aggregate.scala:21](../08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Aggregate.scala#L21). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/transform-lab/src/main/scala/vn/scalaeng/transform/Aggregate.scala:21-30 -->
```scala
  given CommutativeMonoid[Stats] with
    def empty: Stats = Stats(0, BigInt(0), Long.MaxValue, Long.MinValue)
    def combine(a: Stats, b: Stats): Stats =
      Stats(a.count + b.count, a.sum + b.sum, a.min.min(b.min), a.max.max(b.max)) // count: Long đủ cho 2^63 dòng

object Aggregate:
  /** Tổng hợp TỔNG QUÁT: nhóm theo khóa K, đổi mỗi phần tử thành V, gộp bằng Monoid[V]. Một hàm cho mọi loại tổng hợp. */
  def by[A, K, V: Monoid](xs: Iterable[A])(key: A => K)(value: A => V): Map[K, V] =
    xs.foldLeft(Map.empty[K, V]) { (acc, a) =>
      val k = key(a)
```

### 5. Lựa chọn thay thế

`Either` phù hợp bước sau phụ thuộc bước trước và muốn dừng sớm; Validated phù hợp kiểm tra độc lập muốn gom lỗi. Hàm fold chuyên biệt dễ đọc khi chỉ có một loại tổng hợp; abstraction Monoid hữu ích khi cần kiểm luật và tái dùng nhiều mô hình thực thi.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “[error] value toList is not a member of cats.data.NonEmptyChainImpl.Type[vn.scalaeng.transform.DataError], but could be made available as an extension method.” — [08-data-processing/ch20-functional-data-transformation.md:373](../08-data-processing/ch20-functional-data-transformation.md#L373). Thêm Cats Foldable syntax; opaque type không tự lộ method của kiểu bên dưới.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch20-functional-data-transformation.md](../08-data-processing/ch20-functional-data-transformation.md); [08-data-processing/transform-lab/project.scala](../08-data-processing/transform-lab/project.scala)

### 8. Tài liệu chính thức

[Cats](https://typelevel.org/cats/), [Validated](https://typelevel.org/cats/datatypes/validated.html), [Monoid](https://typelevel.org/cats/typeclasses/monoid.html).

<a id="commons-csv"></a>
## Apache Commons CSV

### 1. Là gì / giải quyết vấn đề gì

Commons CSV là parser/printer CSV của Java, xử lý trường có dấu phẩy, quote và xuống dòng theo format đã chọn. Ch21 dùng nó để so sánh CSV với JSON/Avro/Parquet trên cùng dữ liệu đơn hàng, tránh nhầm chi phí định dạng với parser tự viết.

### 2. Phiên bản trong giáo trình

- `org.apache.commons:commons-csv:1.14.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/commons/commons-csv/1.14.1/commons-csv-1.14.1.pom); HTTP 200, 18/10/2026.

Thư viện Java; lab Scala 3.9.0/JDK 21 gọi Java API. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`CSVFormat.RFC4180`, builder header, `CSVPrinter` và iterator CSVRecord là các API chính. Header/schema và phân biệt None với chuỗi rỗng vẫn do ứng dụng định nghĩa; lab dùng notePresent để giữ thông tin này khi round-trip.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/TextOrders.scala:58](../08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/TextOrders.scala#L58). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/TextOrders.scala:58-67 -->
```scala
  def writeCsv(path: Path, rows: Iterator[Order], gzip: Boolean): Unit =
    withWriter(path, gzip) { text =>
      Using.resource(CSVPrinter(text, csv)) { out =>
        rows.foreach { raw =>
          val o = Order.checked(raw)
          out.printRecord(List(o.id.toString, o.userId, o.amountMinor.toString, o.currency,
            o.occurredAt.toString, o.note.isDefined.toString, o.note.getOrElse("")).asJava)
        }
      }
    }
```

### 5. Lựa chọn thay thế

fs2-data hợp pipeline effect theo luồng; Commons CSV gọn cho code đồng bộ trên Java Reader/Writer. `split(",")` không xử lý đúng quote nên chỉ dùng khi hợp đồng thật sự loại mọi ký tự đặc biệt và đã kiểm điều kiện đó.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “CSV header bị đảo” — [08-data-processing/ch21-data-formats.md:787](../08-data-processing/ch21-data-formats.md#L787). Đây là rủi ro được kiểm bằng assertion, không có exception được trích ở mục 10; không bịa thông báo Commons CSV.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch21-data-formats.md](../08-data-processing/ch21-data-formats.md); [08-data-processing/formats-lab/src/test/scala/vn/scalaeng/formats/FormatsSuite.scala](../08-data-processing/formats-lab/src/test/scala/vn/scalaeng/formats/FormatsSuite.scala)

### 8. Tài liệu chính thức

[Commons CSV](https://commons.apache.org/proper/commons-csv/), [API](https://commons.apache.org/proper/commons-csv/apidocs/).

<a id="avro"></a>
## Apache Avro

### 1. Là gì / giải quyết vấn đề gì

Avro lưu bản ghi có schema và quy tắc phân giải giữa writer schema với reader schema. Nó giải quyết việc dữ liệu đã ghi ở phiên bản cũ vẫn cần được consumer mới đọc có kiểm soát; schema evolution không đồng nghĩa mọi đổi kiểu đều tương thích.

### 2. Phiên bản trong giáo trình

- `org.apache.avro:avro:1.12.2` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/avro/avro/1.12.2/avro-1.12.2.pom); HTTP 200, 18/10/2026.

Java API; dùng từ Scala 3, không phải wrapper Scala-native. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`Schema`, `GenericRecord`, `GenericDatumReader/Writer`, `DataFileReader/Writer`, union null và default là các điểm chính. Logical timestamp-millis cần hợp đồng chính xác về độ phân giải thời gian; default của reader dùng khi thiếu field, không tùy tiện sửa dữ liệu writer.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/AvroOrders.scala:39](../08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/AvroOrders.scala#L39). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/AvroOrders.scala:39-50 -->
```scala
  def write(path: Path, rows: Iterator[Order], compressed: Boolean): Unit =
    Using.resource(DataFileWriter[GenericRecord](GenericDatumWriter[GenericRecord](schema))) { writer =>
      writer.setCodec(if compressed then CodecFactory.deflateCodec(6) else CodecFactory.nullCodec())
      writer.create(schema, path.toFile)
      rows.foreach(o => writer.append(record(Order.checked(o))))
    }

  def scan(path: Path, consume: Order => Unit): Unit =
    Using.resource(DataFileReader[GenericRecord](path.toFile, GenericDatumReader[GenericRecord](schema))) { reader =>
      while reader.hasNext do consume(order(reader.next()))
    }

```

### 5. Lựa chọn thay thế

JSON dễ đọc và linh hoạt nhưng schema phải kiểm riêng; Parquet tối ưu đọc cột cho phân tích. Chọn Avro cho luồng bản ghi/schema có quy tắc evolution, Parquet cho truy vấn nhiều hàng ít cột; so theo workload và độ nén thật.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “AvroTypeException: Found long, expecting int” — [08-data-processing/ch21-data-formats.md:783](../08-data-processing/ch21-data-formats.md#L783). Reader không tự narrow long về int dù giá trị hiện tại nhỏ; cần migration hoặc reader long.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch21-data-formats.md](../08-data-processing/ch21-data-formats.md); [13-performance/ch34-performance-cost.md](../13-performance/ch34-performance-cost.md) dùng lại Avro.

### 8. Tài liệu chính thức

[Avro 1.12](https://avro.apache.org/docs/1.12.0/), [specification](https://avro.apache.org/docs/1.12.0/specification/).

<a id="parquet"></a>
## Apache Parquet qua parquet-avro

### 1. Là gì / giải quyết vấn đề gì

Parquet là định dạng lưu trữ theo cột, cho phép đọc một tập cột nhỏ mà không giải mã mọi cột. Module parquet-avro chuyển GenericRecord Avro vào/ra Parquet; schema logical giống nhau không làm đặc tính I/O của hai định dạng giống nhau.

### 2. Phiên bản trong giáo trình

- `org.apache.parquet:parquet-avro:1.18.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-avro/1.18.1/parquet-avro-1.18.1.pom); HTTP 200, 18/10/2026.

Java API, gọi từ Scala 3 ở Ch21. Trong Spark, Parquet nằm trong cây Spark; không ép version trực tiếp Ch21 vào classpath Spark. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Row group, page, compression và projection chi phối chi phí đọc. `AvroParquetWriter/Reader`, `PlainParquetConfiguration`, `LocalInputFile/LocalOutputFile` cho phép lab dùng file local qua Java NIO, không gọi Hadoop FileSystem để đặt quyền Windows. Projection tránh đọc cột thừa nhưng không thay cho việc kiểm schema.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala:14](../08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala#L14). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala:14-25 -->
```scala
  def write(path: Path, rows: Iterator[Order], compressed: Boolean): Unit =
    val builder = AvroParquetWriter.builder[GenericRecord](LocalOutputFile(path))
      .withConf(PlainParquetConfiguration())
      .withSchema(AvroOrders.schema)
      .withDataModel(GenericData.get())
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withRowGroupSize(8L * 1024 * 1024)
      .withPageSize(64 * 1024)
      .withCompressionCodec(if compressed then CompressionCodecName.GZIP else CompressionCodecName.UNCOMPRESSED)
    Using.resource(builder.build()) { writer =>
      rows.foreach(o => writer.write(AvroOrders.record(Order.checked(o))))
    }
```

### 5. Lựa chọn thay thế

CSV/JSON phù hợp trao đổi/debug nhỏ; Avro phù hợp đọc bản ghi đầy đủ và schema evolution. Parquet phù hợp batch analytics; nhiều file nhỏ hoặc chỉ đọc vài bản ghi có thể làm metadata/open cost chiếm ưu thế.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “mất phần lẻ timestamp” — [08-data-processing/ch21-data-formats.md:787](../08-data-processing/ch21-data-formats.md#L787). Bài kiểm bằng assertion, không có exception Parquet riêng ở mục 10. Kiểm round-trip độ chính xác thay vì chỉ so schema hình thức.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch21-data-formats.md](../08-data-processing/ch21-data-formats.md); các job Spark Ch22–26 đọc/ghi Parquet qua dependency Spark bắc cầu.

### 8. Tài liệu chính thức

[Parquet](https://parquet.apache.org/docs/), [parquet-java](https://github.com/apache/parquet-java).

<a id="hadoop"></a>
## Hadoop client API và runtime

### 1. Là gì / giải quyết vấn đề gì

Hadoop client cung cấp các class và runtime mà hệ sinh thái Parquet/Hadoop tham chiếu. Ch21 khai báo cả API và runtime để classpath đọc/ghi Parquet đầy đủ; điều đó không có nghĩa lab đang chạy HDFS cluster.

### 2. Phiên bản trong giáo trình

- `org.apache.hadoop:hadoop-client-api:3.4.2` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.pom); HTTP 200, 18/10/2026.
- `org.apache.hadoop:hadoop-client-runtime:3.4.2` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.pom); HTTP 200, 18/10/2026.

Java API/runtime; không có hậu tố Scala. Khai báo trực tiếp 3.4.2 chỉ trong lab formats và lời giải tương ứng. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

API/runtime tách phần giao diện khỏi phần triển khai/phụ thuộc đóng gói. Lab Parquet chọn PlainParquetConfiguration và LocalInput/OutputFile nên bỏ đường Hadoop FileSystem native cho file local; classpath vẫn cần các class liên quan. Spark quản Hadoop riêng trong bản phân phối.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala:6](../08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala#L6). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/formats-lab/src/main/scala/vn/scalaeng/formats/ParquetOrders.scala:6-17 -->
```scala
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{InputFile, LocalInputFile, LocalOutputFile}
import scala.util.Using

/** Java API called from Scala 3; local NIO paths bypass Hadoop FileSystem / winutils. */
object ParquetOrders:
  def write(path: Path, rows: Iterator[Order], compressed: Boolean): Unit =
    val builder = AvroParquetWriter.builder[GenericRecord](LocalOutputFile(path))
      .withConf(PlainParquetConfiguration())
      .withSchema(AvroOrders.schema)
```

### 5. Lựa chọn thay thế

Chạy Hadoop/HDFS thật chỉ khi cần semantics storage đó; NIO local đơn giản hơn cho unit test format. Không xóa runtime chỉ vì code không import trực tiếp: dependency có thể cần class khi load.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “HADOOP_HOME and hadoop.home.dir are unset” — [09-spark/ch22-spark-fundamentals.md:412](../09-spark/ch22-spark-fundamentals.md#L412). Lỗi thực tế của đường Hadoop trên Windows ở Spark; local NIO Ch21 và BareLocalFileSystem Spark là hai cách tránh hai đường khác nhau, không bao phủ mọi spark-submit.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch21-data-formats.md](../08-data-processing/ch21-data-formats.md); [08-data-processing/formats-lab/build.sbt](../08-data-processing/formats-lab/build.sbt)

### 8. Tài liệu chính thức

[Hadoop 3.4.2 docs](https://hadoop.apache.org/docs/r3.4.2/), [Parquet Java](https://github.com/apache/parquet-java).

<a id="slf4j-nop"></a>
## SLF4J NOP provider

### 1. Là gì / giải quyết vấn đề gì

slf4j-nop là backend chủ ý không ghi log cho SLF4J. Nó làm lab format/benchmark gọn hơn khi đang quan sát kết quả dữ liệu, nhưng bỏ backend log cũng làm mất tín hiệu chẩn đoán nên không dùng như cấu hình vận hành mặc định.

### 2. Phiên bản trong giáo trình

- `org.slf4j:slf4j-nop:2.0.17` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/slf4j/slf4j-nop/2.0.17/slf4j-nop-2.0.17.pom); HTTP 200, 18/10/2026.
- `org.slf4j:slf4j-nop:2.0.20` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/slf4j/slf4j-nop/2.0.20/slf4j-nop-2.0.20.pom); HTTP 200, 18/10/2026.

Java/provider, không có binary Scala. Hai POM đều HTTP 200 ngày 18/10/2026; không tự nâng đồng loạt trong task tài liệu. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Facade SLF4J nhận lời gọi; provider chọn đích ghi, NOP bỏ chúng. Build formats/cost dùng **2.0.17**, Kafka/P4/P5 dùng **2.0.20**; cùng tên thư viện nhưng hai version được giữ riêng theo build. Code app không cần gọi API NOP trực tiếp; provider được tìm qua classpath.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/formats-lab/project.scala:4](../08-data-processing/formats-lab/project.scala#L4). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/formats-lab/project.scala:4-11 -->
```scala
//> using dep io.circe::circe-parser:0.14.17
//> using dep org.apache.commons:commons-csv:1.14.1
//> using dep org.apache.avro:avro:1.12.2
//> using dep org.apache.parquet:parquet-avro:1.18.1
//> using dep org.apache.hadoop:hadoop-client-api:3.4.2
//> using dep org.apache.hadoop:hadoop-client-runtime:3.4.2
//> using dep org.slf4j:slf4j-nop:2.0.17
//> using test.dep org.scalameta::munit:1.2.4
```

Đây là cấu hình classpath của lab đã test, không phải lời gọi API của provider.
### 5. Lựa chọn thay thế

Logback ở phase 4 cho backend có log thật; NOP chỉ phù hợp lab cố ý im lặng. Khi cần điều tra Spark/Kafka, bật logging phù hợp và kiểm chỉ một provider có hiệu lực.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “SLF4J: Defaulting to no-operation (NOP) logger implementation” — [05-backend/ch13-choosing-a-framework.md:514](../05-backend/ch13-choosing-a-framework.md#L514). Đây là fallback khi thiếu binding ở lab HTTP, khác việc chủ động khai báo provider 2.x. Đừng coi việc hết cảnh báo là bằng chứng đã có log.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch21-data-formats.md](../08-data-processing/ch21-data-formats.md); [13-performance/ch34-performance-cost.md](../13-performance/ch34-performance-cost.md); [10-streaming/ch28-kafka-scala.md](../10-streaming/ch28-kafka-scala.md) dùng bản 2.0.20.

### 8. Tài liệu chính thức

[SLF4J manual](https://www.slf4j.org/manual.html), [diagnostic codes](https://www.slf4j.org/codes.html).

<a id="spark"></a>
## Apache Spark SQL

### 1. Là gì / giải quyết vấn đề gì

Spark xây dựng kế hoạch xử lý phân tán để transform/join/aggregate dữ liệu vượt khả năng một tiến trình. Module spark-sql cung cấp DataFrame/Dataset và kéo Spark core bắc cầu; scheduler, shuffle và retry là chi phí/semantics cần học cùng API.

### 2. Phiên bản trong giáo trình

- `org.apache.spark:spark-sql_2.13:4.2.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql_2.13/4.2.0/spark-sql_2.13-4.2.0.pom); HTTP 200, 18/10/2026.

**Chỉ dùng Scala 2.13.18 / JDK 21 trong giáo trình. Artifact `_2.13` không chứng minh hỗ trợ Scala 3.** Probe Scala 3.7.3 gọi tọa độ `_2.13` tường minh; đọc compatibility matrix để hiểu giới hạn runtime/encoder. Không trộn `_3` và `_2.13` của cùng họ. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`SparkSession`, DataFrame, Column, transformation/action, partition và shuffle là mô hình chính. Transformation lazy; action mới chạy, retry có thể chạy lại side effect. Spark SQL/Catalyst tối ưu plan; test local không xác nhận toàn bộ network/storage/recovery của cluster thật.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/Experiments.scala:20](../09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/Experiments.scala#L20). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/Experiments.scala:20-27 -->
```scala
  def lazyEvaluation(spark: SparkSession, n: Long): LazyResult = {
    import spark.implicits._
    val calls   = spark.sparkContext.longAccumulator("map calls")
    val doubled = spark.range(0, n).map { x => calls.add(1); x * 2 } // transformation: CHƯA chạy
    val before  = calls.value
    val rows    = doubled.filter($"value" % 3 === 0).count()        // action: chạy cả chuỗi
    LazyResult(before, calls.value, rows)
  }
```

### 5. Lựa chọn thay thế

Collection/Iterator/fs2 phù hợp dữ liệu một máy và logic ít chi phí vận hành; Spark phù hợp batch phân tán và SQL lớn. Không chuyển List.map nguyên xi rồi giả định cùng hiệu năng/semantics; đo action và xem physical plan.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Error downloading org.apache.spark:spark-sql_3:4.2.0 ... not found” — [09-spark/ch22-spark-fundamentals.md:410](../09-spark/ch22-spark-fundamentals.md#L410). Dùng build Scala 2.13 riêng. Probe Scala 3 trong repo là minh họa giới hạn, không phải đường học production.

### 7. Dùng ở đâu trong giáo trình

[09-spark/ch22-spark-fundamentals.md](../09-spark/ch22-spark-fundamentals.md); [09-spark/ch23-spark-sql-dataframes.md](../09-spark/ch23-spark-sql-dataframes.md); [09-spark/ch24-batch-data-pipeline.md](../09-spark/ch24-batch-data-pipeline.md); [09-spark/ch25-spark-performance.md](../09-spark/ch25-spark-performance.md); [09-spark/ch26-spark-production-engineering.md](../09-spark/ch26-spark-production-engineering.md); [14-projects/p3-batch-pipeline](../14-projects/p3-batch-pipeline)

### 8. Tài liệu chính thức

[Spark 4.2.0](https://spark.apache.org/docs/4.2.0/), [SQL guide](https://spark.apache.org/docs/4.2.0/sql-programming-guide.html), [Scala 3 support issue](https://issues.apache.org/jira/browse/SPARK-54150).

<a id="bare-local-fs"></a>
## hadoop-bare-naked-local-fs

### 1. Là gì / giải quyết vấn đề gì

BareLocalFileSystem cung cấp filesystem local thuần Java để các thao tác Hadoop file local trong Spark tránh một số đường gọi winutils trên Windows. Nó là trợ giúp môi trường lab, không phải engine xử lý dữ liệu hay bản thay thế mọi thành phần native của Hadoop.

### 2. Phiên bản trong giáo trình

- `com.globalmentor:hadoop-bare-naked-local-fs:0.1.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.pom); HTTP 200, 18/10/2026.

Java/Hadoop extension, không có hậu tố Scala; bản 0.1.0 dùng trong các build Spark Scala 2.13. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

Class `com.globalmentor.apache.hadoop.fs.BareLocalFileSystem` được chọn bằng `spark.hadoop.fs.file.impl`. Artifact không cần import trong mã nghiệp vụ vì Hadoop nạp class theo tên cấu hình. Dùng local mode đọc/ghi file khác với spark-submit tải jar và gọi chmod.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/LocalSpark.scala:11](../09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/LocalSpark.scala#L11). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 09-spark/spark-fundamentals-lab/src/main/scala/vn/scalaeng/spark/LocalSpark.scala:11-20 -->
```scala
  def session(app: String, cores: Int = 2, maxFailures: Int = 1): SparkSession =
    SparkSession
      .builder()
      .master(s"local[$cores,$maxFailures]")
      .appName(app)
      .config("spark.ui.enabled", "false")
      // Windows: thay LocalFileSystem của Hadoop (cần winutils.exe để đặt quyền file) bằng bản thuần Java.
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
      .getOrCreate()
}
```

### 5. Lựa chọn thay thế

Linux/WSL2 là môi trường đã dùng để kiểm spark-submit; cấu hình này tiện cho unit test local Windows. Không thêm workaround vào cluster khi không có nhu cầu và chưa kiểm semantics filesystem.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “ERROR SparkContext: Error initializing SparkContext.” — [09-spark/ch26-spark-production-engineering.md:416](../09-spark/ch26-spark-production-engineering.md#L416). Đường Executor.updateDependencies → FileUtil.chmod vẫn cần winutils trên Windows; workaround local filesystem không phủ đường đó.

### 7. Dùng ở đâu trong giáo trình

[09-spark/ch22-spark-fundamentals.md](../09-spark/ch22-spark-fundamentals.md); Ch23–26, P3; [11-data-platform/distributed-lab/project.scala](../11-data-platform/distributed-lab/project.scala) dùng lại.

### 8. Tài liệu chính thức

[Bare Naked Local FileSystem](https://github.com/globalmentor/hadoop-bare-naked-local-fs).

