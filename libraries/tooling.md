# Thư viện — Phase toàn khóa: Công cụ build, kiểm thử và môi trường

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| JDK | 21 | Có; cả 2.13 | Chạy JVM, chẩn đoán | Ch2, Ch11 |
| Scala CLI | 1.9.1 trên máy | Có; chọn bằng directive | Lab nhỏ, test | Ch2, toàn khóa |
| Coursier | 2.1.25-M19 trên máy | Quản lý artifact | Cài tool, cache | Ch2 |
| sbt | 1.12.11 | Build definition 2.12; app 3/2.13 | Nhiều module | Ch9, P1–P5 |
| munit | 1.2.4 | `_3`, `_2.13` | Unit test | Toàn khóa |
| munit-scalacheck / ScalaCheck | 1.2.0 / 1.19.0 bắc cầu | `_3` | Property test | Ch20, Ch33 |
| sbt-scalafmt / scalafmt | 2.5.5 / 3.10.7 | Plugin sbt 1.x | Định dạng | Ch9 |
| sbt-assembly | 2.3.1 | Plugin sbt 1.x | Fat jar | Ch26, P2/P3/P5 |
| Docker | CLI/engine 28.5.1 (bằng chứng P2) | Ngoài JVM | Container PostgreSQL/app | Ch15, P2 |
| WSL2 | WSL 2.6.1.0; Ubuntu 24.04 ở Ch26 | Ngoài JVM | spark-submit trên Linux | Ch26, P3 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

Các phiên bản công cụ trên máy được đọc bằng lệnh `version` ngày 18/10/2026; không phải ràng buộc bắt người học nâng/hạ bản đang hoạt động. `sbt` lấy từ `project/build.properties`, **không khởi chạy sbt trong T27**. JDK trên PATH là Oracle 21.0.7; JVM Scala CLI chọn theo directive có thể khác (bàn giao T01 ghi Temurin 21.0.12.1). Đừng dùng `java -version` trên PATH để suy ra JVM của lab.

<a id="jdk"></a>
## JDK 21

### 1. Là gì / giải quyết vấn đề gì

JDK (Java Development Kit) cung cấp compiler Java, JVM và công cụ quan sát tiến trình. Scala được biên dịch thành bytecode rồi JVM chạy; vì vậy phiên bản Java là một phần của môi trường backend/data, kể cả khi không viết Java.

### 2. Phiên bản trong giáo trình

**JDK 21 LTS**, không có artifact app để khai báo. Máy hiện tại: Oracle 21.0.7 (lệnh `java -version`); lab dùng `//> using jvm 21`. [Trang JDK 21](https://openjdk.org/projects/jdk/21/) và [release Temurin](https://adoptium.net/temurin/releases/?version=21), đối chiếu ngày 18/10/2026.

### 3. Khái niệm cốt lõi

JVM nạp class, cấp phát heap và stack, thực thi bytecode/JIT và thu gom rác. `java`, `javac`, `jcmd`, JFR phục vụ các việc khác nhau; `--add-opens` trong lab Spark là cấu hình runtime, không phải Scala compiler flag.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [01-scala3-core/code/project.scala:1](../01-scala3-core/code/project.scala#L1). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 01-scala3-core/code/project.scala:1-8 -->
```scala
// Cấu hình Scala CLI cho toàn bộ code của Giai đoạn 1 (Scala 3 Core).
// Chạy từ thư mục này:
//   scala-cli run . --main-class scalaeng.ch02.hello
//   scala-cli test .
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all
//> using test.dep org.scalameta::munit:1.2.4
```

### 5. Lựa chọn thay thế

JDK 17 phù hợp một số stack nhưng giáo trình cố định 21 để dùng chung; JDK mới hơn cần kiểm lại Spark và dependency. Chọn bản phân phối có hỗ trợ bảo mật phù hợp thay vì chỉ nhìn tên Oracle/Temurin.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “OutOfMemoryError: Java heap space” — [04-professional-engineering/ch11-jvm-fundamentals.md:326](../04-professional-engineering/ch11-jvm-fundamentals.md#L326). Xem heap dump và dữ liệu giữ lại trước khi tăng heap; chừa bộ nhớ ngoài heap cho container.

### 7. Dùng ở đâu trong giáo trình

[01-scala3-core/ch02-environment-setup.md](../01-scala3-core/ch02-environment-setup.md); [04-professional-engineering/ch11-jvm-fundamentals.md](../04-professional-engineering/ch11-jvm-fundamentals.md)

### 8. Tài liệu chính thức

[JDK 21](https://openjdk.org/projects/jdk/21/), [JFR](https://docs.oracle.com/en/java/javase/21/jfapi/).

<a id="scala-cli"></a>
## Scala CLI

### 1. Là gì / giải quyết vấn đề gì

Scala CLI biến một file hoặc thư mục thành chương trình có thể compile/run/test bằng directive. Nó giảm lượng cấu hình khi học và cho reviewer chạy lại lab độc lập; không tái tạo đầy đủ graph module/scopes của sbt.

### 2. Phiên bản trong giáo trình

**1.9.1** từ `scala-cli.bat version`; Scala mặc định 3.7.3, nhưng build chọn **3.9.0** hoặc **2.13.18**. [Release v1.9.1](https://github.com/VirtusLab/scala-cli/releases/tag/v1.9.1), kiểm phiên bản cài ngày 18/10/2026.

### 3. Khái niệm cốt lõi

`using scala`, `jvm`, `dep`, `test.dep`, `options` chọn compiler, runtime và classpath. `::` thêm hậu tố binary Scala; `:` dùng tên artifact nguyên vẹn. Trong sandbox này luôn dùng `--server=false` để tránh Bloop.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [01-scala3-core/code/project.scala:1](../01-scala3-core/code/project.scala#L1). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 01-scala3-core/code/project.scala:1-8 -->
```scala
// Cấu hình Scala CLI cho toàn bộ code của Giai đoạn 1 (Scala 3 Core).
// Chạy từ thư mục này:
//   scala-cli run . --main-class scalaeng.ch02.hello
//   scala-cli test .
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all
//> using test.dep org.scalameta::munit:1.2.4
```

### 5. Lựa chọn thay thế

Chọn Scala CLI cho script/lab và sbt cho nhiều module, assembly hoặc cấu hình publishing. Nếu chạy test bằng CLI thay cho sbt, phải ghi rõ phạm vi kiểm chứng khác nhau.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Multiple main classes detected” — [01-scala3-core/ch02-environment-setup.md:304](../01-scala3-core/ch02-environment-setup.md#L304). Chỉ rõ `--main-class` khi run; ví dụ directive trên không tự chọn main.

### 7. Dùng ở đâu trong giáo trình

[01-scala3-core/ch02-environment-setup.md](../01-scala3-core/ch02-environment-setup.md); mọi lab có `project.scala`.

### 8. Tài liệu chính thức

[Directives](https://scala-cli.virtuslab.org/docs/reference/directives/), [dependency directives](https://scala-cli.virtuslab.org/docs/reference/directives/#dependency).

<a id="coursier"></a>
## Coursier

### 1. Là gì / giải quyết vấn đề gì

Coursier (`cs`) cài các lệnh Scala và giải quyết dependency từ kho Maven. Nó giúp các lab tải đúng tọa độ và tái sử dụng cache; cài tool với `cs` không đồng nghĩa dependency của mọi project dùng cùng version.

### 2. Phiên bản trong giáo trình

**2.1.25-M19** từ `cs.bat version` ngày 18/10/2026; không có dependency app. [Releases](https://github.com/coursier/coursier/releases). Chưa chạy lại quy trình cài đặt.

### 3. Khái niệm cốt lõi

Resolver đọc POM và lấy cả dependency bắc cầu. Cache lưu file tải về; `COURSIER_CACHE` đổi chỗ cache Maven, còn JVM cache có cấu hình riêng. Directive dưới là đầu vào mà Scala CLI chuyển cho cơ chế tải, không phải API Coursier.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [01-scala3-core/code/project.scala:1](../01-scala3-core/code/project.scala#L1). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 01-scala3-core/code/project.scala:1-8 -->
```scala
// Cấu hình Scala CLI cho toàn bộ code của Giai đoạn 1 (Scala 3 Core).
// Chạy từ thư mục này:
//   scala-cli run . --main-class scalaeng.ch02.hello
//   scala-cli test .
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all
//> using test.dep org.scalameta::munit:1.2.4
```

### 5. Lựa chọn thay thế

Có thể cài JDK/tool bằng trình quản lý hệ điều hành; Coursier thuận tiện cho hệ Scala. Khi mạng/proxy có vấn đề, kiểm URL artifact thật trước; đổi version ngẫu nhiên dễ làm lệch giáo trình.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “command not found: scala” — [01-scala3-core/ch02-environment-setup.md:299](../01-scala3-core/ch02-environment-setup.md#L299). Đây là triệu chứng PATH trong chương; không phải khẳng định đã tái chạy cài Coursier trong T27.

### 7. Dùng ở đâu trong giáo trình

[01-scala3-core/ch02-environment-setup.md](../01-scala3-core/ch02-environment-setup.md)

### 8. Tài liệu chính thức

[Overview](https://get-coursier.io/docs/overview), [cache](https://get-coursier.io/docs/cache), [CLI](https://get-coursier.io/docs/cli-overview).

<a id="sbt"></a>
## sbt

### 1. Là gì / giải quyết vấn đề gì

sbt là build tool quản lý project graph, task, cấu hình và dependency. Nhiều module backend cần tách domain/core/shell và scope Test; sbt biểu diễn ranh giới đó ngay trong build.

### 2. Phiên bản trong giáo trình

**1.12.11**, chốt tại [04-professional-engineering/order-platform/project/build.properties](../04-professional-engineering/order-platform/project/build.properties). [POM sbt](https://repo.maven.apache.org/maven2/org/scala-sbt/sbt/1.12.11/sbt-1.12.11.pom). T27 chỉ đọc cấu hình; không chạy launcher.

### 3. Khái niệm cốt lõi

`ThisBuild`, `settings`, `dependsOn`, `aggregate` có vai trò riêng: aggregate chạy task trên module con, không tự thêm dependency. `% Test` chỉ đưa vào test classpath; `%%` chọn hậu tố Scala của ứng dụng. Build definition sbt 1.x vẫn là Scala 2.12.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [04-professional-engineering/order-platform/build.sbt:23](../04-professional-engineering/order-platform/build.sbt#L23). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 04-professional-engineering/order-platform/build.sbt:23-32 -->
```scala
lazy val domain = project
  .in(file("modules/domain"))
  .settings(commonSettings, name := "order-domain")

// Logic nghiệp vụ: chỉ phụ thuộc domain
lazy val core = project
  .in(file("modules/core"))
  .dependsOn(domain, testkit % Test)                      // testkit chỉ có trên classpath khi test
  .settings(commonSettings, name := "order-core")

```

Đây là cấu hình build gốc; bằng chứng test lab không chứng minh plugin/task sbt đã được chạy trong T27.
### 5. Lựa chọn thay thế

Scala CLI ngắn gọn hơn cho lab đơn. sbt đáng dùng khi cần kiểm ranh giới module, packaging và CI giống production; đổi sang tool khác có chi phí duy trì thêm cấu hình.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Build definition sbt 1.x là Scala 2.12” — [04-professional-engineering/ch09-project-structure.md:386](../04-professional-engineering/ch09-project-structure.md#L386). Viết cú pháp Scala 2 trong build.sbt dù app dùng Scala 3.

### 7. Dùng ở đâu trong giáo trình

[04-professional-engineering/ch09-project-structure.md](../04-professional-engineering/ch09-project-structure.md); P1–P5.

### 8. Tài liệu chính thức

[Reference manual](https://www.scala-sbt.org/1.x/docs/), [library dependencies](https://www.scala-sbt.org/1.x/docs/Library-Dependencies.html).

<a id="munit"></a>
## munit

### 1. Là gì / giải quyết vấn đề gì

munit là test framework chính, chạy ví dụ nghiệp vụ và kiểm hồi quy khi sửa lỗi. Các assertion cho biết hành vi mong muốn; suite xanh chỉ xác nhận những ca đã kiểm chứ không bảo đảm mọi quy tắc đều đúng.

### 2. Phiên bản trong giáo trình

- `org.scalameta:munit_2.13:1.2.4` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/scalameta/munit_2.13/1.2.4/munit_2.13-1.2.4.pom); HTTP 200, 18/10/2026.
- `org.scalameta:munit_3:1.2.4` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/scalameta/munit_3/1.2.4/munit_3-1.2.4.pom); HTTP 200, 18/10/2026.

Có hai artifact riêng `_3` và `_2.13`; nhánh Spark dùng `_2.13`, không trộn cùng classpath. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`FunSuite`, `test`, `assertEquals`, `intercept` tạo ca kiểm thử; `assume` đánh dấu thiếu điều kiện chạy, không biến skip thành pass. Framework test của sbt/Scala CLI phát hiện suite từ classpath Test.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:5](../04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala#L5). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:5-15 -->
```scala
class DomainSuite extends FunSuite:
  test("OrderId"):
    assert(OrderId.parse(" ORD-12 ").isRight)
    assert(OrderId.parse("ORD-").isLeft)
    assert(OrderId.parse("ord-1").isLeft)

  test("Vnd và Quantity"):
    val total = for
      p <- Vnd.parse("120000")
      q <- Quantity.parse("3")
    yield (p * q).dong
```

### 5. Lựa chọn thay thế

ScalaTest có nhiều phong cách đặc tả hơn nhưng tăng lựa chọn cho người mới. ZIO Test phù hợp lab chạy ZIO; giáo trình giữ munit làm mặc định để các module đồng nhất.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Kết quả mong đợi tính bằng chính code đang test” — [12-testing/ch10-testing.md:310](../12-testing/ch10-testing.md#L310). Giữ expected độc lập để tránh cùng sai mà test vẫn xanh.

### 7. Dùng ở đâu trong giáo trình

[12-testing/ch10-testing.md](../12-testing/ch10-testing.md); [04-professional-engineering/order-platform](../04-professional-engineering/order-platform); mọi lab có test munit.

### 8. Tài liệu chính thức

[munit](https://scalameta.org/munit/), [assertions](https://scalameta.org/munit/docs/assertions.html).

<a id="scalacheck"></a>
## ScalaCheck qua munit-scalacheck

### 1. Là gì / giải quyết vấn đề gì

ScalaCheck kiểm tra thuộc tính trên nhiều đầu vào sinh tự động, giúp tìm ca biên mà ví dụ thủ công dễ bỏ sót. `munit-scalacheck` nối mô hình property với báo cáo munit; Ch10 có testkit tự viết để giải thích ý tưởng, chưa dùng thư viện ScalaCheck.

### 2. Phiên bản trong giáo trình

- `org.scalameta:munit-scalacheck_3:1.2.0` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/scalameta/munit-scalacheck_3/1.2.0/munit-scalacheck_3-1.2.0.pom); HTTP 200, 18/10/2026.

Có artifact `_3`; dùng Scala 3.9.0, JDK 21. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`Gen`, `Prop.forAll`, shrinking (thu nhỏ đầu vào lỗi), seed (hạt giống tái hiện) và `ScalaCheckSuite` là các khái niệm chính. Generator xác định miền kiểm tra; 100 ca sinh không chứng minh định luật trên mọi giá trị. POM adapter 1.2.0 kéo **org.scalacheck:scalacheck_3:1.19.0** bắc cầu và munit 1.1.1; build khai báo munit 1.2.4 trực tiếp.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [08-data-processing/transform-lab/src/test/scala/vn/scalaeng/transform/PropertySuite.scala:20](../08-data-processing/transform-lab/src/test/scala/vn/scalaeng/transform/PropertySuite.scala#L20). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 08-data-processing/transform-lab/src/test/scala/vn/scalaeng/transform/PropertySuite.scala:20-27 -->
```scala
  property("Monoid[Stats]: kết hợp — (a |+| b) |+| c == a |+| (b |+| c)"):
    forAll(genStatsOrEmpty, genStatsOrEmpty, genStatsOrEmpty)((a, b, c) => ((a |+| b) |+| c) == (a |+| (b |+| c)))

  property("Monoid[Stats]: giao hoán — a |+| b == b |+| a (điều kiện cho partition không thứ tự)"):
    forAll(genStatsOrEmpty, genStatsOrEmpty)((a, b) => (a |+| b) == (b |+| a))

  property("Monoid[Stats]: phần tử trung hòa — empty |+| a == a == a |+| empty"):
    forAll(genStatsOrEmpty)(a => (Monoid[Stats].empty |+| a) == a && (a |+| Monoid[Stats].empty) == a)
```

### 5. Lựa chọn thay thế

Ví dụ unit test cụ thể dễ giải thích và thích hợp lỗi đã biết; property test tốt cho round-trip, định luật gộp và bất biến dữ liệu. Testkit Ch10/P1 đơn giản để học, không thay thế toàn bộ shrinker và hệ sinh thái ScalaCheck.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “generator thêm giá trị biên” — [08-data-processing/ch20-functional-data-transformation.md:395](../08-data-processing/ch20-functional-data-transformation.md#L395). Generator phải có giá trị biên; không sửa luật để làm test xanh.

### 7. Dùng ở đâu trong giáo trình

[08-data-processing/ch20-functional-data-transformation.md](../08-data-processing/ch20-functional-data-transformation.md); [11-data-platform/ch33-data-quality-testing.md](../11-data-platform/ch33-data-quality-testing.md). Ch10 dùng `vn.scalaeng.order.testkit`, không phải `org.scalacheck`.

### 8. Tài liệu chính thức

[ScalaCheck](https://scalacheck.org/), [adapter munit](https://scalameta.org/munit/docs/integrations/scalacheck.html), [POM ScalaCheck 1.19.0](https://repo.maven.apache.org/maven2/org/scalacheck/scalacheck_3/1.19.0/scalacheck_3-1.19.0.pom).

<a id="scalafmt"></a>
## sbt-scalafmt và scalafmt

### 1. Là gì / giải quyết vấn đề gì

scalafmt chuẩn hóa trình bày mã để review tập trung vào hành vi. Plugin sbt chỉ tích hợp task; engine định dạng có phiên bản riêng nên cập nhật plugin chưa chắc đổi kết quả format.

### 2. Phiên bản trong giáo trình

- `org.scalameta:sbt-scalafmt_2.12_1.0:2.5.5` — [POM Maven Central](https://repo.maven.apache.org/maven2/org/scalameta/sbt-scalafmt_2.12_1.0/2.5.5/sbt-scalafmt_2.12_1.0-2.5.5.pom); HTTP 200, 18/10/2026.

Plugin `_2.12_1.0` dành cho sbt 1.x; engine đọc cú pháp Scala 3 theo dialect, không thêm plugin này vào dependency app. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`scalafmt` sửa định dạng, `scalafmtCheckAll` kiểm trong CI. `.scalafmt.conf` của order-platform chọn engine **3.10.7**, dialect `scala3`, cột 110; plugin **2.5.5** chạy bên build sbt 1.x. Chưa chạy task formatter trong T27.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:5](../04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala#L5). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:5-12 -->
```scala
class DomainSuite extends FunSuite:
  test("OrderId"):
    assert(OrderId.parse(" ORD-12 ").isRight)
    assert(OrderId.parse("ORD-").isLeft)
    assert(OrderId.parse("ord-1").isLeft)

  test("Vnd và Quantity"):
    val total = for
```

Đây là ví dụ phong cách mã trong lab; không phải output của lần chạy formatter T27. Cấu hình chính xác: [04-professional-engineering/order-platform/.scalafmt.conf](../04-professional-engineering/order-platform/.scalafmt.conf).
### 5. Lựa chọn thay thế

IDE formatter dùng được khi cả nhóm khóa cùng cấu hình; scalafmt thuận tiện để CI và máy cá nhân thống nhất. Không dùng format thay cho kiểm correctness.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Build definition sbt 1.x là Scala 2.12” — [04-professional-engineering/ch09-project-structure.md:386](../04-professional-engineering/ch09-project-structure.md#L386). Hậu tố plugin 2.12_1.0 thuộc build tool, không cho biết app có hỗ trợ Scala 3 hay không.

### 7. Dùng ở đâu trong giáo trình

[04-professional-engineering/ch09-project-structure.md](../04-professional-engineering/ch09-project-structure.md); [04-professional-engineering/order-platform/project/plugins.sbt](../04-professional-engineering/order-platform/project/plugins.sbt)

### 8. Tài liệu chính thức

[Installation](https://scalameta.org/scalafmt/docs/installation.html), [configuration](https://scalameta.org/scalafmt/docs/configuration.html), [engine POM 3.10.7](https://repo.maven.apache.org/maven2/org/scalameta/scalafmt-core_2.13/3.10.7/scalafmt-core_2.13-3.10.7.pom).

<a id="assembly"></a>
## sbt-assembly

### 1. Là gì / giải quyết vấn đề gì

sbt-assembly gom bytecode ứng dụng và dependency cần mang theo thành fat jar. Nó giúp triển khai backend hoặc Spark job mà không phải chép thủ công nhiều jar; chọn sai scope có thể đưa cả Spark/Scala runtime vào gói.

### 2. Phiên bản trong giáo trình

- `com.eed3si9n:sbt-assembly_2.12_1.0:2.3.1` — [POM Maven Central](https://repo.maven.apache.org/maven2/com/eed3si9n/sbt-assembly_2.12_1.0/2.3.1/sbt-assembly_2.12_1.0-2.3.1.pom); HTTP 200, 18/10/2026.

Plugin `_2.12_1.0` chạy trong sbt 1.x, độc lập binary version của app. Ngày kiểm chứng POM: 18/10/2026; phiên bản bám build, không có nghĩa là bản mới nhất.

### 3. Khái niệm cốt lõi

`assembly`, `assemblyOption.withIncludeScala(false)`, `Provided` và merge strategy điều khiển nội dung jar. Spark do cluster cấp nên không đóng vào job; `META-INF/services` có service loader cần concat, không được bỏ sạch theo thói quen.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [09-spark/spark-production-lab/build.sbt:43](../09-spark/spark-production-lab/build.sbt#L43). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 09-spark/spark-production-lab/build.sbt:43-53 -->
```scala
    assembly / mainClass := Some("vn.scalaeng.sparkprod.Job"),
    assembly / assemblyJarName := "spark-production-job.jar",
    // Spark trên cụm đã có scala-library 2.13 — đóng thêm một bản vào jar chỉ gây xung đột phiên bản và làm jar to gấp chục lần.
    assembly / assemblyOption ~= (_.withIncludeScala(false)),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    },
    run / fork := true,
    Test / fork := true,
```

Đoạn cấu hình được trích từ build đã có bằng chứng assembly/spark-submit ở Ch26; không chạy task này trong T27.
### 5. Lựa chọn thay thế

Scala CLI package thuận tiện với app nhỏ; container giải quyết môi trường triển khai nhưng vẫn cần artifact JVM đúng. Thin jar nhỏ hơn nhưng môi trường phải cung cấp chính xác dependency.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Quên loại Scala khỏi fat jar” — [09-spark/ch26-spark-production-engineering.md:446](../09-spark/ch26-spark-production-engineering.md#L446). Kiểm nội dung jar và scope thay vì chỉ nhìn build thành công.

### 7. Dùng ở đâu trong giáo trình

[09-spark/ch26-spark-production-engineering.md](../09-spark/ch26-spark-production-engineering.md); [14-projects/p2-backend-service](../14-projects/p2-backend-service), [14-projects/p3-batch-pipeline](../14-projects/p3-batch-pipeline), [14-projects/p5-data-platform](../14-projects/p5-data-platform).

### 8. Tài liệu chính thức

[sbt-assembly](https://github.com/sbt/sbt-assembly).

<a id="docker"></a>
## Docker

### 1. Là gì / giải quyết vấn đề gì

Docker đóng gói tiến trình với filesystem và cấu hình chạy nhất quán; Compose mô tả app và PostgreSQL cùng mạng/volume. Nó giúp người học chạy môi trường tích hợp, nhưng image có sẵn không đồng nghĩa daemon đang chạy hay dữ liệu đã bền.

### 2. Phiên bản trong giáo trình

**Docker CLI 28.5.1**, `docker --version` ngày 18/10/2026; P2 ghi engine **28.5.1**. Container dùng `postgres:18.6`, Temurin JDK/JRE 21. [Docker Engine releases](https://docs.docker.com/engine/release-notes/28/). Chưa chạy daemon/build/up trong T27.

### 3. Khái niệm cốt lõi

Image là khuôn bất biến; container là tiến trình chạy; volume giữ dữ liệu qua lần khởi động. Multi-stage build tách compiler khỏi runtime. Readiness kiểm khả năng phục vụ; SIGTERM cần đủ thời gian để đóng server/pool.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [14-projects/p2-backend-service/app/src/main/scala/vn/scalaeng/p2/app/Main.scala:30](../14-projects/p2-backend-service/app/src/main/scala/vn/scalaeng/p2/app/Main.scala#L30). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 14-projects/p2-backend-service/app/src/main/scala/vn/scalaeng/p2/app/Main.scala:30-39 -->
```scala
object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] = Config.load(sys.env) match
    case Left(es) => IO.println(es.mkString("cấu hình không hợp lệ:\n- ","\n- ","")).as(ExitCode(2))
    case Right(c) =>
      val server = for
        _ <- Resource.eval(Database.migrate(c.db))
        xa <- Database.transactor(c.db)
        store = new PgStore
        tx = Database.runner(xa)
        tokens = new Tokens(c.secret,Clock.systemUTC())
```

### 5. Lựa chọn thay thế

PostgreSQL nhúng zonky thích hợp test khi được phép tạo subprocess; PostgreSQL cài trực tiếp thuận tiện máy dev. Docker phù hợp môi trường nhiều service, đổi lại cần daemon và quản lý volume/cổng.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “Pod bị **OOMKilled** nhưng không có `OutOfMemoryError`” — [04-professional-engineering/ch11-jvm-fundamentals.md:327](../04-professional-engineering/ch11-jvm-fundamentals.md#L327). Giới hạn container tính cả bộ nhớ ngoài heap; lỗi thiết kế tài nguyên này áp dụng cả khi chạy JVM trong Docker.

### 7. Dùng ở đâu trong giáo trình

[06-database/ch15-database-engineering.md](../06-database/ch15-database-engineering.md); [14-projects/p2-backend-service/README.md](../14-projects/p2-backend-service/README.md) (mục 5.4 và 6 có bằng chứng Docker thật).

### 8. Tài liệu chính thức

[Container concepts](https://docs.docker.com/get-started/docker-concepts/the-basics/what-is-a-container/), [Compose](https://docs.docker.com/compose/).

<a id="wsl"></a>
## WSL2

### 1. Là gì / giải quyết vấn đề gì

WSL2 cung cấp môi trường Linux trên Windows để chạy công cụ cần hành vi hệ điều hành Linux. Giáo trình dùng nó cho spark-submit thật khi đường triển khai Windows cần winutils mà cấu hình filesystem thuần Java không phủ hết.

### 2. Phiên bản trong giáo trình

**WSL 2.6.1.0**, kernel **6.6.87.2-1** từ `wsl --version` ngày 18/10/2026; môi trường chạy Ch26 là **Ubuntu 24.04 + Temurin 21.0.12.1** theo chương. [WSL releases](https://github.com/microsoft/WSL/releases). T27 chưa chạy lại spark-submit trong WSL.

### 3. Khái niệm cốt lõi

Filesystem, đường dẫn, biến môi trường và tiến trình ở Linux khác PowerShell/Windows. `SPARK_HOME`, JDK và jar phải tồn tại trong môi trường gọi submit. WSL2 không sửa tự động mọi khác biệt I/O giữa Windows và cluster.

### 4. Ví dụ từ lab đã kiểm thử

Nguồn: [09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala:65](../09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala#L65). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 09-spark/spark-production-lab/src/main/scala/vn/scalaeng/sparkprod/Job.scala:65-74 -->
```scala
object Job {

  def isTransient(e: Throwable): Boolean = Failures.classify(e) == Failures.Transient

  final case class BatchRate(ingestDate: String, rows: Long, rejected: Long) {
    def rate: Double = if (rows == 0) 0.0 else rejected.toDouble / rows
  }

  /** Gate chỉ xét LÔ CỦA NGÀY CHẠY: lô cũ từng tệ không được chặn mọi lần chạy sau. Không có lô cho ngày chạy = dữ liệu chưa đến — báo lỗi,
    * không "thành công rỗng".
```

### 5. Lựa chọn thay thế

Linux máy thật/VM/container phù hợp CI và production; WSL2 thuận tiện phát triển Windows. Lab local có thể dùng BareLocalFileSystem, nhưng đó không chứng minh spark-submit Windows hoạt động.

### 6. Lỗi/bẫy thường gặp

Trích nguyên văn mục 10: “ERROR SparkContext: Error initializing SparkContext.” — [09-spark/ch26-spark-production-engineering.md:416](../09-spark/ch26-spark-production-engineering.md#L416). Stack trace mục 10 đi qua FileUtil.chmod; dùng môi trường Linux đã kiểm chứng và đọc nguyên nhân gốc.

### 7. Dùng ở đâu trong giáo trình

[09-spark/ch26-spark-production-engineering.md](../09-spark/ch26-spark-production-engineering.md); [14-projects/p3-batch-pipeline/README.md](../14-projects/p3-batch-pipeline/README.md).

### 8. Tài liệu chính thức

[WSL documentation](https://learn.microsoft.com/en-us/windows/wsl/), [Spark submitting](https://spark.apache.org/docs/4.2.0/submitting-applications.html).

## Quy trình kiểm chứng dùng trong repo

Ở PowerShell, từ repo root, đặt `$env:COURSIER_CACHE = "$PWD/.cache/coursier"`, rồi chạy `scala-cli.bat test <lab> --server=false`. Đây là hướng dẫn lệnh, không phải transcript T27 đã chạy test. `.cache/` đã được ignore. Không tự bật test PostgreSQL/Kafka khi thiếu quyền hoặc môi trường; đọc biến bật integration test trong README từng lab. Không tính assume/skip vào số pass.

Plugin sbt, assembly, cài tool, daemon Docker và spark-submit là những lớp kiểm chứng riêng. Khi bàn giao, ghi rõ lớp nào đã chạy và liên kết bằng chứng; không suy từ test Scala CLI sang toàn bộ các task đó.
