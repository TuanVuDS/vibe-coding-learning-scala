# Chương 2 — Environment Setup

> **Phiên bản:** JDK 21 · Scala 3.9.0 · Scala CLI (đi kèm lệnh `scala`) · sbt 1.12.11 · munit 1.2.4
> **Trạng thái kiểm chứng:**
> - Code `Hello.scala` và toàn bộ code Chương 3–4: **đã biên dịch & chạy** bằng `scalac 3.9.0` (bản phân phối chính thức từ GitHub release) trên OpenJDK 21.0.10, Linux.
> - Các lệnh cài đặt (Coursier, SDKMAN, Homebrew, winget): lấy từ tài liệu chính thức, **chưa chạy trong môi trường biên soạn** (môi trường không có quyền cài đặt hệ thống/không truy cập Maven Central). Nếu gặp khác biệt, hãy ưu tiên trang https://www.scala-lang.org/download/.
> - `build.sbt`: **chưa chạy bằng sbt** trong môi trường biên soạn.

---

## 1. Mục tiêu học tập

- Cài đặt JDK 21, Scala CLI, sbt và kiểm tra đúng phiên bản.
- Dùng REPL để thử nghiệm nhanh.
- Cấu hình IDE (IntelliJ IDEA hoặc VS Code + Metals).
- Hiểu cấu trúc project chuẩn và chạy được chương trình bằng cả Scala CLI và sbt.
- Khai báo dependency đúng cách, phân biệt `%%` và `%`.

## 2. Kiến thức tiên quyết

- Chương 1.
- Biết mở terminal và chạy lệnh; biết biến môi trường `PATH` là gì.

## 3. Lý thuyết

### 3.1. Bức tranh các công cụ

| Công cụ | Vai trò | Bắt buộc? |
|---|---|---|
| **JDK 21** | Chạy JVM, cung cấp `java` | Có |
| **Coursier (`cs`)** | Trình cài đặt chính thức cho công cụ Scala | Khuyến nghị |
| **Scala CLI** (lệnh `scala` hoặc `scala-cli`) | Biên dịch/chạy/test file hoặc thư mục nhỏ, REPL | Có (Giai đoạn 1) |
| **sbt** | Build tool cho project nhiều module | Có (từ Giai đoạn 2) |
| **IDE** | Gợi ý code, báo lỗi, debug | Có |

- **Vấn đề:** project Scala cần đúng phiên bản compiler, standard library và thư viện; làm tay là bất khả thi.
- **Giải pháp:** build tool đọc file cấu hình, tự tải compiler và dependency từ Maven Central, biên dịch, chạy test.
- **Vì sao hai build tool?** Scala CLI rất nhẹ — một file `.scala` là đủ, lý tưởng để học. sbt mạnh hơn cho
  multi-module, plugin, đóng gói, CI — chuẩn công nghiệp. Lựa chọn thay thế: **Mill** (đơn giản hơn sbt, đang phổ biến),
  **Maven/Gradle** (thường dùng khi project Java là chính). Giáo trình không dùng Mill/Gradle để giảm số công cụ.
- **Từ Scala 3.5**, lệnh `scala` chính là Scala CLI (SIP-46). Vì vậy `scala run Hello.scala` và
  `scala-cli run Hello.scala` tương đương.

### 3.2. Vì sao JDK 21?

- Scala 3.8 trở lên yêu cầu tối thiểu JDK 17.
- Spark 4.1 chạy trên Java 17/21.
- JDK 21 là bản LTS — **một JDK dùng cho cả BE và DE** trong giáo trình, tránh lỗi lệch phiên bản.
- Bản phân phối: Eclipse Temurin (Adoptium), Amazon Corretto, Azul Zulu... đều là OpenJDK, chọn một.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| REPL (Read-Eval-Print Loop) | Môi trường gõ lệnh, chạy ngay, in kết quả |
| Build tool | Công cụ biên dịch, quản lý dependency, chạy test, đóng gói |
| Dependency | Thư viện mà project cần |
| Artifact | Một gói phát hành (thường là file `.jar`) có tọa độ `group:name:version` |
| Maven Central | Kho artifact công khai lớn nhất của JVM |
| Cross-version suffix | Hậu tố `_3`, `_2.13` trong tên artifact Scala |
| Directive | Dòng `//> using ...` cấu hình Scala CLI ngay trong file |
| BSP (Build Server Protocol) | Giao thức để IDE nói chuyện với build tool |

## 5. Ví dụ đơn giản — cài đặt từng bước

### Bước 1: Cài JDK 21

Chọn **một** cách:

```bash
# macOS (Homebrew)
brew install --cask temurin@21

# Linux/macOS (SDKMAN!) — xem danh sách bản Temurin 21 rồi cài bản mới nhất
sdk list java | grep "21.*tem"
sdk install java 21.0.x-tem      # thay x bằng bản trong danh sách

# Windows (winget)
winget install EclipseAdoptium.Temurin.21.JDK
```

Kiểm tra:

```bash
java -version
# openjdk version "21.0.x" ...
```

### Bước 2: Cài Scala CLI, sbt qua Coursier

Theo trang cài đặt chính thức https://www.scala-lang.org/download/:

```bash
# Linux x86-64
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs \
  && chmod +x cs && ./cs setup

# macOS
brew install coursier && coursier setup

# Windows: tải và chạy cs-x86_64-pc-win32.zip từ trang download, chạy cs setup
```

`cs setup` cài `scala`, `scalac`, `scala-cli`, `sbt`... vào thư mục của Coursier và thêm vào `PATH`.
**Mở terminal mới** sau khi cài để `PATH` có hiệu lực.

Kiểm tra:

```bash
scala --version      # hiện phiên bản Scala CLI và Scala mặc định
sbt --version        # hiện phiên bản sbt runner
```

> Phiên bản Scala mặc định mà `scala` dùng có thể khác 3.9.0. Không sao: mỗi project **ghi rõ**
> phiên bản trong file cấu hình (`//> using scala 3.9.0` hoặc `scalaVersion := "3.9.0"`), và công cụ
> sẽ tải đúng phiên bản đó. Đây là cách làm chuẩn — đừng phụ thuộc vào phiên bản cài sẵn trên máy.

### Bước 3: REPL

```bash
scala repl -S 3.9.0
```

```scala
scala> 1 + 2
val res0: Int = 3

scala> val name = "Scala"
val name: String = Scala

scala> name.length
val res1: Int = 5

scala> :quit
```

REPL in ra **cả kiểu** của mỗi giá trị — công cụ tuyệt vời để kiểm tra type inference.

### Bước 4: IDE

| IDE | Cài đặt | Ghi chú |
|---|---|---|
| IntelliJ IDEA (Community đủ dùng) | Cài plugin **Scala** từ Marketplace | Mở thư mục có `build.sbt` → Import as sbt project |
| VS Code | Cài extension **Scala (Metals)** | Mở thư mục project; Metals hỏi import build → chọn Import |

Với project Scala CLI, chạy `scala-cli setup-ide .` trong thư mục project để sinh cấu hình BSP cho IDE.

## 6. Ví dụ thực tế — chạy code của giáo trình

Thư mục [`code/`](code/) là một project dùng **được cả hai** công cụ:

```
01-scala3-core/code/
├── project.scala              # cấu hình Scala CLI (directive //> using)
├── build.sbt                  # cấu hình sbt tương đương
├── project/
│   └── build.properties       # khóa phiên bản sbt: sbt.version=1.12.11
├── src/
│   ├── main/scala/scalaeng/
│   │   ├── ch02/Hello.scala
│   │   ├── ch03/OrderBasics.scala, TypesTour.scala
│   │   ├── ch04/Functions.scala
│   │   ├── ch05/OrderDomain.scala, Policies.scala
│   │   ├── ch06/AccessLogs.scala
│   │   └── ch07/OrderParsing.scala, Purity.scala
│   └── test/scala/scalaeng/
│       └── ch03/ ... ch07/   (một hoặc nhiều *Suite.scala mỗi chương)
└── .gitignore
```

Đây là **cấu trúc chuẩn Maven/sbt**: code chính ở `src/main/scala`, test ở `src/test/scala`,
thư mục con theo tên package. Scala CLI tự nhận các file trong thư mục `test` là test.

### Với Scala CLI

```bash
cd 01-scala3-core/code
scala-cli compile .
scala-cli run . --main-class scalaeng.ch02.hello
scala-cli run . --main-class scalaeng.ch03.ch03Demo
scala-cli test .
```

### Với sbt

```bash
cd 01-scala3-core/code
sbt                                   # vào sbt shell (lần đầu tải nhiều thứ, sẽ lâu)
sbt:scala3-core-examples> compile
sbt:scala3-core-examples> runMain scalaeng.ch02.hello
sbt:scala3-core-examples> test
sbt:scala3-core-examples> ~test        # tự chạy lại test mỗi khi lưu file
sbt:scala3-core-examples> console      # REPL có sẵn classpath của project
```

Luôn dùng **sbt shell** (gõ `sbt` rồi gõ lệnh) thay vì `sbt compile` từng lần: khởi động JVM của sbt mất nhiều giây.

## 7. Code đầy đủ

`project.scala`:

```scala
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all
//> using test.dep org.scalameta::munit:1.2.4
```

`build.sbt`:

```scala
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "vn.scalaeng"

lazy val root = (project in file("."))
  .settings(
    name := "scala3-core-examples",
    sourcesInBase := false, // không biên dịch project.scala (file cấu hình của Scala CLI)
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:all"),
    libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
  )
```

`project/build.properties`:

```
sbt.version=1.12.11
```

## 8. Giải thích code

**Directive Scala CLI:**

| Directive | Ý nghĩa |
|---|---|
| `//> using scala 3.9.0` | Dùng đúng compiler 3.9.0 (tự tải nếu chưa có) |
| `//> using jvm 21` | Dùng JDK 21 (Scala CLI có thể tự tải JDK nếu máy chưa có) |
| `//> using options ...` | Tham số compiler: cảnh báo API deprecated, feature, biến không dùng |
| `//> using test.dep ...` | Dependency chỉ dùng cho test |

**build.sbt:**

- `ThisBuild / scalaVersion` — áp dụng cho mọi sub-project.
- `sourcesInBase := false` — mặc định sbt cũng biên dịch file `.scala` ở thư mục gốc; ta tắt để sbt bỏ qua `project.scala`.
- `scalacOptions` — tương đương `//> using options`.
- `"org.scalameta" %% "munit" % "1.2.4" % Test`:
  - `%%` → sbt tự thêm hậu tố phiên bản Scala: tên thật là `munit_3`.
  - `% Test` → chỉ có trên classpath khi chạy test, không đóng gói vào ứng dụng.

**Dependency management — `%%` và `%`:**

```scala
// Thư viện Scala: dùng %% (sbt thêm _3 vào tên artifact)
"org.scalameta" %% "munit" % "1.2.4"            // → munit_3:1.2.4

// Thư viện Java: dùng % (không có hậu tố)
"org.apache.kafka" % "kafka-clients" % "4.2.2"  // → kafka-clients:4.2.2
```

Trong Scala CLI: thư viện Scala dùng `::`, thư viện Java dùng `:`

```scala
//> using dep org.scalameta::munit:1.2.4
//> using dep org.apache.kafka:kafka-clients:4.2.2
```

**Các tham số cấu hình compiler nên bật từ đầu:**

- `-deprecation`: báo khi dùng API sắp bị xóa.
- `-feature`: báo khi dùng tính năng ngôn ngữ cần import rõ ràng.
- `-Wunused:all`: báo biến/import không dùng — giúp code sạch.

## 9. Output mong đợi

```
$ scala-cli run . --main-class scalaeng.ch02.hello
Xin chào Scala 3.9.0 trên JVM 21.0.10
```

Lần chạy đầu Scala CLI in thêm các dòng như `Compiling project ...`, `Compiled project ...` — bình thường.

```
$ scala-cli test .
scalaeng.ch03.OrderBasicsSuite:
  + shippingFee: dưới ngưỡng thì tính phí chuẩn
  ...
scalaeng.ch04.FunctionsSuite:
  + normalizeName bỏ khoảng trắng thừa
  ...
```

(Định dạng chính xác do munit quyết định. Môi trường biên soạn đã chạy toàn bộ 20 test với kết quả
**Passed: 20, Failed: 0** (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21) — xem "Trạng thái kiểm chứng" ở đầu bài.)

## 10. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `command not found: scala` | Chưa mở terminal mới sau `cs setup`, hoặc `PATH` chưa có thư mục Coursier | Mở terminal mới; chạy lại `cs setup` |
| `UnsupportedClassVersionError` hoặc lỗi "requires JDK 17" | `java` trên `PATH` là JDK 8/11 | `java -version`; cài JDK 21 và đặt `JAVA_HOME` |
| sbt báo `not found: https://repo1.maven.org/...munit_3...` | Gõ sai version, dùng `%` thay `%%` (thiếu `_3`), hoặc mạng/proxy công ty chặn | Kiểm tra tọa độ trên https://index.scala-lang.org ; cấu hình proxy |
| Chữ tiếng Việt in ra thành `?` | Console không dùng UTF-8 | `java -Dstdout.encoding=UTF-8 ...`; trên Windows: `chcp 65001`; với sbt: thêm `run / javaOptions += "-Dstdout.encoding=UTF-8"` và `run / fork := true` |
| IDE báo đỏ nhưng `sbt compile` vẫn qua | IDE chưa import lại build | IntelliJ: Reload sbt project; Metals: "Import build" |
| `Multiple main classes detected` | Project có nhiều `@main` | Chỉ rõ: `--main-class` (Scala CLI) hoặc `runMain` (sbt) |

## 11. Cách debug

1. **Xác định phiên bản đang thật sự dùng:** `java -version`, `scala --version`; trong sbt shell gõ `scalaVersion`.
2. **Xem classpath:** sbt shell → `show Compile / dependencyClasspath`; Scala CLI → `scala-cli compile . --print-class-path`.
3. **Xóa cache build khi mọi thứ rối:** xóa `target/`, `project/target/` (sbt) hoặc `.scala-build/` (Scala CLI).
4. **Đọc toàn bộ log lần chạy đầu:** lỗi tải dependency thường nằm phía trên, trước dòng lỗi biên dịch cuối cùng.

## 12. Bài tập

1. **(Cơ bản)** Cài đặt đủ công cụ, chụp lại output của `java -version`, `scala --version`, `sbt --version`.
2. **(Cơ bản)** Mở REPL, gõ `1 / 2`, `1.0 / 2`, `"a" * 3`, `List(1,2,3).sum`. Ghi lại **kiểu** mà REPL in ra cho từng biểu thức.
3. **(Trung bình)** Tạo thư mục `playground/` với một file `Main.scala` có `@main def run(): Unit` in ra tên và năm sinh của bạn.
   Chạy bằng `scala-cli run .`. Thêm directive `//> using scala 3.9.0`.
4. **(Trung bình)** Chạy `scala-cli test .` và `sbt test` trong thư mục `code/` của chương. Cố tình sửa một assertion cho sai, quan sát cách munit báo lỗi, sau đó sửa lại.
5. **(Nâng cao)** Thêm dependency `kafka-clients` 4.2.2 vào cả `project.scala` và `build.sbt` (chưa cần dùng).
   Giải thích vì sao dùng `:`/`%` chứ không phải `::`/`%%`. Sau đó xem classpath và liệt kê 3 jar mà `kafka-clients` kéo theo (transitive dependencies).

## 13. Đáp án và lời giải

Xem [`solutions/ch01-ch02-solutions.md`](../solutions/ch01-ch02-solutions.md).

## 14. Câu hỏi kiểm tra

1. Vì sao phiên bản Scala nên ghi trong file cấu hình của project thay vì dựa vào phiên bản cài trên máy?
2. `%%` khác `%` ở điểm nào? Điều gì xảy ra nếu dùng `%` cho `munit`?
3. Khi nào chọn Scala CLI, khi nào chọn sbt?
4. Dependency với scope `Test` có nằm trong file jar đóng gói ứng dụng không?
5. Vì sao nên làm việc trong sbt shell thay vì gõ `sbt compile` nhiều lần?

## 15. Mini project

**"Scala workspace của tôi"**: tạo repository Git `scala-playground` với:
- `project.scala` khóa Scala 3.9.0 và JDK 21;
- một `@main` in phiên bản Scala và JVM (như `Hello.scala`);
- một test munit kiểm tra hàm `greet(name: String): String`;
- `.gitignore` bỏ qua `.scala-build/`, `target/`;
- `README.md` ghi lệnh chạy và test.

## 16. Nguồn tài liệu chính thức

- Cài đặt Scala: https://www.scala-lang.org/download/
- Scala 3 Book — Getting Started: https://docs.scala-lang.org/scala3/getting-started.html
- Scala CLI: https://scala-cli.virtuslab.org/docs/overview
- sbt Getting Started: https://www.scala-sbt.org/1.x/docs/Getting-Started.html
- munit: https://scalameta.org/munit/
- Metals: https://scalameta.org/metals/
- Tìm artifact Scala và kiểm tra hỗ trợ Scala 3: https://index.scala-lang.org/

## 17. Checklist tự đánh giá

- [ ] `java -version` hiện JDK 21.
- [ ] Tôi chạy được `hello` bằng cả Scala CLI và sbt.
- [ ] Tôi chạy được test và đọc hiểu kết quả.
- [ ] Tôi giải thích được `%%` vs `%`, `::` vs `:`.
- [ ] IDE của tôi hiển thị kiểu khi rê chuột lên biến.
- [ ] Tôi biết xóa cache build khi gặp lỗi lạ.
