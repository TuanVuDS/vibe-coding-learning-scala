# Chương 9 — Project Structure: từ một file đến project nhiều module

> **Phiên bản:** Scala 3.9.0 · JDK 21 · sbt 1.12.11 · munit 1.2.4 · scalafmt 3.10.7 · sbt-scalafmt 2.5.5 (cần kiểm tra lại)
> **Code:** [`order-platform/`](order-platform/) — project 4 module: `testkit`, `domain`, `core`, `app`
> **Trạng thái kiểm chứng:**
> - Mã nguồn Scala của **mọi module**: **đã biên dịch** bằng `scalac 3.9.0` theo đúng thứ tự phụ thuộc, với đúng tham số CI (`-Werror -Wunused:all -Wvalue-discard`), **không cảnh báo**.
> - Ứng dụng **đã chạy thật** với 4 kịch bản cấu hình; output và exit code trong bài là thật. 25/25 test pass bằng `sbt test` (munit 1.2.4 thật, chạy lại 16/10/2026 trên Windows 11 + JDK 21; lần chạy thật đầu tiên phát hiện và sửa lỗi `\r\n` của `JsonLogger` trên Windows).
> - `build.sbt`, `project/plugins.sbt`, `.scalafmt.conf`, `.github/workflows/ci.yml`: **chưa chạy bằng sbt/GitHub Actions** (môi trường biên soạn không truy cập được Maven Central). Bạn là người chạy xác nhận đầu tiên — xem mục 11.

---

## 1. Mục tiêu học tập

- Tổ chức code bằng package và import đúng quy ước.
- Thiết kế project sbt nhiều module với hướng phụ thuộc rõ ràng (domain ← core ← app).
- Quản lý dependency: phiên bản, scope `Test`, dependency nội bộ giữa module, rủi ro bảo mật.
- Cấu hình ứng dụng theo môi trường (dev/staging/prod) theo nguyên tắc 12-factor, có validate.
- Logging có cấu trúc (structured logging) và tách logging khỏi logic bằng "port".
- Xử lý lỗi ở biên ứng dụng bằng exit code — hợp đồng với scheduler/orchestrator.
- Thiết lập code convention (scalafmt, cảnh báo compiler, `-Werror` trong CI), Git workflow và code review.

## 2. Kiến thức tiên quyết

- Chương 2 (sbt cơ bản), Chương 7 (Functional Core / Imperative Shell, `Either`), Chương 8A (opaque type).

## 3. Lý thuyết

### 3.1. Package và import

```scala
package vn.scalaeng.order.core           // tên miền đảo ngược + dự án + module

import vn.scalaeng.order.domain.*         // mọi thứ trong package (KHÔNG gồm given — Chương 8B)
import java.nio.file.{Files, Path}        // chọn lọc
import scala.jdk.CollectionConverters.*   // chuyển đổi collection Java <-> Scala
import Quantity.value                     // import thành viên của object (ở đây là extension method)
```

Quy ước:
- Tên package chữ thường, khớp với cấu trúc thư mục (`src/main/scala/vn/scalaeng/order/core/`). Scala không bắt buộc khớp nhưng mọi công cụ và đồng nghiệp đều mong đợi điều đó.
- Mỗi module một gốc package riêng (`...order.domain`, `...order.core`, `...order.app`) → nhìn import là biết phụ thuộc.
- Tránh wildcard import từ package lớn của bên thứ ba trong code nghiệp vụ; với package của chính module mình thì chấp nhận được.

**Bài học thật khi biên dịch project này:** trong `Money.scala`, phép `Vnd * Quantity` gọi `q.value` — extension method định nghĩa trong `object Quantity`. Compiler báo:

```
value value is not a member of vn.scalaeng.order.domain.Quantity, but could be made available as an extension method.
The following import might fix the problem:
  import vn.scalaeng.order.domain.Quantity.value
```

Extension method không tự có mặt ở mọi nơi; compiler gợi ý chính xác import cần thêm.

### 3.2. Tổ chức module

- **Vấn đề:** một module lớn chứa cả domain, logic, đọc file, log, cấu hình → logic nghiệp vụ vô tình phụ thuộc vào I/O; test chậm; không tái sử dụng được logic ở nơi khác (ví dụ Kafka consumer ở Chương 28).
- **Giải pháp:** tách module theo **hướng phụ thuộc một chiều**:

```
            ┌────────────┐
            │  testkit   │  (chỉ dùng ở scope Test)
            └─────┬──────┘
                  │ % Test
┌────────┐   ┌────▼───┐   ┌───────┐
│ domain │◄──│  core  │◄──│  app  │
└────────┘   └────────┘   └───────┘
 kiểu,         logic        cấu hình, file,
 validate      thuần        log, exit code
 (không I/O)   (không I/O)  (side effect)
```

| Module | Chứa | Không được chứa |
|---|---|---|
| `domain` | Opaque type, case class, quy tắc hợp lệ của từng giá trị | I/O, thư viện ngoài |
| `core` | Pipeline parse/dedup/aggregate, định nghĩa *port* (`trait Logger`) | Đọc file, `System.err`, `sys.env` |
| `app` | Đọc cấu hình, file, cài đặt port, `main`, exit code | Logic nghiệp vụ |
| `testkit` | Công cụ test dùng chung (Chương 10) | Code production |

Compiler **cưỡng chế** ranh giới: `core` không thể `import ...app.*` vì không phụ thuộc `app`. Kiến trúc được kiểm tra mỗi lần biên dịch, không chỉ nằm trên giấy.

**Khi nào KHÔNG nên tách module:** project nhỏ, một người làm, chưa có ranh giới rõ. Tách quá sớm làm build phức tạp hơn lợi ích. Bắt đầu với package rõ ràng; tách module khi có lý do (tái sử dụng, ranh giới phụ thuộc, thời gian build).

### 3.3. Multi-module với sbt

```scala
lazy val domain = project.in(file("modules/domain")).settings(commonSettings, name := "order-domain")
lazy val core   = project.in(file("modules/core")).dependsOn(domain, testkit % Test).settings(...)
lazy val app    = project.in(file("modules/app")).dependsOn(core, testkit % Test).settings(...)
lazy val root   = project.in(file(".")).aggregate(testkit, domain, core, app)
```

| Khái niệm | Ý nghĩa |
|---|---|
| `dependsOn(x)` | Classpath của module này có output của `x` (phụ thuộc biên dịch) |
| `dependsOn(x % Test)` | Chỉ test của module này thấy `x` |
| `aggregate(...)` | Lệnh chạy ở root được chạy cho mọi module (không tạo phụ thuộc) |
| `commonSettings` | `Seq[Setting[_]]` dùng chung — tránh lặp cấu hình |
| `ThisBuild / key` | Áp dụng cho toàn build |

Trong sbt shell: `core/test` (test một module), `app/run`, `projects` (liệt kê), `project app` (chuyển module).

**Cạm bẫy: `build.sbt` không phải Scala 3.** sbt 1.x biên dịch file build bằng **Scala 2.12**. Vì vậy trong `build.sbt` phải viết `if (isCI) Seq("-Werror") else Seq.empty`,
không được viết `if isCI then ...`. sbt 2.x dùng Scala 3 cho build definition (Chương 2) — một lý do nữa để ghi rõ phiên bản sbt trong `project/build.properties`.

**Phụ thuộc vòng không được phép:** nếu `testkit` phụ thuộc `core` và `core` lại phụ thuộc `testkit % Test`, sbt báo lỗi vòng phụ thuộc. Đó là lý do `testkit` chỉ chứa tiện ích **chung** (Gen, Prop), còn fake riêng của `app` nằm trong test của `app`.

### 3.4. Dependency management

```scala
libraryDependencies += "org.scalameta" %% "munit" % "1.2.4" % Test
```

Nguyên tắc:
1. **Ghi phiên bản cụ thể.** Không dùng `latest.integration` hay khoảng phiên bản — build phải tái lập được.
2. **Đúng scope.** Thư viện test ở `% Test`; driver JDBC có thể là `% Runtime`.
3. **Ít dependency nhất có thể.** Mỗi thư viện là: mã bạn không kiểm soát, xung đột phiên bản tiềm ẩn, bề mặt tấn công bảo mật.
   `domain` và `core` của project này **không có dependency ngoài nào** — có chủ đích.
4. **Kiểm tra cây phụ thuộc:** `sbt "core/dependencyTree"` (có sẵn trong sbt 1.4+ qua lệnh `dependencyTree`), `evicted` để xem phiên bản nào bị thay thế khi xung đột.
5. **Theo dõi lỗ hổng và cập nhật có kiểm soát:** dùng công cụ cập nhật tự động (Scala Steward, Dependabot) — mỗi cập nhật là một PR chạy qua CI.

**Ví dụ thật về rủi ro bảo mật (tra cứu ngày 14/10/2026):** Logback 1.5.36 phát hành 25/06/2026 có bản sửa cho CVE-2026-13006, nhưng ghi chú phát hành
nói rằng bản sửa **đầy đủ** nằm ở 1.5.37. Tháng 02/2026, logback 1.5.30 bị thiếu thư mục `META-INF/services`, khiến nó **không dùng được với SLF4J**, và được sửa ngay ở 1.5.31.
Bài học: (a) cập nhật phiên bản là việc thường xuyên, (b) không nhảy lên bản mới nhất ngay trong ngày phát hành mà không chạy test, (c) đọc release notes.

### 3.5. Build profiles và cấu hình theo môi trường

Hai loại "cấu hình" rất khác nhau:

| | Cấu hình **build** | Cấu hình **runtime** |
|---|---|---|
| Ví dụ | `-Werror` trong CI, phiên bản Scala, plugin | Đường dẫn input, ngưỡng chất lượng, log level, URL database |
| Nằm ở | `build.sbt` (đọc biến `CI`) | Biến môi trường khi chạy |
| Thay đổi khi | Đổi cách build | Đổi môi trường triển khai — **cùng một artifact** |

Nguyên tắc **12-factor**: cùng một file jar chạy ở dev, staging, prod; khác nhau **chỉ** ở biến môi trường. Không build riêng cho từng môi trường.

`AppConfig.load(vars: Map[String, String])`:
- Là hàm **thuần**: nhận `Map`, không tự đọc `sys.env` → test mọi tổ hợp cấu hình không cần đổi biến môi trường của máy.
- **Tích lũy mọi lỗi** (Chương 7): người vận hành sửa tất cả trong một lần deploy.
- **Fail fast khi khởi động:** cấu hình sai → dừng ngay với exit code 1, không chạy nửa chừng rồi hỏng.
- **Mặc định khác nhau theo môi trường:** dev có `INPUT_PATH` mặc định cho tiện; prod **bắt buộc** khai báo — đoán sai đường dẫn dữ liệu ở prod nguy hiểm hơn là dừng.
- **Secrets** (mật khẩu DB, API key) không bao giờ có giá trị mặc định, không log ra, không commit vào Git — Chương 17.

Thư viện cấu hình phổ biến (dùng từ Chương 14): `pureconfig`, `ciris`, Typesafe Config (HOCON). Chương này tự viết để hiểu bản chất; các thư viện làm cùng việc với cú pháp gọn hơn.

### 3.6. Logging

**Structured logging:** mỗi log là dữ liệu có field, không phải câu văn tự do.

```
{"ts":"2026-10-15T02:12:50.592825676Z","level":"INFO","msg":"finished","input_rows":"7","valid":"4","rejected":"2","duplicates":"1","balanced":"true"}
```

Hệ thống thu thập log (Loki, Elasticsearch, CloudWatch) lọc được `level=WARN AND rejected>0` — không thể làm vậy với `"Xong! Có 2 dòng lỗi"`.

Quy tắc:
- **Log ra stderr** (hoặc qua framework), để stdout dành cho dữ liệu nếu chương trình là một bước trong pipeline Unix.
- **Không log dữ liệu nhạy cảm**: mật khẩu, token, số thẻ, thông tin cá nhân. Log `order_id`, không log địa chỉ khách hàng.
- **Level có ý nghĩa:** `ERROR` cần người xử lý; `WARN` bất thường nhưng chạy tiếp; `INFO` mốc quan trọng (start/finish + số liệu); `DEBUG` chi tiết khi điều tra. Không log `INFO` trong vòng lặp từng bản ghi.
- **Mỗi log hữu ích có ngữ cảnh**: id của job/request, số liệu, đường dẫn.

**Logging như một port:** `core` định nghĩa `trait Logger`; `app` cung cấp `JsonLogger`. Logic không phụ thuộc thư viện log nào.

**Trong production** chuẩn JVM là **SLF4J** (API) + **Logback** (implementation). Phiên bản tra cứu ngày 14/10/2026: Logback **1.6.0** (23/07/2026), dùng SLF4J **2.0.18**.
Cấu hình tối thiểu (trạng thái: **chưa kiểm chứng** trong môi trường biên soạn):

```scala
// build.sbt — module app
libraryDependencies ++= Seq(
  "org.slf4j"      % "slf4j-api"       % "2.0.18",
  "ch.qos.logback" % "logback-classic" % "1.6.0" % Runtime
)
```

```xml
<!-- modules/app/src/main/resources/logback.xml -->
<configuration>
  <appender name="STDERR" class="ch.qos.logback.core.ConsoleAppender">
    <target>System.err</target>
    <encoder>
      <pattern>%d{ISO8601} %-5level [%thread] %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="${LOG_LEVEL:-INFO}">
    <appender-ref ref="STDERR"/>
  </root>
</configuration>
```

Một `Slf4jLogger extends Logger` trong `app` sẽ thay `JsonLogger` mà **không sửa dòng nào** của `core`. Với ứng dụng dùng Cats Effect (Chương 16B) ta sẽ dùng `log4cats` trên SLF4J.

### 3.7. Error handling ở biên ứng dụng

Ba tầng xử lý lỗi trong project:

| Tầng | Loại lỗi | Cách xử lý |
|---|---|---|
| `domain`/`core` | Dữ liệu sai từng dòng | `Either` → `RowError`, tiếp tục xử lý (Chương 7) |
| `core` | Cả file sai (thiếu header) | `Either[String, PipelineResult]` |
| `app` | Cấu hình, I/O, ngưỡng chất lượng | Log có cấu trúc + **exit code** |

```scala
object ExitCode:
  val Ok = 0
  val ConfigError = 1
  val DataQualityFailed = 2
  val IoError = 3
```

Exit code là **hợp đồng** với thứ chạy chương trình: cron, Kubernetes Job, Dagster (Chương 31). Exit code khác 0 → orchestrator đánh dấu thất bại, gửi cảnh báo, chặn bước tiếp theo.
Phân biệt các mã giúp tự động hóa quyết định: lỗi cấu hình → không retry; lỗi I/O tạm thời → có thể retry; vượt ngưỡng chất lượng → cần người xem dữ liệu.

`Main.run(...)` trả về `Int`, chỉ `main` gọi `sys.exit` → toàn bộ shell test được (Chương 10).

### 3.8. Code conventions

- **scalafmt** định dạng tự động (`.scalafmt.conf` khóa phiên bản `3.10.7`, dialect `scala3`). Không tranh luận về khoảng trắng trong code review — máy quyết định.
- **Cảnh báo compiler bật đầy đủ:** `-deprecation -feature -Wunused:all -Wvalue-discard`. Trong CI thêm `-Werror`: cảnh báo không được phép tích tụ.
  `-Wvalue-discard` bắt lỗi kiểu "gọi hàm trả `Either` mà bỏ qua kết quả" — một dạng nuốt lỗi.
- **Đặt tên:** `PascalCase` cho kiểu và object, `camelCase` cho method/val, hằng số trong object có thể `PascalCase` (`ExpectedHeader`).
- **Kích thước:** file vài trăm dòng, method vài chục dòng là tín hiệu nên tách.
- **Không `null`, không `.get` trên Option/Either trong code production, không `var` chia sẻ** (Chương 3, 7). Test được phép dùng `.toOption.get` cho ngắn gọn.

### 3.9. Git workflow và code review

**Workflow khuyến nghị cho team nhỏ — trunk-based với nhánh ngắn hạn:**
1. Tạo nhánh từ `main`: `feat/order-dedup-latest`, `fix/csv-bom`.
2. Commit nhỏ, thông điệp rõ (ví dụ theo Conventional Commits: `fix(core): chấp nhận header có BOM UTF-8`).
3. Mở Pull Request sớm; CI chạy `scalafmtCheckAll scalafmtSbtCheck test` với `CI=true`.
4. Ít nhất một người review; merge khi CI xanh. Nhánh sống tối đa vài ngày.

**`.gitignore`** loại bỏ `target/`, `.bsp/`, `.metals/`, `.idea/`, output (`out/`) — không commit artifact build.

**Checklist code review cho project Scala BE/DE:**
- [ ] Logic nghiệp vụ có nằm trong module thuần (`domain`/`core`) không? Có I/O lọt vào core không?
- [ ] Lỗi được biểu diễn bằng kiểu (`Either`, ADT) hay exception/`null`?
- [ ] Có test cho trường hợp biên và cho bug vừa sửa (regression test)?
- [ ] Dependency mới có thật cần thiết? Phiên bản cụ thể? Scope đúng?
- [ ] Log có đủ ngữ cảnh, không lộ dữ liệu nhạy cảm, đúng level?
- [ ] Cấu hình mới có validate và có trong tài liệu triển khai?
- [ ] Với pipeline dữ liệu: kiểm tra cân bằng số bản ghi còn đúng? Chạy lại có cho cùng kết quả?

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Giải thích |
|---|---|
| Multi-module build | Build gồm nhiều module có phụ thuộc lẫn nhau |
| Aggregate / dependsOn | Gộp lệnh / phụ thuộc biên dịch giữa module |
| Dependency scope | Phạm vi dùng dependency (Compile, Test, Runtime) |
| Transitive dependency | Phụ thuộc kéo theo |
| Eviction | Thay một phiên bản dependency bằng phiên bản khác khi xung đột |
| 12-factor app | Bộ nguyên tắc ứng dụng cloud: cấu hình qua môi trường... |
| Structured logging | Log dạng dữ liệu có field |
| Port / adapter | Interface do core định nghĩa / cài đặt ở biên |
| Exit code | Mã kết thúc tiến trình |
| Fail fast | Dừng ngay khi phát hiện sai |
| Trunk-based development | Phát triển trên nhánh chính với nhánh phụ ngắn hạn |
| Pull request / code review | Yêu cầu hợp nhất / xem xét mã |
| CVE | Mã định danh lỗ hổng bảo mật công khai |

## 5. Ví dụ đơn giản

Từ một file sang hai module:

```scala
// modules/domain/src/main/scala/shop/domain/Price.scala
package shop.domain
opaque type Price = Long
object Price:
  def parse(s: String): Either[String, Price] = s.toLongOption.filter(_ > 0).toRight(s"giá sai: $s")
  extension (p: Price) def value: Long = p

// modules/app/src/main/scala/shop/app/Main.scala
package shop.app
import shop.domain.Price
@main def run(raw: String): Unit = println(Price.parse(raw).map(_.value))
```

```scala
// build.sbt
lazy val domain = project.in(file("modules/domain"))
lazy val app    = project.in(file("modules/app")).dependsOn(domain)
```

## 6. Ví dụ thực tế — `order-platform`

Batch job xử lý file đơn hàng hằng ngày, được chạy bởi scheduler:

```
order-platform/
├── build.sbt                     # 4 module, commonSettings, -Werror khi CI=true
├── project/build.properties      # sbt.version=1.12.11
├── project/plugins.sbt           # sbt-scalafmt
├── .scalafmt.conf                # version = 3.10.7, dialect scala3
├── .github/workflows/ci.yml      # format check + test
├── sample-data/orders.csv        # dữ liệu mẫu cho dev
└── modules/
    ├── testkit/                  # Gen, Prop (Chương 10)
    ├── domain/                   # OrderId, Sku, Vnd, Quantity, OrderLine, RowError
    ├── core/                     # CsvLine, OrderPipeline, Reports, trait Logger
    └── app/                      # AppConfig, JsonLogger, Main, ExitCode
```

## 7. Code đầy đủ

Các file chính:

- [`build.sbt`](order-platform/build.sbt)
- [`modules/domain/.../Ids.scala`](order-platform/modules/domain/src/main/scala/vn/scalaeng/order/domain/Ids.scala), [`Money.scala`](order-platform/modules/domain/src/main/scala/vn/scalaeng/order/domain/Money.scala), [`Order.scala`](order-platform/modules/domain/src/main/scala/vn/scalaeng/order/domain/Order.scala)
- [`modules/core/.../CsvLine.scala`](order-platform/modules/core/src/main/scala/vn/scalaeng/order/core/CsvLine.scala), [`OrderPipeline.scala`](order-platform/modules/core/src/main/scala/vn/scalaeng/order/core/OrderPipeline.scala), [`Ports.scala`](order-platform/modules/core/src/main/scala/vn/scalaeng/order/core/Ports.scala)
- [`modules/app/.../AppConfig.scala`](order-platform/modules/app/src/main/scala/vn/scalaeng/order/app/AppConfig.scala), [`JsonLogger.scala`](order-platform/modules/app/src/main/scala/vn/scalaeng/order/app/JsonLogger.scala), [`Main.scala`](order-platform/modules/app/src/main/scala/vn/scalaeng/order/app/Main.scala)

Trích đoạn cấu hình:

```scala
def load(vars: Map[String, String]): Either[List[String], AppConfig] =
  val envE: Either[String, Env] =
    vars.getOrElse("APP_ENV", "dev").trim.toLowerCase match
      case "dev"     => Right(Env.Dev)
      case "staging" => Right(Env.Staging)
      case "prod"    => Right(Env.Prod)
      case other     => Left(s"APP_ENV không hợp lệ: '$other' (dev|staging|prod)")

  val inputE: Either[String, Path] = (vars.get("INPUT_PATH"), envE) match
    case (Some(p), _) if p.trim.nonEmpty => Right(Paths.get(p.trim))
    case (_, Right(Env.Dev))             => Right(Paths.get("sample-data/orders.csv"))
    case _                               => Left("thiếu INPUT_PATH (bắt buộc ngoài môi trường dev)")
  ...
  (envE, inputE, outputE, ratioE, levelE) match
    case (Right(e), Right(i), Right(o), Right(r), Right(l)) => Right(AppConfig(e, i, o, r, l))
    case _ => Left(List(envE, inputE, outputE, ratioE, levelE).collect { case Left(err) => err })
```

## 8. Giải thích code

- **`CsvLine.split`** viết theo phong cách imperative (`var`, `while`, `StringBuilder`) — chấp nhận được vì trạng thái mutable **không thoát khỏi hàm** (Chương 6),
  và đây là vòng lặp nóng chạy cho mọi dòng dữ liệu. Hàm vẫn thuần nhìn từ bên ngoài. Nó thay cho `split(",")` của Chương 7 vốn không xử lý được `"GIAY,DEN"`.
- **`Vnd.+` dùng `Math.addExact`**, `*` dùng `Math.multiplyExact` → tràn số ném `ArithmeticException` thay vì ra số âm (có test).
- **`PipelineResult.isBalanced`** — kiểm tra `input = valid + rejected + duplicates` (Chương 7) trở thành một field trong log, giám sát được ở production.
- **`OrderPipeline.run` nhận cả header** và kiểm tra header trước — file sai định dạng hoàn toàn bị từ chối sớm, không sinh hàng nghìn `RowError` vô nghĩa.
- **Số dòng (`lineNo`) tính theo file gốc** (header là dòng 1, bỏ qua dòng trống nhưng vẫn đếm) → người vận hành mở file và tìm đúng dòng.
- **`JsonLogger` nhận `out: PrintStream` và `now: () => Instant`** — hai "seam" để test (Chương 10). Escape JSON thủ công cho ký tự `"`, `\`, xuống dòng, ký tự điều khiển.
- **`run / fork := true`** trong `build.sbt`: chạy ứng dụng trong JVM riêng để `sys.exit`, biến môi trường và `javaOptions` có tác dụng đúng như khi chạy thật (không giết sbt shell).

## 9. Output mong đợi

Chạy từ thư mục `order-platform` (với sbt: `sbt app/run`; output dưới đây lấy từ lần chạy trực tiếp bằng `java` sau khi biên dịch các module):

**Kịch bản 1 — dev, dữ liệu mẫu có 2/7 dòng lỗi (vượt ngưỡng 5%):**
```
{"ts":"2026-10-15T02:12:50.411744059Z","level":"INFO","msg":"starting","env":"Dev","input":"sample-data/orders.csv"}
{"ts":"2026-10-15T02:12:50.592825676Z","level":"INFO","msg":"finished","input_rows":"7","valid":"4","rejected":"2","duplicates":"1","balanced":"true"}
{"ts":"2026-10-15T02:12:50.679295299Z","level":"WARN","msg":"reject ratio above threshold","ratio":"0.286","max":"0.05"}
exit=2
```
`out/daily_revenue.csv`:
```
date,orders,revenue_vnd
2026-10-01,2,495000
2026-10-02,1,350000
2026-10-03,1,450000
```
`out/rejected.csv`:
```
line_no,reason,raw
4,đơn giá không phải số nguyên: 'abc',"ORD-3,NON-03,abc,1,2026-10-02"
5,sku không hợp lệ: '',"ORD-4,,50000,1,2026-10-02"
6,trùng order_id ORD-1,"ORD-1,AO-01,120000,2,2026-10-01"
```
Dòng `ORD-9,"GIAY,DEN",...` được parse đúng nhờ `CsvLine` (doanh thu ngày 03/10).

**Kịch bản 2 — `MAX_REJECT_RATIO=0.5`:** cùng dữ liệu, `exit=0`.

**Kịch bản 3 — `APP_ENV=prod LOG_LEVEL=loud`, thiếu `INPUT_PATH`:** cả hai lỗi được báo cùng lúc, `exit=1`.
```
{"ts":"...","level":"ERROR","msg":"config error","detail":"thiếu INPUT_PATH (bắt buộc ngoài môi trường dev)"}
{"ts":"...","level":"ERROR","msg":"config error","detail":"LOG_LEVEL không hợp lệ: 'loud'"}
```

**Kịch bản 4 — `INPUT_PATH=/nope.csv`:** `{"level":"ERROR","msg":"input file not found","path":"/nope.csv"}`, `exit=3`.

## 10. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `build.sbt` báo lỗi cú pháp ở `if ... then` | Build definition sbt 1.x là Scala 2.12 | Dùng `if (...) ... else ...` |
| `value domain is not a member of vn.scalaeng.order` khi biên dịch `core` | Thiếu `dependsOn(domain)` (hoặc biên dịch sai thứ tự) | Khai báo phụ thuộc; sbt tự sắp thứ tự |
| `Cyclic reference` / sbt báo vòng phụ thuộc giữa project | A dependsOn B và B dependsOn A (kể cả qua `% Test`) | Tách phần dùng chung ra module thứ ba |
| Extension method "không tồn tại" giữa các file | Chưa import (lỗi thật ở mục 3.1) | Làm theo gợi ý `The following import might fix the problem` |
| `sbt app/run` không đọc được `sample-data/orders.csv` | Thư mục làm việc khi fork là thư mục của project con hoặc root tùy cấu hình | Dùng `INPUT_PATH` tuyệt đối, hoặc đặt `run / baseDirectory := (ThisBuild / baseDirectory).value` |
| Build CI đỏ vì cảnh báo mà máy local không thấy | Local không đặt `CI=true` nên không có `-Werror` | Chạy `CI=true sbt test` trước khi push |
| Log tiếng Việt thành `?` | stderr không UTF-8 | `-Dstderr.encoding=UTF-8` (JDK 19+), cấu hình console |
| Hai phiên bản của cùng thư viện, lỗi `NoSuchMethodError` lúc chạy | Xung đột transitive dependency | `evicted`, `dependencyTree`; ép phiên bản bằng `dependencyOverrides` có cân nhắc |

## 11. Cách debug

- **Kiểm tra build trước tiên** (lần đầu chạy trên máy bạn): `sbt projects` → phải thấy 5 project; `sbt compile` → không cảnh báo; `CI=true sbt test` → mọi test pass;
  `sbt scalafmtCheckAll` → nếu báo lỗi format, chạy `sbt scalafmtAll` rồi xem diff. Nếu sbt không tải được `sbt-scalafmt 2.5.5`, kiểm tra phiên bản trên trang scalafmt và cập nhật `project/plugins.sbt`.
- **Xem cấu hình hiệu lực:** `sbt "show core/scalacOptions"`, `sbt "show app/dependencyClasspath"`.
- **Debug cấu hình runtime:** gọi `AppConfig.load` trong `sbt app/console` với `Map` tự soạn — không cần đổi biến môi trường.
- **Debug exit code:** `echo $?` (Linux/macOS) hoặc `echo %ERRORLEVEL%` (Windows) ngay sau khi chạy.
- **Đọc log có cấu trúc:** `... 2> log.jsonl` rồi lọc bằng `jq 'select(.level=="WARN")' log.jsonl`.

## 12. Bài tập

1. **(Cơ bản)** Thêm biến `REPORT_PREFIX` (mặc định rỗng) vào `AppConfig`, dùng làm tiền tố tên file output. Viết test cho `load`.
2. **(Cơ bản)** Thêm field `job_id` (lấy từ biến `JOB_ID`, mặc định là `"local"`) vào **mọi** dòng log mà không sửa từng chỗ gọi `log.info`. Gợi ý: một `Logger` bọc `Logger` khác.
3. **(Trung bình)** Thêm module `report` giữa `core` và `app` chứa `Reports` (di chuyển khỏi `core`). Vẽ lại sơ đồ phụ thuộc. Giải thích vì sao `report` không được phụ thuộc `app`.
4. **(Trung bình)** Thêm cấu hình `FAIL_ON_DUPLICATES` (true/false, mặc định false). Khi true và có bản ghi trùng → exit code mới `4`. Cập nhật bảng exit code và test.
5. **(Nâng cao)** Viết `Slf4jLogger` (trong module `app`, phụ thuộc `slf4j-api 2.0.18` + `logback-classic 1.6.0`) thay `JsonLogger` khi `LOG_FORMAT=text`.
   Viết `logback.xml`. Chạy và ghi lại output (đây là bài tập cần máy có Internet; ghi rõ phiên bản bạn dùng).

## 13. Đáp án và lời giải

Xem [`solutions/ch09-ch11-solutions.md`](../solutions/ch09-ch11-solutions.md).

## 14. Câu hỏi kiểm tra

1. Vì sao `core` không được đọc `sys.env` trực tiếp?
2. `aggregate` khác `dependsOn` thế nào?
3. Vì sao `build.sbt` không dùng được cú pháp `if ... then` của Scala 3 (với sbt 1.x)?
4. Cấu hình build và cấu hình runtime khác nhau thế nào? Cho ví dụ mỗi loại.
5. Vì sao prod không nên có giá trị mặc định cho `INPUT_PATH`?
6. Vì sao structured logging tốt hơn log dạng câu văn?
7. Exit code `2` (data quality) và `3` (I/O) nên được orchestrator xử lý khác nhau thế nào?
8. Kể 3 rủi ro của việc thêm một dependency.

## 15. Mini project

Biến `order-platform` thành repository Git thật:
1. `git init`, commit đầu tiên chỉ có cấu trúc build và `.gitignore`.
2. Mỗi module một commit, thông điệp theo Conventional Commits.
3. Tạo nhánh `fix/csv-bom`, thêm regression test cho BOM (Chương 10 có sẵn), mở PR (trên GitHub/GitLab cá nhân) để CI chạy.
4. Viết `README.md` cho project: cách build, cách chạy, bảng biến môi trường, bảng exit code.

## 16. Nguồn tài liệu chính thức

- sbt — Multi-project builds: https://www.scala-sbt.org/1.x/docs/Multi-Project.html
- sbt — Library dependencies: https://www.scala-sbt.org/1.x/docs/Library-Dependencies.html
- sbt — Scopes: https://www.scala-sbt.org/1.x/docs/Scopes.html
- Scala 3 Book — Packaging and Imports: https://docs.scala-lang.org/scala3/book/packaging-imports.html
- scalafmt — Installation: https://scalameta.org/scalafmt/docs/installation.html
- The Twelve-Factor App — Config: https://12factor.net/config
- SLF4J: https://www.slf4j.org/manual.html · Logback: https://logback.qos.ch/manual/
- Scala compiler options: https://docs.scala-lang.org/scala3/guides/migration/options-new.html

## 17. Checklist tự đánh giá

- [ ] Tôi giải thích được vì sao mỗi module có/không có dependency nào.
- [ ] Tôi viết được `build.sbt` multi-module với `commonSettings`, `dependsOn`, `aggregate`, scope `Test`.
- [ ] Tôi nhớ `build.sbt` (sbt 1.x) dùng cú pháp Scala 2.
- [ ] Cấu hình của tôi đọc từ biến môi trường, validate, tích lũy lỗi, fail fast.
- [ ] Log của tôi có cấu trúc, đúng level, không lộ dữ liệu nhạy cảm.
- [ ] Ứng dụng của tôi trả exit code có ý nghĩa.
- [ ] Project của tôi có scalafmt, cảnh báo compiler đầy đủ và `-Werror` trong CI.
