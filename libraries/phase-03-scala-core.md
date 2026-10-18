# Thư viện — Phase 3: Scala Core và Professional Engineering

> **Phiên bản:** Scala 3.9.0 / JDK 21; nhánh Spark Scala 2.13.18. **Kiểm chứng T27 (18/10/2026):** quét build và kiểm HTTP POM; đối chiếu đoạn trích với nguồn. Code trích đã có bằng chứng chạy test trong repo, xem [audit T23a](../.team/reviews/T23a-audit.md) và bàn giao/review tương ứng. T27 không chạy lại toàn bộ lab, không chạy sbt. Tài liệu tra cứu theo brief T27, mỗi mục thư viện có 8 phần; không phải chương học 17 mục.

| Thư viện | Phiên bản | Scala 3 | Vai trò | Chương |
|---|---|---|---|---|
| Scala standard library | 3.9.0, do compiler cấp | Có | Kiểu, collection, I/O JVM | Ch1–11, Ch8C |
| munit | 1.2.4 (`munit_3`) | Có | Test hành vi | Ch3–10 |
| sbt-scalafmt | 2.5.5; engine 3.10.7 | Plugin sbt 1.x | Format code | Ch9 |

Quy tắc phân biệt `_3`, `_2.13` và thư viện Java: [compatibility matrix](../00-roadmap/04-compatibility-matrix.md). Công cụ test/build dùng chung: [tooling](tooling.md). Các module trong cùng họ được mô tả chung nhưng liệt kê riêng từng artifact trực tiếp; không biến toàn bộ cây bắc cầu thành dependency trực tiếp.

## Dependency trực tiếp của phase

Phase này dùng **munit 1.2.4** để kiểm thử: [mô tả đầy đủ và POM](tooling.md#munit); **sbt-scalafmt 2.5.5** ở order-platform: [plugin, engine và phạm vi kiểm chứng](tooling.md#scalafmt). Scala CLI/JDK/Coursier/sbt được mô tả ở [tooling](tooling.md). Không có Cats, http4s hay thư viện JSON ngoài trong build phase 3.

[Build Core](../01-scala3-core/code/build.sbt), [directive Core](../01-scala3-core/code/project.scala), [build order-platform](../04-professional-engineering/order-platform/build.sbt), [plugin order-platform](../04-professional-engineering/order-platform/project/plugins.sbt) và [directive Ch8C](../03-type-system/ch08c-lab/project.scala) là các điểm tra cứu chính. Các lời giải dùng cùng munit; không có dependency bổ sung riêng.

## Scala standard library: nền tảng có sẵn

**Là gì / vì sao cần:** standard library (thư viện chuẩn) đi cùng compiler, cung cấp `Option`, `Either`, collection và các phép biến đổi mà mọi bài nghiệp vụ sử dụng. Nó giúp mô hình hóa dữ liệu thiếu/lỗi và xây functional core mà chưa cần framework ngoài. Đây là thành phần nền, không phải dependency do tác giả thêm trong `using dep`.

**Phiên bản và artifact:** Scala **3.9.0**, JDK **21**. Xem [bản phát hành Scala 3.9](https://www.scala-lang.org/news/3.9/) và [API](https://www.scala-lang.org/api/3.9.0/). Từ Scala 3.8, standard library dùng phiên bản 3.x; không áp giả định scala-library 2.13 của các bản Scala 3 cũ cho mọi bản. [Compatibility matrix](../00-roadmap/04-compatibility-matrix.md) ghi kiểm chứng Spark/stdlib thực tế. T27 chỉ đối chiếu build và bằng chứng có sẵn, không biên dịch lại compiler hay xác nhận mọi API.

**Khái niệm cốt lõi:** collection bất biến giúp biến đổi dữ liệu theo bước; `Option` diễn tả có/không, `Either` giữ nguyên nhân lỗi, `Try` bao ngoại lệ đồng bộ. `Future` là tính toán bất đồng bộ đã được lên lịch khi tạo (khác `IO` ở phase 4). Type class/given và opaque type thuộc ngôn ngữ; `vn.scalaeng.order.testkit.Gen/Prop` là mã tự viết của lab, không phải ScalaCheck.

**Ví dụ đã kiểm thử:** test domain Ch9–10 chuyển chuỗi thành kiểu hợp lệ trước khi tính tiền; không có framework nghiệp vụ ngoài.

Nguồn: [04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:11](../04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala#L11). Đoạn trích nguyên văn, đọc cùng import và kiểu dữ liệu trong file gốc.

<!-- source: 04-professional-engineering/order-platform/modules/domain/src/test/scala/vn/scalaeng/order/domain/DomainSuite.scala:11-19 -->
```scala
  test("Vnd và Quantity"):
    val total = for
      p <- Vnd.parse("120000")
      q <- Quantity.parse("3")
    yield (p * q).dong
    assertEquals(total, Right(360000L))
    assert(Vnd.parse("0").isLeft)
    assert(Quantity.parse("-1").isLeft)
    assert(Quantity.parse("1.5").isLeft)
```

**Lựa chọn thay thế:** dùng standard library cho biến đổi thuần nhỏ để người đọc hiểu luồng dữ liệu. Cats bổ sung type class/validation tích lũy khi miền nghiệp vụ đòi hỏi; không cần thêm Cats chỉ để thay `map` có sẵn. Với dữ liệu lớn, iterator/fs2/Spark có mô hình thực thi khác nhau; không giả định `List.groupBy` dùng bộ nhớ như Spark.

**Lỗi/bẫy thật:** Trích nguyên văn mục 10: “The following import might fix the problem” — [04-professional-engineering/ch09-project-structure.md:389](../04-professional-engineering/ch09-project-structure.md#L389). Extension method vẫn cần import ở file sử dụng; cùng package không thay cho mọi import.

**Dùng ở đâu:** [Core](../01-scala3-core/), [Functional Programming](../02-functional-programming/), [Type System](../03-type-system/), [Project Structure](../04-professional-engineering/ch09-project-structure.md), [Testing](../12-testing/ch10-testing.md), [JVM](../04-professional-engineering/ch11-jvm-fundamentals.md). Ch8C nằm trong thư mục type-system nên bảng đối chiếu build xếp ở đây; lộ trình học chuyên sâu có thể để sau.

**Tài liệu chính thức:** [Scala 3 Book](https://docs.scala-lang.org/scala3/book/introduction.html), [Scala 3 Reference](https://docs.scala-lang.org/scala3/reference/), [API 3.9.0](https://www.scala-lang.org/api/3.9.0/).

## Cách đọc bằng chứng kiểm chứng

Audit T23a chạy test với munit thật, nên trạng thái “munit mô phỏng” còn trong một số trang cũ không đại diện toàn bộ lần kiểm sau đó. Tra [báo cáo audit](../.team/reviews/T23a-audit.md) và [review Ch8C](../.team/reviews/T22-ch08c-review.md); riêng task T27 này chỉ kiểm tài liệu. Cài JDK/CLI trên máy mới, task formatter và build sbt vẫn cần người thực hiện ghi nhận riêng, không suy từ đoạn test domain trên.
