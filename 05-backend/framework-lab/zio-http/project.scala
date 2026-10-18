// So sánh (Chương 13): cùng API `products` viết bằng ZIO HTTP. KHÔNG phải stack chính của giáo trình.
// Tách project riêng để không trộn hai hệ sinh thái (Cats Effect và ZIO) trên một classpath.
//   scala-cli run . --main-class vn.scalaeng.fwzio.ZioMain     # cổng 8083
//   scala-cli test .
// Phiên bản kiểm chứng trên Maven Central ngày 16/10/2026.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all -Wvalue-discard
//> using dep dev.zio::zio:2.1.27
//> using dep dev.zio::zio-http:3.11.6
//> using test.dep dev.zio::zio-test:2.1.27
//> using test.dep dev.zio::zio-test-sbt:2.1.27
