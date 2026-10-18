// Thí nghiệm Chương 26 mục 3.9: dùng Spark 4.2.0 (artifact _2.13) từ code Scala 3 — KHÔNG được Apache Spark hỗ trợ chính thức.
// Kết quả thật 17/10/2026 (Windows 11, JDK 21):
//   scala-cli run . --scala 3.3.8   -> chạy (stdlib của Scala 3.3 là scala-library 2.13.18)
//   scala-cli run . --scala 3.7.3   -> chạy (scala-library 2.13.18)
//   scala-cli run . --scala 3.8.4   -> IllegalStateException: Cannot find a SparkSession implementation on the Classpath.
//   scala-cli run . --scala 3.9.0   -> như 3.8 (từ 3.8, stdlib là scala-library 3.x thay cho 2.13)
// Lỗi 3.8+/3.9 là của BUILDER CHUNG (reflection Scala 2 không đọc được stdlib 3.x: "class Array does not have a member apply");
// gọi thẳng org.apache.spark.sql.classic.SparkSession.builder() thì 3.9.0 chạy được DataFrame và encoder kiểu nguyên thủy (review T16 M6).
// Encoder case class tự động không có ở MỌI phiên bản Scala 3 (cần TypeTag của Scala 2). Không tổ hợp nào ở đây được Apache hỗ trợ.
//> using scala 3.7.3
//> using jvm 21
//> using dep org.apache.spark:spark-sql_2.13:4.2.0
//> using javaOpt --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
