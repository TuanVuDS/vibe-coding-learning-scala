// Lời giải code Chương 23 (Scala 2.13). Dùng lại mã nguồn của 09-spark/spark-sql-lab.
//   scala-cli test .
//> using scala 2.13.18
//> using jvm 21
//> using options -deprecation -feature -Xlint:_ -Wunused:imports,privates,locals,params
//> using file ../../09-spark/spark-sql-lab/src/main/scala
//> using resourceDir ../../09-spark/spark-sql-lab/src/main/resources
//> using dep org.apache.spark::spark-sql:4.2.0
//> using dep com.globalmentor:hadoop-bare-naked-local-fs:0.1.0
//> using test.dep org.scalameta::munit:1.2.4
// Đúng các cờ --add-opens mà spark-submit tự thêm (org.apache.spark.launcher.JavaModuleOptions, spark-launcher 4.2.0).
// Thiếu `sun.util.calendar` thì collect() cột timestamp lỗi IllegalAccessException (Chương 23 mục 10).
//> using javaOpt --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
