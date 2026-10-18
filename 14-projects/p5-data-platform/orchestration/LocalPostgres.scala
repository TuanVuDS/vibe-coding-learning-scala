// Công cụ DEV: PostgreSQL 18.6 nhúng; dừng bằng file stop trong thư mục RIÊNG của lần chạy, không phụ thuộc EOF qua scala-cli.bat.
// Không phải một phần của nền tảng — production dùng PostgreSQL thật (Docker/managed). Chạy:
//   orchestration/run-platform.sh tạo marker p5-run.owner rồi gọi: scala-cli run orchestration/LocalPostgres.scala --server=false -- 55432 <run-dir>
// pg-data và pg-binaries đều trong run-dir; không tự dọn dữ liệu. Marker pg.stopped chỉ được ghi khi server thực sự đã đóng cổng.
//> using scala 3.9.0
//> using jvm 21
//> using options -deprecation -feature -Wunused:all
//> using dep io.zonky.test:embedded-postgres:2.2.2
//> using dep io.zonky.test.postgres:embedded-postgres-binaries-windows-amd64:18.6.0
//> using dep io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64:18.6.0
//> using dep org.slf4j:slf4j-nop:2.0.20
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets.UTF_8
import java.net.{ConnectException, InetSocketAddress, Socket}
import scala.util.Using

@main def localPostgres(port: Int, runDirectory: String): Unit =
  val run = Paths.get(runDirectory).toRealPath()
  require(Files.readString(run.resolve("p5-run.owner"), UTF_8).trim == "p5-run-v1", "thiếu marker sở hữu lần chạy")
  require(!Files.exists(run.resolve("jvm.pid")), "thư mục lần chạy đã được dùng")
  def marker(name: String, value: String): Unit =
    Files.writeString(run.resolve(name), value + "\n", UTF_8, StandardOpenOption.CREATE_NEW): Unit
  val self = ProcessHandle.current()
  marker("jvm.started", self.info().startInstant().orElseThrow().toEpochMilli.toString)
  marker("jvm.pid", self.pid().toString)
  val stop = run.resolve("stop")
  // Cleanup có thể được yêu cầu khi Scala CLI còn compile; JVM khởi động trễ không được tạo server mới sau cleanup.
  if Files.exists(stop) then marker("pg.stopped", "cancelled-before-start")
  else
    val binaries = run.resolve("pg-binaries")
    // Cả thư mục parent mà zonky dùng để tìm/dọn instance cũ cũng phải nằm trong run-dir của script.
    System.setProperty("ot.epg.working-dir", binaries.toString): Unit
    val pg = EmbeddedPostgres.builder().setPort(port).setDataDirectory(run.resolve("pg-data"))
      .setOverrideWorkingDirectory(binaries.toFile).setCleanDataDirectory(false).start()
    try
      val serverId = Files.readAllLines(run.resolve("pg-data/postmaster.pid"), UTF_8).get(0).trim.toLong
      val server = ProcessHandle.of(serverId).orElseThrow()
      marker("pg.started", server.info().startInstant().orElseThrow().toEpochMilli.toString)
      marker("pg.pid", serverId.toString)
      marker("pg.ready", pg.getJdbcUrl("postgres", "postgres"))
      println(s"READY ${pg.getJdbcUrl("postgres", "postgres")}")
      while !Files.exists(stop) do Thread.sleep(200)
    finally
      pg.close() // zonky có thể log rồi nuốt lỗi pg_ctl: phải kiểm tra cổng trước khi báo STOPPED.
      val listening = try Using.resource(new Socket()) { socket =>
          socket.connect(new InetSocketAddress("127.0.0.1", port), 300)
          true
        }
        catch case _: ConnectException => false
      require(!listening, s"PostgreSQL vẫn giữ cổng $port sau pg.close(); cần cleanup dự phòng")
      require(!Files.exists(run.resolve("pg-data/postmaster.pid")), "postmaster.pid còn tồn tại sau pg.close(); cần cleanup dự phòng")
      marker("pg.stopped", "closed")
      println("STOPPED")
