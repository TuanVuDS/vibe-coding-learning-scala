package vn.scalaeng.order.app

import munit.FunSuite
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Comparator

/** Integration test: chạy shell thật với file system thật (thư mục tạm), nhưng thay log và đồng hồ. */
class MainIntegrationSuite extends FunSuite:

  /** Fixture: tạo thư mục tạm trước mỗi test, xóa sạch sau test — kể cả khi test thất bại. */
  private val tempDir = FunFixture[Path](
    setup = _ => Files.createTempDirectory("order-platform-test"),
    teardown = dir => Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p => Files.delete(p))
  )

  private val fixedClock: () => Instant = () => Instant.parse("2026-10-14T00:00:00Z")

  private def runWith(env: Map[String, String]): (Int, String) =
    val buf = ByteArrayOutputStream()
    val code = Main.run(env, PrintStream(buf, true, "UTF-8"), fixedClock)
    (code, buf.toString("UTF-8"))

  private val header = "order_id,sku,unit_price,quantity,order_date"

  tempDir.test("dữ liệu sạch -> exit 0, ghi đủ file output"):
    dir =>
      val in = dir.resolve("in.csv")
      Files.writeString(in, s"$header\nORD-1,A,100,2,2026-01-01\nORD-2,B,50,1,2026-01-01\n", UTF_8)
      val (code, logs) = runWith(Map("INPUT_PATH" -> in.toString, "OUTPUT_DIR" -> dir.resolve("out").toString))
      assertEquals(code, ExitCode.Ok)
      assertNoDiff(Files.readString(dir.resolve("out/daily_revenue.csv"), UTF_8), "date,orders,revenue_vnd\n2026-01-01,2,250\n")
      assert(logs.contains("\"balanced\":\"true\""), logs)
      assert(logs.startsWith("{\"ts\":\"2026-10-14T00:00:00Z\""), logs)

  tempDir.test("tỷ lệ lỗi vượt ngưỡng -> exit 2 và có log WARN"):
    dir =>
      val in = dir.resolve("in.csv")
      Files.writeString(in, s"$header\nORD-1,A,100,2,2026-01-01\nbroken\n", UTF_8)
      val (code, logs) = runWith(Map("INPUT_PATH" -> in.toString, "OUTPUT_DIR" -> dir.toString))
      assertEquals(code, ExitCode.DataQualityFailed)
      assert(logs.contains("\"level\":\"WARN\""), logs)

  tempDir.test("file không tồn tại -> exit 3"):
    dir =>
      val (code, _) = runWith(Map("INPUT_PATH" -> dir.resolve("missing.csv").toString))
      assertEquals(code, ExitCode.IoError)

  test("cấu hình sai -> exit 1, không đụng tới file system"):
    val (code, logs) = runWith(Map("APP_ENV" -> "prod"))
    assertEquals(code, ExitCode.ConfigError)
    assert(logs.contains("INPUT_PATH"), logs)
