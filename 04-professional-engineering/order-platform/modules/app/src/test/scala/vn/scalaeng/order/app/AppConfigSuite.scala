package vn.scalaeng.order.app

import munit.FunSuite
import java.nio.file.Paths
import java.io.{ByteArrayOutputStream, PrintStream}
import java.time.Instant

class AppConfigSuite extends FunSuite:

  test("dev có giá trị mặc định"):
    val cfg = AppConfig.load(Map.empty)
    assertEquals(cfg.map(_.inputPath), Right(Paths.get("sample-data/orders.csv")))
    assertEquals(cfg.map(_.maxRejectRatio), Right(0.05))

  test("prod bắt buộc INPUT_PATH"):
    assertEquals(AppConfig.load(Map("APP_ENV" -> "prod")), Left(List("thiếu INPUT_PATH (bắt buộc ngoài môi trường dev)")))
    assert(AppConfig.load(Map("APP_ENV" -> "prod", "INPUT_PATH" -> "/data/in.csv")).isRight)

  test("tích lũy mọi lỗi cấu hình"):
    val r = AppConfig.load(Map("APP_ENV" -> "qa", "MAX_REJECT_RATIO" -> "2", "LOG_LEVEL" -> "verbose"))
    assertEquals(r.left.map(_.size), Left(4))

  test("JsonLogger escape và lọc theo level"):
    val buf = ByteArrayOutputStream()
    val log = JsonLogger(LogLevel.Warn, PrintStream(buf, true, "UTF-8"), () => Instant.parse("2026-10-14T00:00:00Z"))
    log.info("bị lọc")
    log.warn("xin \"chào\"\nAn", "k" -> "v")
    assertEquals(buf.toString("UTF-8"),
      "{\"ts\":\"2026-10-14T00:00:00Z\",\"level\":\"WARN\",\"msg\":\"xin \\\"chào\\\"\\nAn\",\"k\":\"v\"}\n")
