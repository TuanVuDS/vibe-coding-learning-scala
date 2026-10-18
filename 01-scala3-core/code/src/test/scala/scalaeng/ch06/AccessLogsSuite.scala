package scalaeng.ch06

import munit.FunSuite

class AccessLogsSuite extends FunSuite:
  import AccessLogs.*

  private val logs = parseAll(sampleLines)
  private def log(ts: Long, user: String, path: String, status: Int, latency: Long) =
    AccessLog(ts, user, "GET", path, status, latency)

  test("parseAll bỏ qua dòng sai định dạng"):
    assertEquals(logs.size, 7)
    assertEquals(countInvalid(sampleLines), 2)

  test("parseAll chấp nhận nhiều khoảng trắng"):
    assertEquals(parseAll(List("  1 u GET /a   200   5 ")).map(_.latencyMs), List(5L))

  test("partition và errorRate"):
    val (errors, ok) = splitErrors(logs)
    assertEquals(errors.map(_.status), List(500, 503))
    assertEquals(ok.size, 5)
    assertEqualsDouble(errorRate(logs), 2.0 / 7, 1e-9)
    assertEqualsDouble(errorRate(Nil), 0.0, 0.0)

  test("topPaths sắp xếp ổn định khi bằng nhau"):
    val data = List(log(1, "a", "/b", 200, 1), log(2, "a", "/a", 200, 1), log(3, "a", "/c", 200, 1), log(4, "a", "/c", 200, 1))
    assertEquals(topPaths(data, 3), List(("/c", 2), ("/a", 1), ("/b", 1)))

  test("uniqueUsers bỏ user ẩn danh"):
    assertEquals(uniqueUsers(logs), Set("u1", "u2", "u3"))

  test("latencyStats một lượt duyệt"):
    assertEquals(latencyStats(logs), LatencyStats(7, 2693L, 1500L))
    assertEquals(latencyStats(Nil).average, 0.0)

  test("latencyPercentile nearest-rank"):
    val data = (1 to 10).toList.map(i => log(i, "u", "/", 200, i * 10L))
    assertEquals(latencyPercentile(data, 50), 50L)
    assertEquals(latencyPercentile(data, 90), 90L)
    assertEquals(latencyPercentile(data, 100), 100L)
    intercept[IllegalArgumentException](latencyPercentile(Nil, 50))

  test("slowestPerUser"):
    assertEquals(slowestPerUser(logs).view.mapValues(_.latencyMs).toMap, Map("u1" -> 120L, "u2" -> 60L, "u3" -> 1500L))

  test("flatMap path segments"):
    assertEquals(pathSegments(logs), List("42", "7", "api", "orders", "products"))

  test("flatten + sort các lô"):
    val merged = mergeBatches(List(List(log(3, "a", "/", 200, 1)), Nil, List(log(1, "b", "/", 200, 1))))
    assertEquals(merged.map(_.timestampMs), List(1L, 3L))

  test("hasErrorBurst"):
    assert(hasErrorBurst(logs, 3, 2))
    assert(!hasErrorBurst(logs, 2, 2))

  test("errorPathsImperative"):
    assertEquals(errorPathsImperative(logs), List("/api/orders", "/api/orders"))
