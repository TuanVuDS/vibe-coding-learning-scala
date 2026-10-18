package vn.scalaeng.cost

class CostSuite extends munit.FunSuite:
  test("nearest rank preserves tails rather than interpolating") {
    assertEquals(Models.percentile(Vector(1, 2, 100), .5), 2.0)
    assertEquals(Models.percentile(Vector(1, 2, 100), .95), 100.0)
    assertEquals(Models.percentile(Vector(4), 1), 4.0)
  }
  test("percentiles reject empty invalid samples and invalid p") {
    intercept[IllegalArgumentException](Models.percentile(Vector.empty, .5))
    intercept[IllegalArgumentException](Models.percentile(Vector(Double.NaN), .5))
    intercept[IllegalArgumentException](Models.percentile(Vector(-1), .5))
    intercept[IllegalArgumentException](Models.percentile(Vector(1), 0))
  }
  test("cost uses aggregate fleet throughput and node-hours") {
    val cost = Models.costPerMillion(50000, 2, BigDecimal("0.12"))
    assert((cost - BigDecimal(1) / 750).abs < BigDecimal("1e-25"))
    assertEquals(Models.costPerMillion(1, 1, BigDecimal(0)), BigDecimal(0))
  }
  test("cost rejects zero infinite throughput negative price and nodes") {
    intercept[IllegalArgumentException](Models.costPerMillion(0, 1, BigDecimal(1)))
    intercept[IllegalArgumentException](Models.costPerMillion(Double.PositiveInfinity, 1, BigDecimal(1)))
    intercept[IllegalArgumentException](Models.costPerMillion(1, 0, BigDecimal(1)))
    intercept[IllegalArgumentException](Models.costPerMillion(1, 1, BigDecimal(-1)))
  }
  test("Little law reserves utilization and rounds up") {
    assertEquals(Models.concurrency(500, 20, .7), 15)
    assertEquals(Models.concurrency(0, 20, .7), 0)
    intercept[IllegalArgumentException](Models.concurrency(1, 1, 0))
    intercept[IllegalArgumentException](Models.concurrency(1e300, 1e300, .7))
  }
  test("partition count handles empty exact remainder and huge byte counts") {
    assertEquals(Models.partitions(0, 128), BigInt(1))
    assertEquals(Models.partitions(256, 128), BigInt(2))
    assertEquals(Models.partitions(257, 128), BigInt(3))
    assertEquals(Models.partitions(BigInt(Long.MaxValue) * 4, 1), BigInt(Long.MaxValue) * 4)
    intercept[IllegalArgumentException](Models.partitions(1, 0))
  }
  test("scheduler accounts for overhead empty tasks and hot partition") {
    assertEquals(Models.makespan(Vector(10, 10, 10, 10), 2, 1), 22.0)
    assertEquals(Models.makespan(Vector(37, 1, 1, 1), 2, 1), 38.0)
    assertEquals(Models.makespan(Vector.empty, 2, 1), 0.0)
    intercept[IllegalArgumentException](Models.makespan(Vector(1), 0, 0))
  }
  test("cache requires strictly positive saving including build") {
    assertEquals(Models.cacheBreakEven(150, 90, 20), Some(BigInt(3)))
    assertEquals(Models.cacheBreakEven(140, 90, 20), Some(BigInt(3)))
    assertEquals(Models.cacheBreakEven(0, 90, 20), Some(BigInt(1)))
    assertEquals(Models.cacheBreakEven(10, 20, 20), None)
    intercept[IllegalArgumentException](Models.cacheBreakEven(-1, 2, 1))
  }
  test("network estimates use effective bytes per second not link bit rate") {
    assertEquals(Models.transferSeconds(100000000, 50000000), 2.0)
    assertEquals(Models.transferSeconds(0, 50000000), 0.0)
    intercept[IllegalArgumentException](Models.transferSeconds(1, 0))
  }
  test("memory budget never returns negative slots and handles BigInt") {
    assertEquals(Models.concurrentMemory(1024, 256, 256, 128), BigInt(4))
    assertEquals(Models.concurrentMemory(100, 90, 90, 1), BigInt(0))
    assertEquals(Models.concurrentMemory(BigInt(2).pow(100), 0, 0, 1), BigInt(2).pow(100))
    intercept[IllegalArgumentException](Models.concurrentMemory(1, 0, 0, 0))
  }
  test("JSON and Avro preserve unicode escapes duplicate rows and extreme money") {
    val rows = Vector(Order(Long.MaxValue, "U-\"\\\n", Long.MinValue, "đơn\n😀"), Orders.fixture(1), Orders.fixture(1))
    assertEquals(Orders.readJson(Orders.json(rows)), rows)
    assertEquals(Orders.readAvro(Orders.avro(rows)), rows)
  }
  test("empty payload round trips and truncated Avro fails") {
    assertEquals(Orders.readJson(Orders.json(Vector.empty)), Vector.empty)
    assertEquals(Orders.readAvro(Orders.avro(Vector.empty)), Vector.empty)
    intercept[java.io.EOFException](Orders.readAvro(Array.emptyByteArray))
  }
  test("query rewrite preserves aggregate for selected unselected and duplicate rows") {
    val cases = Vector(Vector.empty[Order], Vector(Orders.fixture(1)),
      Vector(Orders.fixture(100), Orders.fixture(100), Orders.fixture(3)), Vector.tabulate(1001)(Orders.fixture))
    cases.foreach(rows => assertEquals(Orders.query(rows, true), Orders.query(rows, false)))
  }
  test("consumed checksum responds to input rather than returning a constant") {
    assertNotEquals(Orders.work(1, 128), Orders.work(2, 128))
    assertEquals(Orders.work(42, 0), 42L)
  }
  test("batching respects cap timeout final partial batch and preserves every event") {
    val ranges = Models.batchRanges(125, 128, 100000, 5000000)
    assertEquals(ranges, Vector(0 -> 51, 51 -> 102, 102 -> 125))
    assertEquals(ranges.flatMap((a, b) => a until b), (0 until 125).toVector)
    assertEquals(Models.batchRanges(2, 128, 100, 0), Vector(0 -> 1, 1 -> 2))
    assertEquals(Models.batchRanges(0, 1, 1, 0), Vector.empty)
    intercept[IllegalArgumentException](Models.batchRanges(1, 0, 1, 0))
  }
