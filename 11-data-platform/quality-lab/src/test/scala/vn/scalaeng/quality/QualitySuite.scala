package vn.scalaeng.quality

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDate}

/** Các tầng kiểm thử của Chương 33, từ nhỏ tới lớn: unit (từng hàm) → check (trên tập) → contract → golden/regression → drift → end-to-end. */
class QualitySuite extends munit.FunSuite:
  private val d15 = LocalDate.parse("2026-10-15")
  private def raw(id: String, st: String = "PAID", qty: String = "1", price: String = "100", ts: String = "2026-10-15T01:00:00Z",
      batch: String = "2026-10-15", sku: String = "KB-01", cust: String = "C1", day: String = "2026-10-15") =
    RawOrder(batch, id, cust, day, st, sku, qty, price, ts)
  private def ok(r: RawOrder): Order = Pipeline.validate(r).toOption.get
  private val segs = Map("C1" -> "Retail", "C2" -> "Wholesale", "C3" -> "Retail")

  private def fixture: Output =
    Pipeline.run(DataFiles.batches(Paths.get("data/input")).map(DataFiles.toRaw.tupled), Pipeline.segments(DataFiles.customers(Paths.get("data/customers.csv"))).toOption.get)
  private def contract(v: Int): Contract =
    Contract.parse(new String(Files.readAllBytes(Paths.get(s"contracts/orders.v$v.json")), StandardCharsets.UTF_8)).toOption.get

  // ---------- 1. Unit test cho transformation ----------

  test("unit: validate gom MỌI lỗi; chuẩn hóa sku/customer, status không phân biệt hoa thường") {
    assertEquals(Pipeline.validate(raw("X", st = "SHIPPED", qty = "0", price = "-5", ts = "x")).left.toOption.get.errors, List(
      "status: phải là một trong PAID/REFUNDED/CANCELLED", "quantity: số nguyên 1..1000", "unit_price_vnd: số nguyên dương", "updated_at: phải là timestamp ISO-8601"))
    val o = ok(raw("X", st = "paid", sku = " kb-01 ", cust = "c1"))
    assertEquals((o.status, o.sku, o.customerId), (Status.Paid, "KB-01", "C1"))
  }

  test("unit: daily dùng BigInt — 1000 × Long.MaxValue không tràn") {
    val f = Fact(ok(raw("X", qty = "1000", price = Long.MaxValue.toString)), "Retail")
    assertEquals(Pipeline.daily(Seq(f)).head.revenueVnd, BigInt(Long.MaxValue) * 1000)
  }

  // ---------- 2. Checks trên tập ----------

  test("checks: uniqueness, toàn vẹn tham chiếu, số dòng, độ mới — mỗi check bắt một loại lỗi khác") {
    val f1 = Fact(ok(raw("A")), "Retail"); val f2 = Fact(ok(raw("B", cust = "C9")), "UNKNOWN")
    val checks = OutputChecks.facts(segs.keySet, asOf = LocalDate.parse("2026-10-16"))
    assertEquals(checks.map(_.run(Seq(f1, f1, f2)).failed), List(2, 1, 0, 0))
    assertEquals(OutputChecks.facts(segs.keySet, asOf = LocalDate.parse("2026-10-20")).last.run(Seq(f1)).samples,
      List("mới nhất 2026-10-15, asOf 2026-10-20"))
    assertEquals(Check.rowCount[Int]("n", Severity.Error, 1, 5).run(Nil).failed, 1)
  }

  // ---------- 3. Data contract + schema evolution ----------

  test("contract: kiểu sai, thiếu trường bắt buộc, trường thừa, enum phân biệt hoa thường") {
    val good = Map("order_id" -> "O1", "customer_id" -> "C1", "day" -> "2026-10-15", "status" -> "PAID", "sku" -> "KB-01",
      "quantity" -> "2", "unit_price_vnd" -> "450000", "updated_at" -> "2026-10-15T01:00:00Z")
    assertEquals(Contract.validate(contract(1), good), Nil)
    assertEquals(Contract.validate(contract(1), good - "sku" + ("status" -> "paid") + ("coupon" -> "X")), List(
      "coupon: trường không có trong contract orders v1", "status='paid': không phải enum {PAID,REFUNDED,CANCELLED}", "sku: bắt buộc"))
    // Contract đúng KIỂU nhưng không thay quy tắc nghiệp vụ: quantity 0 vẫn là int hợp lệ.
    assertEquals(Contract.validate(contract(1), good + ("quantity" -> "0")), Nil)
    // Reader projection: trường lạ bị bỏ qua (consumer cũ đọc được), trường mình cần vẫn phải đúng.
    assertEquals(Contract.read(contract(1), good + ("coupon" -> "X")), Nil)
    assertEquals(Contract.read(contract(1), good - "sku"), List("sku: bắt buộc"))
  }

  test("contract: v1 → v2 (FORWARD: reader cũ, writer mới) — int → long, bắt buộc → nullable, thêm enum là BREAKING; thêm trường là compatible") {
    import Contract.Impact.*
    assertEquals(Contract.compatibility(contract(1), contract(2)).map(c => (c.field, c.impact)), List(
      "quantity" -> Breaking, "sku" -> Breaking, "status" -> Breaking, "channel" -> Compatible))
    val dropped = contract(1).copy(fields = contract(1).fields.filterNot(_.name == "sku"))
    assertEquals(Contract.compatibility(contract(1), dropped), List(Contract.Change("sku", "bị bỏ", Breaking)))
    // Phản ví dụ cụ thể cho int → long (review T17 M1): writer v2 được ghi 2147483648, reader v1 không đọc được.
    val big = Map("order_id" -> "O1", "customer_id" -> "C1", "day" -> "2026-10-15", "status" -> "PAID", "sku" -> "KB-01",
      "quantity" -> "2147483648", "unit_price_vnd" -> "1", "updated_at" -> "2026-10-15T01:00:00Z")
    assertEquals((Contract.validate(contract(2), big), Contract.read(contract(1), big)), (Nil, List("quantity='2147483648': không phải int")))
    assert(Contract.parse("""{"name":"x","version":1,"owner":"o","fields":[{"name":"a","type":"decimal"}]}""").left.toOption.get.contains("kiểu lạ"))
  }

  /** Bản ghi "biên" sinh từ contract: với mỗi trường, lần lượt thử từng giá trị biên của kiểu (các trường khác giữ giá trị đầu tiên). */
  private def boundaryRecords(c: Contract): List[Map[String, String]] =
    def samples(f: FieldSpec): List[String] = (f.`type` match
      case "string"    => List("s")
      case "int"       => List("0", Int.MaxValue.toString, Int.MinValue.toString)
      case "long"      => List("1", (Int.MaxValue.toLong + 1).toString, Long.MaxValue.toString)
      case "date"      => List("2026-10-15")
      case "timestamp" => List("2026-10-15T01:00:00Z")
      case _           => f.values.getOrElse(Nil)) ++ (if f.nullable then List("") else Nil)
    val base = c.fields.map(f => f.name -> samples(f).head).toMap
    c.fields.flatMap(f => samples(f).map(v => base + (f.name -> v)))

  test("contract: CAM KẾT của compatibility — mọi thay đổi Compatible ⇒ reader cũ đọc được MỌI bản ghi biên hợp lệ của writer mới; Breaking ⇒ có phản ví dụ") {
    def field(n: String, t: String, nullable: Boolean = false, values: Option[List[String]] = None) = FieldSpec(n, t, nullable, values)
    def c(fs: FieldSpec*) = Contract("x", 1, "o", fs.toList)
    val e = Some(List("A", "B"))
    val pairs = List(                                       // (old, next)
      c(field("a", "int")) -> c(field("a", "long")),             // int → long
      c(field("a", "long")) -> c(field("a", "int")),             // long → int
      c(field("a", "int")) -> c(field("a", "int"), field("b", "string", nullable = true)), // thêm trường
      c(field("a", "int"), field("b", "string", nullable = true)) -> c(field("a", "int")),  // bỏ trường nullable
      c(field("a", "int"), field("b", "string")) -> c(field("a", "int")),                   // bỏ trường bắt buộc
      c(field("a", "string")) -> c(field("a", "string", nullable = true)),                  // bắt buộc → nullable
      c(field("a", "string", nullable = true)) -> c(field("a", "string")),                  // nullable → bắt buộc
      c(field("a", "enum", values = e)) -> c(field("a", "enum", values = Some(List("A", "B", "C")))), // thêm enum
      c(field("a", "enum", values = e)) -> c(field("a", "enum", values = Some(List("A")))))           // bớt enum
    val verdicts = pairs.map { (o, n) =>
      val compatible = Contract.compatibility(o, n).forall(_.impact == Contract.Impact.Compatible)
      val readable   = boundaryRecords(n).filter(Contract.validate(n, _).isEmpty).forall(Contract.read(o, _).isEmpty)
      assertEquals(readable, compatible, s"$o -> $n: ${Contract.compatibility(o, n)}")
      compatible
    }
    assertEquals(verdicts, List(false, true, true, true, false, false, true, false, true))
  }

  // ---------- 4. Golden dataset + regression ----------

  test("golden: daily_revenue, rejected, conflicts của dữ liệu mẫu tính tay") {
    val out = fixture
    for (name, text) <- List("daily_revenue.csv" -> Csv.daily(out.daily), "rejected.csv" -> Csv.rejected(out.rejected),
        "conflicts.csv" -> Csv.conflicts(out.conflicts)) do
      Golden.check(name, text).left.foreach(fail(_))
  }

  test("regression: một 'tối ưu' tưởng vô hại (bỏ qua dedup phiên bản) bị golden bắt — unit test của validate vẫn xanh") {
    val raws = DataFiles.batches(Paths.get("data/input")).map(DataFiles.toRaw.tupled)
    val buggy = Pipeline.daily(Pipeline.facts(raws.flatMap(r => Pipeline.validate(r).toOption).distinctBy(o => (o.lineKey, o.payload)), segs))
    val result = Golden.check("daily_revenue.csv", Csv.daily(buggy))
    assert(result.isLeft, "golden phải đỏ")
    assert(result.left.toOption.get.contains("+ 2026-10-15,Retail,1600000,3,2"), result.left.toOption.get)
    Files.deleteIfExists(Golden.dir.resolve("daily_revenue.csv.actual"))
  }

  // ---------- 5. Drift: thay đổi ngoài mong đợi trên dữ liệu THẬT ----------

  test("drift: mất một khách (ví dụ nguồn thiếu file) làm doanh thu đổi 2,05% → vượt ngưỡng 1%; chạy lại y hệt → không drift") {
    val out = fixture
    val t = Tolerance(BigDecimal("0.01"), BigDecimal("0.05"), BigDecimal("0.10"))
    assertEquals(Drift.compare(Profile.of(out.daily), Profile.of(fixture.daily), t), Nil)
    assertEquals(Drift.compare(Profile.of(out.daily), Profile.of(Pipeline.daily(out.facts.filterNot(_.order.customerId == "C9"))), t),
      List("doanh thu đổi 2.0500% (21900000 → 21450000) > 1.00%"))
    val allUnknown = Pipeline.daily(out.facts.map(_.copy(segment = "UNKNOWN")))
    assert(Drift.compare(Profile.of(out.daily), Profile.of(allUnknown), t).exists(_.startsWith("tỷ trọng UNKNOWN 100.0000%")))
  }

  // ---------- 6. End-to-end: file → pipeline → file → so golden ----------

  test("e2e: chép input ra thư mục tạm, chạy qua đúng đường đọc file, output khớp golden; customers trùng bị từ chối") {
    val tmp = Files.createTempDirectory("ch33-e2e")
    Seq("2026-10-15", "2026-10-16").foreach { d =>
      Files.createDirectories(tmp.resolve(s"input/ingest_date=$d"))
      Files.copy(Paths.get(s"data/input/ingest_date=$d/orders.csv"), tmp.resolve(s"input/ingest_date=$d/orders.csv"))
    }
    Files.copy(Paths.get("data/customers.csv"), tmp.resolve("customers.csv"))
    val out = Pipeline.run(DataFiles.batches(tmp.resolve("input")).map(DataFiles.toRaw.tupled),
      Pipeline.segments(DataFiles.customers(tmp.resolve("customers.csv"))).toOption.get)
    Golden.check("daily_revenue.csv", Csv.daily(out.daily)).left.foreach(fail(_))
    Files.write(tmp.resolve("customers.csv"), "C1,Wholesale,HN\n".getBytes(StandardCharsets.UTF_8), java.nio.file.StandardOpenOption.APPEND)
    assertEquals(Pipeline.segments(DataFiles.customers(tmp.resolve("customers.csv"))), Left(List("C1")))
  }

  test("DataFiles: dòng THỪA cột bị từ chối (không bỏ âm thầm); BOM + CRLF đọc đúng (review T17 M3, m2)") {
    val dir = Files.createTempDirectory("ch33-files")
    val header = "order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at"
    val ok = "O1,C1,2026-10-15,PAID,KB-01,2,450000,2026-10-15T01:00:00Z"
    Files.write(dir.resolve("a.csv"), ("\uFEFF" + header + "\r\n" + ok + "\r\n" + ok + ",UNDECLARED\r\n").getBytes(StandardCharsets.UTF_8))
    val recs = DataFiles.records(dir.resolve("a.csv"))
    assertEquals(recs.head.get("order_id"), Some("O1"))
    assertEquals(Contract.validate(contract(1), recs.head), Nil)
    assertEquals(Contract.validate(contract(1), recs(1)), List("dòng có giá trị thừa ngoài header: 'UNDECLARED'"))
    assertEquals(Pipeline.validate(DataFiles.toRaw("2026-10-15", recs(1))).left.toOption.map(_.errors),
      Some(List("dòng có giá trị thừa ngoài header: 'UNDECLARED'")))
    // Cột thừa RỖNG hoặc chỉ khoảng trắng (dấu phẩy cuối dòng) vẫn là dòng sai cấu trúc — review T17 vòng 2.
    Files.write(dir.resolve("b.csv"), (header + "\n" + ok + ",\n" + ok + ",   \n" + ok + ",,\n").getBytes(StandardCharsets.UTF_8))
    val trailing = DataFiles.records(dir.resolve("b.csv"))
    assertEquals(trailing.map(r => Pipeline.validate(DataFiles.toRaw("2026-10-15", r)).left.toOption.map(_.errors)),
      Vector(Some(List("dòng có giá trị thừa ngoài header: ''")), Some(List("dòng có giá trị thừa ngoài header: '   '")),
        Some(List("dòng có giá trị thừa ngoài header: ','"))))
  }

  test("dữ liệu đến trễ (cụ thể): bản cũ hơn đến SAU không đè bản mới; hoàn tiền đến muộn sửa đúng ngày cũ") {
    val paid  = raw("O1", ts = "2026-10-15T01:00:00Z")
    val refund = raw("O1", st = "REFUNDED", ts = "2026-10-16T09:00:00Z", batch = "2026-10-16")
    val staleCopy = raw("O1", ts = "2026-10-14T23:00:00Z", batch = "2026-10-17") // bản cũ hơn, đến muộn nhất
    val out = Pipeline.run(Seq(paid, refund, staleCopy), segs)
    assertEquals((out.facts.map(_.order.status), out.daily, out.superseded), (Vector(Status.Refunded), Vector.empty, 2))
    assertEquals(Instant.parse("2026-10-16T09:00:00Z"), out.facts.head.order.updatedAt)
    assertEquals(out.facts.head.order.day, d15)
  }
