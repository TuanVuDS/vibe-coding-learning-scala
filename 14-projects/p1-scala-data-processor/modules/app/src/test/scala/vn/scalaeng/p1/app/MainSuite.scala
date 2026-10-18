package vn.scalaeng.p1.app

import munit.FunSuite
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.Comparator

class MainSuite extends FunSuite:
  // Thư mục gốc project: sbt chạy test từ root (không fork) hoặc từ thư mục module (khi fork).
  private val root = Paths.get(sys.props.getOrElse("p1.root",
    if Files.exists(Paths.get("data/orders.csv")) then "." else "../.."))
  private val tmp = FunFixture[Path](
    setup = _ => Files.createTempDirectory("p1-test"),
    teardown = d => Files.walk(d).sorted(Comparator.reverseOrder()).forEach(p => Files.delete(p))
  )
  private def run(args: String*): (Int, String) =
    val buf = ByteArrayOutputStream()
    val code = Main.run(args.toList, PrintStream(buf, true, "UTF-8"), () => Instant.parse("2026-10-15T00:00:00Z"))
    (code, buf.toString("UTF-8"))

  test("Args: tích lũy lỗi"):
    assertEquals(Args.parse(List("--orders", "a", "--bogus", "x", "--max-reject-ratio", "2")).left.map(_.toSet),
      Left(Set("tham số không biết: --bogus", "thiếu --users", "thiếu --out", "--max-reject-ratio phải trong [0, 1]: '2'")))
    assert(Args.parse(List("--orders")).isLeft)

  tmp.test("dữ liệu mẫu: vượt ngưỡng 10% -> exit 2 nhưng vẫn ghi output để điều tra"):
    out =>
      val (code, logs) = run("--orders", root.resolve("data/orders.csv").toString, "--users", root.resolve("data/users.json").toString, "--out", out.toString)
      assertEquals(code, ExitCode.DataQuality)
      assert(Files.exists(out.resolve("report.csv")))
      assert(logs.contains("\"balanced\":true"), logs)
      assert(logs.contains("\"msg\":\"orders reference unknown users\""), logs)

  tmp.test("chạy lại: output giống hệt từng byte, không còn file .tmp"):
    out =>
      val args = Seq("--orders", root.resolve("data/orders.csv").toString, "--users", root.resolve("data/users.json").toString,
                     "--out", out.toString, "--max-reject-ratio", "0.5")
      assertEquals(run(args*)._1, ExitCode.Ok)
      val first = List("report.csv", "rejected.csv", "summary.json").map(f => Files.readAllBytes(out.resolve(f)).toList)
      assertEquals(run(args*)._1, ExitCode.Ok)
      val second = List("report.csv", "rejected.csv", "summary.json").map(f => Files.readAllBytes(out.resolve(f)).toList)
      assertEquals(first, second)
      assertEquals(Files.list(out).filter(_.toString.endsWith(".tmp")).count(), 0L)

  tmp.test("file thiếu -> exit 3; JSON hỏng -> exit 2"):
    out =>
      assertEquals(run("--orders", out.resolve("x.csv").toString, "--users", "u", "--out", out.toString)._1, ExitCode.Io)
      val o = out.resolve("o.csv"); val u = out.resolve("u.json")
      Files.writeString(o, "order_id,user_id,sku,unit_price,quantity,status,order_date,updated_at\n", UTF_8)
      Files.writeString(u, "[{", UTF_8)
      assertEquals(run("--orders", o.toString, "--users", u.toString, "--out", out.toString)._1, ExitCode.DataQuality)
