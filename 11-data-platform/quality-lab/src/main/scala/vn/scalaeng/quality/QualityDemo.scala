package vn.scalaeng.quality

import java.nio.charset.StandardCharsets
import java.nio.file.{Files as JFiles, Paths}
import java.time.LocalDate

object QualityDemo:
  def main(args: Array[String]): Unit =
    val raw   = DataFiles.batches(Paths.get("data/input"))
    val cust  = DataFiles.customers(Paths.get("data/customers.csv"))
    val v1    = Contract.parse(new String(JFiles.readAllBytes(Paths.get("contracts/orders.v1.json")), StandardCharsets.UTF_8)).fold(sys.error, identity)
    val v2    = Contract.parse(new String(JFiles.readAllBytes(Paths.get("contracts/orders.v2.json")), StandardCharsets.UTF_8)).fold(sys.error, identity)

    println(s"== Contract ${v1.name} v${v1.version} (chủ sở hữu ${v1.owner}) trên ${raw.size} bản ghi input")
    raw.map((b, r) => (b, r, Contract.validate(v1, r))).filter(_._3.nonEmpty).foreach { (b, r, es) =>
      println(s"   lô $b ${r.getOrElse("order_id", "?")}: ${es.mkString("; ")}")
    }
    println(s"== Tương thích v${v1.version} → v${v2.version} (góc nhìn người tiêu thụ)")
    Contract.compatibility(v1, v2).foreach(c => println(f"   ${c.impact}%-10s ${c.field}: ${c.description}"))

    val segs = Pipeline.segments(cust).fold(d => sys.error(s"customers trùng: $d"), identity)
    val out  = Pipeline.run(raw.map(DataFiles.toRaw.tupled), segs)
    println(s"== Pipeline: fact=${out.facts.size} rejected=${out.rejected.size} conflicts=${out.conflicts.size} trùng=${out.exactDuplicates} thay-thế=${out.superseded}")
    println("== Checks trên output")
    (OutputChecks.facts(segs.keySet, LocalDate.parse("2026-10-17")).map(_.run(out.facts)) ++ OutputChecks.daily.map(_.run(out.daily))).foreach { r =>
      println(f"   ${if r.passed then "PASS" else "FAIL"} ${r.severity}%-5s ${r.name}%-42s ${r.failed}/${r.checked} ${r.samples.mkString(", ")}")
    }
    print(Csv.daily(out.daily))
    val base = Profile.of(out.daily)
    val noRefunds = Pipeline.daily(out.facts.filterNot(_.order.customerId == "C9"))
    println(s"== Drift: profile hiện tại $base")
    println(s"   giả sử lần chạy sau MẤT khách C9 → ${Drift.compare(base, Profile.of(noRefunds), Tolerance(BigDecimal("0.01"), BigDecimal("0.05"), BigDecimal("0.10")))}")
