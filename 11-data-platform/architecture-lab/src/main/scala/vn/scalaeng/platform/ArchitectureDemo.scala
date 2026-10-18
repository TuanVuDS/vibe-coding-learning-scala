package vn.scalaeng.platform

import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate
import scala.jdk.CollectionConverters.*

/** Kịch bản của bài (mục 9): chạy trên BẢN SAO dữ liệu mẫu trong thư mục tạm, không đụng `data/`. */
object ArchitectureDemo:
  def copyTree(from: Path, to: Path, skip: Path => Boolean = _ => false): Unit =
    val s = Files.walk(from)
    try s.iterator().asScala.filterNot(skip).foreach { f =>
      val t = to.resolve(from.relativize(f).toString)
      if Files.isDirectory(f) then Files.createDirectories(t) else Files.copy(f, t)
    } finally s.close()

  def main(args: Array[String]): Unit =
    val src = Paths.get(args.headOption.getOrElse("data"))
    val root = Files.createTempDirectory("ch31-platform")
    val batch17 = src.resolve("raw/orders/ingest_date=2026-10-17")
    copyTree(src, root, f => f.startsWith(batch17))
    val p = Platform(root)
    val (d15, d17) = (LocalDate.parse("2026-10-15"), LocalDate.parse("2026-10-17"))
    def show(title: String, steps: Vector[Orchestrator.Step]): Unit =
      println(s"== $title"); steps.foreach(s => println(s"   ${s.day} ${s.asset}: ${s.action} (${s.detail})"))

    show("Lần 1: backfill 15..16 trên hai lô đầu", Orchestrator.backfill(p, Assets.all, d15, LocalDate.parse("2026-10-16"), "run-1"))
    show("Lần 2: chạy lại y hệt (idempotent)", Orchestrator.backfill(p, Assets.all, d15, LocalDate.parse("2026-10-16"), "run-2"))

    copyTree(batch17, root.resolve("raw/orders/ingest_date=2026-10-17"))
    println("== Lô 2026-10-17 đến (hoàn tiền O6 của ngày 16 + đơn ngày 17). Trạng thái TRƯỚC khi chạy:")
    Iterator.iterate(d15)(_.plusDays(1)).takeWhile(!_.isAfter(d17)).foreach { d =>
      Assets.all.foreach(a => println(s"   $d ${a.name}: ${Orchestrator.describe(Orchestrator.freshness(p, Assets.all, a, d))}"))
    }
    show("Lần 3: backfill 15..17", Orchestrator.backfill(p, Assets.all, d15, d17, "run-3"))
    println("== Impact của raw/orders/ingest_date=2026-10-17/orders.csv:")
    Orchestrator.impact(p, "raw/orders/ingest_date=2026-10-17/orders.csv").foreach(r => println(s"   $r"))
    println("== serving/daily_revenue:")
    Iterator.iterate(d15)(_.plusDays(1)).takeWhile(!_.isAfter(d17)).foreach { d =>
      Files2.lines(p.partitionFile(DailyRevenue.layer, DailyRevenue.name, d)).foreach(l => println(s"   $d | $l"))
    }
    val rawLines = p.rawFiles("orders").flatMap(f => Files2.lines(f).drop(1))
    val batch = Iterator.iterate(d15)(_.plusDays(1)).takeWhile(!_.isAfter(d17)).map(d => d -> SpeedLayer.batchPaid(p, d)).toMap
    println("== Đối soát speed layer (cộng PAID khi đến) với batch:")
    SpeedLayer.reconcile(SpeedLayer.runningPaid(rawLines), batch).foreach(l => println(s"   $l"))
    println(s"== Lineage: ${Lineage.all(p).size} dòng trong ${p.relative(p.lineageFile)}")
