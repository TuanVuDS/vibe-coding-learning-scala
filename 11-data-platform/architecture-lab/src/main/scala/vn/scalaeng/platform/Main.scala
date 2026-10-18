package vn.scalaeng.platform

import io.circe.syntax.*

import java.nio.file.Paths
import java.time.LocalDate
import java.util.UUID

/** CLI — điểm vào cho người vận hành VÀ cho orchestrator (Dagster gọi `materialize`). Mã thoát: 0 thành công, 1 lỗi, 2 sai tham số. */
object Main:
  private def opts(args: Seq[String]): Map[String, String] =
    args.collect { case a if a.startsWith("--") && a.contains("=") => val (k, v) = a.drop(2).span(_ != '='); k -> v.drop(1) }.toMap

  def run(args: Seq[String], out: String => Unit): Int =
    val o = opts(args.drop(1))
    def day(k: String) = o.get(k).flatMap(v => scala.util.Try(LocalDate.parse(v)).toOption)
    val root = o.get("root").map(Paths.get(_)).map(Platform(_))
    val runId = o.getOrElse("run-id", UUID.randomUUID().toString.take(8))
    import Lineage.given
    (args.headOption, root) match
      case (Some("materialize"), Some(p)) =>
        (o.get("asset").flatMap(Assets.byName), day("day")) match
          case (Some(a), Some(d)) => out(Orchestrator.materialize(p, a, d, runId).asJson.noSpaces); 0
          case _                  => out("cần --asset=<staging_orders|daily_revenue> --day=YYYY-MM-DD"); 2
      case (Some("status"), Some(p)) =>
        (day("from"), day("to")) match
          case (Some(f), Some(t)) =>
            Iterator.iterate(f)(_.plusDays(1)).takeWhile(!_.isAfter(t)).foreach { d =>
              Assets.all.foreach(a => out(s"$d ${a.name}: ${Orchestrator.describe(Orchestrator.freshness(p, Assets.all, a, d))}"))
            }
            0
          case _ => out("cần --from= --to="); 2
      case (Some("backfill"), Some(p)) =>
        (day("from"), day("to")) match
          case (Some(f), Some(t)) =>
            Orchestrator.backfill(p, Assets.all, f, t, runId, force = o.get("force").contains("true"))
              .foreach(s => out(s"${s.day} ${s.asset}: ${s.action} (${s.detail})"))
            0
          case _ => out("cần --from= --to="); 2
      case (Some("impact"), Some(p)) =>
        o.get("raw") match
          case Some(ref) => Orchestrator.impact(p, ref).foreach(out); 0
          case None      => out("cần --raw=<đường dẫn tương đối trong root>"); 2
      case _ =>
        out("dùng: materialize|status|backfill|impact --root=<thư mục> ..."); 2

  def main(args: Array[String]): Unit =
    val code = scala.util.Try(run(args.toSeq, println)).fold(e => { System.err.println(s"lỗi: $e"); 1 }, identity)
    sys.exit(code)
