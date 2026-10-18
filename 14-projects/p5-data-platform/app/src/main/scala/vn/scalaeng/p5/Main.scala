package vn.scalaeng.p5

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import io.circe.syntax.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.doobie.Transactor

import java.nio.file.Paths
import java.time.{Instant, LocalDate}
import java.util.UUID

/** Các BƯỚC của nền tảng dưới dạng lệnh — mỗi lệnh làm một việc, mã thoát có nghĩa, để orchestrator (Dagster) gọi từng bước.
  * Mã thoát: 0 thành công · 1 lỗi bất ngờ · 2 sai tham số · 3 dữ liệu bị cách ly (contract) · 4 quality gate chặn.
  */
object Main extends IOApp:
  private def opts(args: List[String]): Map[String, String] =
    args.collect { case a if a.startsWith("--") && a.contains("=") => val (k, v) = a.drop(2).span(_ != '='); k -> v.drop(1) }.toMap

  def xa(o: Map[String, String]): Transactor[IO] =
    Transactor.fromDriverManager[IO](driver = "org.postgresql.Driver", url = o("jdbc-url"), user = o.getOrElse("db-user", "postgres"),
      password = o.getOrElse("db-password", "postgres"), logHandler = None)

  /** Đọc partition daily đã qua quality gate thành các dòng cho serving. */
  def dailyRows(lake: Lake, day: LocalDate): List[DailyRow] =
    Io.lines(lake.partFile(DailyRevenue, day)).drop(1).map(_.split(",", -1)).map(r =>
      DailyRow(day, r(0), BigDecimal(r(1)), r(2).toInt, r(3).toInt)).toList

  /** Quality gate → nạp serving. Tách thành hàm để test e2e gọi trực tiếp. Đọc file nằm TRONG `IO.blocking`: bản đầu gọi `Quality.check` ngay khi
    * DỰNG chương trình, nên `IO(tính staging) *> publish(...)` đọc đĩa trước khi staging kịp được tính (NoSuchFileException — mục 9 README).
    */
  def publish(lake: Lake, db: ServingDb, day: LocalDate, runId: String, now: Instant): IO[Either[List[String], String]] =
    IO.blocking(Quality.check(lake, day)).flatMap {
      case Nil =>
        IO.blocking((dailyRows(lake, day), Io.hash(lake.partFile(DailyRevenue, day))))
          .flatMap((rows, hash) => db.load(day, rows, hash, runId, now)).map(Right(_))
      case problems => IO.pure(Left(problems))
    }

  def run(args: List[String]): IO[ExitCode] =
    val o = opts(args.drop(1))
    val lake = o.get("root").map(r => Lake(Paths.get(r)))
    val runId = o.getOrElse("run-id", UUID.randomUUID().toString.take(8))
    def day(k: String) = o.get(k).flatMap(v => scala.util.Try(LocalDate.parse(v)).toOption)
    def say(s: String) = IO.println(s)
    (args.headOption, lake) match
      case (Some("land-file"), Some(l)) =>
        (o.get("file"), day("ingest-date")) match
          case (Some(f), Some(d)) =>
            val r = FileLanding.land(l, Paths.get(f), d)
            r.rawFile.fold(say(s"quarantined ${r.quarantined.get}: ${r.reason.get}").as(ExitCode(3)))(raw => say(s"landed $raw").as(ExitCode.Success))
          case _ => say("cần --file= --ingest-date=").as(ExitCode(2))
      case (Some("materialize"), Some(l)) =>
        (o.get("asset").flatMap(Assets.byName), day("day")) match
          case (Some(a), Some(d)) =>
            import Lineage.given
            IO.blocking(Orchestrator.materialize(l, a, d, runId)).flatMap(m => say(m.asJson.noSpaces)).as(ExitCode.Success)
          case _ => say("cần --asset=<staging_orders|daily_revenue> --day=YYYY-MM-DD").as(ExitCode(2))
      case (Some("stale"), Some(l)) =>
        // Ngày nào có asset cũ/thiếu THEO LINEAGE — orchestrator không tự biết lô hôm nay chạm ngày cũ nào (README mục 6).
        (day("from"), day("to")) match
          case (Some(f), Some(t)) =>
            val days = Orchestrator.days(f, t).filter(d => Assets.all.exists(a => Orchestrator.status(l, a, d) != Status.Fresh))
            say(days.mkString(" ")).as(ExitCode.Success)
          case _ => say("cần --from= --to=").as(ExitCode(2))
      case (Some("reconcile"), Some(l)) =>
        // Đối soát CẤP RAW: dòng không thuộc ngày nào không bao giờ hiện trong báo cáo theo ngày. Chính sách: KHÔNG chặn publish các ngày hợp lệ
        // (chúng không bị ảnh hưởng), nhưng công bố số lượng + vị trí vào `_quality/raw_reconcile.json` và stdout để Dagster gắn metadata/cảnh báo.
        IO.blocking {
          val r = Reconcile.report(l)
          val json = io.circe.Json.obj("raw_rows" -> r.rawRows.asJson, "assigned" -> r.assigned.values.sum.asJson,
            "assigned_by_day" -> r.assigned.toList.sortBy(_._1).map((d, n) => d.toString -> n).toMap.asJson,
            "unassigned" -> r.unassigned.size.asJson, "unassigned_rows" -> r.unassigned.asJson, "balanced" -> r.balanced.asJson).noSpaces
          Io.writeAtomically(l.root.resolve("_quality").resolve("raw_reconcile.json"), json + "\n")
          (json, r.balanced)
        }.flatMap((json, balanced) => say(json).as(if balanced then ExitCode.Success else ExitCode.Error))
      case (Some("publish"), Some(l)) if o.contains("jdbc-url") =>
        day("day") match
          case Some(d) =>
            val db = ServingDb(xa(o))
            db.migrate *> publish(l, db, d, runId, Instant.now()).flatMap {
              case Right(r)       => say(s"""{"day":"$d","serving":"$r"}""").as(ExitCode.Success)
              case Left(problems) => say(s"quality gate chặn $d: ${problems.mkString("; ")}").as(ExitCode(4))
            }
          case None => say("cần --day=").as(ExitCode(2))
      case (Some("serve"), _) if o.contains("jdbc-url") =>
        val db = ServingDb(xa(o))
        val port = o.get("port").flatMap(_.toIntOption).flatMap(Port.fromInt).getOrElse(Port.fromInt(8085).get)
        db.migrate *> EmberServerBuilder.default[IO].withHost(Host.fromString("127.0.0.1").get).withPort(port)
          .withHttpApp(Api.routes(db, () => Instant.now()).orNotFound).build.useForever.as(ExitCode.Success)
      case _ => say("dùng: land-file|materialize|stale|reconcile|publish|serve --root=<lake> ... (publish/serve cần --jdbc-url=)").as(ExitCode(2))
