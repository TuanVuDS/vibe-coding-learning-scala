package vn.scalaeng.p2.http

import cats.data.Kleisli
import cats.effect.{IO,Ref}
import cats.syntax.all.*
import io.circe.syntax.*
import org.http4s.{HttpApp,HttpRoutes,Request,Response,Status}
import org.http4s.dsl.io.*
import org.http4s.server.middleware.{EntityLimiter,Logger,RequestId}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.concurrent.duration.*
import vn.scalaeng.p2.service.{Bulkhead,Saturated}

final class Metrics private (counts: Ref[IO,Map[(Int,String),Long]], failures: Ref[IO,Long]):
  def loginFailure: IO[Unit] = failures.update(_ + 1)
  def wrap(app: HttpApp[IO]): HttpApp[IO] = Kleisli { req =>
    val start = IO.monotonic
    (start,app(req)).tupled.flatMap { (t,response) =>
      IO.monotonic.flatMap { end => counts.update { c =>
        val status = response.status.code
        c.updated((status,"count"),c.getOrElse((status,"count"),0L)+1)
          .updated((status,"nanos"),c.getOrElse((status,"nanos"),0L)+(end-t).toNanos)
      }}.as(response)
    }
  }
  def render: IO[String] = (counts.get,failures.get).mapN { (c,f) =>
    val statuses = c.keys.map(_._1).toList.distinct.sorted
    val requests = statuses.map(s => s"http_requests_total{status=\"$s\"} ${c((s,"count"))}")
    val durations = statuses.flatMap { s => List(
      s"http_request_duration_seconds_sum{status=\"$s\"} ${c((s,"nanos")).toDouble / 1e9}",
      s"http_request_duration_seconds_count{status=\"$s\"} ${c((s,"count"))}") }
    (List("# TYPE http_requests_total counter") ++ requests ++
      List("# TYPE http_request_duration_seconds summary") ++ durations ++
      List("# TYPE login_failures_total counter",s"login_failures_total $f")).mkString("\n") + "\n"
  }
object Metrics:
  def make: IO[Metrics] = (Ref.of[IO,Map[(Int,String),Long]](Map.empty),Ref.of[IO,Long](0L)).mapN(new Metrics(_,_))

object Operations:
  val MaxRequestBodyBytes: Long = 64 * 1024
  private val logger = Slf4jLogger.getLoggerFromName[IO]("vn.scalaeng.p2.errors")
  private def problem(status: Int,detail: String,id: Option[String] = None): Response[IO] =
    Response[IO](Status.fromInt(status).toOption.get).withEntity(Problem.of(status,detail).copy(requestId=id).asJson.noSpaces)
      .putHeaders(org.http4s.Header.Raw(CIString("Content-Type"),"application/problem+json"))
  def app(api: HttpRoutes[IO], ready: IO[Boolean], metrics: Metrics, bulkhead: Bulkhead, openApi: String): HttpApp[IO] =
    val ops = HttpRoutes.of[IO] {
      case GET -> Root / "health" | GET -> Root / "health" / "live" => Ok("UP")
      case GET -> Root / "health" / "ready" =>
        ready.timeout(1.second).handleError(_ => false).flatMap(ok => if ok then Ok("database=UP") else ServiceUnavailable("database=DOWN"))
      case GET -> Root / "metrics" => metrics.render.flatMap(Ok(_))
      case GET -> Root / "openapi.yaml" => Ok(openApi)
    }
    val routes = ops <+> api
    val routed: HttpApp[IO] = Kleisli { (req: Request[IO]) =>
      routes(req).getOrElse(problem(404,"route không tồn tại"))
    }
    val limited = EntityLimiter.httpApp(routed, limit = MaxRequestBodyBytes)
    val bounded: HttpApp[IO] = Kleisli { (req: Request[IO]) =>
      if req.contentLength.exists(_ > MaxRequestBodyBytes) then IO.pure(problem(413,"body tối đa 65536 byte"))
      else if req.uri.path.renderString.startsWith("/api/") then bulkhead(limited(req)).timeout(5.seconds) else limited(req)
    }
    val handled: HttpApp[IO] = Kleisli { (req: Request[IO]) =>
      val id = req.headers.get(CIString("X-Request-ID")).map(_.head.value)
      bounded(req).handleErrorWith {
        case _: EntityLimiter.EntityTooLarge => IO.pure(problem(413,"body tối đa 65536 byte",id))
        case _: Saturated => IO.pure(problem(503,"dịch vụ bận",id))
        case _: java.util.concurrent.TimeoutException => IO.pure(problem(503,"dịch vụ quá thời gian",id))
        case e => logger.error(e)(s"request_id=${id.getOrElse("-")}").as(problem(500,"lỗi nội bộ; liên hệ hỗ trợ kèm request id",id))
      }
    }
    RequestId.httpApp(Logger.httpApp(logHeaders=true,logBody=false)(metrics.wrap(handled)))
