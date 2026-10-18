package vn.scalaeng.sec

import cats.data.Kleisli
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import io.circe.Json
import io.prometheus.client.Counter
import org.http4s.{HttpApp, HttpRoutes, Request}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.middleware.{CORS, Logger, Metrics, RequestId}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.concurrent.duration.*

/** Health check: `live` = tiến trình còn chạy (orchestrator restart nếu fail);
  * `ready` = sẵn sàng nhận traffic (load balancer ngừng gửi request nếu fail, KHÔNG restart).
  */
final class Health(checks: List[(String, IO[Boolean])]):
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "health" / "live" => Ok(Json.obj("status" -> Json.fromString("UP")))
    case GET -> Root / "health" / "ready" =>
      checks.traverse((name, check) => check.timeout(1.second).handleError(_ => false).map(name -> _)).flatMap { results =>
        val body = Json.obj(results.map((n, ok) => n -> Json.fromString(if ok then "UP" else "DOWN"))*)
        if results.forall(_._2) then Ok(body) else ServiceUnavailable(body)
      }
  }

object Middleware:
  private val logger = Slf4jLogger.getLoggerFromName[IO]("vn.scalaeng.sec.errors")

  /** Mọi exception chưa xử lý → log ĐẦY ĐỦ ở server (kèm request id), trả client Problem 500 KHÔNG có stack trace. */
  def errorHandler(app: HttpApp[IO]): HttpApp[IO] = Kleisli { (req: Request[IO]) =>
    app(req).handleErrorWith { e =>
      val requestId = req.headers.get(CIString("X-Request-ID")).map(_.head.value).getOrElse("-")
      logger.error(e)(s"lỗi không mong đợi request_id=$requestId ${req.method} ${req.uri.path}") *>
        InternalServerError(Json.obj(
          "status"    -> Json.fromInt(500),
          "title"     -> Json.fromString("Internal Server Error"),
          "detail"    -> Json.fromString("đã có lỗi xảy ra; liên hệ hỗ trợ kèm mã request"),
          "requestId" -> Json.fromString(requestId)
        ))
    }
  }

  /** CORS chỉ cho các origin được cấu hình. Danh sách rỗng = không cho trình duyệt ở origin khác gọi API. */
  def cors(allowedOrigins: List[String])(routes: HttpRoutes[IO]): HttpRoutes[IO] =
    val allowed = allowedOrigins.toSet
    CORS.policy
      .withAllowOriginHostCi(origin => allowed.contains(origin.toString))
      .withAllowMethodsIn(Set(GET, POST))
      .withAllowHeadersIn(Set(CIString("Authorization"), CIString("Content-Type")))
      .withAllowCredentials(false)
      .withMaxAge(10.minutes)
      .apply(routes)

  /** Log mỗi request/response. `Authorization`, `Cookie`, `Set-Cookie` được CHE theo mặc định của http4s. */
  def logging(logAction: Option[String => IO[Unit]])(app: HttpApp[IO]): HttpApp[IO] =
    Logger.httpApp(logHeaders = true, logBody = false, logAction = logAction)(app)

final case class Built(app: HttpApp[IO], users: UserStore, dbUp: Ref[IO, Boolean], loginFailures: Counter)

object AppWiring:
  /** Ghép toàn bộ ứng dụng thành một `Resource`: mọi thứ cần đóng (metrics registry, server) được đóng theo thứ tự ngược.
    * `isRevoked` cho phép lời giải bài 3 cắm danh sách thu hồi token vào tầng xác thực.
    */
  def build(
      config: SecurityConfig,
      clock: java.time.Clock,
      logAction: Option[String => IO[Unit]] = None,
      extraRoutes: HttpRoutes[IO] = HttpRoutes.empty,
      isRevoked: VerifiedToken => IO[Boolean] = _ => IO.pure(false)
  ): Resource[IO, Built] =
    for
      metricsSvc <- PrometheusExportService.build[IO]
      ops        <- Prometheus.metricsOps[IO](metricsSvc.collectorRegistry, "http")
      // Metric NGHIỆP VỤ, bổ sung cho metric HTTP chung. Chỉ endpoint login gọi `inc()` (mục 10.6).
      failures    = Counter.build("login_failures_total", "Số lần đăng nhập thất bại").register(metricsSvc.collectorRegistry)
      users      <- Resource.eval(UserStore.empty)
      tokens      = Tokens(config, clock)
      auth       <- Resource.eval(AuthService.make(users, tokens))
      dbUp       <- Resource.eval(Ref.of[IO, Boolean](true))
      limiter    <- Resource.eval(RateLimiter.make(config.loginAttemptsPerMinute, 1.minute))
      api         = Api(ApiDeps(users, auth, tokens, config, limiter, IO(failures.inc()), isRevoked))
      // Readiness ở lab là một CỜ mô phỏng database (`dbUp`). Project 2 thay bằng truy vấn `select 1` thật có timeout.
      health      = Health(List("database" -> dbUp.get))
      apiRoutes   = Metrics[IO](ops)(Middleware.cors(config.allowedOrigins)(api.routes))
      all         = health.routes <+> metricsSvc.routes <+> extraRoutes <+> apiRoutes
      app         = RequestId.httpApp(Middleware.logging(logAction)(Middleware.errorHandler(all.orNotFound)))
    yield Built(app, users, dbUp, failures)
