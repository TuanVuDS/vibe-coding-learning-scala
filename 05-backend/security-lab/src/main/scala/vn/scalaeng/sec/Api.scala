package vn.scalaeng.sec

import cats.effect.IO
import io.circe.Codec
import org.http4s.HttpRoutes
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.model.ValuedEndpointOutput

/** Trong OpenAPI, Role là enum chuỗi — khớp với codec JSON ở `Role`. */
given Schema[Role] = Schema.derivedEnumeration[Role].defaultStringBased

final case class Problem(status: Int, title: String, detail: String, errors: List[String] = Nil) derives Codec.AsObject
final case class RegisterRequest(email: String, name: String, password: String) derives Codec.AsObject
final case class LoginRequest(email: String, password: String) derives Codec.AsObject
final case class TokenResponse(accessToken: String, tokenType: String, expiresIn: Long) derives Codec.AsObject
final case class UserView(id: Long, email: String, name: String, role: Role) derives Codec.AsObject

object UserView:
  def of(u: StoredUser): UserView = UserView(u.id, u.email, u.name, u.role)

/** Lỗi của endpoint đăng nhập: 401, hoặc 429 kèm header `Retry-After` (giây). */
enum LoginError:
  case Unauthorized(problem: Problem)
  case TooManyAttempts(problem: Problem, retryAfterSeconds: Long)

/** Những gì Api cần từ bên ngoài. `isRevoked` kiểm tra token ĐÃ XÁC MINH (theo `jti`); mặc định: không thu hồi gì. */
final case class ApiDeps(
    users: UserStore,
    authService: AuthService,
    tokens: Tokens,
    config: SecurityConfig,
    loginLimiter: RateLimiter,
    onLoginFailure: IO[Unit],
    isRevoked: VerifiedToken => IO[Boolean] = _ => IO.pure(false)
)

/** Endpoint có bảo vệ: phần `securityIn` (token) được xử lý TRƯỚC, ra `AuthUser`; logic nghiệp vụ chỉ chạy khi đã xác thực.
  * Authentication (bạn là ai) → 401; Authorization (bạn được làm gì) → 403.
  */
final class Api(deps: ApiDeps):
  import deps.{authService, config, isRevoked, loginLimiter, onLoginFailure, tokens, users}

  private def problem(code: StatusCode) =
    oneOfVariantValueMatcher(code, jsonBody[Problem]) { case p: Problem => p.status == code.code }

  private val base = endpoint.in("api" / "v1")

  /** Tapir parse header `Authorization` (scheme không phân biệt hoa thường, đúng HTTP), xác minh JWT, rồi kiểm tra thu hồi
    * theo `jti` — TẤT CẢ ở một chỗ. Bản đầu kiểm tra thu hồi bằng middleware tự parse header → `bearer` viết thường đi lọt (mục 10.8).
    */
  private val secured =
    base
      .securityIn(auth.bearer[String]())
      .errorOut(oneOf[Problem](problem(StatusCode.Unauthorized), problem(StatusCode.Forbidden), problem(StatusCode.NotFound)))
      .serverSecurityLogic[AuthUser, IO] { token =>
        tokens.verifyClaims(token) match
          case Left(_) => IO.pure(Left(Problem(401, "Unauthorized", "token không hợp lệ hoặc đã hết hạn")))
          case Right(v) =>
            isRevoked(v).map(revoked => if revoked then Left(Problem(401, "Unauthorized", "token đã bị thu hồi")) else Right(v.user))
      }

  private def requireRole(user: AuthUser, role: Role): Either[Problem, Unit] =
    Either.cond(user.role == role, (), Problem(403, "Forbidden", "bạn không có quyền thực hiện thao tác này"))

  val register: ServerEndpoint[Any, IO] =
    base.post.in("auth" / "register").in(jsonBody[RegisterRequest])
      .out(statusCode(StatusCode.Created)).out(jsonBody[UserView])
      .errorOut(oneOf[Problem](problem(StatusCode.Conflict), problem(StatusCode.UnprocessableEntity)))
      .serverLogic { r =>
        users.register(r.email, r.name, r.password).map {
          case Right(u)                            => Right(UserView.of(u))
          case Left(RegisterError.EmailTaken)      => Left(Problem(409, "Conflict", "email đã được đăng ký"))
          case Left(RegisterError.Invalid(errors)) => Left(Problem(422, "Unprocessable Content", "mật khẩu không đạt yêu cầu", errors))
        }
      }

  /** Rate limit và metric nằm TRONG logic của endpoint — sau khi router đã nhận diện đúng endpoint.
    * Bản đầu đặt ở middleware so khớp chuỗi path `/api/v1/auth/login` → `/login/` hay `/%6Cogin` đi lọt (mục 10.7).
    */
  val login: ServerEndpoint[Any, IO] =
    base.post.in("auth" / "login").in(jsonBody[LoginRequest])
      .in(extractFromRequest(_.connectionInfo.remote.map(_.getAddress.getHostAddress).getOrElse("unknown")))
      .out(jsonBody[TokenResponse])
      .errorOut(oneOf[LoginError](
        oneOfVariant(StatusCode.Unauthorized, jsonBody[Problem].mapTo[LoginError.Unauthorized]),
        oneOfVariant(StatusCode.TooManyRequests, jsonBody[Problem].and(header[Long]("Retry-After")).mapTo[LoginError.TooManyAttempts])
      ))
      .serverLogic { (r, clientIp) =>
        loginLimiter.acquire(clientIp).flatMap {
          case Left(wait) =>
            val seconds = math.max(1L, (wait.toMillis + 999) / 1000)
            IO.pure(Left(LoginError.TooManyAttempts(Problem(429, "Too Many Requests", "thử lại sau ít phút"), seconds)))
          case Right(()) =>
            authService.login(r.email, r.password).flatMap {
              case Some(t) => IO.pure(Right(TokenResponse(t, "Bearer", config.tokenTtl.toSeconds)))
              case None    => onLoginFailure.as(Left(LoginError.Unauthorized(Problem(401, "Unauthorized", "email hoặc mật khẩu không đúng"))))
            }
        }
      }

  val me: ServerEndpoint[Any, IO] =
    secured.get.in("me").out(jsonBody[UserView]).serverLogic { user => _ =>
      users.find(user.id).map(_.map(UserView.of).toRight(Problem(404, "Not Found", "tài khoản không còn tồn tại")))
    }

  /** Authorization theo VAI TRÒ: chỉ Admin. */
  val listUsers: ServerEndpoint[Any, IO] =
    secured.get.in("admin" / "users").out(jsonBody[List[UserView]]).serverLogic { user => _ =>
      requireRole(user, Role.Admin) match
        case Left(p)   => IO.pure(Left(p))
        case Right(()) => users.all.map(us => Right(us.map(UserView.of)))
    }

  /** Authorization theo QUYỀN SỞ HỮU: user chỉ xem hồ sơ của chính mình, Admin xem được mọi hồ sơ.
    * Không có kiểm tra này là lỗ hổng IDOR (Insecure Direct Object Reference) — đổi id trên URL là xem được dữ liệu người khác.
    */
  val userById: ServerEndpoint[Any, IO] =
    secured.get.in("users" / path[Long]("id")).out(jsonBody[UserView]).serverLogic { user => id =>
      if user.role != Role.Admin && user.id != id then IO.pure(Left(Problem(403, "Forbidden", "bạn chỉ xem được hồ sơ của mình")))
      else users.find(id).map(_.map(UserView.of).toRight(Problem(404, "Not Found", s"không có user id=$id")))
    }

  val all: List[ServerEndpoint[Any, IO]] = List(register, login, me, listUsers, userById)

  val routes: HttpRoutes[IO] = Http4sServerInterpreter[IO](Api.serverOptions).toRoutes(all)

object Api:
  /** Lỗi decode (JSON hỏng, thiếu header Authorization...) trả Problem JSON. `response` nhận cả STATUS và HEADER mà Tapir
    * đã chọn (401 + `WWW-Authenticate` khi thiếu token), nên `status` trong body luôn khớp status HTTP (mục 10.2).
    */
  private val decodeFailures: DefaultDecodeFailureHandler[IO] =
    DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
      ValuedEndpointOutput(
        statusCode.and(headers).and(jsonBody[Problem]),
        (code, hdrs, Problem(code.code, if code == StatusCode.Unauthorized then "Unauthorized" else "Bad Request", msg))
      )
    )

  /** `exceptionHandler(None)`: exception trong `serverLogic` KHÔNG bị Tapir tự biến thành "Internal server error" dạng text,
    * mà đi tiếp ra ngoài tới `Middleware.errorHandler` — MỘT chỗ duy nhất log lỗi kèm request id và trả Problem 500 (mục 10.10).
    */
  val serverOptions: Http4sServerOptions[IO] =
    Http4sServerOptions.customiseInterceptors[IO]
      .decodeFailureHandler(decodeFailures)
      .exceptionHandler(None)
      .options
