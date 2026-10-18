package vn.scalaeng.solutions.ch17

import cats.data.Kleisli
import cats.effect.{IO, Ref, Resource}
import fs2.io.file.Path
import fs2.io.net.Network
import fs2.io.net.tls.TLSContext
import java.time.Instant
import org.http4s.{Header, HttpApp, HttpRoutes, Request, Status}
import org.typelevel.ci.CIString
import scala.concurrent.duration.*
import vn.scalaeng.sec.*

/** Bài 1 — RFC 6750: token SAI/HẾT HẠN phải kèm `WWW-Authenticate: Bearer error="invalid_token"`
  * để client biết nên làm mới token chứ không phải hỏi lại mật khẩu.
  */
object InvalidTokenChallenge:
  def apply(routes: HttpRoutes[IO]): HttpRoutes[IO] = Kleisli { (req: Request[IO]) =>
    routes(req).map { resp =>
      val hadToken = req.headers.get(CIString("Authorization")).isDefined
      if resp.status == Status.Unauthorized && hadToken then
        resp.putHeaders(Header.Raw(CIString("WWW-Authenticate"), "Bearer error=\"invalid_token\""))
      else resp
    }
  }

/** Bài 2 — header bảo mật cho API JSON. HSTS chỉ có nghĩa khi đã phục vụ qua HTTPS. */
object SecurityHeaders:
  def apply(httpsOnly: Boolean)(app: HttpApp[IO]): HttpApp[IO] = Kleisli { (req: Request[IO]) =>
    app(req).map { resp =>
      val base = resp.putHeaders(
        Header.Raw(CIString("X-Content-Type-Options"), "nosniff"),
        Header.Raw(CIString("Cache-Control"), "no-store"),
        Header.Raw(CIString("Referrer-Policy"), "no-referrer")
      )
      if httpsOnly then base.putHeaders(Header.Raw(CIString("Strict-Transport-Security"), "max-age=31536000; includeSubDomains"))
      else base
    }
  }

/** Bài 3 — đăng xuất: thu hồi token theo `jti`, kiểm tra SAU khi chữ ký đã được xác minh (cắm vào `ApiDeps.isRevoked`).
  *
  * Bản đầu của lời giải băm CHUỖI token và chặn bằng middleware riêng. Review chéo chỉ ra hai cách vượt qua:
  *  (1) gửi `bearer` viết thường — middleware chỉ nhận đúng `Bearer `, còn Tapir chấp nhận mọi kiểu hoa thường;
  *  (2) đổi ký tự base64url cuối của chữ ký sang ký tự TƯƠNG ĐƯƠNG (khác các bit thừa) — chữ ký vẫn đúng, chuỗi đã khác.
  * Cả hai biến mất khi: một parser duy nhất (Tapir) + so theo `jti` đã nằm trong payload được ký.
  * Giới hạn còn lại: danh sách trong bộ nhớ chỉ đúng với MỘT instance (nhiều instance → Redis/PostgreSQL).
  */
final class Revocation private (revoked: Ref[IO, Map[String, Instant]]):
  def revoke(token: VerifiedToken): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      // Bản ghi chỉ cần sống đến khi token tự hết hạn; dọn bản ghi cũ mỗi lần ghi để Map không lớn mãi.
      revoked.update(m => m.filter((_, exp) => exp.isAfter(now)).updated(token.tokenId, token.expiresAt))
    }

  def isRevoked(token: VerifiedToken): IO[Boolean] = revoked.get.map(_.contains(token.tokenId))

object Revocation:
  def make: IO[Revocation] = Ref.of[IO, Map[String, Instant]](Map.empty).map(new Revocation(_))

/** Bài 4 — giới hạn số lần đăng nhập SAI theo email (chống credential stuffing từ nhiều IP).
  *
  * Chỉ lần SAI mới bị đếm; đăng nhập đúng xóa bộ đếm của email đó. Để N request sai ĐỒNG THỜI không vượt quota,
  * mỗi request phải "giữ chỗ" một suất một cách nguyên tử TRƯỚC khi kiểm tra mật khẩu (giống `FOR UPDATE` ở Chương 15):
  *   giữ chỗ (failures + reserved < max) → kiểm tra mật khẩu → đúng: xóa bộ đếm / sai: đổi suất giữ chỗ thành 1 lần sai.
  * `onError` và `onCancel` trả suất giữ chỗ cả khi kiểm tra bị lỗi hoặc bị hủy.
  */
final class FailureLimiter private (max: Int, window: FiniteDuration, state: Ref[IO, Map[String, FailureLimiter.Entry]]):
  import FailureLimiter.Entry

  def guard(email: String)(attempt: IO[Option[String]]): IO[GuardedResult] =
    val key = Email.canonical(email)
    IO.monotonic.flatMap { now =>
      state.modify { m =>
        val e = m.getOrElse(key, Entry(Nil, 0)).prune(now - window)
        if e.failures.size + e.reserved >= max then
          val retry = e.failures.minOption.map(oldest => (oldest + window) - now).getOrElse(window)
          (m.updated(key, e), Left(retry))
        else (m.updated(key, e.copy(reserved = e.reserved + 1)), Right(()))
      }
    }.flatMap {
      case Left(retry) => IO.pure(GuardedResult.TooManyAttempts(retry))
      case Right(()) =>
        attempt.flatTap {
          case Some(_) => state.update(_ - key) // đúng mật khẩu: xóa lịch sử sai
          case None    => IO.monotonic.flatMap(t => state.update(m => m.updatedWith(key)(_.map(e => Entry(t :: e.failures, e.reserved - 1)))))
        }.onError(_ => state.update(m => m.updatedWith(key)(_.map(e => e.copy(reserved = e.reserved - 1)))))
          .onCancel(state.update(m => m.updatedWith(key)(_.map(e => e.copy(reserved = e.reserved - 1)))))
          .map(_.fold(GuardedResult.Rejected)(GuardedResult.Token(_)))
    }

enum GuardedResult:
  case Token(value: String)
  case Rejected
  case TooManyAttempts(retryAfter: FiniteDuration)

object FailureLimiter:
  private final case class Entry(failures: List[FiniteDuration], reserved: Int):
    def prune(cutoff: FiniteDuration): Entry = copy(failures = failures.filter(_ > cutoff))

  def make(max: Int, window: FiniteDuration): IO[FailureLimiter] =
    Ref.of[IO, Map[String, Entry]](Map.empty).map(new FailureLimiter(max, window, _))

/** Bài 5 — HTTPS bằng keystore PKCS12. Production thường kết thúc TLS ở load balancer/ingress; tự bật TLS trong
  * ứng dụng cần khi yêu cầu mã hóa đến tận service (zero-trust) hoặc chạy không có proxy.
  */
object Tls:
  def context(keystore: java.nio.file.Path, password: String): Resource[IO, TLSContext[IO]] =
    Resource.eval(Network[IO].tlsContext.fromKeyStoreFile(Path.fromNioPath(keystore), password.toCharArray, password.toCharArray))
