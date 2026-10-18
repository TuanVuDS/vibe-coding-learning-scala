package vn.scalaeng.sec

import io.circe.{Codec, Decoder, Encoder, parser}
import io.circe.syntax.*
import java.time.{Clock, Instant}
import java.util.UUID
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import scala.util.{Failure, Success}

enum Role:
  case Customer, Admin

object Role:
  /** JSON là chuỗi "Customer"/"Admin". `derives Codec.AsObject` cho enum sẽ ra `{"Customer":{}}` — hình dạng tệ cho API. */
  given Codec[Role] = Codec.from(
    Decoder[String].emap(s => Role.values.find(_.toString == s).toRight(s"vai trò không hợp lệ: $s")),
    Encoder[String].contramap(_.toString)
  )

final case class AuthUser(id: Long, email: String, role: Role)

/** Kết quả xác minh token: người dùng + id duy nhất của token (`jti`) + thời điểm hết hạn.
  * `tokenId` dùng để THU HỒI token: so theo `jti` sau khi đã xác minh chữ ký, KHÔNG so theo chuỗi token
  * (cùng một chữ ký có thể có nhiều cách viết base64url khác nhau — review chéo đã chứng minh, xem mục 10.9 của bài).
  */
final case class VerifiedToken(user: AuthUser, tokenId: String, expiresAt: Instant)

private final case class Claims(email: String, role: Role) derives Codec.AsObject

/** Access token JWT ký HS256. Token KHÔNG mã hóa nội dung: ai cũng đọc được payload (base64) — đừng đặt bí mật vào đó.
  * Chữ ký chỉ chứng minh token do server phát hành và chưa bị sửa.
  */
final class Tokens(config: SecurityConfig, clock: Clock):
  private val jwt       = JwtCirce(clock)
  private val Algorithm = JwtAlgorithm.HS256
  private given Clock   = clock

  /** `tokenId` mặc định là UUID ngẫu nhiên; test truyền giá trị cố định. */
  def issue(user: AuthUser, tokenId: String = UUID.randomUUID().toString): String =
    val claim = JwtClaim(content = Claims(user.email, user.role).asJson.noSpaces)
      .by(config.jwtIssuer)
      .about(user.id.toString)
      .withId(tokenId)
      .issuedNow
      .expiresIn(config.tokenTtl.toSeconds)
    jwt.encode(claim, config.jwtSecret.reveal, Algorithm)

  /** Chỉ chấp nhận ĐÚNG HS256 (chặn `alg: none`), kiểm tra chữ ký, và BẮT BUỘC có `exp`, `jti`, đúng issuer.
    * Thư viện chỉ kiểm tra `exp` NẾU CÓ — token không có `exp` vẫn "hợp lệ" với thư viện, nên ta phải tự đòi.
    */
  def verifyClaims(token: String): Either[String, VerifiedToken] =
    jwt.decode(token, config.jwtSecret.reveal, Seq(Algorithm)) match
      case Failure(e) => Left(e.getClass.getSimpleName)
      case Success(claim) =>
        for
          exp    <- claim.expiration.toRight("thiếu exp")
          jti    <- claim.jwtId.filter(_.nonEmpty).toRight("thiếu jti")
          _      <- Either.cond(claim.issuer.contains(config.jwtIssuer), (), "issuer không hợp lệ")
          id     <- claim.subject.flatMap(_.toLongOption).toRight("thiếu subject")
          claims <- parser.decode[Claims](claim.content).left.map(_ => "payload không hợp lệ")
        yield VerifiedToken(AuthUser(id, claims.email, claims.role), jti, Instant.ofEpochSecond(exp))

  def verify(token: String): Either[String, AuthUser] = verifyClaims(token).map(_.user)
