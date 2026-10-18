package vn.scalaeng.p2.security

import java.time.{Clock, Instant}
import java.util.UUID
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import vn.scalaeng.p2.domain.UserId

final class Secret(val reveal: String):
  override def toString: String = "Secret(****)"
final case class VerifiedToken(userId: UserId, jti: String, expires: Instant)

final class Tokens(secret: Secret, clock: Clock):
  private val jwt = JwtCirce(clock)
  private given Clock = clock
  val ttlSeconds: Long = 900L
  val issuer = "scalaeng-p2"
  def issue(id: UserId, jti: String = UUID.randomUUID().toString): String =
    jwt.encode(JwtClaim().by(issuer).to(Set(issuer)).about(id.value.toString).withId(jti).issuedNow.expiresIn(ttlSeconds),
      secret.reveal, JwtAlgorithm.HS256)
  def verify(raw: String): Either[String, VerifiedToken] =
    jwt.decode(raw,secret.reveal,Seq(JwtAlgorithm.HS256)).toEither.left.map(_.getClass.getSimpleName).flatMap { c =>
      for
        exp <- c.expiration.filter(_ > clock.instant().getEpochSecond).toRight("thiếu exp hoặc hết hạn")
        jti <- c.jwtId.filter(s => scala.util.Try(UUID.fromString(s)).isSuccess).toRight("thiếu jti UUID")
        _ <- Either.cond(c.issuer.contains(issuer) && c.audience.exists(_.contains(issuer)), (), "issuer/audience sai")
        id <- c.subject.flatMap(_.toLongOption).toRight("subject sai").flatMap(UserId.from)
      yield VerifiedToken(id,jti,Instant.ofEpochSecond(exp))
    }
