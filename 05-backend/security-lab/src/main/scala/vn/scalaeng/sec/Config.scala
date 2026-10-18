package vn.scalaeng.sec

import scala.concurrent.duration.*

/** Bí mật không bao giờ được in ra log: `toString` luôn che. Muốn dùng giá trị phải gọi `reveal` — dễ tìm khi review. */
final class Secret private (value: String):
  def reveal: String            = value
  override def toString: String = "Secret(****)"

object Secret:
  def apply(value: String): Secret = new Secret(value)

final case class SecurityConfig(
    jwtSecret: Secret,
    jwtIssuer: String,
    tokenTtl: FiniteDuration,
    allowedOrigins: List[String],
    loginAttemptsPerMinute: Int
)

object SecurityConfig:
  /** Đọc từ biến môi trường và tích lũy MỌI lỗi. JWT_SECRET KHÔNG có giá trị mặc định: thiếu là không khởi động. */
  def load(env: Map[String, String]): Either[List[String], SecurityConfig] =
    val secret = env.get("JWT_SECRET") match
      case None                  => Left("thiếu JWT_SECRET")
      case Some(s) if s.length < 32 => Left("JWT_SECRET phải dài ít nhất 32 ký tự (HS256 cần khóa >= 256 bit)")
      case Some(s)               => Right(Secret(s))
    val ttl = env.get("TOKEN_TTL_MINUTES").map(_.toIntOption) match
      case None                         => Right(15.minutes)
      case Some(Some(m)) if m > 0 && m <= 60 => Right(m.minutes)
      case Some(_)                      => Left("TOKEN_TTL_MINUTES phải là số nguyên 1..60")
    val origins  = env.get("CORS_ORIGINS").map(_.split(',').toList.map(_.trim).filter(_.nonEmpty)).getOrElse(Nil)
    val attempts = env.get("LOGIN_ATTEMPTS_PER_MINUTE").flatMap(_.toIntOption).getOrElse(5)
    (secret, ttl) match
      case (Right(s), Right(t)) => Right(SecurityConfig(s, env.getOrElse("JWT_ISSUER", "scalaeng-orders"), t, origins, attempts))
      case (s, t)               => Left(List(s, t).collect { case Left(e) => e })
