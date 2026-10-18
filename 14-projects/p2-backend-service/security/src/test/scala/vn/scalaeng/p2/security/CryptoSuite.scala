package vn.scalaeng.p2.security

import cats.effect.IO
import java.time.{Clock,Instant,ZoneOffset}
import pdi.jwt.{JwtAlgorithm,JwtCirce,JwtClaim}
import vn.scalaeng.p2.domain.UserId

class CryptoSuite extends munit.CatsEffectSuite:
  private val now = Instant.parse("2026-10-17T00:00:00Z")
  private val clock = Clock.fixed(now,ZoneOffset.UTC)
  private val secret = new Secret("test-only-secret-32-bytes-1234567890")
  private val tokens = new Tokens(secret,clock)
  private val id = UserId.from(1).toOption.get
  test("JWT hợp lệ; sai secret, hết hạn và thiếu exp/jti bị chặn") {
    IO {
      val token = tokens.issue(id)
      assertEquals(tokens.verify(token).map(_.userId),Right(id))
      assert(new Tokens(new Secret("different-test-key-123456789012345"),clock).verify(token).isLeft)
      assert(new Tokens(secret,Clock.fixed(now.plusSeconds(901),ZoneOffset.UTC)).verify(token).isLeft)
      val jwt = JwtCirce(clock)
      val base = JwtClaim(subject=Some("1"),issuer=Some(tokens.issuer),audience=Some(Set(tokens.issuer)))
      assert(tokens.verify(jwt.encode(base.withId(java.util.UUID.randomUUID().toString),secret.reveal,JwtAlgorithm.HS256)).isLeft)
      assert(tokens.verify(jwt.encode(JwtClaim(subject=Some("1"),issuer=Some(tokens.issuer),audience=Some(Set(tokens.issuer)),expiration=Some(now.plusSeconds(60).getEpochSecond)),secret.reveal,JwtAlgorithm.HS256)).isLeft)
    }
  }
  test("Argon2id salt khác nhau, verify đúng/sai và policy") {
    for
      a <- Passwords.hash("một câu mật khẩu rất dài 2026")
      b <- Passwords.hash("một câu mật khẩu rất dài 2026")
      ok <- Passwords.verify("một câu mật khẩu rất dài 2026",a)
      bad <- Passwords.verify("sai",a)
    yield {assert(a.startsWith("$argon2id$"));assertNotEquals(a,b);assert(ok);assert(!bad);assert(Passwords.policy("ngắn","a@b.vn").nonEmpty)}
  }
  test("secret không lộ qua toString") { IO(assert(!secret.toString.contains(secret.reveal))) }
  test("password policy không phụ thuộc locale Thổ Nhĩ Kỳ") {
    IO {
      val before = java.util.Locale.getDefault
      try
        java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"))
        assert(Passwords.policy("PRIVATE phrase long 2026","private@example.vn").contains("mật khẩu không được chứa tên email"))
        assert(Passwords.policy("QWERTYUIOP12345","other@example.vn").contains("mật khẩu quá phổ biến"))
      finally java.util.Locale.setDefault(before)
    }
  }
