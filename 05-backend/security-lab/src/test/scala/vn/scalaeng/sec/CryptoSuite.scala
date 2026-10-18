package vn.scalaeng.sec

import java.util.Base64
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class CryptoSuite extends CatsEffectSuite:

  test("config: thiếu hoặc quá ngắn JWT_SECRET -> không khởi động; tích lũy mọi lỗi"):
    assertEquals(SecurityConfig.load(Map.empty).left.map(_.size), Left(1))
    assertEquals(
      SecurityConfig.load(Map("JWT_SECRET" -> "ngan", "TOKEN_TTL_MINUTES" -> "999")),
      Left(List("JWT_SECRET phải dài ít nhất 32 ký tự (HS256 cần khóa >= 256 bit)", "TOKEN_TTL_MINUTES phải là số nguyên 1..60"))
    )

  test("Secret không lộ qua toString (log, exception, println config)"):
    assertEquals(Fixtures.config.jwtSecret.toString, "Secret(****)")
    assert(!Fixtures.config.toString.contains("test-secret"), Fixtures.config.toString)

  test("Argon2id: hash tự mô tả tham số, salt ngẫu nhiên, verify đúng/sai"):
    for
      h1 <- Passwords.hash("mat-khau-du-dai-2026")
      h2 <- Passwords.hash("mat-khau-du-dai-2026")
      ok <- Passwords.verify("mat-khau-du-dai-2026", h1)
      no <- Passwords.verify("mat-khau-du-dai-2027", h1)
    yield
      assert(h1.startsWith("$argon2id$v=19$m=19456,t=2,p=1$"), h1)
      assertNotEquals(h1, h2) // cùng mật khẩu, khác salt -> khác hash: không tra bảng cầu vồng được
      assertEquals((ok, no), (true, false))

  test("chính sách mật khẩu"):
    assertEquals(Passwords.policy("ngan", "an@x.vn"), List("mật khẩu phải có ít nhất 15 ký tự"))
    assertEquals(Passwords.policy("password1234567", "an@x.vn"), List("mật khẩu quá phổ biến"))
    assertEquals(Passwords.policy("nguyenvanan-2026", "nguyenvanan@x.vn"), List("mật khẩu không được chứa tên email"))
    assertEquals(Passwords.policy("mot cau dai de nho 2026", "an@x.vn"), Nil)

  private val user = AuthUser(7, "an@x.vn", Role.Customer)

  test("JWT: phát hành rồi xác minh lại được"):
    val tokens = Tokens(Fixtures.config, Fixtures.clock())
    assertEquals(tokens.verify(tokens.issue(user)), Right(user))

  test("JWT: hết hạn sau TTL (đồng hồ giả, không phải chờ 15 phút)"):
    val clock  = Fixtures.clock()
    val tokens = Tokens(Fixtures.config, clock)
    val t      = tokens.issue(user)
    clock.advance(14.minutes)
    assert(tokens.verify(t).isRight)
    clock.advance(2.minutes)
    assertEquals(tokens.verify(t), Left("JwtExpirationException"))

  test("JWT: sửa payload (nâng quyền thành Admin) -> chữ ký sai"):
    val tokens                    = Tokens(Fixtures.config, Fixtures.clock())
    val Array(header, payload, signature) = tokens.issue(user).split('.')
    val decoded                   = String(Base64.getUrlDecoder.decode(payload), "UTF-8")
    val forged                    = Base64.getUrlEncoder.withoutPadding.encodeToString(decoded.replace("Customer", "Admin").getBytes("UTF-8"))
    assert(decoded.contains("Customer"), decoded) // payload chỉ là base64: AI CŨNG ĐỌC ĐƯỢC
    assertEquals(tokens.verify(s"$header.$forged.$signature"), Left("JwtValidationException"))

  test("JWT: token alg=none (không chữ ký) bị từ chối"):
    val tokens  = Tokens(Fixtures.config, Fixtures.clock())
    val payload = tokens.issue(user).split('.')(1)
    val none    = Base64.getUrlEncoder.withoutPadding.encodeToString("""{"alg":"none","typ":"JWT"}""".getBytes("UTF-8"))
    assert(tokens.verify(s"$none.$payload.").isLeft)

  test("JWT: token do issuer khác (cùng secret) phát hành bị từ chối"):
    val other = Tokens(Fixtures.config.copy(jwtIssuer = "he-thong-khac"), Fixtures.clock())
    assertEquals(Tokens(Fixtures.config, Fixtures.clock()).verify(other.issue(user)), Left("issuer không hợp lệ"))

  test("JWT: token ký đúng secret nhưng KHÔNG có exp bị từ chối (thư viện chỉ kiểm tra exp nếu có)"):
    val noExp = JwtCirce.encode(
      JwtClaim(content = """{"email":"an@x.vn","role":"Customer"}""").by(Fixtures.config.jwtIssuer).about("7").withId("t-1"),
      Fixtures.config.jwtSecret.reveal, JwtAlgorithm.HS256
    )
    assertEquals(Tokens(Fixtures.config, Fixtures.clock()).verify(noExp), Left("thiếu exp"))

  test("JWT: mỗi token có jti riêng; verifyClaims trả jti và thời điểm hết hạn"):
    val tokens = Tokens(Fixtures.config, Fixtures.clock())
    val v1     = tokens.verifyClaims(tokens.issue(user)).toOption.get
    val v2     = tokens.verifyClaims(tokens.issue(user)).toOption.get
    assertNotEquals(v1.tokenId, v2.tokenId)
    assertEquals(v1.expiresAt.toString, "2026-10-17T00:15:00Z")
