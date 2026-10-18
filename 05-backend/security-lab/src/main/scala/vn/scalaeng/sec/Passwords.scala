package vn.scalaeng.sec

import cats.effect.IO
import com.password4j.{Argon2Function, Password}
import com.password4j.types.Argon2

/** Băm mật khẩu bằng Argon2id. Tham số theo OWASP Password Storage Cheat Sheet:
  * bộ nhớ 19 MiB (19456 KiB), 2 vòng lặp, song song 1. Kết quả là một chuỗi tự mô tả
  * (`$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`), nên đổi tham số sau này vẫn kiểm tra được hash cũ.
  */
object Passwords:
  private val argon2id = Argon2Function.getInstance(19456, 2, 1, 32, Argon2.ID)

  /** Chậm CÓ CHỦ Ý (~vài chục ms) và tốn RAM để đoán thử hàng loạt rất đắt → chạy trên blocking pool (Chương 16B). */
  def hash(plain: String): IO[String] =
    IO.blocking(Password.hash(plain).addRandomSalt(16).`with`(argon2id).getResult)

  def verify(plain: String, stored: String): IO[Boolean] =
    IO.blocking(Password.check(plain, stored).`with`(Argon2Function.getInstanceFromHash(stored)))

  /** Chính sách tối thiểu theo NIST SP 800-63B-4: mật khẩu là yếu tố xác thực DUY NHẤT phải dài ít nhất 15 ký tự;
    * độ dài quan trọng hơn quy tắc thành phần — KHÔNG ép "phải có ký tự đặc biệt".
    */
  def policy(plain: String, email: String): List[String] =
    List(
      Option.when(plain.length < 15)("mật khẩu phải có ít nhất 15 ký tự"),
      Option.when(plain.length > 128)("mật khẩu tối đa 128 ký tự"),
      Option.when(Common.contains(plain.toLowerCase))("mật khẩu quá phổ biến"),
      Option.when(plain.toLowerCase.contains(email.takeWhile(_ != '@').toLowerCase) && email.indexOf('@') > 2)(
        "mật khẩu không được chứa tên email"
      )
    ).flatten

  /** Danh sách minh họa. Thực tế dùng danh sách mật khẩu đã bị lộ (hàng triệu mục) hoặc dịch vụ kiểm tra k-anonymity. */
  private val Common = Set("123456789012345", "password1234567", "matkhau12345678", "qwertyuiop12345", "abc123456789012")
