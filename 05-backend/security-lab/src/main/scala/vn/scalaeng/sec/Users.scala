package vn.scalaeng.sec

import cats.effect.{IO, Ref}
import java.util.Locale

final case class StoredUser(id: Long, email: String, name: String, passwordHash: String, role: Role):
  def auth: AuthUser = AuthUser(id, email, role)

enum RegisterError:
  case Invalid(errors: List[String])
  case EmailTaken

/** MỘT quy tắc chuẩn hóa email dùng ở MỌI nơi: đăng ký, tra cứu, đăng nhập, giới hạn tần suất.
  * Nếu mỗi nơi chuẩn hóa một kiểu, " AN@x.vn" có thể vượt giới hạn theo email nhưng lại đăng nhập được (hoặc ngược lại).
  * `Locale.ROOT`: chuyển chữ thường theo quy tắc cố định, không phụ thuộc ngôn ngữ của máy chủ.
  */
object Email:
  def canonical(raw: String): String = raw.trim.toLowerCase(Locale.ROOT)

/** Kho user trong bộ nhớ (Chương 15 thay bằng PostgreSQL). Chỉ lưu HASH, không bao giờ lưu mật khẩu gốc. */
final class UserStore private (state: Ref[IO, Map[Long, StoredUser]]):
  def findByEmail(email: String): IO[Option[StoredUser]] =
    val key = Email.canonical(email)
    state.get.map(_.values.find(_.email == key))

  def find(id: Long): IO[Option[StoredUser]] = state.get.map(_.get(id))

  def all: IO[List[StoredUser]] = state.get.map(_.values.toList.sortBy(_.id))

  def register(email: String, name: String, password: String, role: Role = Role.Customer): IO[Either[RegisterError, StoredUser]] =
    val key = Email.canonical(email)
    Passwords.policy(password, key) match
      case errors @ (_ :: _) => IO.pure(Left(RegisterError.Invalid(errors)))
      case Nil =>
        // Băm TRƯỚC khi vào `modify`: hàm trong `modify` phải nhanh và thuần (có thể chạy lại khi tranh chấp).
        Passwords.hash(password).flatMap { hash =>
          state.modify { users =>
            if users.values.exists(_.email == key) then (users, Left(RegisterError.EmailTaken))
            else
              val u = StoredUser(users.size + 1L, key, name, hash, role)
              (users.updated(u.id, u), Right(u))
          }
        }

object UserStore:
  def empty: IO[UserStore] = Ref.of[IO, Map[Long, StoredUser]](Map.empty).map(new UserStore(_))

/** Đăng nhập. Hai nguyên tắc:
  *  1. Cùng MỘT thông báo cho "sai email" và "sai mật khẩu" — không cho kẻ tấn công dò email nào đã đăng ký.
  *  2. Khi email không tồn tại vẫn chạy verify trên một hash giả, để thời gian phản hồi hai trường hợp tương đương.
  */
final class AuthService(users: UserStore, tokens: Tokens, dummyHash: String):
  def login(email: String, password: String): IO[Option[String]] =
    users.findByEmail(email).flatMap {
      case Some(u) => Passwords.verify(password, u.passwordHash).map(ok => Option.when(ok)(tokens.issue(u.auth)))
      case None    => Passwords.verify(password, dummyHash).as(None)
    }

object AuthService:
  def make(users: UserStore, tokens: Tokens): IO[AuthService] =
    Passwords.hash("không-phải-mật-khẩu-của-ai").map(new AuthService(users, tokens, _))
