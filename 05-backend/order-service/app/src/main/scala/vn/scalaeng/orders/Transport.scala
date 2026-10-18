package vn.scalaeng.orders

import io.circe.{Codec, Decoder, Encoder}
import org.http4s.Status
import sttp.tapir.Schema

// DTO (Data Transfer Object) thuộc biên HTTP. Domain không derive/import circe hay Tapir.
final case class CreateUserRequest(name: String, email: String)
final case class CreateOrderRequest(totalMinor: Long)
final case class StatusRequest(status: OrderStatus)
final case class UserView(id: Long, name: String, email: String)
final case class OrderView(id: Long, userId: Long, totalMinor: Long, currency: String, status: OrderStatus)
final case class UsersPage(items: List[UserView], total: Int, offset: Int, limit: Int)
final case class OrdersPage(items: List[OrderView], total: Int, offset: Int, limit: Int)
final case class Problem(`type`: String, title: String, status: Int, detail: String, errors: List[String])

object Problem:
  def of(status: Int, detail: String, errors: List[String] = Nil): Problem =
    Problem("about:blank", Status.fromInt(status).fold(_ => "Error", _.reason), status, detail, errors)
  def from(error: DomainError): Problem = error match
    case DomainError.Invalid(errors) => of(422, "dữ liệu không hợp lệ", errors)
    case DomainError.UnknownUser(id) => of(404, s"không có user id=${id.value}")
    case DomainError.UnknownOrder(id) => of(404, s"không có order id=${id.value}")
    case DomainError.DuplicateEmail(_) => of(409, "email đã tồn tại")
    case DomainError.InvalidTransition(from, to) => of(409, s"không thể chuyển $from sang $to")

object Views:
  def user(u: User): UserView = UserView(u.id.value, u.name, u.email)
  def order(o: Order): OrderView = OrderView(o.id.value, o.userId.value, o.total.minor, "VND", o.status)
  def users(p: Page[User]): UsersPage = UsersPage(p.items.map(user), p.total, p.offset, p.limit)
  def orders(p: Page[Order]): OrdersPage = OrdersPage(p.items.map(order), p.total, p.offset, p.limit)

object TransportCodecs:
  given Encoder[OrderStatus] = Encoder.encodeString.contramap(_.toString)
  given Decoder[OrderStatus] = Decoder.decodeString.emap(s =>
    OrderStatus.values.find(_.toString == s).toRight("status chỉ nhận Pending, Paid, Shipped, Cancelled"))
  given Schema[OrderStatus] = Schema.derivedEnumeration[OrderStatus].defaultStringBased
  given Codec.AsObject[CreateUserRequest] = Codec.AsObject.derived
  given Codec.AsObject[CreateOrderRequest] = Codec.AsObject.derived
  given Codec.AsObject[StatusRequest] = Codec.AsObject.derived
  given Codec.AsObject[UserView] = Codec.AsObject.derived
  given Codec.AsObject[OrderView] = Codec.AsObject.derived
  given Codec.AsObject[UsersPage] = Codec.AsObject.derived
  given Codec.AsObject[OrdersPage] = Codec.AsObject.derived
  given Codec.AsObject[Problem] = Codec.AsObject.derived
