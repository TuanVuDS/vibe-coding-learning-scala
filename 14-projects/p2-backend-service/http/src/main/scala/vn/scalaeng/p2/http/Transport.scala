package vn.scalaeng.p2.http

import io.circe.{Codec, Decoder, Encoder}
import org.http4s.Status
import sttp.tapir.Schema
import vn.scalaeng.p2.domain.*

final case class RegisterRequest(name: String,email: String,password: String) derives Codec.AsObject
final case class LoginRequest(email: String,password: String) derives Codec.AsObject
final case class TokenView(accessToken: String,tokenType: String,expiresIn: Long) derives Codec.AsObject
final case class ProductRequest(sku: String,name: String,price: Long,stock: Int) derives Codec.AsObject:
  def draft: ProductDraft = ProductDraft(sku,name,price,stock)
final case class CheckoutRequest(lines: List[Line])
final case class StatusRequest(status: OrderStatus)
final case class UserView(id: Long,name: String,email: String) derives Codec.AsObject
final case class OrderView(id: Long,userId: Long,totalMinor: Long,status: OrderStatus)
final case class UsersPage(items: List[UserView],total: Int,offset: Int,limit: Int) derives Codec.AsObject
final case class OrdersPage(items: List[OrderView],total: Int,offset: Int,limit: Int)
final case class ProductsPage(items: List[Product],total: Int,offset: Int,limit: Int)
final case class Problem(`type`: String,title: String,status: Int,detail: String,errors: List[String] = Nil,requestId: Option[String] = None) derives Codec.AsObject
object Problem:
  def of(status: Int,detail: String,errors: List[String] = Nil): Problem =
    Problem("about:blank",Status.fromInt(status).toOption.map(_.reason).getOrElse("Error"),status,detail,errors)
  def from(e: DomainError): Problem = e match
    case DomainError.Invalid(es) => of(422,"dữ liệu không hợp lệ",es)
    case DomainError.Forbidden => of(403,"không có quyền")
    case DomainError.UnknownUser(_) => of(404,"không có user")
    case DomainError.UnknownOrder(_) => of(404,"không có order")
    case DomainError.MissingProduct => of(404,"không có sản phẩm hoạt động hoặc SKU đã đổi")
    case DomainError.DuplicateEmail(_) => of(409,"email đã tồn tại")
    case DomainError.OutOfStock => of(409,"không đủ tồn kho")
    case DomainError.Conflict => of(409,"SKU đã tồn tại")
    case DomainError.InvalidTransition(_,_) => of(409,"không thể chuyển trạng thái")
object Views:
  def user(u: User): UserView = UserView(u.id.value,u.name,u.email)
  def order(o: Order): OrderView = OrderView(o.id.value,o.userId.value,o.total.minor,o.status)
object JsonCodecs:
  given Codec[OrderStatus] = Codec.from(
    Decoder.decodeString.emap(s => OrderStatus.values.find(_.toString == s).toRight("status lạ")),
    Encoder.encodeString.contramap(_.toString))
  given Schema[OrderStatus] = Schema.derivedEnumeration[OrderStatus].defaultStringBased
  given Codec.AsObject[Line] = Codec.AsObject.derived
  given Codec.AsObject[Product] = Codec.AsObject.derived
  given Codec.AsObject[CheckoutRequest] = Codec.AsObject.derived
  given Codec.AsObject[StatusRequest] = Codec.AsObject.derived
  given Codec.AsObject[OrderView] = Codec.AsObject.derived
  given Codec.AsObject[OrdersPage] = Codec.AsObject.derived
  given Codec.AsObject[ProductsPage] = Codec.AsObject.derived
