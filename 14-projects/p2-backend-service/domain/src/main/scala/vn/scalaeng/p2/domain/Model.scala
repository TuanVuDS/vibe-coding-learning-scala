package vn.scalaeng.p2.domain

import java.util.Locale

opaque type UserId = Long
object UserId:
  def from(value: Long): Either[String, UserId] = Either.cond(value > 0, value, "userId phải dương")
  private[p2] def assigned(value: Long): UserId = value
  extension (id: UserId) def value: Long = id

opaque type OrderId = Long
object OrderId:
  def from(value: Long): Either[String, OrderId] = Either.cond(value > 0, value, "orderId phải dương")
  private[p2] def assigned(value: Long): OrderId = value
  extension (id: OrderId) def value: Long = id

/** VND có số chữ số thập phân bằng 0: một đơn vị nhỏ nhất = một đồng. Không dùng Double. */
opaque type Money = Long
object Money:
  def fromMinor(value: Long): Either[String, Money] =
    Either.cond(value > 0, value, "totalMinor phải dương (VND)")
  extension (money: Money) def minor: Long = money

enum OrderStatus:
  case Pending, Paid, Shipped, Cancelled
  def canMoveTo(next: OrderStatus): Boolean =
    this == next || ((this, next) match
      case (Pending, Paid | Cancelled) => true
      case (Paid, Shipped) => true
      case _ => false)

final case class User(id: UserId, name: String, email: String)
final case class UserDraft(name: String, email: String)
final case class Order(id: OrderId, userId: UserId, total: Money, status: OrderStatus)

enum DomainError:
  case Forbidden
  case MissingProduct
  case OutOfStock
  case Conflict
  case Invalid(errors: List[String])
  case UnknownUser(id: UserId)
  case UnknownOrder(id: OrderId)
  case DuplicateEmail(email: String)
  case InvalidTransition(from: OrderStatus, to: OrderStatus)

enum UserSort:
  case Id, Name, Email

/** Constructor private: mọi truy vấn công khai phải đi qua Validation.query/page. */
final case class PageRequest private (offset: Int, limit: Int)
object PageRequest:
  def from(offset: Int, limit: Int): Either[List[String], PageRequest] =
    val errors = List(
      Option.when(offset < 0 || offset > 1_000_000)("offset phải trong 0..1000000"),
      Option.when(limit < 1 || limit > 100)("limit phải trong 1..100")
    ).flatten
    if errors.nonEmpty then Left(errors) else Right(PageRequest(offset, limit))

final case class UserQuery(q: Option[String], sort: UserSort, descending: Boolean, page: PageRequest)
final case class Page[A](items: List[A], total: Int, offset: Int, limit: Int)

object Validation:
  def user(name: String, email: String): Either[DomainError, UserDraft] =
    val n = name.trim
    val e = email.trim.toLowerCase(Locale.ROOT)
    val errors = List(
      Option.when(n.isEmpty || n.length > 100)("name phải có 1..100 ký tự"),
      Option.when(e.length > 254 || !e.matches("[^\\s@]+@[^\\s@]+\\.[^\\s@]+"))("email không hợp lệ")
    ).flatten
    if errors.nonEmpty then Left(DomainError.Invalid(errors)) else Right(UserDraft(n, e))

  def order(userId: Long, totalMinor: Long): Either[DomainError, (UserId, Money)] =
    val id = UserId.from(userId)
    val total = Money.fromMinor(totalMinor)
    val errors = id.left.toOption.toList ++ total.left.toOption.toList
    if errors.nonEmpty then Left(DomainError.Invalid(errors))
    else (for i <- id; t <- total yield (i, t)).left.map(e => DomainError.Invalid(List(e)))

  def query(q: Option[String], sort: String, offset: Int, limit: Int): Either[DomainError, UserQuery] =
    val descending = sort.startsWith("-")
    val field = sort.stripPrefix("-") match
      case "id" => Right(UserSort.Id)
      case "name" => Right(UserSort.Name)
      case "email" => Right(UserSort.Email)
      case _ => Left("sort chỉ nhận id, name, email (thêm '-' để giảm dần)")
    val page = PageRequest.from(offset, limit)
    val errors = field.left.toOption.toList ++ page.left.toOption.toList.flatten ++
      Option.when(q.exists(_.length > 100))("q tối đa 100 ký tự").toList
    if errors.nonEmpty then Left(DomainError.Invalid(errors))
    else (for f <- field; p <- page.left.map(_.mkString(", "))
          yield UserQuery(q.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty), f, descending, p))
      .left.map(e => DomainError.Invalid(List(e)))
