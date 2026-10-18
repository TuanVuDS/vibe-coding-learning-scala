package scalaeng.solutions

import java.time.LocalDate
import scala.util.Try

/** Lời giải bài tập Chương 7. */
object Ch07Solutions:

  // Bài 1
  def parsePositiveInt(s: String): Option[Int] =
    s.trim.toIntOption.filter(_ > 0)

  // Bài 2
  final case class User(id: String, managerId: Option[String], email: Option[String])

  def managerEmail(db: Map[String, User], userId: String): Option[String] =
    for
      user      <- db.get(userId)
      managerId <- user.managerId
      manager   <- db.get(managerId)
      email     <- manager.email
    yield email

  // Bài 3: today là tham số -> hàm thuần, test được, backfill được
  enum DateError:
    case Empty
    case BadFormat(raw: String)
    case InFuture(date: LocalDate)

  def parseBirthDate(raw: String, today: LocalDate): Either[DateError, LocalDate] =
    val s = raw.trim
    for
      nonEmpty <- Either.cond(s.nonEmpty, s, DateError.Empty)
      date     <- Try(LocalDate.parse(nonEmpty)).toEither.left.map(_ => DateError.BadFormat(s))
      _        <- Either.cond(!date.isAfter(today), (), DateError.InFuture(date))
    yield date

  // Bài 4: tích lũy lỗi không dùng thư viện ngoài
  enum SignupError:
    case EmptyName
    case InvalidEmail(raw: String)
    case InvalidAge(raw: String)
    case TooYoung(age: Int)

  final case class Signup(name: String, email: String, age: Int)

  def validateSignup(name: String, email: String, age: String): Either[List[SignupError], Signup] =
    import SignupError.*
    val nameE: Either[SignupError, String] =
      Either.cond(name.trim.nonEmpty, name.trim, EmptyName)
    val emailE: Either[SignupError, String] =
      val e = email.trim
      Either.cond(e.indexOf('@') > 0 && e.indexOf('@') < e.length - 1, e.toLowerCase, InvalidEmail(email))
    val ageE: Either[SignupError, Int] =
      age.trim.toIntOption.toRight(InvalidAge(age)).flatMap(a => Either.cond(a >= 13, a, TooYoung(a)))

    (nameE, emailE, ageE) match
      case (Right(n), Right(e), Right(a)) => Right(Signup(n, e, a))
      case _ => Left(List(nameE, emailE, ageE).collect { case Left(err) => err })

  // Bài 5: tồn kho theo sự kiện, báo chỉ số sự kiện lỗi
  enum StockEvent:
    case Received(sku: String, qty: Int)
    case Reserved(sku: String, qty: Int)
    case Released(sku: String, qty: Int)

  enum StockError:
    case NonPositiveQty(eventIndex: Int)
    case Insufficient(eventIndex: Int, sku: String, available: Int, requested: Int)

  def applyAll(initial: Map[String, Int], events: List[StockEvent]): Either[StockError, Map[String, Int]] =
    import StockEvent.*, StockError.*
    def step(stock: Map[String, Int], event: StockEvent, idx: Int): Either[StockError, Map[String, Int]] =
      event match
        case Received(_, q) if q <= 0 => Left(NonPositiveQty(idx))
        case Reserved(_, q) if q <= 0 => Left(NonPositiveQty(idx))
        case Released(_, q) if q <= 0 => Left(NonPositiveQty(idx))
        case Received(sku, q)         => Right(stock.updated(sku, stock.getOrElse(sku, 0) + q))
        case Released(sku, q)         => Right(stock.updated(sku, stock.getOrElse(sku, 0) + q))
        case Reserved(sku, q) =>
          val have = stock.getOrElse(sku, 0)
          if have < q then Left(Insufficient(idx, sku, have, q))
          else Right(stock.updated(sku, have - q))

    events.zipWithIndex.foldLeft[Either[StockError, Map[String, Int]]](Right(initial)):
      case (acc, (event, idx)) => acc.flatMap(step(_, event, idx))
