package scalaeng.ch08a

/** Union types (A | B): "một trong các kiểu", không cần khai báo cha chung. */
final case class ParseFailure(field: String, raw: String)
final case class RuleViolation(rule: String)

object Unions:
  type ImportError = ParseFailure | RuleViolation

  def describe(e: ImportError): String = e match
    case ParseFailure(f, raw) => s"parse $f: '$raw'"
    case RuleViolation(rule)  => s"vi phạm: $rule"

  /** Input cấu hình có thể là số hoặc chuỗi "30s"/"5m". */
  def timeoutSeconds(raw: Int | String): Either[String, Int] = raw match
    case n: Int                          => Right(n)
    case s: String if s.endsWith("s")    => s.dropRight(1).toIntOption.toRight(s"sai: $s")
    case s: String if s.endsWith("m")    => s.dropRight(1).toIntOption.map(_ * 60).toRight(s"sai: $s")
    case s: String                       => Left(s"không hiểu đơn vị: $s")

/** Intersection types (A & B): giá trị có ĐỒNG THỜI mọi thành viên của A và B. */
trait HasId:
  def id: String
trait HasUpdatedAt:
  def updatedAt: Long

final case class CustomerRow(id: String, name: String, updatedAt: Long) extends HasId, HasUpdatedAt
final case class ProductRow(id: String, sku: String, updatedAt: Long) extends HasId, HasUpdatedAt

object Intersections:
  /** Dedup "giữ bản mới nhất" dùng cho MỌI bảng có id và updatedAt. */
  def latestById[A <: HasId & HasUpdatedAt](rows: List[A]): List[A] =
    rows.groupMapReduce(_.id)(identity)((x, y) => if y.updatedAt > x.updatedAt then y else x)
      .values.toList.sortBy(_.id)

/** Type alias: đặt tên cho kiểu dài, KHÔNG tạo kiểu mới (khác opaque type). */
object Aliases:
  type Row = Map[String, String]
  type Validator = Row => List[String]

  val requireEmail: Validator = row =>
    if row.get("email").exists(_.contains("@")) then Nil else List("email thiếu hoặc sai")
  val requireName: Validator = row =>
    if row.get("name").exists(_.trim.nonEmpty) then Nil else List("name rỗng")

  def validateAll(validators: List[Validator])(row: Row): List[String] =
    validators.flatMap(v => v(row))
