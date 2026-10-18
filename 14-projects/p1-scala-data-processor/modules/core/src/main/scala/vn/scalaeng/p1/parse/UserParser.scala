package vn.scalaeng.p1.parse

import vn.scalaeng.p1.json.Json
import vn.scalaeng.p1.json.Json.*
import vn.scalaeng.p1.model.*

object UserParser:
  private type Fields = Map[String, Json]

  private def required(m: Fields, key: String): Either[String, String] = m.get(key) match
    case Some(JStr(s)) if s.trim.nonEmpty => Right(s)
    case Some(JStr(_))                    => Left("rỗng")
    case None | Some(JNull)               => Left("thiếu trường bắt buộc")
    case Some(other)                      => Left(s"phải là chuỗi, nhận ${other.render}")

  /** Trường tùy chọn: thiếu, null hoặc chuỗi rỗng -> None; có giá trị thì PHẢI hợp lệ. */
  private def optional[A](m: Fields, key: String)(p: String => Either[String, A]): Either[String, Option[A]] =
    m.get(key) match
      case None | Some(JNull)                 => Right(None)
      case Some(JStr(s)) if s.trim.isEmpty    => Right(None)
      case Some(JStr(s))                      => p(s).map(Some(_))
      case Some(other)                        => Left(s"phải là chuỗi hoặc null, nhận ${other.render}")

  def parseUser(position: Int, j: Json): Either[Rejected, User] =
    def reject(issues: List[Issue]) = Rejected(Source.Users, position, j.render, issues)
    j match
      case JObj(fs) =>
        val m: Fields = fs.toMap
        def at[A](key: String)(e: Either[String, A]): Either[Issue, A] = e.left.map(Issue(key, _))
        val all = (
          at("user_id")(required(m, "user_id").flatMap(UserId.parse)),
          at("name")(required(m, "name").map(Text.collapse)),
          at("email")(optional(m, "email")(Email.parse)),
          at("phone")(optional(m, "phone")(Phone.parse)),
          at("city")(required(m, "city").flatMap(City.parse))
        )
        all match
          case (Right(id), Right(name), Right(email), Right(phone), Right(city)) => Right(User(id, name, email, phone, city))
          case other => Left(reject(Issues.collect(other)))
      case other => Left(reject(List(Issue("_item", s"phải là object, nhận ${other.render.take(40)}"))))

  /** Lỗi cấp file (không phải JSON, không phải mảng) là lỗi chặn — trả Left. */
  /** Trả về (JSON gốc đã render, kết quả parse) cho từng phần tử. */
  def parseAll(text: String): Either[String, List[(String, Either[Rejected, User])]] =
    vn.scalaeng.p1.json.JsonParser.parse(text) match
      case Left(err)        => Left(s"users.json không phải JSON hợp lệ: $err")
      case Right(JArr(xs))  => Right(xs.toList.zipWithIndex.map((j, i) => (j.render, parseUser(i + 1, j))))
      case Right(other)     => Left(s"users.json phải là mảng JSON, nhận ${other.render.take(40)}")
