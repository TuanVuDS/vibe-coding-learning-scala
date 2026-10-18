package vn.scalaeng.p1.model

import java.text.Normalizer

/** Chuẩn hóa Unicode về NFC: "ệ" dựng sẵn và "ệ" tổ hợp trở thành cùng một chuỗi. */
object Text:
  def nfc(s: String): String = Normalizer.normalize(s, Normalizer.Form.NFC)
  def collapse(s: String): String = nfc(s).trim.replaceAll("\\s+", " ")

opaque type Email = String
object Email:
  def parse(raw: String): Either[String, Email] =
    val s = Text.nfc(raw).trim.toLowerCase
    s.split("@", -1).toList match
      case local :: domain :: Nil
          if local.nonEmpty && !s.exists(_.isWhitespace) && domain.contains('.') &&
            !domain.startsWith(".") && !domain.endsWith(".") && !domain.contains("..") =>
        Right(s)
      case _ => Left(s"'$raw' không phải email hợp lệ")
  extension (e: Email) def value: String = e

/** Số di động VN dạng nội địa 10 chữ số. Chấp nhận +84/84, khoảng trắng, dấu chấm, gạch nối, ngoặc. */
opaque type Phone = String
object Phone:
  def parse(raw: String): Either[String, Phone] =
    val cleaned = raw.trim.filterNot(c => c == ' ' || c == '.' || c == '-' || c == '(' || c == ')')
    val local =
      if cleaned.startsWith("+84") then "0" + cleaned.drop(3)
      else if cleaned.startsWith("84") && cleaned.length == 11 then "0" + cleaned.drop(2)
      else cleaned
    if local.length == 10 && local.startsWith("0") && local.forall(_.isDigit) then Right(local)
    else Left(s"'$raw' không phải số điện thoại VN hợp lệ")
  extension (p: Phone) def value: String = p

/** Thành phố chuẩn hóa về tên chính thức; tên không có trong bảng được giữ (đã chuẩn hóa khoảng trắng). */
opaque type City = String
object City:
  val Unknown: City = "UNKNOWN"
  private val aliases: Map[String, String] = Map(
    "hồ chí minh" -> "Hồ Chí Minh", "tp hcm" -> "Hồ Chí Minh", "tp.hcm" -> "Hồ Chí Minh", "hcm" -> "Hồ Chí Minh",
    "ho chi minh" -> "Hồ Chí Minh", "sài gòn" -> "Hồ Chí Minh", "saigon" -> "Hồ Chí Minh",
    "hà nội" -> "Hà Nội", "ha noi" -> "Hà Nội", "hanoi" -> "Hà Nội", "hn" -> "Hà Nội",
    "đà nẵng" -> "Đà Nẵng", "da nang" -> "Đà Nẵng", "danang" -> "Đà Nẵng"
  )
  def parse(raw: String): Either[String, City] =
    val s = Text.collapse(raw)
    if s.isEmpty then Left("thành phố rỗng") else Right(aliases.getOrElse(s.toLowerCase, s))
  extension (c: City) def value: String = c
