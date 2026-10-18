package vn.scalaeng.solutions

import vn.scalaeng.http.*
import java.security.MessageDigest

object Ch12Solutions:
  // Bài 2: sort có validate, tie-break theo id để phân trang ổn định
  def sortProducts(ps: List[Product], sort: Option[String]): Either[Response, List[Product]] =
    sort.getOrElse("id") match
      case "id"         => Right(ps.sortBy(_.id))
      case "price_asc"  => Right(ps.sortBy(p => (p.price, p.id)))
      case "price_desc" => Right(ps.sortBy(p => (-p.price, p.id)))
      case other        => Left(Response.problem(400, "Bad Request", s"sort phải là id|price_asc|price_desc, nhận '$other'"))

  // Bài 3: ETag từ SHA-256 của body; middleware chung cho mọi GET trả 200
  def etag(body: String): String =
    val digest = MessageDigest.getInstance("SHA-256").digest(body.getBytes("UTF-8"))
    "\"" + digest.take(8).map(b => f"${b & 0xff}%02x").mkString + "\""

  val conditionalGet: Middleware = next => req =>
    val res = next(req)
    if req.method != Method.GET || res.status != 200 then res
    else
      val tag = etag(res.body)
      val matches = req.header("If-None-Match").exists(_.split(",").map(_.trim).exists(t => t == tag || t == "*"))
      if matches then Response(304, Map("ETag" -> tag)) else res.withHeader("ETag", tag)
