package vn.scalaeng.http

import vn.scalaeng.http.json.Json
import vn.scalaeng.http.json.Json.*
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8

enum Method:
  case GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS
object Method:
  def parse(s: String): Option[Method] = Method.values.find(_.toString == s)

/** Request bất biến. Header lưu tên chữ thường (HTTP header không phân biệt hoa thường). */
final case class Request(
    method: Method,
    path: String,
    query: Map[String, List[String]] = Map.empty,
    headers: Map[String, String] = Map.empty,
    body: String = "",
    attributes: Map[String, String] = Map.empty   // dữ liệu do middleware gắn vào: request id, role...
):
  def header(name: String): Option[String] = headers.get(name.toLowerCase)
  def param(name: String): Option[String] = query.get(name).flatMap(_.headOption)
  def segments: List[String] = path.split("/").toList.filter(_.nonEmpty)
  def withAttr(key: String, value: String): Request = copy(attributes = attributes.updated(key, value))

final case class Response(status: Int, headers: Map[String, String] = Map.empty, body: String = ""):
  def withHeader(name: String, value: String): Response = copy(headers = headers.updated(name, value))

/** Hai khái niệm trung tâm: handler là HÀM, middleware là hàm biến đổi handler. */
type Handler = Request => Response
type Middleware = Handler => Handler

object Response:
  val JsonType = "application/json; charset=utf-8"
  def json(status: Int, body: Json): Response = Response(status, Map("Content-Type" -> JsonType), body.render)
  def noContent: Response = Response(204)

  /** Lỗi theo chuẩn "Problem Details for HTTP APIs" (RFC 9457). */
  def problem(status: Int, title: String, detail: String, extra: (String, Json)*): Response =
    val body = JObj(Vector("type" -> JStr("about:blank"), "title" -> JStr(title), "status" -> JNum(status), "detail" -> JStr(detail)) ++ extra)
    Response(status, Map("Content-Type" -> "application/problem+json; charset=utf-8"), body.render)

object Query:
  /** "a=1&b=x%20y&a=2" -> Map(a -> List(1, 2), b -> List(x y)). */
  def parse(raw: String): Map[String, List[String]] =
    if raw == null || raw.isEmpty then Map.empty
    else
      raw.split("&").toList.filter(_.nonEmpty).map { kv =>
        val (k, v) = kv.split("=", 2) match
          case Array(k, v) => (k, v)
          case Array(k)    => (k, "")
          case _           => ("", "")
        URLDecoder.decode(k, UTF_8) -> URLDecoder.decode(v, UTF_8)
      }.groupMap(_._1)(_._2)
