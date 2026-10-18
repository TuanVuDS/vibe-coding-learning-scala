package vn.scalaeng.http

import vn.scalaeng.http.json.Json.*

enum Role:
  case Reader, Admin

object Middlewares:
  val RequestIdHeader = "X-Request-Id"

  /** Gắn request id (lấy từ client nếu có, nếu không thì sinh mới) vào request và response — để truy vết log. */
  def requestId(newId: () => String): Middleware = next => req =>
    val id = req.header(RequestIdHeader).filter(_.matches("[A-Za-z0-9._-]{1,64}")).getOrElse(newId())
    next(req.withAttr("requestId", id)).withHeader(RequestIdHeader, id)

  /** Access log có cấu trúc. `clock` trả mili-giây để test đo được thời gian. */
  def accessLog(log: String => Unit, clock: () => Long): Middleware = next => req =>
    val start = clock()
    val res = next(req)
    log(JObj(Vector(
      "request_id" -> JStr(req.attributes.getOrElse("requestId", "-")), "method" -> JStr(req.method.toString),
      "path" -> JStr(req.path), "status" -> JNum(res.status), "duration_ms" -> JNum(clock() - start)
    )).render)
    res

  /** Lưới an toàn cuối: exception -> 500 KHÔNG lộ chi tiết nội bộ ra client (chi tiết đi vào log). */
  def errorHandling(log: String => Unit): Middleware = next => req =>
    try next(req)
    catch case scala.util.control.NonFatal(e) =>
      val id = req.attributes.getOrElse("requestId", "-")
      log(JObj(Vector("level" -> JStr("ERROR"), "request_id" -> JStr(id), "error" -> JStr(e.toString))).render)
      Response.problem(500, "Internal Server Error", s"lỗi không mong đợi, mã tra cứu: $id")

  /** Authentication: xác định "bạn là ai" từ header Authorization: Bearer <token>.
    * Token sai -> 401 ngay. Không có token -> đi tiếp ẩn danh (route công khai vẫn dùng được).
    */
  def authenticate(tokens: Map[String, Role]): Middleware = next => req =>
    req.header("Authorization") match
      case None => next(req)
      case Some(h) if h.startsWith("Bearer ") =>
        tokens.get(h.drop(7).trim) match
          case Some(role) => next(req.withAttr("role", role.toString))
          case None       => unauthorized("token không hợp lệ")
      case Some(_) => unauthorized("chỉ hỗ trợ Authorization: Bearer <token>")

  private def unauthorized(detail: String): Response =
    Response.problem(401, "Unauthorized", detail).withHeader("WWW-Authenticate", "Bearer")

  /** Authorization: "bạn có được làm việc này không". Áp dụng cho từng route. */
  def requireRole(role: Role)(h: (Request, Map[String, String]) => Response): (Request, Map[String, String]) => Response =
    (req, params) =>
      req.attributes.get("role") match
        case None                          => unauthorized("cần đăng nhập")
        case Some(r) if r == role.toString => h(req, params)
        case Some(r)                       => Response.problem(403, "Forbidden", s"vai trò $r không được phép, cần $role")

  /** Ghép middleware: danh sách đầu tiên là lớp NGOÀI CÙNG. */
  def compose(ms: Middleware*): Middleware = ms.foldRight(identity[Handler])((m, acc) => h => m(acc(h)))
