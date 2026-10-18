package vn.scalaeng.http

import vn.scalaeng.http.json.{Json, JsonParser}
import vn.scalaeng.http.json.Json.*
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

final case class Product(id: Long, sku: String, name: String, price: Long):
  def toJson: Json = JObj(Vector("id" -> JNum(id), "sku" -> JStr(sku), "name" -> JStr(name), "price" -> JNum(price)))

final case class NewProduct(sku: String, name: String, price: Long)

enum CreateResult:
  case Created(product: Product)
  case DuplicateSku(existingId: Long)

/** Store trong bộ nhớ, AN TOÀN ĐA LUỒNG: trạng thái bất biến trong AtomicReference, cập nhật bằng compare-and-set.
  * HttpServer xử lý request trên nhiều thread (Chương 16A giải thích chi tiết).
  */
final class ProductStore:
  private final case class State(nextId: Long, byId: Map[Long, Product])
  private val state = AtomicReference(State(1L, Map.empty))

  /** Vòng lặp CAS: tính trạng thái mới từ trạng thái hiện tại; nếu thread khác đã đổi trước thì làm lại. */
  @tailrec private def modify[A](f: State => (State, A)): A =
    val current = state.get()
    val (next, result) = f(current)
    if state.compareAndSet(current, next) then result else modify(f)

  def create(np: NewProduct): CreateResult = modify { s =>
    s.byId.values.find(_.sku == np.sku) match
      case Some(existing) => (s, CreateResult.DuplicateSku(existing.id))
      case None =>
        val p = Product(s.nextId, np.sku, np.name, np.price)
        (State(s.nextId + 1, s.byId.updated(p.id, p)), CreateResult.Created(p))
  }

  def get(id: Long): Option[Product] = state.get().byId.get(id)

  def delete(id: Long): Boolean = modify(s => if s.byId.contains(id) then (s.copy(byId = s.byId - id), true) else (s, false))

  /** Lọc + sắp xếp ổn định theo id + phân trang. Trả (trang hiện tại, tổng số phần tử khớp). */
  def list(q: Option[String], page: Int, size: Int): (List[Product], Int) =
    val all = state.get().byId.values.toList.sortBy(_.id)
      .filter(p => q.forall(t => p.name.toLowerCase.contains(t.toLowerCase)))
    (all.slice((page - 1) * size, page * size), all.size)

object ProductApi:
  private val Base = "/api/v1/products"

  private def validationError(errors: List[(String, String)]): Response =
    Response.problem(422, "Unprocessable Content", "dữ liệu không hợp lệ",
      "errors" -> JArr(errors.toVector.map((f, m) => JObj(Vector("field" -> JStr(f), "message" -> JStr(m))))))

  /** Parse + validate body. Tích lũy MỌI lỗi để client sửa một lần. */
  def decodeNewProduct(body: String): Either[Response, NewProduct] =
    JsonParser.parse(body) match
      case Left(err) => Left(Response.problem(400, "Bad Request", s"body không phải JSON hợp lệ: $err"))
      case Right(JObj(fs)) =>
        val m = fs.toMap
        val sku = m.get("sku") match
          case Some(JStr(s)) if s.matches("[A-Z0-9][A-Z0-9-]{0,31}") => Right(s)
          case Some(JStr(s)) => Left("sku" -> s"'$s' không hợp lệ (A-Z, 0-9, '-', tối đa 32)")
          case _             => Left("sku" -> "bắt buộc, kiểu chuỗi")
        val name = m.get("name") match
          case Some(JStr(s)) if s.trim.nonEmpty && s.trim.length <= 100 => Right(s.trim)
          case Some(JStr(_)) => Left("name" -> "độ dài 1..100")
          case _             => Left("name" -> "bắt buộc, kiểu chuỗi")
        val price = m.get("price") match
          case Some(JNum(n)) if n.isWhole && n > 0 && n <= BigDecimal(10_000_000_000L) => Right(n.toLong)
          case Some(JNum(_)) => Left("price" -> "số nguyên trong khoảng 1..10.000.000.000")
          case _             => Left("price" -> "bắt buộc, kiểu số")
        (sku, name, price) match
          case (Right(s), Right(n), Right(p)) => Right(NewProduct(s, n, p))
          case t => Left(validationError(t.productIterator.collect { case Left((f: String, msg: String)) => (f, msg) }.toList))
      case Right(_) => Left(Response.problem(400, "Bad Request", "body phải là JSON object"))

  private def intParam(req: Request, name: String, default: Int, min: Int, max: Int): Either[Response, Int] =
    req.param(name) match
      case None => Right(default)
      case Some(raw) => raw.toIntOption.filter(v => v >= min && v <= max)
          .toRight(Response.problem(400, "Bad Request", s"tham số $name phải là số nguyên trong [$min, $max], nhận '$raw'"))

  private def idParam(params: Map[String, String]): Either[Response, Long] =
    params.get("id").flatMap(_.toLongOption).filter(_ > 0)
      .toRight(Response.problem(400, "Bad Request", s"id phải là số nguyên dương, nhận '${params.getOrElse("id", "")}'"))

  private def merge(e: Either[Response, Response]): Response = e.fold(identity, identity)

  def routes(store: ProductStore): List[Route] = List(
    Route(Method.GET, "/health")((_, _) => Response.json(200, JObj(Vector("status" -> JStr("ok"))))),

    Route(Method.GET, Base) { (req, _) =>
      merge(for
        page <- intParam(req, "page", 1, 1, 100_000)
        size <- intParam(req, "size", 20, 1, 100)
      yield
        val (items, total) = store.list(req.param("q").filter(_.nonEmpty), page, size)
        Response.json(200, JObj(Vector(
          "items" -> JArr(items.toVector.map(_.toJson)),
          "page" -> JNum(page), "size" -> JNum(size), "total" -> JNum(total)
        ))))
    },

    Route(Method.GET, s"$Base/:id") { (_, params) =>
      merge(idParam(params).map(id =>
        store.get(id).fold(Response.problem(404, "Not Found", s"không có sản phẩm id=$id"))(p => Response.json(200, p.toJson))))
    },

    Route(Method.POST, Base)(Middlewares.requireRole(Role.Admin) { (req, _) =>
      if !req.header("Content-Type").exists(_.toLowerCase.startsWith("application/json")) then
        Response.problem(415, "Unsupported Media Type", "Content-Type phải là application/json")
      else
        merge(decodeNewProduct(req.body).map(np =>
          store.create(np) match
            case CreateResult.Created(p) => Response.json(201, p.toJson).withHeader("Location", s"$Base/${p.id}")
            case CreateResult.DuplicateSku(id) =>
              Response.problem(409, "Conflict", s"sku ${np.sku} đã tồn tại", "existing_id" -> JNum(id))))
    }),

    Route(Method.DELETE, s"$Base/:id")(Middlewares.requireRole(Role.Admin) { (_, params) =>
      merge(idParam(params).map(id =>
        if store.delete(id) then Response.noContent else Response.problem(404, "Not Found", s"không có sản phẩm id=$id")))
    })
  )
