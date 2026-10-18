package vn.scalaeng.fwzio

import zio.*
import zio.http.*
import zio.schema.{DeriveSchema, Schema}
import zio.schema.codec.JsonCodec.schemaBasedBinaryCodec

// Cùng domain với bản Typelevel, nhưng codec JSON lấy từ zio-schema thay cho circe.
final case class Product(id: Long, sku: String, name: String, price: Long)
final case class NewProduct(sku: String, name: String, price: Long)
final case class FieldError(field: String, message: String)
final case class Problem(status: Int, title: String, detail: String, errors: List[FieldError] = Nil)

object Product:
  given Schema[Product] = DeriveSchema.gen[Product]
object NewProduct:
  given Schema[NewProduct] = DeriveSchema.gen[NewProduct]
object FieldError:
  given Schema[FieldError] = DeriveSchema.gen[FieldError]
object Problem:
  given Schema[Problem] = DeriveSchema.gen[Problem]

object Validation:
  def validate(np: NewProduct): Either[List[FieldError], NewProduct] =
    val errors = List(
      Option.when(!np.sku.matches("[A-Z0-9-]{3,20}"))(FieldError("sku", "3-20 ký tự A-Z, 0-9 hoặc dấu gạch ngang")),
      Option.when(np.name.trim.isEmpty)(FieldError("name", "không được rỗng")),
      Option.when(np.price <= 0)(FieldError("price", "phải > 0"))
    ).flatten
    if errors.isEmpty then Right(np) else Left(errors)

object ZioApi:
  final case class State(nextId: Long, byId: Map[Long, Product])

  private def json[A: Schema](status: Status, a: A): Response =
    Response(status = status, body = Body.from(a)).addHeader(Header.ContentType(MediaType.application.json))

  /** `Ref` của ZIO đóng vai trò giống `Ref` của Cats Effect. */
  def routes(state: Ref[State]): Routes[Any, Nothing] = Routes(
    Method.GET / "api" / "v1" / "products" / long("id") -> handler { (id: Long, _: Request) =>
      state.get.map(_.byId.get(id) match
        case Some(p) => json(Status.Ok, p)
        case None    => json(Status.NotFound, Problem(404, "Not Found", s"không có sản phẩm id=$id")))
    },
    Method.POST / "api" / "v1" / "products" -> handler { (req: Request) =>
      req.body.to[NewProduct].foldZIO(
        _ => ZIO.succeed(json(Status.BadRequest, Problem(400, "Bad Request", "body phải là JSON có đủ sku, name, price"))),
        np =>
          Validation.validate(np) match
            case Left(errors) =>
              ZIO.succeed(json(Status.UnprocessableEntity, Problem(422, "Unprocessable Content", "dữ liệu không hợp lệ", errors)))
            case Right(valid) =>
              state.modify { s =>
                s.byId.values.find(_.sku == valid.sku) match
                  case Some(existing) =>
                    (json(Status.Conflict, Problem(409, "Conflict", s"sku đã tồn tại (id=${existing.id})")), s)
                  case None =>
                    val p = Product(s.nextId, valid.sku, valid.name, valid.price)
                    (json(Status.Created, p).addHeader(Header.Location(URL.root / "api" / "v1" / "products" / p.id.toString)),
                      State(s.nextId + 1, s.byId.updated(p.id, p)))
              }
      )
    }
  )

  def make: UIO[Routes[Any, Nothing]] = Ref.make(State(1L, Map.empty)).map(routes)

object ZioMain extends ZIOAppDefault:
  def run: ZIO[Any, Throwable, Nothing] =
    ZioApi.make.flatMap(r => Server.serve(r)).provide(Server.defaultWithPort(8083))
