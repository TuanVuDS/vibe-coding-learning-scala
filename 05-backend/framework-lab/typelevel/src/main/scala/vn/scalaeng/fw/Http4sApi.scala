package vn.scalaeng.fw

import cats.effect.IO
import org.http4s.{HttpRoutes, Uri}
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*
import org.http4s.headers.Location

/** Cách 1 — http4s thuần: route là một hàm `Request => IO[Option[Response]]`, viết bằng pattern matching. */
object Http4sApi:
  def routes(store: ProductStore): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "api" / "v1" / "products" / LongVar(id) =>
      store.get(id).flatMap {
        case Some(p) => Ok(p)
        case None    => NotFound(Problem(404, "Not Found", s"không có sản phẩm id=$id"))
      }

    case req @ POST -> Root / "api" / "v1" / "products" =>
      req.attemptAs[NewProduct].value.flatMap {
        case Left(_) =>
          BadRequest(Problem(400, "Bad Request", "body phải là JSON có đủ sku, name, price"))
        case Right(np) =>
          Validation.validate(np) match
            case Left(errors) =>
              UnprocessableContent(Problem(422, "Unprocessable Content", "dữ liệu không hợp lệ", errors))
            case Right(valid) =>
              store.create(valid).flatMap {
                case CreateResult.Created(p) =>
                  Created(p, Location(Uri.unsafeFromString(s"/api/v1/products/${p.id}")))
                case CreateResult.DuplicateSku(existing) =>
                  Conflict(Problem(409, "Conflict", s"sku đã tồn tại (id=$existing)"))
              }
      }
  }
