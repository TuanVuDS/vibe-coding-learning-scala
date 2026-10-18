package vn.scalaeng.fw

import cats.effect.IO
import org.http4s.HttpRoutes
import sttp.apispec.openapi.circe.yaml.*
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter

/** Cách 2 — Tapir: endpoint là DỮ LIỆU (đầu vào, đầu ra, lỗi). Từ cùng một mô tả, Tapir sinh
  * (a) route chạy trên http4s và (b) tài liệu OpenAPI. Logic nghiệp vụ không đổi.
  */
object TapirApi:
  private def problem(code: StatusCode): EndpointOutput.OneOfVariant[Problem] =
    oneOfVariantValueMatcher(code, jsonBody[Problem]) { case p: Problem => p.status == code.code }

  // Mỗi endpoint chỉ khai báo những lỗi NÓ thực sự trả về, để tài liệu OpenAPI không nói sai.
  private def problems(first: StatusCode, rest: StatusCode*): EndpointOutput.OneOf[Problem, Problem] =
    oneOf[Problem](problem(first), rest.map(problem)*)

  private val products = endpoint.in("api" / "v1" / "products")

  val getProduct: PublicEndpoint[Long, Problem, Product, Any] =
    products.get
      .in(path[Long]("id"))
      .errorOut(problems(StatusCode.NotFound))
      .out(jsonBody[Product])
      .description("Lấy một sản phẩm theo id")

  val createProduct: PublicEndpoint[NewProduct, Problem, (Product, String), Any] =
    products.post
      .in(jsonBody[NewProduct])
      .errorOut(problems(StatusCode.Conflict, StatusCode.UnprocessableEntity))
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[Product])
      .out(header[String]("Location"))
      .description("Tạo sản phẩm mới; sku phải duy nhất")

  def serverEndpoints(store: ProductStore): List[ServerEndpoint[Any, IO]] = List(
    getProduct.serverLogic(id => store.get(id).map(_.toRight(Problem(404, "Not Found", s"không có sản phẩm id=$id")))),
    createProduct.serverLogic { np =>
      Validation.validate(np) match
        case Left(errors) => IO.pure(Left(Problem(422, "Unprocessable Content", "dữ liệu không hợp lệ", errors)))
        case Right(valid) =>
          store.create(valid).map {
            case CreateResult.Created(p)             => Right((p, s"/api/v1/products/${p.id}"))
            case CreateResult.DuplicateSku(existing) => Left(Problem(409, "Conflict", s"sku đã tồn tại (id=$existing)"))
          }
    }
  )

  def routes(store: ProductStore): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(serverEndpoints(store))

  val openApiYaml: String =
    OpenAPIDocsInterpreter().toOpenAPI(List(getProduct, createProduct), "Products API", "1.0.0").toYaml
