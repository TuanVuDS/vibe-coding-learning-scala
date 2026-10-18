package vn.scalaeng.solutions.ch13

import cats.effect.IO
import cats.syntax.semigroupk.*
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import vn.scalaeng.fw.{Http4sApi, Problem, Product, ProductStore, TapirApi}

/** Bài 2 — liệt kê sản phẩm, lọc theo `?q=`. */
object ListProducts:
  private object Q extends OptionalQueryParamDecoderMatcher[String]("q")

  def http4s(store: ProductStore): HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "api" / "v1" / "products" :? Q(q) => store.list(q).flatMap(Ok(_))
  }

  val endpointDef: PublicEndpoint[Option[String], Unit, List[Product], Any] =
    endpoint.get
      .in("api" / "v1" / "products")
      .in(query[Option[String]]("q").description("lọc theo tên, không phân biệt hoa thường"))
      .out(jsonBody[List[Product]])

  def tapir(store: ProductStore): HttpRoutes[IO] =
    Http4sServerInterpreter[IO]().toRoutes(endpointDef.serverLogicSuccess(store.list))

/** Bài 3 — http4s: id không phải số trả 400 dạng Problem thay vì 404 "Not found". */
object BadIdAs400:
  private val fallback: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "api" / "v1" / "products" / raw =>
    BadRequest(Problem(400, "Bad Request", s"id phải là số nguyên, nhận được '$raw'"))
  }

  /** Route chính đứng TRƯỚC: chỉ khi `LongVar` không khớp thì mới rơi xuống fallback. */
  def routes(store: ProductStore): HttpRoutes[IO] = Http4sApi.routes(store) <+> fallback

/** Bài 4 — Tapir: lỗi decode (JSON hỏng, path sai kiểu) trả Problem JSON thay vì text/plain.
  *
  * Chỉ thay cách hiển thị của DECODE FAILURE, và lấy status từ chính Tapir (`response` nhận status + header đã chọn).
  * Bản đầu dùng `defaultHandlers(msg => Problem(400, ...))`: hàm đó thay renderer của CẢ decode failure, reject (405...)
  * và exception, nhưng chỉ nhận thông điệp → `PUT /api/v1/products` trả HTTP 405 với body `"status":400` (review chéo, M1).
  */
object TapirProblemOnDecodeFailure:
  private val decodeFailures: DefaultDecodeFailureHandler[IO] =
    DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
      ValuedEndpointOutput(statusCode.and(headers).and(jsonBody[Problem]), (code, hdrs, Problem(code.code, "Bad Request", msg)))
    )

  private val options: Http4sServerOptions[IO] =
    Http4sServerOptions.customiseInterceptors[IO].decodeFailureHandler(decodeFailures).options

  def routes(store: ProductStore): HttpRoutes[IO] =
    Http4sServerInterpreter[IO](options).toRoutes(TapirApi.serverEndpoints(store))
