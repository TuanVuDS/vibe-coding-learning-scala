package vn.scalaeng.orders

import cats.effect.IO
import cats.syntax.semigroupk.*
import io.circe.syntax.*
import org.http4s.{Header, HttpApp, HttpRoutes, Response, Status}
import org.http4s.dsl.io.*
import org.typelevel.ci.CIString
import sttp.apispec.openapi.circe.yaml.*
import sttp.model.{MediaType, StatusCode}
import sttp.tapir.*
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.interceptor.exception.DefaultExceptionHandler
import sttp.tapir.server.interceptor.reject.RejectHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import TransportCodecs.given

object Api:
  private val jsonProblem = jsonBody[Problem]
  private val problemFormat = new CodecFormat:
    val mediaType: MediaType = MediaType.unsafeParse("application/problem+json")
  val problemBody: EndpointIO.Body[String, Problem] =
    jsonProblem.copy(codec = jsonProblem.codec.format(problemFormat))

  private def variant(code: StatusCode): EndpointOutput.OneOfVariant[Problem] =
    oneOfVariantValueMatcher(code, problemBody) { case p: Problem => p.status == code.code }
  private def errors(codes: StatusCode*): EndpointOutput.OneOf[Problem, Problem] =
    val all = List(StatusCode.BadRequest, StatusCode.InternalServerError) ++ codes
    oneOf[Problem](variant(all.head), all.tail.map(variant)*)

  private val users = endpoint.in("api" / "v1" / "users")
  private val orders = endpoint.in("api" / "v1" / "orders")
  private val offset = query[Int]("offset").default(0).description("0..1000000; số dòng bỏ qua")
  private val limit = query[Int]("limit").default(20).description("1..100; số dòng tối đa")

  val createUser = users.post.name("createUser").in(oneOfBody(jsonBody[CreateUserRequest]))
    .errorOut(errors(StatusCode.Conflict, StatusCode.UnprocessableEntity, StatusCode.UnsupportedMediaType))
    .out(statusCode(StatusCode.Created)).out(jsonBody[UserView]).out(header[String]("Location"))
  val getUser = users.get.name("getUser").in(path[Long]("userId"))
    .errorOut(errors(StatusCode.NotFound, StatusCode.UnprocessableEntity)).out(jsonBody[UserView])
  val listUsers = users.get.name("listUsers")
    .in(query[Option[String]]("q").description("Chứa trong name hoặc email; không phân biệt hoa thường"))
    .in(query[String]("sort").default("id").description("id, name, email; '-' cho giảm dần; id là khóa phụ"))
    .in(offset).in(limit).errorOut(errors(StatusCode.UnprocessableEntity)).out(jsonBody[UsersPage])
  val createOrder = users.post.name("createOrder").in(path[Long]("userId") / "orders")
    .in(oneOfBody(jsonBody[CreateOrderRequest]))
    .errorOut(errors(StatusCode.NotFound, StatusCode.UnprocessableEntity, StatusCode.UnsupportedMediaType))
    .out(statusCode(StatusCode.Created)).out(jsonBody[OrderView]).out(header[String]("Location"))
  val getOrder = orders.get.name("getOrder").in(path[Long]("orderId"))
    .errorOut(errors(StatusCode.NotFound, StatusCode.UnprocessableEntity)).out(jsonBody[OrderView])
  val listOrders = users.get.name("listOrders").in(path[Long]("userId") / "orders").in(offset).in(limit)
    .errorOut(errors(StatusCode.NotFound, StatusCode.UnprocessableEntity)).out(jsonBody[OrdersPage])
  val updateStatus = orders.patch.name("updateStatus").in(path[Long]("orderId") / "status")
    .in(oneOfBody(jsonBody[StatusRequest]))
    .errorOut(errors(StatusCode.NotFound, StatusCode.Conflict, StatusCode.UnprocessableEntity, StatusCode.UnsupportedMediaType))
    .out(jsonBody[OrderView])

  val endpoints: List[AnyEndpoint] = List(createUser, getUser, listUsers, createOrder, getOrder, listOrders, updateStatus)
  val openApiYaml: String = OpenAPIDocsInterpreter().toOpenAPI(endpoints, "Users and Orders API", "1.0.0").toYaml

  private def result[A, B](io: IO[Either[DomainError, A]])(f: A => B): IO[Either[Problem, B]] =
    io.map(_.left.map(Problem.from).map(f))

  def serverEndpoints(service: OrderService): List[ServerEndpoint[Any, IO]] = List(
    createUser.serverLogic(r => result(service.createUser(r.name, r.email))(u => (Views.user(u), s"/api/v1/users/${u.id.value}"))),
    getUser.serverLogic(id => result(service.getUser(id))(Views.user)),
    listUsers.serverLogic((q, sort, off, lim) => result(service.listUsers(q, sort, off, lim))(Views.users)),
    createOrder.serverLogic((id, r) => result(service.createOrder(id, r.totalMinor))(o => (Views.order(o), s"/api/v1/orders/${o.id.value}"))),
    getOrder.serverLogic(id => result(service.getOrder(id))(Views.order)),
    listOrders.serverLogic((id, off, lim) => result(service.listOrders(id, off, lim))(Views.orders)),
    updateStatus.serverLogic((id, r) => result(service.updateStatus(id, r.status))(Views.order))
  )

  private val decodeFailures = DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
    ValuedEndpointOutput(statusCode.and(headers).and(problemBody), (code, hdrs, Problem.of(code.code, msg))))
  private val exceptions = DefaultExceptionHandler[IO]((code, msg) =>
    ValuedEndpointOutput(statusCode.and(problemBody), (code, Problem.of(code.code, msg))))
  // Method đã được decode sau path: các mismatch này cho biết chính xác Allow của path đó.
  private val rejections = RejectHandler.pure[IO] { ctx =>
    val methods = ctx.failure.failures.flatMap(_.failingInput match
      case EndpointInput.FixedMethod(method, _, _) => Some(method.method)
      case _ => None).distinct.sorted
    Option.when(methods.nonEmpty)(ValuedEndpointOutput(
      statusCode.and(header[String]("Allow")).and(problemBody),
      (StatusCode.MethodNotAllowed, methods.mkString(", "), Problem.of(405, "Method Not Allowed"))))
  }
  private val options: Http4sServerOptions[IO] = Http4sServerOptions.customiseInterceptors[IO]
    .decodeFailureHandler(decodeFailures).exceptionHandler(exceptions).rejectHandler(rejections).options

  private val docs: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "docs" / "openapi.yaml" =>
    Ok(openApiYaml).map(_.putHeaders(Header.Raw(CIString("Content-Type"), "application/yaml; charset=UTF-8")))
  }

  def routes(service: OrderService): HttpRoutes[IO] =
    docs <+> Http4sServerInterpreter[IO](options).toRoutes(serverEndpoints(service))

  // Fallback của http4s cũng cần Problem; orNotFound mặc định có body rỗng.
  def httpApp(service: OrderService): HttpApp[IO] =
    val fallback = HttpRoutes.of[IO] { case _ =>
      IO.pure(Response[IO](Status.NotFound).withEntity(Problem.of(404, "không có route này").asJson.noSpaces)
        .putHeaders(Header.Raw(CIString("Content-Type"), "application/problem+json")))
    }
    (routes(service) <+> fallback).orNotFound
