package vn.scalaeng.p2.http

import cats.effect.IO
import org.http4s.HttpRoutes
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.{Http4sServerInterpreter,Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import sttp.tapir.server.interceptor.reject.RejectHandler
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.apispec.openapi.circe.yaml.*
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.service.{Shop,TtlCache}
import vn.scalaeng.p2.security.{AuthService,RateLimiter,VerifiedToken}
import JsonCodecs.given

final class Api[F[_]](shop: Shop[F], authentication: AuthService[F], limiter: RateLimiter,
    cache: TtlCache[Long,Option[Product]], onLoginFailure: IO[Unit]):
  private val base = endpoint.in("api" / "v1")
  private val problemBody = Api.problemBody
  private def variant(code: Int) = oneOfVariantValueMatcher(StatusCode(code),problemBody) { case p: Problem => p.status == code }
  private val errors = oneOf[Problem](variant(400),List(401,403,404,409,415,422,429,500,503).map(variant)*)
  private val public = base.errorOut(errors)
  private val secured = base.securityIn(auth.bearer[String]()).errorOut(errors)
    .serverSecurityLogic[(Principal,VerifiedToken),IO](raw => authentication.authenticate(raw).map(_.left.map(_ => Problem.of(401,"token không hợp lệ hoặc đã thu hồi"))))
  private val off = query[Int]("offset").default(0)
  private val lim = query[Int]("limit").default(20)
  private def result[A,B](io: IO[Either[DomainError,A]])(f: A => B): IO[Either[Problem,B]] = io.map(_.left.map(Problem.from).map(f))
  private def page(offset: Int,limit: Int) = PageRequest.from(offset,limit).left.map(es => Problem.of(422,"phân trang không hợp lệ",es))
  private def uid(raw: Long) = UserId.from(raw).left.map(e => Problem.of(422,e))
  private def oid(raw: Long) = OrderId.from(raw).left.map(e => Problem.of(422,e))
  private def valid[A,B](e: Either[Problem,A])(f: A => IO[Either[Problem,B]]): IO[Either[Problem,B]] = e.fold(p => IO.pure(Left(p)),f)

  val register: ServerEndpoint[Any,IO] = public.post.in("auth" / "register").name("register")
    .in(oneOfBody(jsonBody[RegisterRequest])).out(statusCode(StatusCode.Created)).out(jsonBody[UserView])
    .serverLogic(r => result(authentication.register(r.name,r.email,r.password))(Views.user))
  val login: ServerEndpoint[Any,IO] = public.post.in("auth" / "login").name("login")
    .in(oneOfBody(jsonBody[LoginRequest])).in(extractFromRequest(_.connectionInfo.remote.map(_.getAddress.getHostAddress).getOrElse("unknown")))
    .out(jsonBody[TokenView]).serverLogic { (r,ip) =>
      // Endpoint logic, not a path-string middleware: aliases share one quota.
      limiter.acquire(ip).flatMap {
        case Left(_) => IO.pure(Left(Problem.of(429,"thử lại sau một phút")))
        case Right(()) => authentication.login(r.email,r.password).flatMap {
          case Some(t) => IO.pure(Right(TokenView(t,"Bearer",900)))
          case None => onLoginFailure.as(Left(Problem.of(401,"email hoặc mật khẩu không đúng")))
        }
      }
    }
  val logout: ServerEndpoint[Any,IO] = secured.post.in("auth" / "logout").name("logout").out(statusCode(StatusCode.NoContent))
    .serverLogic { case (_,v) => _ => authentication.logout(v).as(Right(())) }
  val getUser: ServerEndpoint[Any,IO] = secured.get.in("users" / path[Long]("userId")).name("getUser").out(jsonBody[UserView])
    .serverLogic { case (p,_) => id => valid(uid(id))(u => result(shop.user(p,u))(Views.user)) }
  val users: ServerEndpoint[Any,IO] = secured.get.in("users").name("listUsers").in(query[Option[String]]("q"))
    .in(query[String]("sort").default("id")).in(off).in(lim).out(jsonBody[UsersPage])
    .serverLogic { case (p,_) => (q,sort,o,l) => valid(Validation.query(q,sort,o,l).left.map(Problem.from)) { query =>
      result(shop.users(p,query))(pg => UsersPage(pg.items.map(Views.user),pg.total,pg.offset,pg.limit))
    }}
  val checkout: ServerEndpoint[Any,IO] = secured.post.in("users" / path[Long]("userId") / "orders").name("checkout")
    .in(oneOfBody(jsonBody[CheckoutRequest])).out(statusCode(StatusCode.Created)).out(jsonBody[OrderView])
    .serverLogic { case (p,_) => (id,r) => valid(uid(id))(u => result(shop.checkout(p,u,r.lines))(Views.order).flatTap(e => if e.isRight then cache.invalidate else IO.unit)) }
  val getOrder: ServerEndpoint[Any,IO] = secured.get.in("orders" / path[Long]("orderId")).name("getOrder").out(jsonBody[OrderView])
    .serverLogic { case (p,_) => id => valid(oid(id))(i => result(shop.order(p,i))(Views.order)) }
  val orders: ServerEndpoint[Any,IO] = secured.get.in("users" / path[Long]("userId") / "orders").name("listOrders")
    .in(off).in(lim).out(jsonBody[OrdersPage]).serverLogic { case (p,_) => (id,o,l) =>
      valid(for u <- uid(id); pg <- page(o,l) yield (u,pg)) { (u,pg) =>
        result(shop.orders(p,u,pg))(r => OrdersPage(r.items.map(Views.order),r.total,r.offset,r.limit))
      }
    }
  val status: ServerEndpoint[Any,IO] = secured.patch.in("orders" / path[Long]("orderId") / "status").name("updateStatus")
    .in(oneOfBody(jsonBody[StatusRequest])).out(jsonBody[OrderView]).serverLogic { case (p,_) => (id,r) =>
      valid(oid(id))(i => result(shop.status(p,i,r.status))(Views.order).flatTap(e => if e.isRight then cache.invalidate else IO.unit))
    }
  val product: ServerEndpoint[Any,IO] = public.get.in("products" / path[Long]("productId")).name("getProduct")
    .out(jsonBody[Product]).serverLogic(id => cache.get(id)(shop.product(id)).map(_.toRight(Problem.of(404,"không có sản phẩm"))))
  val products: ServerEndpoint[Any,IO] = public.get.in("products").name("listProducts").in(query[Option[String]]("q"))
    .in(query[String]("sort").default("price")).in(off).in(lim).out(jsonBody[ProductsPage])
    .serverLogic { (q,s,o,l) =>
      val input = for
        pg <- page(o,l)
        _ <- Either.cond(Set("price","-price").contains(s) && q.forall(_.length <= 100),(),Problem.of(422,"sort: price/-price; q tối đa 100"))
      yield pg
      valid(input)(pg => shop.products(q,s == "-price",pg).map(r => Right(ProductsPage(r.items,r.total,r.offset,r.limit))))
    }
  val createProduct: ServerEndpoint[Any,IO] = secured.post.in("products").name("createProduct").in(oneOfBody(jsonBody[ProductRequest]))
    .out(statusCode(StatusCode.Created)).out(jsonBody[Product]).serverLogic { case (p,_) => r =>
      result(shop.saveProduct(p,None,r.draft))(identity).flatTap(e => if e.isRight then cache.invalidate else IO.unit)
    }
  val updateProduct: ServerEndpoint[Any,IO] = secured.put.in("products" / path[Long]("productId")).name("updateProduct")
    .in(oneOfBody(jsonBody[ProductRequest])).out(jsonBody[Product]).serverLogic { case (p,_) => (id,r) =>
      result(shop.saveProduct(p,Some(id),r.draft))(identity).flatTap(e => if e.isRight then cache.invalidate else IO.unit)
    }
  val deleteProduct: ServerEndpoint[Any,IO] = secured.delete.in("products" / path[Long]("productId")).name("deleteProduct")
    .out(statusCode(StatusCode.NoContent)).serverLogic { case (p,_) => id =>
      result(shop.deleteProduct(p,id))(identity).flatTap(e => if e.isRight then cache.invalidate else IO.unit)
    }
  val all = List(register,login,logout,getUser,users,checkout,getOrder,orders,status,product,products,createProduct,updateProduct,deleteProduct)
  val routes: HttpRoutes[IO] = Http4sServerInterpreter[IO](Api.options).toRoutes(all)
  val openApi: String = OpenAPIDocsInterpreter().toOpenAPI(all.map(_.endpoint),"Scala Engineering P2","1.0.0").toYaml

object Api:
  val problemFormat = new CodecFormat:
    val mediaType = sttp.model.MediaType.ApplicationProblemJson
  val problemBody: EndpointIO.Body[String,Problem] =
    jsonBody[Problem].copy(codec = jsonBody[Problem].codec.format(problemFormat))
  private val rejections = RejectHandler.pure[IO] { ctx =>
    val methods = ctx.failure.failures.flatMap(_.failingInput match
      case EndpointInput.FixedMethod(method,_,_) => Some(method.method)
      case _ => None).distinct.sorted
    Option.when(methods.nonEmpty)(ValuedEndpointOutput(statusCode.and(header[String]("Allow")).and(problemBody),
      (StatusCode.MethodNotAllowed,methods.mkString(", "),Problem.of(405,"Method Not Allowed"))))
  }
  val options: Http4sServerOptions[IO] = Http4sServerOptions.customiseInterceptors[IO]
    .exceptionHandler(None).rejectHandler(rejections)
    .decodeFailureHandler(DefaultDecodeFailureHandler[IO].copy[IO](response = (code,hdrs,msg) =>
      ValuedEndpointOutput(statusCode.and(headers).and(problemBody),
        (code,hdrs,Problem.of(code.code,msg)))))
    .options
