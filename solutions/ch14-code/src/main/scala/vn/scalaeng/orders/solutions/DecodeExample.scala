package vn.scalaeng.orders.solutions

import cats.effect.IO
import org.http4s.HttpApp
import sttp.tapir.*
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.DefaultDecodeFailureHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import vn.scalaeng.orders.{Api, Problem}

object DecodeExample:
  // Bài 6: handler nhận cả status/header của Tapir, không gán cứng 400.
  private val handler = DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
    ValuedEndpointOutput(statusCode.and(headers).and(Api.problemBody), (code, hdrs, Problem.of(code.code, msg))))
  private val options = Http4sServerOptions.customiseInterceptors[IO].decodeFailureHandler(handler).options
  private val secured = endpoint.get.in("protected").securityIn(auth.bearer[String]())
    .errorOut(Api.problemBody).out(stringBody)
    .serverSecurityLogicPure[String, IO](token => Right(token))
    .serverLogic(_ => _ => IO.pure(Right("ok")))
  val app: HttpApp[IO] = Http4sServerInterpreter[IO](options).toRoutes(secured).orNotFound
