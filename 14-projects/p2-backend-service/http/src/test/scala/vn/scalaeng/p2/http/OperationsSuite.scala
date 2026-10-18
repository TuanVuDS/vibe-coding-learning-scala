package vn.scalaeng.p2.http

import cats.effect.IO
import org.http4s.{Request,Uri}
import sttp.tapir.*
import sttp.tapir.server.http4s.Http4sServerInterpreter
import vn.scalaeng.p2.service.Bulkhead

class OperationsSuite extends munit.CatsEffectSuite:
  test("Prometheus duration summary có TYPE, sum/count cùng status") {
    for
      metrics <- Metrics.make
      app = metrics.wrap(org.http4s.HttpApp[IO](r => IO.pure(org.http4s.Response[IO](
        if r.uri.path.renderString == "/denied" then org.http4s.Status.Forbidden else org.http4s.Status.Ok))))
      _ <- app(Request[IO]())
      _ <- app(Request[IO]())
      _ <- app(Request[IO](uri=Uri.unsafeFromString("/denied")))
      text <- metrics.render
    yield {
      assert(text.contains("# TYPE http_request_duration_seconds summary\n"))
      assert(text.contains("http_request_duration_seconds_count{status=\"200\"} 2\n"))
      assert(text.contains("http_request_duration_seconds_sum{status=\"200\"} "))
      assert(text.contains("http_requests_total{status=\"200\"} 2\n"))
      assert(text.contains("http_request_duration_seconds_count{status=\"403\"} 1\n"))
      val families = text.linesIterator.filterNot(_.startsWith("#")).map(_.takeWhile(_ != '{').takeWhile(_ != ' ')).toList
      assertEquals(families,List("http_requests_total","http_requests_total",
        "http_request_duration_seconds_sum","http_request_duration_seconds_count",
        "http_request_duration_seconds_sum","http_request_duration_seconds_count","login_failures_total"))
    }
  }
  test("Tapir exception đi qua errorHandler, có request id, không lộ secret") {
    val boom=endpoint.get.in("api" / "boom").out(stringBody).serverLogicSuccess[IO](_ => IO.raiseError[String](new IllegalStateException("private-password")))
    for
      metrics <- Metrics.make
      b <- Bulkhead.make(1)
      routes=Http4sServerInterpreter[IO](Api.options).toRoutes(boom)
      app=Operations.app(routes,IO.pure(true),metrics,b,"")
      response <- app(Request[IO](uri=Uri.unsafeFromString("/api/boom")))
      body <- response.bodyText.compile.string
    yield {assertEquals(response.status.code,500);assert(body.contains("requestId"));assert(!body.contains("private-password"));assert(response.headers.headers.exists(_.name.toString.equalsIgnoreCase("X-Request-ID")))}
  }
  test("readiness không treo vô hạn khi dependency không trả lời") {
    for
      metrics <- Metrics.make
      b <- Bulkhead.make(1)
      app=Operations.app(org.http4s.HttpRoutes.empty[IO],IO.never[Boolean],metrics,b,"")
      response <- app(Request[IO](uri=Uri.unsafeFromString("/health/ready")))
    yield assertEquals(response.status.code,503)
  }
