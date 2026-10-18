package vn.scalaeng.orders

import cats.effect.IO
import io.circe.{Decoder, parser}
import org.http4s.{Header, HttpApp, Method, Request, Response, Uri}
import org.typelevel.ci.CIString

object Fixtures:
  def service: IO[OrderService] = InMemoryOrderRepository.empty.map(new OrderService(_))
  def app: IO[HttpApp[IO]] = service.map(Api.httpApp)
  def request(method: Method, path: String, body: String = ""): Request[IO] =
    val r = Request[IO](method, Uri.unsafeFromString(path))
    if body.isEmpty then r else r.withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"), "application/json"))
  def read[A: Decoder](r: Response[IO]): IO[A] =
    r.bodyText.compile.string.flatMap(s => IO.fromEither(parser.decode[A](s)))
  def right[A](e: Either[DomainError, A]): IO[A] = IO.fromEither(e.left.map(x => new AssertionError(x.toString)))
