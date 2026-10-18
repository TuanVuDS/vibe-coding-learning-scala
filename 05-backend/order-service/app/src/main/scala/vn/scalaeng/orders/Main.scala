package vn.scalaeng.orders

import cats.effect.{IO, IOApp}
import com.comcast.ip4s.{host, port}
import org.http4s.ember.server.EmberServerBuilder

object Main extends IOApp.Simple:
  def run: IO[Unit] = InMemoryOrderRepository.empty.flatMap { repo =>
    EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"8084")
      .withHttpApp(Api.httpApp(new OrderService(repo))).build
      .evalTap(server => IO.println(s"orders-api lắng nghe tại ${server.address}"))
      .useForever
  }

object PrintOpenApi extends IOApp.Simple:
  def run: IO[Unit] = IO.println(Api.openApiYaml)
