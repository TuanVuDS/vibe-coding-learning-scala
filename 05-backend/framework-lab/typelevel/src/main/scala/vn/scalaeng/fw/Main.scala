package vn.scalaeng.fw

import cats.effect.{IO, IOApp}
import cats.syntax.semigroupk.*
import com.comcast.ip4s.{Port, host}
import org.http4s.HttpRoutes
import org.http4s.dsl.io.*
import org.http4s.ember.server.EmberServerBuilder

object Serve:
  /** Chạy server Ember đến khi bấm Ctrl+C. `Resource` đảm bảo socket được đóng khi dừng (Chương 16B). */
  def apply(name: String, port: Int)(routes: ProductStore => HttpRoutes[IO]): IO[Nothing] =
    ProductStore.empty.flatMap { store =>
      EmberServerBuilder
        .default[IO]
        .withHost(host"127.0.0.1")
        .withPort(Port.fromInt(port).getOrElse(sys.error(s"cổng không hợp lệ: $port")))
        .withHttpApp(routes(store).orNotFound)
        .build
        .evalTap(server => IO.println(s"$name lắng nghe tại ${server.address}"))
        .useForever
    }

object Http4sMain extends IOApp.Simple:
  def run: IO[Unit] = Serve("http4s", 8081)(Http4sApi.routes)

object TapirMain extends IOApp.Simple:
  private val docs: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "docs" / "openapi.yaml" =>
    Ok(TapirApi.openApiYaml)
  }
  def run: IO[Unit] = Serve("tapir", 8082)(store => TapirApi.routes(store) <+> docs)

object PrintOpenApi extends IOApp.Simple:
  def run: IO[Unit] = IO.println(TapirApi.openApiYaml)
