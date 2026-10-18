package vn.scalaeng.sec

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Port, host}
import org.http4s.ember.server.EmberServerBuilder
import scala.concurrent.duration.*

/** Chạy:  JWT_SECRET=<ít nhất 32 ký tự> scala-cli run . --main-class vn.scalaeng.sec.SecureMain
  * Ctrl+C (SIGINT) hoặc SIGTERM → IOApp hủy fiber chính → Resource đóng server: ngừng nhận kết nối mới, chờ request đang
  * chạy tối đa `shutdownTimeout`, rồi đóng metrics registry. Đó là graceful shutdown.
  */
object SecureMain extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    SecurityConfig.load(sys.env) match
      case Left(errors) =>
        IO.println(errors.mkString("cấu hình không hợp lệ:\n  - ", "\n  - ", "")).as(ExitCode(2))
      case Right(config) =>
        val server =
          for
            built  <- AppWiring.build(config, java.time.Clock.systemUTC())
            _      <- cats.effect.Resource.eval(built.users.register("admin@scalaeng.vn", "Quản trị", "doi-mat-khau-nay-ngay-2026", Role.Admin))
            server <- EmberServerBuilder.default[IO]
                        .withHost(host"127.0.0.1")
                        .withPort(Port.fromInt(sys.env.get("PORT").flatMap(_.toIntOption).getOrElse(8090)).get)
                        .withHttpApp(built.app)
                        .withShutdownTimeout(10.seconds)
                        .build
          yield server
        server
          .evalTap(s => IO.println(s"secure-api lắng nghe tại ${s.address} (config: $config)"))
          .useForever
          .onCancel(IO.println("nhận tín hiệu dừng: đang đóng server..."))
          .as(ExitCode.Success)
