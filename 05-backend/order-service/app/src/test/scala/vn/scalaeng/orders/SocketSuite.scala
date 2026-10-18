package vn.scalaeng.orders

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{host, port}
import munit.CatsEffectSuite
import org.http4s.{Method, Uri}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import Fixtures.*
import TransportCodecs.given

class SocketSuite extends CatsEffectSuite:
  test("socket thật cổng 0: tạo user, order, cập nhật status, đọc lại và decode failure"):
    val resources = for
      a <- Resource.eval(app)
      server <- EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0").withHttpApp(a).build
      client <- EmberClientBuilder.default[IO].build
    yield (Uri.unsafeFromString(s"http://127.0.0.1:${server.address.getPort}"), client)
    resources.use { (base, client) =>
      def send[A: io.circe.Decoder](method: Method, path: String, body: String = ""): IO[(Int, A)] =
        val r = request(method, path, body).withUri(Uri.unsafeFromString(base.toString + path))
        client.run(r).use(response => read[A](response).map(response.status.code -> _))
      for
        u <- send[UserView](Method.POST, "/api/v1/users", """{"name":"Nguyễn An","email":"an@shop.vn"}""")
        o <- send[OrderView](Method.POST, "/api/v1/users/1/orders", """{"totalMinor":450000}""")
        paid <- send[OrderView](Method.PATCH, "/api/v1/orders/1/status", """{"status":"Paid"}""")
        got <- send[OrderView](Method.GET, "/api/v1/orders/1")
        bad <- send[Problem](Method.GET, "/api/v1/users?limit=abc")
      yield
        assertEquals((u._1, u._2.name), (201, "Nguyễn An"))
        assertEquals((o._1, o._2.totalMinor), (201, 450000L))
        assertEquals(paid, got)
        assertEquals(got._2.status, OrderStatus.Paid)
        assertEquals((bad._1, bad._2.status), (400, 400))
    }
