package vn.scalaeng.p2.http

import cats.effect.{IO,Resource}
import cats.syntax.all.*
import java.time.Clock
import org.http4s.{Header,HttpApp,Method,Request,Uri}
import org.typelevel.ci.CIString
import io.circe.parser.parse
import scala.concurrent.duration.*
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.security.*
import vn.scalaeng.p2.service.*

final case class HttpFixture(app: HttpApp[IO],store: MemoryStore,auth: AuthService[IO],tokens: Tokens,api: Api[IO])
class ApiSuite extends munit.CatsEffectSuite:
  private def fixture(ready: IO[Boolean] = IO.pure(true)): IO[HttpFixture] = for
    store <- MemoryStore.make
    tokens = new Tokens(new Secret("test-only-secret-1234567890123456789"),Clock.systemUTC())
    auth <- AuthService.make(store,MemoryStore.runner,tokens)
    metrics <- Metrics.make
    limiter <- RateLimiter.make(2,1.minute)
    cache <- TtlCache.make[Long,Option[Product]](1.minute,100)
    bulkhead <- Bulkhead.make(8)
    api = new Api(new Shop(store,MemoryStore.runner),auth,limiter,cache,metrics.loginFailure)
  yield HttpFixture(Operations.app(api.routes,ready,metrics,bulkhead,api.openApi),store,auth,tokens,api)
  private def call(f: HttpFixture,method: Method,path: String,body: String="",token: Option[String]=None,scheme: String="Bearer",contentType: String="application/json"): IO[(Int,String,String)] =
    val request = Request[IO](method,Uri.unsafeFromString(path)).withEntity(body)
      .putHeaders(Header.Raw(CIString("Content-Type"),contentType))
    val req=token.fold(request)(t => request.putHeaders(Header.Raw(CIString("Authorization"),s"$scheme $t")))
    f.app(req).flatMap(r => r.bodyText.compile.string.map(b => (r.status.code,b,r.headers.get(CIString("Content-Type")).map(_.head.value).getOrElse(""))))
  private val password="một câu bí mật dài năm 2026"
  test("body quá 64 KiB trả Problem 413 cả khi không có Content-Length") {
    for
      f <- fixture()
      body = s"""{"name":"${"a" * 65536}","email":"x@example.vn","password":"$password"}"""
      known <- call(f,Method.POST,"/api/v1/auth/register",body)
      request = Request[IO](Method.POST,Uri.unsafeFromString("/api/v1/auth/register"))
        .withBodyStream(fs2.Stream.emits(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)).covary[IO])
        .putHeaders(Header.Raw(CIString("Content-Type"),"application/json"))
      streamed <- f.app(request)
      text <- streamed.bodyText.compile.string
      users <- f.store.listUsers(Validation.query(None,"id",0,20).toOption.get)
    yield {
      assertEquals(known._1,413);assert(known._3.startsWith("application/problem+json"))
      assertEquals(streamed.status.code,413);assert(text.contains("\"status\":413"));assertEquals(users.total,0)
    }
  }
  private def user(f: HttpFixture,email: String="an@example.vn"): IO[(User,String)] = for
    u <- f.auth.register("An",email,password).map(_.toOption.get)
    t <- f.auth.login(email,password).map(_.get)
  yield (u,t)
  test("register/login HTTP, duplicate và malformed JSON là Problem") {
    for
      f <- fixture()
      a <- call(f,Method.POST,"/api/v1/auth/register",s"""{"name":"An","email":"an@example.vn","password":"$password"}""")
      b <- call(f,Method.POST,"/api/v1/auth/register",s"""{"name":"An","email":"AN@example.vn","password":"$password"}""")
      c <- call(f,Method.POST,"/api/v1/auth/login",s"""{"email":"an@example.vn","password":"$password"}""")
      d <- call(f,Method.POST,"/api/v1/auth/register","{")
      method <- call(f,Method.DELETE,"/api/v1/auth/register")
      media <- call(f,Method.POST,"/api/v1/auth/register","{}",contentType="text/plain")
    yield {assertEquals(a._1,201);assertEquals(b._1,409);assertEquals(c._1,200);assert(c._2.contains("accessToken"));assertEquals(d._1,400);assertEquals(method._1,405);assertEquals(media._1,415);assert(method._3.startsWith("application/problem+json"));assert(d._3.startsWith("application/problem+json"));assertEquals(parse(d._2).toOption.get.hcursor.get[Int]("status"),Right(400))}
  }
  test("thiếu token 401; owner và admin; giá do server tính; cache invalidate") {
    for
      f <- fixture()
      a <- user(f)
      b <- user(f,"binh@example.vn")
      _ <- f.store.promote(b._1.id)
      admin <- f.auth.login("binh@example.vn",password).map(_.get)
      no <- call(f,Method.GET,s"/api/v1/users/${a._1.id.value}")
      forbidden <- call(f,Method.GET,s"/api/v1/users/${b._1.id.value}",token=Some(a._2))
      deny <- call(f,Method.POST,"/api/v1/products","""{"sku":"P","name":"Bút","price":10,"stock":5}""",Some(a._2))
      create <- call(f,Method.POST,"/api/v1/products","""{"sku":"P","name":"Bút","price":10,"stock":5}""",Some(admin))
      before <- call(f,Method.GET,"/api/v1/products/1")
      order <- call(f,Method.POST,s"/api/v1/users/${a._1.id.value}/orders","""{"lines":[{"productId":1,"quantity":2}],"totalMinor":1}""",Some(a._2))
      after <- call(f,Method.GET,"/api/v1/products/1")
      read <- call(f,Method.GET,"/api/v1/orders/1",token=Some(admin))
      c <- user(f,"cuong@example.vn")
      cross <- call(f,Method.GET,"/api/v1/orders/1",token=Some(c._2))
      crossWrite <- call(f,Method.POST,s"/api/v1/users/${c._1.id.value}/orders","""{"lines":[{"productId":1,"quantity":1}]}""",Some(a._2))
      other <- call(f,Method.GET,"/api/v1/orders/1",token=Some(f.tokens.issue(UserId.assigned(999))))
    yield {assertEquals(no._1,401);assertEquals(forbidden._1,403);assertEquals(deny._1,403);assertEquals(create._1,201);assert(before._2.contains("\"stock\":5"));assertEquals(order._1,201);assert(order._2.contains("\"totalMinor\":20"));assert(after._2.contains("\"stock\":3"));assertEquals(read._1,200);assertEquals(cross._1,403);assertEquals(crossWrite._1,403);assertEquals(other._1,401)}
  }
  test("logout chặn scheme hoa/thường và mọi biến thể chữ ký cùng jti") {
    for
      f <- fixture()
      a <- user(f)
      out <- call(f,Method.POST,"/api/v1/auth/logout",token=Some(a._2))
      results <- List("Bearer","bearer","bEaReR").traverse(s => call(f,Method.GET,s"/api/v1/users/${a._1.id.value}",token=Some(a._2),scheme=s))
      alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
      token=a._2
      idx=alphabet.indexOf(token.last)
      variants=(0 until 4).map(i => token.dropRight(1)+alphabet.charAt((idx/4)*4+i)).toList
      responses <- variants.traverse(t => call(f,Method.GET,s"/api/v1/users/${a._1.id.value}",token=Some(t)))
    yield {assertEquals(out._1,204);assert(results.forall(_._1==401));assert(responses.forall(_._1==401));assert(variants.exists(f.tokens.verify(_).isRight))}
  }
  test("login aliases chia sẻ quota và metric chỉ đếm login sai") {
    for
      f <- fixture()
      responses <- List("login","login/","%6Cogin").traverse(s => call(f,Method.POST,s"/api/v1/auth/$s","""{"email":"x@example.vn","password":"sai"}"""))
      _ <- call(f,Method.GET,"/api/v1/users/1")
      metrics <- call(f,Method.GET,"/metrics")
    yield {assertEquals(responses.map(_._1),List(401,401,429));assert(metrics._2.contains("login_failures_total 2"))}
  }
  test("readiness timeout; liveness không lệ thuộc DB; route thiếu có Problem") {
    for
      f <- fixture(IO.raiseError(new RuntimeException("DB down")))
      ready <- call(f,Method.GET,"/health/ready")
      live <- call(f,Method.GET,"/health/live")
      missing <- call(f,Method.GET,"/khong-co")
    yield {assertEquals(ready._1,503);assertEquals(live._1,200);assertEquals(missing._1,404);assert(missing._3.startsWith("application/problem+json"))}
  }
  test("OpenAPI từ 14 endpoints; không lộ hash; enum chuỗi và lỗi thật") {
    fixture().map { f =>
      val yaml=f.api.openApi
      assertEquals("operationId:".r.findAllIn(yaml).length,14)
      List("/api/v1/products:","/api/v1/auth/logout:","application/problem+json","Cancelled","bearer").foreach(s => assert(yaml.contains(s),s))
      assert(!yaml.contains("password_hash"))
    }
  }
  test("socket Ember thật và Resource đóng cổng") {
    import com.comcast.ip4s.{host,port}
    import org.http4s.ember.server.EmberServerBuilder
    import org.http4s.ember.client.EmberClientBuilder
    fixture().flatMap { f =>
      val server=EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0").withHttpApp(f.app).withShutdownTimeout(1.second).build
      server.use { s =>
        EmberClientBuilder.default[IO].build.use { client =>
          val uri=Uri.unsafeFromString(s"http://127.0.0.1:${s.address.getPort}/health/live")
          client.run(Request[IO](uri=uri)).use(r => r.bodyText.compile.string.map(body => {
            assertEquals(r.status.code,200);assertEquals(body,"UP")
          }))
        }.as(s.address)
      }.flatMap { address =>
        Resource.fromAutoCloseable(IO.blocking(new java.net.Socket())).use { socket =>
          IO.blocking(socket.connect(address,1000)).attempt.map(result => assert(result.isLeft))
        }
      }
    }
  }
