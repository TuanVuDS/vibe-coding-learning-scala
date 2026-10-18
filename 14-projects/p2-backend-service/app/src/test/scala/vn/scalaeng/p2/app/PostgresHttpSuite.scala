package vn.scalaeng.p2.app

import cats.effect.{IO,Resource}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import io.circe.parser.parse
import java.time.Clock
import org.http4s.{Header,HttpApp,Method,Request,Uri}
import org.typelevel.ci.CIString
import org.typelevel.doobie.implicits.*
import scala.concurrent.duration.*
import vn.scalaeng.p2.db.*
import vn.scalaeng.p2.domain.Product
import vn.scalaeng.p2.http.*
import vn.scalaeng.p2.security.*
import vn.scalaeng.p2.service.*

class PostgresHttpSuite extends munit.CatsEffectSuite:
  override def munitIOTimeout: Duration = 2.minutes
  private def call(app: HttpApp[IO],m: Method,path: String,body: String="",token: Option[String]=None): IO[(Int,String)] =
    val r=Request[IO](m,Uri.unsafeFromString(path)).withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"),"application/json"))
    val req=token.fold(r)(t => r.putHeaders(Header.Raw(CIString("Authorization"),s"Bearer $t")))
    app(req).flatMap(response => response.bodyText.compile.string.map(response.status.code -> _))
  test("HTTP → service → PostgreSQL: auth, checkout, IDOR, admin, readiness và logout") {
    IO.defer {
      assume(sys.env.get("RUN_DB_TESTS").contains("1"),"đặt RUN_DB_TESTS=1 ngoài sandbox")
      val resources=for
        pg <- Resource.make(IO.blocking(EmbeddedPostgres.builder().start()))(p => IO.blocking(p.close()))
        cfg=new DbConfig(pg.getJdbcUrl("postgres","postgres"),"postgres","postgres",4)
        _ <- Resource.eval(Database.migrate(cfg))
        xa <- Database.transactor(cfg)
        auth <- Resource.eval(AuthService.make(new PgStore,Database.runner(xa),new Tokens(new Secret("integration-only-1234567890123456789"),Clock.systemUTC())))
        metrics <- Resource.eval(Metrics.make)
        limiter <- Resource.eval(RateLimiter.make(10,1.minute))
        cache <- Resource.eval(TtlCache.make[Long,Option[Product]](1.second,20))
        b <- Resource.eval(Bulkhead.make(8))
        api=new Api(new Shop(new PgStore,Database.runner(xa)),auth,limiter,cache,metrics.loginFailure)
      yield (Operations.app(api.routes,Database.ready(xa),metrics,b,api.openApi),xa)
      resources.use { (app,xa) =>
        val pwd="một câu mật khẩu bí mật 2026"
        for
          register <- call(app,Method.POST,"/api/v1/auth/register",s"""{"name":"An","email":"an@example.vn","password":"$pwd"}""")
          _ <- call(app,Method.POST,"/api/v1/auth/register",s"""{"name":"Bình","email":"binh@example.vn","password":"$pwd"}""")
          login <- call(app,Method.POST,"/api/v1/auth/login",s"""{"email":"an@example.vn","password":"$pwd"}""")
          token=parse(login._2).toOption.get.hcursor.get[String]("accessToken").toOption.get
          _ <- sql"update users set role='Admin' where id=2".update.run.transact(xa)
          adminLogin <- call(app,Method.POST,"/api/v1/auth/login",s"""{"email":"binh@example.vn","password":"$pwd"}""")
          admin=parse(adminLogin._2).toOption.get.hcursor.get[String]("accessToken").toOption.get
          product <- call(app,Method.POST,"/api/v1/products","""{"sku":"P","name":"Bút","price":100,"stock":1}""",Some(admin))
          checkout <- call(app,Method.POST,"/api/v1/users/1/orders","""{"lines":[{"productId":1,"quantity":1}]}""",Some(token))
          sold <- call(app,Method.POST,"/api/v1/users/1/orders","""{"lines":[{"productId":1,"quantity":1}]}""",Some(token))
          idor <- call(app,Method.GET,"/api/v1/users/2",token=Some(token))
          ready <- call(app,Method.GET,"/health/ready")
          _ <- call(app,Method.POST,"/api/v1/auth/logout",token=Some(token))
          revoked <- call(app,Method.GET,"/api/v1/orders/1",token=Some(token))
        yield {
          assertEquals(register._1,201);assertEquals(product._1,201);assertEquals(checkout._1,201)
          assert(checkout._2.contains("\"totalMinor\":100"));assertEquals(sold._1,409)
          assertEquals(idor._1,403);assertEquals(ready._1,200);assertEquals(revoked._1,401)
        }
      }
    }
  }
