package vn.scalaeng.p2.app

import cats.syntax.all.*
import cats.effect.{ExitCode,IO,IOApp,Resource}
import com.comcast.ip4s.{host,Port}
import java.time.Clock
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.doobie.implicits.*
import scala.concurrent.duration.*
import vn.scalaeng.p2.db.*
import vn.scalaeng.p2.http.*
import vn.scalaeng.p2.security.*
import vn.scalaeng.p2.service.*
import vn.scalaeng.p2.domain.*
import org.typelevel.doobie.{ConnectionIO,Transactor}

final case class Config(db: DbConfig, secret: Secret, port: Int)
object Config:
  def load(env: Map[String,String]): Either[List[String],Config] =
    val required = List("DB_URL","DB_USER","DB_PASSWORD","JWT_SECRET")
    val missing = required.filter(k => env.get(k).forall(_.trim.isEmpty)).map(k => s"thiếu $k")
    val port = env.getOrElse("PORT","8080").toIntOption.filter(n => n > 0 && n <= 65535)
    val pool = env.getOrElse("DB_POOL_SIZE","8").toIntOption.filter(n => n >= 1 && n <= 32)
    val errors = missing ++ Option.when(env.get("JWT_SECRET").exists(_.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 32))("JWT_SECRET tối thiểu 32 byte").toList ++
      Option.when(port.isEmpty)("PORT phải trong 1..65535").toList ++ Option.when(pool.isEmpty)("DB_POOL_SIZE phải trong 1..32").toList ++
      Option.when(env.get("DB_URL").exists(s => !s.startsWith("jdbc:postgresql://")))("DB_URL phải dùng PostgreSQL").toList
    if errors.nonEmpty then Left(errors)
    else Right(Config(new DbConfig(env("DB_URL"),env("DB_USER"),env("DB_PASSWORD"),pool.get),new Secret(env("JWT_SECRET")),port.get))

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] = Config.load(sys.env) match
    case Left(es) => IO.println(es.mkString("cấu hình không hợp lệ:\n- ","\n- ","")).as(ExitCode(2))
    case Right(c) =>
      val server = for
        _ <- Resource.eval(Database.migrate(c.db))
        xa <- Database.transactor(c.db)
        store = new PgStore
        tx = Database.runner(xa)
        tokens = new Tokens(c.secret,Clock.systemUTC())
        auth <- Resource.eval(AuthService.make(store,tx,tokens))
        metrics <- Resource.eval(Metrics.make)
        limiter <- Resource.eval(RateLimiter.make(5,1.minute))
        cache <- Resource.eval(TtlCache.make[Long,Option[Product]](2.seconds,1000))
        bulkhead <- Resource.eval(Bulkhead.make(32))
        api = new Api(new Shop(store,tx),auth,limiter,cache,metrics.loginFailure)
        app = Operations.app(api.routes,Database.ready(xa),metrics,bulkhead,api.openApi)
        s <- EmberServerBuilder.default[IO].withHost(host"0.0.0.0").withPort(Port.fromInt(c.port).get)
          .withHttpApp(app).withShutdownTimeout(10.seconds).build
      yield s
      server.evalTap(s => IO.println(s"p2 lắng nghe tại ${s.address}")).useForever.as(ExitCode.Success)

/** Explicit out-of-band bootstrap, never grant Admin through a public HTTP request. */
object BootstrapAdmin extends IOApp:
  def run(args: List[String]): IO[ExitCode] = Config.load(sys.env) match
    case Left(es) => IO.println(es.mkString("; ")).as(ExitCode(2))
    case Right(c) =>
      (sys.env.get("ADMIN_EMAIL"),sys.env.get("ADMIN_PASSWORD")) match
        case (Some(email),Some(password)) =>
          (Resource.eval(Database.migrate(c.db)) *> Database.transactor(c.db)).use { xa =>
            create(xa,email,password).flatMap {
              case Left(_) => IO.println("không tạo được admin; không tự nâng quyền tài khoản đã tồn tại").as(ExitCode(2))
              case Right(_) => IO.println("đã tạo admin").as(ExitCode.Success)
            }
          }
        case _ => IO.println("thiếu ADMIN_EMAIL hoặc ADMIN_PASSWORD").as(ExitCode(2))

  /** Validate/hash before taking a connection; registration and promotion commit together. */
  private[app] def create(xa: Transactor[IO], email: String, password: String): IO[Either[DomainError,User]] =
    val input = Validation.user("Quản trị",email).flatMap { draft =>
      val errors = Passwords.policy(password,draft.email)
      Either.cond(errors.isEmpty,draft,DomainError.Invalid(errors))
    }
    input.fold(e => IO.pure(Left(e)), draft => Passwords.hash(password).flatMap { hash =>
      import vn.scalaeng.p2.db.Codecs.given
      val program = new PgStore().register(draft,hash).flatMap {
        case Left(e) => (Left(e): Either[DomainError,User]).pure[ConnectionIO]
        case Right(user) => sql"update users set role='Admin' where id=${user.id}".update.run.as(Right(user): Either[DomainError,User])
      }
      program.transact(xa)
    })
