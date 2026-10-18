package vn.scalaeng.http

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Adapter mỏng: HttpExchange (JDK, mutable) <-> Request/Response (bất biến). Không có logic nghiệp vụ ở đây. */
object HttpAdapter:
  val MaxBodyBytes = 1_000_000

  def toRequest(ex: HttpExchange): Either[Response, Request] =
    Method.parse(ex.getRequestMethod) match
      case None => Left(Response.problem(501, "Not Implemented", s"method ${ex.getRequestMethod} không được hỗ trợ"))
      case Some(method) =>
        val bytes = ex.getRequestBody.readNBytes(MaxBodyBytes + 1)
        if bytes.length > MaxBodyBytes then Left(Response.problem(413, "Content Too Large", s"body vượt $MaxBodyBytes byte"))
        else
          val headers = ex.getRequestHeaders.asScala.toMap.collect { case (k, vs) if !vs.isEmpty => k.toLowerCase -> vs.get(0) }
          Right(Request(method, ex.getRequestURI.getPath, Query.parse(ex.getRequestURI.getRawQuery), headers, String(bytes, UTF_8)))

  def write(ex: HttpExchange, res: Response): Unit =
    res.headers.foreach((k, v) => ex.getResponseHeaders.set(k, v))
    val bytes = res.body.getBytes(UTF_8)
    // -1 = không có body (bắt buộc cho 204); ngược lại khai báo đúng Content-Length
    ex.sendResponseHeaders(res.status, if bytes.isEmpty then -1 else bytes.length.toLong)
    if bytes.nonEmpty then ex.getResponseBody.write(bytes)
    ex.close()

  final case class Running(server: HttpServer, pool: ExecutorService):
    def port: Int = server.getAddress.getPort
    /** Graceful shutdown: ngừng nhận kết nối mới, chờ tối đa `graceSeconds` cho request đang xử lý. */
    def stop(graceSeconds: Int): Unit =
      server.stop(graceSeconds)
      pool.shutdown()
      if !pool.awaitTermination(graceSeconds.toLong + 1, TimeUnit.SECONDS) then { pool.shutdownNow(); () }

  def start(handler: Handler, host: String, port: Int, threads: Int): Running =
    val server = HttpServer.create(InetSocketAddress(host, port), 0)
    val pool = Executors.newFixedThreadPool(threads)
    server.createContext("/", ex => write(ex, toRequest(ex).fold(identity, handler)))
    server.setExecutor(pool)
    server.start()
    Running(server, pool)

object App:
  def handler(store: ProductStore, tokens: Map[String, Role], log: String => Unit,
              clock: () => Long = () => System.currentTimeMillis(),
              newId: () => String = () => UUID.randomUUID().toString): Handler =
    val mw = Middlewares.compose(
      Middlewares.requestId(newId),     // ngoài cùng: mọi response đều có request id
      Middlewares.accessLog(log, clock),
      Middlewares.errorHandling(log),
      Middlewares.authenticate(tokens)
    )
    mw(Router(ProductApi.routes(store)))

object Main:
  def main(args: Array[String]): Unit =
    val env = sys.env
    (env.get("ADMIN_TOKEN"), env.get("READER_TOKEN")) match
      case (Some(admin), Some(reader)) if admin.length >= 16 && reader.length >= 16 && admin != reader =>
        val port = env.get("PORT").flatMap(_.toIntOption).getOrElse(8080)
        val log: String => Unit = line => System.err.println(line)
        val running = HttpAdapter.start(
          App.handler(ProductStore(), Map(admin -> Role.Admin, reader -> Role.Reader), log), "0.0.0.0", port, threads = 8)
        sys.addShutdownHook { log("""{"msg":"shutting down"}"""); running.stop(graceSeconds = 5) }
        log(s"""{"msg":"listening","port":${running.port}}""")
      case _ =>
        System.err.println("cần ADMIN_TOKEN và READER_TOKEN khác nhau, mỗi token >= 16 ký tự (KHÔNG có giá trị mặc định)")
        sys.exit(1)
