package vn.scalaeng.fwzio

import zio.*
import zio.http.*
import zio.test.*

/** Cùng các kịch bản với ContractSuite của bản Typelevel, viết bằng zio-test. */
object ZioApiSpec extends ZIOSpecDefault:
  private def post(body: String) =
    Request.post(URL.root / "api" / "v1" / "products", Body.fromString(body))
      .addHeader(Header.ContentType(MediaType.application.json))

  def spec = suite("ZIO HTTP products")(
    test("POST hợp lệ -> 201 + Location, GET lại -> 200") {
      for
        routes  <- ZioApi.make
        created <- routes.runZIO(post("""{"sku":"SKU-1","name":"Bàn phím","price":450000}"""))
        fetched <- routes.runZIO(Request.get(URL.root / "api" / "v1" / "products" / "1"))
        body    <- fetched.body.asString
      yield assertTrue(
        created.status == Status.Created,
        created.headers.get(Header.Location).map(_.url.encode).contains("/api/v1/products/1"),
        fetched.status == Status.Ok,
        body == """{"id":1,"sku":"SKU-1","name":"Bàn phím","price":450000}"""
      )
    },
    test("id không tồn tại -> 404") {
      ZioApi.make.flatMap(_.runZIO(Request.get(URL.root / "api" / "v1" / "products" / "99")))
        .map(r => assertTrue(r.status == Status.NotFound))
    },
    test("tích lũy mọi lỗi validation -> 422") {
      ZioApi.make.flatMap(_.runZIO(post("""{"sku":"x","name":" ","price":0}""")))
        .map(r => assertTrue(r.status == Status.UnprocessableEntity))
    },
    test("sku trùng -> 409") {
      for
        routes <- ZioApi.make
        _      <- routes.runZIO(post("""{"sku":"SKU-1","name":"A","price":1}"""))
        r2     <- routes.runZIO(post("""{"sku":"SKU-1","name":"B","price":2}"""))
      yield assertTrue(r2.status == Status.Conflict)
    },
    test("JSON hỏng -> 400") {
      ZioApi.make.flatMap(_.runZIO(post("""{"sku": """))).map(r => assertTrue(r.status == Status.BadRequest))
    }
  )
