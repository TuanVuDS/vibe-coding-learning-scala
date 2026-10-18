package vn.scalaeng.fw

import munit.FunSuite

class OpenApiSuite extends FunSuite:
  private val yaml = TapirApi.openApiYaml

  test("tài liệu OpenAPI có đủ path và mã lỗi đã khai báo"):
    assert(yaml.contains("/api/v1/products/{id}:"), yaml)
    assert(yaml.contains("/api/v1/products:"), yaml)
    List("'201'", "'404'", "'409'", "'422'").foreach(code => assert(yaml.contains(code), s"thiếu $code"))

  test("mỗi endpoint chỉ liệt kê lỗi của chính nó"):
    val (getPart, postPart) = yaml.splitAt(yaml.indexOf("/api/v1/products:"))
    assert(!getPart.contains("'409'") && !getPart.contains("'422'"), getPart)
    val postPaths = postPart.split("components:").head
    assert(!postPaths.contains("'404'"), postPaths)

  test("schema Product được sinh từ case class"):
    assert(yaml.contains("Product:"), yaml)
    assert(yaml.contains("NewProduct:"), yaml)
