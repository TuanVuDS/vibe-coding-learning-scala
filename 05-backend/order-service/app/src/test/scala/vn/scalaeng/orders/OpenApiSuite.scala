package vn.scalaeng.orders

import munit.FunSuite

class OpenApiSuite extends FunSuite:
  private val yaml = Api.openApiYaml
  test("7 operationId, 5 paths và các mã lỗi thực tế được khai báo"):
    List("createUser", "getUser", "listUsers", "createOrder", "getOrder", "listOrders", "updateStatus")
      .foreach(op => assert(yaml.contains(s"operationId: $op"), yaml))
    List("/api/v1/users:", "/api/v1/users/{userId}:", "/api/v1/users/{userId}/orders:",
      "/api/v1/orders/{orderId}:", "/api/v1/orders/{orderId}/status:")
      .foreach(path => assert(yaml.contains(path), yaml))
    List(200, 201, 400, 404, 409, 415, 422, 500).foreach(code => assert(yaml.contains(s"'$code'"), yaml))

  test("Problem media type, pagination, whitelist và enum chuỗi có trong schema"):
    List("application/problem+json", "offset", "limit", "sort", "Pending", "Paid", "Shipped", "Cancelled", "totalMinor")
      .foreach(value => assert(yaml.contains(value), yaml))
