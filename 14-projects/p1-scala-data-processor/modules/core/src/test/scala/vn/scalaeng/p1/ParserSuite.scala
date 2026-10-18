package vn.scalaeng.p1

import munit.FunSuite
import vn.scalaeng.p1.model.*
import vn.scalaeng.p1.parse.{OrderParser, UserParser}
import vn.scalaeng.p1.json.JsonParser
import java.time.LocalDate

class ParserSuite extends FunSuite:
  private def fieldsOf(r: Either[Rejected, ?]): List[String] = r.left.toOption.toList.flatMap(_.issues.map(_.field))

  test("order hợp lệ"):
    val o = OrderParser.parseRow(2, "ORD-1,U-1,AO-01,120000,2,PAID,2026-10-01,2026-10-01T02:00:00Z")
    assertEquals(o.map(x => (x.orderId.value, x.total.dong, x.lineNo)), Right(("ORD-1", 240000L, 2)))

  test("order: TẤT CẢ lỗi được thu thập"):
    val r = OrderParser.parseRow(3, "x,y,z z,abc,0,SHIPPED,2026-13-01,yesterday")
    assertEquals(fieldsOf(r), List("order_id", "user_id", "sku", "unit_price", "quantity", "status", "order_date", "updated_at"))

  test("order: lỗi cấu trúc dòng"):
    assertEquals(fieldsOf(OrderParser.parseRow(4, "ORD-1,U-1")), List("_line"))
    assertEquals(fieldsOf(OrderParser.parseRow(5, "ORD-1,\"U-1")), List("_line"))

  test("order: quy tắc liên trường theo giờ Việt Nam"):
    // 2026-10-01T18:30Z = 01:30 ngày 02/10 ở VN -> hợp lệ với order_date 02/10
    assert(OrderParser.parseRow(2, "ORD-1,U-1,A,1,1,PAID,2026-10-02,2026-10-01T18:30:00Z").isRight)
    // 2026-10-01T16:59Z = 23:59 ngày 01/10 ở VN -> sớm hơn order_date 02/10
    assertEquals(fieldsOf(OrderParser.parseRow(2, "ORD-1,U-1,A,1,1,PAID,2026-10-02,2026-10-01T16:59:00Z")), List("updated_at"))

  test("user: trường tùy chọn thiếu/null/rỗng là None, sai thì báo lỗi"):
    val ok = UserParser.parseUser(1, JsonParser.parse("""{"user_id":"u-1","name":" An ","email":null,"phone":"","city":"hn"}""").toOption.get)
    assertEquals(ok.map(u => (u.userId.value, u.name, u.email, u.phone, u.city.value)), Right(("U-1", "An", None, None, "Hà Nội")))
    val bad = UserParser.parseUser(2, JsonParser.parse("""{"user_id":"U-2","name":123,"email":"x@","phone":42,"city":""}""").toOption.get)
    assertEquals(fieldsOf(bad), List("name", "email", "phone", "city"))

  test("user: phần tử không phải object, file không phải mảng"):
    assertEquals(fieldsOf(UserParser.parseUser(3, JsonParser.parse("\"U-6\"").toOption.get)), List("_item"))
    assert(UserParser.parseAll("{}").isLeft)
    assert(UserParser.parseAll("[").isLeft)
    assertEquals(UserParser.parseAll("[]"), Right(Nil))

  test("ngày nghiệp vụ: LocalDate từ order_date"):
    val o = OrderParser.parseRow(2, "ORD-1,U-1,A,1,1,CANCELLED,2026-02-28,2026-02-28T00:00:00Z")
    assertEquals(o.map(_.orderDate), Right(LocalDate.of(2026, 2, 28)))
