package vn.scalaeng.p1

import munit.FunSuite
import vn.scalaeng.p1.csv.Csv
import vn.scalaeng.p1.json.{Json, JsonParser}
import vn.scalaeng.p1.json.Json.*

class CsvJsonSuite extends FunSuite:
  test("CSV: các trường hợp RFC 4180"):
    assertEquals(Csv.parseLine("a,b,c"), Right(Vector("a", "b", "c")))
    assertEquals(Csv.parseLine("a,\"b,c\",d"), Right(Vector("a", "b,c", "d")))
    assertEquals(Csv.parseLine("\"x\"\"y\",z"), Right(Vector("x\"y", "z")))
    assertEquals(Csv.parseLine(",,"), Right(Vector("", "", "")))
    assertEquals(Csv.parseLine(""), Right(Vector("")))
    assert(Csv.parseLine("\"open").isLeft)
    assert(Csv.parseLine("a,b\"c").isLeft)
    assert(Csv.parseLine("\"a\"b").isLeft)

  test("CSV: round-trip với ký tự nguy hiểm"):
    val field = Gen.stringOf(5, Gen.oneOf('a', 'đ', ',', '"', ' ', '\n'))
    val fields = Gen.choose(1, 5).flatMap(Gen.listOfN(_, field))
    // '\n' bị loại khỏi kỳ vọng: parseLine làm việc trên MỘT dòng — giới hạn đã ghi
    assertEquals(Prop.forAll(fields.map(_.map(_.filterNot(_ == '\n'))), runs = 500)(fs =>
      Csv.parseLine(Csv.renderRow(fs)) == Right(fs.toVector)), None)

  test("CSV: không dùng `var` vẫn xử lý dòng dài (đệ quy đuôi)"):
    val long = List.fill(50_000)("x").mkString(",")
    assertEquals(Csv.parseLine(long).map(_.size), Right(50_000))

  test("JSON: giá trị cơ bản và lồng nhau"):
    assertEquals(JsonParser.parse(""" {"a": [1, -2.5, true, null], "b": {"c": "đ"}} """),
      Right(JObj(Vector("a" -> JArr(Vector(JNum(1), JNum(BigDecimal("-2.5")), JBool(true), JNull)), "b" -> JObj(Vector("c" -> JStr("đ")))))))
    assertEquals(JsonParser.parse("[]"), Right(JArr(Vector.empty)))
    assertEquals(JsonParser.parse("{}"), Right(JObj(Vector.empty)))

  test("JSON: escape và \\u"):
    assertEquals(JsonParser.parse("\"a\\\"b\\\\c\\n\\u0111\""), Right(JStr("a\"b\\c\nđ")))

  test("JSON: lỗi có vị trí"):
    assertEquals(JsonParser.parse("[1, 2,]"), Left("ký tự không mong đợi ']' ở vị trí 6"))
    assertEquals(JsonParser.parse("{\"a\" 1}"), Left("mong đợi ':' ở vị trí 5"))
    assertEquals(JsonParser.parse("[1] x"), Left("ký tự thừa ở vị trí 4"))
    assert(JsonParser.parse("\"abc").isLeft)
    assert(JsonParser.parse("01").isLeft)
    assert(JsonParser.parse("[" * 100 + "]" * 100).left.exists(_.startsWith("lồng quá sâu")))

  test("JSON: render rồi parse trả lại giá trị cũ (round-trip)"):
    val leaf: Gen[Json] = Gen.oneOf[Gen[Json]](
      Gen.choose(-1000, 1000).map(n => JNum(BigDecimal(n))),
      Gen.stringOf(6, Gen.oneOf('a', 'ệ', '"', '\\', '\n', '\t', '\u0001', '/')).map(JStr(_)),
      Gen.oneOf[Json](JNull, JBool(true), JBool(false))
    ).flatMap(identity)
    def tree(depth: Int): Gen[Json] =
      if depth == 0 then leaf
      else Gen.choose(0, 2).flatMap {
        case 0 => leaf
        case 1 => Gen.listOf(3, tree(depth - 1)).map(xs => JArr(xs.toVector))
        case _ => Gen.listOf(3, Gen.stringOf(3, Gen.oneOf('k', 'é')).flatMap(k => tree(depth - 1).map(k -> _))).map(fs => JObj(fs.toVector))
      }
    assertEquals(Prop.forAll(tree(4), runs = 300)(j => JsonParser.parse(j.render) == Right(j)), None)
