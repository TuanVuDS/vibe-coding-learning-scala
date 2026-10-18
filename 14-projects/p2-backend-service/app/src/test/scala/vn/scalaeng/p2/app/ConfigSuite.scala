package vn.scalaeng.p2.app

class ConfigSuite extends munit.FunSuite:
  test("không có secret mặc định; lỗi config không in giá trị") {
    val bad=Map("DB_URL"->"jdbc:postgresql://localhost/shop","DB_USER"->"shop","DB_PASSWORD"->"hidden","JWT_SECRET"->"short","PORT"->"oops")
    val errors=Config.load(bad).swap.toOption.get.mkString(";")
    assert(!errors.contains("hidden"));assert(errors.contains("JWT_SECRET"));assert(errors.contains("PORT"))
    assertEquals(Config.load(Map.empty).swap.toOption.get.count(_.startsWith("thiếu")),4)
    assert(!Config.load(bad.updated("JWT_SECRET","test-only-12345678901234567890123456").updated("PORT","8080")).toOption.get.toString.contains("hidden"))
  }
