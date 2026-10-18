package vn.scalaeng.db

import cats.syntax.all.*
import org.typelevel.doobie.implicits.*

class RepositorySuite extends PostgresSuite:

  test("migration: Flyway ghi 2 version; chạy lại không áp dụng gì thêm"):
    for
      again    <- Database.migrate(config)
      versions <- sql"select version from flyway_schema_history order by installed_rank".query[String].to[List].transact(xa)
    yield
      assertEquals(again, 0)
      assertEquals(versions, List("1", "2"))

  test("tạo user, đọc lại theo id và theo email không phân biệt hoa thường"):
    for
      created <- UserRepo.create("An@Example.vn", "Nguyễn An").transact(xa)
      user     = created.toOption.get
      byId    <- UserRepo.find(user.id).transact(xa)
      byEmail <- UserRepo.findByEmail("an@example.VN").transact(xa)
    yield
      assertEquals(user.name, "Nguyễn An")
      assertEquals(byId, Some(user))
      assertEquals(byEmail.map(_.id), Some(user.id))

  test("email trùng (khác hoa thường) -> Left(DuplicateEmail), không phải exception"):
    for
      _   <- UserRepo.create("an@example.vn", "An").transact(xa)
      dup <- UserRepo.create("AN@example.vn", "An 2").transact(xa)
    yield assertEquals(dup.left.map(_.toString), Left("DuplicateEmail(AN@example.vn)"))

  test("email trùng trong CÙNG transaction: ON CONFLICT để transaction dùng tiếp được"):
    val tx = for
      _     <- UserRepo.create("an@example.vn", "An")
      dup   <- UserRepo.create("AN@example.vn", "An 2")
      after <- UserRepo.findByEmail("an@example.vn")       // vẫn chạy được trong cùng transaction
    yield (dup.isLeft, after.map(_.name))
    tx.transact(xa).map(r => assertEquals(r, (true, Some("An"))))

  test("attemptSomeSqlState để lại transaction bị abort: câu lệnh sau lỗi nhận SQLSTATE 25P02"):
    val tx = for
      _   <- UserRepo.create("an@example.vn", "An")
      dup <- UserRepo.createCatchingUniqueViolation("AN@example.vn", "An 2") // Left ở phía Scala...
      _   <- UserRepo.findByEmail("an@example.vn")                           // ...nhưng PostgreSQL đã abort transaction
    yield dup
    tx.transact(xa).attempt.map {
      case Left(e: java.sql.SQLException) => assertEquals(e.getSQLState, "25P02")
      case other                          => fail(s"mong đợi 25P02, nhận $other")
    }

  test("CHECK constraint chặn dữ liệu sai dù ứng dụng quên validate"):
    ProductRepo.create("BAD-1", "Giá âm", -1, 1).transact(xa).attempt.map { r =>
      assert(r.left.exists(_.getMessage.contains("products_price_vnd_check")), r.toString)
    }

  test("list: lọc ilike, sort theo whitelist, phân trang ổn định"):
    val names = List("Bình", "An", "Châu", "An Khang", "Dũng")
    for
      _     <- names.traverse_(n => UserRepo.create(s"${n.filter(_.isLetter).toLowerCase}@x.vn", n)).transact(xa)
      page1 <- UserRepo.list(None, UserSort.ByName, descending = false, limit = 2, offset = 0).transact(xa)
      page2 <- UserRepo.list(None, UserSort.ByName, descending = false, limit = 2, offset = 2).transact(xa)
      an    <- UserRepo.list(Some("an"), UserSort.ByName, descending = true, limit = 10, offset = 0).transact(xa)
    yield
      assertEquals((page1 ++ page2).map(_.name), List("An", "An Khang", "Bình", "Châu"))
      assertEquals(an.map(_.name), List("An Khang", "An"))

  test("SQL injection: ghép chuỗi lộ toàn bộ bảng; tham số hóa thì không"):
    val attack = "x' or '1'='1"
    for
      _      <- List("a@x.vn" -> "An", "b@x.vn" -> "Bình").traverse_((e, n) => UserRepo.create(e, n)).transact(xa)
      unsafe <- UserRepo.unsafeFindByName(attack).transact(xa)
      safe   <- UserRepo.safeFindByName(attack).transact(xa)
    yield
      assertEquals(unsafe.size, 2) // kẻ tấn công đọc được MỌI user
      assertEquals(safe, Nil)      // chuỗi tấn công chỉ là một giá trị so sánh bình thường
