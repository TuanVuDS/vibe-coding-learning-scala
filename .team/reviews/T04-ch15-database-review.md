# Review chéo T04 — Chương 15 (Codex)

Ngày review: **17/10/2026**. Đã đọc handoff, toàn bộ bài học, nguồn Scala/SQL, build Scala CLI/sbt, compose, các test, lời giải và migration lời giải. Không sửa nguồn của tác giả.

**Kết luận: cần sửa 4 MAJOR, 4 MINOR; không có BLOCKER. Test database bị chặn ở bước khởi động PostgreSQL trong sandbox, không coi là lỗi bài.**

## BLOCKER

Không phát hiện.

## MAJOR

### M1 — Bắt unique violation trong ConnectionIO để lại transaction bị abort

- Vị trí: `06-database/db-lab/src/main/scala/vn/scalaeng/db/Repositories.scala:18`; giải thích `06-database/ch15-database-engineering.md:378` và nguyên tắc ghép repository tại dòng 129.
- `attemptSomeSqlState` chỉ chuyển exception thành giá trị Scala, không tạo savepoint hay rollback. PostgreSQL vẫn giữ transaction ở trạng thái aborted sau lỗi 23505. Một chương trình ghép `UserRepo.create(emailTrung, ...)` rồi `UserRepo.find(...)` trong **cùng** `transact` sẽ lỗi 25P02; nếu đã ghi dữ liệu trước lỗi, bắt lỗi rồi kết thúc chương trình cũng không cứu được các thay đổi đó. Test hiện tại gọi `transact` riêng cho mỗi create, nên không kiểm tra trường hợp ghép mà chương đang khuyến khích.
- Đây là kết luận từ tài liệu PostgreSQL và **mã nguồn đúng Doobie RC13**, chưa tái hiện trên server ở sandbox này: `doobie/util/catchsql.scala` trong sources JAR cho thấy `attemptSomeSqlState` chỉ dùng `recoverWith`/`pure(Left(...))`.
- Đề xuất: dùng `INSERT ... ON CONFLICT (lower(email)) DO NOTHING RETURNING ...` rồi chuyển kết quả optional thành Either, hoặc dùng savepoint đúng cách. Một lựa chọn khác là để SQL exception thoát tới transaction boundary rồi map lỗi sau rollback, nhưng phải giải thích khác biệt với API ConnectionIO hiện tại. Thêm test duplicate rồi truy vấn/ghi tiếp trong cùng transaction.
- Nguồn: [PostgreSQL — Transactions, phần savepoint và phục hồi aborted transaction](https://www.postgresql.org/docs/18/tutorial-transactions.html); [Doobie — Error Handling](https://typelevel.org/doobie/docs/09-Error-Handling.html); [sources JAR RC13](https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/1.0.0-RC13/doobie-core_3-1.0.0-RC13-sources.jar). Đối chiếu 17/10/2026.

### M2 — Tràn số làm sai số lượng và tổng tiền nhưng vẫn qua ràng buộc DB

- Vị trí: `06-database/db-lab/src/main/scala/vn/scalaeng/db/OrderService.scala:19` và dòng 45; `06-database/db-lab/src/main/scala/vn/scalaeng/db/Model.scala:34` cho phép mọi Int.
- Gộp số lượng bằng `_ + _` trên Int có thể wrap; nhân/cộng tiền bằng Long cũng wrap. Ví dụ ba dòng cùng sản phẩm có số lượng dương `Int.MaxValue, Int.MaxValue, 3` bị gộp thành **1**: có thể mua thành công từ kho 10 dù yêu cầu thực vượt xa tồn kho. Với giá `4611686018427387904L`, số lượng 4, tổng tiền wrap về **0**, vẫn hợp `CHECK (total_vnd >= 0)`.
- Đã chạy probe không cần DB, tái sử dụng nguồn và gọi hàm `check` thật qua reflection; output:

  ```text
  PROBE merged positive quantities = 1
  PROBE service check = Right(List((1,4,4611686018427387904)))
  PROBE total using service arithmetic = 0
  ```

- Probe xác nhận bước kiểm tra và phép tính Scala; chưa chạy INSERT trên PostgreSQL. Schema hiện tại không có ràng buộc đối chiếu tổng tiền với chi tiết để bắt kết quả 0 này.
- Đề xuất: validate từng quantity > 0 **trước khi gộp**, đặt giới hạn nghiệp vụ rõ, gộp bằng kiểu rộng/checked arithmetic; dùng `Math.addExact`/`multiplyExact` hoặc BigInt rồi kiểm tra miền Long. Trả lỗi nghiệp vụ khi vượt giới hạn; test cả số lượng trùng, quantity không hợp lệ và tràn tổng tiền.

### M3 — Scala CLI dùng sai phiên bản munit so với chuẩn và sbt

- Vị trí: `06-database/db-lab/project.scala:17`.
- `munit-cats-effect:2.2.1` kéo `munit_3:1.3.6`, dù file cũng ghi munit 1.2.4. Đã đối chiếu POM chính thức và log resolver thực sự tải munit/munit-diff 1.3.6. `06-database/db-lab/build.sbt:22`, bài học và code lời giải dùng **2.2.0**, nên hai đường kiểm chứng không cùng stack và Scala CLI vi phạm AGENTS.md mục 5.
- Đề xuất: đổi directive thành 2.2.0, chạy lại cả lab và lời giải, cập nhật trạng thái kiểm chứng theo stack thực dùng. Không đổi munit chuẩn của giáo trình.
- Nguồn: [munit-cats-effect 2.2.1 POM](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/2.2.1/munit-cats-effect_3-2.2.1.pom), kiểm tra 17/10/2026.

### M4 — Không nên gộp REPEATABLE READ với SERIALIZABLE thành bảo đảm cho logic phức tạp

- Vị trí: `06-database/ch15-database-engineering.md:174`.
- PostgreSQL REPEATABLE READ chặn lost update trên cùng dòng trong thí nghiệm, nhưng vẫn cho phép serialization anomaly/write skew giữa nhiều dòng. Retry 40001 không khắc phục các giao dịch sai mà **cả hai đều commit**, vì khi đó không có 40001. Câu hiện tại dễ khiến người mới dùng REPEATABLE READ thay SERIALIZABLE cho bất biến nghiệp vụ nhiều dòng.
- Đề xuất: nói rõ REPEATABLE READ là snapshot isolation và phạm vi thí nghiệm cùng dòng; với bất biến phức tạp cần phân tích explicit locks/ràng buộc hoặc SERIALIZABLE + retry toàn transaction. Thêm ví dụ ngắn hai dòng độc lập hoặc dẫn đúng mục hạn chế trong tài liệu.
- Nguồn: [PostgreSQL 18 — Transaction Isolation, bảng 13.1 và mục Repeatable Read](https://www.postgresql.org/docs/18/transaction-iso.html), kiểm tra 17/10/2026.

## MINOR

### m1 — Base64 không chống sửa cursor

- Vị trí: `solutions/ch15-solutions.md:64`.
- Base64 là encoding có thể đảo ngược, client vẫn tự tạo/sửa giá trị được; không phải bảo đảm toàn vẹn.
- Đề xuất: gọi là opaque representation để client không phụ thuộc định dạng nội bộ; validate cursor sau decode. Nếu thật sự cần chống sửa, dùng MAC/chữ ký hoặc token ngẫu nhiên lưu phía server, đồng thời vẫn kiểm tra quyền truy cập.

### m2 — Keyset pagination cần hợp đồng cho page size

- Vị trí: `solutions/ch15-code/src/main/scala/vn/scalaeng/solutions/ch15/Ch15Solutions.scala:56` và dòng 61.
- Với `size = 0` và bảng có dữ liệu, query lấy một dòng nhưng `rows.take(0).last` ném exception. `size + 1` cũng overflow khi size là Int.MaxValue. API/lời giải không khai báo hay kiểm tra tiền điều kiện này.
- Đề xuất: giới hạn size, ví dụ 1–100, trả lỗi rõ trước khi query; thêm ca biên và ghi tiền điều kiện trong lời giải. Với test phân trang hiện tại, nên assert các created_at trùng nhau để chứng minh tie-break bằng id như bài giải thích.

### m3 — Sleep mở cửa sổ race nhưng không bảo đảm hai transaction cùng đọc stock cũ

- Vị trí: `06-database/db-lab/src/test/scala/vn/scalaeng/db/TransactionSuite.scala:64`, các assertion dòng 72 và 86.
- `Thread.sleep(300)` tăng xác suất nhưng không đồng bộ lịch chạy. Máy chậm/tải cao có thể khởi động transaction thứ hai sau khi bên thứ nhất commit, khiến test đòi `(true,true)` hoặc đúng một 40001 thất bại dù PostgreSQL hoạt động đúng.
- Đề xuất: dùng barrier/latch có timeout ở hook sau SELECT để buộc cả hai đã đọc trước khi UPDATE; không dùng barrier trong luồng FOR UPDATE vì sẽ chặn transaction đang giữ khóa. Không kết luận đây là test đã fail logic: reviewer chưa chạy được assertion do fixture môi trường.

### m4 — Hướng dẫn debug migration chưa đúng với rollback DDL của PostgreSQL

- Vị trí: `06-database/ch15-database-engineering.md:517`.
- Với migration SQL chạy trong transaction như lab, PostgreSQL rollback cả thay đổi schema và bản ghi history khi migration thất bại; không nhất thiết còn dòng `success = false` để tìm. Người mới làm theo có thể không thấy gì và tưởng Flyway không ghi lỗi.
- Đề xuất: hướng dẫn đọc exception/log Flyway trước; giải thích bản ghi failed khác nhau giữa migration transactional và non-transactional/database khác. Không mặc định dùng `repair` để che migration hỏng.
- Nguồn: [Flyway — Migration transaction handling](https://documentation.red-gate.com/flyway/flyway-concepts/migrations/migration-transaction-handling), kiểm tra 17/10/2026.

## Kiểm chứng thực chạy và giới hạn sandbox

Đặt COURSIER_CACHE/SCALA_CLI_HOME dưới `.cache/`, TMP/TEMP và `java.io.tmpdir` về `.cache/tmp` để output/binary/data kiểm chứng nằm trong phạm vi cho phép:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:SCALA_CLI_HOME = "$PWD/.cache/scala-cli"
$env:TMP = "$PWD/.cache/tmp"
$env:TEMP = $env:TMP
scala-cli.bat test 06-database/db-lab --server=false --workspace .cache/review-t04-lab --java-opt "-Djava.io.tmpdir=$PWD/.cache/tmp"
scala-cli.bat test solutions/ch15-code --server=false --workspace .cache/review-t04-solutions --java-opt "-Djava.io.tmpdir=$PWD/.cache/tmp"
```

| Phạm vi | Kết quả reviewer thực thu |
|---|---|
| Lab (14 test được định nghĩa) | Compile xong; 3 lỗi fixture `beforeAll(postgres)`, 0 assertion test chạy thành công; exit 1 |
| Lời giải (7 test được định nghĩa) | Compile xong; 1 lỗi fixture `beforeAll(postgres)`, 0 assertion test chạy thành công; exit 1 |

Output thật chung:

```text
java.io.IOException: Gave up waiting for server to start after 10000ms
Caused by: java.sql.SQLException: connect failed
Caused by: java.net.ConnectException: Connection refused: getsockopt
```

Chẩn đoán thêm bằng binary 18.6 đã giải nén trong `.cache/tmp/embedded-pg/`: `initdb --version` và `postgres --version` đều trả PostgreSQL 18.6; initdb tạo được cluster chẩn đoán dưới `.cache/`, nhưng `pg_ctl ... start` trên chính cluster đó trả:

```text
pg_ctl: could not create restricted token: error code 87
pg_ctl: could not start server: error code 3
START_EXIT_CODE=1
```

**Không tính các lỗi khởi động này vào BLOCKER/MAJOR/MINOR của bài. Không xác nhận 14/14 hay 7/7 bằng lượt chạy reviewer.** Bốn lỗi fixture không đồng nghĩa bốn test nghiệp vụ sai. Log: `.cache/review-t04-lab.log`, `.cache/review-t04-solutions.log`, `.cache/review-t04-initdb.log`. Không chạy sbt theo giới hạn AGENTS.md; không khởi động Docker.

Probe arithmetic: `.cache/review-t04-arithmetic.scala`, log `.cache/review-t04-arithmetic.log`; dùng đúng Scala 3.9.0/JDK 21 và nguồn service gốc. Không dùng probe này để nhận đã kiểm chứng database.

## Nội dung, output và lời giải

- Đủ **17/17 mục**, không rỗng; tiếng Việt rõ, có thuật ngữ, đánh đổi, ví dụ nối tiếp users/orders. Mạch schema → transaction → race → index → pool phù hợp. Mục SQL tối thiểu khá cô đọng cho người chưa biết SQL, nhưng lời giải bổ sung JOIN/GROUP BY bằng ví dụ cụ thể.
- Sáu bài tập có code/lời giải và bảy test tương ứng, gồm hủy đồng thời, pagination, conditional UPDATE, query plan. Chưa xác nhận runtime các lời giải do sandbox. V3 chủ động giả định chưa có đơn CANCELLED cũ (comment dòng 5); nên nhấn mạnh giả định này trong bài giải và thêm hướng dẫn backfill nếu chuyển sang DB đã có dữ liệu, không xem đó là lỗi chạy của fixture rỗng hiện tại.
- Đọc code cho thấy output DbDemo hai lần khớp cách tạo sản phẩm mới theo timestamp, tìm lại user và giá trị OutOfStock. **Chưa tái chạy DbDemo, EXPLAIN ANALYZE hay các diagnostic SQLSTATE trên PostgreSQL**; chúng vẫn là bằng chứng từ handoff tác giả, không nâng thành bằng chứng runtime reviewer.
- Đoạn EXPLAIN ANALYZE có costs/timing lấy từ script riêng tác giả khai báo; helper `Database.explain` trong lab chủ động tắt costs/timing. Nên cung cấp lệnh SQL/script tái lập nếu muốn người học thu đúng định dạng transcript, nhưng chưa có bằng chứng transcript sai.

## Đối chiếu phiên bản và nguồn

Đã tải metadata Maven Central trực tiếp ngày **17/10/2026**: Doobie RC13, Flyway 13.10.3, pgjdbc 42.7.14, embedded-postgres 2.2.2/binary Windows 18.6.0, Slick 4.0.0-RC2 (stable 3.6.1 được bài nêu), Skunk 2.0.0-RC3. Các artifact dùng trong lab đã resolve/compile; package mới `org.typelevel.doobie` được xác nhận bởi sources JAR và tài liệu RC13.

- [Doobie metadata](https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/maven-metadata.xml)
- [Flyway metadata](https://repo1.maven.org/maven2/org/flywaydb/flyway-core/maven-metadata.xml)
- [pgjdbc metadata](https://repo1.maven.org/maven2/org/postgresql/postgresql/maven-metadata.xml)
- [embedded-postgres metadata](https://repo1.maven.org/maven2/io/zonky/test/embedded-postgres/maven-metadata.xml)
- [binary Windows metadata](https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/maven-metadata.xml)
- [Slick metadata](https://repo1.maven.org/maven2/com/typesafe/slick/slick_3/maven-metadata.xml)
- [Skunk metadata](https://repo1.maven.org/maven2/org/tpolecat/skunk-core_3/maven-metadata.xml)

Đã kiểm tra tài liệu PostgreSQL về conditional UPDATE/SELECT FOR UPDATE và 40001: mô tả thí nghiệm cùng một dòng phù hợp. Compose được tác giả ghi chưa kiểm chứng; đường volume PostgreSQL 18 phù hợp cấu trúc image chính thức, nhưng reviewer không xác nhận đã chạy image.
