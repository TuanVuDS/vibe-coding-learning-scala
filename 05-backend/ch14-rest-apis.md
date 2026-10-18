# Chương 14 — Building REST APIs: từ hợp đồng HTTP đến repository

> Phiên bản: Scala **3.9.0** · JDK **21** · Cats Effect **3.7.1** · http4s **0.23.38** · Tapir **1.13.33** · circe-core **0.14.17** · sttp-apispec **0.11.10** · munit **1.2.4** · munit-cats-effect **2.2.0**. Trạng thái kiểm chứng: **đã biên dịch và chạy**, 26 test lab + 6 test lời giải pass với `-Werror`, 22 trao đổi curl thật ngày 17/10/2026. Đã review chéo bởi Claude; reviewer đã chạy sbt **25/25** trước vòng sửa này. Vòng 2 thêm một test phản ví dụ, đã chạy lại Scala CLI; chờ lead kiểm tra lại thay đổi.

Chương 12 giải thích HTTP bằng server JDK; [Ch13](ch13-choosing-a-framework.md) chọn Tapir + http4s. Bây giờ ta xây một service quản lý **người dùng và đơn hàng**, giữ hợp đồng HTTP ổn định trong khi có thể thay cách lưu trữ ở [Ch15](../06-database/ch15-database-engineering.md). Học [Ch16B](../07-concurrency/ch16b-cats-effect.md) trước chương này để hiểu `IO`, `Resource`, `Ref`.

## Vị trí trong lộ trình

Học chương này sau [Ch13](ch13-choosing-a-framework.md) và [Ch16B](../07-concurrency/ch16b-cats-effect.md). Ch13 chốt stack; Ch16B cung cấp `IO`, `Resource`, `Ref` để hiểu cách server chạy.

Sau chương này, học Ch15 về lưu trữ PostgreSQL, Ch17 về bảo mật/vận hành và Ch18 về kiến trúc ứng dụng. **Project 2 ghép Ch14 + Ch15 + Ch17**: hợp đồng REST, database và bảo mật thành một backend hoàn chỉnh.

## 1. Mục tiêu học tập

- Mô tả routing (định tuyến), request parsing (đọc đầu vào), response serialization (mã hóa đầu ra) bằng endpoint Tapir có kiểu.
- Tách domain, service, repository và DTO; thay repository mà không thay JSON hay URL.
- Dùng opaque ID, enum trạng thái, số tiền nguyên; tích lũy lỗi validation độc lập.
- Trả Problem JSON nhất quán cho lỗi nghiệp vụ, decode failure, route thiếu và exception.
- Thiết kế lọc, whitelist sort, offset/limit có thứ tự xác định; phân tích khi nào chuyển sang cursor.
- Sinh OpenAPI từ chính bảy endpoint đang chạy; test không mạng và test HTTP qua socket thật.

## 2. Kiến thức tiên quyết

Cần biết `Either`, `Option`, case class, enum, opaque type, `given`, pattern matching; biết ghép `IO` và quản lý vòng đời bằng `Resource`. Ôn Ch12 về method/status/Problem Details, Ch13 về `PublicEndpoint` và interpreter (bộ diễn giải endpoint).

Đây là **API học kiến trúc**, lưu RAM, một instance. Restart mất dữ liệu. Chưa có xác thực/phân quyền; Ch17 bổ sung các phần đó. `totalMinor` được nhập trực tiếp để tập trung vào HTTP: chỉ phù hợp một caller nội bộ đáng tin trong lab. Checkout thật phải lấy giá từ catalog phía server và tính tổng trong transaction như Ch15, không tin số tiền do khách hàng gửi.

`for`, enum, opaque type thuộc ngôn ngữ Scala; `IO`/`Ref` thuộc Cats Effect; codec thuộc circe; `endpoint`/`Schema` thuộc Tapir; Ember thuộc http4s. Đừng xem các import của framework là cú pháp có sẵn của Scala.

## 3. Lý thuyết

### 3.1. Hợp đồng trước, router sau

**Vấn đề:** nếu mỗi handler tự chọn URL, status và body, client phải đoán cách gọi. Khi thêm endpoint mới, các lựa chọn dễ lệch nhau.

Hợp đồng (contract) ghi lại những gì client quan sát được qua HTTP. Nó không mô tả cấu trúc class nội bộ. Ta chốt tài nguyên, method và lỗi trước khi cài đặt:

| Method và path | Input | Thành công | Lỗi nghiệp vụ |
|---|---|---|---|
| `POST /api/v1/users` | `{name,email}` | 201 + UserView + Location | 422 validation, 409 email trùng |
| `GET /api/v1/users/{userId}` | Long dương | 200 + UserView | 422 ID không dương, 404 thiếu user |
| `GET /api/v1/users` | `q,sort,offset,limit` | 200 + UsersPage | 422 query ngoài quy tắc |
| `POST /api/v1/users/{userId}/orders` | `{totalMinor}` | 201 + OrderView + Location | 422 validation, 404 thiếu user |
| `GET /api/v1/orders/{orderId}` | Long dương | 200 + OrderView | 422 ID không dương, 404 thiếu order |
| `GET /api/v1/users/{userId}/orders` | ID, offset, limit | 200 + OrdersPage, sort ID tăng | 422 validation, 404 thiếu user |
| `PATCH /api/v1/orders/{orderId}/status` | `{status}` | 200 + OrderView | 422 ID, 404 thiếu order, 409 transition |

Mỗi endpoint còn có thể trả 400 do decode và 500 do exception; các endpoint có JSON body có thể trả 415 nếu media type không được hỗ trợ. Danh sách rỗng của **user tồn tại** là 200 với `items: []`; user không tồn tại là 404. ID hợp lệ về cú pháp nhưng không có trong repository khác với ID không hợp lệ.

Tapir ghép method, path, query và body thành một giá trị mô tả endpoint. Tách mô tả khỏi xử lý giúp dùng lại hợp đồng để sinh tài liệu.

`serverLogic` gắn hàm nghiệp vụ vào endpoint. Sau đó http4s interpreter tạo `HttpRoutes[IO]` để nhận request.

Query không nằm trong path: `/users?q=an` vẫn là endpoint danh sách. Segment cố định và số segment giúp phân biệt `/users/{id}` với `/users/{id}/orders`.

Bắt đầu bằng đường dẫn chung. Hai query bên dưới không tham gia đường dẫn; chúng được ghép vào endpoint danh sách ở mục 3.2.

Trích [Api.scala, từ dòng 36](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val users = endpoint.in("api" / "v1" / "users")
private val orders = endpoint.in("api" / "v1" / "orders")
private val offset = query[Int]("offset").default(0).description("0..1000000; số dòng bỏ qua")
private val limit = query[Int]("limit").default(20).description("1..100; số dòng tối đa")
```

Endpoint tạo user khai báo cả đầu vào và đầu ra. `errors(...)` là helper tạo `oneOf[Problem]`: mỗi status là một nhánh có cùng kiểu body. Helper tự thêm 400 và 500 vào các lỗi riêng của endpoint.

Trích [Api.scala, từ dòng 30](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private def variant(code: StatusCode): EndpointOutput.OneOfVariant[Problem] =
  oneOfVariantValueMatcher(code, problemBody) { case p: Problem => p.status == code.code }
private def errors(codes: StatusCode*): EndpointOutput.OneOf[Problem, Problem] =
  val all = List(StatusCode.BadRequest, StatusCode.InternalServerError) ++ codes
  oneOf[Problem](variant(all.head), all.tail.map(variant)*)
```

Trích [Api.scala, từ dòng 41](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
val createUser = users.post.name("createUser").in(oneOfBody(jsonBody[CreateUserRequest]))
  .errorOut(errors(StatusCode.Conflict, StatusCode.UnprocessableEntity, StatusCode.UnsupportedMediaType))
  .out(statusCode(StatusCode.Created)).out(jsonBody[UserView]).out(header[String]("Location"))
```

`oneOfBody` chỉ chấp nhận media type đã khai báo. Đầu ra thành công có HTTP 201, body `UserView` và header `Location` để client biết URL của user vừa tạo.

Hợp đồng chưa tự gọi repository. `result` đổi lỗi domain thành Problem và đổi dữ liệu thành DTO. `serverLogic` mới nối từng hợp đồng với service:

Trích [Api.scala, từ dòng 66](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private def result[A, B](io: IO[Either[DomainError, A]])(f: A => B): IO[Either[Problem, B]] =
  io.map(_.left.map(Problem.from).map(f))

def serverEndpoints(service: OrderService): List[ServerEndpoint[Any, IO]] = List(
  createUser.serverLogic(r => result(service.createUser(r.name, r.email))(u => (Views.user(u), s"/api/v1/users/${u.id.value}"))),
  getUser.serverLogic(id => result(service.getUser(id))(Views.user)),
  listUsers.serverLogic((q, sort, off, lim) => result(service.listUsers(q, sort, off, lim))(Views.users)),
  createOrder.serverLogic((id, r) => result(service.createOrder(id, r.totalMinor))(o => (Views.order(o), s"/api/v1/orders/${o.id.value}"))),
  getOrder.serverLogic(id => result(service.getOrder(id))(Views.order)),
  listOrders.serverLogic((id, off, lim) => result(service.listOrders(id, off, lim))(Views.orders)),
  updateStatus.serverLogic((id, r) => result(service.updateStatus(id, r.status))(Views.order))
)
```

Trong dòng `createUser.serverLogic`, tham số `r` là DTO đã decode. Nhánh thành công trả tuple `(UserView, Location)` đúng thứ tự hai output có dữ liệu của endpoint; status 201 là giá trị cố định. Các dòng sau áp dụng cùng cách nối cho sáu endpoint còn lại.

**Thay thế:** router http4s thuần như Ch13 linh hoạt hơn khi có streaming rất đặc thù. Đổi lại, người viết phải tự đồng bộ hợp đồng và tài liệu. Chọn Tapir khi cần nhiều endpoint có JSON và OpenAPI.

Tránh URL kiểu `/createUser`, dùng GET để ghi hoặc trả 200 cho mọi lỗi. Cache, retry và monitoring đều dựa vào ngữ nghĩa HTTP; các lựa chọn đó khiến chúng hiểu sai kết quả.

POST tạo mới **không idempotent**: gửi lại tạo thêm order. PATCH trong lab nhận **trạng thái đích tuyệt đối** và cho phép gửi lại cùng trạng thái, nên retry cùng lệnh không đổi kết quả nghiệp vụ. Đây là quy tắc của API này; không suy ra mọi PATCH đều idempotent. Thanh toán thật còn cần idempotency key và chống lặp side effect.

### 3.2. Parsing khác validation như thế nào?

**Vấn đề:** body có thể sai cú pháp JSON. Nó cũng có thể đúng JSON nhưng chứa dữ liệu sai nghiệp vụ. Hai trường hợp cần cách giải thích khác nhau cho client.

Nếu gộp cả hai thành exception, ta khó xác định bước nào từ chối request. Ta cũng bỏ lỡ cơ hội báo đủ các lỗi nghiệp vụ độc lập.

Luồng xử lý:

```text
HTTP bytes -> Tapir/circe decode -> DTO -> service validation -> domain -> repository
                 400/415                  422                 404/409 hoặc dữ liệu
```

- `limit=abc`: không đọc được Int → 400 trước khi service chạy.
- `limit=101`: đọc được Int, vi phạm quy tắc tối đa 100 → 422 ở service.
- `{"name":" ","email":"bad"}`: đọc được DTO, trả **hai lỗi cùng lúc**.
- `{"name":3}`: decode field không đúng kiểu → 400; không hứa tích lũy mọi decode failure.

`Validation.user`, `Validation.order`, `Validation.query` thu các lỗi độc lập vào `List[String]`. Chúng chỉ tạo kết quả hợp lệ khi danh sách rỗng.

Khi một bước phụ thuộc kết quả bước trước, dùng `Either`/for để dừng sớm là hợp lý. Chẳng hạn, `listOrders` kiểm tra ID trước rồi mới kiểm tra page.

Ta muốn người dùng sửa cả tên và email trong một lần gửi lại. Vì vậy hai kiểm tra độc lập phải chạy trước khi quyết định trả Left.

Trích [Model.scala, từ dòng 60](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala):

```scala
def user(name: String, email: String): Either[DomainError, UserDraft] =
  val n = name.trim
  val e = email.trim.toLowerCase(Locale.ROOT)
  val errors = List(
    Option.when(n.isEmpty || n.length > 100)("name phải có 1..100 ký tự"),
    Option.when(e.length > 254 || !e.matches("[^\\s@]+@[^\\s@]+\\.[^\\s@]+"))("email không hợp lệ")
  ).flatten
  if errors.nonEmpty then Left(DomainError.Invalid(errors)) else Right(UserDraft(n, e))
```

`Option.when` tạo một lỗi khi điều kiện đúng. `flatten` bỏ các `None` và giữ mọi lỗi còn lại. Chỉ khi danh sách rỗng, hàm mới tạo `UserDraft` đã chuẩn hóa.

Ở biên HTTP, kiểu Int và giá trị mặc định được khai báo như sau. Đoạn này đã xuất hiện cùng đường dẫn ở mục 3.1; đọc lại để phân biệt việc decode Int với kiểm tra khoảng hợp lệ:

Trích [Api.scala, từ dòng 38](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val offset = query[Int]("offset").default(0).description("0..1000000; số dòng bỏ qua")
private val limit = query[Int]("limit").default(20).description("1..100; số dòng tối đa")
```

Trích [Api.scala, từ dòng 46](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
val listUsers = users.get.name("listUsers")
  .in(query[Option[String]]("q").description("Chứa trong name hoặc email; không phân biệt hoa thường"))
  .in(query[String]("sort").default("id").description("id, name, email; '-' cho giảm dần; id là khóa phụ"))
  .in(offset).in(limit).errorOut(errors(StatusCode.UnprocessableEntity)).out(jsonBody[UsersPage])
```

`description` giải thích giới hạn cho người đọc OpenAPI; nó không phải validator thực thi. `Validation.query` và `PageRequest.from` mới kiểm tra khoảng giá trị tại service. Vì vậy `limit=101` đi qua decoder rồi nhận 422.

**Thay thế:** `ValidatedNel` của Cats giúp ghép validation lớn bằng `mapN`. Cách này tốt khi có nhiều cấu trúc lồng nhau, nhưng thêm abstraction người học phải biết. Lab dùng danh sách thuần để dễ theo dõi.

Đưa mọi validator vào Tapir giúp OpenAPI sinh constraint. Đổi lại, lỗi mặc định là 400 và decoder chỉ báo failure gặp trước. Ở đây quy tắc ngữ nghĩa được giữ trong domain/service; giới hạn được giải thích bằng description.

Đừng viết lại quy tắc name/email ở từng route. Cũng không dùng `.get` trên `Either` đầu vào để né việc xử lý nhánh lỗi.

Regex email chỉ là kiểm tra định dạng đơn giản, không chứng minh hộp thư tồn tại hay hỗ trợ mọi địa chỉ quốc tế. Chính sách lab trim + lowercase toàn bộ email bằng `Locale.ROOT`, xem email là khóa không phân biệt hoa thường; khi thay DB phải giữ cùng chính sách và unique constraint tương ứng.

### 3.3. Domain không phải JSON

**Vấn đề:** dùng `Long` cho mọi ID khiến compiler chấp nhận `getUser(orderId)`; dùng `Double` cho tiền gây sai số. Derive JSON trực tiếp từ domain lại khiến việc đổi tên field nội bộ làm vỡ client.

Ta cần compiler phân biệt ID người dùng với ID đơn hàng. Lab dùng `opaque type UserId = Long`, `OrderId = Long` và `Money = Long`. Bên ngoài scope định nghĩa, chúng là kiểu riêng và không tự chuyển cho nhau.

Kiểu riêng chưa đủ bảo đảm giá trị hợp lệ lúc chạy. `from` và `fromMinor` kiểm tra giá trị dương. Factory `assigned` được giới hạn trong package để repository cấp ID. Tính đúng vẫn phụ thuộc cách dùng các factory.

`UserDraft` mang dữ liệu đã chuẩn hóa theo hợp đồng service → repository. Constructor của nó không ngăn mọi caller tạo dữ liệu sai; service chịu trách nhiệm chuẩn hóa trước khi gọi repository.

Factory là cửa vào để kiểm tra dữ liệu trước khi có ID. Extension `.value` cho phép biên HTTP lấy lại Long khi tạo DTO:

Trích [Model.scala, từ dòng 5](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala):

```scala
opaque type UserId = Long
object UserId:
  def from(value: Long): Either[String, UserId] = Either.cond(value > 0, value, "userId phải dương")
  private[orders] def assigned(value: Long): UserId = value
  extension (id: UserId) def value: Long = id
```

Money cũng có factory riêng. Tên `fromMinor` nhắc caller rằng đầu vào đã ở đơn vị nhỏ nhất:

Trích [Model.scala, từ dòng 18](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala):

```scala
opaque type Money = Long
object Money:
  def fromMinor(value: Long): Either[String, Money] =
    Either.cond(value > 0, value, "totalMinor phải dương (VND)")
  extension (money: Money) def minor: Long = money
```

`Money` ở chương này chỉ biểu diễn **VND**, đơn vị nhỏ nhất là một đồng; `450000` là 450.000 VND. Không dùng tên `amount` mơ hồ rồi ngầm đoán đơn vị. USD có cent nên không thể tái sử dụng quy tắc VND mà bỏ currency. Bài tập 1 tính tổng bằng `BigInt` trung gian để phát hiện tràn trước khi trở về Long; chỉ đổi Double sang Long ở cuối thì đã quá muộn.

`OrderStatus` có bốn giá trị và các cạnh hợp lệ:

```text
Pending -> Paid -> Shipped
    └----> Cancelled
Mọi trạng thái -> chính nó (retry); các cạnh khác bị từ chối.
```

Để đổi cấu trúc domain mà vẫn giữ JSON cũ, ta cần một biên chuyển đổi rõ ràng. `User` và `Order` không import circe hay Tapir. `Transport.scala` chứa DTO và `TransportCodecs.given`. `Views` đổi domain thành DTO.

Enum được encode thành chuỗi `"Paid"`. Schema của Tapir cũng mô tả enum chuỗi. Codec chạy lúc nhận/trả JSON, còn Schema dùng sinh tài liệu. Đây là hai mô tả độc lập nên cần test giữ chúng khớp nhau.

Để client gửi `"Paid"`, ta chọn codec chuỗi tường minh thay vì phụ thuộc biểu diễn enum mặc định. Schema phải mô tả cùng lựa chọn đó:

Trích [Transport.scala, từ dòng 33](order-service/app/src/main/scala/vn/scalaeng/orders/Transport.scala):

```scala
object TransportCodecs:
  given Encoder[OrderStatus] = Encoder.encodeString.contramap(_.toString)
  given Decoder[OrderStatus] = Decoder.decodeString.emap(s =>
    OrderStatus.values.find(_.toString == s).toRight("status chỉ nhận Pending, Paid, Shipped, Cancelled"))
  given Schema[OrderStatus] = Schema.derivedEnumeration[OrderStatus].defaultStringBased
```

`Encoder` xử lý chiều domain → JSON. `Decoder` xử lý chiều JSON → enum và từ chối chuỗi ngoài bốn trạng thái. `Schema` phục vụ OpenAPI; nó không thay thế decoder. Các given này nằm ở app, nên module domain vẫn không phụ thuộc circe/Tapir.

**Thay thế:** case class bọc ID dễ inspect và có thể gắn nhiều phương thức. Nó có wrapper object tùy cách JVM tối ưu. Opaque type phù hợp khi cần phân biệt kiểu mà vẫn dùng biểu diễn Long.

BigDecimal phù hợp với số tiền cần phép chia/làm tròn có quy tắc rõ. Long theo đơn vị nhỏ nhất đơn giản cho lưu trữ và giao tiếp trong lab này.

Dùng DTO khi public contract cần tiến hóa độc lập. Với công cụ nội bộ nhỏ, encode domain trực tiếp có thể chấp nhận. Các lỗi thường gặp là enum thành `{"Paid":{}}`, lộ cấu trúc nội bộ hoặc chỉ dùng ID có kiểu rồi quên validation.

### 3.4. Ba lớp và hợp đồng repository

**Vấn đề:** route vừa parse JSON vừa sửa Map vừa quyết định trạng thái làm test khó và khóa HTTP vào cách lưu trữ. Ta chia trách nhiệm:

```text
Api/Transport (HTTP + JSON + status)
        | gọi
OrderService (validate + điều phối, trả DomainError)
        | phụ thuộc trait
OrderRepository (IO; hợp đồng lưu trữ nguyên tử)
        ^ được cài bởi
InMemoryOrderRepository (Ref[IO, State])

domain/ (model + quy tắc thuần) <- app/ (mọi phần có effect/HTTP)
```

`IO[Either[DomainError,A]]` phân biệt hai loại thất bại. `Left` là kết quả nghiệp vụ dự kiến, chẳng hạn email trùng. Failed IO biểu diễn lỗi hạ tầng hoặc bug.

Repository không trả `Problem` và trait không import kiểu HTTP/DB. Nhờ đó cùng lỗi nghiệp vụ có thể được trình bày khác nhau ở một biên giao tiếp khác. `getUser` và `getOrder` trả `Option`; service đổi None thành lỗi resource thiếu. Các method ghi trả Either để biểu diễn xung đột uniqueness, khóa ngoại (FK) hoặc chuyển trạng thái.

Hợp đồng đầy đủ ở [OrderRepository.scala](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala), gồm create/get/list users, create/get/list orders theo user, update status. Mỗi thao tác ghi sở hữu những điều kiện phải nguyên tử của nó. Repository không chỉ là lớp CRUD thụ động: cập nhật có điều kiện cần được thực thi ở nơi giữ trạng thái.

Đây là toàn bộ cổng lưu trữ mà service phụ thuộc. Người cài repository phải giữ ngữ nghĩa của từng method, dù dữ liệu nằm trong Ref hay DB:

Trích [OrderRepository.scala, từ dòng 11](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala):

```scala
trait OrderRepository:
  def createUser(draft: UserDraft): IO[Either[DomainError, User]]
  def getUser(id: UserId): IO[Option[User]]
  def listUsers(query: UserQuery): IO[Page[User]]
  def createOrder(userId: UserId, total: Money): IO[Either[DomainError, Order]]
  def getOrder(id: OrderId): IO[Option[Order]]
  def listOrders(userId: UserId, page: PageRequest): IO[Either[DomainError, Page[Order]]]
  def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]]
```

Service cần bỏ qua repository khi validation đã thất bại. Helper `run` biểu diễn đúng quyết định đó:

Trích [OrderService.scala, từ dòng 6](order-service/app/src/main/scala/vn/scalaeng/orders/OrderService.scala):

```scala
final class OrderService(repository: OrderRepository):
  private def run[A, B](input: Either[DomainError, A])(f: A => IO[Either[DomainError, B]]): IO[Either[DomainError, B]] =
    input.fold(e => IO.pure(Left(e)), f)
```

`input.fold` có hai nhánh. Left được bọc vào IO mà không gọi hàm `f`; Right mới truyền dữ liệu hợp lệ cho `f`. Vì thế validation là code thuần, còn đọc/ghi vẫn nằm trong IO.

**Nối Ch15:** Ch15 có repository trả `ConnectionIO` để service ghép nhiều câu SQL trong một transaction. Để cài trait `IO` của chương này, ta cần adapter. Adapter tạo `ConnectionIO[Either[DomainError,A]]` bên trong từng method rồi gọi `.transact(xa)`.

Unique email cần constraint DB. Create order cần khóa ngoại (FK). Transition cần row lock hoặc conditional UPDATE để giữ điều kiện nguyên tử.

Hai lời gọi repository trả IO nối bằng for **không tự thành một transaction**. Nếu use case mới cần trừ tồn kho, insert order và insert items cùng rollback, hãy thêm thao tác nguyên tử cấp use case hoặc dùng transaction algebra riêng. Không ghép các method đã transact riêng rồi tuyên bố có rollback chung.

Code Ch15 dùng package/model riêng, chưa phải adapter cắm trực tiếp vào lab này.

**Thay thế:** `OrderRepository[F[_]]` hỗ trợ nhiều effect và kiểm soát transaction linh hoạt hơn. Đổi lại, cần giải thích type class constraints và effect polymorphism. IO đủ cho stack đã chọn và dễ theo dõi hơn ở bước này.

Tránh tạo interface cho mọi hàm chỉ để đạt số lớp. Ở đây trait có mục đích cụ thể: thay Ref bằng DB và đưa lỗi giả có kiểm soát vào test (fault injection).

### 3.5. Ref không tự làm mọi chuỗi thao tác trở nên nguyên tử

**Vấn đề:** hai request cùng thấy email chưa tồn tại rồi cùng insert; hoặc cùng đọc Pending rồi ghi Paid/Cancelled, dẫn đến mất cập nhật. `Ref.get` an toàn cho một lần đọc, nhưng `get.flatMap(...set...)` gồm nhiều bước vẫn có race.

Điều kiện nghiệp vụ phải còn đúng ngay tại lúc ghi. Vì vậy `createUser`, `createOrder` và `updateStatus` dùng **một** `state.modify`. Callback đọc, kiểm tra, cập nhật và trả kết quả trong cùng thao tác.

`State` chứa users, orders và bộ cấp ID. Create order kiểm tra user và thêm order trên cùng snapshot. Listing chỉ đọc `state.get` một lần rồi filter, sort và page. Nhờ đó total và items trong **một response** khớp nhau.

Đọc hai cách viết cạnh nhau. Phiên bản **SAI** dưới đây chỉ nằm trong test minh họa; state được rút gọn còn Map orders để tập trung vào chỗ tranh chấp:

Trích [RefRaceExampleSuite.scala, từ dòng 11](order-service/app/src/test/scala/vn/scalaeng/orders/RefRaceExampleSuite.scala):

```scala
private def unsafeUpdateStatus(state: Ref[IO, Map[OrderId, Order]], id: OrderId,
    next: OrderStatus, afterRead: IO[Unit]): IO[Either[DomainError, Order]] =
  state.get.flatMap { snapshot =>
    afterRead *> (snapshot.get(id) match
      case None => IO.pure(Left(DomainError.UnknownOrder(id)))
      case Some(o) if !o.status.canMoveTo(next) => IO.pure(Left(DomainError.InvalidTransition(o.status, next)))
      case Some(o) =>
        val updated = o.copy(status = next)
        state.set(snapshot.updated(id, updated)).as(Right(updated)))
  }
```

`snapshot` được lấy trước khi ghi. Request Paid và Cancelled có thể cùng đọc Pending, cùng vượt kiểm tra rồi cùng báo thành công. Lần `set` sau ghi đè lần trước.

`afterRead` là điểm đồng bộ riêng của test. CountDownLatch chờ cả hai request đọc xong rồi mới cho chúng ghi, nên test tái hiện lỗi mà không cần sleep. Test pass khi quan sát được **hai Right** trái với hợp đồng cần bảo vệ; điều đó xác nhận phản ví dụ có lỗi nghiệp vụ.

Phiên bản dùng trong server gom việc đọc, kiểm tra và ghi vào cùng `modify`:

Trích [OrderRepository.scala, từ dòng 64](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala):

```scala
def updateStatus(id: OrderId, next: OrderStatus): IO[Either[DomainError, Order]] = state.modify { s =>
  s.orders.get(id) match
    case None => (s, Left(DomainError.UnknownOrder(id)))
    case Some(o) if !o.status.canMoveTo(next) => (s, Left(DomainError.InvalidTransition(o.status, next)))
    case Some(o) =>
      val updated = o.copy(status = next)
      (s.copy(orders = s.orders.updated(id, updated)), Right(updated))
}
```

Với Map nằm trong State của repository, kết quả callback là `(State, Either)`. Hai nhánh lỗi giữ nguyên `s`. Khi một request đã chuyển Pending thành Paid, request Cancelled phải kiểm tra lại trên trạng thái mới và nhận conflict. `RepositorySuite` kiểm tra rằng chỉ có một Right.

Hàm truyền vào `modify` phải thuần: có thể được thử lại khi tranh chấp. Không gửi email, chạy SQL, gọi đồng hồ hay sinh UUID ở trong đó. `Math.addExact` giúp không âm thầm quay vòng bộ cấp ID khi Long tràn; tình huống hết ID là failed IO, không phải một ID mới hợp lệ.

**Thay thế:** nhiều Ref giảm phạm vi tranh chấp (contention), nhưng khó giữ quan hệ users/orders. Synchronized/lock có thể chặn thread.

Một Ref đơn giản và phù hợp lab nhỏ. Nó không chia sẻ dữ liệu giữa hai instance và không thay thế DB phân tán. DB ở Ch15 cung cấp transaction, constraint và lưu bền.

Test tranh chấp dùng `parTraverse` để bắt các vi phạm thường gặp. Số tình huống test hữu hạn không phải chứng minh toán học cho mọi cách thực thi xen kẽ (interleaving).

### 3.6. Error handling: một định dạng, nhiều nguồn lỗi

**Vấn đề:** `errorOut` chỉ xử lý kết quả Left của endpoint. Request còn có thể thất bại ở những thời điểm khác, nên cần xác định từng nguồn lỗi.

JSON hỏng bị chặn trước logic. Exception đi theo nhánh failed IO. URL thiếu đi qua fallback http4s. Chỉ derive `Problem` không khiến các nhánh này tự dùng cùng một body lỗi.

Lab theo [Problem Details RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html): `type: about:blank`, title của HTTP status, `status`, `detail`, và extension `errors`. Response lỗi có `Content-Type: application/problem+json`. HTTP status là giá trị mà client/proxy xử lý; `status` trong body phải khớp nó.

| Nguồn | Nơi xử lý trong lab | Ví dụ |
|---|---|---|
| DomainError | `Problem.from` + endpoint `errorOut(oneOf(...))` | 404, 409, 422 |
| DecodeFailure | `DefaultDecodeFailureHandler.copy(response = ...)` | 400, 415; ví dụ 401 ở lời giải |
| Failed IO | `DefaultExceptionHandler` tùy biến body | 500, không lộ exception message |
| Không khớp method | `RejectHandler` đọc method mismatch, thêm Allow | 405 |
| Không khớp route | fallback HttpRoutes | 404 |

`problemBody` đổi **codec format** sang `application/problem+json`, thay vì chỉ sửa header sau khi đã sinh OpenAPI. `oneOfVariantValueMatcher` chọn output dựa trên `p.status`, để HTTP status và body đến từ cùng một giá trị.

Trước hết, body lỗi cần có media type riêng ngay trong codec. Nhờ vậy runtime và OpenAPI cùng biết đây là Problem JSON:

Trích [Api.scala, từ dòng 24](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val jsonProblem = jsonBody[Problem]
private val problemFormat = new CodecFormat:
  val mediaType: MediaType = MediaType.unsafeParse("application/problem+json")
val problemBody: EndpointIO.Body[String, Problem] =
  jsonProblem.copy(codec = jsonProblem.codec.format(problemFormat))
```

Decode failure và exception xảy ra ngoài nhánh Left của nghiệp vụ, nên cần hai handler riêng:

Trích [Api.scala, từ dòng 79](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val decodeFailures = DefaultDecodeFailureHandler[IO].copy[IO](response = (code, hdrs, msg) =>
  ValuedEndpointOutput(statusCode.and(headers).and(problemBody), (code, hdrs, Problem.of(code.code, msg))))
private val exceptions = DefaultExceptionHandler[IO]((code, msg) =>
  ValuedEndpointOutput(statusCode.and(problemBody), (code, Problem.of(code.code, msg))))
```

Callback decode nhận `code`, `hdrs`, `msg` từ Tapir. Body dùng `code.code`, còn output giữ nguyên headers. Exception handler dùng thông điệp do handler mặc định cung cấp; test fault injection ở lời giải kiểm tra rằng bí mật trong exception không xuất hiện ở response 500.

Với method sai, ta cần biết các method hợp lệ của đúng path. Handler sau thu các FixedMethod đã mismatch, loại trùng rồi tạo 405 với `Allow`:

Trích [Api.scala, từ dòng 84](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val rejections = RejectHandler.pure[IO] { ctx =>
  val methods = ctx.failure.failures.flatMap(_.failingInput match
    case EndpointInput.FixedMethod(method, _, _) => Some(method.method)
    case _ => None).distinct.sorted
  Option.when(methods.nonEmpty)(ValuedEndpointOutput(
    statusCode.and(header[String]("Allow")).and(problemBody),
    (StatusCode.MethodNotAllowed, methods.mkString(", "), Problem.of(405, "Method Not Allowed"))))
}
```

Các handler phải được gắn vào interpreter thì mới có tác dụng:

Trích [Api.scala, từ dòng 92](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala):

```scala
private val options: Http4sServerOptions[IO] = Http4sServerOptions.customiseInterceptors[IO]
  .decodeFailureHandler(decodeFailures).exceptionHandler(exceptions).rejectHandler(rejections).options
```

Fallback 404 của http4s vẫn được xử lý riêng trong `Api.httpApp`. Nó cần thiết cho URL không khớp endpoint nào.

Vì sao cần `oneOfBody(jsonBody[...])` ở đầu vào? Với phiên bản Tapir đang dùng, `jsonBody` đơn lẻ vẫn thử decode body bất kể Content-Type. Có codec JSON chưa đủ để từ chối text/plain.

`oneOfBody` kiểm tra media type application/json và vẫn chấp nhận charset UTF-8. Cả text/plain và application/json có charset đều đã được gọi curl thật.

Reject handler chỉ thu method mismatch sau khi path đã khớp. Nhờ đó response 405 có `Allow` đúng cho path đang được gọi.

Điểm nối với [Ch17, mục 10.2](ch17-security-production.md#10-lỗi-thường-gặp-thật): dùng `DefaultDecodeFailureHandler[IO].copy[IO](response = (code,hdrs,msg) => ...)`. Callback nhận status/header Tapir đã chọn. Không dùng hàm chỉ nhận message rồi gán cứng Problem 400: thiếu bearer token sẽ có HTTP 401 nhưng body 400, hoặc làm mất `WWW-Authenticate`. Bài tập 6 kiểm chứng trường hợp này mà chưa dạy xác thực thật.

**Thay thế:** lỗi JSON tự thiết kế dễ làm lúc đầu, nhưng client phải học nhiều định dạng khi gọi nhiều service. Plain text thuận tiện debug nhưng khó xử lý có cấu trúc. Problem Details phù hợp API công khai hoặc có nhiều consumer.

Không đưa stack trace, SQL hay bí mật vào detail của 500. Ch17 bổ sung log request ID, exception có kiểm soát và logger backend. Lab này chưa thay thế phần vận hành đó.

### 3.7. Lọc, sort và pagination phải là một thiết kế

**Vấn đề:** trả toàn bộ dữ liệu tốn RAM/băng thông; chia trang không có thứ tự ổn định khiến user thấy lặp/mất dòng. Chốt trình tự **filter → sort → page**. `total` là số dòng sau filter, trước page.

Users nhận `q` để tìm chuỗi con trong name/email. Phép tìm kiếm không phân biệt hoa thường nhưng không bỏ dấu.

`sort` chỉ nhận `id`, `name`, `email`; thêm `-` phía trước để giảm dần. Khi sort theo name, nhiều user có thể trùng tên. Repository dùng ID làm khóa phụ (tie-breaker) cho cả name và email để quy tắc thứ tự nhất quán. Cả tuple được đảo khi giảm dần, nên ID phụ cũng giảm dần.

Không nhận tên cột tùy ý từ client. Khi sang Ch15, whitelist còn ngăn việc ghép tên cột SQL không an toàn. Bind parameter chỉ dành cho **giá trị**; nó không thay thế tên cột.

Whitelist biến chuỗi không đáng tin thành một enum nhỏ. Tên sort khác ba lựa chọn dưới đây trở thành lỗi validation, không được đưa tiếp xuống repository:

Trích [Model.scala, từ dòng 43](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala):

```scala
enum UserSort:
  case Id, Name, Email
```

Trích [Model.scala, từ dòng 76](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala):

```scala
def query(q: Option[String], sort: String, offset: Int, limit: Int): Either[DomainError, UserQuery] =
  val descending = sort.startsWith("-")
  val field = sort.stripPrefix("-") match
    case "id" => Right(UserSort.Id)
    case "name" => Right(UserSort.Name)
    case "email" => Right(UserSort.Email)
    case _ => Left("sort chỉ nhận id, name, email (thêm '-' để giảm dần)")
  val page = PageRequest.from(offset, limit)
  val errors = field.left.toOption.toList ++ page.left.toOption.toList.flatten ++
    Option.when(q.exists(_.length > 100))("q tối đa 100 ký tự").toList
  if errors.nonEmpty then Left(DomainError.Invalid(errors))
  else (for f <- field; p <- page.left.map(_.mkString(", "))
        yield UserQuery(q.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty), f, descending, p))
    .left.map(e => DomainError.Invalid(List(e)))
```

Sau validation, repository không cần diễn giải lại tên sort. Nó lọc, chọn thứ tự bằng pattern matching rồi mới gọi `pageOf`:

Trích [OrderRepository.scala, từ dòng 39](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala):

```scala
def listUsers(query: UserQuery): IO[Page[User]] = state.get.map { s =>
  val filtered = s.users.values.toList.filter(u => query.q.forall(q =>
    u.name.toLowerCase(Locale.ROOT).contains(q) || u.email.contains(q)))
  val sorted = query.sort match
    case UserSort.Id => filtered.sortBy(_.id.value)
    case UserSort.Name => filtered.sortBy(u => (u.name.toLowerCase(Locale.ROOT), u.id.value))
    case UserSort.Email => filtered.sortBy(u => (u.email, u.id.value))
  pageOf(if query.descending then sorted.reverse else sorted, query.page)
}
```

Trích [OrderRepository.scala, từ dòng 25](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala):

```scala
private def pageOf[A](all: List[A], p: PageRequest): Page[A] =
  Page(all.slice(p.offset, p.offset + p.limit), all.size, p.offset, p.limit)
```

`all` của `pageOf` là danh sách đã lọc và sắp xếp. Vì vậy `all.size` là total trước khi slice; số phần tử trả trong trang có thể nhỏ hơn total.

`offset` mặc định 0, trong 0..1.000.000; `limit` mặc định 20, trong 1..100. Đây là giới hạn hợp đồng của lab, không phải chuẩn HTTP. Offset vượt số dòng hiện có vẫn 200 với trang rỗng và total đúng. Orders theo user nhận cùng offset/limit và sort ID tăng; chưa nhận `q`/`sort` như users.

| Thiết kế | Cách gọi | Ưu | Nhược / khi dùng |
|---|---|---|---|
| Offset/limit | `?offset=20&limit=20` | Đơn giản, nhảy đến trang tùy ý | Offset sâu tốn chi phí DB; insert/delete giữa hai lần đọc có thể làm dịch trang. Dùng danh sách nhỏ/admin |
| Cursor/keyset | `?after=123&limit=20` | Truy vấn theo khóa sau cùng, hợp feed lớn | Khó nhảy trang; cursor phải bao gồm khóa sort phụ. Dùng dữ liệu nhiều/chảy liên tục |

ID tie-break chỉ làm thứ tự xác định **trên một tập dữ liệu**. Nó không tạo snapshot xuyên nhiều request. Nếu sort theo name, cursor phải có `(name,id)`; rename vẫn có thể làm dòng dịch chuyển giữa các trang.

Không dùng offset được encode base64 rồi gọi đó là keyset. Bài tập 3 dùng ID tăng dần và lấy dư một dòng để quyết định next cursor. Cách này không hứa snapshot cho toàn bộ lượt duyệt.

### 3.8. Versioning và OpenAPI

**Vấn đề:** client cũ vẫn cần hoạt động sau khi server thay đổi. Trước mỗi thay đổi hợp đồng, ta cần hỏi client hiện tại có tiếp tục hiểu response và gửi request hợp lệ hay không.

Lab dùng path `/api/v1` vì phiên bản hiện rõ trong URL, curl, log và routing. Thêm field optional thường có thể tương thích. Đổi đơn vị tiền, xóa field bắt buộc hoặc đổi ý nghĩa status cần đánh giá breaking change. Những thay đổi đó thường cần v2 và lộ trình chuyển đổi.

**Thay thế:** header `API-Version` giữ URL gọn, nhưng cần thiết kế cache key/Vary và tooling. Media type có version qua `Accept` hỗ trợ content negotiation, đổi lại khó thao tác hơn.

Path versioning phù hợp giáo trình và public API cần dễ tra cứu. Không tạo v2 cho mọi lần deploy. Test `/api/v2/users` hiện trả 404 vì chưa có v2.

Muốn tài liệu theo kịp route, ta dùng lại cùng tập endpoint. `Api.endpoints` là nguồn cho cả route và `OpenAPIDocsInterpreter`. YAML được sinh từ đó và phục vụ ở `/docs/openapi.yaml`.

Tài liệu mô tả body, status, Location, query và enum. Các giới hạn business chưa có Schema validator được giải thích bằng description.

OpenAPI không tự suy ra cạnh chuyển trạng thái, tính nguyên tử hay quy tắc retry của POST. Những quy tắc đó vẫn cần văn bản và test.

Swagger UI là lựa chọn thêm, không bắt buộc để có OpenAPI. Lab phục vụ YAML trực tiếp, không thêm artifact UI. Đừng viết YAML bằng tay song song rồi để schema lệch khỏi codec; test tài liệu cần đi cùng test response thật. Các lỗi middleware chung như 404 route/405 không được liệt kê như lỗi nghiệp vụ của từng operation.

### 3.9. Các lớp kiểm thử bổ sung nhau

**Vấn đề:** unit test service pass chưa bảo đảm JSON, schema và HTTP status đúng. Nếu chỉ dùng curl để kiểm tra mọi nhánh, mỗi lần chạy lại chậm và phụ thuộc cổng cố định.

Ta cần nhiều lớp test vì mỗi lớp quan sát một loại lỗi khác nhau:

1. `DomainSuite`: quy tắc thuần, bao phủ đủ 16 cặp chuyển trạng thái.
2. `RepositorySuite`: Ref, duplicate đồng thời, cập nhật cạnh tranh, filter/sort/page.
3. `ContractSuite`: gọi **HttpApp thật** như một hàm, không mở socket; kiểm tra UTF-8, Location, status/body/media type, mọi endpoint và lỗi decode.
4. `OpenApiSuite`: đường dẫn, operationId, status, media type và enum trong YAML sinh ra.
5. `SocketSuite`: Ember server + Ember client trong `Resource`, bind `127.0.0.1:0` để OS chọn cổng trống; chạy tạo user → tạo order → Paid → GET → query sai.

Contract test nên khẳng định điều client thấy. Test sau gọi `HttpApp.run` trực tiếp, tạo user rồi GET lại theo cùng HTTP contract:

Trích [ContractSuite.scala, từ dòng 24](order-service/app/src/test/scala/vn/scalaeng/orders/ContractSuite.scala):

```scala
test("POST user -> 201 Location -> GET, UTF-8 round trip"):
  for
    a <- app
    r <- a.run(request(Method.POST, "/api/v1/users", """{"name":"Nguyễn An","email":" AN@SHOP.VN "}"""))
    u <- read[UserView](r)
    g <- a.run(request(Method.GET, "/api/v1/users/1"))
    got <- read[UserView](g)
  yield
    assertEquals(r.status, Status.Created)
    assertEquals(r.headers.get(org.typelevel.ci.CIString("Location")).map(_.head.value), Some("/api/v1/users/1"))
    assertEquals(got, u)
    assertEquals((u.name, u.email), ("Nguyễn An", "an@shop.vn"))
```

`app`, `request`, `read` là helper trong [Fixtures.scala](order-service/app/src/test/scala/vn/scalaeng/orders/Fixtures.scala). `app` tạo repository mới cho mỗi test. Request có body sẽ được gắn application/json; `read` decode body thật bằng circe. Không có socket trong luồng trên.

Socket test bổ sung bằng cách mở đúng server và client. Đây là phần acquire tài nguyên của `SocketSuite`; phần gửi request và assert nằm tiếp ngay sau trong file:

Trích [SocketSuite.scala, từ dòng 12](order-service/app/src/test/scala/vn/scalaeng/orders/SocketSuite.scala):

```scala
class SocketSuite extends CatsEffectSuite:
  test("socket thật cổng 0: tạo user, order, cập nhật status, đọc lại và decode failure"):
    val resources = for
      a <- Resource.eval(app)
      server <- EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0").withHttpApp(a).build
      client <- EmberClientBuilder.default[IO].build
    yield (Uri.unsafeFromString(s"http://127.0.0.1:${server.address.getPort}"), client)
```

Cổng 0 yêu cầu hệ điều hành chọn cổng trống. Test lấy lại cổng thực từ `server.address`, rồi dùng `resources.use` để chạy request. Khi use kết thúc hoặc assertion fail, Resource đóng client và server. Không cần đoán thời gian khởi động bằng sleep.

Unit/contract tests không cần Internet hay Docker sau khi dependency đã tải. Socket test chỉ dùng loopback máy hiện tại; không gọi dịch vụ bên ngoài và đóng server/client kể cả khi assertion fail. **Chưa cần** integration test PostgreSQL ở chương này. Khi thêm DB/Docker, tách suite và `assume` khi thiếu môi trường như quy ước repo; không bỏ qua những test Ref hiện có.

**Thay thế:** mock HTTP client không kiểm tra được router/codec của server. Chạy server thật cho mọi case lại chậm hơn và khó cô lập. Vì vậy ta dùng nhiều test không mạng và một số luồng socket có giá trị.

Khi thay repository DB, nên chạy lại cùng contract trên cách ghép thành phần (wiring) mới. Chỉ giữ test SQL riêng chưa chứng minh HTTP contract còn đúng.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong chương |
|---|---|
| Routing | Chọn endpoint bằng method/path |
| Parsing / decoding | Đọc dữ liệu trên dây thành giá trị có kiểu |
| Serialization / encoding | Biến giá trị thành JSON/bytes trả client |
| Codec | Cặp encoder và decoder; không tự xác thực nghiệp vụ |
| DTO | Data Transfer Object, cấu trúc dữ liệu ở biên giao tiếp |
| Service layer | Tầng validation và điều phối use case |
| Repository | Cổng lưu trữ có hợp đồng, che cách lưu dữ liệu |
| Atomic operation | Một thao tác không lộ trạng thái cập nhật dở dang |
| Snapshot | Ảnh chụp trạng thái tại một thời điểm đọc |
| Whitelist | Tập lựa chọn được phép, mặc định từ chối phần còn lại |
| Pagination / keyset | Chia trang / chia trang dựa trên khóa sau cùng |
| Idempotency | Lặp lại cùng lệnh không đổi kết quả nghiệp vụ sau lần đầu |
| Contract test | Test những gì client quan sát, độc lập cách lưu trữ |
| Optimistic concurrency | Kiểm tra version khi ghi để phát hiện cập nhật cạnh tranh |

## 5. Ví dụ đơn giản

Đọc `Api.getUser` rồi lần theo một request: Tapir đọc `userId` thành Long, `OrderService.getUser` kiểm tra dương, repository trả Option, service đổi None thành DomainError, API đổi thành Problem. Kết quả thành công đi qua `Views.user` và circe.

Trích [OrderService.scala, từ dòng 16](order-service/app/src/main/scala/vn/scalaeng/orders/OrderService.scala):

```scala
def getUser(raw: Long): IO[Either[DomainError, User]] =
  run(userId(raw))(id => repository.getUser(id).map(_.toRight(DomainError.UnknownUser(id))))
```

`run` là helper private trong `OrderService`: Left validation trở thành `IO.pure(Left(...))`, Right mới gọi repository. Không cần `unsafeRunSync` hay catch exception để biểu diễn “user chưa tồn tại”. Chạy ví dụ bằng lệnh server ở mục 7 và GET `/api/v1/users/99`; output thật ở mục 9.

## 6. Ví dụ thực tế — tiếp nhận đơn hàng

Tạo user Nguyễn An, lấy Location, tạo đơn 450.000 VND cho user đó; order bắt đầu Pending. PATCH sang Paid, đọc lại và list theo user. Gửi lại Paid thành công; chuyển Paid → Cancelled bị từ chối vì chính sách lab yêu cầu quy trình hoàn tiền riêng.

Các quyết định thiết kế nhìn thấy được qua luồng này: service không được tạo order mồ côi, ID user và ID order có namespace riêng, tiền không bị đổi đơn vị khi serialize, mutation trạng thái phải kiểm tra trên giá trị **hiện tại**, và lỗi conflict không được ghi đè state. API/backend nhận request đồng thời; không giả định hai lần curl luôn đại diện cho mọi cách request có thể xen nhau.

## 7. Code đầy đủ và cách chạy

```text
05-backend/order-service/
├── build.sbt                    root aggregate(domain, app), app dependsOn(domain)
├── project/build.properties     sbt 1.12.11
├── project.scala                Scala CLI gom hai module
├── domain/src/main/scala/vn/scalaeng/orders/Model.scala
├── domain/src/test/scala/vn/scalaeng/orders/DomainSuite.scala
├── app/src/main/scala/vn/scalaeng/orders/
│   ├── OrderRepository.scala     trait + Ref implementation
│   ├── OrderService.scala        validation + use case
│   ├── Transport.scala           DTO + codecs + mapping
│   ├── Api.scala                 bảy endpoint + handlers + YAML
│   └── Main.scala                IOApp + Ember Resource
├── app/src/test/scala/vn/scalaeng/orders/
│   ├── Fixtures.scala
│   ├── RepositorySuite.scala
│   ├── ContractSuite.scala
│   ├── OpenApiSuite.scala
│   ├── SocketSuite.scala
│   └── RefRaceExampleSuite.scala   phản ví dụ get/set, chỉ có trong test
├── requests/                    body UTF-8 dùng với curl.exe
└── evidence/                    output thật + nguồn phiên bản
```

Đọc [Model](order-service/domain/src/main/scala/vn/scalaeng/orders/Model.scala), [repository](order-service/app/src/main/scala/vn/scalaeng/orders/OrderRepository.scala), [service](order-service/app/src/main/scala/vn/scalaeng/orders/OrderService.scala), [transport](order-service/app/src/main/scala/vn/scalaeng/orders/Transport.scala) và [HTTP](order-service/app/src/main/scala/vn/scalaeng/orders/Api.scala) theo thứ tự này. Code đầy đủ nằm ở các file, không phải các fragment cần tự đoán import.

Từ thư mục gốc repo, chạy build nhiều module bằng sbt:

```powershell
cd 05-backend/order-service
sbt.bat test
sbt.bat "app/run"
```

Hoặc dùng Scala CLI từ thư mục `order-service`:

```powershell
scala-cli.bat test .
scala-cli.bat run . --main-class vn.scalaeng.orders.Main
```

Khi server đã chạy, mở terminal khác tại cùng thư mục `order-service`:

```powershell
curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/user.json' http://127.0.0.1:8084/api/v1/users
curl.exe -i http://127.0.0.1:8084/api/v1/users/1
curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/order.json' http://127.0.0.1:8084/api/v1/users/1/orders
curl.exe -i -X PATCH -H "Content-Type: application/json" --data-binary '@requests/paid.json' http://127.0.0.1:8084/api/v1/orders/1/status
curl.exe -i 'http://127.0.0.1:8084/api/v1/users?q=AN&sort=-name&offset=0&limit=2'
curl.exe -i http://127.0.0.1:8084/docs/openapi.yaml
```

Dùng `curl.exe` để tránh alias PowerShell, và `--data-binary @file` để body tiếng Việt không bị thay encoding. Gửi lại `user.json` trên server hiện có sẽ ra 409; muốn lặp lại luồng từ ID 1, dừng Ctrl+C rồi chạy lại server. Các lệnh không mạng vẫn cần tải dependency lần đầu.

Muốn chỉ in YAML bằng sbt, chạy `sbt.bat "app/runMain vn.scalaeng.orders.PrintOpenApi"`. Root chỉ aggregate, nên `run` không tự chọn app. `build.sbt` giữ domain không phụ thuộc HTTP/JSON/IO. Scala CLI gom source để kiểm chứng thuận tiện, nhưng không kiểm tra ranh giới module như sbt.

**Ghi chú cho môi trường hạn chế như sandbox Codex:** nếu Bloop bị chặn, dùng `--server=false`. Đặt cache trong project khi môi trường không cho ghi cache mặc định. Đây là cấu hình theo môi trường, không phải bước bắt buộc của lab:

```powershell
$env:COURSIER_CACHE = Join-Path (Get-Location) '.cache/coursier'
scala-cli.bat test . --server=false --scalac-option -Werror
scala-cli.bat run . --server=false --main-class vn.scalaeng.orders.Main
```

`.cache/` đã được ignore. `-Werror` được dùng khi kiểm chứng để compiler từ chối mọi cảnh báo.

## 8. Giải thích code

Hãy đọc code theo đường đi của một request tạo user. Mục tiêu là biết mỗi quyết định nằm ở đâu, để khi test fail ta tìm đúng lớp cần sửa.

**Đầu vào HTTP → DTO.** `Api.createUser` xác định path, method và body. Tapir/circe decode JSON thành `CreateUserRequest` trước khi chạy `serverLogic`. Sai cú pháp hoặc sai media type được xử lý ở biên này, chưa gọi service.

**DTO → domain hợp lệ.** `OrderService` nhận primitive từ DTO rồi gọi validation. `Validation.user` tạo dữ liệu mới đã trim/lowercase; nó không sửa DTO bằng mutation. Các use case khác đổi Long thành ID/Money trước khi gọi trait. Enum giữ chính sách transition thuần và không biết HTTP status.

**Domain → repository.** Helper `run` chỉ gọi repository ở nhánh Right. IO mô tả tác vụ; việc tạo IO chưa chạy tác vụ ngay. Main hoặc test runtime mới thực thi nó.

**Repository → state mới.** `state.modify` trả tuple `(state mới, kết quả)`. Khi gặp lỗi nghiệp vụ, callback trả lại `s` nguyên vẹn, nên ID không tăng trên nhánh thất bại. Map là bất biến; trạng thái chia sẻ chỉ đổi qua Ref. Đọc mục 3.5 để thấy vì sao một lần get rồi một lần set vẫn sai dù từng thao tác riêng lẻ an toàn.

**Một lần đọc danh sách.** Repository lấy state một lần để total và items cùng thuộc một snapshot. Nó sort theo `(nameLower,id)` để không phụ thuộc thứ tự duyệt Map. Với giảm dần, nó reverse cả danh sách đã sort rồi mới lấy page.

**Domain → DTO → JSON.** `Views` chọn field công khai và lấy Long từ opaque ID/Money. `TransportCodecs` giữ codec ở biên app. Trong Scala 3, cần `import TransportCodecs.given` để đưa các given ẩn danh vào scope; `import TransportCodecs.*` thông thường không đưa chúng vào. Test đọc body text rồi decode bằng circe, tránh wildcard entity decoder cho String như lưu ý Ch17.

**Kết quả nghiệp vụ → HTTP.** `Api.result` map Right sang DTO và Left sang Problem. Nó không bắt failed IO, vì lỗi đó cần đi tới exception handler. Nếu nuốt mọi exception thành 404, client sẽ nhầm lỗi hạ tầng với user không tồn tại.

**Lỗi trước hoặc ngoài logic.** Decode handler giữ `code` và `hdrs` mà Tapir đã chọn, chỉ thay body. Content-Type của Problem đến từ codec format nên tài liệu và response có cùng media type. Reject handler xử lý method sai; fallback http4s xử lý route thiếu. Thiếu một trong các nhánh này sẽ khiến định dạng lỗi không nhất quán.

**Vòng đời process.** Main tạo repository một lần rồi dùng chung cho các request. Nếu tạo Ref trong mỗi handler, GET sẽ không thấy POST trước đó. Ember bind loopback 8084; `useForever` giữ Resource cho đến khi process bị dừng.

**Vòng đời test.** Mỗi test tạo repository riêng để không phụ thuộc thứ tự suite. Contract test gọi HttpApp như hàm. Socket test dùng Resource acquire server/client rồi mới gửi request tới cổng 0 đã được cấp; không sleep để đoán server sẵn sàng.

## 9. Output mong đợi — ghi lại từ lần chạy thật

Kiểm chứng ngày **17/10/2026 theo giờ máy (UTC+7)**, HTTP Date bên dưới dùng UTC. [Transcript đầy đủ](order-service/evidence/curl-transcript.txt) giữ nguyên stdout của `curl.exe -i`; bản trích dưới bỏ Date, Connection, Content-Length. [capture_http.py](order-service/evidence/capture_http.py) gọi 22 request, kiểm tra status và status trong Problem; chạy script khi Main vừa khởi động, RAM rỗng.

**Tạo user rồi đọc lại:**

```text
$ curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/user.json' http://127.0.0.1:8084/api/v1/users
HTTP/1.1 201 Created
Location: /api/v1/users/1
Content-Type: application/json

{"id":1,"name":"Nguyễn An","email":"an@scalaeng.vn"}

$ curl.exe -i http://127.0.0.1:8084/api/v1/users/1
HTTP/1.1 200 OK
Content-Type: application/json

{"id":1,"name":"Nguyễn An","email":"an@scalaeng.vn"}
```

**Validation và lỗi decode không cùng loại:**

```text
$ curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/invalid-user.json' http://127.0.0.1:8084/api/v1/users
HTTP/1.1 422 Unprocessable Content
Content-Type: application/problem+json

{"type":"about:blank","title":"Unprocessable Content","status":422,"detail":"dữ liệu không hợp lệ","errors":["name phải có 1..100 ký tự","email không hợp lệ"]}

$ curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/broken.json' http://127.0.0.1:8084/api/v1/users
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json

{"type":"about:blank","title":"Bad Request","status":400,"detail":"Invalid value for: body (exhausted input)","errors":[]}

$ curl.exe -i 'http://127.0.0.1:8084/api/v1/users?limit=abc'
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json

{"type":"about:blank","title":"Bad Request","status":400,"detail":"Invalid value for: query parameter limit","errors":[]}

$ curl.exe -i -H "Content-Type: text/plain" --data-binary '@requests/user.json' http://127.0.0.1:8084/api/v1/users
HTTP/1.1 415 Unsupported Media Type
Content-Type: application/problem+json

{"type":"about:blank","title":"Unsupported Media Type","status":415,"detail":"Invalid value (value mismatch)","errors":[]}
```

**Filter/sort/page và luồng order:**

```text
$ curl.exe -i 'http://127.0.0.1:8084/api/v1/users?q=AN&sort=-name&offset=0&limit=2'
HTTP/1.1 200 OK
Content-Type: application/json

{"items":[{"id":1,"name":"Nguyễn An","email":"an@scalaeng.vn"}],"total":1,"offset":0,"limit":2}

$ curl.exe -i -H "Content-Type: application/json" --data-binary '@requests/order.json' http://127.0.0.1:8084/api/v1/users/1/orders
HTTP/1.1 201 Created
Location: /api/v1/orders/1
Content-Type: application/json

{"id":1,"userId":1,"totalMinor":450000,"currency":"VND","status":"Pending"}

$ curl.exe -i -X PATCH -H "Content-Type: application/json" --data-binary '@requests/paid.json' http://127.0.0.1:8084/api/v1/orders/1/status
HTTP/1.1 200 OK
Content-Type: application/json

{"id":1,"userId":1,"totalMinor":450000,"currency":"VND","status":"Paid"}

$ curl.exe -i -X PATCH -H "Content-Type: application/json" --data-binary '@requests/cancelled.json' http://127.0.0.1:8084/api/v1/orders/1/status
HTTP/1.1 409 Conflict
Content-Type: application/problem+json

{"type":"about:blank","title":"Conflict","status":409,"detail":"không thể chuyển Paid sang Cancelled","errors":[]}
```

**OpenAPI được sinh từ đúng bảy endpoint:** GET `/docs/openapi.yaml` trả `Content-Type: application/yaml; charset=UTF-8`, bắt đầu bằng đoạn thật sau; [toàn bộ YAML](order-service/evidence/openapi.yaml) có 5 path, 7 operationId và enum trạng thái dạng chuỗi.

```yaml
openapi: 3.1.0
info:
  title: Users and Orders API
  version: 1.0.0
paths:
  /api/v1/users:
    get:
      operationId: listUsers
```

**Test thật vòng 2:** chạy `scala-cli.bat test . --server=false --scalac-option -Werror` tại order-service, 0 compiler warning, **26 pass/0 fail/0 ignored**. `--server=false` là cấu hình sandbox như mục 7. Output tóm tắt nguyên văn từ [tests-round2.txt](order-service/evidence/tests-round2.txt):

```text
Test run vn.scalaeng.orders.ContractSuite finished: 0 failed, 0 ignored, 11 total 1.77s
Test run vn.scalaeng.orders.DomainSuite finished: 0 failed, 0 ignored, 5 total 0.016s
Test run vn.scalaeng.orders.OpenApiSuite finished: 0 failed, 0 ignored, 2 total 0.008s
Test run vn.scalaeng.orders.RefRaceExampleSuite finished: 0 failed, 0 ignored, 1 total 0.024s
Test run vn.scalaeng.orders.RepositorySuite finished: 0 failed, 0 ignored, 6 total 0.049s
Test run vn.scalaeng.orders.SocketSuite finished: 0 failed, 0 ignored, 1 total 0.683s
```

Lời giải chạy lại **6/6 test pass**, gồm fault injection 500 và decode 401; xem [log vòng 2](../solutions/ch14-code/test-output-round2.txt). Compiler không cảnh báo. Scala CLI vẫn có **hint** nâng version munit/munit-cats-effect, nhưng giữ phiên bản theo chuẩn repo (2.2.1 kéo munit 1.3.6). Runtime chưa cấu hình SLF4J binding nên có thông báo ở mục 10.5.

Claude đã chạy sbt **25/25** trước vòng sửa này: domain 5, app 20, theo [review T03](../.team/reviews/T03-ch14-rest-apis-review.md). Vòng 2 thêm một test phản ví dụ ở app; Scala CLI đã chạy cả 26 test. **Codex không chạy lại sbt** trong sandbox; lead cần kiểm tra lại bản sửa này. Kết quả sbt 25/25 không được dùng để tuyên bố test mới đã chạy bằng sbt.

Mọi khối Scala trong bài đều trích nguyên từ source đã biên dịch, chỉ bỏ thụt lề chung để trình bày. Có thể đối chiếu lại bằng `python 05-backend/order-service/evidence/verify_snippets.py` từ root repo. Script kiểm tra đoạn trích khớp source, không thay thế việc biên dịch và chạy test.

## 10. Lỗi thường gặp — đối chiếu thông báo thật

**10.1. Parse được Int không có nghĩa query hợp lệ.** `limit=abc` có detail thật `Invalid value for: query parameter limit`; `limit=101` đi đến validation với lỗi `limit phải trong 1..100`. Khi thêm validator Tapir, hãy cập nhật test vì status có thể đổi từ 422 sang 400.

**10.2. Nhầm MediaType debug string với header.** Lần viết test đầu, assertion nhận `MediaType(application/problem+json)` thay vì chuỗi `application/problem+json`. Đổi sang `.renderString` lại gây lỗi compiler thật, lưu tại [compile-error.txt](order-service/evidence/compile-error.txt):

```text
value renderString is not a member of org.http4s.MediaType, but could be made available as an extension method.
```

Sửa test bằng cách so sánh kiểu MediaType hoặc dùng hai field `mainType`/`subType`; code hiện dùng cách thứ hai. Không đoán API theo tên của kiểu tương tự trong thư viện khác.

**10.3. Body HTTP 401 vẫn chứa status 400.** Đây là lỗi đã quan sát ở Ch17 mục 10.2, không phải behavior mong muốn của lab. Dùng `.copy(response = ...)` như `Api.decodeFailures`; lời giải bài 6 thực sự chạy endpoint có bearer input để kiểm tra cả body 401 và header challenge.

**10.4. Chuyển trạng thái không được phép không phải lỗi JSON.** `Paid` và `Cancelled` đều là giá trị enum hợp lệ, nhưng cạnh Paid → Cancelled không tồn tại. Response thật là 409 với detail trong transcript, state vẫn Paid. Đừng nới enum decoder để giải quyết conflict; sửa chính sách nghiệp vụ nếu yêu cầu thay đổi.

**10.5. Runtime chưa có backend log.** Khi chạy lab có thể thấy thông báo SLF4J thật:

```text
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
```

Đây là runtime thiếu binding, khác compiler warning. Để tập trung vào contract, lab chưa thêm logging backend. Khi vận hành, nối cấu hình logging/error handler của Ch17 để exception không bị mất dấu; không coi một body 500 đã che chi tiết là đủ khả năng quan sát.

**10.6. Nhầm codec JSON với kiểm tra media type.** Trước khi đổi sang oneOfBody, curl gửi `user.json` với `Content-Type: text/plain` vẫn được `HTTP/1.1 201 Created`; test kỳ vọng 415 với body `{}` lại nhận 400 do thiếu field. Sửa bằng `oneOfBody(jsonBody[...])`, không đổi decode handler để mọi lỗi body đều thành 415. Bản hiện tại trả detail thật `Invalid value (value mismatch)` và HTTP/body cùng 415 (mục 9).

## 11. Cách debug

1. Chạy một test bằng `scala-cli.bat test . --test-only '*ContractSuite'`. Tìm case fail đầu tiên, kiểm tra HTTP status rồi content type rồi body.
2. Gửi đúng request bằng `curl.exe -i`; trên Windows dùng file UTF-8 có sẵn. Với query có `&`, đặt toàn URL trong dấu nháy.
3. 400: xem path/query/body nào decode thất bại. 415: xem Content-Type. 422: xem `errors`. 404: phân biệt resource thiếu và route sai version. 409: xem uniqueness/transition.
4. GET sau POST thấy rỗng: kiểm tra có dùng cùng Ref và cùng process không, chưa phải kết luận circe mất field. Test mỗi case dùng Ref mới là chủ ý.
5. Sai trang: tái hiện trên tập ID biết trước; kiểm tra filter trước slice, stable sort, total trước page. Có ghi đồng thời giữa hai request thì offset có thể dịch dù không có bug.
6. Lỗi 500: không trả `exception.getMessage` cho client. Tái hiện bằng repository thay thế (bài 5), xem cause phía server qua cấu hình Ch17.
7. So `/docs/openapi.yaml` với response thực tế khi đổi codec/status. Một YAML sinh thành công không chứng minh mọi handler thực thi đúng.

## 12. Bài tập — cơ bản đến nâng cao

1. **Tính tiền an toàn:** viết `LineTotal.calculate(List[(Long,Int)])`. Giá đơn vị và số lượng phải dương, đơn không rỗng. Trả Money hoặc lỗi; test cả tràn phép nhân và tổng nhiều dòng, không dùng Double.
2. **Lọc/sort đơn hàng:** viết hàm thuần nhận tập order của một user, status tùy chọn, sort total tăng/giảm, page. Lọc trước sort/page; dùng ID làm tie-break. Test total đúng và offset trong tập đã lọc. Chưa cần thêm endpoint vào lab.
3. **Cursor pagination:** viết page users theo `after` (ID, 0 là bắt đầu), limit 1..100. Lấy dư một dòng để chỉ trả next cursor khi còn dữ liệu; test ID có gap và insert mới giữa hai lần đọc. Nêu giới hạn snapshot và sort bằng field mutable.
4. **Cập nhật có điều kiện:** tạo kho order riêng có version ban đầu 0. `update(id, expectedVersion, next)` kiểm tra version và transition trong một Ref.modify, tăng version khi thành công. Hai request cùng version chỉ một được ghi. Đề xuất mapping lỗi version ra HTTP 412 với If-Match; bài này kiểm thử storage primitive, chưa thêm ETag endpoint.
5. **Fault injection:** cài decorator repository khiến `getUser` thất bại bằng IO có message chứa bí mật giả. Gọi HttpApp thật và kiểm tra 500 Problem không lộ message/stack trace. Không thêm route `/boom` vào server.
6. **Decode failure không gán cứng 400:** tạo endpoint Tapir nhỏ có bearer input, dùng cách xử lý Problem của chương này. Không gửi header: kiểm tra HTTP 401, body status 401, title và `WWW-Authenticate` cùng được giữ. Đây là bài về decode, không phải một cơ chế kiểm tra token hợp lệ.

## 13. Đáp án và lời giải

Xem [giải thích sáu bài](../solutions/ch14-solutions.md), [code mở rộng](../solutions/ch14-code/src/main/scala/vn/scalaeng/orders/solutions/Extensions.scala), [ví dụ decode](../solutions/ch14-code/src/main/scala/vn/scalaeng/orders/solutions/DecodeExample.scala) và [test](../solutions/ch14-code/src/test/scala/vn/scalaeng/orders/solutions/SolutionsSuite.scala). Project lời giải dùng lại source chính của lab, không copy một phiên bản service khác.

## 14. Câu hỏi kiểm tra

1. Vì sao không trả Problem từ repository? `IO[Either[E,A]]` khác `Either[E,IO[A]]` ở đâu?
2. Tại sao JSON hỏng không đi qua `serverLogic`? Muốn body lỗi thống nhất phải đổi những handler nào?
3. Hai `Ref.get` và `Ref.set` an toàn riêng lẻ có bảo vệ uniqueness không?
4. Vì sao sort name cần ID phụ? Stable sort có bảo đảm snapshot xuyên các trang không?
5. Khi nào dùng 400, 415, 422, 404, 409? Retry POST và retry PATCH trạng thái trong lab khác nhau thế nào?
6. Vì sao thay repository IO bằng nhiều method Doobie đã transact chưa đủ cho transaction nhiều bước?
7. Có OpenAPI có đồng nghĩa codec và business rule đã đúng không?

<details><summary>Gợi ý tự đối chiếu</summary>

1. Repository không phụ thuộc HTTP. Với IO[Either], việc chạy mới tạo kết quả nghiệp vụ và có thể failed IO; Either[E,IO[A]] quyết định nhánh Left/Right trước khi chạy effect.
2. Decode chạy trước logic. Cần errorOut, decode/exception/reject handlers và fallback route.
3. Không, request khác xen giữa đọc và ghi. Gom điều kiện + ghi trong một modify.
4. Tie-break tạo thứ tự toàn phần; vẫn có dịch trang nếu tập dữ liệu thay đổi giữa request.
5. 400 sai cách đọc kiểu/cú pháp; 415 sai media type; 422 sai ngữ nghĩa; 404 thiếu resource/route; 409 xung đột hiện trạng. POST có thể tạo đơn mới, PATCH đích trạng thái cho phép lặp cùng đích.
6. Mỗi transact là transaction riêng. Cần một chương trình DB chung hoặc method nguyên tử cấp use case.
7. Không, Schema không thực thi codec hay suy luận mọi chính sách. Cần contract/domain/socket tests.

</details>

## 15. Mini project — danh sách đơn hàng có điều kiện cập nhật

Nối bài 2 và 4 vào API: GET orders của một user nhận status/total sort qua whitelist, PATCH status dùng If-Match/ETag. Giữ v1 cũ chạy được hoặc giải thích cụ thể thay đổi nào breaking trước khi thêm v2.

Tiêu chí: total sau filter; sort xác định; 400/422 phân biệt rõ; 404 khi user thiếu; 412 khi version cũ; không lost update khi hai request cùng version; Problem body khớp status/header; OpenAPI mô tả If-Match/ETag và mọi lỗi mới. Viết contract test không mạng cho các nhánh và một socket test luồng đầy đủ. Đây là yêu cầu tự làm sau lab, không được đánh dấu đã có endpoint ETag trong code cơ sở.

## 16. Nguồn tài liệu chính thức

Đã đối chiếu nguồn ngày **17/10/2026**; artifact đã publish và build Scala 3 (`_3`) được kiểm tra qua `maven-metadata.xml`, không suy ra từ Scala 2. Danh sách URL/version và kết quả kiểm tra lưu trong [versions.json](order-service/evidence/versions.json).

- [Tapir: error handling](https://tapir.softwaremill.com/en/latest/server/errors.html), [server logic](https://tapir.softwaremill.com/en/latest/server/logic.html), [OpenAPI](https://tapir.softwaremill.com/en/latest/docs/openapi.html).
- [Source Tapir v1.13.33: DecodeFailureHandler](https://github.com/softwaremill/tapir/blob/v1.13.33/server/core/src/main/scala/sttp/tapir/server/interceptor/decodefailure/DecodeFailureHandler.scala), [ExceptionHandler](https://github.com/softwaremill/tapir/blob/v1.13.33/server/core/src/main/scala/sttp/tapir/server/interceptor/exception/ExceptionHandler.scala), [RejectHandler](https://github.com/softwaremill/tapir/blob/v1.13.33/server/core/src/main/scala/sttp/tapir/server/interceptor/reject/RejectHandler.scala).
- [http4s documentation](https://http4s.org/v0.23/), [Cats Effect Ref](https://typelevel.org/cats-effect/docs/std/ref), [Resource](https://typelevel.org/cats-effect/docs/std/resource), [circe codecs](https://circe.github.io/circe/codec.html).
- [RFC 9457 — Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html), [RFC 9110 — HTTP semantics](https://www.rfc-editor.org/rfc/rfc9110.html), [RFC 5789 — PATCH](https://www.rfc-editor.org/rfc/rfc5789.html).
- Maven metadata: [http4s server](https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/maven-metadata.xml), [Tapir](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-http4s-server_3/maven-metadata.xml), [circe](https://repo1.maven.org/maven2/io/circe/circe-core_3/maven-metadata.xml), [sttp-apispec](https://repo1.maven.org/maven2/com/softwaremill/sttp/apispec/openapi-circe-yaml_3/maven-metadata.xml), [Cats Effect](https://repo1.maven.org/maven2/org/typelevel/cats-effect_3/maven-metadata.xml), [munit](https://repo1.maven.org/maven2/org/scalameta/munit_3/maven-metadata.xml), [munit-cats-effect](https://repo1.maven.org/maven2/org/typelevel/munit-cats-effect_3/maven-metadata.xml).

## 17. Checklist tự đánh giá

- [ ] Giải thích được request đi qua decoder, DTO, service và repository thế nào.
- [ ] Phân biệt codec, Schema và validation; domain không phụ thuộc HTTP/JSON.
- [ ] Dùng opaque ID và Money đúng đơn vị, kiểm tra được tổng tiền tràn số.
- [ ] Chứng minh duplicate/transition cạnh tranh được bảo vệ bằng một thao tác nguyên tử.
- [ ] Giải thích filter → stable sort → page, total và giới hạn offset/cursor.
- [ ] Kiểm tra body Problem khớp status HTTP, kể cả decode failure và 500.
- [ ] Đọc được YAML sinh tự động tại `/docs/openapi.yaml` và tìm schema/status tương ứng.
- [ ] Chạy test không mạng và luồng socket cổng 0, không dựa Docker.
- [ ] Hoàn thành sáu bài tập và diễn giải giới hạn của versioning/transaction adapter.

Checklist dành cho người học. Claude đã review chéo; bản sửa vòng 2 chờ lead kiểm tra lại và cập nhật tiến độ chung.
