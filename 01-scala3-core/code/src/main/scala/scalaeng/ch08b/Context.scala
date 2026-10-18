package scalaeng.ch08b

/** Using clauses cho "ngữ cảnh" truyền ngầm qua nhiều tầng: request id, clock. */
final case class RequestContext(requestId: String, userId: Option[String])

trait Clock:
  def nowMillis(): Long

object Clock:
  /** Clock thật — side effect, chỉ dùng ở shell. */
  val system: Clock = () => System.currentTimeMillis()
  def fixed(t: Long): Clock = () => t

object AuditLog:
  def entry(action: String)(using ctx: RequestContext, clock: Clock): String =
    s"${clock.nowMillis()} [${ctx.requestId}] user=${ctx.userId.getOrElse("anonymous")} $action"

object OrderService:
  /** Hàm tầng trên KHÔNG cần nhắc đến ctx/clock khi gọi AuditLog: chúng được truyền tiếp tự động. */
  def placeOrder(orderId: String)(using RequestContext, Clock): List[String] =
    List(AuditLog.entry(s"validate $orderId"), AuditLog.entry(s"place $orderId"))

/** Ordering là type class có sẵn trong standard library. */
final case class Product(sku: String, price: Long, rating: Double)

object ProductOrderings:
  val byPriceAsc: Ordering[Product] = Ordering.by(_.price)
  val byRatingDescThenPrice: Ordering[Product] =
    Ordering.by[Product, Double](p => -p.rating).orElseBy(_.price)

/** derives CanEqual + strictEquality: cấm so sánh == giữa hai kiểu không liên quan. */
final case class Sku(value: String) derives CanEqual
