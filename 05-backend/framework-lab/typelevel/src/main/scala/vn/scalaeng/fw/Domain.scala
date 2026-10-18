package vn.scalaeng.fw

import cats.effect.{IO, Ref}
import io.circe.Codec

// Domain dùng chung cho CẢ HAI cách viết API. Codec JSON (circe) được derive ngay trên case class
// cho gọn; Chương 14 sẽ tách codec ra khỏi domain.
final case class Product(id: Long, sku: String, name: String, price: Long) derives Codec.AsObject
final case class NewProduct(sku: String, name: String, price: Long) derives Codec.AsObject
final case class FieldError(field: String, message: String) derives Codec.AsObject

/** Dạng rút gọn của Problem Details (RFC 9457) đã dùng ở Chương 12. */
final case class Problem(status: Int, title: String, detail: String, errors: List[FieldError] = Nil)
    derives Codec.AsObject

object Validation:
  private val SkuPattern = "[A-Z0-9-]{3,20}"

  /** Tích lũy mọi lỗi để client sửa một lần (giống Chương 12). */
  def validate(np: NewProduct): Either[List[FieldError], NewProduct] =
    val errors = List(
      Option.when(!np.sku.matches(SkuPattern))(FieldError("sku", "3-20 ký tự A-Z, 0-9 hoặc dấu gạch ngang")),
      Option.when(np.name.trim.isEmpty)(FieldError("name", "không được rỗng")),
      Option.when(np.price <= 0)(FieldError("price", "phải > 0"))
    ).flatten
    if errors.isEmpty then Right(np) else Left(errors)

enum CreateResult:
  case Created(product: Product)
  case DuplicateSku(existingId: Long)

/** Store trong bộ nhớ. `Ref` (Cats Effect) thay cho AtomicReference + vòng lặp CAS của Chương 12. */
final class ProductStore private (state: Ref[IO, ProductStore.State]):
  def get(id: Long): IO[Option[Product]] = state.get.map(_.byId.get(id))

  /** Sắp xếp theo id, lọc theo tên (không phân biệt hoa thường) nếu có `q`. */
  def list(q: Option[String]): IO[List[Product]] = state.get.map { s =>
    s.byId.values.toList.sortBy(_.id).filter(p => q.forall(t => p.name.toLowerCase.contains(t.toLowerCase)))
  }

  def create(np: NewProduct): IO[CreateResult] = state.modify { s =>
    s.byId.values.find(_.sku == np.sku) match
      case Some(existing) => (s, CreateResult.DuplicateSku(existing.id))
      case None =>
        val p = Product(s.nextId, np.sku, np.name, np.price)
        (ProductStore.State(s.nextId + 1, s.byId.updated(p.id, p)), CreateResult.Created(p))
  }

object ProductStore:
  final case class State(nextId: Long, byId: Map[Long, Product])
  def empty: IO[ProductStore] = Ref.of[IO, State](State(1L, Map.empty)).map(new ProductStore(_))
