package vn.scalaeng.p2.db

import cats.syntax.all.*
import org.typelevel.doobie.*
import org.typelevel.doobie.implicits.*
import vn.scalaeng.p2.domain.*

object Codecs:
  given Meta[UserId] = Meta[Long].timap(UserId.assigned)(_.value)
  given Meta[OrderId] = Meta[Long].timap(OrderId.assigned)(_.value)
  given Meta[Money] = Meta[Long].tiemap(Money.fromMinor)(_.minor)
  given Meta[OrderStatus] = Meta[String].tiemap(s => OrderStatus.values.find(_.toString == s).toRight("status lạ"))(_.toString)

/** Same eight operations as Ch14, with ConnectionIO instead of IO. Never transact here. */
final class PgStore extends Store[ConnectionIO]:
  import Codecs.given
  def createUser(d: UserDraft): ConnectionIO[Either[DomainError, User]] = register(d, "!login-disabled!")
  def register(d: UserDraft, hash: String): ConnectionIO[Either[DomainError, User]] =
    sql"""insert into users(name,email,password_hash) values (${d.name},${d.email},$hash)
      on conflict ((lower(email))) do nothing returning id,name,email""".query[User].option
      .map(_.toRight(DomainError.DuplicateEmail(d.email)))
  def getUser(id: UserId): ConnectionIO[Option[User]] =
    sql"select id,name,email from users where id=$id".query[User].option
  private def accountQuery(where: Fragment): ConnectionIO[Option[Account]] =
    (fr"select id,name,email,password_hash,role from users where" ++ where).query[(User, String, String)].option
      .map(_.map((u,h,r) => Account(u,h,r)))
  def account(email: String): ConnectionIO[Option[Account]] = accountQuery(fr"lower(email)=lower($email)")
  def accountById(id: UserId): ConnectionIO[Option[Account]] = accountQuery(fr"id=$id")
  def revoked(jti: String): ConnectionIO[Boolean] =
    sql"select exists(select 1 from revoked_tokens where jti=$jti and expires_epoch > extract(epoch from now()))".query[Boolean].unique
  def revoke(jti: String, expiresEpoch: Long): ConnectionIO[Unit] =
    (sql"delete from revoked_tokens where expires_epoch <= extract(epoch from now())".update.run *>
      sql"insert into revoked_tokens values ($jti,$expiresEpoch) on conflict (jti) do nothing".update.run).void

  def listUsers(q: UserQuery): ConnectionIO[Page[User]] =
    val where = Fragments.whereAndOpt(q.q.map(s => fr"(position($s in lower(name))>0 or position($s in email)>0)"))
    val sort = q.sort match
      case UserSort.Id => fr"id"
      case UserSort.Name => fr"lower(name)"
      case UserSort.Email => fr"email"
    val direction = if q.descending then fr"desc" else fr"asc"
    // One statement, one snapshot, including an empty page; avoids count/items snapshot drift.
    (fr"with filtered as (select id,name,email from users" ++ where ++ fr"""),
      page as (select * from filtered order by""" ++ sort ++ direction ++ fr",id limit ${q.page.limit} offset ${q.page.offset})" ++
      fr"select (select count(*) from filtered),page.id,page.name,page.email from (select 1) anchor left join page on true order by" ++ sort ++ direction ++ fr",page.id")
      .query[(Int, Option[Long], Option[String], Option[String])].to[List].map { rows =>
        val items = rows.flatMap((_,id,n,e) => for i <- id; name <- n; email <- e yield User(UserId.assigned(i), name,email))
        Page(items, rows.head._1, q.page.offset, q.page.limit)
      }
  def createOrder(userId: UserId, total: Money): ConnectionIO[Either[DomainError, Order]] =
    sql"""insert into orders(user_id,total) select id,$total from users where id=$userId
      returning id,user_id,total,status""".query[Order].option.map(_.toRight(DomainError.UnknownUser(userId)))
  def getOrder(id: OrderId): ConnectionIO[Option[Order]] =
    sql"select id,user_id,total,status from orders where id=$id".query[Order].option
  def lockOrder(id: OrderId): ConnectionIO[Option[Order]] =
    sql"select id,user_id,total,status from orders where id=$id for update".query[Order].option
  def listOrders(userId: UserId, page: PageRequest): ConnectionIO[Either[DomainError, Page[Order]]] =
    getUser(userId).flatMap {
      case None => (Left(DomainError.UnknownUser(userId)): Either[DomainError, Page[Order]]).pure[ConnectionIO]
      case Some(_) =>
        sql"""with filtered as (select id,user_id,total,status from orders where user_id=$userId),
          page as (select * from filtered order by id limit ${page.limit} offset ${page.offset})
          select (select count(*) from filtered),page.id,page.user_id,page.total,page.status
          from (select 1) anchor left join page on true order by page.id""".query[(Int, Option[Long], Option[Long], Option[Long], Option[String])].to[List].map { rows =>
            val items = rows.flatMap((_,id,u,t,s) => for i<-id; uid<-u; total<-t; status<-s yield
              Order(OrderId.assigned(i), UserId.assigned(uid), Money.fromMinor(total).toOption.get, OrderStatus.valueOf(status)))
            Right(Page(items, rows.head._1, page.offset, page.limit))
          }
    }
  def updateStatus(id: OrderId, next: OrderStatus): ConnectionIO[Either[DomainError, Order]] =
    lockOrder(id).flatMap {
      case None => (Left(DomainError.UnknownOrder(id)): Either[DomainError, Order]).pure[ConnectionIO]
      case Some(o) if !o.status.canMoveTo(next) => (Left(DomainError.InvalidTransition(o.status,next)): Either[DomainError, Order]).pure[ConnectionIO]
      case Some(o) => sql"update orders set status=$next where id=$id".update.run.as(Right(o.copy(status=next)))
    }
  def product(id: Long): ConnectionIO[Option[Product]] =
    sql"select id,sku,name,price,stock,active from products where id=$id and active".query[Product].option
  def products(q: Option[String], descending: Boolean, page: PageRequest): ConnectionIO[Page[Product]] =
    val where = fr"where active" ++ q.fold(Fragment.empty)(s => fr"and position(${s.toLowerCase(java.util.Locale.ROOT)} in lower(name))>0")
    val dir = if descending then fr"desc" else fr"asc"
    (fr"with filtered as (select * from products" ++ where ++ fr"), page as (select * from filtered order by price" ++ dir ++
      fr",id limit ${page.limit} offset ${page.offset}) select (select count(*) from filtered), page.id,page.sku,page.name,page.price,page.stock,page.active from (select 1) anchor left join page on true order by page.price" ++ dir ++ fr",page.id")
      .query[(Int, Option[Long], Option[String], Option[String], Option[Long], Option[Int], Option[Boolean])].to[List].map { rows =>
        val items = rows.flatMap((_,id,s,n,p,st,a) => for i<-id; sku<-s; name<-n; price<-p; stock<-st; active<-a yield Product(i,sku,name,price,stock,active))
        Page(items, rows.head._1, page.offset, page.limit)
      }
  def saveProduct(id: Option[Long], d: ProductDraft): ConnectionIO[Either[DomainError, Product]] = id match
    case None => sql"""insert into products(sku,name,price,stock) values (${d.sku},${d.name},${d.price},${d.stock})
      on conflict (sku) do nothing returning id,sku,name,price,stock,active""".query[Product].option.map(_.toRight(DomainError.Conflict))
    case Some(i) =>
      // Do not expose editable SKU: stable identity avoids catching unique violations inside a transaction.
      sql"""update products set name=${d.name},price=${d.price},stock=${d.stock}
        where id=$i and sku=${d.sku} and active returning id,sku,name,price,stock,active""".query[Product].option.map(_.toRight(DomainError.MissingProduct))
  def deleteProduct(id: Long): ConnectionIO[Boolean] =
    sql"update products set active=false where id=$id and active".update.run.map(_ == 1)
  def lockProducts(ids: List[Long]): ConnectionIO[List[Product]] =
    cats.data.NonEmptyList.fromList(ids) match
      case None => List.empty[Product].pure[ConnectionIO]
      case Some(nonEmpty) => (fr"select id,sku,name,price,stock,active from products where" ++ Fragments.in(fr"id",nonEmpty) ++
        fr"order by id for update").query[Product].to[List]
  def insertItems(id: OrderId, lines: List[PricedLine]): ConnectionIO[Unit] =
    Update[(OrderId,Long,Int,Long)]("insert into order_items values (?,?,?,?)")
      .updateMany(lines.map(l => (id,l.productId,l.quantity,l.price))).void
  def decrement(lines: List[PricedLine]): ConnectionIO[Unit] = lines.traverse_(l =>
    sql"update products set stock=stock-${l.quantity} where id=${l.productId}".update.run)
  def restore(id: OrderId): ConnectionIO[Unit] =
    // Lock all affected products in id order before updating, same as checkout.
    sql"select product_id from order_items where order_id=$id order by product_id".query[Long].to[List].flatMap(lockProducts) *>
      sql"""update products p set stock=p.stock+i.quantity from order_items i
        where i.order_id=$id and p.id=i.product_id""".update.run.void
