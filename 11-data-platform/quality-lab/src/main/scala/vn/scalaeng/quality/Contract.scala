package vn.scalaeng.quality

import io.circe.{Decoder, HCursor}
import io.circe.parser.decode

import java.time.{Instant, LocalDate}
import scala.util.Try

/** DATA CONTRACT: thỏa thuận giữa bên SẢN XUẤT và bên TIÊU THỤ dữ liệu về schema + ngữ nghĩa — là tài liệu VÀ là thứ máy kiểm tra được.
  * Ở đây: tên trường, kiểu, nullable, tập giá trị cho enum, ví dụ `contracts/orders.v1.json`. Thay đổi contract phải qua kiểm tra TƯƠNG THÍCH:
  * bản mới có làm vỡ người đang đọc bản cũ không?
  */
final case class FieldSpec(name: String, `type`: String, nullable: Boolean, values: Option[List[String]])
final case class Contract(name: String, version: Int, owner: String, fields: List[FieldSpec])

object Contract:
  given Decoder[FieldSpec] = (c: HCursor) =>
    for
      n  <- c.get[String]("name")
      t  <- c.get[String]("type")
      nb <- c.getOrElse[Boolean]("nullable")(false)
      v  <- c.get[Option[List[String]]]("values")
    yield FieldSpec(n, t, nb, v)
  given Decoder[Contract] = (c: HCursor) =>
    for
      n <- c.get[String]("name"); v <- c.get[Int]("version"); o <- c.get[String]("owner"); f <- c.get[List[FieldSpec]]("fields")
    yield Contract(n, v, o, f)

  val Types: Set[String] = Set("string", "int", "long", "date", "timestamp", "enum")

  def parse(json: String): Either[String, Contract] =
    decode[Contract](json).left.map(e => s"contract không đọc được: ${e.getMessage}").flatMap { c =>
      val bad = c.fields.filterNot(f => Types(f.`type`)).map(f => s"${f.name}: kiểu lạ '${f.`type`}'") ++
        c.fields.filter(f => f.`type` == "enum" && f.values.forall(_.isEmpty)).map(f => s"${f.name}: enum cần 'values'") ++
        c.fields.groupBy(_.name).collect { case (n, fs) if fs.size > 1 => s"$n: khai báo trùng" }
      Either.cond(bad.isEmpty, c, bad.mkString("; "))
    }

  /** Khóa đặc biệt mà `DataFiles.records` thêm khi một dòng có NHIỀU giá trị hơn header — không phải tên trường, là dấu hiệu dòng hỏng. */
  val ExtraColumns = "_extra_columns"

  private def fieldErrors(c: Contract, record: Map[String, String]): List[String] =
    c.fields.flatMap { f =>
      record.get(f.name).map(_.trim).filter(_.nonEmpty) match
        case None if f.nullable => None
        case None               => Some(s"${f.name}: bắt buộc")
        case Some(v) =>
          val ok = f.`type` match
            case "string"    => true
            case "int"       => v.toIntOption.isDefined
            case "long"      => v.toLongOption.isDefined
            case "date"      => Try(LocalDate.parse(v)).isSuccess
            case "timestamp" => Try(Instant.parse(v)).isSuccess
            case "enum"      => f.values.exists(_.contains(v))
            case _           => false
          Option.when(!ok)(s"${f.name}='$v': không phải ${f.`type`}${f.values.fold("")(vs => vs.mkString(" {", ",", "}"))}")
    }

  private def extraColumns(record: Map[String, String]): List[String] =
    record.get(ExtraColumns).toList.map(v => s"dòng có giá trị thừa ngoài header: '$v'")

  /** PRODUCER CONFORMANCE — producer có giữ đúng lời không? Nghiêm: trường KHÔNG có trong contract cũng là vi phạm (producer đổi schema mà không
    * đổi contract là chuyện cần biết ngay). Chạy ở biên ingest, đếm theo producer.
    */
  def validate(c: Contract, record: Map[String, String]): List[String] =
    val known = c.fields.map(_.name).toSet
    val unknown = (record.keySet - ExtraColumns -- known).toList.sorted.map(k => s"$k: trường không có trong contract ${c.name} v${c.version}")
    extraColumns(record) ++ unknown ++ fieldErrors(c, record)

  /** READER PROJECTION — consumer viết theo contract `c` có ĐỌC được bản ghi này không? Trường lạ bị BỎ QUA (như Avro reader bỏ trường chỉ có ở
    * writer); trường mình cần thì phải có và parse được theo kiểu của `c`. Đây là ngữ nghĩa mà `compatibility` bảo đảm (review T17 M2).
    */
  def read(c: Contract, record: Map[String, String]): List[String] = extraColumns(record) ++ fieldErrors(c, record)

  enum Impact:
    case Compatible, Breaking

  final case class Change(field: String, description: String, impact: Impact)

  /** Tương thích FORWARD (thuật ngữ Confluent Schema Registry): consumer viết theo `old` (READER cũ) có đọc được dữ liệu producer ghi theo `next`
    * (WRITER mới) không? Cam kết kiểm được: mọi bản ghi đạt `validate(next, r)` thì `read(old, r)` không lỗi — nếu mọi thay đổi là Compatible.
    *   - Thêm trường: Compatible (reader cũ bỏ qua). Bỏ trường: Breaking nếu reader cũ cần nó (bắt buộc); Compatible nếu nó nullable ở `old`.
    *   - Đổi kiểu: Breaking — kể cả int → long: writer mới được phép ghi 2147483648, reader int cũ không parse được (review T17 M1). Chiều ngược lại
    *     long → int (thu hẹp miền writer) là Compatible: mọi int đều là long hợp lệ với reader cũ.
    *   - Bắt buộc → nullable: Breaking (reader cũ gặp ô trống). Nullable → bắt buộc: Compatible.
    *   - THÊM giá trị enum: Breaking (reader cũ gặp giá trị lạ — Avro chỉ chịu được nếu reader khai báo default). Bớt giá trị: Compatible.
    * Hướng ngược lại (BACKWARD: reader MỚI đọc dữ liệu CŨ) là `compatibility(next, old)` với vai trò đổi chỗ — đừng dùng một hàm cho cả hai hướng.
    */
  def compatibility(old: Contract, next: Contract): List[Change] =
    val o = old.fields.map(f => f.name -> f).toMap
    val n = next.fields.map(f => f.name -> f).toMap
    val removed = (o.keySet -- n.keySet).toList.sorted.map { k =>
      if o(k).nullable then Change(k, "bị bỏ (reader cũ cho phép thiếu)", Impact.Compatible) else Change(k, "bị bỏ", Impact.Breaking)
    }
    val added = (n.keySet -- o.keySet).toList.sorted.map(k =>
      Change(k, s"được thêm (${if n(k).nullable then "nullable" else "bắt buộc"}) — reader cũ bỏ qua", Impact.Compatible))
    val changed = (o.keySet & n.keySet).toList.sorted.flatMap { k =>
      val (a, b) = (o(k), n(k))
      val typeChange =
        if a.`type` == b.`type` then None
        else if a.`type` == "long" && b.`type` == "int" then Some(Change(k, "long → int (writer thu hẹp miền)", Impact.Compatible))
        else Some(Change(k, s"${a.`type`} → ${b.`type`}", Impact.Breaking))
      val nullChange =
        if !a.nullable && b.nullable then Some(Change(k, "bắt buộc → nullable", Impact.Breaking))
        else if a.nullable && !b.nullable then Some(Change(k, "nullable → bắt buộc", Impact.Compatible))
        else None
      val enumChange = (a.values, b.values) match
        case (Some(av), Some(bv)) =>
          val plus = bv.diff(av); val minus = av.diff(bv)
          plus.headOption.map(_ => Change(k, s"thêm giá trị ${plus.mkString(",")}", Impact.Breaking)).toList ++
            minus.headOption.map(_ => Change(k, s"bớt giá trị ${minus.mkString(",")}", Impact.Compatible)).toList
        case _ => Nil
      typeChange.toList ++ nullChange.toList ++ enumChange
    }
    removed ++ changed ++ added
