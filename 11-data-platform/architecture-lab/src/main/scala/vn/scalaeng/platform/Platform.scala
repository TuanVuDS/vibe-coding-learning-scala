package vn.scalaeng.platform

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.nio.file.StandardOpenOption.{CREATE, WRITE}
import java.security.MessageDigest
import java.time.{Instant, LocalDate}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Bố cục lưu trữ của nền tảng — mỗi LỚP có một quy tắc ghi khác nhau:
  *   - `raw/<nguồn>/ingest_date=D/` (các file .csv): đúng như nguồn gửi, CHỈ THÊM (append-only), không ai sửa. Là "nguồn sự thật" để tính lại bất cứ lúc nào.
  *   - `staging/<asset>/day=D/part.csv`: đã parse, validate, dedup — theo ngày NGHIỆP VỤ (không theo ngày nhận).
  *   - `serving/<asset>/day=D/part.csv`: bảng cho người đọc (dashboard, API). Ghi đè nguyên tử từng partition.
  *   - `_lineage/materializations.jsonl`: mỗi lần tính một partition để lại một dòng: dùng input nào (kèm hash), code version nào, ra bao nhiêu dòng.
  */
final case class Platform(root: Path):
  def rawFiles(source: String): Vector[Path] =
    val dir = root.resolve("raw").resolve(source)
    if !Files.isDirectory(dir) then Vector.empty
    else
      val s = Files.walk(dir)
      try s.iterator().asScala.filter(f => Files.isRegularFile(f) && f.toString.endsWith(".csv")).toVector.sortBy(_.toString) finally s.close()

  def partitionFile(layer: String, asset: String, day: LocalDate): Path =
    root.resolve(layer).resolve(asset).resolve(s"day=$day").resolve("part.csv")

  def relative(p: Path): String = root.relativize(p).toString.replace('\\', '/')

  val lineageFile: Path = root.resolve("_lineage").resolve("materializations.jsonl")

object Files2:
  /** Khóa độc quyền toàn root, từ chụp input đến append lineage. Từ chối ngay run trùng (cả trong JVM lẫn giữa các JVM).
    * Khóa thô giúp downstream không đọc giữa publish và append upstream; producer raw/dimension vẫn phải publish file hoàn chỉnh.
    */
  def withWriterLock[A](p: Platform)(body: => A): A =
    Files.createDirectories(p.lineageFile.getParent)
    val channel = FileChannel.open(p.lineageFile.resolveSibling("writer.lock"), CREATE, WRITE)
    try
      val lock = try channel.tryLock() catch
        case _: OverlappingFileLockException => throw new IllegalStateException("root đang có materialize khác; chạy lại sau")
      if lock == null then throw new IllegalStateException("root đang có materialize khác; chạy lại sau")
      try body finally lock.release()
    finally channel.close()

  /** Hash nội dung (16 ký tự hex đầu của SHA-256) — đủ để biết "input có đổi không", không dùng cho bảo mật. */
  def hash(p: Path): String = if !Files.exists(p) then "missing" else hashBytes(Files.readAllBytes(p))
  def hashBytes(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).take(8).map(b => f"${b & 0xff}%02x").mkString

  def csvBytes(lines: Seq[String]): Array[Byte] = lines.mkString("", "\n", "\n").getBytes(UTF_8)

  /** Ghi đè NGUYÊN TỬ một partition: file tạm cùng thư mục rồi ATOMIC_MOVE — người đọc thấy bản cũ hoặc bản mới, không thấy bản ghi dở. */
  def writeAtomically(target: Path, lines: Seq[String]): Unit =
    Files.createDirectories(target.getParent)
    val tmp = Files.createTempFile(target.getParent, "part.", ".tmp")
    try
      Files.write(tmp, csvBytes(lines))
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      ()
    finally { Files.deleteIfExists(tmp); () }

  def lines(p: Path): Vector[String] = Files.readAllLines(p, UTF_8).asScala.toVector.filter(_.trim.nonEmpty)

final case class Order(orderId: String, customerId: String, day: LocalDate, status: String, sku: String, quantity: Int, priceVnd: Long,
    updatedAt: Instant)

object RawOrders:
  val Header = "order_id,customer_id,day,status,sku,quantity,unit_price_vnd,updated_at"
  private val Statuses = Set("PAID", "REFUNDED", "CANCELLED")

  /** Một dòng raw → đơn hợp lệ (đã chuẩn hóa) hoặc None. Quy tắc gọn của Chương 24/33 — chương này tập trung vào KIẾN TRÚC, không vào validation. */
  def parse(line: String): Option[Order] =
    line.split(",", -1).map(_.trim) match
      case Array(id, cust, day, st, sku, qty, price, ts) =>
        for
          d  <- Try(LocalDate.parse(day)).toOption
          s  <- Option(st.toUpperCase(java.util.Locale.ROOT)).filter(Statuses)
          q  <- qty.toIntOption.filter(q => q >= 1 && q <= 1000)
          pr <- price.toLongOption.filter(_ > 0)
          t  <- Try(Instant.parse(ts)).toOption
          if id.nonEmpty && cust.nonEmpty && sku.nonEmpty
        yield Order(id, cust.toUpperCase(java.util.Locale.ROOT), d, s, sku.toUpperCase(java.util.Locale.ROOT), q, pr, t)
      case _ => None

  /** Ngày nghiệp vụ mà một dòng raw KHAI BÁO (cột thứ 3), kể cả khi dòng đó không hợp lệ — dùng cho lineage. */
  def declaredDay(line: String): Option[String] = line.split(",", -1).lift(2).map(_.trim)
