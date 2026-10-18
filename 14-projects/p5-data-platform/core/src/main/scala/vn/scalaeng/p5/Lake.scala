package vn.scalaeng.p5

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import java.time.LocalDate
import scala.jdk.CollectionConverters.*

/** Bố cục "data lake" trên filesystem (Chương 31): raw append-only → staging → serving-ready, cộng lineage và quarantine.
  *   raw/orders/ingest_date=D/      các file .jsonl — Kafka landing hoặc file thả vào, KHÔNG sửa
  *   quarantine/orders/              file vi phạm contract, giữ nguyên để báo producer
  *   staging/<asset>/day=D/          part.jsonl (staging) hoặc part.csv (daily), + rejected.jsonl
  *   _quality/raw_reconcile.json     đối soát cấp raw (lệnh `reconcile`)
  *   dimension/customers.csv         bảng khách (do hệ thống khác cung cấp)
  *   _lineage/materializations.jsonl
  */
final case class Lake(root: Path):
  def rawDir(source: String): Path = root.resolve("raw").resolve(source)
  def rawFiles(source: String): Vector[Path] =
    val dir = rawDir(source)
    if !Files.isDirectory(dir) then Vector.empty
    else
      val s = Files.walk(dir)
      try s.iterator().asScala.filter(f => Files.isRegularFile(f) && f.toString.endsWith(".jsonl")).toVector.sortBy(_.toString) finally s.close()
  def partitionDir(asset: String, day: LocalDate): Path = root.resolve("staging").resolve(asset).resolve(s"day=$day")
  def partFile(a: Asset, day: LocalDate): Path = partitionDir(a.name, day).resolve(a.fileName)
  def customers: Path = root.resolve("dimension").resolve("customers.csv")
  def lineageFile: Path = root.resolve("_lineage").resolve("materializations.jsonl")
  def relative(p: Path): String = root.relativize(p).toString.replace('\\', '/')

object Io:
  def hash(p: Path): String = if !Files.exists(p) then "missing" else hashBytes(Files.readAllBytes(p))
  def hashBytes(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).take(8).map(b => f"${b & 0xff}%02x").mkString

  def hashText(s: String): String =
    MessageDigest.getInstance("SHA-256").digest(s.getBytes(UTF_8)).take(8).map(b => f"${b & 0xff}%02x").mkString

  /** Ghi nguyên tử + fsync nội dung: file tạm trong cùng thư mục, `force`, rồi ATOMIC_MOVE (thay thế nếu có). */
  def writeAtomically(target: Path, content: String): Unit =
    Files.createDirectories(target.getParent)
    val tmp = Files.createTempFile(target.getParent, target.getFileName.toString + ".", ".tmp")
    try
      val ch = java.nio.channels.FileChannel.open(tmp, java.nio.file.StandardOpenOption.WRITE)
      try { ch.write(java.nio.ByteBuffer.wrap(content.getBytes(UTF_8))); ch.force(true) } finally ch.close()
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      ()
    finally { Files.deleteIfExists(tmp); () }

  def lines(p: Path): Vector[String] = Files.readAllLines(p, UTF_8).asScala.toVector.filter(_.trim.nonEmpty)
