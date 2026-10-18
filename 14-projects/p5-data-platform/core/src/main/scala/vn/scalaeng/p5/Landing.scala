package vn.scalaeng.p5

import java.nio.file.{Files, Path}
import java.time.LocalDate

/** INGESTION: hai đường vào cùng một lớp raw. Cả hai kiểm CONTRACT ở cửa ngõ; vi phạm → quarantine (giữ nguyên để báo producer), không vào raw. */
object FileLanding:
  final case class Landed(rawFile: Option[String], quarantined: Option[String], reason: Option[String])

  /** Thả một file .jsonl. Tên file trong raw = hash nội dung → thả lại cùng file là no-op (idempotent), thả file khác tên mà cùng nội dung cũng vậy. */
  def land(lake: Lake, drop: Path, ingestDate: LocalDate): Landed =
    val content = new String(Files.readAllBytes(drop), java.nio.charset.StandardCharsets.UTF_8)
    val lines = content.linesIterator.filter(_.trim.nonEmpty).toVector
    val problems = lines.zipWithIndex.flatMap((l, i) => OrderEvent.conform(l).left.toOption.map(e => s"dòng ${i + 1}: $e"))
    val name = s"drop-${Io.hashText(content)}.jsonl"
    if lines.isEmpty || problems.nonEmpty then
      val q = lake.root.resolve("quarantine").resolve("orders").resolve(s"ingest_date=$ingestDate").resolve(name)
      Io.writeAtomically(q, content)
      val reason = if lines.isEmpty then "file rỗng" else problems.take(3).mkString("; ")
      Io.writeAtomically(q.resolveSibling(name + ".reason.txt"), reason + "\n")
      Landed(None, Some(lake.relative(q)), Some(reason))
    else
      val target = lake.rawDir("orders").resolve(s"ingest_date=$ingestDate").resolve(name)
      if !Files.exists(target) then Io.writeAtomically(target, content)
      Landed(Some(lake.relative(target)), None, None)

