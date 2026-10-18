package vn.scalaeng.p3

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.Comparator
import scala.jdk.CollectionConverters._

/** WRITE → AUDIT → PUBLISH (Chương 24 bài 4 + 5, gộp lại):
  *   1. ghi MỌI bảng của lần thực thi vào `_staging/<execution_id>/<bảng>/...` — thư mục phục vụ chưa bị chạm;
  *   2. audit đọc lại staging; lỗi → xóa staging, phục vụ giữ nguyên bản cũ;
  *   3. publish theo một KẾ HOẠCH di chuyển: partition cũ → `_trash/<execution_id>/`, partition mới từ staging vào chỗ; partition đang phục vụ mà không
  *      còn trong kết quả bị đưa vào thùng rác (output là SNAPSHOT của input ≤ ngày chạy — review P3 M3).
  * Mỗi lần đổi tên là nguyên tử trên cùng hệ thống file cục bộ; CẢ lần publish thì KHÔNG. Vì vậy (review P3 M2):
  *   - preflight trước khi đụng thư mục phục vụ (staging đủ, thùng rác của lần này chưa tồn tại);
  *   - nhật ký: mỗi bước thành công được ghi (kèm fsync) vào `_publish/<execution_id>.journal` TRƯỚC bước kế tiếp;
  *   - lỗi giữa chừng → đảo ngược các bước đã làm theo thứ tự ngược; đảo ngược hỏng → GIỮ staging, thùng rác và nhật ký để phục hồi bằng tay.
  * Người đọc vẫn có thể thấy trạng thái trộn TRONG lúc publish/rollback. Muốn snapshot nhất quán nhiều bảng: table format (Delta/Iceberg) hoặc con trỏ
  * phiên bản — Chương 31. Object storage không có rename nguyên tử: thiết kế này chỉ cho hệ thống file cục bộ/chia sẻ.
  */
object Publish {
  def deleteTree(p: Path): Unit =
    if (Files.exists(p)) { val s = Files.walk(p); try s.sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(Files.delete) finally s.close() }

  def dayDirs(table: Path): Set[String] =
    if (!Files.isDirectory(table)) Set.empty
    else { val s = Files.list(table); try s.iterator().asScala.map(_.getFileName.toString).filter(_.startsWith("day=")).toSet finally s.close() }

  def stage(df: DataFrame, staging: Path, table: String): Unit =
    df.repartition(col("day")).write.mode("errorifexists").partitionBy("day").parquet(staging.resolve(table).toString)

  final case class Move(from: Path, to: Path)
  final case class Published(table: String, replaced: Set[String], added: Set[String], removed: Set[String])

  /** Lỗi publish. `rolledBack = true`: thư mục phục vụ đã về đúng bản trước. `false`: trạng thái trộn — xem `journal`. */
  final class PublishFailed(message: String, cause: Throwable, val journal: Path, val rolledBack: Boolean) extends RuntimeException(message, cause)

  /** Kế hoạch cho một bảng partition theo `day` (snapshot: partition không còn trong staging bị gỡ). */
  def planPartitioned(staging: Path, serving: Path, trash: Path, table: String): (List[Move], Published) = {
    val from = staging.resolve(table); val to = serving.resolve(table); val bin = trash.resolve(table)
    val incoming = dayDirs(from); val existing = dayDirs(to)
    val moves = incoming.toList.sorted.flatMap { d =>
      (if (existing(d)) List(Move(to.resolve(d), bin.resolve(d))) else Nil) :+ Move(from.resolve(d), to.resolve(d))
    } ++ (existing -- incoming).toList.sorted.map(d => Move(to.resolve(d), bin.resolve(d)))
    (moves, Published(table, incoming & existing, incoming -- existing, existing -- incoming))
  }

  /** Kế hoạch cho một bảng không partition (bản ghi lỗi, xung đột): bảng cũ vào thùng rác — không XÓA trước khi bản mới vào chỗ. */
  def planWhole(staging: Path, serving: Path, trash: Path, table: String): List[Move] =
    (if (Files.exists(serving.resolve(table))) List(Move(serving.resolve(table), trash.resolve(table))) else Nil) :+
      Move(staging.resolve(table), serving.resolve(table))

  /** Kiểm tra trước khi đụng thư mục phục vụ. Trả danh sách vấn đề (rỗng = được đi tiếp). */
  def preflight(moves: List[Move], staging: Path, trash: Path): List[String] =
    (if (Files.exists(trash)) List(s"thùng rác $trash đã tồn tại — execution_id bị dùng lại?") else Nil) ++
      moves.filter(m => m.from.startsWith(staging) && !Files.exists(m.from)).map(m => s"thiếu ${m.from} trong staging")

  /** Thực hiện kế hoạch, ghi nhật ký từng bước; lỗi → rollback. `beforeMove` chỉ cho test chèn lỗi. */
  def execute(moves: List[Move], journal: Path, beforeMove: Move => Unit = _ => ()): Unit = {
    Files.createDirectories(journal.getParent)
    def record(line: String): Unit = {
      Files.write(journal, (line + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.DSYNC)
      ()
    }
    // Trong lúc ROLLBACK, nhật ký chỉ là best-effort: ghi hỏng không được ngăn việc đảo ngược (review P3 vòng 2, R2-M2).
    def bestEffort(line: String): Unit = { scala.util.Try(record(line)); () }
    var done = List.empty[Move] // các bước đã xong, mới nhất ở đầu — cập nhật NGAY sau move, trước mọi thao tác có thể ném lỗi
    try moves.foreach { m =>
      beforeMove(m)
      record(s"INTENT	${m.from}	${m.to}") // ghi ý định TRƯỚC khi move: chết giữa chừng vẫn biết bước nào có thể đã xảy ra
      Files.createDirectories(m.to.getParent)
      Files.move(m.from, m.to, StandardCopyOption.ATOMIC_MOVE)
      done = m :: done
      record(s"DONE	${m.from}	${m.to}")
    } catch {
      case e: Exception =>
        bestEffort(s"FAILED	$e")
        val undo = scala.util.Try(done.foreach { m =>
          Files.move(m.to, m.from, StandardCopyOption.ATOMIC_MOVE)
          bestEffort(s"UNDONE	${m.to}	${m.from}")
        })
        bestEffort(if (undo.isSuccess) "ROLLED_BACK" else s"ROLLBACK_FAILED	${undo.failed.get}")
        throw new PublishFailed(s"publish hỏng sau ${done.size}/${moves.size} bước: $e", e, journal, undo.isSuccess)
    }
    bestEffort("COMMITTED") // dữ liệu đã thay xong; thiếu dòng này chỉ làm nhật ký chưa đủ, không làm hỏng publish
  }
}
