package vn.scalaeng.quality

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/** GOLDEN (snapshot) test: output đã được NGƯỜI duyệt một lần, lưu thành file trong git. Test so output mới với file đó.
  *   - Khác → test đỏ, ghi `<tên>.actual` cạnh file golden để `diff`/review.
  *   - Thay đổi CÓ CHỦ ĐÍCH → chạy với `UPDATE_GOLDEN=1` để ghi đè golden, rồi review diff trong pull request như review code.
  * Golden chỉ đáng tin khi dữ liệu mẫu được TÍNH TAY (Chương 24 mục 6) và bao phủ các trường hợp khó (trùng, trễ, xung đột, mồ côi).
  */
object Golden:
  val dir: Path = Paths.get("golden")

  def check(name: String, actual: String): Either[String, Unit] =
    val file = dir.resolve(name)
    if sys.env.get("UPDATE_GOLDEN").contains("1") then
      Files.createDirectories(dir); Files.write(file, actual.getBytes(StandardCharsets.UTF_8)); Right(())
    else if !Files.exists(file) then Left(s"chưa có golden $file — chạy với UPDATE_GOLDEN=1 sau khi tự kiểm tra output")
    else
      val expected = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).replace("\r\n", "\n")
      val actualLf = actual.replace("\r\n", "\n") // chuẩn hóa CẢ HAI phía: writer khác có thể sinh CRLF
      if expected == actualLf then Right(())
      else
        val act = dir.resolve(name + ".actual")
        Files.write(act, actual.getBytes(StandardCharsets.UTF_8))
        val (e, a) = (expected.linesIterator.toVector, actualLf.linesIterator.toVector)
        val diff = (e.diff(a).map("- " + _) ++ a.diff(e).map("+ " + _)).mkString("\n")
        Left(s"$name khác golden (đã ghi $act):\n$diff")
