package vn.scalaeng.p4

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import scala.util.Using

/** Một process/thread sở hữu store; thay snapshot nguyên tử trên cùng filesystem.
  * Chỉ kiểm chứng process crash, chưa kiểm mất điện hay quyền ghi phân tán.
  */
final class Store(dir: Path, width: Long = 100, delay: Long = 50,
    beforeReplace: () => Unit = () => (), clock: () => Long = () => java.time.Instant.now().getEpochSecond,
    maxFutureSkew: Long = 300, maxSnapshotBytes: Long = 1024 * 1024) extends AutoCloseable:
  require(width > 0 && delay >= 0, "invalid window policy")
  require(maxFutureSkew >= 0 && maxSnapshotBytes > 0, "invalid store limits")
  Files.createDirectories(dir)
  private val lockChannel = FileChannel.open(dir.resolve("writer.lock"),
    StandardOpenOption.CREATE, StandardOpenOption.WRITE)
  private val lock =
    try
      val acquired = lockChannel.tryLock()
      if acquired == null then throw new IllegalStateException("store already owned")
      acquired
    catch
      case e: OverlappingFileLockException =>
        lockChannel.close()
        throw new IllegalStateException("store already owned", e)
      case e: Exception =>
        lockChannel.close()
        throw e
  private val target = dir.resolve("state.json")
  private var current =
    try
      if Files.exists(target) then checkSize(Files.size(target))
      val loaded = if Files.exists(target) then Json.restore(Files.readString(target, UTF_8))
        else State(2, width, delay, maxFutureSkew, 0, Map.empty, Vector.empty, Map.empty)
      require(loaded.format == 2 && loaded.width == width && loaded.delay == delay &&
        loaded.maxFutureSkew == maxFutureSkew, "snapshot policy mismatch")
      loaded
    catch
      case e: Exception =>
        lock.release()
        lockChannel.close()
        throw e

  def state: State = current

  private def checkSize(size: Long): Unit =
    if size > maxSnapshotBytes then throw new IllegalStateException("snapshot byte limit exceeded")

  private def save(next: State): Unit =
    val encoded = Json.snapshot(next).getBytes(UTF_8)
    checkSize(encoded.length.toLong)
    val tmp = dir.resolve("state.tmp") // Bỏ qua tmp còn dở sau crash, không phục hồi từ nó.
    Using.resource(FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)) { channel =>
      val bytes = ByteBuffer.wrap(encoded)
      while bytes.hasRemaining do channel.write(bytes)
      channel.force(true)
    }
    beforeReplace()
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    current = next // Chỉ công bố state RAM khi thay snapshot thành công.

  /** prepare chỉ đọc/kiểm tra; không tạo tác động không thể rollback. */
  def process(in: Input, prepare: Event => Unit = _ => (), maxAttempts: Int = 3,
      pause: Int => Unit = n => Thread.sleep(10L * n)): Receipt =
    require(in.partition >= 0 && in.offset >= 0 && in.topic.matches("[A-Za-z0-9._-]+"), "invalid source identity")
    current.receipts.get(in.id) match
      case Some(old) =>
        require(old.input == in, "source identity reused with different payload")
        old
      case None =>
        var next = current
        val result = Json.event(in.raw) match
          case Left(reason) => Receipt(in, "invalid", Some(reason), 0)
          case Right(e) if in.key != Some(e.orderId) => Receipt(in, "invalid", Some("key must equal orderId"), 0)
          case Right(e) if BigInt(e.eventTime) > BigInt(clock()) + maxFutureSkew =>
            Receipt(in, "invalid", Some("eventTime in the future"), 0)
          case Right(e) => current.seen.get(e.eventId) match
            case Some(old) if old == e => Receipt(in, "duplicate", None, 0)
            case Some(_) => Receipt(in, "conflict", Some("event_id conflict"), 0)
            case None if e.eventTime < (current.maxTime - delay).max(0) =>
              Receipt(in, "late", Some("event time below previous watermark"), 0)
            case None => Retry.run(maxAttempts, pause)(prepare(e)) match
              case Left((reason, attempts)) => Receipt(in, "exhausted", Some(reason), attempts)
              case Right(attempts) =>
                val start = e.eventTime / width * width
                val previous = current.buckets.find(b => b.start == start && b.orderId == e.orderId)
                val updated = Bucket(start, e.orderId, previous.fold(BigInt(0))(_.total) + BigInt(e.amount))
                next = current.copy(maxTime = current.maxTime.max(e.eventTime),
                  seen = current.seen.updated(e.eventId, e), buckets =
                    current.buckets.filterNot(b => b.start == start && b.orderId == e.orderId) :+ updated)
                Receipt(in, "accepted", None, attempts)
        save(next.copy(receipts = next.receipts.updated(in.id, result)))
        result

  def close(): Unit =
    try lock.release()
    finally lockChannel.close()

object Pipeline:
  /** Chờ DLT ack trước source commit; lỗi truyền lên để replay nguồn. */
  def handle(store: Store, input: Input, publish: Receipt => Unit, commit: () => Unit,
      prepare: Event => Unit = _ => (), pause: Int => Unit = n => Thread.sleep(10L * n)): Receipt =
    val result = store.process(input, prepare, pause = pause)
    if result.dead then publish(result)
    commit()
    result
