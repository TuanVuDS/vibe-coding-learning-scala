package vn.scalaeng.formats

import java.nio.ByteBuffer
import java.nio.file.Path
import org.apache.parquet.io.{InputFile, LocalInputFile, SeekableInputStream}

/** Count logical bytes returned by the InputFile API, not physical disk traffic. */
final class CountingInputFile(delegate: InputFile) extends InputFile:
  private var countedBytes: Long = 0L
  def bytesRead: Long = countedBytes

  override def getLength(): Long = delegate.getLength()
  override def newStream(): SeekableInputStream =
    val in = delegate.newStream()
    new SeekableInputStream:
      override def getPos(): Long = in.getPos()
      override def seek(position: Long): Unit = in.seek(position)
      override def read(): Int =
        val result = in.read()
        if result >= 0 then countedBytes += 1
        result
      override def read(bytes: Array[Byte], offset: Int, length: Int): Int =
        val count = in.read(bytes, offset, length)
        if count > 0 then countedBytes += count
        count
      override def readFully(bytes: Array[Byte]): Unit =
        in.readFully(bytes)
        countedBytes += bytes.length
      override def readFully(bytes: Array[Byte], offset: Int, length: Int): Unit =
        in.readFully(bytes, offset, length)
        countedBytes += length
      override def read(buffer: ByteBuffer): Int =
        val count = in.read(buffer)
        if count > 0 then countedBytes += count
        count
      override def readFully(buffer: ByteBuffer): Unit =
        val remaining = buffer.remaining()
        in.readFully(buffer)
        countedBytes += remaining
      override def close(): Unit = in.close()

object ProjectionProbe:
  def main(args: Array[String]): Unit =
    require(args.length == 1, "usage: ProjectionProbe <parquet-file>")
    val full = CountingInputFile(LocalInputFile(Path.of(args(0))))
    val projected = CountingInputFile(LocalInputFile(Path.of(args(0))))
    val digest = Digest.of(consume => ParquetOrders.scanFile(full, consume))
    val amount = ParquetOrders.sumFile(projected)
    require(amount == digest.amount)
    println(s"rows=${digest.count} amountMinor=$amount fullApiBytes=${full.bytesRead} projectedApiBytes=${projected.bytesRead}")
