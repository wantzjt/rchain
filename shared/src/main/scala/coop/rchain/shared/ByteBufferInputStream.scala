package coop.rchain.shared

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(private val buffer: ByteBuffer) extends InputStream {

  private var closed = false

  override def read(dest: Array[Byte]): Int =
    read(dest, 0, dest.length)

  override def read(dest: Array[Byte], offset: Int, length: Int): Int =
    if (closed || buffer.remaining() == 0) {
      -1
    } else {
      val count = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, count)
      count
    }

  override def read(): Int =
    if (closed || buffer.remaining() == 0) {
      -1
    } else {
      buffer.get() & 0xFF
    }

  override def skip(bytes: Long): Long =
    if (closed) {
      0L
    } else {
      val count = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position + count)
      count
    }

  override def close(): Unit =
    closed = true
}
