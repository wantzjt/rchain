package coop.rchain.rspace

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import cats.syntax.either._
import coop.rchain.rspace.util.withResource
import coop.rchain.shared.ByteBufferInputStream

import scala.collection.mutable.ArrayBuffer

package object examples {

  def makeSerializeFromSerializable[T <: Serializable]: Serialize[T] =
    new Serialize[T] {

      def encode(a: T): ArrayBuffer[Byte] =
        withResource(new ByteArrayOutputStream()) { baos =>
          withResource(new ObjectOutputStream(baos)) { (oos: ObjectOutputStream) =>
            oos.writeObject(a)
          }
          ArrayBuffer[Byte]() ++= baos.toByteArray
        }

      def decode(bytes: ByteBuffer): Either[Throwable, T] =
        Either.catchNonFatal {
          withResource(new ByteBufferInputStream(bytes)) { bais =>
            withResource(new ObjectInputStream(bais)) { ois =>
              ois.readObject.asInstanceOf[T]
            }
          }
        }
    }
}
