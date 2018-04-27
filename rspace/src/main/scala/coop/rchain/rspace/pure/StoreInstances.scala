package coop.rchain.rspace.pure

import java.nio.ByteBuffer

import cats.data.ReaderT
import coop.rchain.catscontrib.Capture
import coop.rchain.rspace.Serialize
import coop.rchain.rspace.internal._
import org.lmdbjava.Txn
import java.security.MessageDigest

import coop.rchain.rspace.util.ignore
import coop.rchain.rspace.internal.scodecs._
import scodec.Codec
import scodec.bits.{BitVector, ByteVector}

import scala.collection.immutable.Seq

object StoreInstances {

  implicit def storeLMDB[F[_], C, P, A, K](
      implicit
      serializeC: Serialize[C],
      serializeP: Serialize[P],
      serializeA: Serialize[A],
      serializeK: Serialize[K],
      captureF: Capture[F]): Store[ReaderT[F, LMDBContext, ?], C, P, A, K] =
    new Store[ReaderT[F, LMDBContext, ?], C, P, A, K] {

      private[this] def capture[X](x: X): F[X] = captureF.capture(x)

      type T = Txn[ByteBuffer]

      def createTxnRead(): ReaderT[F, LMDBContext, Txn[ByteBuffer]] =
        ReaderT(ctx => capture(ctx.env.txnRead()))

      def createTxnWrite(): ReaderT[F, LMDBContext, Txn[ByteBuffer]] =
        ReaderT(ctx => capture(ctx.env.txnWrite()))

      def withTxn[R](txn: Txn[ByteBuffer])(f: Txn[ByteBuffer] => R): R =
        try {
          val ret: R = f(txn)
          txn.commit()
          ret
        } catch {
          case ex: Throwable =>
            txn.abort()
            throw ex
        } finally {
          txn.close()
        }

      type H = ByteBuffer

      override def hashChannels(channels: Seq[C]): ReaderT[F, LMDBContext, ByteBuffer] =
        ReaderT(_ => capture(hashBytes(toByteBuffer(channels))))

      override def getChannels(txn: Txn[ByteBuffer],
                               channelsHash: ByteBuffer): ReaderT[F, LMDBContext, Seq[C]] =
        ReaderT(ctx =>
            capture(
              Option(ctx.dbKeys.get(txn, channelsHash))
                .map(fromByteBuffer[C])
                .getOrElse(Seq.empty[C])))

      private[rspace] def putChannels(txn: T, channels: Seq[C]): ReaderT[F, LMDBContext, H] =
        ReaderT(ctx => {
          val channelsBytes = toByteBuffer(channels)
          val channelsHash  = hashBytes(channelsBytes)
          ctx.dbKeys.put(txn, channelsHash, channelsBytes)
          capture(channelsHash)
        })

      private[this] def readDatumByteses(txn: T, channelsHash: H): ReaderT[F, LMDBContext, Option[Seq[DatumBytes]]] =
        ReaderT(ctx => capture(Option(ctx.dbData.get(txn, channelsHash)).map(fromByteBuffer(_, datumBytesesCodec))))

      private[this] def writeDatumByteses(txn: T, channelsHash: H, values: Seq[DatumBytes]): ReaderT[F, LMDBContext, Unit] = {
        if (values.nonEmpty) {
          ReaderT(ctx => {
            ctx.dbData.put(txn, channelsHash, toByteBuffer(values, datumBytesesCodec))
            capture(())
          })
        } else {
          ReaderT(ctx => {
            ctx.dbData.delete(txn, channelsHash)
            collectGarbage(txn, channelsHash, waitingContinuationsCollected = true)
            capture(())
          })
        }
      }

      override def getData(txn: T, channels: Seq[C]): ReaderT[F, LMDBContext, Seq[Datum[A]]] = {
        hashChannels(channels).flatMap(channelsHash => readDatumByteses(txn, channelsHash))
          .map(datumByteses =>
            datumByteses.map(_.map(bytes => Datum(fromByteVector[A](bytes.datumBytes), bytes.persist)))
              .getOrElse(Seq.empty[Datum[A]]))
      }

      override def putDatum(txn: T,
                            channels: Seq[C],
                            datum: Datum[A]): ReaderT[F, LMDBContext, Unit] = ???

      override def removeDatum(txn: T,
                               channels: Seq[C],
                               index: Int): ReaderT[F, LMDBContext, Unit] = ???

      override def removeDatum(txn: T, channel: C, index: Int): ReaderT[F, LMDBContext, Unit] =
        ???

      override def removeWaitingContinuation(txn: T,
                                             channels: Seq[C],
                                             index: Int): ReaderT[F, LMDBContext, Unit] = ???

      override def putWaitingContinuation(
          txn: T,
          channels: Seq[C],
          continuation: WaitingContinuation[P, K]): ReaderT[F, LMDBContext, Unit] = ???

      override def getWaitingContinuation(
          txn: T,
          channels: Seq[C]): ReaderT[F, LMDBContext, Seq[WaitingContinuation[P, K]]] =
        throw new NotImplementedError()

      override def removeJoin(txn: Txn[ByteBuffer],
                              channel: C,
                              channels: Seq[C]): ReaderT[F, LMDBContext, Unit] = ???

      override def removeAllJoins(txn: T, channel: C): ReaderT[F, LMDBContext, Unit] =
        ???

      override def putJoin(txn: T, channel: C, channels: Seq[C]): ReaderT[F, LMDBContext, Unit] =
        ???

      override def getJoin(txn: T, channel: C): ReaderT[F, LMDBContext, Seq[Seq[C]]] =
        ???

      override def removeAll(txn: T, channels: Seq[C]): ReaderT[F, LMDBContext, Unit] = ???

      override def toMap: ReaderT[F, LMDBContext, Map[Seq[C], Row[P, A, K]]] = ???

      override def close(): ReaderT[F, LMDBContext, Unit] = ???

      def collectGarbage(txn: T,
                         channelsHash: H,
                         dataCollected: Boolean = false,
                         waitingContinuationsCollected: Boolean = false,
                         joinsCollected: Boolean = false): Unit = {}
    }

  private[rspace] def toByteVector[T](value: T)(implicit st: Serialize[T]): ByteVector =
    ByteVector(st.encode(value))

  private[rspace] def fromByteVector[T](vector: ByteVector)(implicit st: Serialize[T]): T =
    st.decode(vector.toArray) match {
      case Left(err)     => throw new Exception(err)
      case Right(result) => result
    }

  private[rspace] def toByteBuffer[T](value: T, codec: Codec[T]): ByteBuffer =
    toByteBuffer(toBitVector(value, codec))

  private[rspace] def toByteBuffer[T](values: Seq[T])(implicit st: Serialize[T]): ByteBuffer =
    toByteBuffer(toBitVector(toByteVectorSeq(values), byteVectorsCodec))

  private[rspace] def toByteBuffer(vector: BitVector): ByteBuffer = {
    val bytes          = vector.bytes
    val bb: ByteBuffer = ByteBuffer.allocateDirect(bytes.size.toInt)
    bytes.copyToBuffer(bb)
    bb.flip()
    bb
  }

  private[rspace] def toByteVectorSeq[T](values: Seq[T])(
      implicit st: Serialize[T]): Seq[ByteVector] =
    values.map(st.encode).map(ByteVector(_))

  private[rspace] def fromByteVectors[T](vectors: Seq[ByteVector])(
      implicit st: Serialize[T]): Seq[T] =
    vectors
      .map(_.toArray)
      .map(st.decode)
      .map {
        case Left(err)     => throw new Exception(err)
        case Right(values) => values
      }

  private[rspace] def toByteVectors(byteBuffer: ByteBuffer): Seq[Seq[ByteVector]] =
    fromBitVector(BitVector(byteBuffer), byteVectorsCodec)
      .map(x => fromBitVector(x.bits, byteVectorsCodec))

  private[rspace] def toByteBuffer(vectors: Seq[Seq[ByteVector]]): ByteBuffer = {
    val bl = vectors.map(toBitVector(_, byteVectorsCodec).toByteVector)
    toByteBuffer(bl, byteVectorsCodec)
  }

  private[rspace] def fromByteBuffer[T](byteBuffer: ByteBuffer)(implicit st: Serialize[T]): Seq[T] =
    fromBitVector(BitVector(byteBuffer), byteVectorsCodec)
      .map(_.toArray)
      .map(st.decode)
      .map {
        case Left(err)     => throw new Exception(err)
        case Right(values) => values
      }

  private[rspace] def fromByteBuffer[T](byteBuffer: ByteBuffer, codec: Codec[T]): T =
    fromBitVector(BitVector(byteBuffer), codec)

  private[rspace] def hashBytes(byteBuffer: ByteBuffer): ByteBuffer = {
    byteBuffer.mark()
    val fetched = new Array[Byte](byteBuffer.remaining())
    ignore {
      byteBuffer.get(fetched)
    }
    byteBuffer.reset()
    hashBytes(fetched)
  }

  private[rspace] def hashBytes(bytes: Array[Byte]): ByteBuffer = {
    val dataArr    = MessageDigest.getInstance("SHA-256").digest(bytes)
    val byteBuffer = ByteBuffer.allocateDirect(dataArr.length)
    byteBuffer.put(dataArr).flip()
    byteBuffer
  }
}
