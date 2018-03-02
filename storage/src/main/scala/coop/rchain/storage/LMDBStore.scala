package coop.rchain.storage

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest

import com.google.protobuf.{ByteString, MessageLite}
import cats.syntax.either._
import coop.rchain.storage.util._
import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.{Dbi, Env, EnvFlags, Txn}
import coop.rchain.models.{BytesList, Serialize, SerializeInstances}

class LMDBStore[C, P, A, K] private (env: Env[ByteBuffer],
                                     _dbKeys: Dbi[ByteBuffer],
                                     _dbPs: Dbi[ByteBuffer],
                                     _dbAs: Dbi[ByteBuffer],
                                     _dbK: Dbi[ByteBuffer],
                                     _dbJoins: Dbi[ByteBuffer])(implicit sc: Serialize[C],
                                                                pc: Serialize[P],
                                                                ac: Serialize[A],
                                                                kc: Serialize[K])
    extends IStore[C, P, A, K]
    with SerializeInstances {
  type H = ByteBuffer

  private[storage] def toBB[TItem](values: List[TItem])(implicit serialize: Serialize[TItem]): H = {
    val encoded = values.map(serialize.encode)
    val bl      = BytesList().withValues(encoded.map(ByteString.copyFrom))
    ByteBuffer.wrap(bl.toByteArray)
  }

  private[storage] def fromBB[TItem](bytesOpt: Option[ByteBuffer])(
      implicit serialize: Serialize[TItem]): Option[List[TItem]] =
    bytesOpt.map(bytes => {
      val bl = BytesList.parseFrom(bytes.array)
      val x = bl.values
        .map(x => serialize.decode(x.toByteArray))
        .filter(_.isRight)
        .map(_.right.get)
        .toList
      x
    })

  private[storage] def hashC(packedCs: H): H =
    LMDBStore.hashBytes(packedCs)

  private[storage] def hashC(c: C)(implicit serialize: Serialize[C]): H =
    LMDBStore.hashBytes(serialize.encode(c))

  private[storage] def hashC(cs: List[C])(implicit serialize: Serialize[C]): H =
    LMDBStore.hashBytes(toBB(cs)(serialize))

  private[storage] def getKey(txn: T, s: H): List[C] =
    fromBB[C](Option(_dbKeys.get(txn, s))).getOrElse(List.empty)

  private[storage] def putCs(txn: T, channels: List[C]): Unit =
    putCsH(txn, channels)

  private[storage] def putCsH(txn: T, channels: List[C]): H = {
    val packedCs = toBB(channels)
    val hashCs   = hashC(packedCs)
    _dbKeys.put(txn, hashCs, packedCs)
    hashCs
  }

  type T = Txn[ByteBuffer]

  def createTxnRead(): T = env.txnWrite

  def createTxnWrite(): T = env.txnWrite

  def withTxn[R](txn: T)(f: T => R): R =
    f(txn)

  def putA(txn: T, channels: List[C], a: A): Unit = {
    val hashCs = putCsH(txn, channels)
    val as     = fromBB[A](Option(_dbAs.get(txn, hashCs))).getOrElse(List.empty)
    _dbPs.put(txn, hashCs, toBB(a :: as))
  }

  def putK(txn: T, channels: List[C], patterns: List[P], k: K): Unit = {
    val hashCs = putCsH(txn, channels)
    val ps     = fromBB[P](Option(_dbPs.get(txn, hashCs))).getOrElse(List.empty)
    _dbPs.put(txn, hashCs, toBB(patterns ++ ps))
    _dbK.put(txn, hashCs, toBB(List(k)))
  }

  def getPs(txn: T, channels: List[C]): List[P] = {
    val hashCs = hashC(channels)
    fromBB[P](Option(_dbPs.get(txn, hashCs))).getOrElse(List.empty)
  }

  def getAs(txn: T, channels: List[C]): List[A] = {
    val hashCs = hashC(channels)
    fromBB[A](Option(_dbAs.get(txn, hashCs))).getOrElse(List.empty)
  }

  def getK(txn: T, curr: List[C]): Option[(List[P], K)] = {
    val hashCs = hashC(curr)
    for {
      ps <- fromBB[P](Option(_dbPs.get(txn, hashCs)))
      k  <- fromBB[K](Option(_dbK.get(txn, hashCs))).flatMap(_.headOption)
    } yield (ps, k)
  }

  def removeA(txn: T, channels: List[C], index: Int): Unit = {
    val hashCs = hashC(channels)
    for (as <- fromBB[A](Option(_dbAs.get(txn, hashCs)))) {
      val newAs = LMDBStore.dropIndex(as, index)
      _dbAs.put(txn, hashCs, toBB(newAs))
    }
  }

  def removeK(txn: T, channels: List[C], index: Int): Unit = {
    val hashCs = hashC(channels)
    for (ps <- fromBB[P](Option(_dbPs.get(txn, hashCs)))) {
      val newPs = LMDBStore.dropIndex(ps, index)
      _dbPs.put(txn, hashCs, toBB(newPs))
    }
    _dbK.delete(txn, hashCs)
  }

  def addJoin(txn: T, c: C, cs: List[C]): Unit = {}

  def getJoin(txn: T, c: C): List[List[C]] =
    throw new NotImplementedError("TODO")

  def removeJoin(txn: T, c: C, cs: List[C]): Unit = {}

  def removeAllJoins(txn: T, c: C): Unit = {}

  def close(): Unit = {
    _dbKeys.close()
    _dbAs.close()
    _dbPs.close()
    _dbK.close()
    _dbJoins.close()
  }
}

object LMDBStore {
  def hashBytes(bs: ByteBuffer): ByteBuffer = {
    val dataArr    = MessageDigest.getInstance("SHA-256").digest(bs.array)
    val byteBuffer = ByteBuffer.allocateDirect(dataArr.length)
    byteBuffer.put(dataArr)
  }

  def hashBytes(bs: Array[Byte]): ByteBuffer = {
    val dataArr    = MessageDigest.getInstance("SHA-256").digest(bs)
    val byteBuffer = ByteBuffer.allocateDirect(dataArr.length)
    byteBuffer.put(dataArr)
  }

  def hashString(s: String): ByteBuffer =
    hashBytes(s.getBytes(StandardCharsets.UTF_8))

  /** Drops the 'i'th element of a list.
    */
  def dropIndex[TItem](xs: List[TItem], n: Int): List[TItem] = {
    val (l1, l2) = xs splitAt n
    l1 ++ (l2 drop 1)
  }

  /**
		* Creates an instance of [[IStore]]
		* @param path Path to the database files
		* @param mapSize Maximum size of the database, in bytes
		*/
  def create[C, P, A, K](path: Path, mapSize: Long)(implicit sc: Serialize[C],
                                                    pc: Serialize[P],
                                                    ac: Serialize[A],
                                                    kc: Serialize[K]): IStore[C, P, A, K] = {
    val env: Env[ByteBuffer] =
      Env.create().setMapSize(mapSize).setMaxDbs(8).open(path.toFile)

    val dbKeys: Dbi[ByteBuffer]  = env.openDbi("Keys", MDB_CREATE)
    val dbPs: Dbi[ByteBuffer]    = env.openDbi("Ps", MDB_CREATE)
    val dbAs: Dbi[ByteBuffer]    = env.openDbi("As", MDB_CREATE)
    val dbK: Dbi[ByteBuffer]     = env.openDbi("K", MDB_CREATE)
    val dbJoins: Dbi[ByteBuffer] = env.openDbi("Joins", MDB_CREATE)
    new LMDBStore[C, P, A, K](env, dbKeys, dbPs, dbAs, dbK, dbJoins)
  }
}