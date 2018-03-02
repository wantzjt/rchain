package coop.rchain.storage

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest

import cats.implicits._
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

  private[storage] def hashC(packedCs: H): H =
    LMDBStore.hashBytes(packedCs)

  private[storage] def hashC(c: C)(implicit serialize: Serialize[C]): H =
    LMDBStore.hashBytes(serialize.encode(c))

  private[storage] def hashC(cs: List[C])(implicit serialize: Serialize[C]): H =
    LMDBStore.hashBytes(LMDBStore.toBB(cs)(serialize))

  private[storage] def getKey(txn: T, s: H): List[C] =
    LMDBStore.fromBB[C](Option(_dbKeys.get(txn, s))).getOrElse(List.empty)

  private[storage] def putCs(txn: T, channels: List[C]): Unit =
    putCsH(txn, channels)

  private[storage] def putCsH(txn: T, channels: List[C]): H = {
    val packedCs = LMDBStore.toBB(channels)
    val hashCs   = hashC(packedCs)
    _dbKeys.put(txn, hashCs, packedCs)
    hashCs
  }

  type T = Txn[ByteBuffer]

  def createTxnRead(): T = env.txnWrite

  def createTxnWrite(): T = env.txnWrite

  def withTxn[R](txn: T)(f: T => R): R =
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

  def putA(txn: T, channels: List[C], a: A): Unit = {
    val hashCs = putCsH(txn, channels)
    val as     = LMDBStore.fromBB[A](Option(_dbAs.get(txn, hashCs))).getOrElse(List.empty)
    _dbPs.put(txn, hashCs, LMDBStore.toBB(a :: as))
  }

  def putK(txn: T, channels: List[C], patterns: List[P], k: K): Unit = {
    val hashCs = putCsH(txn, channels)
    val ps     = LMDBStore.fromBB[P](Option(_dbPs.get(txn, hashCs))).getOrElse(List.empty)
    _dbPs.put(txn, hashCs, LMDBStore.toBB(patterns ++ ps))
    _dbK.put(txn, hashCs, LMDBStore.toBB(List(k)))
  }

  def getPs(txn: T, channels: List[C]): List[P] = {
    val hashCs = hashC(channels)
    LMDBStore.fromBB[P](Option(_dbPs.get(txn, hashCs))).getOrElse(List.empty)
  }

  def getAs(txn: T, channels: List[C]): List[A] = {
    val hashCs = hashC(channels)
    LMDBStore.fromBB[A](Option(_dbAs.get(txn, hashCs))).getOrElse(List.empty)
  }

  def getK(txn: T, curr: List[C]): Option[(List[P], K)] = {
    val hashCs = hashC(curr)
    for {
      ps <- LMDBStore.fromBB[P](Option(_dbPs.get(txn, hashCs)))
      k  <- LMDBStore.fromBB[K](Option(_dbK.get(txn, hashCs))).flatMap(_.headOption)
    } yield (ps, k)
  }

  def removeA(txn: T, channels: List[C], index: Int): Unit = {
    val hashCs = hashC(channels)
    for (as <- LMDBStore.fromBB[A](Option(_dbAs.get(txn, hashCs)))) {
      val newAs = util.dropIndex(as, index)
      _dbAs.put(txn, hashCs, LMDBStore.toBB(newAs))
    }
  }

  def removeK(txn: T, channels: List[C], index: Int): Unit = {
    val hashCs = hashC(channels)
    for (ps <- LMDBStore.fromBB[P](Option(_dbPs.get(txn, hashCs)))) {
      val newPs = util.dropIndex(ps, index)
      _dbPs.put(txn, hashCs, LMDBStore.toBB(newPs))
    }
    _dbK.delete(txn, hashCs)
  }

  def addJoin(txn: T, c: C, cs: List[C]): Unit = {
//    val joinKey = hashC(c)
//    val csKey   = putCsH(txn, cs)
//    val oldCsList =
//      LMDBStore.fromBB[BytesList](Option(_dbJoins.get(txn, joinKey))).getOrElse(List.empty)
//    _dbJoins.put(txn, joinKey, LMDBStore.toBB(csKey :: oldCsList))
  }

  def getJoin(txn: T, c: C): List[List[C]] =
//    val joinKey = hashC(c)
//    val oldCsList = LMDBStore.fromBB[BytesList](Option(_dbJoins.get(txn, joinKey)))
//    oldCsList.flatten.map(
//
//    )
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
    val fetched = new Array[Byte](bs.remaining())
    ignore { bs.get(fetched) }
    val dataArr    = MessageDigest.getInstance("SHA-256").digest(fetched)
    val byteBuffer = ByteBuffer.allocateDirect(dataArr.length)
    byteBuffer.put(dataArr).flip()
    byteBuffer
  }

  def hashBytes(bs: Array[Byte]): ByteBuffer = {
    val dataArr    = MessageDigest.getInstance("SHA-256").digest(bs)
    val byteBuffer = ByteBuffer.allocateDirect(dataArr.length)
    byteBuffer.put(dataArr).flip()
    byteBuffer
  }

  def hashString(s: String): ByteBuffer =
    hashBytes(s.getBytes(StandardCharsets.UTF_8))

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

  private[storage] def toBB[TItem](values: List[TItem])(
      implicit serialize: Serialize[TItem]): ByteBuffer = {
    val encoded        = values.map(serialize.encode)
    val bl             = BytesList().withValues(encoded.map(ByteString.copyFrom)).toByteArray
    val bb: ByteBuffer = ByteBuffer.allocateDirect(bl.length)
    bb.put(bl).flip()
    bb
  }

  private[storage] def fromBB[TItem](bytesOpt: Option[ByteBuffer])(
      implicit serialize: Serialize[TItem]): Option[List[TItem]] =
    bytesOpt.map(bytes => {
      val fetched = new Array[Byte](bytes.remaining())
      ignore { bytes.get(fetched) }
      val bl = BytesList.parseFrom(bytes.array)
      val x: Either[Throwable, List[TItem]] = bl.values
        .map(x => serialize.decode(x.toByteArray))
        .toList
        .sequence[Either[Throwable, ?], TItem]
      x match {
        case Left(err)     => throw new Exception(err)
        case Right(values) => values
      }
    })
}

/*
class Storage private (env: Env[ByteBuffer], db: Dbi[ByteBuffer])
    extends IStorage
    with AutoCloseable {

  def put[A](key: Key, value: A)(implicit s: Serialize[A]): Either[Error, Unit] =
    Either
      .catchNonFatal {
        val encoded = s.encode(value)
        val keyBuff = ByteBuffer.allocateDirect(env.getMaxKeySize)
        val valBuff = ByteBuffer.allocateDirect(encoded.length)
        ignore { keyBuff.put(key.bytes).flip() }
        ignore { valBuff.put(encoded).flip() }
        db.put(keyBuff, valBuff)
      }
      .leftMap(StorageError.apply)

  def get[A](key: Key)(implicit s: Serialize[A]): Either[Error, A] =
    Either
      .catchNonFatal {
        val keyBuff = ByteBuffer.allocateDirect(env.getMaxKeySize)
        ignore { keyBuff.put(key.bytes).flip() }
        withResource(env.txnRead()) { (txn: Txn[ByteBuffer]) =>
          if (db.get(txn, keyBuff) != null) {
            val fetchedBuff = txn.`val`()
            val fetched     = new Array[Byte](fetchedBuff.remaining())
            ignore { fetchedBuff.get(fetched) }
            s.decode(fetched)
          } else {
            Left[Error, A](NotFound)
          }
        }
      }
      .leftMap(StorageError.apply)
      .joinRight

  def remove(key: Key): Either[Error, Boolean] =
    Either
      .catchNonFatal {
        val keyBuff = ByteBuffer.allocateDirect(env.getMaxKeySize)
        ignore { keyBuff.put(key.bytes).flip() }
        db.delete(keyBuff)
      }
      .leftMap(StorageError.apply)

  def close(): Unit = {
    db.close()
    env.close()
  }
}

object Storage {

  /**
 * Creates an instance of [[Storage]]
 *
 * @param path Path to the database files
 * @param name Name of the database
 * @param mapSize Maximum size of the database, in bytes
 */
  def create(path: Path, name: String, mapSize: Long): Storage = {
    val env: Env[ByteBuffer] =
      Env.create().setMapSize(mapSize).setMaxDbs(1).open(path.toFile)
    val db: Dbi[ByteBuffer] = env.openDbi(name, MDB_CREATE)
    new Storage(env, db)
  }

 */
