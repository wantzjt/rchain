package coop.rchain.rspace.pure

import java.nio.file.{Files, Path}

import cats.data.ReaderT
import cats.implicits._
import cats.{Id, Monad}
import com.typesafe.scalalogging.Logger
import coop.rchain.catscontrib.Capture
import coop.rchain.rspace.examples.StringExamples
import org.scalatest._
import coop.rchain.rspace.pure.rspace._
import coop.rchain.rspace.extended._
import coop.rchain.rspace.test._
import coop.rchain.rspace.examples.StringExamples._
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.internal.Datum
import coop.rchain.rspace.test.recursivelyDeletePath
import org.scalatest._

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

abstract class StorageTestsBase[F[_], CTX] extends FlatSpec with Matchers with OptionValues {
  type TStore =
    Store[ReaderT[F, CTX, ?], String, Pattern, String, StringsCaptor] with ITestableStore[
      ReaderT[F, CTX, ?],
      String,
      Pattern]

  type TCTX = CTX

  val logger: Logger = Logger(this.getClass.getName.stripSuffix("$"))

  val ctx: CTX

  implicit val monadF: Monad[F]

  implicit val captureF: Capture[F]

  def createStore: TStore

  def consume(channels: Seq[String],
              patterns: Seq[Pattern],
              continuation: StringsCaptor,
              persist: Boolean)(implicit store: TStore) =
    coop.rchain.rspace.pure.rspace
      .consume[ReaderT[F, CTX, ?], String, Pattern, String, StringsCaptor](channels,
                                                                           patterns,
                                                                           continuation,
                                                                           persist)

  override def withFixture(test: NoArgTest): Outcome = {
    logger.debug(s"Test: ${test.name}")
    super.withFixture(test)
  }

  def withTestMany(f: TStore => Seq[ReaderT[F, CTX, _]]): Unit = {
    val store = createStore
    store.clear().run(ctx)
    try {
      for (runnable <- f(store)) {
        runnable.run(ctx)
      }
    } finally {
      store.close().run(ctx)
    }
  }

  def withTest(f: TStore => ReaderT[F, CTX, _]): Unit = {
    val store = createStore
    store.clear().run(ctx)
    try {
      val runnable = f(store)
      runnable.run(ctx)
    } finally {
      store.close().run(ctx)
    }
  }

  def withError[TErr: ClassTag](f: TStore => ReaderT[F, CTX, _]): Unit = {
    val store = createStore
    store.clear().run(ctx)
    try {
      an[TErr] shouldBe thrownBy(f(store).run(ctx))
    } finally {
      store.close().run(ctx)
    }
  }

  "produce" should
    "persist a piece of data in the store" in withTest { implicit store =>
    val key = List("ch1")

    def check(r: Option[(StringExamples.StringsCaptor, Seq[String])]): ReaderT[F, CTX, Unit] =
      store.createTxnRead().flatMap { txn =>
        store.withTxn(txn) { txn =>
          for {
            keyHash <- store.hashChannels(key)
            _       <- store.getChannels(txn, keyHash).map(_ shouldBe key)
            _       <- store.getPatterns(txn, key).map(_ shouldBe Nil)
            _       <- store.getData(txn, key).map(_ shouldBe List(Datum("datum", persist = false)))
            _       <- store.getWaitingContinuation(txn, key).map(_ shouldBe Nil)
            _       <- store.isEmpty(txn).map(_ shouldBe false)
            _       <- ReaderT.pure[F, CTX, Assertion](r shouldBe None)
          } yield ()
        }
      }

    for {
      r <- produce(key.head, "datum", persist = false)
      _ <- check(r)
    } yield ()
  }

  "producing twice on the same channel" should
    "persist two pieces of data in the store" in withTestMany { implicit store =>
    val key = List("ch1")

    val step1 = for {
      r1      <- produce(key.head, "datum1", persist = false)
      keyHash <- store.hashChannels(key)
      txn     <- store.createTxnRead()
    } yield {
      r1 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, keyHash).map(_ shouldBe key)
        store.getPatterns(txn, key).map(_ shouldBe Nil)
        store.getData(txn, key).map(_ shouldBe List(Datum("datum1", persist = false)))
        store.getWaitingContinuation(txn, key).map(_ shouldBe Nil)
      }
    }

    val step2 = for {
      r2      <- produce(key.head, "datum2", persist = false)
      keyHash <- store.hashChannels(key)
      txn     <- store.createTxnRead()
    } yield {
      r2 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, keyHash).map(_ shouldBe key)
        store.getPatterns(txn, key).map(_ shouldBe Nil)
        store
          .getData(txn, key)
          .map(_ should contain theSameElementsAs List(Datum("datum1", persist = false),
                                                       Datum("datum2", persist = false)))

        store.getWaitingContinuation(txn, key).map(_ shouldBe Nil)
        //store is not empty - we have 2 As stored
        store.isEmpty(txn).map(_ shouldBe false)
      }
    }

    List(step1, step2)
  }

  "consuming on one channel" should
    "persist a continuation in the store" in withTest { implicit store =>
    val key      = List("ch1")
    val patterns = List(Wildcard)

    for {
      keyHash <- store.hashChannels(key)
      r       <- consume(key, patterns, new StringsCaptor, persist = false)
      txn     <- store.createTxnRead()
    } yield {
      r shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, keyHash).map(_ shouldBe List("ch1"))
        store.getPatterns(txn, key).map(_ shouldBe List(patterns))
        store.getData(txn, key).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, key).map(_ should not be empty)

        //there is a continuation stored in the storage
        store.isEmpty(txn).map(_ shouldBe false)
      }
    }
  }

  "consuming with a list of patterns that is a different length than the list of channels" should
    "throw" in withError[IllegalArgumentException] { implicit store =>
    for {
      r <- consume(List("ch1", "ch2"), List(Wildcard), new StringsCaptor, persist = false)
    } yield {
      r shouldBe None
    }
  }

  "consuming on three channels" should
    "persist a continuation in the store" in withTest { implicit store =>
    val key      = List("ch1", "ch2", "ch3")
    val patterns = List(Wildcard, Wildcard, Wildcard)

    for {
      keyHash <- store.hashChannels(key)
      r       <- consume(key, patterns, new StringsCaptor, persist = false)
      txn     <- store.createTxnRead()
    } yield {
      r shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, keyHash).map(_ shouldBe key)
        store.getPatterns(txn, key).map(_ shouldBe List(patterns))
        store.getData(txn, key).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, key).map(_ should not be empty)
        //continuation is left in the storage
        store.isEmpty(txn).map(_ shouldBe false)
      }
    }
  }

  "producing and then consuming on the same channel" should
    "return the continuation and data" in withTestMany { implicit store =>
    val key = List("ch1")
    val step1 = for {
      keyHash <- store.hashChannels(key)
      r1      <- produce(key.head, "datum", persist = false)
      txn     <- store.createTxnRead()
    } yield {
      r1 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, keyHash).map(_ shouldBe key)
        store.getPatterns(txn, key).map(_ shouldBe Nil)
        store.getData(txn, key).map(_ shouldBe List(Datum("datum", persist = false)))
        store.getWaitingContinuation(txn, key).map(_ shouldBe Nil)
      }
    }

    val step2 = for {
      keyHash <- store.hashChannels(key)
      r2      <- consume(key, List(Wildcard), new StringsCaptor, persist = false)
      txn     <- store.createTxnRead()
    } yield {
      r2 shouldBe defined

      runK(r2)

      getK(r2).results should contain theSameElementsAs List(List("datum"))

      store.withTxn(txn) { txn =>
        store.isEmpty(txn).map(_ shouldBe true)
        store.getChannels(txn, keyHash).map(_ shouldBe Nil)
        store.getPatterns(txn, key).map(_ shouldBe Nil)
        store.getData(txn, key).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, key).map(_ shouldBe Nil)
      }
    }

    List(step1, step2)
  }

  "producing three times then doing consuming three times" should "work" in withTest {
    implicit store =>
      for {
        r1  <- produce("ch1", "datum1", persist = false)
        r2  <- produce("ch1", "datum2", persist = false)
        r3  <- produce("ch1", "datum3", persist = false)
        r4  <- consume(List("ch1"), List(Wildcard), new StringsCaptor, persist = false)
        r5  <- consume(List("ch1"), List(Wildcard), new StringsCaptor, persist = false)
        r6  <- consume(List("ch1"), List(Wildcard), new StringsCaptor, persist = false)
        txn <- store.createTxnRead()
      } yield {
        r1 shouldBe None
        r2 shouldBe None
        r3 shouldBe None

        runK(r4)
        getK(r4).results should contain oneOf (List("datum1"), List("datum2"), List("datum3"))

        runK(r5)
        getK(r5).results should contain oneOf (List("datum1"), List("datum2"), List("datum3"))

        runK(r6)
        getK(r6).results should contain oneOf (List("datum1"), List("datum2"), List("datum3"))

        store.withTxn(txn) { txn =>
          store.isEmpty(txn).map(_ shouldBe true)
        }
      }
  }

  "producing on channel, consuming on that channel and another, and then producing on the other channel" should
    "return a continuation and all the data" in withTestMany { implicit store =>
    val produceKey1 = List("ch1")

    val step1 = for {
      produceKey1Hash <- store.hashChannels(produceKey1)
      r1              <- produce(produceKey1.head, "datum1", persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r1 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey1Hash).map(_ shouldBe produceKey1)
        store.getPatterns(txn, produceKey1).map(_ shouldBe Nil)
        store.getData(txn, produceKey1).map(_ shouldBe List(Datum("datum1", persist = false)))
        store.getWaitingContinuation(txn, produceKey1).map(_ shouldBe Nil)
      }
    }

    val consumeKey     = List("ch1", "ch2")
    val consumePattern = List(Wildcard, Wildcard)

    val step2 = for {
      produceKey1Hash <- store.hashChannels(produceKey1)
      consumeKeyHash  <- store.hashChannels(consumeKey)
      r2              <- consume(consumeKey, consumePattern, new StringsCaptor, persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r2 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey1Hash).map(_ shouldBe produceKey1)
        store.getPatterns(txn, produceKey1).map(_ shouldBe Nil)
        store.getData(txn, produceKey1).map(_ shouldBe List(Datum("datum1", persist = false)))
        store.getWaitingContinuation(txn, produceKey1).map(_ shouldBe Nil)
        store.getChannels(txn, consumeKeyHash).map(_ shouldBe consumeKey)
        store.getPatterns(txn, consumeKey).map(_ shouldBe List(consumePattern))
        store.getData(txn, consumeKey).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, consumeKey).map(_ should not be empty)
      }
    }

    val produceKey2 = List("ch2")
    val step3 = for {
      produceKey1Hash <- store.hashChannels(produceKey1)
      consumeKeyHash  <- store.hashChannels(consumeKey)
      produceKey2Hash <- store.hashChannels(produceKey2)
      r3              <- produce(produceKey2.head, "datum2", persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r3 shouldBe defined

      runK(r3)
      getK(r3).results should contain theSameElementsAs List(List("datum1", "datum2"))

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey1Hash).map(_ shouldBe Nil)
        store.getPatterns(txn, produceKey1).map(_ shouldBe Nil)
        store.getData(txn, produceKey1).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, produceKey1).map(_ shouldBe Nil)
        store.getChannels(txn, consumeKeyHash).map(_ shouldBe Nil)
        store.getPatterns(txn, consumeKey).map(_ shouldBe Nil)
        store.getData(txn, consumeKey).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, consumeKey).map(_ shouldBe Nil)
        store.getChannels(txn, produceKey2Hash).map(_ shouldBe Nil)
        store.getPatterns(txn, produceKey2).map(_ shouldBe Nil)
        store.getData(txn, produceKey2).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, produceKey2).map(_ shouldBe Nil)
        store.isEmpty(txn).map(_ shouldBe true)
      }
    }

    List(step1, step2, step3)
  }

  "producing on three different channels and then consuming once on all three" should
    "return the continuation and all the data" in withTestMany { implicit store =>
    val produceKey1 = List("ch1")
    val produceKey2 = List("ch2")
    val produceKey3 = List("ch3")
    val consumeKey  = List("ch1", "ch2", "ch3")
    val patterns    = List(Wildcard, Wildcard, Wildcard)

    val step1 = for {
      produceKey1Hash <- store.hashChannels(produceKey1)
      r1              <- produce(produceKey1.head, "datum1", persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r1 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey1Hash).map(_ shouldBe produceKey1)
        store.getPatterns(txn, produceKey1).map(_ shouldBe Nil)
        store.getData(txn, produceKey1).map(_ shouldBe List(Datum("datum1", persist = false)))
        store.getWaitingContinuation(txn, produceKey1).map(_ shouldBe Nil)
      }
    }

    val step2 = for {
      produceKey2Hash <- store.hashChannels(produceKey2)
      r2              <- produce(produceKey2.head, "datum2", persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r2 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey2Hash).map(_ shouldBe produceKey2)
        store.getPatterns(txn, produceKey2).map(_ shouldBe Nil)
        store.getData(txn, produceKey2).map(_ shouldBe List(Datum("datum2", persist = false)))
        store.getWaitingContinuation(txn, produceKey2).map(_ shouldBe Nil)
      }
    }

    val step3 = for {
      produceKey3Hash <- store.hashChannels(produceKey3)
      r3              <- produce(produceKey3.head, "datum3", persist = false)
      txn             <- store.createTxnRead()
    } yield {
      r3 shouldBe None

      store.withTxn(txn) { txn =>
        store.getChannels(txn, produceKey3Hash).map(_ shouldBe produceKey3)
        store.getPatterns(txn, produceKey3).map(_ shouldBe Nil)
        store.getData(txn, produceKey3).map(_ shouldBe List(Datum("datum3", persist = false)))
        store.getWaitingContinuation(txn, produceKey3).map(_ shouldBe Nil)
      }
    }

    val step4 = for {
      consumeKeyHash <- store.hashChannels(consumeKey)
      r4             <- consume(List("ch1", "ch2", "ch3"), patterns, new StringsCaptor, persist = false)
      txn            <- store.createTxnRead()
    } yield {
      r4 shouldBe defined
      runK(r4)
      getK(r4).results should contain theSameElementsAs List(List("datum1", "datum2", "datum3"))

      store.withTxn(txn) { txn =>
        store.getChannels(txn, consumeKeyHash).map(_ shouldBe Nil)
        store.getPatterns(txn, consumeKey).map(_ shouldBe Nil)
        store.getData(txn, consumeKey).map(_ shouldBe Nil)
        store.getWaitingContinuation(txn, consumeKey).map(_ shouldBe Nil)

        store.isEmpty(txn).map(_ shouldBe true)
      }
    }

    List(step1, step2, step3, step4)
  }
}

class InMemoryStorageTest
    extends StorageTestsBase[
      Id,
      InMemoryStoreInstance.InMemoryContext[String, Pattern, String, StringsCaptor]] {

  implicit val monadF: Monad[Id] = cats.catsInstancesForId

  implicit val captureF: Capture[Id] = Capture.idCapture

  def createStore: TStore =
    InMemoryStoreInstance.storeInMemory[Id, String, Pattern, String, StringsCaptor]

  val ctx = new TCTX()
}

class LMDBStoreActionsTest extends StorageTestsBase[Id, LMDBContext] with BeforeAndAfterAll {
  val dbDir: Path   = Files.createTempDirectory("rchain-storage-test-")
  val mapSize: Long = 1024L * 1024L * 1024L

  implicit val monadF: Monad[Id] = cats.catsInstancesForId

  implicit val captureF: Capture[Id] = Capture.idCapture

  def createStore: TStore = StoreInstances.storeLMDB[Id, String, Pattern, String, StringsCaptor]
  val ctx                 = LMDBContext(dbDir, mapSize)

  override def afterAll(): Unit =
    recursivelyDeletePath(dbDir)
}
