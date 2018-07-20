package coop.rchain.rspace.bench

import cats.implicits._
import coop.rchain.rspace.bench.BasicBench.BenchState
import coop.rchain.rspace.{Blake2b256Hash, Checkpoint}
import coop.rchain.rspace.examples.StringExamples._
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.history.Branch
import coop.rchain.rspace.util._
import coop.rchain.rspace.{LMDBStore, _}
import coop.rchain.rspace.util.{getK, runK}
import org.openjdk.jmh.annotations._
import scodec.Codec

//for debug/check run from IDEA with
//rspaceBench/jmh:run BulkInsertBench -i 1 -wi 0 -f 0 -t 1
//on hyper-threaded machines for standalone check use -f <number of physical cores>
//for example
//java -jar target/scala-2.12/rspacebench_2.12-0.1.0-SNAPSHOT.jar BulkInsertBench -i 10 -wi 5 -f 2 -t 2
class BulkInsertBench {
  import BulkInsertBench._

  @Benchmark
  @Threads(1) //we're checking transactions performance, so don't waster time/resources on threads concurrency
  def insertCursors(state: BulkInsertState): Unit = {
    state.testStore.bulkInserUseCursors = true
    state.testSpace.reset(state.checkpoint.get.root)
  }

  @Benchmark
  @Threads(1) //we're checking transactions performance, so don't waster time/resources on threads concurrency
  def insertStandard(state: BulkInsertState): Unit = {
    state.testStore.bulkInserUseCursors = false
    state.testSpace.reset(state.checkpoint.get.root)
  }
}

object BulkInsertBench {
  implicit val codecString: Codec[String]   = implicitly[Serialize[String]].toCodec
  implicit val codecP: Codec[Pattern]       = implicitly[Serialize[Pattern]].toCodec
  implicit val codecK: Codec[StringsCaptor] = implicitly[Serialize[StringsCaptor]].toCodec

  @State(Scope.Benchmark)
  class BulkInsertState extends BenchState {
    var checkpoint: Option[Checkpoint]      = None
    var emptyCheckpoint: Option[Checkpoint] = None

    @Setup
    def doSetup(): Unit = {
      println("setting up BulkInsertBench")
      emptyCheckpoint = Some(testSpace.createCheckpoint())

      for (i <- 0 to 1000) {
        testSpace.consume(List("ch1_" + i.toString, "ch2" + i.toString),
                          List(StringMatch("bad"), StringMatch("finger")),
                          new StringsCaptor,
                          false)

        testSpace.produce("ch1_" + i.toString, "bad", false)
      }

      checkpoint = Some(testSpace.createCheckpoint())
      println("BulkInsertBench setup finished")
    }
  }
}
