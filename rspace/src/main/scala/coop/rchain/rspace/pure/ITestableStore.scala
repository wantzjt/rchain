package coop.rchain.rspace.pure

import scala.collection.immutable.Seq

/**
	* Used for unit-tests and other rspace-local calls
	*/
private[rspace] trait ITestableStore[F[_], C, P] {

  private[rspace] type T

  def getPatterns(txn: T, channels: Seq[C]): F[Seq[Seq[P]]]

  def isEmpty(txn: T): F[Boolean]

  def clear(): F[Unit]
}
