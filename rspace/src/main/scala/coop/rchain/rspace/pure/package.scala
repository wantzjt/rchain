package coop.rchain.rspace

import cats.Monad
import coop.rchain.catscontrib.Capture
import coop.rchain.metrics.Metrics
import cats.implicits._

import scala.collection.immutable._
import coop.rchain.rspace.{consume => fconsume, install => finstall, produce => fproduce}

/**
  * Monad wrapper for [[coop.rchain.rspace]] package object (produce/consume/install)
  */
package object pure {

  /** Searches the store for data matching all the given patterns at the given channels.
    *
    * If no match is found, then the continuation and patterns are put in the store at the given
    * channels.
    *
    * If a match is found, then the continuation is returned along with the matching data.
    *
    * Matching data stored with the `persist` flag set to `true` will not be removed when it is
    * retrieved. See below for more information about using the `persist` flag.
    *
    * '''NOTE''':
    *
    * A call to [[consume]] that is made with the persist flag set to `true` only persists when
    * there is no matching data.
    *
    * This means that in order to make a continuation "stick" in the store, the user will have to
    * continue to call [[consume]] until a `None` is received.
    *
    * @param store A store which satisfies the [[IStore]] interface.
    * @param channels A Seq of channels on which to search for matching data
    * @param patterns A Seq of patterns with which to search for matching data
    * @param continuation A continuation
    * @param persist Whether or not to attempt to persist the data
    * @param m A match that matches patterns with data
    * @param c A [[Capture]]
    * @tparam F A monad
    * @tparam C A type representing a channel
    * @tparam P A type representing a pattern
    * @tparam A A type representing a piece of data
    * @tparam K A type representing a continuation
    * @return
    */
  def consume[F[_], C, P, A, K](store: IStore[C, P, A, K],
                                channels: Seq[C],
                                patterns: Seq[P],
                                continuation: K,
                                persist: Boolean)(implicit m: Match[P, A],
                                                  c: Capture[F],
                                                  mnd: Monad[F],
                                                  mcs: Metrics[F]): F[Option[(K, Seq[A])]] =
    for {
      _ <- Metrics[F].incrementCounter("rspace-2-consumes-count")
      capTime <- c.capture {
                  val nsStart = System.nanoTime()
                  (fconsume(store, channels, patterns, continuation, persist),
                   System.nanoTime() - nsStart)
                }
      _ <- Metrics[F].record("rspace-2-consume-time", capTime._2)
    } yield capTime._1

  /** Searches the store for a continuation that has patterns that match the given data at the
    * given channel.
    *
    * If no match is found, then the data is put in the store at the given channel.
    *
    * If a match is found, then the continuation is returned along with the matching data.
    *
    * Matching data or continuations stored with the `persist` flag set to `true` will not be
    * removed when they are retrieved. See below for more information about using the `persist`
    * flag.
    *
    * '''NOTE''':
    *
    * A call to [[produce]] that is made with the persist flag set to `true` only persists when
    * there are no matching continuations.
    *
    * This means that in order to make a piece of data "stick" in the store, the user will have to
    * continue to call [[produce]] until a `None` is received.
    *
    * @param store A store which satisfies the [[IStore]] interface.
    * @param channel A channel on which to search for matching continuations and/or store data
    * @param data A piece of data
    * @param persist Whether or not to attempt to persist the data
    * @param m A match that matches patterns with data
    * @param c A [[Capture]]
    * @tparam F A monad
    * @tparam C A type representing a channel
    * @tparam P A type representing a pattern
    * @tparam A A type representing a piece of data
    * @tparam K A type representing a continuation
    */
  def produce[F[_], C, P, A, K](store: IStore[C, P, A, K], channel: C, data: A, persist: Boolean)(
      implicit
      m: Match[P, A],
      c: Capture[F],
      mnd: Monad[F],
      mcs: Metrics[F]): F[Option[(K, Seq[A])]] =
    for {
      _ <- Metrics[F].incrementCounter("rspace-2-produces-count")
      capTime <- c.capture {
                  val nsStart = System.nanoTime()
                  (fproduce(store, channel, data, persist), System.nanoTime() - nsStart)
                }
      _ <- Metrics[F].record("rspace-2-produce-time", capTime._2)
    } yield capTime._1

  /** Searches the store for data matching all the given patterns at the given channels.
    *
    * If no match is found, all existing continuations, data and joins removed,
    * and then the continuation and patterns are put in the store at the given
    * channels.
    *
    * If a match is found, then the continuation is returned along with the matching data.
    *
    * Matching data stored with the `persist` flag set to `true` will not be removed when it is
    * retrieved. See below for more information about using the `persist` flag.
    *
    * '''NOTE''':
    *
    * A call to [[consume]] that is made with the persist flag set to `true` only persists when
    * there is no matching data.
    *
    * This means that in order to make a continuation "stick" in the store, the user will have to
    * continue to call [[consume]] until a `None` is received.
    *
    * @param store A store which satisfies the [[IStore]] interface.
    * @param channels A Seq of channels on which to search for matching data
    * @param patterns A Seq of patterns with which to search for matching data
    * @param continuation A continuation
    * @param m A match that matches patterns with data
    * @param c A [[Capture]]
    * @tparam F A monad
    * @tparam C A type representing a channel
    * @tparam P A type representing a pattern
    * @tparam A A type representing a piece of data
    * @tparam K A type representing a continuation
    */
  def install[F[_], C, P, A, K](
      store: IStore[C, P, A, K],
      channels: Seq[C],
      patterns: Seq[P],
      continuation: K)(implicit m: Match[P, A], c: Capture[F]): F[Option[(K, Seq[A])]] =
    c.capture(finstall(store, channels, patterns, continuation))
}
