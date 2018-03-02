package coop.rchain.storage.test

import java.nio.charset.StandardCharsets

import coop.rchain.models.Serialize
import coop.rchain.storage.Match

object implicits {

  implicit object stringMatch extends Match[Pattern, String] {
    def get(p: Pattern, a: String): Option[String] = Some(a).filter(p.isMatch)
  }

  implicit object stringSerialize extends Serialize[String] {

    def encode(a: String): Array[Byte] =
      a.getBytes(StandardCharsets.UTF_8)

    def decode(bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, StandardCharsets.UTF_8))
  }

  implicit object patternSerializeWrapper extends Serialize[Pattern] {
    override def encode(a: Pattern): Array[Byte] =
      throw new NotImplementedError("TODO")

    override def decode(bytes: Array[Byte]): Either[Throwable, Pattern] =
      throw new NotImplementedError("TODO")
  }

  implicit object stringListSerialize extends Serialize[List[String] => Unit] {
    def encode(a: List[String] => Unit): Array[Byte] =
      throw new NotImplementedError("TODO")

    def decode(bytes: Array[Byte]): Either[Throwable, List[String] => Unit] =
      throw new NotImplementedError("TODO")
  }
}
