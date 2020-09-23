package net.bitnine.agens.cypher.impl.encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.okapi.api.value.CypherValue.CypherValue

import scala.language.implicitConversions


trait LowPriorityCypherValueEncoders {
  implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]

  implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]

  implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
}
