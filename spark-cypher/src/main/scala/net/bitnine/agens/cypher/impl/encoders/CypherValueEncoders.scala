package net.bitnine.agens.cypher.impl.encoders

import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.okapi.api.value.CypherValue._

import net.bitnine.agens.cypher.api.value.{CAPSNode, CAPSRelationship}


trait CypherValueEncoders extends LowPriorityCypherValueEncoders {
  implicit def cypherNodeEncoder: ExpressionEncoder[CAPSNode] = kryo[CAPSNode]

  implicit def cypherRelationshipEncoder: ExpressionEncoder[CAPSRelationship] = kryo[CAPSRelationship]

  implicit def cypherMapEncoder: ExpressionEncoder[CypherMap] = kryo[CypherMap]
}
