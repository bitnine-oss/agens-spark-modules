package net.bitnine.agens.cypher.api.value

import org.opencypher.okapi.api.value.CypherValue._
import net.bitnine.agens.cypher.api.value.CAPSEntity._

import net.bitnine.agens.cypher.impl.expressions.AddPrefix.addPrefix
import net.bitnine.agens.cypher.impl.expressions.EncodeLong._


object CAPSEntity {

  implicit class RichId(id: Seq[Byte]) {

    def toHex: String = s"0x${id.map(id => "%02X".format(id)).mkString}"

  }

  implicit class LongIdEncoding(val l: Long) extends AnyVal {

    def withPrefix(prefix: Int): Array[Byte] = l.encodeAsCAPSId.withPrefix(prefix.toByte)

    def encodeAsCAPSId: Array[Byte] = encodeLong(l)

  }

  implicit class RichCAPSId(val id: Array[Byte]) extends AnyVal {

    def withPrefix(prefix: Int): Array[Byte] = addPrefix(prefix.toByte, id)

  }

}

object CAPSNode {

  def apply(
    id: Long,
    labels: Set[String]
  ): CAPSNode = CAPSNode(id.encodeAsCAPSId, labels)

  def apply(
    id: Long,
    labels: Set[String],
    properties: CypherMap
  ): CAPSNode = CAPSNode(id.encodeAsCAPSId, labels, properties)

}

/**
  * Representation of a Cypher node in the CAPS implementation. A node contains an id of type [[Long]], a set of string labels and a map of properties.
  *
  * @param id         the id of the node, unique within the containing graph.
  * @param labels     the labels of the node.
  * @param properties the properties of the node.
  */
case class CAPSNode(
  override val id: Seq[Byte],
  override val labels: Set[String] = Set.empty,
  override val properties: CypherMap = CypherMap.empty
) extends CypherNode[Seq[Byte]] {

  override type I = CAPSNode

  override def copy(id: Seq[Byte] = id, labels: Set[String] = labels, properties: CypherMap = properties): CAPSNode = {
    CAPSNode(id, labels, properties)
  }
  override def toString: String = s"${getClass.getSimpleName}(id=${id.toHex}, labels=$labels, properties=$properties)"
}

object CAPSRelationship {

  def apply(
    id: Long,
    startId: Long,
    endId: Long,
    relType: String
  ): CAPSRelationship = CAPSRelationship(id.encodeAsCAPSId, startId.encodeAsCAPSId, endId.encodeAsCAPSId, relType)

  def apply(
    id: Long,
    startId: Long,
    endId: Long,
    relType: String,
    properties: CypherMap
  ): CAPSRelationship = CAPSRelationship(id.encodeAsCAPSId, startId.encodeAsCAPSId, endId.encodeAsCAPSId, relType, properties)
}

/**
  * Representation of a Cypher relationship in the CAPS implementation. A relationship contains an id of type [[Long]], ids of its adjacent nodes, a relationship type and a map of properties.
  *
  * @param id         the id of the relationship, unique within the containing graph.
  * @param startId    the id of the source node.
  * @param endId      the id of the target node.
  * @param relType    the relationship type.
  * @param properties the properties of the node.
  */
case class CAPSRelationship(
  override val id: Seq[Byte],
  override val startId: Seq[Byte],
  override val endId: Seq[Byte],
  override val relType: String,
  override val properties: CypherMap = CypherMap.empty
) extends CypherRelationship[Seq[Byte]] {

  override type I = CAPSRelationship

  override def copy(
    id: Seq[Byte] = id,
    startId: Seq[Byte] = startId,
    endId: Seq[Byte] = endId,
    relType: String = relType,
    properties: CypherMap = properties
  ): CAPSRelationship = CAPSRelationship(id, startId, endId, relType, properties)

  override def toString: String = s"${getClass.getSimpleName}(id=${id.toHex}, startId=${startId.toHex}, endId=${endId.toHex}, relType=$relType, properties=$properties)"

}
