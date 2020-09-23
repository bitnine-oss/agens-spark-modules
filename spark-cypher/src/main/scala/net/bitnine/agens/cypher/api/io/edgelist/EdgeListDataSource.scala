package net.bitnine.agens.cypher.api.io.edgelist

import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.GraphEntity.sourceIdKey
import net.bitnine.agens.cypher.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import net.bitnine.agens.cypher.api.io.edgelist.EdgeListDataSource._
import net.bitnine.agens.cypher.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import net.bitnine.agens.cypher.schema.CAPSSchema


object EdgeListDataSource {

  val NODE_LABEL = "V"

  val REL_TYPE = "E"

  val GRAPH_NAME = GraphName("graph")

  val SCHEMA: Schema = CAPSSchema.empty
    .withNodePropertyKeys(Set(NODE_LABEL), PropertyKeys.empty)
    .withRelationshipPropertyKeys(REL_TYPE, PropertyKeys.empty)
}

/**
  * A read-only data source that is able to read graphs from edge list files. Input files are expected to contain one
  * edge per row, e.g.
  *
  * 0 1
  * 1 2
  *
  * describes a graph with two edges (one from vertex 0 to 1 and one from vertex 1 to 2).
  *
  * The data source can be parameterized with options used by the underlying Spark Csv reader.
  *
  * @param path    path to the edge list file
  * @param options Spark Csv reader options
  * @param caps    CAPS session
  */
case class EdgeListDataSource(path: String, options: Map[String, String] = Map.empty)(implicit caps: CAPSSession)
  extends PropertyGraphDataSource {

  override def hasGraph(name: GraphName): Boolean = name == GRAPH_NAME

  override def graph(name: GraphName): PropertyGraph = {
    val reader = options.foldLeft(caps.sparkSession.read) {
      case (current, (key, value)) => current.option(key, value)
    }

    val rawRels = reader
      .schema(StructType(Seq(
        StructField(sourceStartNodeKey, LongType),
        StructField(sourceEndNodeKey, LongType))))
      .csv(path)
      .withColumn(sourceIdKey, functions.monotonically_increasing_id())
      .select(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)

    val rawNodes = rawRels
      .select(rawRels.col(sourceStartNodeKey).as(sourceIdKey))
      .union(rawRels.select(rawRels.col(sourceEndNodeKey).as(sourceIdKey)))
      .distinct()

    caps.graphs.create(CAPSNodeTable(Set(NODE_LABEL), rawNodes), CAPSRelationshipTable(REL_TYPE, rawRels))
  }

  override def schema(name: GraphName): Option[Schema] = Some(SCHEMA)

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw UnsupportedOperationException("Storing an edge list is not supported")

  override def delete(name: GraphName): Unit =
    throw UnsupportedOperationException("Deleting an edge list is not supported")

  override val graphNames: Set[GraphName] = Set(GRAPH_NAME)
}
