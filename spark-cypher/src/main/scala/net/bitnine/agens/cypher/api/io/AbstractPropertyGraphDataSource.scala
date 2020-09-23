package net.bitnine.agens.cypher.api.io

import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTIdentity, CypherType}
import org.opencypher.okapi.impl.exception.GraphNotFoundException
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.metadata.CAPSGraphMetaData
import net.bitnine.agens.cypher.api.io.util.CAPSGraphExport._
import net.bitnine.agens.cypher.impl.CAPSConverters._
import net.bitnine.agens.cypher.impl.io.CAPSPropertyGraphDataSource
import net.bitnine.agens.cypher.schema.CAPSSchema
import net.bitnine.agens.cypher.schema.CAPSSchema._


object AbstractPropertyGraphDataSource {

  def nodeColsWithCypherType(schema: Schema, labelCombination: Set[String]): Map[String, CypherType] = {
    val propertyColsWithCypherType = schema.nodePropertyKeysForCombinations(Set(labelCombination)).map {
      case (key, cypherType) => key.toPropertyColumnName -> cypherType
    }
    propertyColsWithCypherType + (GraphEntity.sourceIdKey -> CTIdentity)
  }

  def relColsWithCypherType(schema: Schema, relType: String): Map[String, CypherType] = {
    val propertyColsWithCypherType = schema.relationshipPropertyKeys(relType).map {
      case (key, cypherType) => key.toPropertyColumnName -> cypherType
    }
    propertyColsWithCypherType ++ Relationship.nonPropertyAttributes.map(_ -> CTIdentity)
  }
}

/**
  * Abstract data source implementation that takes care of caching graph names and schemas.
  *
  * It automatically creates initializes a ScanGraphs an only requires the implementor to provider simpler methods for
  * reading/writing files and tables.
  */
abstract class AbstractPropertyGraphDataSource extends CAPSPropertyGraphDataSource {

  implicit val caps: CAPSSession

  def tableStorageFormat: StorageFormat

  protected var schemaCache: Map[GraphName, CAPSSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected def readSchema(graphName: GraphName): CAPSSchema

  protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit

  protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData

  protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, labelCombination: Set[String], sparkSchema: StructType): DataFrame

  protected def writeNodeTable(graphName: GraphName, labelCombination: Set[String], table: DataFrame): Unit

  protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame

  protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit

  override def graphNames: Set[GraphName] = graphNameCache

  override def hasGraph(graphName: GraphName): Boolean = graphNameCache.contains(graphName)

  override def delete(graphName: GraphName): Unit = {
    deleteGraph(graphName)
    schemaCache -= graphName
    graphNameCache -= graphName
  }

  override def graph(name: GraphName): PropertyGraph = {
    if (!hasGraph(name)) {
      throw GraphNotFoundException(s"Graph with name '$name'")
    } else {
      val capsSchema: CAPSSchema = schema(name).get
      val capsMetaData: CAPSGraphMetaData = readCAPSGraphMetaData(name)
      val nodeTables = capsSchema.allCombinations.map { combo =>
        val df = readNodeTable(name, combo, capsSchema.canonicalNodeStructType(combo))
        CAPSNodeTable(combo, df)
      }
      val relTables = capsSchema.relationshipTypes.map { relType =>
        val df = readRelationshipTable(name, relType, capsSchema.canonicalRelStructType(relType))
        CAPSRelationshipTable(relType, df)
      }
      if (nodeTables.isEmpty) {
        caps.graphs.empty
      } else {
        caps.graphs.create(Some(capsSchema), (nodeTables ++ relTables).toSeq: _*)
      }
    }
  }

  override def schema(graphName: GraphName): Option[CAPSSchema] = {
    if (schemaCache.contains(graphName)) {
      schemaCache.get(graphName)
    } else {
      val s = readSchema(graphName)
      schemaCache += graphName -> s
      Some(s)
    }
  }


  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    checkStorable(graphName)

    val poolSize = caps.sparkSession.sparkContext.statusTracker.getExecutorInfos.length

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(poolSize))

    try {
      val relationalGraph = graph.asCaps

      val schema = relationalGraph.schema.asCaps
      schemaCache += graphName -> schema
      graphNameCache += graphName
      writeCAPSGraphMetaData(graphName, CAPSGraphMetaData(tableStorageFormat.name))
      writeSchema(graphName, schema)

      val nodeWrites = schema.labelCombinations.combos.map { combo =>
        Future {
          writeNodeTable(graphName, combo, relationalGraph.canonicalNodeTable(combo))
        }
      }

      val relWrites = schema.relationshipTypes.map { relType =>
        Future {
          writeRelationshipTable(graphName, relType, relationalGraph.canonicalRelationshipTable(relType))
        }
      }

      waitForWriteCompletion(nodeWrites)
      waitForWriteCompletion(relWrites)
    } finally {
      executionContext.shutdown()
    }
  }

  protected def waitForWriteCompletion(writeFutures: Set[Future[Unit]])(implicit ec: ExecutionContext): Unit = {
    writeFutures.foreach { writeFuture =>
      Await.ready(writeFuture, Duration.Inf)
      writeFuture.onComplete {
        case Success(_) =>
        case Failure(e) => throw e
      }
    }
  }
}
