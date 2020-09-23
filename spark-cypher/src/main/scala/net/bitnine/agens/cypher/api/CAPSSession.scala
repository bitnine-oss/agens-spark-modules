package net.bitnine.agens.cypher.api

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.planning.{RelationalCypherResult, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.graph.ScanGraph

import net.bitnine.agens.cypher.api.io._
import net.bitnine.agens.cypher.impl.graph.CAPSGraphFactory
import net.bitnine.agens.cypher.impl.table.SparkTable.DataFrameTable
import net.bitnine.agens.cypher.impl.{CAPSRecords, CAPSRecordsFactory}

import scala.reflect.runtime.universe._

/**
  * Spark specific Cypher session implementation.
  *
  * This class is the main entry point for working with the CAPS system. It wraps a [[SparkSession]] and allows
  * running Cypher queries over a set of distributed Spark data frames.
  *
  * @param sparkSession The Spark session representing the cluster to execute on
  */
sealed class CAPSSession(val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with Serializable {

  override type Result = RelationalCypherResult[DataFrameTable]

  override type Records = CAPSRecords

  protected implicit val caps: CAPSSession = this

  override val records: CAPSRecordsFactory = CAPSRecordsFactory()

  override val graphs: CAPSGraphFactory = CAPSGraphFactory()

  override val entityTables: CAPSEntityTableFactory.type = CAPSEntityTableFactory
  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[net.bitnine.agens.cypher.api.io.Node]]
    * @tparam R relationship type implementing [[net.bitnine.agens.cypher.api.io.Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty
  ): PropertyGraph = {
    graphs.create(CAPSNodeTable(nodes), CAPSRelationshipTable(relationships))
  }

  // **추가
  def readFrom[T <: Table[T] : TypeTag](entityTables: List[EntityTable[T]])(implicit session: RelationalCypherSession[T]): PropertyGraph = {
    create(None, entityTables: _ *)
  }

  // **추가
  def create[T <: Table[T] : TypeTag](maybeSchema: Option[Schema], entityTables: EntityTable[T]*)(implicit session: RelationalCypherSession[T]): PropertyGraph = {
    val allTables = entityTables
    val schema = maybeSchema.getOrElse(allTables.map(_.schema).reduce[Schema](_ ++ _))
    new ScanGraph(allTables, schema)
  }


  def sql(query: String): CAPSRecords =
    records.wrap(sparkSession.sql(query))
}

object CAPSSession extends Serializable {

  /**
    * Creates a new [[net.bitnine.agens.cypher.api.CAPSSession]] based on the given [[org.apache.spark.sql.SparkSession]].
    *
    * @return CAPS session
    */
  def create(implicit sparkSession: SparkSession): CAPSSession = new CAPSSession(sparkSession)

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(settings: (String, String)*): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.default.parallelism", "8")
    conf.setAll(settings)

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  /**
    * Import this into scope in order to use:
    *
    * {{{
    * import org.opencypher.caps.api.CAPSSession.RecordsAsDF
    * // ...
    * val df: DataFrame = results.records.asDF
    * }}}
    */
  implicit class RecordsAsDF(val records: CypherRecords) extends AnyVal {
    /**
      * Extracts the underlying [[org.apache.spark.sql#DataFrame]] from the given [[records]].
      *
      * Note that the column names in the returned DF do not necessarily correspond to the names of the Cypher RETURN
      * items, e.g. "RETURN n.name" does not mean that the column for that item is named "n.name".
      *
      * @return [[org.apache.spark.sql#DataFrame]] representing the records
      */
    def asDataFrame: DataFrame = records match {
      case caps: CAPSRecords => caps.table.df
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }

    /**
      * Converts all values stored in this table to instances of the corresponding CypherValue class.
      * In particular, this de-flattens, or collects, flattened entities (nodes and relationships) into
      * compact CypherNode/CypherRelationship objects.
      *
      * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
      *
      * @return [[org.apache.spark.sql.Dataset]] of CypherMaps
      */
    def asDataset: Dataset[CypherMap] = records match {
      case caps: CAPSRecords => caps.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }
}
