package net.bitnine.agens.cypher.api.io.sql

import java.net.URI

import org.apache.spark.sql.{DataFrame, DataFrameReader, functions}
import org.opencypher.graphddl.GraphDdl.PropertyMappings
import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

import scala.reflect.io.Path

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.AbstractPropertyGraphDataSource._
import net.bitnine.agens.cypher.api.io.GraphEntity.sourceIdKey
import net.bitnine.agens.cypher.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import net.bitnine.agens.cypher.api.io._
import net.bitnine.agens.cypher.api.io.sql.IdGenerationStrategy._
import net.bitnine.agens.cypher.api.io.sql.SqlDataSourceConfig.{File, Hive, Jdbc}
import net.bitnine.agens.cypher.impl.convert.SparkConversions._
import net.bitnine.agens.cypher.impl.io.CAPSPropertyGraphDataSource
import net.bitnine.agens.cypher.impl.table.SparkTable._
import net.bitnine.agens.cypher.schema.CAPSSchema
import net.bitnine.agens.cypher.schema.CAPSSchema._


case class SqlPropertyGraphDataSource(
  graphDdl: GraphDdl,
  sqlDataSourceConfigs: Map[String, SqlDataSourceConfig],
  idGenerationStrategy: IdGenerationStrategy = SerializedId
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = graphDdl.graphs.contains(graphName)

  override def graph(graphName: GraphName): PropertyGraph = {

    val ddlGraph = graphDdl.graphs.getOrElse(graphName, throw GraphNotFoundException(s"Graph $graphName not found"))
    val schema = ddlGraph.graphType

    // Build CAPS node tables
    val nodeDataFrames = ddlGraph.nodeToViewMappings.mapValues(nvm => readTable(nvm.view))

    // Generate node identifiers
    val nodeDataFramesWithIds = createIdForTables(nodeDataFrames, ddlGraph, sourceIdKey, idGenerationStrategy, schema)

    val nodeTables = nodeDataFramesWithIds.map {
      case (nodeViewKey, nodeDf) =>
        val nodeElementTypes = nodeViewKey.nodeType.elementTypes
        val columnsWithType = nodeColsWithCypherType(schema, nodeElementTypes)
        val inputNodeMapping = createNodeMapping(nodeElementTypes, ddlGraph.nodeToViewMappings(nodeViewKey).propertyMappings)
        val normalizedDf = normalizeDataFrame(nodeDf, inputNodeMapping, columnsWithType).castToLong
        val normalizedMapping = normalizeMapping(inputNodeMapping)

        normalizedDf.validateColumnTypes(columnsWithType)

        CAPSEntityTable.create(normalizedMapping, normalizedDf)
    }.toSeq

    // Build CAPS relationship tables
    val relDataFrames = ddlGraph.edgeToViewMappings.map(evm => evm.key -> readTable(evm.view)).toMap

    // Generate relationship identifiers
    val relDataFramesWithIds = createIdForTables(relDataFrames, ddlGraph, sourceIdKey, idGenerationStrategy, schema)

    val relationshipTables = ddlGraph.edgeToViewMappings.map { edgeToViewMapping =>
      val edgeViewKey = edgeToViewMapping.key
      val relElementType = edgeViewKey.relType.elementType
      val relDf = relDataFramesWithIds(edgeViewKey)
      val startNodeViewKey = edgeToViewMapping.startNode.nodeViewKey
      val endNodeViewKey = edgeToViewMapping.endNode.nodeViewKey

      // generate the start/end node id using the same parameters as for the corresponding node table
      val idColumnNamesStartNode = edgeToViewMapping.startNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
      val relsWithStartNodeId = createIdForTable(relDf, startNodeViewKey, idColumnNamesStartNode, sourceStartNodeKey, idGenerationStrategy, schema)
      val idColumnNamesEndNode = edgeToViewMapping.endNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
      val relsWithEndNodeId = createIdForTable(relsWithStartNodeId, endNodeViewKey, idColumnNamesEndNode, sourceEndNodeKey, idGenerationStrategy, schema)

      val columnsWithType = relColsWithCypherType(schema, relElementType)
      val inputRelMapping = createRelationshipMapping(relElementType, edgeToViewMapping.propertyMappings)
      val normalizedDf = normalizeDataFrame(relsWithEndNodeId, inputRelMapping, columnsWithType).castToLong
      val normalizedMapping = normalizeMapping(inputRelMapping)

      normalizedDf.validateColumnTypes(columnsWithType)

      CAPSEntityTable.create(normalizedMapping, normalizedDf)
    }

    caps.graphs.create(Some(schema), nodeTables.head, nodeTables.tail ++ relationshipTables: _*)
  }

  private def readTable(viewId: ViewId): DataFrame = {
    val sqlDataSourceConfig = sqlDataSourceConfigs.get(viewId.dataSource) match {
      case None =>
        val knownDataSources = sqlDataSourceConfigs.keys.mkString("'", "';'", "'")
        throw SqlDataSourceConfigException(s"Data source '${viewId.dataSource}' not configured; see data sources configuration. Known data sources: $knownDataSources")
      case Some(config) =>
        config
    }

    val inputTable = sqlDataSourceConfig match {
      case hive@Hive => readSqlTable(viewId, hive)
      case jdbc: Jdbc => readSqlTable(viewId, jdbc)
      case file: File => readFile(viewId, file)
    }

    inputTable.safeRenameColumns(inputTable.columns.map(col => col -> col.toPropertyColumnName).toMap)
  }

  private def readSqlTable(viewId: ViewId, sqlDataSourceConfig: SqlDataSourceConfig) = {
    val spark = caps.sparkSession

    implicit class DataFrameReaderOps(read: DataFrameReader) {
      def maybeOption(key: String, value: Option[String]): DataFrameReader =
        value.fold(read)(read.option(key, _))
    }

    sqlDataSourceConfig match {
      case Jdbc(url, driver, options) =>
        spark.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("fetchSize", "100") // default value
          .options(options)
          .option("dbtable", viewId.tableName)
          .load()

      case SqlDataSourceConfig.Hive =>
        spark.table(viewId.tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }
  }

  private def readFile(viewId: ViewId, dataSourceConfig: File): DataFrame = {
    val spark = caps.sparkSession

    val optionsByFormat: Map[StorageFormat, Map[String, String]] = Map(
      FileFormat.csv -> Map("header" -> "true", "inferSchema" -> "true")
    )

    val viewPath = viewId.parts.lastOption.getOrElse(
      malformed("File names must be defined with the data source", viewId.parts.mkString(".")))

    val filePath = if (new URI(viewPath).isAbsolute) {
      viewPath
    } else {
      dataSourceConfig.basePath match {
        case Some(rootPath) => (Path(rootPath) / Path(viewPath)).toString()
        case None => unsupported("Relative view file names require basePath to be set")
      }
    }

    spark.read
      .format(dataSourceConfig.format.name)
      .options(optionsByFormat.getOrElse(dataSourceConfig.format, Map.empty))
      .options(dataSourceConfig.options)
      .load(filePath.toString)
  }

  private def normalizeDataFrame(
    dataFrame: DataFrame,
    mapping: EntityMapping,
    columnTypes: Map[String, CypherType]
  ): DataFrame = {
    val fields = dataFrame.schema.fields
    val indexedFields = fields.map(field => field.name.toLowerCase).zipWithIndex.toMap

    val properties = mapping.properties.values.flatten
    val columnRenamings = properties.map {
      case (property, column) if indexedFields.contains(column.toLowerCase) =>
        fields(indexedFields(column.toLowerCase)).name -> property.toPropertyColumnName
      case (_, column) => throw IllegalArgumentException(
        expected = s"Column with name $column",
        actual = indexedFields)
    }.toMap
    val renamedDf = dataFrame.safeRenameColumns(columnRenamings)
    val columnCasts = columnTypes.map { case (columnName, cypherType) =>
      renamedDf.col(columnRenamings.getOrElse(columnName, columnName)) -> cypherType.getSparkType
    }
    renamedDf.transformColumns(columnTypes.keys.toSeq: _*)(column => column.cast(columnCasts(column)))
  }

  private def normalizeMapping(mapping: EntityMapping): EntityMapping = {
    val updatedMapping = mapping.properties.map {
      case (entity, propertyMap) => entity -> propertyMap.map { case (prop, _) => prop -> prop.toPropertyColumnName }
    }

    mapping.copy(properties = updatedMapping)
  }

  private def createNodeMapping(labelCombination: Set[String], propertyMappings: PropertyMappings): EntityMapping = {
    val propertyKeyMapping = propertyMappings.map {
      case (propertyKey, columnName) => propertyKey -> columnName.toPropertyColumnName
    }

    NodeMappingBuilder
      .on(sourceIdKey)
      .withImpliedLabels(labelCombination.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build
  }

  private def createRelationshipMapping(
    relType: String,
    propertyMappings: PropertyMappings
  ): EntityMapping = {
    val propertyKeyMapping = propertyMappings.map {
      case (propertyKey, columnName) => propertyKey -> columnName.toPropertyColumnName
    }

    RelationshipMappingBuilder
      .on(sourceIdKey)
      .from(sourceStartNodeKey)
      .to(sourceEndNodeKey)
      .withRelType(relType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build
  }

  /**
    * Creates a 64-bit identifier for each row in the given input table. The identifier is computed by hashing or
    * serializing (depending on the strategy) the view name, the element type (i.e. its labels) and the values stored in a
    * given set of columns.
    *
    * @param dataFrame      input table / view
    * @param elementViewKey node / edge view key used for hashing
    * @param idColumnNames  columns used for hashing
    * @param newIdColumn    name of the new id column
    * @tparam T node / edge view key
    * @return input table / view with an additional column that contains potentially unique identifiers
    */
  private def createIdForTable[T <: ElementViewKey](
    dataFrame: DataFrame,
    elementViewKey: T,
    idColumnNames: List[String],
    newIdColumn: String,
    strategy: IdGenerationStrategy,
    schema: Schema
  ): DataFrame = {
    val idColumns = idColumnNames.map(dataFrame.col)
    strategy match {
      case HashedId =>
        val viewLiteral = functions.lit(elementViewKey.viewId.parts.mkString("."))
        val elementTypeLiterals = elementViewKey.elementType.toSeq.sorted.map(functions.lit)
        dataFrame.withHashColumn(Seq(viewLiteral) ++ elementTypeLiterals ++ idColumns, newIdColumn)
      case SerializedId =>
        val typeToId: Map[List[String], Int] =
          (schema.labelCombinations.combos.map(_.toList.sorted) ++ schema.relationshipTypes.map(List(_)))
            .toList
            .sortBy(s => s.mkString)
            .zipWithIndex.toMap
        val elementTypeToIntegerId = typeToId(elementViewKey.elementType.toList.sorted)
        dataFrame.withSerializedIdColumn(functions.lit(elementTypeToIntegerId) :: idColumns, newIdColumn)
    }
  }

  /**
    * Creates a 64-bit identifier for each row in the given input tables. The identifier is computed by hashing or
    * serializing a specific set of columns of the input table. For node tables, we either pick the the join columns
    * from the relationship mappings (i.e. the columns we join on) or all columns if the node is unconnected.
    *
    * In order to avoid or reduce the probability of ID collisions (depending on the strategy), the view name and the
    * element type (i.e. its labels) are additional input for the hash function and ID serializer.
    *
    * @param views       input tables
    * @param ddlGraph    DDL graph instance definition
    * @param newIdColumn name of the new id column
    * @tparam T node / edge view key
    * @return input tables with an additional column that contains potentially unique identifiers
    */
  private def createIdForTables[T <: ElementViewKey](
    views: Map[T, DataFrame],
    ddlGraph: Graph,
    newIdColumn: String,
    strategy: IdGenerationStrategy,
    schema: Schema
  ): Map[T, DataFrame] = {
    views.map { case (elementViewKey, dataFrame) =>
      val idColumnNames = elementViewKey match {
        case nvk: NodeViewKey => ddlGraph.nodeIdColumnsFor(nvk) match {
          case Some(columnNames) => columnNames.map(_.toPropertyColumnName)
          case None => dataFrame.columns.toList
        }
        case _: EdgeViewKey => dataFrame.columns.toList
      }
      elementViewKey -> createIdForTable(dataFrame, elementViewKey, idColumnNames, newIdColumn, strategy, schema)
    }
  }

  override def schema(name: GraphName): Option[CAPSSchema] = graphDdl.graphs.get(name).map(_.graphType.asCaps)

  override def store(name: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  override def delete(name: GraphName): Unit = unsupported("deleting a graph")

  override def graphNames: Set[GraphName] = graphDdl.graphs.keySet

  private val className = getClass.getSimpleName

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  private def notFound(needle: Any, haystack: Traversable[Any] = Traversable.empty): Nothing =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${stringList(haystack)}" else "",
      actual = needle
    )

  def malformed(desc: String, identifier: String): Nothing =
    throw MalformedIdentifier(s"$desc: $identifier")

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")

}
